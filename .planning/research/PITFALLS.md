# Pitfalls: Code Review Findings and Prevention for natsql v2.0.0

**Project:** natsql — NATS-native materialized view engine
**Researched:** 2026-05-31
**Confidence:** HIGH — all pitfalls verified against source code through systematic code review

## Critical Pitfalls Found in Code Review

### Pitfall CR-01: Concurrent Workers Destroy Stream Ordering

**What goes wrong:** Processing JetStream messages with concurrent goroutines (16 workers) destroys the per-subject ordering guarantee. Two updates for the same primary key can be applied in the wrong order, corrupting materialized state.

**Why it happens:** The materializer creates one durable consumer per view, which NATS delivers in order. A bridge goroutine feeds messages into a buffered channel, and 16 worker goroutines drain that channel concurrently. `kv.Put` is last-writer-wins, so the final state depends on goroutine scheduling, not message order.

**Warning signs:**
- Integration tests with rapid updates to the same PK show incorrect final values
- Race detector (`-race`) may not catch this — it's a logic bug, not a data race
- Materialized state appears to "jump backwards" for frequently-updated keys

**Prevention:**
- Remove the worker pool entirely (this fix)
- Process messages sequentially in a single goroutine per view
- If throughput is insufficient, partition the source stream by subject and create one sequential consumer per partition

**Recovery:** Re-materialize from source stream. The stream contains the true event order; replay reconstructs correct state.

---

### Pitfall CR-02: Double PK Sanitization Creates Unreachable Rows

**What goes wrong:** Rows with PK values containing `_`, `|`, `/`, `*`, or `>` are stored under a double-sanitized key on write but single-sanitized on read. The row is permanently unreachable via query.

**Why it happens:** The PK encoding path has three stages — mapper, writer, and query planner — that each handle sanitization independently:
1. `mapper.go` calls `stringifyValue(val)` which calls `SanitizePK()`
2. The mapper joins sanitized parts and stores the result in `RowMutation.PK`
3. `writer.go` calls `kv.PkKey(viewName, mut.PK)` which calls `SanitizePK()` again on the already-sanitized string
4. The query planner joins raw values (unsanitized) and `executor.go` calls `kv.PkKey()` which sanitizes once

Result: write path sanitizes twice, read path sanitizes once. Different keys.

**Warning signs:**
- Rows with underscores in PK values are missing from query results
- `kv.Keys()` reveals keys with double-encoded patterns (`__p` instead of `_p`, `__` instead of `_`)
- Black-box tests with `_`, `|`, `/`, `*`, `>` in PK values fail

**Prevention:**
- Design a single canonical PK encoder function used by ALL paths
- Store raw PK parts in `RowMutation.PK`, sanitize only at the KV boundary
- The function `BuildPkKey(viewName, pkParts, separator)` is the single source of truth

**Never do:**
- Don't sanitize PK values before joining them (sanitize after joining)
- Don't have separate PK encoding logic in different components
- Don't pass already-sanitized PK strings between components — pass the raw parts

---

### Pitfall CR-03: PK Conditions Removed from Post-Filter Causes Wrong Results

**What goes wrong:** Queries with contradictory or duplicate equality predicates on the same PK column produce wrong results. `WHERE id = 'u1' AND id != 'u1'` returns the row instead of zero results.

**Why it happens:** `findPKEqConditions` stores one equality condition per PK column. `BuildPlan` then removes every condition whose column appears in that map from the post-filter list. The removed conditions are "assumed satisfied" by the PK lookup, but contradictory predicates on the same column are not satisfied.

**Root cause:** The planner assumes one equality condition per PK column is sufficient and all other predicates on that column are redundant. This is incorrect — contradictory predicates on the same column change the result.

**Warning signs:**
- Queries with negations on PK columns return unexpected rows
- Queries with `WHERE pk = 'a' AND pk = 'b'` return a row instead of zero
- Unit tests for planner show conditions removed from post-filter

**Prevention:**
- Keep ALL original WHERE conditions as post-filters (this fix)
- The PK lookup narrows the search space; post-filters verify each condition
- Add short-circuit optimization for contradictory equalities but never skip post-filtering

---

### Pitfall CR-07: Start Returns Success But Services Are Down

**What goes wrong:** `Engine.Start()` returns `nil` even when the HTTP listener fails to bind, materializer consumer setup fails, or NATS handler registration fails. The engine reports "started" but queries fail.

**Why it happens:** Three patterns combine:
1. HTTP server: `httpServer.Serve()` is called in a goroutine. Bind failures happen inside the goroutine and are only logged.
2. Materializer: `materialize.Run()` is called in a goroutine. Consumer setup errors return from `Run()` but the goroutine only logs them.
3. NATS handler: registration errors are logged but `Start` continues.

**Warning signs:**
- Engine reports `started=true` but queries return "connection refused"
- Logs show "failed to register NATS query handler" or "HTTP server error" after "engine started"
- Port conflicts during startup give no clear error to the caller

**Prevention:**
- Bind HTTP listener with `net.Listen` BEFORE entering goroutine (this fix)
- Capture materializer consumer setup errors via startup channel and make them fatal
- Decide: NATS handler failure = degraded mode (log prominently, continue)

---

### Pitfall CR-10: All KV Write Errors Go to DLQ, Causing Data Loss

**What goes wrong:** A transient NATS cluster outage causes every event during that window to be sent to DLQ and acknowledged. Valid events are permanently dropped from the materialized view.

**Why it happens:** `processEvent` has a single error path for `writer.Apply`:
```
writeErr → publishToDLQ → msg.Ack()
```

No distinction between "this event can never be processed" (terminal) and "try again later" (transient). A temporary KV timeout is treated the same as an invalid schema version.

**Warning signs:**
- DLQ stream grows during NATS cluster maintenance windows
- Materialized view diverges from source stream after transient outages
- Events appear in DLQ with errors like "context deadline exceeded" or "connection refused"

**Prevention:**
- Classify errors at the caller level (this fix):
  - Context cancellation → NAK
  - Network timeout, connection refused → transient → NAK with backoff
  - Key too long, invalid value → terminal → DLQ + Ack
- Use exponential backoff for NAK'd messages (JetStream handles this)

---

### Pitfall CR-11: InactiveThreshold Deletes Durable Consumers on Extended Downtime

**What goes wrong:** If the engine is down for more than one hour, NATS deletes the durable consumer. On restart, the consumer is recreated with `DeliverAllPolicy`, replaying the entire stream from the beginning.

**Why it happens:** `consumer.go:53` sets `InactiveThreshold: 1 * time.Hour`. This is appropriate for ephemeral consumers but contradicts the purpose of a durable consumer — surviving extended downtime.

**Warning signs:**
- Restart after >1h downtime triggers a full stream replay
- DLQ receives duplicate entries during replay (events that were previously DLQ'd are reprocessed)
- Startup takes much longer than expected after weekends or holidays

**Prevention:**
- Remove `InactiveThreshold` from durable consumer config (this fix)
- Durable consumers should survive indefinitely until explicitly deleted

---

### Pitfall CR-14: Stream Creation Mutates External Streams

**What goes wrong:** In external NATS mode, the CLI creates or updates source streams with subjects `${source_stream}.>` regardless of the configured `source_subject`. This can:
1. Add unexpected subjects to existing streams
2. Create streams with subjects that don't match the user's source subject

**Why it happens:** The CLI unconditionally creates/updates every source stream at startup. The subject is hardcoded as `v.SourceStream + ".>"` without considering `source_subject`.

**Warning signs:**
- Existing NATS streams show unexpected subjects after natsql starts
- JetStream stream configuration changes silently

**Prevention:**
- Only auto-create streams in embedded mode (engine owns them)
- In external mode, warn if stream doesn't exist; let consumer setup fail with clear error
- Add `--create-streams` flag for explicit opt-in (this fix)

---

## Architectural Anti-Patterns

### Anti-Pattern: Same SQL Key Construction in Multiple Places

**What we had:** PK key construction logic was independently implemented in:
- `mapper.go:214-234` (stringifyValue → SanitizePK)
- `writer.go:48` (PkKey → SanitizePK)
- `planner.go:30-38` (fmt.Sprint + join)
- `executor.go:22` (PkKey → SanitizePK)
- `kv.go:57-59` (PkKey → SanitizePK)
- `kv.go:127-154` (EncodePKValue)

That's **six places** with overlapping PK encoding logic, three of which handle sanitization differently.

**Prevention:** One function. `BuildPkKey()` in `kv.go`. Used everywhere.

### Anti-Pattern: Errors in Goroutines Become Log Messages

**What we had:** Three places where errors in goroutines are logged instead of propagated:
1. Materializer consumer setup failure (in goroutine wrapping `materialize.Run`)
2. HTTP server bind/listen failure (in HTTP `Serve` goroutine)
3. NATS handler registration failure (logged, not error)

**Prevention:** For HTTP: bind before goroutine. For materializer: propagate through channel or make setup synchronous. For NATS: decision to log vs fail should be explicit.

### Anti-Pattern: Config Accepted But Not Wired

**What we had:** 
- `cfg.HTTP.Port` is set by CLI but never read by engine constructors
- `ConsumerConfig.BatchSize` is stored but only affects MaxAckPending
- `IndexConfig` is validated but never used

**Prevention:** Either wire config values to behavior or reject them at validation time. Config fields should either DO something or be rejected.

---

## "Looks Done But Isn't" Checklist

Items that appear complete but have hidden issues:

- [ ] **PK encoding**: Appears to work for common cases (strings, numbers). BUT double-sanitization breaks PK values with `_`, `|`, `/`, `*`, `>` — test these explicitly.
- [ ] **SELELCT *:** Returns the full stored row. BUT leaks `_meta` internal fields — SELECT * should return only schema columns.
- [ ] **Engine.Start():** Returns nil, engine reports "started". BUT HTTP may not be listening, materializer may not be consuming — verify each component independently.
- [ ] **Error handling in processEvent:** All errors appear handled. BUT transient vs terminal classification is missing — all errors go to DLQ.
- [ ] **HTTP body size detection:** Appears to check body size. BUT uses fragile string comparison — use `*http.MaxBytesError` and `errors.As` instead.
- [ ] **Test helpers:** Appear adequate. BUT same helpers duplicated across 5+ test files — extract into shared utility.
- [ ] **PK lookup queries:** Appear to handle WHERE correctly. BUT PK conditions are removed from post-filter — contradictory predicates produce wrong results.

---

## Recovery Strategies

| Pitfall | Recovery Cost | Recovery Steps |
|---------|---------------|----------------|
| CR-01: State corruption from out-of-order writes | HIGH (re-materialize) | Delete durable consumer, purge view KV keys, replay source stream from beginning |
| CR-02: Unreachable rows due to double-sanitization | MEDIUM (re-materialize affected rows) | Re-materialize view with corrected PK encoder |
| CR-03: Wrong query results from contradictory predicates | LOW (fix code, re-query) | Fix planner logic, re-execute query — no data corruption, just bad results |
| CR-07: Engine "started" but not serving | MEDIUM (restart with fix) | Fix startup sequence, restart engine |
| CR-10: Data loss from transient errors going to DLQ | HIGH (re-materialize lost events) | Replay source stream for affected events; may need to merge DLQ events back |

---

## Pitfall-to-Phase Mapping

| Pitfall | Fix Wave | Verification |
|---------|----------|--------------|
| CR-01: Concurrent workers | Wave 2 | Integration test: 100 rapid updates to same PK, assert final value |
| CR-02: Double PK sanitization | Wave 1 | Black-box tests for PK with `_`, `|`, `/`, `*`, `>` |
| CR-03: PK conditions removed | Wave 3 | Unit tests for planner: contradictory predicates |
| CR-04: SELECT * leaks _meta | Wave 3 | Integration test: SELECT * omits _meta |
| CR-05: Unsupported SQL silent | Wave 1 | Negative tests for each unsupported construct |
| CR-06: HTTP port ignored | Wave 4 | CLI test: verify HTTP binds to configured port |
| CR-07: Startup errors swallowed | Wave 4 | Integration test: port conflict, assert Start() error |
| CR-08: Config validation incomplete | Wave 1 | Unit tests for each cross-validation case |
| CR-09: Number precision loss | Wave 3 | Test values >2^53 in both write and query |
| CR-10: All errors go to DLQ | Wave 2 | Unit test: transient vs terminal classification |
| CR-11: InactiveThreshold deletes consumer | Wave 2 | Integration test: consumer survives extended pause |
| CR-12: BatchSize misnamed | Wave 2 | Unit tests for config mapping |
| CR-13: Cross-view scan cost | Wave 3 | Benchmark with multiple large views |
| CR-14: Stream creation mutates | Wave 5 | CLI test: embedded vs external stream behavior |
| CR-18: HTTP fragile error detection | Wave 5 | Test oversized body, trailing data |
| CR-19: NATS errors ignored | Wave 5 | Test with mocked NATS connection |
| CR-22: Dead code | Wave 6 | `go vet`, `go build` with -tags |

---

*Pitfalls research for: natsql v2.0.0 Code Review Remediation*
*Researched: 2026-05-31*
