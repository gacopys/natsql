# Feature Landscape: Code Review Corrections for natsql v1.2

**Project:** natsql — NATS-native materialized view engine
**Researched:** 2026-05-31
**Mode:** Feature corrections from code review findings

## Overview

This document maps the 25 code review findings to the feature behaviors they correct. Rather than introducing new user-facing features, these corrections fix bugs and behavioral issues in existing features. The "feature" here is **correctness** — each fix makes an existing feature work as documented.

---

## Critical Correctness Features

### CORR-01: Ordered Stream Materialization (CR-01)

**What the feature should do:** Events from a JetStream source stream are materialized to KV in stream order. Two events for the same primary key are applied in the order they were published.

**What was wrong:** The 16-goroutine worker pool could process events out of order. Event A (PK=x, set name=Alice) published before Event B (PK=x, set name=Bob) could be applied after Event B, leaving the KV state as Alice instead of Bob.

**Fix:** Remove the worker pool. Process messages sequentially in the bridge goroutine.

**Verification:** Integration test: publish 100 rapid updates to the same PK, each setting a monotonically increasing counter. Assert final counter value in KV equals 100.

### CORR-02: Consistent PK Encoding (CR-02)

**What the feature should do:** A row materialized with PK value `a_b` should be queryable via `WHERE pk = 'a_b'`. The KV key used for storage and lookup must be identical.

**What was wrong:** Write path sanitized PK twice (mapper + writer). Read path sanitized PK once (executor only). Rows with `_`, `|`, `/`, `*`, `>` in PK values were stored under a different key than queries looked up.

**Fix:** Single `BuildPkKey()` function called once on both paths.

**Verification:** Black-box tests for PK values containing `_`, `|`, `/`, `*`, `>`, and custom separators. Materialize row → query by PK → assert found.

### CORR-03: Correct Predicate Semantics (CR-03)

**What the feature should do:** SQL WHERE conditions must be evaluated according to standard SQL semantics. Contradictory predicates (`id = 'u1' AND id != 'u1'`) must return zero rows.

**What was wrong:** PK equality conditions were removed from post-filtering. `WHERE id = 'u1' AND id != 'u1'` used PK lookup for `u1` but never applied the `id != 'u1'` filter, returning the row.

**Fix:** Keep ALL original conditions as post-filters. Add contradiction detection for short-circuit optimization.

**Verification:** Unit tests for planner: contradictory PK equalities return empty plan. Integration tests for executor: contradictory predicates return zero rows.

---

## High-Severity Corrections

### CORR-04: SELECT * Excludes Metadata (CR-04)

**What the feature should do:** `SELECT *` returns only the user-defined schema columns. Internal `_meta` fields are not exposed.

**What was wrong:** `projectRow` returned the full stored map for `SELECT *`, including `_meta` with `stream_seq` and `updated_at`.

**Fix:** Pass schema column names into plans. `SELECT *` projects only those columns.

**Verification:** Black-box test asserting `_meta` is not present in `SELECT *` results.

### CORR-05: Unsupported SQL Rejection (CR-05)

**What the feature should do:** Queries with unsupported clauses (ORDER BY, GROUP BY, DISTINCT, HAVING, aggregations, subqueries) should return a clear error message.

**What was wrong:** Non-column SELECT expressions were silently ignored. ORDER BY, GROUP BY, etc. were parsed but never validated.

**Fix:** Explicitly check and reject unsupported clauses in `Parse()`.

**Verification:** Negative test for each unsupported construct: `SELECT COUNT(*)`, `SELECT DISTINCT`, `ORDER BY`, `GROUP BY`, `HAVING`.

### CORR-06: HTTP Port Respects Config (CR-06)

**What the feature should do:** The standalone server's HTTP port should match the configured value in `http.port` or `--port`.

**What was wrong:** CLI logged `cfg.HTTP.Port` but engine hardcoded port 8080.

**Fix:** Initialize `queryPort` from `cfg.HTTP.Port` in engine constructors.

**Verification:** CLI-level test starting server with non-default port and verifying HTTP server binds there.

### CORR-07: Startup Error Propagation (CR-07)

**What the feature should do:** `Engine.Start()` must return an error if any core service (HTTP listener, materializer consumer setup, NATS handler) fails to start.

**What was wrong:** Materializer errors were only logged inside goroutines. HTTP listen errors happened after `Serve` in goroutine. `Start()` returned nil even with failures.

**Fix:** Bind HTTP listener synchronously. Propagate materializer consumer setup errors. Log NATS handler failure prominently but don't block.

**Verification:** Integration test: start engine with port conflict → expect `Start()` error.

### CORR-08: Config Cross-Validation (CR-08)

**What the feature should do:** Config validation must verify that `key_fields` references only `primary_key` columns, and all `primary_key` columns appear in `key_fields`.

**What was wrong:** Validation checked `key_fields` non-empty and `primary_key` exists, but never cross-referenced them. Invalid configs could start and send every event to DLQ.

**Fix:** Add cross-validation logic in `Config.Validate()`.

**Verification:** Unit tests for each invalid config: key_field not a column, key_field references non-PK column, PK column not in key_fields, duplicate column names.

### CORR-09: Precise Number Handling (CR-09)

**What the feature should do:** Large integer values (>2^53) must preserve exact precision during query execution.

**What was wrong:** Executor used `json.Unmarshal` which converts all numbers to `float64`, losing precision above 2^53. Mapper used `json.Decoder.UseNumber()` and stored `json.Number`.

**Fix:** Use `json.Decoder.UseNumber()` in executor too. Update `valuesEqual` to compare `json.Number` exactly.

**Verification:** Tests for values like `9007199254740993` (2^53+1) in both write and query paths.

### CORR-10: Transient Error Handling (CR-10)

**What the feature should do:** Transient KV write failures (network timeout, NATS cluster unavailable) should NAK the message for redelivery, not send it to DLQ. Only terminal errors should go to DLQ.

**What was wrong:** All writer errors were treated equally — DLQ + Ack. A temporary NATS outage permanently dropped valid events.

**Fix:** Classify errors. Transient → NAK with backoff. Terminal → DLQ + Ack.

**Verification:** Unit test for `isTransientError()` classification. Integration test: simulate KV failure, assert NAK not DLQ.

### CORR-11: Durable Consumer Persistence (CR-11)

**What the feature should do:** Durable consumers persist until explicitly deleted, surviving extended engine downtime.

**What was wrong:** `InactiveThreshold: 1h` caused NATS to delete durable consumers after one hour of inactivity. Restart after >1h replayed entire stream.

**Fix:** Remove `InactiveThreshold` from consumer config.

**Verification:** Integration test: create consumer, stop engine for >1h (or simulate), restart, verify consumer resumes from last ack.

### CORR-12: Accurate Config Naming (CR-12)

**What the feature should do:** Config field names accurately reflect their runtime behavior.

**What was wrong:** `batch_size` only influenced `MaxAckPending` and did not control fetch batch size.

**Fix:** Rename to `max_ack_pending`.

**Verification:** Unit tests assert config mapping produces correct `MaxAckPending` value.

### CORR-13: Scoped Full Scans (CR-13)

**What the feature should do:** A full scan on one view should not pay the cost of scanning keys from other views.

**What was wrong:** `WatchAll` scanned the entire shared bucket, filtering by prefix client-side. Cross-view cost was unbounded.

**Fix:** Add view prefix constant and document cross-view cost. Implement server-side filtering where NATS API supports it.

**Verification:** Benchmark with multiple large views. Document performance characteristics.

### CORR-14: Safe Stream Creation (CR-14)

**What the feature should do:** Stream creation should respect `source_subject` and not mutate external streams.

**What was wrong:** CLI created streams with `${source_stream}.>` regardless of `source_subject`. External streams were silently mutated.

**Fix:** Only auto-create streams in embedded mode. Respect `source_subject`. Add `--create-streams` flag for opt-in.

**Verification:** CLI test: verify stream creation uses configured subject. Verify external streams are not modified.

---

## Medium/Low Corrections

| Fix | Feature Behavior | Verification |
|-----|-----------------|--------------|
| CR-15: `$.field` prefix support | Field paths with or without `$.` prefix work consistently | Test both `$.user.id` and `user.id` |
| CR-16: Index config rejection | Config with indexes returns clear error at load time | Test: config with indexes → validation error |
| CR-17: Delete semantics | Documented as deferred; no behavioral change | Documentation review |
| CR-18: HTTP JSON handling | Trailing data rejected; `MaxBytesError` use `errors.As` | Test large body, test trailing comma |
| CR-19: NATS handler errors | Flush and Respond errors checked and logged | Test with mocked NATS connection |
| CR-20: Error message typo | "unmarshaling row" instead of "marshaling row" | Code review |
| CR-21: Example errors | All example errors checked; connection ownership explicit | Manual review |
| CR-22: Dead code removal | Unused symbols removed from codebase | `go vet`, build check |
| CR-23: gofmt formatting | All files pass gofmt | CI check |
| CR-24: Test helper dedup | Shared test utilities extracted | Code review |
| CR-25: Docs sync | README and docs match implemented behavior | Documentation review |

---

## Feature Dependency Map

```
CR-02 (Canonical PK) ──required_by──> CR-01 (Ordered processing)
                                     └──> CR-03 (Predicate handling)
                                     └──> CR-09 (Number precision in query)
                                     
CR-08 (Config validation) ──independent──> All other fixes (CI gate)

CR-01 (Ordered processing) ──independent──> CR-10 (Error classification)
                                           └──> CR-11 (Consumer durability)
                                           └──> CR-12 (Config naming)

CR-03 (Predicates) ──uses──> CR-09 (Number comparison in valuesEqual)
                   └──> CR-04 (projectRow signature)

CR-07 (Startup errors) ──uses──> CR-06 (HTTP port plumbing)
```

---

## MVP Definition for v1.2

### Must Fix (Critical — Data Correctness)

1. **CR-01**: Ordered stream processing (data corruption without this)
2. **CR-02**: Consistent PK encoding (data unreachable without this)
3. **CR-03**: Correct predicate semantics (wrong query results without this)

### Must Fix (High — User-Visible Behavior)

4. **CR-04**: SELECT * filters metadata (internal fields leaked)
5. **CR-05**: Unsupported SQL rejection (silent wrong results)
6. **CR-07**: Startup error propagation (engine says "started" when it isn't)
7. **CR-09**: Number precision (data corruption for large integers)
8. **CR-10**: Transient error handling (data loss on temporary outage)
9. **CR-14**: Safe stream creation (external stream mutation)

### Should Fix (High/Medium — Operational Correctness)

10. **CR-06**: HTTP port from config (config ignored)
11. **CR-08**: Config cross-validation (invalid configs not caught)
12. **CR-11**: Durable consumer persistence (replay after 1h downtime)
13. **CR-12**: Accurate config naming
14. **CR-13**: Scoped full scans (cross-view cost)
15. **CR-18**: HTTP body handling (fragile error detection)
16. **CR-19**: NATS handler errors (ignored errors)

### Polish (Low — Code Quality)

17. CR-15: `$.field` prefix support
18. CR-16: Index config rejection
19. CR-20: Error message typo
20. CR-21: Example error checking
21. CR-22: Dead code removal
22. CR-23: gofmt formatting
23. CR-24: Test helper deduplication
24. CR-25: Documentation sync

### Deferred

- **CR-17**: Delete/tombstone semantics (needs design, not a bug fix)

---

*Feature research for: natsql v1.2 Code Review Corrections*
*Researched: 2026-05-31*
