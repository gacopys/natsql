# Verification Findings: natsql v1.2 Code Review

**Verification date:** 2026-06-01
**Scope:** All 25 findings from cr.md, verified against source code before Phase 8 changes
**Status:** 25 confirmed / 0 dismissed of 25

All 25 findings from the comprehensive code review remain present in the current source code. No fixes from the original cr.md suggestions have been applied yet. The verification establishes the baseline for Phase 8-11 fix efforts.

***

## Critical Findings

### CR-01: Materializer can corrupt state by processing ordered stream messages concurrently
- **Severity:** Critical
- **Status:** CONFIRMED
- **Evidence:** cr.md lines 16-22
- **Source check:** `internal/materialize/materializer.go` at lines 20, 188-198
- **Code snippet:**
  - Line 20: `const materializerWorkers = 16`
  - Lines 188-198: Worker pool loop spawning 16 goroutines processing from `msgCh` channel
- **Reasoning:** The 16-goroutine worker pool persists in `materializer.go`. Messages flow from a single durable consumer through a channel (`msgCh`) to 16 parallel workers, each calling `processEvent`. `processEvent` calls `writer.Apply` which does a raw `kv.Put` (writer.go:49), making no attempt to preserve JetStream stream order. Two updates for the same PK can arrive out of order, and since there is no CAS check against stream sequence, an older event can overwrite a newer event. This violates materialized-view correctness — the project constraint specifies CAS-based (read-committed) consistency, not last-writer-wins by goroutine scheduling.
- **Fix phase:** Phase 9 (Materializer Correctness)

### CR-02: Primary-key sanitization is inconsistent and breaks lookups for special-character keys
- **Severity:** Critical
- **Status:** CONFIRMED
- **Evidence:** cr.md lines 26-30
- **Source check:** `internal/materialize/mapper.go` lines 214-234, `internal/materialize/writer.go` line 48, `internal/kv/kv.go` lines 57-58, `internal/query/planner.go` lines 30-38, `internal/query/executor.go` line 22
- **Code snippet:**
  - mapper.go:98: `pkParts[i] = stringifyValue(val)` — calls `SanitizePK` at line 234
  - writer.go:48: `pkKey := kv.PkKey(w.viewName, mut.PK)` — calls `SanitizePK` again at kv.go:57-58
  - planner.go:32: `pkValues[i] = fmt.Sprint(pkConditions[kf].Value)` — no sanitization
  - executor.go:22: `key := kv.PkKey(p.ViewName, p.PkValue)` — single sanitization via `PkKey`
- **Reasoning:** The `stringifyValue` function at mapper.go:214 calls `kv.SanitizePK(s)` (line 234), sanitizing each PK component. Then `writer.Apply` calls `kv.PkKey(w.viewName, mut.PK)` at writer.go:48, which calls `SanitizePK` again (kv.go:57-58). So the write path double-sanitizes. On the read path, `planner.go:32` joins raw values with `fmt.Sprint` (no sanitization), and `executor.go:22` calls `PkKey` which sanitizes once. A row with PK value `a_b` is stored under `view/pk/a__b` (double-escaped: `_` → `__` → `____`... no wait, let me re-check. `SanitizePK("a_b")` → `a__b`. Then `PkKey("view", "a__b")` → `view/pk/a____b`. On query, `fmt.Sprint("a_b")` → `a_b`. Then `PkKey("view", "a_b")` → `view/pk/a__b`. So the query looks for `a__b` but the row is stored at `a____b` — a mismatch. This confirms the inconsistent encoding bug.
- **Fix phase:** Phase 8 (Foundation — canonical PK encoder)

### CR-03: PK fast path drops contradictory predicates on the same PK column
- **Severity:** Critical
- **Status:** CONFIRMED
- **Evidence:** cr.md lines 34-38
- **Source check:** `internal/query/planner.go` lines 40-46
- **Code snippet:**
  - planner.go:42-44: `for _, c := range q.Where { if _, isPK := pkConditions[c.Column]; !isPK { nonPKConditions = append(nonPKConditions, c) } }`
- **Reasoning:** In `BuildPlan` (planner.go:40-46), after `findPKEqConditions` finds PK equality conditions, ALL conditions whose column is in the PK set are removed from the post-filter. This means `WHERE id = 'u1' AND id != 'u1'` returns `u1` because the `id != 'u1'` condition is dropped entirely. Similarly, `WHERE id = 'u1' AND id = 'u2'` returns whichever PK lookup finds first. All same-column predicates are removed, not just the specific equality condition used for the lookup key. The fundamental issue is that removal is by column name, not by the specific condition expression.
- **Fix phase:** Phase 10 (Query Engine Correctness)

## High Findings

### CR-04: SELECT * exposes internal _meta fields
- **Severity:** High
- **Status:** CONFIRMED
- **Evidence:** cr.md lines 44-48
- **Source check:** `internal/materialize/writer.go` lines 38-40, `internal/query/executor.go` lines 145-148
- **Code snippet:**
  - writer.go:38-40: Stores `_meta` in row data: `row["_meta"] = map[string]any{...}`
  - executor.go:145-148: `projectRow` with nil columns returns row as-is
- **Reasoning:** `projectRow` at executor.go:146-147 returns the full stored map for nil columns (SELECT *). The `_meta` dictionary is stored alongside user data when rows are written (writer.go:38-40). Since `_meta` is not part of the declared schema and cannot be explicitly selected through column validation, SELECT * is supposed to return only schema columns. The current implementation leaks implementation metadata (`_meta.stream_seq`, `_meta.updated_at`) into user query results.
- **Fix phase:** Phase 10 (Query Engine Correctness)

### CR-05: Unsupported SQL clauses and expressions are silently accepted
- **Severity:** High
- **Status:** CONFIRMED
- **Evidence:** cr.md lines 52-56
- **Source check:** `internal/query/parser.go` lines 47-50, 62-63, 76-102
- **Code snippet:**
  - parser.go:47-49: Extract SELECT expressions: `q.Select = extractSelectExprs(sel.SelectExprs)` — no error check
  - parser.go:62-63: Extract LIMIT: silently proceeds if present
  - parser.go:90-94: Non-column expressions simply `continue` — silently ignored
- **Reasoning:** `extractSelectExprs` at parser.go:76-102 silently ignores unsupported select expressions. Non-column expressions like `COUNT(*)`, `1`, `CONCAT(...)` hit the `continue` at line 94 and are dropped from the result. The Parse function never checks for `DISTINCT`, `ORDER BY`, `GROUP BY`, `HAVING`, or aggregations. A query like `SELECT COUNT(*) FROM users WHERE id = 'x'` would be parsed as `SELECT *` with a WHERE clause, silently returning incorrect results rather than rejecting the unsupported construct.
- **Fix phase:** Phase 8 (Foundation — parser hardening)

### CR-06: CLI http.port and --port are logged but not applied
- **Severity:** High
- **Status:** CONFIRMED
- **Evidence:** cr.md lines 60-63
- **Source check:** `internal/engine/engine.go` lines 138, 194, `cmd/natsql/main.go` lines 97-98, 161
- **Code snippet:**
  - engine.go:138: `queryPort: 8080` — hardcoded default in `New()`
  - engine.go:194: `queryPort: 8080` — hardcoded default in `NewEmbedded()`
  - main.go:97-98: `cfg.HTTP.Port = httpPort` — CLI sets config value but never passes it to engine
  - main.go:161: `"http_port", cfg.HTTP.Port` — logged but never wired to engine constructor
- **Reasoning:** The CLI applies `cfg.HTTP.Port` from the `--port` flag at main.go:97-98 and logs it at main.go:161, but neither `natsql.New` nor `natsql.NewEmbedded` receive `natsql.WithQueryPort(cfg.HTTP.Port)`. Both engine constructors default `queryPort` to 8080. The HTTP server always starts on port 8080 regardless of config or CLI flags. The configured port value is effectively dead code between config loading and engine construction.
- **Fix phase:** Phase 11 (Engine Lifecycle)

### CR-07: Start can report success even when core services failed to start
- **Severity:** High
- **Status:** CONFIRMED
- **Evidence:** cr.md lines 68-72
- **Source check:** `internal/engine/engine.go` lines 253-256, 267-275, 288-300
- **Code snippet:**
  - Lines 253-261: Materializer launched in goroutine — errors logged but not propagated to `Start`
  - Lines 267-275: NATS handler registration error logged but not fatal — `Start` continues
  - Lines 288-295: HTTP `ListenAndServe` in goroutine — port conflicts logged, `Start` returns nil
  - Line 300: `e.started = true` set unconditionally after all goroutines launched
- **Reasoning:** `Start()` launches the materializer, NATS handler, and HTTP server as goroutines without any mechanism to propagate startup errors back to the caller. Materializer consumer setup failures are logged but `Start` returns nil. NATS handler registration failure at line 269-270 is logged but `Start` continues. HTTP `ListenAndServe` happens in a goroutine (line 289-295), so port bind failures are only logged. The engine reports "started" (line 300-301) while core services may be completely non-functional.
- **Fix phase:** Phase 11 (Engine Lifecycle)

### CR-08: Config validation does not prove key_fields and primary-key columns are consistent
- **Severity:** High
- **Status:** CONFIRMED
- **Evidence:** cr.md lines 76-80
- **Source check:** `internal/cfg/config.go` lines 156-187
- **Code snippet:**
  - Lines 156-158: Check `len(v.KeyFields) == 0` — only checks at least one exists
  - Lines 164-183: Check at least one column has `primary_key=true`
  - Missing: cross-validation between `key_fields` and `primary_key` columns
- **Reasoning:** The `Validate` method (config.go:136-194) checks that at least one `key_field` exists (line 156) and at least one column has `primary_key=true` (line 185), but it does not verify: (1) each `key_field` names an existing column, (2) each `key_field` column is marked `primary_key=true`, (3) every `primary_key=true` column is listed in `key_fields`, or (4) values are unique across view names, column names, and key fields. An invalid config can pass validation, start the engine, and send every event to DLQ because PK field extraction fails at runtime.
- **Fix phase:** Phase 8 (Foundation — config validation)

### CR-09: Large numeric values lose precision during query execution
- **Severity:** High
- **Status:** CONFIRMED
- **Evidence:** cr.md lines 84-88
- **Source check:** `internal/query/executor.go` lines 32, 97
- **Code snippet:**
  - Line 32: `json.Unmarshal(entry.Value(), &row)` — plain unmarshal, no UseNumber
  - Line 97: `json.Unmarshal(data, &fullRow)` — same issue in full-scan path
- **Reasoning:** The mapper at mapper.go:68-69 uses `json.Decoder.UseNumber()` to preserve JSON number precision during event parsing. However, the query executor at executor.go:32 and executor.go:97 uses plain `json.Unmarshal`, which decodes JSON numbers as `float64`. A stored value like `9007199254740993` (which exceeds 2^53 integer precision) is stored initially as `json.Number`, then serialized to JSON by `json.Marshal`, and on query becomes a `float64` via `json.Unmarshal`. The conversion to float64 loses precision, so equality comparisons and IN filters can return wrong results for large integers.
- **Fix phase:** Phase 10 (Query Engine Correctness)

### CR-10: Transient KV write failures are acknowledged and sent to DLQ, causing data loss
- **Severity:** High
- **Status:** CONFIRMED
- **Evidence:** cr.md lines 92-96
- **Source check:** `internal/materialize/materializer.go` lines 250-265
- **Code snippet:**
  - Lines 250-265: All write errors follow the same path: `publishToDLQ` + `msg.Ack()`
- **Reasoning:** In `processEvent` at materializer.go:250-265, ALL `writer.Apply` errors are treated identically: publish a copy to DLQ, then ack the original stream message. There is no error classification. Transient failures (NATS connection hiccup, KV temporary outage, context deadline) are treated the same as deterministic bad events. A temporary KV outage permanently drops a valid event from the materialized view. The fix should distinguish between malformed input (DLQ + Ack), transient errors (Nak with backoff or leave unacked for redelivery), and terminal write errors (DLQ + Ack).
- **Fix phase:** Phase 9 (Materializer Correctness)

### CR-11: Durable consumers can be deleted after inactivity, undermining crash recovery
- **Severity:** High
- **Status:** CONFIRMED
- **Evidence:** cr.md lines 100-104
- **Source check:** `internal/materialize/consumer.go` lines 46-53
- **Code snippet:**
  - Line 53: `InactiveThreshold: 1 * time.Hour` — consumer can be deleted after 1 hour of inactivity
- **Reasoning:** `SetupConsumer` at consumer.go:46-53 sets `InactiveThreshold` to 1 hour for the durable consumer. If the engine is down longer than one hour, the NATS server can delete the durable consumer. On the next startup, `CreateOrUpdateConsumer` with `DeliverAllPolicy` replays the entire stream from the beginning. While upserts make this mostly idempotent, it duplicates DLQ entries, adds startup load, and violates the stated durable-consumer recovery behavior. The expected behavior is that the durable consumer survives any reasonable downtime.
- **Fix phase:** Phase 9 (Materializer Correctness)

### CR-12: ConsumerConfig.BatchSize does not control fetch batch size
- **Severity:** High
- **Status:** CONFIRMED
- **Evidence:** cr.md lines 108-112
- **Source check:** `internal/materialize/consumer.go` lines 41, 52
- **Code snippet:**
  - Line 41: `batchSize := cfg.BatchSize`
  - Line 52: `MaxAckPending: batchSize * 2`
- **Reasoning:** The `BatchSize` field in `ConsumerConfig` is named to suggest it controls how many messages are fetched per batch. However, at consumer.go:52, `batchSize` only influences `MaxAckPending` (set to `batchSize * 2`). The materializer uses `cons.Messages()` and `msgCtx.Next()` for single-message-at-a-time processing (materializer.go:120,136). The configured batch size does not control application-level batching or pull batch size. The setting name is misleading — it should either be renamed to reflect its actual behavior or the materializer should use `Fetch`/`FetchNoWait` for genuine batched processing.
- **Fix phase:** Phase 9 (Materializer Correctness)

### CR-13: Full-scan queries scan the entire shared KV bucket for every view
- **Severity:** High
- **Status:** CONFIRMED
- **Evidence:** cr.md lines 116-120
- **Source check:** `internal/query/executor.go` lines 49-51
- **Code snippet:**
  - Line 49: `prefix := p.ViewName + "/pk/"` — prefix defined
  - Line 51: `watcher, err := kvb.WatchAll(ctx)` — but uses WatchAll, not a prefix filter
- **Reasoning:** The `FullScanPlan.Execute` method at executor.go:51 uses `kvb.WatchAll(ctx)` which delivers every key-value pair in the entire shared bucket. While line 69 checks `strings.HasPrefix(entry.Key(), prefix)`, this filtering happens client-side after receiving all updates. A full scan on one view incurs the cost of scanning all schemas and rows for ALL views in the single shared `natsql-views` bucket. For multiple views with large datasets, this is increasingly expensive. The NATS KV API does not support server-side prefix filtering via `Watch`...
- **Fix phase:** Phase 10 (Query Engine Correctness)

### CR-14: CLI mutates or creates source streams without respecting configured subjects
- **Severity:** High
- **Status:** CONFIRMED
- **Evidence:** cr.md lines 124-128
- **Source check:** `cmd/natsql/main.go` lines 135-137
- **Code snippet:**
  - Line 135-137: `Subjects: []string{v.SourceStream + ".>"}` — hardcoded subject pattern
- **Reasoning:** The standalone CLI at main.go:135-137 creates or updates every source stream with the subject pattern `{source_stream}.>`. It ignores the `source_subject` field from the view config entirely. In external NATS mode, this can unexpectedly alter an existing stream's subject set (via `CreateOrUpdateStream`), potentially exposing messages to consumers that shouldn't receive them, or creating streams whose subjects don't match the user's configured source subject. The `source_subject` is only used for consumer filtering at consumer.go:56-58, not for stream configuration.
- **Fix phase:** Phase 11 (Transport/CLI)

## Medium Findings

### CR-15: Config/tests imply JSONPath-style $.field, but mapper only supports dot paths
- **Severity:** Medium
- **Status:** CONFIRMED
- **Evidence:** cr.md lines 134-138
- **Source check:** `config_test.go` lines 18, 22, 60, 73, etc., `internal/materialize/mapper.go` lines 121-125
- **Code snippet:**
  - config_test.go:18: `from: $.user_id` — test config uses `$.` prefix
  - mapper.go:124-125: `parts := strings.Split(path, ".")` — naive split, no `$` stripping
- **Reasoning:** Multiple config test files use `from: $.user_id` and `from: $.name` (config_test.go:18,22,60,73, etc.), implying `$.` JSONPath prefix syntax is supported. However, `extractNestedField` at mapper.go:124-125 simply splits on `.` and does not handle a leading `$` token. With a `$.user_id` path, `strings.Split("$.user_id", ".")` produces `["$", "user_id"]`, and the mapper looks for a top-level `$` field in the event data. A user following the implied format from tests would have all their events sent to DLQ because the field lookup fails.
- **Fix phase:** Phase 11 (Cleanup)

### CR-16: Index config is accepted but ignored
- **Severity:** Medium
- **Status:** CONFIRMED
- **Evidence:** cr.md lines 142-146
- **Source check:** `internal/cfg/config.go` lines 79, 91-95
- **Code snippet:**
  - Line 79: `Indexes []IndexConfig` — field accepted in ViewConfig
  - Lines 91-95: `IndexConfig` struct with comment "placeholder for Phase 2"
- **Reasoning:** Users can define `indexes` in their view configuration, and the config validates successfully. However, neither the materializer nor the query engine uses this data. The `IndexConfig` struct at config.go:91-95 is explicitly documented as "a placeholder for Phase 2." Because non-PK filters always result in full-scan queries, the presence of index configuration creates a false expectation of indexed query performance. The user configures something that looks useful but has zero effect on query behavior.
- **Fix phase:** Phase 11 (Cleanup)

### CR-17: No delete/tombstone semantics for materialized rows
- **Severity:** Medium
- **Status:** CONFIRMED
- **Evidence:** cr.md lines 150-154
- **Source check:** `internal/materialize/mapper.go` line 63, `internal/materialize/writer.go` line 24
- **Code snippet:**
  - mapper.go:63: `MapRow` only produces RowMutations — no delete representation
  - writer.go:24-27: `Apply` only handles upsert via `kv.Put`
- **Reasoning:** The `MapRow` function at mapper.go:63-119 only produces upsert mutations. `Writer.Apply` at writer.go:24-55 only calls `kv.Put`. There is no mechanism to delete a row from the KV snapshot. For a "current state" materialized view, events that represent object deletion (e.g., `{"event": "delete", "id": "..."}`) have no way to remove the row. There is no configured operation field, subject convention, or tombstone predicate to trigger a deletion.
- **Fix phase:** Deferred (v2)

### CR-18: HTTP JSON handling accepts trailing data and uses fragile body-too-large detection
- **Severity:** Medium
- **Status:** CONFIRMED
- **Evidence:** cr.md lines 158-162
- **Source check:** `internal/transport/http.go` lines 26-41
- **Code snippet:**
  - Line 31: `if err.Error() == "http: request body too large"` — fragile string comparison
  - Lines 40-41: Drain and close body — no trailing data validation
- **Reasoning:** The HTTP handler at http.go:31 detects oversized request bodies by comparing `err.Error()` to a hardcoded string `"http: request body too large"`. This is fragile and depends on Go stdlib error message stability. After the first JSON decode, lines 40-41 drain the body with `io.Copy(io.Discard, r.Body)` and close it, but never check if the remaining data is valid (i.e., only whitespace). A request like `{"sql": "..."} {"another": "json"}` with concatenated JSON would silently pass without error.
- **Fix phase:** Phase 11 (Transport/CLI)

### CR-19: NATS request-reply ignores subscription flush and response errors
- **Severity:** Medium
- **Status:** CONFIRMED
- **Evidence:** cr.md lines 166-170
- **Source check:** `internal/transport/nats.go` lines 33-41
- **Code snippet:**
  - Line 33: `msg.Respond([]byte(errResp))` — return value ignored
  - Line 36: `msg.Respond(data)` — return value ignored
  - Line 41: `nc.Flush()` — return value ignored
- **Reasoning:** `RegisterNATSHandler` at nats.go:24-43 has two ignored error sources. First, `nc.Flush()` at line 41 returns an error if the flush fails, potentially indicating a broken subscription that will never receive messages. Second, `msg.Respond(data)` at lines 33 and 36 returns an error if the reply subject is no longer valid or the connection is closed. These errors can hide broken subscriptions or failed responses, making silent data loss invisible to operators.
- **Fix phase:** Phase 11 (Transport/CLI)

### CR-20: Error message in PK lookup says "marshaling" while unmarshaling
- **Severity:** Medium
- **Status:** CONFIRMED
- **Evidence:** cr.md lines 174-178
- **Source check:** `internal/query/executor.go` lines 32-33
- **Code snippet:**
  - Line 32-33: `json.Unmarshal(entry.Value(), &row); err != nil { ... "marshaling row: %w" }`
- **Reasoning:** In `PKLookupPlan.Execute`, the code calls `json.Unmarshal` to decode stored JSON row data but wraps the error with `"marshaling row"`. The correct term should be "unmarshaling row" (or "unmarshalling" per UK spelling used elsewhere). This misleading error message slows debugging when stored data is corrupt, because the developer sees "marshaling" and looks in the wrong direction.
- **Fix phase:** Phase 11 (Transport/CLI)

## Low Findings

### CR-21: Examples ignore important errors and contain lifecycle ownership hazards
- **Severity:** Low
- **Status:** CONFIRMED
- **Evidence:** cr.md lines 182-186
- **Source check:** `examples/02-composite-keys/main.go` lines 55-56, 73, `examples/05-library-embed/main.go` lines 59, 83, 89, 114, 127
- **Code snippet:**
  - `02-composite-keys/main.go:55-56`: `js, _ := jetstream.New(eng.NC())` — ignores JetStream creation error
  - `02-composite-keys/main.go:57-59`: `js.CreateOrUpdateStream(...)` return value ignored
  - `02-composite-keys/main.go:73`: `js.Publish(...)` return value ignored
  - `05-library-embed/main.go:59`: `defer nc.Close()` — also used by engine created later
  - `05-library-embed/main.go:83`: `natsql.New(js, cfg)` — error ignored
  - `05-library-embed/main.go:89`: `eng.Start(ctx)` — return value ignored
  - `05-library-embed/main.go:114`: `natsql.NewWithNATS(nc, ...)` — error ignored
  - `05-library-embed/main.go:127`: `eng2.Start(ctx)` — return value ignored
- **Reasoning:** Multiple example files ignore critical errors from JetStream creation, stream creation, engine creation, engine start, and publish operations. In `05-library-embed/main.go`, the same `nc` connection is passed to `natsql.New(js, cfg)` and then to the user's own `nc.Close()` (line 59) before `natsql.NewWithNATS(nc, ...)` (line 114), which may close the connection, leaving the first engine with a broken connection. This creates lifecycle ownership hazards that are invisible when errors are unchecked.
- **Fix phase:** Phase 11 (Cleanup)

### CR-22: Several symbols are dead, stale, or misleading
- **Severity:** Low
- **Status:** CONFIRMED
- **Evidence:** cr.md lines 192-196
- **Source check:** `internal/kv/kv.go` lines 15, 44, 127, `internal/materialize/mapper.go` lines 25-27, `internal/materialize/materializer.go` line 96, `internal/engine/engine.go` line 443
- **Code snippet:**
  - kv.go:15: `SchemaPrefix = "schemas:"` — defined but never used in production
  - kv.go:45: `MustInitBucket` — only used in tests (kv_test.go:287,301), not in production
  - kv.go:127: `EncodePKValue` — only used in tests (kv_test.go:38-101,265-277), not in production. Panics on `/` and `:`, while production uses `SanitizePK` instead
  - mapper.go:27: `ErrSkipAndAck` — defined but never referenced in production code
  - materializer.go:96: `dlqStream` parameter in `Run` — received but not used; DLQ publish uses `js` directly
  - engine.go:443: `Stats.LastError` — struct field exists but is never assigned; `Stats()` method (lines 455-471) sets all other fields but never populates `LastError`
- **Reasoning:** Six symbols are dead or misleading. `SchemaPrefix`, `MustInitBucket`, `EncodePKValue`, and `ErrSkipAndAck` are defined but never used in production flow. `EncodePKValue` also panics on certain characters while production uses the safer `SanitizePK`. The `dlqStream` parameter in `Run` is received but not consumed. `Stats.LastError` is a struct field that is never populated by the `Stats()` method, making it always empty in engine stats output.
- **Fix phase:** Phase 11 (Cleanup)

### CR-23: Formatting drift hurts readability
- **Severity:** Low
- **Status:** CONFIRMED
- **Evidence:** cr.md lines 200-202
- **Source check:** Verified by running `gofmt -l $(git ls-files '*.go')`
- **Files with drift:**
  - `cmd/natsql/main.go`
  - `examples/05-library-embed/main.go`
  - `internal/cfg/config.go`
  - `internal/engine/engine.go`
  - `internal/engine/engine_test.go`
  - `internal/kv/kv_test.go`
  - `internal/materialize/mapper.go`
  - `internal/materialize/mapper_test.go`
  - `internal/materialize/materializer.go`
  - `internal/query/query_test.go`
  - `internal/query/types.go`
  - `internal/transport/transport_test.go`
  - `natsql_blackbox_test.go`
- **Reasoning:** Running `gofmt -l` on all tracked Go files reports 13 files with formatting drift — the exact same set as the original cr.md report. The CI pipeline builds, vets, tests, and races but does not enforce `gofmt` formatting. The formatting issues are consistent with tabs-vs-spaces subtle incorrect indentation rather than wholesale style differences.
- **Fix phase:** Phase 11 (Cleanup)

### CR-24: Test helpers are duplicated heavily across packages
- **Severity:** Low
- **Status:** CONFIRMED
- **Evidence:** cr.md lines 206-209
- **Source check:** `internal/query/query_test.go` line 161, `internal/kv/kv_test.go` line 306, `internal/materialize/consumer_test.go` line 194, `internal/engine/engine_test.go` line 1300, `natsql_blackbox_test.go` line 681
- **Code snippet (representative):**
  - query_test.go:161-191: `func startEmbeddedNATS(t *testing.T) (*server.Server, *nats.Conn, jetstream.JetStream)`
  - kv_test.go:306-342: Identical helper with different server name
  - engine_test.go:1300-1330: Identical helper
  - consumer_test.go:194-224: Identical helper
  - natsql_blackbox_test.go:681-711: Identical helper
- **Reasoning:** The `startEmbeddedNATS` function is duplicated across 5 test files with virtually identical implementations (differing only in `ServerName` values). Similarly, `createStream` helpers are duplicated. This makes test changes tedious and risks inconsistent defaults across packages. A shared test utility would eliminate the duplication.
- **Fix phase:** Phase 11 (Cleanup)

### CR-25: Documentation and feature status are out of sync
- **Severity:** Low
- **Status:** CONFIRMED
- **Evidence:** cr.md lines 214-218
- **Source check:** `README.md` lines 82-85, `internal/query/parser.go` lines 62-63, `internal/query/executor.go` lines 73-78 (LIMIT handling in full scan), `natsql_blackbox_test.go` lines 400, 404 (LIMIT tests)
- **Code snippet:**
  - README.md:85: `| LIMIT support | Planned |` — documented as planned
  - parser.go:62-69: `extractLimit` implemented and called in `Parse`
  - executor.go:73-78, 87-92, 114-118, 132-134: Full-scan path applies LIMIT
- **Reasoning:** LIMIT is documented in README.md as "Planned" (line 85), but `extractLimit` at parser.go:221-237 fully parses LIMIT clauses, and the `FullScanPlan.Execute` method at executor.go contains complete LIMIT enforcement logic (early break at line 76-78, mid-scan stop at line 87-92, post-collection trim at line 132-134). The black-box test at natsql_blackbox_test.go:400-404 tests LIMIT behavior. It is unclear whether LIMIT is a fully supported API or an accidentally exposed internal implementation detail. Either way, the documentation is out of sync with the implementation.
- **Fix phase:** Phase 11 (Cleanup)

***

## Summary

| Severity | Total | Confirmed | Dismissed |
|----------|-------|-----------|-----------|
| Critical | 3 | 3 | 0 |
| High | 11 | 11 | 0 |
| Medium | 7 | 7 | 0 |
| Low | 4 | 4 | 0 |
| **Total** | **25** | **25** | **0** |

***

## Phase Mapping

| Phase | Fixes |
|-------|-------|
| 8 (Foundation) | CR-02 (canonical PK encoder), CR-05 (parser hardening), CR-08 (config validation) |
| 9 (Materializer Correctness) | CR-01 (ordered processing), CR-10 (error classification), CR-11 (consumer durability), CR-12 (BatchSize naming) |
| 10 (Query Engine Correctness) | CR-03 (predicate handling), CR-04 (meta field filtering), CR-09 (number precision), CR-13 (full-scan architecture) |
| 11 (Lifecycle, Transport, Cleanup) | CR-06 (HTTP port), CR-07 (startup errors), CR-14 (stream creation), CR-18 (HTTP robustness), CR-19 (NATS errors), CR-20 (error message), CR-15 (JSONPath prefix), CR-16 (index config), CR-21 (examples), CR-22 (dead code), CR-23 (gofmt), CR-24 (test helpers), CR-25 (docs sync) |
| N/A (Deferred to v2) | CR-17 (delete semantics) |
