# natsql Architecture

Authoritative architecture map for the natsql materialized view engine. Read this before touching any package. It defines components, data flow, invariants, key encodings, lifecycle, and the edge cases every change must preserve.

## 1. Big Picture

```
┌─────────────┐     ┌──────────────┐     ┌────────────┐     ┌───────────┐
│ JetStream   │────▶│ Materializer │────▶│ KV Bucket  │────▶│ SQL       │
│ Stream      │     │ (consumer →  │     │ (JetStream │     │ Engine    │
│ (changelog) │     │  mapper →    │     │  KV store) │     │ (vitess   │
│             │     │  writer)     │     │ (snapshot) │     │  parser)  │
└─────────────┘     └──────────────┘     └────────────┘     └─────┬─────┘
                          │                                      │
                          ▼                                      ▼
                   ┌──────────────┐                  ┌──────────────────┐
                   │  DLQ Stream  │                  │  Transport Layer │
                   │  (bad events)│                  │  NATS / HTTP /   │
                   └──────────────┘                  │  In-Process Go  │
                                                     └──────────────────┘
```

Three components, no more. New work extends existing components; do not add a new top-level package without strong justification and a GSD discussion.

- **Materializer** (`internal/materialize/`): consumes a durable JetStream subscription, maps JSON events to KV mutations, writes to the KV bucket, routes malformed/terminal failures to the DLQ stream.
- **Query Engine** (`internal/query/`): parses `SELECT`, validates against the stored schema, builds a plan (PK lookup / full scan / empty), executes against the KV bucket, projects columns.
- **Transport** (`internal/transport/`): exposes `Engine.Query` over NATS request-reply (`natsql.query`) and HTTP (`POST /api/v1/query`). Both use the identical `QueryResult` JSON envelope.

`Engine` (`internal/engine/`) wires the three together, owns lifecycle, and is re-exported by the root `natsql` facade.

## 2. Data Flow

### 2.1 Write Path (event → KV)

```
JetStream stream
  → SetupConsumer (durable pull, AckExplicit, DeliverAll, no InactiveThreshold)
  → cons.Messages()  (single goroutine — NO worker pool)
  → processEvent:
      mapper.MapRow(msg)
        - json.Decode + UseNumber (preserves exact precision)
        - extractNestedField (dot-path, $. prefix supported, max depth 8)
        - validateType per ColumnType (string/number/boolean/timestamp)
        - stringifyValue → raw PK parts (NOT sanitized here)
      writer.Apply(mut)
        - row = rowData + _meta{stream_seq, updated_at(RFC3339Nano)}
        - kv.BuildPkKey(view, pkParts, sep)  ← SINGLE canonical encoder
        - kv.Put(key, rowJSON)
      on success: msg.Ack()
      on map error (ErrMalformedEvent): publishToDLQ + Ack
      on write error - classify:
        - transient (deadline/cancel/context-closed/conn-refused/no-leader/timeout):
            msg.Nak() → redelivery
        - terminal: publishToDLQ + Ack
  → heartbeat every 60s (events_processed counter)
  → drain on Close(): cons.Drain() before ctx cancel (D-58 — prevents redelivery)
```

**Invariant: events for one PK are applied in stream order.** This is why there is no worker pool. If throughput needs to scale, partition the source stream by subject and run one sequential consumer per partition — not parallel consumers on the same subject.

### 2.2 Read Path (SQL → results)

```
Engine.Query(ctx, sql)
  → lazy-init KV bucket under mutex (works before Start())
  → query.Parse(sql)
      - vitess sqlparser.NewTestParser().Parse
      - assert *sqlparser.Select
      - reject unsupported: DISTINCT, ORDER BY, GROUP BY, HAVING
      - extract table (single table only), columns (or nil for *), WHERE (AND only), LIMIT
  → kv.LoadSchema(viewName)   (always fresh from KV — D-27)
  → query.Validate(q, schema) (SELECT & WHERE columns must exist)
  → query.BuildPlan(q, schema)
      - if all KeyFields have OpEq → PKLookupPlan
      - detect contradictory PK equalities → EmptyPlan (zero I/O)
      - else → FullScanPlan
      - ALL Where conditions carried as post-filters (PK + non-PK)
  → plan.Execute(ctx, kvb)
      - PKLookupPlan: kv.Get(BuildPkKey(...)) → json.Decode+UseNumber → post-filter → project
      - FullScanPlan: WatchAll → prefix-filter "{view}/pk/" → 16-worker sem → filter → project → LIMIT
      - EmptyPlan: return [] (no KV I/O)
  → normalize nil results → []map[string]any{} (D-33)
  → QueryResult{Results, Error:nil}
```

### 2.3 Query Transport

- **HTTP** (`internal/transport/http.go`): chi router, `POST /api/v1/query`. `MaxBytesReader` caps body at 1 MiB. Double-decode to reject trailing data. Errors return `{"results":[],"error":"…"}` JSON with proper status codes (413, 400). 30s middleware timeout.
- **NATS** (`internal/transport/nats.go`): subscribe `natsql.query`, request body is the raw SQL string, reply is the `QueryResult` JSON. 30s per-request context. `nc.Flush()` after subscribe; subscription returned for cleanup.
- **In-process Go**: `eng.Query(ctx, sql)` — same code path, used by tests and library embedders.

## 3. Storage Layout

All state lives in a **single** JetStream KV bucket, `natsql-views` (`kv.DefaultBucket`), plus one DLQ stream `natsql-dlq`.

### 3.1 KV Key Schema

```
{viewName}/pk/{sanitizedPk}     → row JSON (schema columns + _meta)
{viewName}/meta/schema          → ViewSchema JSON
```

- `viewName` is the configured view name (validated to be a NATS-KV-safe identifier).
- Row values are `{"<col>": <typed>, …, "_meta": {"stream_seq":N, "updated_at":"…"}}`.
- `SELECT *` excludes keys prefixed with `_` (so `_meta` is internal). Explicit column projection yields those columns only; missing columns → `nil` (D-31).

### 3.2 Canonical PK Encoding (CR-02 — critical invariant)

`kv.BuildPkKey(viewName, pkParts []string, separator string)` is the **single** place that constructs a row key. Both the writer and the query executor MUST call it. Sanitization happens exactly once, inside it.

```
BuildPkKey:
  for each part: SanitizePK(part)     // _ → __, | → _p, / → _s, * → _a, > → _g
  join sanitized parts with separator // separator is NOT sanitized (validated to KV-safe chars)
  return viewName + "/pk/" + join
```

- `RowMutation.PkParts` carries **raw** (unsanitized) PK component values. The mapper does NOT call `SanitizePK`. The writer does, via `BuildPkKey`.
- The planner produces raw PK parts in `KeyFields` order using `fmt.Sprint(value)`, matching the mapper's `stringifyValue` representation. The executor calls `BuildPkKey` with those raw parts + the schema's separator.
- **Re-introducing double sanitization, or sanitizing in the mapper, breaks read/write consistency** — rows with `_`, `|`, `/`, `*`, `>` in PKs become unreachable from query. Tests in `kv_test.go` and `natsql_blackbox_test.go` guard this.
- Default separator is `/` (a KV-safe char). `SanitizePK` escapes `/` in PK values to `_s`, so composite keys cannot collide with separator slashes. Custom separators are validated against `^[-/_=.a-zA-Z0-9]+$`.

### 3.3 Schema in KV

- `ViewSchema{Name, Columns, KeyFields, KeySeparator, Version=1}` stored at `{view}/meta/schema` on `Start()` (best-effort, logged on failure).
- Loaded fresh per query (`D-27`) — schema is never cached in the engine. If you cache it, you must handle config-reload invalidation.
- `LoadSchema` returns `(nil, nil)` when the key is absent (view not materialized yet); `Query` maps that to `"view %q not found"`.

## 4. Query Engine Internals

### 4.1 SQL Dialect (see also `SQL_DIALECT.md`)

| Feature | Status |
|---|---|
| `SELECT *`, column projection, comma lists | shipped |
| `WHERE col = 'v'`, `IN (...)`, `!=` | shipped |
| `AND` of conditions | shipped |
| `LIMIT n` | shipped |
| `OR`, `>` `<` `>=` `<=`, `ORDER BY`, `GROUP BY`, `DISTINCT`, `HAVING`, aggregates, subqueries, JOINs, DML | **rejected at parse time** with explicit error |
| RANGE scans backed by secondary indexes, deletes/tombstones | deferred to v2 |

`WHERE` is **required** — a bare `SELECT * FROM v` is a parse error. This is intentional v1 scoping.

### 4.2 Plan Selection

- `PKLookupPlan`: requires an `OpEq` condition for *every* `KeyField` (composite keys need the full set). One `kv.Get`, optionally post-filtered by all original `Where` conditions.
- `FullScanPlan`: otherwise. `WatchAll` over the bucket, prefix-filter by `{view}/pk/`, type-aware `filterRow` on all `Where` conditions, 16-worker parallel decode/filter, LIMIT applied under mutex.
- `EmptyPlan`: contradictory PK equalities on the same column (e.g. `WHERE id='a' AND id='b'`) short-circuit to zero results with no KV I/O.

### 4.3 Type-Aware Comparison (`valuesEqual`)

Rows are decoded with `json.Decoder.UseNumber()`, so numbers surface as `json.Number`. `valuesEqual` normalizes `json.Number → int64` (no `.`) or `float64` (`.` present), then compares across numeric types (`int64`⇄`float64`). `bool`, `string`, `nil` are direct. Fallback is `fmt.Sprint` string equality for exotic types. **Do not regress to plain `json.Unmarshal`** — that coerces all numbers to `float64` and loses precision above 2^53 (CR-09). Tests in `query_test.go` cover `9007199254740993`.

### 4.4 Projection (`projectRow`)

- `columns == nil` (SELECT *): copy row, **drop keys prefixed `_`** (filters `_meta`). Returns schema columns only effectively.
- Explicit columns: build new map; present → copied, absent → `nil` (D-31). A `*` inside an explicit list returns the full raw row (including `_meta`) — a known escape hatch.
- Order of columns in the output map is not guaranteed (Go maps); results are JSON objects keyed by column name.

## 5. Materializer Internals

### 5.1 Consumer Setup

- Durable name: `natsql-{viewName}` (D-10). Resumable across restarts.
- `AckExplicit`, `DeliverAll` (replay from beginning on first create; resume on restart).
- **No `InactiveThreshold`** (CR-11) — durable consumers live until explicitly deleted. Setting one causes full-stream replay after long downtime.
- Defaults (config zeros → applied): `MaxDeliver=10`, `AckWait=30s`, `MaxAckPending=50`. `BatchSize` is deprecated → migrated to `MaxAckPending` in `SetDefaults`.
- `source_subject` (optional) sets `FilterSubject` for subject-scoped consumption.

### 5.2 Error Classification (CR-10)

`classifyWriteError` decides what to do when `writer.Apply` fails:

| Class | Signals | Action |
|---|---|---|
| Transient | `context.Canceled`/`DeadlineExceeded`, `connection refused`/`closed`/`no leader`/`timeout` | `msg.Nak()` — let JetStream redeliver |
| Terminal | everything else (bad data path, key validation, marshal) | `publishToDLQ` + `msg.Ack()` |
| Context canceled | `ctx.Err() != nil` | `msg.Nak()` (redeliver on restart) |

- `ErrMalformedEvent` (mapper): always DLQ + Ack (bad event data never improves on retry).
- `publishToDLQ` failure: NAK the original (don't ack — allow retry of both DLQ and original).
- The DLQ envelope is `{original_message_b64, view_name, error, timestamp(RFC3339)}`.

### 5.3 Lifecycle & Shutdown (D-57 ordering)

```
1. httpServer.Shutdown(ctx 5s)     — stop serving queries
2. natsSub.Unsubscribe()           — stop accepting new NATS queries
3. close(drainChans...)             — each materializer: cons.Drain() (acks in-flight, no redelivery)
4. cancel()                          — stop remaining loops
5. wg.Wait()                        — wait for all materializers + HTTP goroutine
```

The root facade `Engine.Close()` then additionally shuts down the embedded NATS node (if `NewEmbedded`) and closes the owned NATS connection (if `NewWithNATS`). `Close()` is idempotent and returns `ErrNotStarted` if called before `Start`.

## 6. Engine Lifecycle

- `New` (caller owns JetStream + NATS conn), `NewWithNATS` (engine owns `nc.Close`), `NewEmbedded` (engine owns embedded node + conn). The facade re-exports the three and wires ownership into `Engine.Close`.
- `Start()` is synchronous and idempotent (`ErrAlreadyStarted`): `kv.InitBucket` → `EnsureDLQStream` → store all schemas (`Warn` on failure) → launch materializers (with a 500ms startup-error window → fail-fast) → register NATS handler (failure is fatal) → bind HTTP listener (`net.Listen` before `Serve`, so bind errors propagate — CR-07).
- `Query()` is thread-safe and works even before `Start()` (lazy-inits the KV bucket under the mutex).
- `Stats()` is safe at any lifecycle phase: returns `Started`, `Goroutines` (`runtime.NumGoroutine`), `Views` (from config), `HTTPServing`.

## 7. Configuration Surface

See `internal/cfg/config.go` and `CONVENTIONS.md` §11. Key cross-validations enforced:

- At least one column with `primary_key: true`.
- Every `key_fields` entry references a column that has `primary_key: true`.
- Every `primary_key: true` column appears in `key_fields`.
- Column names unique within a view.
- `key_separator` matches the NATS KV key charset.
- `Indexes` block rejected (v1) with actionable error.
- Duplicate view names rejected.

## 8. Edge Cases & Hidden Invariants

These are the things that look innocent but break the engine. Preserve them when refactoring.

1. **Double-sanitization** — sanitize PK exactly once in `BuildPkKey`. Re-adding `SanitizePK` in the mapper or planner makes PKs containing `_ | / * >` unreachable from reads. Tests: `kv_test.go::TestBuildPkKey_*`, `natsql_blackbox_test.go::TestBlackBox_…composite…`.
2. **Loop-capture race** — Go 1.22+ loop var capture is relied upon; `copyloopvar` linter guards against gratuitous copies. Don't add `vc := e.cfg.Views[i]` shadows unless needed for goroutine capture (we do capture explicitly in `startViewMaterializers`).
3. **`json.Number` precision** — `UseNumber` must be used in both mapper and executor. Reverting to `json.Unmarshal` silently corrupts integers > 2^53 (CR-09).
4. **`SELECT *` metadata leak** — `projectRow` strips `_`-prefixed keys for `*`. If you add new internal fields, prefix them `_` or they leak to users.
5. **Contradictory PK predicates** — `WHERE id='a' AND id='b'` MUST return `[]` (EmptyPlan), not look up `'a'` and return it. Planner checks `pkSeen` map for conflicting OpEq values on the same PK column.
6. **All operators kept as post-filters** — `PKLookupPlan.Where` carries ALL original conditions (including the PK equality). Re-removing them reintroduces `WHERE id='u1' AND id!='u1'` returning the row (CR-03).
7. **Sequential materialization** — no shared worker pool on the write path. Re-introducing concurrency re-breaks per-PK ordering.
8. **Drain before cancel** — closing drain channels before `cancel()` lets `cons.Drain()` ack in-flight messages; without it, restart redelivers everything since last ack.
9. **`InactiveThreshold` absence** — setting it makes the durable consumer get deleted after ~1h idle → full-stream replay on restart. Never add it back.
10. **Stream creation scope** — CLI only auto-creates streams in embedded mode, and respects `source_subject` when it does. In external mode, requires `--create-streams` opt-in to avoid mutating user streams (CR-14).
11. **HTTP body framing** — must reject trailing bytes after the JSON body and cap size at 1 MiB via `MaxBytesReader`. Use `errors.As(*http.MaxBytesError)` not string matching (CR-18).
12. **NATS handler `Flush`** — must check `nc.Flush()` error after subscribe, else "subscribed but not actually registered" races (CR-19).
13. **Schema freshness** — schemas are loaded from KV on every query (D-27). If you introduce in-memory caching, you must add invalidation on config rebuild.
14. **Result normalization** — `nil` results become `[]map[string]any{}` so the JSON envelope is always `{"results":[...], "error":null}` (D-33). Don't return `nil` slices to the transport.
15. **Field path depth** — `extractNestedField` caps at 8 levels (`maxNestingDepth`) for safety (T-02-02). A pathological event can't recurse forever.
16. **`$.` prefix** — dot paths may start with `$.`; stripped in `extractNestedField` for backward-compatible config (`$.user.id` ≡ `user.id`).
17. **Composite key separator safety** — default `/`; PK values with `/` are escaped to `_s` so they can't cross separator. Custom separators must be KV-safe; a separator matching a value's escaped sequence is theoretically possible for pathological inputs — keep separators simple.
18. **`Query` thread-safety before `Start`** — relies on `kv.InitBucket` lazy-init guarded by `e.mu` (FIX-ENG-02). Don't move this out of the lock.

## 9. Performance Characteristics

- PK lookup: O(1) `kv.Get`.
- Full scan: O(all keys in the bucket), prefix-filtered client-side, parallelized 16-way for decode/filter. Cross-view cost is real — scoped scans with per-view buckets deferred to v2 (CR-13).
- Materialization: bounded by KV write latency, single goroutine per view. `MaxAckPending` controls in-flight backpressure (default 50).
- Rough ceiling: ~100K keys comfortably; 1M+ keys — `WatchAll` full scans become slow. Document, don't pretend otherwise.

## 10. Security Posture

- HTTP binds `127.0.0.1` by default (T-02-06). `WithHTTPServer("0.0.0.0:…")` is the documented escape hatch; do not silently change the default bind.
- Embedded NATS: single-node, `NoLog`, `NoSigs`, random port unless configured.
- SQL is read-only; there is no injection surface through SQL because writes don't happen via SQL. Still, the parser rejects everything except the narrow supported grammar.
- No secrets in logs. `slog` structured fields should never include credentials (and there are none in this codebase today).
- `gosec` linter is on (only G115 — integer overflow conversion — is excluded, judged acceptable for bounded KV key sizes).

## 11. Extension Points (when adding features)

- **New column type**: add to `cfg.ColumnType` consts + `Valid()` (exhaustive switch), extend `mapper.validateType`, extend `valuesEqual`/`normalizeValue` in the executor, add type-integrity test. `exhaustive` linter will flag half-done switches.
- **New WHERE operator** (`>`,`<`): add `Op` const, map in `comparisonToCondition`, extend `filterRow` switch (exhaustive), add planner logic (index vs scan), update `SQL_DIALECT.md`.
- **Secondary indexes**: `cfg.Indexes` is currently rejected in `Validate`. To implement: remove the rejection, add index maintenance in `writer.Apply` (separate prefix like `idx.<view>.<col>.<val>.>/<pk>`), add an index-scan `Plan`, update `planner.BuildPlan`.
- **Delete/tombstones**: not designed (v2). `kv` package doc flags this. Any design must atomically remove the row key and its index entries.
- **Per-view KV buckets**: breaking change — needs config migration and a `kv.BucketFor(view)` indirection. Scoped scans (CR-13) are the payoff.
- **OR conditions**: parser currently rejects `OrExpr`. To enable: extend `extractConditions`, add a query plan that fans out to multiple PK/index lookups and unions.

When extending, prefer adding to the existing component over creating a new package, and write black-box tests at the `natsql_blackbox_test.go` level in addition to unit tests.