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
- **Transport** (`internal/transport/`): exposes `Engine.Query` over NATS request-reply (`natsql.query` by default, queue group `natsql`) and HTTP (`POST /api/v1/query`, plus `/healthz` and `/readyz`). Both use the identical `QueryResult` JSON envelope.

`Engine` (`internal/engine/`) wires the three together, owns lifecycle, and is re-exported by the root `natsql` facade.

## 2. Data Flow

### 2.1 Write Path (event → KV)

```
JetStream stream
  → SetupConsumer (durable pull, AckExplicit, DeliverAll, server MaxDeliver -1, no InactiveThreshold)
  → cons.Messages()  (single goroutine — NO worker pool) → Options.OnReady (readiness, LC-03)
  → processEvent:
      mapper.MapRow(msg)
        - json.Decode + UseNumber (preserves exact precision)
        - extractNestedField (dot-path, $. prefix supported, max depth 8)
        - validateType per ColumnType (string/number/boolean/timestamp)
        - kv.FormatPKValue → raw, canonical PK parts (numbers as exact plain decimals; NOT escaped here)
      writer.Apply(mut)
        - row = rowData + _meta{stream_seq, stream, stream_created, updated_at(RFC3339Nano)}
        - kv.BuildPKKey(view, pkParts, sep)  ← SINGLE canonical encoder
        - kv.Get(key) → if stored row is from the same source stream incarnation
          with stream_seq >= this event's: skip (already applied / stale redelivery)
        - else kv.Create (absent) or kv.Update(key, rowJSON, revision) — CAS; re-read on conflict
      on success: msg.Ack()
      on map error (ErrMalformedEvent): publishToDLQ + Ack
      on write error - classify:
        - terminal (invalid key, unencodable row, row too large): publishToDLQ + Ack
        - transient (everything else): msg.NakWithDelay(250ms doubling, max 30s);
            once the retry budget is used (NumDelivered >= max_deliver, default 10,
            -1 = never): publishToDLQ + Ack
      DLQ publish failure: NakWithDelay — the server redelivers without limit, so the
        event is retried (and the DLQ with it) instead of being dropped (WR-07)
  → heartbeat every 60s (events_processed counter)
  → drain on Close(): msgCtx.Drain(), then wait (≤10s) for Run to return, then ctx cancel (D-58)
```

**Invariant: a row never moves backwards in stream order.** JetStream can redeliver an older event after a newer one (NAK, lost ack, AckWait expiry), so processing order alone doesn't guarantee this; the writer's sequence check plus CAS write does (WR-01). Sequences are only compared within one source stream incarnation (`_meta.stream` + `_meta.stream_created`), because they restart when a stream is recreated; rows from another incarnation, or written before `_meta` recorded it, are overwritten. Sequential processing per view is still required: there is no worker pool. If throughput needs to scale, partition the source stream by subject and run one sequential consumer per partition — not parallel consumers on the same subject.

### 2.2 Read Path (SQL → results)

```
Engine.Query(ctx, sql)
  → query.Parse(sql)
      - vitess sqlparser.NewTestParser().Parse
      - assert *sqlparser.Select
      - reject unsupported: DISTINCT, ORDER BY, GROUP BY, HAVING, OFFSET
      - extract table (single table only), columns (or nil for *), WHERE (AND only), LIMIT (HasLimit)
      - numeric literals: int64 when they fit, else json.Number (exact digits)
  → lazily open the KV bucket (no lock held during I/O; works before Start())
  → kv.LoadSchema(viewName)   (always fresh from KV — D-27)
  → query.Validate(q, schema) (SELECT & WHERE columns must exist)
  → query.BuildPlan(q, schema)
      - expand SELECT * into the schema's columns (QE-01)
      - LIMIT 0, contradictory PK equalities, PK = NULL → EmptyPlan (zero I/O)
      - every KeyField constrained by = or IN (≤256 candidate keys) → PKLookupPlan
      - else → FullScanPlan
      - ALL Where conditions carried as post-filters (PK + non-PK)
  → plan.Execute(ctx, kvb)
      - PKLookupPlan: per candidate key kv.Get(BuildPKKey(...)) → json.Decode+UseNumber → post-filter → project → LIMIT
      - FullScanPlan: Watch("{view}.pk.>", IgnoreDeletes) (server-side filter) → 16-worker sem →
          filter → project → LIMIT; succeeds only after the end-of-snapshot marker,
          errors on cancellation or the first undecodable row (QE-02)
      - EmptyPlan: return [] (no KV I/O)
  → normalize nil results → []map[string]any{} (D-33)
  → QueryResult{Results, Error:nil}
```

### 2.3 Query Transport

- **HTTP** (`internal/transport/http.go`): chi router, `POST /api/v1/query`. `MaxBytesReader` caps body at 1 MiB. Double-decode to reject trailing data. Errors return `{"results":[],"error":"…"}` JSON with proper status codes (413, 400). 30s middleware timeout. `GET /healthz` (liveness, always 200) and `GET /readyz` (200 when started, connected and every view is consuming; 503 otherwise) return the `Stats` document.
- **NATS** (`internal/transport/nats.go`): `QueueSubscribe(nats.query_subject, "natsql")` — engines on one subject split requests instead of each executing every one; use different subjects for independent deployments. Request body is the raw SQL string, reply is the `QueryResult` JSON. Each request runs on its own goroutine, at most 16 at once and 256 executing or waiting; beyond that the reply is an overload error. 30s timeout measured from arrival. A reply larger than the connection's max payload is replaced by a "result too large" error envelope (TR-02); requests without a reply subject are ignored. `nc.FlushTimeout` after subscribe (CR-19). `NATSHandler.Close` unsubscribes and waits for in-flight queries (5s grace, then cancels them).
- **In-process Go**: `eng.Query(ctx, sql)` — same code path, used by tests and library embedders.

## 3. Storage Layout

All state lives in a **single** JetStream KV bucket, `natsql-views` (`kv.DefaultBucket`), plus one DLQ stream `natsql-dlq`.

### 3.1 KV Key Schema

```
{viewName}.pk.{encodedPk}       → row JSON (schema columns + _meta)
{viewName}.meta.schema          → ViewSchema JSON
```

- This is key layout 2 (`kv.KeyLayoutVersion`, PF-01). Keys are `.`-delimited so a scan can ask the server for one view's rows with `Watch("{view}.pk.>")` instead of streaming the whole bucket. Layout 1 (`{view}/pk/…`, `{view}/meta/schema`) is no longer read: views stored in it are refused at `Start` until dropped (§3.3).
- `viewName` is the configured view name, validated as `^[A-Za-z_][A-Za-z0-9_]*$` (CF-01), so it is a single subject token, a valid durable-name suffix and a SQL identifier.
- Row values are `{"<col>": <typed>, …, "_meta": {"stream_seq":N, "stream":"…", "stream_created":"…", "updated_at":"…"}}`.
- `SELECT *` returns the schema's columns only (never `_meta`, never fields of an older schema). Explicit column projection yields those columns only; missing columns → `nil` (D-31).

### 3.2 Canonical PK Encoding (CR-02 — critical invariant)

`kv.BuildPKKey(viewName, pkParts []string, separator string)` is the **single** place that constructs a row key. Both the writer and the query executor MUST call it. Escaping happens exactly once, inside it.

```
BuildPKKey:
  for each part:
    empty part → _e
    byte by byte:
      _ → __                                          // the escape character
      composite key and byte occurs in separator → _xHH
      byte outside [A-Za-z0-9-=] → _xHH               // '.', '/', '@', ' ', ':', '+', UTF-8 …
  join encoded parts with separator   // separator is NOT escaped (validated)
  return viewName + ".pk." + join
```

Every output is a valid NATS KV key with no empty subject tokens, components never add tokens of their own, and the encoding is injective for a fixed number of components (`TestBuildPKKey_ValidAndInjective` checks this exhaustively over an adversarial alphabet, and `TestBuildPKKey_RoundTripThroughNATS` against a real server) (WR-03, WR-04).

The PK parts themselves come from **`kv.FormatPKValue`**, the single canonical formatter, called by both the mapper and the planner (WR-06): numbers become exact plain decimals with no exponent or redundant zeros (`1`, `1.0`, `1e0` → `1`; `1e3` → `1000`; `-0` → `0`), never through `float64`; strings are as-is; booleans `true`/`false`; timestamps RFC 3339 with nanoseconds. NULL and non-finite numbers are not keys: the mapper dead-letters such events and the planner turns such lookups into `EmptyPlan`.

- `RowMutation.PKParts` carries **raw** (unescaped, canonical) PK component values. The mapper does NOT escape them. The writer does, via `BuildPKKey`.
- The planner produces raw PK parts in `KeyFields` order with `kv.FormatPKValue`, so `WHERE id = 1.0` finds the row an event with `"id": 1` wrote. The executor calls `BuildPKKey` with those raw parts + the schema's separator.
- **Re-introducing double escaping, or escaping in the mapper, breaks read/write consistency** — rows with `_`, `.`, `/` or other escaped bytes in PKs become unreachable from query. Tests in `kv_test.go` and `natsql_blackbox_test.go` guard this.
- Default separator is `/`. Custom separators may use letters, digits and `- / . =`; `_` (the escape character) and `..` (an empty subject token) are rejected by `Validate`.

### 3.3 Schema in KV

- `ViewSchema{Name, Columns, KeyFields, KeySeparator, Version=1, SourceStream, Fingerprint}` stored at `{view}.meta.schema` by `Start()` — the only writer (WR-09). A failed write fails `Start`.
- `Fingerprint` hashes everything that decides a view's rows and keys: source stream and filter, column names/mappings/types/PK flags, key fields, separator and `KeyLayoutVersion` (not consumer tuning). On `Start`, a view whose stored schema is missing or has a different fingerprint fails with `ErrViewRebuildRequired` if it has materialized state: a durable consumer `natsql-{view}` (on the configured or previously stored source stream) or rows under `{view}.pk.>` (`kv.HasRows`, one wildcard last-message lookup). A durable consumer with no current-layout schema means an earlier version materialized the view. Resuming would mix rows of two definitions, and stale keys would never be deleted. A schema with no state behind it (e.g. a `Start` that failed on a missing source stream) is simply replaced.
- `Engine.DropView(ctx, view)` / `natsql drop-view --view V` rebuild such a view: delete the durable consumer, purge `{view}.>` and any layout-1 `{view}/…` keys from the bucket's stream (no delete markers left), then the next `Start` replays the source stream from the beginning. Events the stream no longer retains are lost; this is never done automatically.
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

- `PKLookupPlan`: requires an `=` or `IN` condition for *every* `KeyField` (composite keys need the full set). Each key field takes its first `=` value, or else its first `IN` list; the cross product (deduplicated, ≤256 keys, else a full scan) gives the candidate keys, read with one `kv.Get` each and post-filtered by all original `Where` conditions (PF-01).
- `FullScanPlan`: otherwise. `Watch("{view}.pk.>", IgnoreDeletes())` so the server sends only this view's rows and skips delete markers, type-aware `filterRow` on all `Where` conditions, 16-worker parallel decode/filter, LIMIT applied under mutex. The scan must see the watcher's end-of-snapshot marker to succeed; context cancellation or a watcher closing early is an error (`scan interrupted…`), never a partial success (QE-02). When the scan stops early (LIMIT), the watcher is stopped and its channel drained in the background, or nats.go's watch callback stays blocked on the full channel forever (QE-04).
- `EmptyPlan`: `LIMIT 0` (QE-03), contradictory PK equalities on the same column (e.g. `WHERE id='a' AND id='b'`, even when other key fields are unconstrained), and PK conditions no key can satisfy (`= NULL`) short-circuit to zero results with no KV I/O. `OFFSET` (including `LIMIT o, n`) is rejected at parse time.

### 4.3 Type-Aware Comparison (`valuesEqual`)

Rows are decoded with `json.Decoder.UseNumber()`, so numbers surface as `json.Number`. `valuesEqual` normalizes `json.Number → int64` (no `.`) or `float64` (`.` present), then compares across numeric types (`int64`⇄`float64`). `bool`, `string`, `nil` are direct. Fallback is `fmt.Sprint` string equality for exotic types. **Do not regress to plain `json.Unmarshal`** — that coerces all numbers to `float64` and loses precision above 2^53 (CR-09). Tests in `query_test.go` cover `9007199254740993`.

### 4.4 Projection (`projectRow`)

- The planner expands `*` (alone or inside a list) into the schema's columns, so every plan projects an explicit list: present → copied, absent → `nil` (D-31). `_meta` and fields outside the schema never appear (QE-01).
- Plans built by hand with `Columns == nil` (tests) fall back to the row's fields not prefixed `_`.
- Order of columns in the output map is not guaranteed (Go maps); results are JSON objects keyed by column name.

## 5. Materializer Internals

### 5.1 Consumer Setup

- Durable name: `natsql-{viewName}` (D-10). Resumable across restarts.
- `AckExplicit`, `DeliverAll` (replay from beginning on first create; resume on restart).
- **No `InactiveThreshold`** (CR-11) — durable consumers live until explicitly deleted. Setting one causes full-stream replay after long downtime.
- The server-side `MaxDeliver` is always `-1` (WR-07). The view's `consumer.max_deliver` is the materializer's own retry budget (default 10, `-1` = never dead-letter transient failures): once used, the event goes to the DLQ. If the server stopped at `MaxDeliver` instead, an event whose last attempt also failed to reach the DLQ, or whose `AckWait` kept expiring, would be lost from both the view and the DLQ. Existing durables are updated to `-1` on start.
- Other defaults (config zeros → applied): `AckWait=30s`, `MaxAckPending=50`. `BatchSize` is deprecated → migrated to `MaxAckPending` in `SetDefaults`.
- `source_subject` (optional) sets `FilterSubject` for subject-scoped consumption.

### 5.2 Error Classification (CR-10)

`classifyWriteError` decides what to do when `writer.Apply` fails:

| Class | Signals | Action |
|---|---|---|
| Terminal | `jetstream.ErrInvalidKey`, `nats.ErrMaxPayload`, JetStream API error 10054 (message too large), `errEncodeRow`, `errNilMutation` | `publishToDLQ` + `msg.Ack()` |
| Transient | everything else, including unrecognized errors | `msg.NakWithDelay` (250ms doubling, capped at 30s); once the retry budget is used, `publishToDLQ` + `msg.Ack()` |
| Context canceled | `ctx.Err() != nil` | `msg.Nak()` (redeliver on restart) |

- Errors are matched with `errors.Is`/`errors.As` and JetStream error codes, never by message text (the text includes the PK) (WR-02).
- Unknown errors default to transient: misclassifying a recoverable error as terminal acks a valid event and drops its update, while the reverse only delays it until its retry budget dead-letters it.
- `ErrMalformedEvent` (mapper): always DLQ + Ack (bad event data never improves on retry).
- `publishToDLQ` failure: NAK the original with backoff (don't ack — allow retry of both DLQ and original). The server redelivers without limit, so this holds on the last attempt too.
- The DLQ envelope is `{original_message_b64, payload_omitted, original_size, original_stream, original_subject, original_sequence, num_delivered, view_name, error (≤4 KiB), timestamp (RFC3339Nano)}`. Base64 grows an event by a third, so an event that fit the max payload may not fit wrapped: when the server or the DLQ stream rejects the envelope as too large, it is re-sent with `payload_omitted: true` and no payload — fetch the event from `original_stream` by `original_sequence` (WR-08).
- `Run` records counters (`events_processed`, `dead_lettered`, `retried`) and the last error in `materialize.Stats`, which the engine reports per view (OP-03).
- Consumer `Next` errors are retried with backoff (100ms doubling to 5s). When the iterator closes without a drain (the connection closed for good, the consumer was deleted) or processing panics, `Run` returns an error instead of looping or returning nil (OP-02, LC-03).

### 5.3 Lifecycle & Shutdown (D-57 ordering)

```
1. httpServer.Shutdown(ctx 5s)     — stop serving queries
2. NATSHandler.Close()             — unsubscribe, wait for in-flight NATS queries (5s, then cancel)
3. close(drain chans) + wait ≤10s — each materializer: msgCtx.Drain(), finishes and acks what it already received
4. cancel()                          — stop anything that didn't drain in time
5. wait                              — all supervisors + the HTTP goroutine
```

`Close` holds only the lifecycle lock while it waits; queries and `Stats` never take that lock, so a query arriving during shutdown is answered or refused instead of deadlocking with `http.Server.Shutdown` (LC-04). The root facade `Engine.Close()` then additionally shuts down the embedded NATS node (if `NewEmbedded`) and closes the owned NATS connection (if `NewWithNATS`), exactly once even under concurrent calls. `Close()` returns `ErrNotStarted` if the engine isn't running.

## 6. Engine Lifecycle

- `New` (caller owns JetStream + NATS conn), `NewWithNATS` (engine owns `nc.Close`), `NewEmbedded` (engine owns embedded node + conn). The facade re-exports the three and wires ownership into `Engine.Close`.
- `Start()` is synchronous (`ErrAlreadyStarted` when running): `kv.InitBucket` (open, create only if missing) → `EnsureDLQStream` (same) → publish all schemas, refusing changed or earlier-layout views (`ErrViewRebuildRequired`, WR-09) → start a supervisor per view and **wait until every materializer reports ready** (`Options.OnReady` after consumer setup and `Messages()`), or one fails, or the `Start` ctx ends (LC-03) → register NATS handler (failure is fatal) → bind HTTP listener (`net.Listen` before `Serve`, so bind errors propagate — CR-07).
- Every resource is recorded in a `running` struct as it is acquired; if any step fails, `Start` releases all of them in D-57 order before returning, so a failed `Start` leaves no materializer, subscription or listener behind and can be retried (LC-01).
- The `Start` ctx bounds startup only. Materializers run under `context.WithoutCancel(ctx)` and stop only on `Close` (LC-02).
- Each view runs under a supervisor: a run that fails after startup is restarted with backoff (1s doubling to 1m, reset after a minute of healthy running), and its state goes `running` → `restarting` → `running`. A closed NATS connection can't recover, so it leaves the view `failed`. A recovered panic is a failure, not a clean stop.
- Locks: `lifecycle` serializes `Start`/`Close`/`DropView` and is held across their I/O; `mu` guards the running state and KV handle and is never held across I/O. `Query()` and `Stats()` only take `mu` briefly (LC-04).
- `Query()` is thread-safe and works even before `Start()` (lazily *opens* the KV bucket via `kv.OpenBucket` without holding a lock; it never creates or reconfigures it, and parses SQL before any KV I/O).
- Existing buckets and DLQ streams are never reconfigured: history, TTL, limits and replicas belong to whoever provisioned them (WR-05).
- `Stats()` is safe at any lifecycle phase: `Started`, `Ready` (started, NATS connected, every view `running`), `NATSConnected`, `Goroutines` (process-wide), `Views`, `HTTPServing`, `HTTPAddr` (the bound address), query counters, and per-view `ViewStatus` (state, restarts, events processed/dead-lettered/retried, last error). `/readyz` serves it over HTTP (OP-03).
- `NewEmbedded` without `nats.store_dir` stores JetStream data in a private temp directory deleted on shutdown, so engines never share state through nats-server's default `$TMPDIR/nats/jetstream` (OP-01). The embedded server's warnings and errors go to the engine's `slog` logger (notices at debug).

## 7. Configuration Surface

See `internal/cfg/config.go` and `CONVENTIONS.md` §11. Key cross-validations enforced:

- At least one column with `primary_key: true`.
- Every `key_fields` entry references a column that has `primary_key: true`.
- Every `primary_key: true` column appears in `key_fields`.
- Column names unique within a view.
- `key_separator` uses only letters, digits and `- / . =`, and doesn't contain `..`.
- `Indexes` block rejected (v1) with actionable error.
- View names match `^[A-Za-z_][A-Za-z0-9_]*$` (CF-01); duplicates rejected.
- `consumer.max_deliver` is `-1`, `0` (default 10) or positive.
- `nats.query_subject` (default `natsql.query`) is a literal subject: no wildcards, spaces or empty tokens.

## 8. Edge Cases & Hidden Invariants

These are the things that look innocent but break the engine. Preserve them when refactoring.

1. **Double-escaping** — escape PKs exactly once in `BuildPKKey`, and format PK values only with `kv.FormatPKValue` on both paths. Escaping in the mapper or planner, or formatting numbers any other way (e.g. `fmt.Sprint`), makes PKs unreachable from reads. Tests: `kv_test.go::TestBuildPKKey_*`, `TestFormatPKValue_*`, `query/executor_test.go::TestQuery_NumericPKSpellings`.
2. **Loop-capture race** — Go 1.22+ loop var capture is relied upon; `copyloopvar` linter guards against gratuitous copies. Don't add `vc := e.cfg.Views[i]` shadows unless needed for goroutine capture (each `viewRun` copies its view config in `startViews`).
3. **`json.Number` precision** — `UseNumber` must be used in both mapper and executor. Reverting to `json.Unmarshal` silently corrupts integers > 2^53 (CR-09).
4. **`SELECT *` metadata leak** — the planner expands `*` into schema columns, so every plan projects explicitly. Never skip `projectRow` on any plan (the full scan used to, leaking `_meta`).
5. **Contradictory PK predicates** — `WHERE id='a' AND id='b'` MUST return `[]` (EmptyPlan), not look up `'a'` and return it. `hasContradictoryPK` checks for conflicting `=` values on the same PK column.
6. **All operators kept as post-filters** — `PKLookupPlan.Where` carries ALL original conditions (including the PK equality). Re-removing them reintroduces `WHERE id='u1' AND id!='u1'` returning the row (CR-03).
7. **Sequential materialization** — no shared worker pool on the write path. Re-introducing concurrency re-breaks per-PK ordering.
8. **Drain before cancel** — close the drain channels, *wait* for the materializers (bounded), then `cancel()`. Cancelling right after closing the channels races the drain and aborts in-flight events, so restart redelivers them.
9. **`InactiveThreshold` absence** — setting it makes the durable consumer get deleted after ~1h idle → full-stream replay on restart. Never add it back.
10. **Stream creation scope** — CLI only auto-creates streams in embedded mode, and respects `source_subject` when it does. In external mode, requires `--create-streams` opt-in to avoid mutating user streams (CR-14).
11. **HTTP body framing** — must reject trailing bytes after the JSON body and cap size at 1 MiB via `MaxBytesReader`. Use `errors.As(*http.MaxBytesError)` not string matching (CR-18).
12. **NATS handler `Flush`** — must check the `nc.FlushTimeout()` error after subscribe, else "subscribed but not actually registered" races (CR-19). The handler must never run a query on the subscription's delivery goroutine (TR-01).
13. **Schema freshness** — schemas are loaded from KV on every query (D-27). If you introduce in-memory caching, you must add invalidation on config rebuild.
14. **Result normalization** — `nil` results become `[]map[string]any{}` so the JSON envelope is always `{"results":[...], "error":null}` (D-33). Don't return `nil` slices to the transport.
15. **Field path depth** — `extractNestedField` caps at 8 levels (`maxNestingDepth`) for safety (T-02-02). A pathological event can't recurse forever.
16. **`$.` prefix** — dot paths may start with `$.`; stripped in `extractNestedField` for backward-compatible config (`$.user.id` ≡ `user.id`).
17. **Composite key separator safety** — in composite keys, every character of the separator is escaped inside components, so a component can't contain the separator (WR-04). This is why `_` and `..` separators are rejected. Changing a view's `key_separator` or `key_fields` changes its keys; the fingerprint check makes `Start` refuse until the view is dropped.
18. **`Query` thread-safety before `Start`** — the lazily opened KV handle is read and set under `e.mu`, but the open itself runs without the lock (FIX-ENG-02, LC-04). Never make the read path create or update the bucket (WR-05), and never make it take the `lifecycle` lock.
19. **Ordered, CAS-guarded writes** — `Writer.Apply` must keep comparing `_meta.stream_seq` within one source incarnation and writing with `Create`/`Update(revision)`. Reverting to `kv.Put` lets redelivered older events overwrite newer rows (WR-01).
20. **Fingerprints cover the key layout** — any change to `BuildPKKey`, `FormatPKValue` or the key layout must bump `kv.KeyLayoutVersion`, so existing views are refused and rebuilt instead of silently splitting across two encodings (WR-09).
21. **Scans need the snapshot marker** — a `FullScanPlan` result is only complete after the watcher's `nil` entry. Treating a closed channel as the end of the scan returns partial results as success (QE-02).
22. **Stop *and* drain watchers** — after `watcher.Stop()`, keep reading `Updates()` until it closes (`stopWatcher`), or the nats.go callback leaks blocked on a full channel (QE-04).
23. **Server-side `MaxDeliver` stays -1** — the retry budget lives in the materializer. A finite server limit strands events whose DLQ publish fails on the last attempt (WR-07).

## 9. Performance Characteristics

- PK lookup: O(1) `kv.Get`.
- PK `IN` lookup: one `kv.Get` per distinct candidate key (≤256).
- Full scan: O(keys of this view) — the server filters by `{view}.pk.>`, so other views' rows are never sent (PF-01). Parallelized 16-way for decode/filter.
- Materialization: bounded by KV write latency, single goroutine per view. `MaxAckPending` controls in-flight backpressure (default 50).
- Rough ceiling: ~100K keys per view comfortably; 1M+ keys in one view — full scans become slow. Document, don't pretend otherwise.
- NATS queries: at most 16 executing per engine; HTTP queries are not limited.

## 10. Security Posture

- HTTP binds `127.0.0.1` by default (T-02-06). `WithHTTPServer("0.0.0.0:…")` is the documented escape hatch; do not silently change the default bind.
- Embedded NATS: single-node, `NoSigs`, random port unless configured, logs routed to slog; private temp store unless `store_dir` is set.
- SQL is read-only; there is no injection surface through SQL because writes don't happen via SQL. Still, the parser rejects everything except the narrow supported grammar.
- No secrets in logs. `slog` structured fields should never include credentials (and there are none in this codebase today).
- `gosec` linter is on (only G115 — integer overflow conversion — is excluded, judged acceptable for bounded KV key sizes).

## 11. Extension Points (when adding features)

- **New column type**: add to `cfg.ColumnType` consts + `Valid()` (exhaustive switch), extend `mapper.validateType`, extend `valuesEqual`/`normalizeValue` in the executor, add type-integrity test. `exhaustive` linter will flag half-done switches.
- **New WHERE operator** (`>`,`<`): add `Op` const, map in `comparisonToCondition`, extend `filterRow` switch (exhaustive), add planner logic (index vs scan), update `SQL_DIALECT.md`.
- **Secondary indexes**: `cfg.Indexes` is currently rejected in `Validate`. To implement: remove the rejection, add index maintenance in `writer.Apply` (separate prefix like `idx.<view>.<col>.<val>.>/<pk>`), add an index-scan `Plan`, update `planner.BuildPlan`.
- **Delete/tombstones**: not designed (v2). `kv` package doc flags this. Any design must atomically remove the row key and its index entries.
- **Per-view KV buckets**: breaking change — needs config migration and a `kv.BucketFor(view)` indirection. Scoped scans no longer need them (key layout 2 filters per view on the server).
- **OR conditions**: parser currently rejects `OrExpr`. To enable: extend `extractConditions`, add a query plan that fans out to multiple PK/index lookups and unions.

When extending, prefer adding to the existing component over creating a new package, and write black-box tests at the `natsql_blackbox_test.go` level in addition to unit tests.