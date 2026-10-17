# Changelog

## [Unreleased]

Fixes for the five Critical findings (WR-01 to WR-05) and the 21 High findings of the October 2026 code review. **This release changes the module path and the KV key layout; read the upgrade notes.**

### Module
- **Changed**: The module path is now `github.com/gacopys/natsql/v2`. Go rejects `v2.x` tags for a module path without the major-version suffix, so `go get github.com/gacopys/natsql@latest` resolved to v1.0.0 and v2.0.0 couldn't be installed. `go.mod` retracts v2.0.0, which was tagged before the suffix (BLD-01)

### Materializer & Storage
- **Fixed**: A redelivered older event (after a NAK, a lost ack or an `AckWait` expiry) could overwrite a newer row. `Writer.Apply` now skips events whose stream sequence is not newer than the stored row's, and writes with compare-and-swap (`Create`/`Update` against the revision it read), retrying when a concurrent writer gets there first. Sequences are only compared within one incarnation of the source stream, so a recreated stream's restarted sequences still apply (WR-01)
- **Fixed**: Write errors not on a short substring list were treated as permanent, so events failing on recoverable errors (`no responders`, `no response from stream`, `JetStream system temporarily unavailable`, disconnects) were dead-lettered and acked, dropping their updates. Only invalid keys, unencodable rows and oversized rows are now terminal, matched by error identity and JetStream error code rather than message text; everything else is retried (WR-02)
- **Fixed**: Transient failures were NAK'd with no delay, so a one-second outage used up `MaxDeliver` within milliseconds and the event was silently dropped. Retries now back off (250ms doubling, capped at 30s), and an event that uses up its retry budget is dead-lettered instead of NAK'd (WR-02)
- **Fixed**: An event whose last delivery attempt also failed to reach the DLQ, or whose `AckWait` kept expiring, was dropped by the server at `MaxDeliver`, outside both the view and the DLQ. Durable consumers now have no server-side delivery limit (existing ones are updated on start); `consumer.max_deliver` is the materializer's retry budget before dead-lettering, and `-1` now really means "retry forever" instead of being replaced by 10 (WR-07)
- **Fixed**: PK values containing characters that aren't valid in NATS KV keys (`@`, spaces, `:`, `+`, non-ASCII, `.` in awkward places) produced invalid keys, so their events were dead-lettered and PK lookups failed with `nats: invalid key`. This affected every `timestamp` primary key. `kv.BuildPKKey` now escapes these bytes as `_xHH`, so every PK maps to a valid key (WR-03)
- **Fixed**: With a custom `key_separator`, composite keys could collide (`("globex-sales","widget")` and `("globex","sales-widget")` both became `globex-sales-widget`), so one row overwrote another. Separator characters are now escaped inside components, making the encoding injective (WR-04)
- **Fixed**: A numeric PK's key depended on how the number was spelled in the event and in the query: `1` and `1.0` were two rows, and `WHERE id = 1.0` or `id = 1e3` missed rows that `IN` found. `kv.FormatPKValue` is now the single PK formatter for the write and read paths and writes numbers as exact plain decimals (`1.0`, `1e0` → `1`; `1e3` → `1000`; `-0` → `0`) without going through `float64`. SQL decimal literals keep their exact digits (WR-06)
- **Fixed**: Every query and every `Start` called `CreateOrUpdateKeyValue` with default settings, which reset an existing bucket's history, TTL, size limit and replicas and deleted retained revisions. The bucket is now opened as-is and only created when missing; queries never create or modify it and parse the SQL before touching storage. The DLQ stream is likewise created only when missing, so operator changes to its retention or replicas survive restarts (WR-05)
- **Fixed**: Events too large to fit the max payload once base64-encoded in a DLQ envelope couldn't be dead-lettered and were retried until dropped. Such envelopes are re-sent with `payload_omitted: true` and a reference to the event instead. Every envelope now also records `original_stream`, `original_subject`, `original_sequence`, `num_delivered` and `original_size`, uses an RFC 3339 timestamp with nanoseconds, and caps the error text at 4 KiB (WR-08)
- **Fixed**: Schemas were written twice (by `Start` and by each materializer), a failed write was only a warning, and a changed view definition silently overwrote the schema while the consumer resumed: old rows were never rebuilt, and rows under changed keys stayed forever, duplicating in scans. `Start` is now the only schema writer and fails if it can't write one. Each schema records a fingerprint of the definition (source stream and filter, columns, mappings, types, key fields, separator, key layout); `Start` refuses a view whose stored fingerprint differs, or that has no current schema, with `ErrViewRebuildRequired` when the view has materialized state (a durable consumer or rows); a schema with nothing materialized behind it is replaced (WR-09)
- **Added**: `Engine.DropView(ctx, view)` and `natsql drop-view --view V` delete a view's durable consumer, rows and schema (in the current and the earlier key layout) so the next start replays it from its source stream (WR-09)
- **Changed**: KV keys are `.`-delimited: `{view}.pk.{pk}` and `{view}.meta.schema` instead of `{view}/pk/{pk}` and `{view}/meta/schema`, so the server can send a scan only its own view's rows. Inside PK components, `.` and every other byte outside `[A-Za-z0-9-=]` is escaped as `_xHH` and an empty component is `_e` (PF-01)
- **Changed**: View names must match `^[A-Za-z_][A-Za-z0-9_]*$`. Names with `.`, `-`, `/`, spaces or wildcards passed validation and then failed at runtime (or, with `/`, could alias another view's keys) (CF-01)
- **Changed**: Row `_meta` gains `stream` and `stream_created`, which identify the source stream incarnation used for ordering
- **Changed**: Each materialized event now does a KV read before its write (one extra round trip per event)

### Query Engine
- **Fixed**: `SELECT *` returned `_meta` on full scans and in mixed projections such as `SELECT *, id`. `*` now expands to the schema's columns on every plan, so internal fields and fields of an older schema never appear (QE-01)
- **Fixed**: A scan interrupted by its timeout, a client disconnect or cancellation returned the rows read so far, or none, as a successful result. A scan now succeeds only after reading the whole snapshot and otherwise returns `scan interrupted before all rows were read`. It also stops at the first undecodable row instead of reading on (QE-02)
- **Fixed**: `LIMIT 0` returned rows, and `OFFSET` (including `LIMIT o, n`) was silently ignored. `LIMIT 0` now returns no rows without reading storage, and `OFFSET` is rejected (QE-03)
- **Fixed**: Every scan stopped early by `LIMIT` on a view with more than 256 rows leaked a goroutine blocked inside nats.go's KV watcher. Stopped watchers are now drained until nats.go closes them (QE-04)
- **Fixed**: A full scan read every row of every view in the bucket and filtered client-side, and `WHERE pk IN (...)` always took that path. Scans now ask the server for one view's rows only, and PK `IN` lists (up to 256 candidate keys, including composite keys mixing `=` and `IN`) become point lookups (PF-01)
- **Fixed**: A KV delete or purge marker (from manual maintenance) broke every full scan of its view with `unmarshaling row: EOF`; markers are now skipped (QE-09)
- **Fixed**: `WHERE pk = NULL` built the key `<nil>` and failed; it now returns no rows. Contradictory PK equalities are detected even when other key fields are unconstrained (QE-10)

### Engine Lifecycle
- **Fixed**: A `Start` that failed after launching materializers (for example on an HTTP port in use) left them running and the NATS query subscription answering, while `Close` reported `engine not started`; retrying started a second materializer on the same durable. A failing `Start` now releases everything it started, in shutdown order, and can be retried (LC-01)
- **Fixed**: The context passed to `Start` controlled how long materializers ran, so `ctx, cancel := context.WithTimeout(…); defer cancel(); eng.Start(ctx)` silently stopped all materialization while the engine kept reporting `Started`. It now bounds startup only; materializers run until `Close` (LC-02)
- **Fixed**: `Start` declared success after a fixed 500ms whether or not consumers were set up, and later materializer failures (including recovered panics, which looked like clean exits) went unnoticed. `Start` now waits until every materializer reports that it is consuming. Each view runs under a supervisor that restarts a failed materializer with backoff (1s doubling to 1m); a closed NATS connection leaves the view `failed` (LC-03)
- **Fixed**: The engine mutex was held across `Start`'s network calls and readiness wait, `Close`'s shutdown waits, and every query's bucket open, so queries and `Stats` blocked behind lifecycle operations, and a query arriving during `Close` could stall `http.Server.Shutdown`. Lifecycle operations now use a separate lock that queries and `Stats` never take, and `Close` waits for in-flight NATS queries (LC-04)
- **Fixed**: `Close` canceled materializers immediately after signaling the drain, so the drain was often skipped and in-flight events were redelivered. It now waits (up to 10s) for materializers to finish draining before canceling. Materializers also stop their pull subscription and helper goroutines when they exit (LC-05)
- **Fixed**: Concurrent `Close` calls on the facade raced on the owned connection and embedded server; they are now released exactly once (LC-06)
- **Added**: `Stats()` reports `Ready`, `NATSConnected`, `HTTPAddr` (the bound address, useful with port 0), query counters, and per-view `ViewStatus` (state, restarts, events processed, dead-lettered and retried, last error). `natsql.Stats`, `natsql.ViewStatus` and `natsql.ErrViewRebuildRequired` are exported (OP-03)

### Transport
- **Added**: `GET /healthz` (liveness) and `GET /readyz` (200 when started, connected to NATS and every view is consuming; 503 otherwise), both returning the `Stats` document (OP-03)
- **Fixed**: NATS queries ran one at a time on the subscription's delivery goroutine, so one slow scan blocked every lookup behind it, and every engine on the account answered every request. Queries now run concurrently (at most 16 at once, 256 executing or queued, then an immediate overload error), the 30s timeout counts from arrival, and engines subscribe in queue group `natsql`. The subject is configurable with `nats.query_subject` (default `natsql.query`) (TR-01)
- **Fixed**: A NATS query result larger than the server's max payload got no reply, so the requester timed out. It now gets an error reply suggesting a `LIMIT` or the HTTP API. Error replies are encoded with `encoding/json`, so they are always valid JSON, and requests without a reply subject are ignored (TR-02, TR-08)

### Embedded NATS & CLI
- **Fixed**: Embedded engines without `nats.store_dir` all used nats-server's shared default store (`$TMPDIR/nats/jetstream`), so separate engines and programs reopened each other's streams and views. Each such engine now gets a private temporary store, deleted on `Close`. The embedded server's warnings and errors now go to the engine's logger instead of being discarded (OP-01)
- **Fixed**: After about two minutes of NATS outage the CLI's connection closed for good, and each materializer then logged an error every 100ms forever while the process looked healthy. The CLI now reconnects indefinitely and logs disconnects and reconnects, materializers back off on consumer errors (100ms doubling to 5s), and if the connection does close the server exits non-zero (OP-02)
- **Fixed**: The CLI installed its signal handler only after `Start`, and didn't close the engine when `Start` failed (OP-09)
- **Fixed**: The `.gitignore` entry for the `natsql` binary also ignored the `cmd/natsql/` directory (BLD-03)

### Config Validation
- **Changed**: `key_separator` values containing `_` (the PK escape character) or `..` (an empty NATS subject token) are now rejected
- **Changed**: `consumer.max_deliver` values below `-1` and invalid `nats.query_subject` values are rejected

### Tests
- **Fixed**: `requireError` in the black-box tests never checked the expected message, so every error test passed on any error; three of them were asserting messages the engine doesn't produce. The NATS flush test now exercises a flush failure after a successful subscribe, and the heartbeat test waits for an actual heartbeat (TS-01)

### Build & CI
- **Changed**: Lint runs on golangci-lint v2.14.0, because v1.64.8 can't analyze code built with Go 1.27. `.golangci.yml` is migrated to the v2 format with the same linters and settings. The new findings are fixed: tests use `slog.DiscardHandler`, and the HTTP listener binds through `net.ListenConfig`
- **Security**: Builds, tests and CI use Go 1.27.2 through a `toolchain` line in `go.mod` and every example's `go.mod`. This fixes the 14 standard-library advisories `govulncheck` reports for Go 1.26.5 (e.g. GO-2026-6617, GO-2026-6089, GO-2026-5972). The `go` line stays at 1.26.5, so library users are not forced onto Go 1.27
- **Changed**: Every example now checks its results (row counts and values, DLQ contents, error messages, readiness, log output) and exits non-zero on a wrong one, instead of printing whatever came back. CI used to pass any example that didn't crash: 12 and 19 printed success after querying a stream their view didn't read, and 12, 15 and 19 only ran because of state left in the shared default store. Examples wait for materialization by polling instead of fixed sleeps, and return errors from `run()` so their engines are always closed (EX-01, EX-02)
- **Changed**: CI now runs all 19 examples through `examples/run.sh`, including 06-cli-server (builds the `natsql` binary, runs a new Go client against it, and checks for a clean shutdown) and 07-perf-benchmark (new `-events` flag; CI uses 2,000 events and checks every benchmark query's row count). A guard job fails if an example is missing from the workflow. `make examples` now stops at the first build failure (BLD-04), and `make examples-run` runs every example locally

### Upgrade notes
- **Change imports** from `github.com/gacopys/natsql` to `github.com/gacopys/natsql/v2`, and `go get github.com/gacopys/natsql/v2@latest` (the first release tagged after this change).
- **Every existing view must be rebuilt.** The key layout changed, so `Start` refuses views materialized by earlier versions with `view must be rebuilt`. For each view: stop every natsql process materializing it, run `natsql drop-view --config=config.yaml --view=<view>` (add `-e --store-dir=…` in embedded mode) or call `Engine.DropView`, then start natsql. The view is replayed from the beginning of its source stream; **events the stream no longer retains (`max_age`, `max_msgs`, interest or work-queue retention) can't be replayed**, so check retention first. `drop-view` deletes the durable consumer and purges both the old `{view}/…` keys and the new `{view}.…` keys.
- **Changing a view's definition now requires the same rebuild**: columns, mappings, types, key fields, separator, `source_stream` or `source_subject`. Consumer tuning (`max_ack_pending`, `max_deliver`, `ack_wait_seconds`) can change freely.
- **Rename views** whose names contain anything other than letters, digits and `_` (or start with a digit); such configs now fail validation.
- **Recovering events dead-lettered by the old behavior.** Events rejected for invalid PK characters, or dead-lettered because a transient error was misclassified, are in `natsql-dlq` and missing from the view. Rebuilding the view replays them.
- **`max_deliver`** keeps its meaning as the number of attempts before an event is dead-lettered, but the server no longer stops redelivering on its own; `-1` now retries forever.
- **Embedded mode without `store_dir` no longer persists anything across restarts** (it used to share nats-server's default temp directory with every other embedded server). Set `nats.store_dir` to keep views.
- **NATS queries** now use queue group `natsql`: engines on the same subject share requests. Engines with different configs on one account must use different `nats.query_subject` values.
- **`SELECT *`** returns exactly the schema's columns; fields stored by an older definition no longer appear.
- **Configs with a `_` or `..` separator now fail validation.** Choose another separator; the view then needs a rebuild.
- **Bucket and DLQ settings are no longer reset at startup.** Settings you changed on `natsql-views` or `natsql-dlq` now persist, and natsql no longer repairs them for you.

## [2.0.0] — 2026-07-01

> **122 commits** since v1.0.0 across 170 files changed (16.5k additions, 9.9k deletions).
> This release spans milestones v1.2 and v3 (planning phases 08–12), covering a complete internal
> rewrite of query correctness, materializer lifecycle, transport robustness, config validation,
> code quality infrastructure, and documentation.

### Highlights
- Complete internal re-architecture of the materializer write path (sequential processing, proper error classification, drain-before-cancel)
- Query engine correctness overhaul (json.Number precision, post-filter all conditions, contradictory PK detection, canonical PK encoding)
- Transport robustness hardened (proper MaxBytesError handling, trailing data rejection, surface Flush/Respond errors)
- Config validation coverage expanded (key_fields↔primary_key cross-validation, separator checks, duplicate names, index rejection)
- Comprehensive code quality infrastructure (~40+ linters, Makefile, parallel CI workflows, 100% gocyclo score)
- 12 new examples, full architecture/conventions documentation, SQL dialect spec

---

### SQL Dialect & Query Engine
- **Shipped**: `LIMIT n` support — rows capped at execution time (T-02-10)
- **Shipped**: `!=` (not-equal) operator in WHERE clauses
- **Shipped**: `$.` prefix notation in field paths (stripped before extraction)
- **Shipped**: `IN (value, value, ...)` multi-value list lookups
- **Added**: Rejection of unsupported SQL constructs with descriptive errors — `OR`, `ORDER BY`, `GROUP BY`, `HAVING`, `JOIN`, subqueries, aggregations, DML (D-23)
- **Fixed**: All WHERE conditions now post-filtered, including PK equalities (CR-03) — ensures full scan and PK lookup plans produce identical results
- **Fixed**: Contradictory PK predicates (`WHERE id = 'a' AND id = 'b'`) return zero results via `EmptyPlan` instead of an incorrect lookup (D-02)
- **Fixed**: `json.Number` precision — uses `decoder.UseNumber()` everywhere; integers >2^53 preserved exactly (CR-09)
- **Fixed**: `SELECT *` now strips `_`-prefixed keys (e.g., `_meta`) from output (D-31)
- **Fixed**: Type-aware `valuesEqual` for `json.Number` comparisons (string `"42"` ≠ number `42`)
- **Added**: `BuildPKKey` canonical PK encoder — single function used by both materializer (write) and query engine (read) paths; PK parts sanitized individually before join (CR-02)
- **Fixed**: Unix seconds-timestamp (`time_s`) in schema; error message phrasing corrected
- **Added**: SQL_DIALECT.md specification document

### Materializer & Engine Lifecycle
- **Changed**: Sequential event processing — single goroutine per view, no worker pool (D-01, D-02). Preserves JetStream per-subject ordering.
- **Added**: Error classification — transient errors (deadline, timeout, conn-refused, no-leader, closed) → NAK; terminal/malformed → DLQ + Ack (D-14)
- **Changed**: Removed `InactiveThreshold` on durable consumers — prevents premature consumer expiry (CR-11)
- **Renamed**: `BatchSize` → `MaxAckPending` (CLN-06). Automatic migration in `SetDefaults()` for backward compatibility. Old field kept as deprecated alias.
- **Fixed**: Drain-before-cancel shutdown order — `cons.Drain()` called before context cancellation, preventing unnecessary redeliveries on restart (D-58)
- **Fixed**: All startup errors now propagated synchronously in `Start()` — no more silent failures (CR-07)
- **Fixed**: HTTP port now wired from `Config.HTTP.Port` in engine constructors with `8080` fallback (D-13)
- **Fixed**: `WithQueryPort(0)` lets OS choose a random free port — eliminates port conflicts in parallel tests
- **Changed**: `Run()` signature simplified — `dlqStream` parameter removed (DLQ stream resolved internally)

### Config Validation
- **Added**: Cross-validation of `key_fields` vs `primary_key: true` columns — mismatch returns detailed error (CR-01)
- **Added**: Separator charset validation — rejects keys `*`, `>`, `/` as separators (CR-04)
- **Added**: Duplicate view name detection
- **Added**: Index configuration rejection — `indexes` block returns clear "not yet supported" error (CR-16, CLN-02)
- **Refactored**: `Validate()` extracted into `validateView()` and `crossValidateKeyFields()` helpers

### Transport Robustness
- **Fixed**: HTTP properly distinguishes `*http.MaxBytesError` via `errors.As` — returns 413 for oversized bodies vs 400 for invalid JSON (D-17)
- **Added**: Trailing data rejection — extra bytes after JSON body return 400 with `"unexpected data after JSON body"` (D-18)
- **Fixed**: NATS transport surfaces `Respond()` and `Flush()` errors as warnings (CR-19)
- **Fixed**: HTTP server binds `127.0.0.1` before `Serve()` — synchronous port binding (T-02-06)
- **Added**: `slog.Warn` logging for failed NATS responses

### CLI
- **Added**: `--create-streams` flag — opt-in source stream auto-creation in external NATS mode (CR-14)
- **Added**: `source_subject` support — streams created with the view's source subject filter instead of `<stream>.>` wildcard
- **Changed**: Embedded mode always auto-creates streams; external mode requires explicit `--create-streams`
- **Refactored**: `runServer()` decomposed into `createEngine()` and `createSourceStreams()` helpers
- **Fixed**: `--port 0` now correctly passes through to `WithQueryPort(0)` for random port selection

### Code Quality & CI Infrastructure
- **Added**: `Makefile` with targets: `build`, `lint`, `lint-fix`, `vuln`, `test`, `coverage`, `examples`, `gocyclo`, `dupl`
- **Added**: `.golangci.yml` configuration with ~40+ linters: `gofumpt`, `gci`, `errcheck`, `exhaustive`, `errorlint`, `gosec`, `revive`, `depguard`, `tagliatelle`, `godot`, `paralleltest`, `nakedret`, `perfsprint`, `usestdlibvars`, `misspell`, `predeclared`, `bodyclose`, `noctx`, `copyloopvar`, `sloglint`
- **Changed**: CI split into 7 per-service parallel workflows: `lint`, `build`, `test`, `examples`, `vulnerability`, `cyclo`, `dupl` — each with own README badge
- **Achieved**: 100% gocyclo score — all functions ≤ 15 cyclomatic complexity
- **Added**: Code duplication checking (`art-dupl`, 50-token threshold) with CI badge
- **Added**: `govulncheck` integration — Go 1.26.4 toolchain with zero vulnerability findings
- **Added**: README badges for Go Report Card, license, latest tag, GitHub stars
- **Added**: `example-*/` and `.idea/` to `.gitignore`
- **Added**: `testutil` package (`internal/testutil/`) — shared `StartEmbeddedNATS(t)` and `CreateStream(t, ctx, js, name)` helpers with `t.TempDir()` isolated stores
- **Fixed**: All golangci-lint issues across the codebase (100% clean)
- **Fixed**: Concurrent port handling for multi-engine tests (random 0 port)
- **Fixed**: Cross-test JetStream store pollution via isolated `t.TempDir()` stores in testutil

### Documentation
- **Added**: `CONVENTIONS.md` — comprehensive coding conventions (naming, error handling, constructors, testing, config, git, CI)
- **Added**: `ARCHITECTURE.md` — full architecture reference with component descriptions, storage layout, write/read paths, lifecycle, critical invariants, extension points
- **Added**: `SQL_DIALECT.md` — SQL dialect specification with examples, supported/unsupported features, field path notation
- **Updated**: `AGENTS.md` aggregator points to CONVENTIONS.md, ARCHITECTURE.md
- **Updated**: `README.md` — 19 examples listed, updated feature table (LIMIT, `$.` prefix, `!=` marked as shipped), quick-start section, Go Report Card badges
- **Added**: Package doc comments on all exported symbols across `kv`, `engine`, `query`, `materialize`, `transport`, `testutil`, root `natsql`

### Examples (12 new)
| # | Example | What It Shows |
|---|---------|---------------|
| 08 | `where-operators` | `WHERE IN`, `WHERE !=`, full table scan |
| 09 | `key-separator` | Custom `key_separator`, PK sanitization |
| 10 | `source-subject` | `source_subject` filter, subject routing |
| 11 | `consumer-tuning` | `max_ack_pending`, `max_deliver`, retry config |
| 12 | `http-options` | `WithQueryPort`, `WithHTTPServer`, custom port |
| 13 | `bool-timestamp` | `boolean` and `timestamp` column types |
| 14 | `yaml-vs-json` | JSON config file, dual-format `LoadConfig` |
| 15 | `autocreate-streams` | `--create-streams` flag, stream existence |
| 16 | `poll-for-ready` | Polling for materialization, `LIMIT` |
| 17 | `dual-engine` | Two independent engines in one process |
| 18 | `nats-request` | Pure NATS request-reply, no HTTP |
| 19 | `custom-logger` | `slog` structured logging, `WithLogger` |

All 7 original examples (01–07) updated for v2 API changes.

### Internal Refactoring
- **Changed**: All `fmt.Errorf("static message")` → `errors.New("static message")` for constant error strings
- **Changed**: All `json.Unmarshal(data, &v)` → `json.NewDecoder(bytes.NewReader(data)).UseNumber()` for numeric precision
- **Fixed**: `MustInitBucket()` (panic-based) removed in favor of call-site error handling
- **Fixed**: `gofmt`/`gofumpt` import ordering and alignment across all source files

### Breaking Changes (from v1.0.0)
- `BatchSize` field renamed to `MaxAckPending` (automatic migration in SetDefaults, old name kept as deprecated alias)
- `PkKey` function deprecated in favor of `BuildPKKey` (old function still available)
- `Run()` no longer takes a `dlqStream` parameter (DLQ stream resolved internally)
- `Writer` constructor now requires separator parameter
- Error sentinel types changed: `fmt.Errorf` → `errors.New` for constant strings (wire format unchanged)
- `MustInitBucket()` removed (use `InitBucket()` instead)

---

## [1.0.0] — 2026-07-01

Initial release of natsql — a NATS-native materialized view engine.

### Highlights
- Define stream-to-KV materializations declaratively in YAML or JSON
- Query materialized state with read-only SQL via HTTP or NATS request-reply
- Zero external infrastructure — runs with a standalone NATS server or with an embedded NATS server (single binary)
- Usable as a standalone CLI binary or as a Go library embedded in your own application

### Deployment Modes
- **External NATS**: Connect to an existing NATS JetStream cluster (`--nats-url`)
- **Embedded NATS**: Start a NATS server in-process (`--embedded`), no external process needed
- **Go Library**: Import `github.com/gacopys/natsql` and call `New()`, `NewWithNATS()`, or `NewEmbedded()`

### CLI
- `natsql server` command with flags:
  - `--config` / `-c` — path to config file (default `config.yaml`)
  - `--embedded` / `-e` — start embedded NATS server
  - `--nats-url` / `-u` — NATS server URL (overrides config)
  - `--store-dir` — JetStream store directory (embedded mode)
  - `--port` / `-p` — HTTP query API port (overrides config)
- CLI flags take precedence over config file values
- Graceful shutdown on SIGINT/SIGTERM

### Configuration (YAML / JSON)
- `nats` section: URL, embedded mode toggle, store directory, port
- `http` section: listen port (default 8080)
- `views` section (at least one required):
  - View name, source JetStream stream, optional source subject filter
  - Primary key fields with custom separator (default `|`)
  - Column definitions with JSON dot-path extraction (`from: "org.id"`)
  - Four column types: `string`, `number`, `boolean`, `timestamp`
  - Consumer tuning: `batch_size`, `max_deliver`, `ack_wait_seconds`
  - Forward-compatible `indexes` field (parsed, ignored in v1)

### HTTP API
- `POST /api/v1/query` — execute a SQL query
- Request: `{"sql": "SELECT * FROM users WHERE user_id = 'abc'"}`
- Response: `{"results": [...], "error": null}`
- Always returns HTTP 200; errors in JSON body
- 1 MB request body limit (HTTP 413)
- Invalid JSON body returns HTTP 400
- Middleware: request logging, panic recovery, 30-second timeout
- Bruno API collection with 23 test requests included

### NATS Request-Reply API
- Subject: `natsql.query`
- Request payload: raw SQL string
- Response payload: same JSON envelope as HTTP API
- Example: `nats req natsql.query "SELECT * FROM users WHERE user_id = 'abc'"`

### SQL Dialect (Read-Only)
- **Supported**: `SELECT *`, `SELECT col1, col2`, `FROM view_name`
  - `WHERE col = 'val'` (equality)
  - `WHERE col != 'val'` (not-equal)
  - `WHERE col IN ('a', 'b', 'c')` (value list)
  - Multiple conditions combined with `AND`
  - `LIMIT n`
  - String, integer, float, boolean, and NULL literals
- **Not supported in v1**: `OR`, range operators (`>`, `<`, `>=`, `<=`), `BETWEEN`, `LIKE`, `ORDER BY`, `GROUP BY`, `HAVING`, `JOIN`, subqueries, aggregations, DML statements
- WHERE clause is required for all queries
- Typed JSON output: strings as strings, numbers as numbers, booleans as `true`/`false`, NULL as `null`
- Empty result sets return `"results": []` (not null)

### Query Execution
- **PK Lookup Plan** (O(1)): Direct KV key lookup when all primary key fields have equality conditions
- **Full Scan Plan** (O(n)): Streaming all-key iteration with 16-worker parallel pool when PK is missing or non-equality operator used
- Non-PK WHERE conditions applied as client-side post-filter on PK lookups
- Column projection and LIMIT enforced at execution time
- Unknown view → descriptive error; unknown column → descriptive error; missing WHERE → error

### Materialized Views
- Auto-creates source JetStream streams on startup (subject: `<streamName>.>`)
- Durable pull consumers per view (name: `natsql-{viewName}`), resume from last position on restart
- Single KV bucket (`natsql-views`) stores all views with namespaced keys
- Composite primary keys: all key field values joined with configurable separator
- 16 concurrent workers per view, 64-message processing buffer
- Heartbeat logging every 60 seconds with event count
- JSON dot-path column extraction from nested objects

### Error Handling & DLQ
- Dead-letter queue stream (`natsql-dlq`) with 7-day retention
- Malformed JSON, type mismatches, and missing key fields → DLQ + continue processing
- KV write failures → DLQ + continue
- DLQ envelope includes base64-encoded original message, view name, error, and timestamp

### Storage
- PK key sanitization for NATS KV safety (`_`, `|`, `/`, `*`, `>` characters encoded)
- Row metadata stored per record: source stream sequence number and update timestamp
- CAS-based consistency (read-committed isolation)
- View schemas stored alongside data in KV bucket

### Go Library API
- Three constructors: `New(js, cfg, opts...)`, `NewWithNATS(nc, cfg, opts...)`, `NewEmbedded(cfg, opts...)`
- Options: `WithLogger(*slog.Logger)`, `WithHTTPServer(addr string)`, `WithQueryPort(int)`
- Methods: `Start(ctx)`, `Query(ctx, sql)`, `Close()`, `Stats()`, `NC()`, `EmbedNode()`
- Thread-safe `Query()` available at any lifecycle phase (before/after `Start`)
- Error sentinels: `ErrAlreadyStarted`, `ErrNotStarted`
- Graceful 5-step shutdown on `Close()`: HTTP → NATS → consumer drain → context cancel → goroutine wait

### Examples
- `01-hello-natsql` — minimal embedded setup, single view, HTTP + SQL query, stats
- `02-composite-keys` — multi-field primary key, nested JSON extraction, custom separator
- `03-malformed-events` — DLQ handling for invalid, malformed, and missing-key events
- `04-multiple-views` — two independent views with cross-view isolation
- `05-library-embed` — `New()` and `NewWithNATS()` constructors with custom HTTP port
- `06-cli-server` — standalone binary with config file and embedded NATS
- `07-perf-benchmark` — 1M events, 64 parallel publishers, 7 query pattern categories
