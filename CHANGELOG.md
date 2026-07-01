# Changelog

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
