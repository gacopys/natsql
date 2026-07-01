# Changelog

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
