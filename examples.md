# natsql — Example Suite

Every natsql feature has a dedicated, self-contained example under `examples/`.
Each example is a standalone Go program with its own `go.mod`.

## Existing Examples

| # | Directory | Feature(s) Covered |
|---|-----------|-------------------|
| 01 | `01-hello-natsql` | Embedded NATS, PK lookup (`WHERE =`), `eng.Query()`, HTTP API, `Stats()`, default consumer |
| 02 | `02-composite-keys` | Composite primary keys, dot-path `from` (`org.id`), NATS request-reply |
| 03 | `03-malformed-events` | DLQ (invalid JSON, type mismatch, missing key), column type validation |
| 04 | `04-multiple-views` | Multiple views in one engine, cross-view isolation, column projection |
| 05 | `05-library-embed` | `New()`, `NewWithNATS()`, `WithLogger()`, in-process Go embedding |
| 06 | `06-cli-server` | YAML config file, `natsql server` CLI, `LoadConfig`, config validation |
| 07 | `07-perf-benchmark` | High-throughput pub/sub, parallel publishers, query latency benchmarks |

## Proposed New Examples

| # | Directory | Feature(s) Covered |
|---|-----------|-------------------|
| 08 | `08-where-operators` | `WHERE ... IN`, `WHERE ... !=`, full table scan (non-PK filter) |
| 09 | `09-key-separator` | Custom `key_separator`, PK key encoding (`SanitizePK`) |
| 10 | `10-source-subject` | `source_subject` filter, consuming from a subset of stream subjects |
| 11 | `11-consumer-tuning` | `max_ack_pending`, `max_deliver`, `ack_wait_seconds`, consumer behavior |
| 12 | `12-http-options` | `WithHTTPServer()`, `WithQueryPort()`, custom HTTP address/port, HTTP-only mode |
| 13 | `13-bool-timestamp` | `boolean` and `timestamp` column types, type filtering |
| 14 | `14-yaml-vs-json` | JSON config file alternative, `LoadConfig` with `.json`, dual-format support |
| 15 | `15-autocreate-streams` | `--create-streams` flag, external NATS mode, stream existence checks |
| 16 | `16-poll-for-ready` | Polling query loop to wait for materialization, `LIMIT` usage |
| 17 | `17-dual-engine` | Two independent engines in one process, port isolation, separate KV namespaces |
| 18 | `18-nats-request` | Pure NATS client: `nats req natsql.query`, no HTTP, raw response |
| 19 | `19-custom-logger` | Structured logging with `slog`, log levels, `WithLogger()` in detail |

---

## Example 08: `where-operators`

**Covers:** `WHERE ... IN`, `WHERE ... !=`, full table scan on non-PK column.

```yaml
# config.yaml
views:
  - name: products
    source_stream: product-events
    key_fields: [id]
    columns:
      - { name: id,     from: id,     type: string, primary_key: true }
      - { name: name,   from: name,   type: string }
      - { name: price,  from: price,  type: number }
      - { name: status, from: status, type: string }
```

**What it demonstrates:**
- `SELECT * FROM products WHERE status IN ('active', 'pending')` — IN operator
- `SELECT * FROM products WHERE status != 'deleted'` — != operator
- `SELECT * FROM products WHERE price = 9.99` — full table scan on non-PK column
- Measure latency difference between PK lookup and full scan

Run: `cd examples/08-where-operators && go run .`

---

## Example 09: `key-separator`

**Covers:** Custom `key_separator`, `SanitizePK` encoding, wild PK values.

```yaml
# config.yaml
views:
  - name: tenant_items
    source_stream: tenant-events
    key_fields: [tenant_id, item_id]
    key_separator: "-"
    columns:
      - { name: tenant_id, from: tenant_id, type: string, primary_key: true }
      - { name: item_id,   from: item_id,   type: string, primary_key: true }
      - { name: value,     from: value,     type: number }
```

**What it demonstrates:**
- Custom separator `"-"` instead of default `"/"`
- PK values containing `/`, `*`, `_` are sanitized
- Composite PK lookup with two key parts
- Inspect raw KV keys to see encoding

Run: `cd examples/09-key-separator && go run .`

---

## Example 10: `source-subject`

**Covers:** `source_subject` filter, consuming a subset of stream subjects.

```yaml
# config.yaml
views:
  - name: user_creates
    source_stream: all-events
    source_subject: all-events.user.created
    key_fields: [id]
    columns:
      - { name: id,   from: id,   type: string, primary_key: true }
      - { name: name, from: name, type: string }
  - name: user_deletes
    source_stream: all-events
    source_subject: all-events.user.deleted
    key_fields: [id]
    columns:
      - { name: id,     from: id,     type: string, primary_key: true }
      - { name: reason, from: reason, type: string }
```

**What it demonstrates:**
- Single stream, two views each watching different subjects
- `source_subject` defaults to `{stream}.>` when omitted
- Cross-view filtering by subject

Run: `cd examples/10-source-subject && go run .`

---

## Example 11: `consumer-tuning`

**Covers:** `max_ack_pending`, `max_deliver`, `ack_wait_seconds`, consumer behavior.

```yaml
# config.yaml
views:
  - name: fast_events
    source_stream: events
    key_fields: [id]
    columns:
      - { name: id, from: id, type: string, primary_key: true }
    consumer:
      max_ack_pending: 256
      max_deliver: 3
      ack_wait_seconds: 5
```

**What it demonstrates:**
- High-throughput consumer with 256 in-flight messages
- Only 3 delivery attempts before dead-letter
- Short 5-second ack wait for fast retry
- Contrast with default consumer settings
- Verify consumer config via `nats consumer info`

Run: `cd examples/11-consumer-tuning && go run .`

---

## Example 12: `http-options`

**Covers:** `WithHTTPServer()`, `WithQueryPort()`, custom HTTP binding, HTTP-only operation.

```yaml
# config.yaml
http:
  port: 9090
views:
  - name: items
    source_stream: items
    key_fields: [id]
    columns:
      - { name: id, from: id, type: string, primary_key: true }
```

**What it demonstrates:**
- `natsql.WithHTTPServer(":9090")` overrides default port
- `natsql.WithQueryPort(0)` lets OS pick a free port
- Engine without NATS query handler (nc nil) still serves HTTP
- `curl` against custom port
- Two concurrent engines on different ports

Run: `cd examples/12-http-options && go run .`

---

## Example 13: `bool-timestamp`

**Covers:** `boolean` and `timestamp` column types, type filtering.

```yaml
# config.yaml
views:
  - name: sessions
    source_stream: session-events
    key_fields: [token]
    columns:
      - { name: token,     from: token,     type: string,    primary_key: true }
      - { name: active,    from: active,    type: boolean }
      - { name: created,   from: created,   type: timestamp }
      - { name: user_id,   from: user_id,   type: string }
```

**What it demonstrates:**
- `WHERE active = true` / `WHERE active = false` on boolean column
- `WHERE created = '2025-01-01T00:00:00Z'` on timestamp column
- Boolean values serialize as JSON `true`/`false`
- Timestamps stored as strings, returned in query results

Run: `cd examples/13-bool-timestamp && go run .`

---

## Example 14: `yaml-vs-json`

**Covers:** JSON config file, `LoadConfig` dual-format support.

```json
{
  "views": [
    {
      "name": "users",
      "source_stream": "events",
      "key_fields": ["id"],
      "columns": [
        { "name": "id", "from": "id", "type": "string", "primary_key": true },
        { "name": "email", "from": "email", "type": "string" }
      ]
    }
  ]
}
```

**What it demonstrates:**
- Identical config expressed in JSON and YAML
- `LoadConfig("config.json")` auto-detects format by extension
- Both produce identical engine state
- Side-by-side comparison of two formats

Run: `cd examples/14-yaml-vs-json && go run .`

---

## Example 15: `autocreate-streams`

**Covers:** `--create-streams` CLI flag, external NATS mode, stream existence checks.

```
natsql server --config=config.yaml -u nats://localhost:4222 --create-streams
```

**What it demonstrates:**
- External NATS (no embedded server)
- `--create-streams` creates source streams that don't exist
- Skips streams that already exist (logs "already exists, skipping")
- Without `--create-streams`, only a warning is logged
- Contrast embedded mode (always creates) vs external (opt-in)

Run: Requires a running NATS server. `cd examples/15-autocreate-streams && ./run.sh`

---

## Example 16: `poll-for-ready`

**Covers:** Polling for materialization completeness, `.Results` count, `LIMIT` usage.

```go
// Poll pattern: wait until all events are visible via query
for {
    res := eng.Query(ctx, "SELECT * FROM orders WHERE order_id != ''")
    if res.Error == nil && len(res.Results) >= totalEvents {
        break
    }
    if time.Since(start) > 30*time.Second {
        log.Fatal("timeout waiting for materialization")
    }
    time.Sleep(200 * time.Millisecond)
}
```

**What it demonstrates:**
- Polling query loop until all rows appear
- Timeout guard to prevent infinite wait
- `!=` filter to count all rows (no true all-rows query in v1)
- `LIMIT` query: `SELECT * FROM orders WHERE order_id != '' LIMIT 5`

Run: `cd examples/16-poll-for-ready && go run .`

---

## Example 17: `dual-engine`

**Covers:** Two independent engines in one process, port isolation.

```go
eng1, _ := natsql.NewEmbedded(cfg1, natsql.WithQueryPort(8081))
eng2, _ := natsql.NewEmbedded(cfg2, natsql.WithQueryPort(8082))
```

**What it demonstrates:**
- Two embedded NATS servers in one process
- Each engine has its own KV namespace (because servers are separate)
- Port isolation: HTTP queries go to different ports
- Independent Start/Close lifecycles
- Useful for multi-tenant or shard-by-customer scenarios

Run: `cd examples/17-dual-engine && go run .`

---

## Example 18: `nats-request`

**Covers:** Pure NATS client query, raw NATS request-reply, no HTTP.

```bash
nats req natsql.query "SELECT * FROM users WHERE user_id = 'abc123'"
```

```go
// Go client using nats.Conn
nc.Request("natsql.query", []byte("SELECT * FROM users WHERE id = 'x'"), 5*time.Second)
```

**What it demonstrates:**
- Query via `nats req` CLI tool
- Raw NATS `Request()` from Go
- JSON response envelope parsing
- No HTTP server needed (optional)

Run: Requires `nats` CLI. `cd examples/18-nats-request && ./run.sh`

---

## Example 19: `custom-logger`

**Covers:** Structured logging with `slog`, `WithLogger()`, log levels.

```go
handler := slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug})
logger := slog.New(handler)
eng, _ := natsql.NewEmbedded(cfg, natsql.WithLogger(logger))
```

**What it demonstrates:**
- Custom `slog.Logger` with debug level
- Materializer heartbeat logs
- Consumer setup logs
- Query execution logs
- JSON log handler alternative
- Discarding logs (`io.Discard`) for benchmarking

Run: `cd examples/19-custom-logger && go run .`
