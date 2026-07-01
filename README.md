# NATSQL — The NATS-Native Materialized View Engine

[![Lint](https://github.com/gacopys/natsql/actions/workflows/lint.yml/badge.svg?branch=master)](https://github.com/gacopys/natsql/actions/workflows/lint.yml)
[![Vulnerability](https://github.com/gacopys/natsql/actions/workflows/vulnerability.yml/badge.svg?branch=master)](https://github.com/gacopys/natsql/actions/workflows/vulnerability.yml)
[![Build](https://github.com/gacopys/natsql/actions/workflows/build.yml/badge.svg?branch=master)](https://github.com/gacopys/natsql/actions/workflows/build.yml)
[![Test](https://github.com/gacopys/natsql/actions/workflows/test.yml/badge.svg?branch=master)](https://github.com/gacopys/natsql/actions/workflows/test.yml)
[![Examples](https://github.com/gacopys/natsql/actions/workflows/examples.yml/badge.svg?branch=master)](https://github.com/gacopys/natsql/actions/workflows/examples.yml)
[![Cyclomatic Complexity](https://github.com/gacopys/natsql/actions/workflows/cyclo.yml/badge.svg?branch=master)](https://github.com/gacopys/natsql/actions/workflows/cyclo.yml)
[![Code Duplication](https://github.com/gacopys/natsql/actions/workflows/dupl.yml/badge.svg?branch=master)](https://github.com/gacopys/natsql/actions/workflows/dupl.yml)

[![License](https://img.shields.io/github/license/gacopys/natsql)](LICENSE)
[![Go Version](https://img.shields.io/github/go-mod/go-version/gacopys/natsql)](go.mod)
[![Latest tag](https://img.shields.io/github/v/tag/gacopys/natsql)](https://github.com/gacopys/natsql/tags)
[![GitHub stars](https://img.shields.io/github/stars/gacopys/natsql?style=social)](https://github.com/gacopys/natsql)
[![Buy Me a Coffee](https://img.shields.io/badge/Buy%20Me%20a%20Coffee-donate-yellow.svg)](https://buymeacoffee.com/gacopys)
[![PayPal](https://img.shields.io/badge/PayPal-donate-blue.svg)](https://paypal.me/gacopys)

> Query your NATS JetStream state with SQL. Zero infrastructure beyond NATS.

```
event → JetStream stream → Materializer → KV bucket → SQL query → JSON result
                              │                           │
                              └── malformed → DLQ stream   └── NATS / HTTP / Go
```

natsql lets you define materialized views over JetStream streams using a simple YAML or JSON config, then query the resulting state with `SELECT ... WHERE ...` — no Postgres, no Redis, no Kafka. Just NATS.

**Give a developer a stream, and they'll build event-driven systems. Give them a materialized view engine, and they'll query their state.**

---

## Why natsql?

If you're building on NATS JetStream, you already have ordering, persistence, and replay. But you don't have queryable state — every read requires scanning a stream's messages, maintaining your own snapshot, or bolting on a second database.

**natsql is that second database, except it's not a database at all.** It's a thin SQL query layer over NATS KV, fed by a configurable materializer that consumes your streams. The result: you can `SELECT` from your event stream state the same way you'd query a table.

### Who is this for?

- **NATS developers** who need simple queryable state without leaving the NATS ecosystem
- **Event-driven system builders** who want to materialize a stream into a KV snapshot and query it
- **Team leads** looking to reduce infrastructure surface area — one less Postgres/Redis cluster
- **Anyone prototyping** — embedded NATS means you can be up in 60 seconds with zero dependencies

---

## Quick Start

```bash
# Try it with the hello-natsql example (embedded NATS, one view)
cd examples/01-hello-natsql && go run .
```

That's it. No Docker, no databases, no config. The example starts an embedded NATS server, creates a materialized view, publishes an event, and queries it via SQL and HTTP.

```go
// Minimal example — 6 lines of config, one query
cfg := &natsql.Config{
    Views: []natsql.ViewConfig{{
        Name:         "users",
        SourceStream: "events",
        KeyFields:    []string{"user_id"},
        Columns: []natsql.ColumnConfig{
            {Name: "user_id", From: "id", Type: natsql.ColumnTypeString, PrimaryKey: true},
            {Name: "name", From: "name", Type: natsql.ColumnTypeString},
            {Name: "email", From: "email", Type: natsql.ColumnTypeString},
        },
    }},
}

eng, _ := natsql.NewEmbedded(cfg)
// ... create stream, publish events, then:
res := eng.Query(ctx, "SELECT * FROM users WHERE user_id = 'abc123'")
// res.Results → [{"user_id": "abc123", "name": "Alice", "email": "alice@example.com"}]
```

---

## How It Works

### 1. Define Views

```yaml
# config.yaml
views:
  - name: users
    source_stream: user-events
    key_fields:
      - user_id
    columns:
      - name: user_id
        from: id
        type: string
        primary_key: true
      - name: name
        from: name
        type: string
      - name: email
        from: email
        type: string
      - name: age
        from: age
        type: number
```

### 2. Publish Events

Events are JSON payloads published to a JetStream stream. The materializer consumes them in order and updates the KV bucket snapshot.

```json
{"id": "abc123", "name": "Alice", "email": "alice@example.com", "age": 30}
```

### 3. Query State

```sql
SELECT * FROM users WHERE user_id = 'abc123'
SELECT name, email FROM users WHERE user_id = 'abc123'
```

Query results are returned as typed JSON — exactly what you'd expect.

---

## Deployment Modes

### A) Embedded NATS (zero infrastructure)

```go
eng, err := natsql.NewEmbedded(cfg)
```

Starts a NATS JetStream server in-process. No external NATS required. Perfect for development, testing, single-node deployments, and appliances.

### B) Go Library Embed

```go
// You manage NATS, pass your JetStream context
eng, err := natsql.New(js, cfg)

// Or pass your NATS connection, engine creates JetStream
eng, err := natsql.NewWithNATS(nc, cfg)
```

Import natsql into your existing Go application. Works with your existing NATS connection and JetStream context.

### C) Standalone CLI Server

```bash
natsql server --config=config.yaml --embedded
natsql server --config=config.yaml -u nats://my-cluster:4222
```

The `natsql` binary provides a full server with HTTP + NATS query endpoints, graceful shutdown, and config-driven setup.

---

## Query APIs

All three APIs return the identical JSON result envelope: `{"results": [...], "error": null}`.

### HTTP

```bash
curl -X POST http://127.0.0.1:8080/api/v1/query \
  -H 'Content-Type: application/json' \
  -d '{"sql": "SELECT * FROM users WHERE user_id = '\''abc123'\''"}'
```

### NATS Request-Reply

```bash
nats req natsql.query "SELECT * FROM users WHERE user_id = 'abc123'"
```

### In-Process Go

```go
res := eng.Query(ctx, "SELECT * FROM users WHERE user_id = 'abc123'")
```

---

## SQL Dialect

natsql speaks a minimal, read-only SQL dialect curated for the 90% use case:

```sql
SELECT [column1, column2, ... | *]
FROM view_name
[WHERE condition [AND condition ...]]
[LIMIT n]
```

**Supported:**

- `SELECT *` and explicit column projection
- `WHERE col = 'val'` (equality)
- `WHERE col != 'val'` (not-equal)
- `WHERE col IN ('a', 'b', 'c')` (value list)
- Multiple conditions joined with `AND`
- `LIMIT n`
- String, integer, float, boolean, and `NULL` literals
- Typed JSON output (numbers stay numbers, booleans stay booleans)
- Dot-path field notation (`org.id`), optional `$.` prefix

**Not supported:**

- `OR`, range operators (`>`, `<`, `>=`, `<=`), `BETWEEN`, `LIKE`
- `ORDER BY`, `GROUP BY`, `HAVING`, `DISTINCT`
- `JOIN`, subqueries, aggregations
- DML (`INSERT`/`UPDATE`/`DELETE`) — writes happen exclusively through streams

> **Full SQL spec:** See [`SQL_DIALECT.md`](SQL_DIALECT.md) for the complete dialect reference, including unsupported constructs and deferred features.

---

## Configuration

Views are defined declaratively in YAML or JSON:

```yaml
views:
  - name: orders
    source_stream: order-events
    key_fields:
      - org_id
      - order_id
    key_separator: "-"
    columns:
      - name: org_id
        from: org.id
        type: string
        primary_key: true
      - name: order_id
        from: order.id
        type: string
        primary_key: true
      - name: total
        from: order.total
        type: number
      - name: status
        from: order.status
        type: string
```

### Column Types

| Type | JSON Source |
|------|-------------|
| `string` | `"alice"` |
| `number` | `42`, `3.14` |
| `boolean` | `true`, `false` |
| `timestamp` | ISO 8601 string |

Column mapping uses dot-path notation (`org.id`) for nested JSON fields. The `$.` prefix is optional (`$.user.id` and `user.id` are equivalent).

---

## Examples

Hands-on examples in `examples/` — each is a standalone Go program:

| # | Example | What It Shows |
|---|---------|---------------|
| 01 | [hello-natsql](examples/01-hello-natsql) | Embedded NATS, one view, SQL + HTTP query |
| 02 | [composite-keys](examples/02-composite-keys) | Multi-field PK, nested JSON path mapping |
| 03 | [malformed-events](examples/03-malformed-events) | Invalid JSON → DLQ, type mismatch → DLQ |
| 04 | [multiple-views](examples/04-multiple-views) | Two views, cross-view isolation |
| 05 | [library-embed](examples/05-library-embed) | Embedding in Go apps (`New` / `NewWithNATS`) |
| 06 | [cli-server](examples/06-cli-server) | Standalone binary with config file |
| 07 | [perf-benchmark](examples/07-perf-benchmark) | High-throughput pub/sub, query latency benchmarks |
| 08 | [where-operators](examples/08-where-operators) | `WHERE IN`, `WHERE !=`, full table scan |
| 09 | [key-separator](examples/09-key-separator) | Custom `key_separator`, PK sanitization |
| 10 | [source-subject](examples/10-source-subject) | `source_subject` filter, subject routing |
| 11 | [consumer-tuning](examples/11-consumer-tuning) | `max_ack_pending`, `max_deliver`, retry config |
| 12 | [http-options](examples/12-http-options) | `WithQueryPort`, `WithHTTPServer`, custom port |
| 13 | [bool-timestamp](examples/13-bool-timestamp) | `boolean` and `timestamp` column types |
| 14 | [yaml-vs-json](examples/14-yaml-vs-json) | JSON config file, dual-format `LoadConfig` |
| 15 | [autocreate-streams](examples/15-autocreate-streams) | `--create-streams` flag, stream existence |
| 16 | [poll-for-ready](examples/16-poll-for-ready) | Polling for materialization, `LIMIT` |
| 17 | [dual-engine](examples/17-dual-engine) | Two independent engines in one process |
| 18 | [nats-request](examples/18-nats-request) | Pure NATS request-reply, no HTTP |
| 19 | [custom-logger](examples/19-custom-logger) | `slog` structured logging, `WithLogger` |

```bash
cd examples/01-hello-natsql && go run .
```

---

## Architecture

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
                    └──────────────┘                  │  In-Process Go   │
                                                      └──────────────────┘
```

- **Materializer**: Consumes a durable JetStream subscription, maps JSON events to KV mutations, writes to the KV bucket, and routes malformed events to the DLQ stream.
- **KV Bucket**: A single JetStream KV bucket (`natsql-views`) stores all materialized rows and view schemas. PK lookups are O(1) `Get` calls.
- **SQL Engine**: Parses queries with the vitess sqlparser, validates against the stored schema, builds a plan (PK lookup or full scan), and executes against the KV bucket.
- **Transport**: Routes queries from NATS request-reply, HTTP, or in-process Go calls through the same engine.

> **Full architecture reference:** See [`ARCHITECTURE.md`](ARCHITECTURE.md) for the authoritative component map, data flow, storage layout, lifecycle, invariants, and extension points.
>
> **API specification:** See [`openapi.yaml`](openapi.yaml) for the HTTP query endpoint — rendered interactively by GitHub.

---

## License

MIT — use it, ship it, build on it.
