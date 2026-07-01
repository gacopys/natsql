# NATSQL — The NATS-Native Materialized View Engine

[![Lint](https://github.com/gacopys/natsql/actions/workflows/lint.yml/badge.svg?branch=master)](https://github.com/gacopys/natsql/actions/workflows/lint.yml)
[![Vulnerability](https://github.com/gacopys/natsql/actions/workflows/vulnerability.yml/badge.svg?branch=master)](https://github.com/gacopys/natsql/actions/workflows/vulnerability.yml)
[![Build](https://github.com/gacopys/natsql/actions/workflows/build.yml/badge.svg?branch=master)](https://github.com/gacopys/natsql/actions/workflows/build.yml)
[![Test](https://github.com/gacopys/natsql/actions/workflows/test.yml/badge.svg?branch=master)](https://github.com/gacopys/natsql/actions/workflows/test.yml)
[![Examples](https://github.com/gacopys/natsql/actions/workflows/examples.yml/badge.svg?branch=master)](https://github.com/gacopys/natsql/actions/workflows/examples.yml)

**Query your NATS JetStream state with SQL. Zero infrastructure beyond NATS.**

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

## Features

| Feature | Status |
|---------|--------|
| Materialize JetStream events → KV bucket snapshots | **Shipped** |
| Query via `SELECT ... WHERE` with PK lookup | **Shipped** |
| Read-only SQL using standard syntax | **Shipped** |
| Query via NATS request-reply (`natsql.query`) | **Shipped** |
| Query via HTTP JSON API (`POST /api/v1/query`) | **Shipped** |
| Config-driven view definitions (YAML/JSON) | **Shipped** |
| Go library embed (`natsql.New`, `NewWithNATS`, `NewEmbedded`) | **Shipped** |
| Standalone CLI binary (`natsql server`) | **Shipped** |
| Embedded NATS server (zero infrastructure) | **Shipped** |
| Composite primary keys | **Shipped** |
| Column projection (`SELECT col1, col2`) | **Shipped** |
| Malformed event handling → DLQ | **Shipped** |
| Durable consumers (crash recovery) | **Shipped** |
| Graceful shutdown with consumer drain | **Shipped** |
| Full scan queries (non-PK WHERE) | **Shipped** |
| Range scans (`>`, `<`) | Planned |
| Secondary indexes | Planned |
| LIMIT support | **Shipped** |
| `.` / `$.` prefix notation in field paths | **Shipped** |

### Known Limitations

- **Delete/Tombstone semantics:** Rows cannot be deleted from the materialized view once written. This is a deliberate v1 omission — a delete mode (operation field, subject convention, or tombstone predicate) is planned for v2. See `internal/kv/kv.go` package docs for details.
- **Range scans:** `WHERE` with `>` or `<` operators performs a client-side filter over a full table scan. Dedicated index-backed range scans are planned for v2.
- **Secondary indexes:** Only primary-key columns are indexed. Queries on non-PK columns perform a full table scan.

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
SELECT name, email FROM users WHERE age > 25   -- (coming soon)
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

## SQL Dialect

natsql speaks a minimal, read-only SQL dialect curated for the 90% use case:

| Feature | Supported | Example |
|---------|-----------|---------|
| `SELECT *` | ✓ | `SELECT * FROM users WHERE id = 'x'` |
| Column projection | ✓ | `SELECT name, email FROM users WHERE id = 'x'` |
| `WHERE` with `=` | ✓ | `WHERE user_id = 'abc'` |
| `WHERE` with `IN` | ✓ | `WHERE status IN ('active', 'pending')` |
| `WHERE` with `!=` | ✓ | `WHERE status != 'deleted'` |
| AND conditions | ✓ | `WHERE org_id = 'acme' AND order_id = 'ord-1'` |
| OR conditions | ✗ v1 | Planned |
| Range scans | ✗ v1 | Planned (next) |
| LIMIT | ✗ v1 | Planned (next) |
| JOINs | ✗ v1 | Deferred |
| Aggregations | ✗ v1 | Deferred |
| DML (INSERT/UPDATE/DELETE) | ✗ | Writes only through streams |

---

## Query APIs

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

All three APIs return identical JSON result envelopes.

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

Column mapping uses dot-path notation (`org.id`) for nested JSON fields.

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

```bash
cd examples/01-hello-natsql && go run .
```

---

## Architecture (30-second version)

```
┌─────────────┐     ┌──────────────┐     ┌────────────┐     ┌───────────┐
│ JetStream   │────▶│ Materializer │────▶│ KV Bucket  │────▶│ SQL       │
│ Stream      │     │ (consumer +  │     │ (JetStream │     │ Engine    │
│ (changelog) │     │  mapper +    │     │  KV store) │     │ (vitess   │
│             │     │  writer)     │     │ (snapshot) │     │  parser)  │
└─────────────┘     └──────────────┘     └────────────┘     └─────┬─────┘
                          │                                        │
                          ▼                                        ▼
                   ┌──────────────┐                    ┌──────────────────┐
                   │  DLQ Stream  │                    │  Transport Layer│
                   │  (bad events)│                    │  NATS / HTTP /   │
                   └──────────────┘                    │  In-Process Go   │
                                                       └──────────────────┘
```

- **Materializer**: Consumes a durable JetStream subscription, maps JSON events to KV mutations, writes to the KV bucket, and sends malformed events to the DLQ stream.
- **KV Bucket**: A single JetStream KV bucket (`natsql-views`) stores all materialized rows and view schemas. PK lookups are O(1) `Get` calls.
- **SQL Engine**: Parses queries with vitess sqlparser, validates against the stored schema, builds a plan (PK lookup or full scan), and executes against the KV bucket.
- **Transport**: Routes queries from NATS request-reply, HTTP, or in-process Go calls through the same engine.

---

## Project Status

**natsql v1.0 — shipped May 2026.** 7,300+ lines of Go across 29 source files. The core concept is proven: materialize streams to KV and query with SQL — all on NATS, zero external infrastructure.

The next milestone adds range scans, LIMIT, and secondary indexes.

---

## License

MIT — use it, ship it, build on it.
