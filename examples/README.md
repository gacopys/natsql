# natsql Examples

Hands-on examples that demonstrate every natsql use case. Each example is a standalone Go program in its own directory — just `cd` and `go run .`

## Quick Start

```bash
# 1. Simplest demo — embedded NATS, one view, SQL + HTTP queries
cd 01-hello-natsql && go run .

# 2. Composite keys + nested JSON path mapping
cd ../02-composite-keys && go run .

# 3. Error handling — malformed events → DLQ
cd ../03-malformed-events && go run .

# 4. Multiple views in one engine
cd ../04-multiple-views && go run .

# 5. Embed natsql in your own Go app
cd ../05-library-embed && go run .

# 6. Standalone binary with config file
cd ../06-cli-server && # see README.md
```

## Examples Overview

| # | Name | What it shows |
|---|------|---------------|
| 01 | hello-natsql | Embedded NATS, define view, publish event, SQL + HTTP query, stats |
| 02 | composite-keys | Multi-field PK (`key_fields`), dot-path column mapping (`from: "org.id"`) |
| 03 | malformed-events | Invalid JSON → DLQ, type mismatch → DLQ, missing key → DLQ |
| 04 | multiple-views | Two views in one engine, cross-view isolation, error messages |
| 05 | library-embed | `natsql.New()` and `natsql.NewWithNATS()` for embedding in Go apps |
| 06 | cli-server | `natsql server --config=config.yaml --embedded` standalone binary |

## Architecture

```
event → JetStream stream → Materializer → KV bucket → SQL query → JSON result
                              │                           │
                              └── malformed → DLQ stream   └── NATS / HTTP
```

All examples use `natsql.NewEmbedded()` (zero infrastructure) unless noted.
