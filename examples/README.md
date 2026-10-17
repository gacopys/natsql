# natsql Examples

Hands-on examples that demonstrate every natsql use case. Each example is a standalone Go program in its own directory — just `cd` and `go run .`

Every example checks its own results and exits non-zero if one is wrong, so they double as end-to-end tests. CI runs all of them through `run.sh`:

```bash
examples/run.sh                     # every example (also: make examples-run)
examples/run.sh 03-malformed-events # one example
```

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

# 6. Standalone binary with config file: builds natsql, starts it, runs a client against it
cd ../06-cli-server && ./smoke.sh   # see README.md to run the steps by hand
```

## Examples Overview

| # | Name | What it shows |
|---|------|---------------|
| 01 | hello-natsql | Embedded NATS, define view, publish event, SQL + HTTP query, stats |
| 02 | composite-keys | Multi-field PK (`key_fields`), dot-path column mapping (`from: "org.id"`) |
| 03 | malformed-events | Invalid JSON → DLQ, type mismatch → DLQ, missing key → DLQ |
| 04 | multiple-views | Two views in one engine, cross-view isolation, error messages |
| 05 | library-embed | `natsql.New()` and `natsql.NewWithNATS()` for embedding in Go apps |
| 06 | cli-server | `natsql server --config=config.yaml` standalone binary, with a plain NATS + HTTP client |
| 07 | perf-benchmark | Query latency by plan type; `-events=N` sets the dataset size (default 1,000,000) |
| 08 | where-operators | `WHERE IN`, `WHERE !=`, full scans, PK `IN` lookups |
| 09 | key-separator | Custom `key_separator`; key parts containing it stay distinct |
| 10 | source-subject | `source_subject` filters, two views on one stream |
| 11 | consumer-tuning | `max_ack_pending`, `max_deliver`, `ack_wait_seconds`, per-view counters |
| 12 | http-options | `WithQueryPort`, `WithHTTPServer(":0")`, `Stats().HTTPAddr`, `/readyz` |
| 13 | bool-timestamp | `boolean` and `timestamp` column types |
| 14 | yaml-vs-json | YAML file, JSON file and Go struct configs are equivalent |
| 15 | autocreate-streams | Persistent `store_dir`, restart on existing streams, missing-stream error |
| 16 | poll-for-ready | Polling for materialization, `LIMIT` |
| 17 | dual-engine | Two independent engines in one process |
| 18 | nats-request | Queries over NATS request-reply |
| 19 | custom-logger | `WithLogger` with `slog` handlers, including the embedded NATS server's logs |

## Architecture

```
event → JetStream stream → Materializer → KV bucket → SQL query → JSON result
                              │                           │
                              └── malformed → DLQ stream   └── NATS / HTTP
```

All examples use `natsql.NewEmbedded()` (zero infrastructure) unless noted.
