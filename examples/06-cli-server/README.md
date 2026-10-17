# 06 — CLI Server

Demonstrates the `natsql server` standalone binary.

## Run everything

```bash
./smoke.sh
```

It builds the binary, starts it with `config.yaml`, runs the Go client in `client/` against it (publish events over NATS, query over HTTP and NATS, check every result), and stops the server with SIGTERM. It exits non-zero if a result is wrong or the server doesn't shut down cleanly. CI runs it the same way. The steps below do the same by hand.

## Prerequisites

Build the natsql binary first:
```bash
cd ../.. && go build -o examples/06-cli-server/natsql-server ./cmd/natsql/
```

## Run (embedded NATS — zero infrastructure)

```bash
./natsql-server server --config=config.yaml --embedded
```

(From repo root: `go run ./cmd/natsql/ server --config=examples/06-cli-server/config.yaml --embedded`)

The server starts with embedded NATS on port 4223 (`nats.port` in `config.yaml`) and HTTP on port 8080. In embedded mode it creates the `events` and `inventory` source streams.

## Publish test events

In another terminal, run the Go client (`cd client && go run .`), which publishes these events and checks the query results, or publish them with the NATS CLI:

```bash
# Install NATS CLI: https://github.com/nats-io/natscli
nats pub events.user-created '{"id": "u1", "name": "Alice", "email": "alice@test.com", "age": 30}'

# Publish to inventory stream
nats pub inventory.product-added '{"sku": "SKU-001", "title": "Widget", "price": 9.99, "stock": true}'
```

## Query via HTTP

```bash
# Query users
curl -X POST http://127.0.0.1:8080/api/v1/query \
  -H 'Content-Type: application/json' \
  -d '{"sql": "SELECT * FROM users WHERE user_id = '\''u1'\''"}'

# Query products
curl -X POST http://127.0.0.1:8080/api/v1/query \
  -H 'Content-Type: application/json' \
  -d '{"sql": "SELECT title, price FROM products WHERE sku = '\''SKU-001'\''"}'

# Try an error — unknown view
curl -X POST http://127.0.0.1:8080/api/v1/query \
  -H 'Content-Type: application/json' \
  -d '{"sql": "SELECT * FROM ghost WHERE id = '\''x'\''"}'
```

## Key features demonstrated

- Config-driven view definitions (YAML)
- Embedded NATS mode (no infrastructure)
- HTTP query API at `/api/v1/query`
- Graceful shutdown on Ctrl+C
- Multiple views in one server
