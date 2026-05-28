# 06 — CLI Server

Demonstrates the `natsql server` standalone binary.

## Prerequisites

Build the natsql binary first:
```bash
cd ../../natsql && go build -o ../natsql-server ./cmd/natsql/
```

## Run (embedded NATS — zero infrastructure)

```bash
./natsql-server server --config=config.yaml --embedded
```

The server starts with embedded NATS on port 4222 and HTTP on port 8080.

## Publish test events

In another terminal, use the NATS CLI or Go code to publish events:

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
