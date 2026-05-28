# 02 — Composite Keys + Nested JSON

Demonstrates multi-field primary keys and dot-path column mapping.

## Run

```bash
go run .
```

## What you'll see

Events with nested JSON like `{"org": {"id": "acme"}, "order": {"id": "ord-1", ...}}` are mapped to flat columns using dot paths (`from: "org.id"`). The composite key `key_fields: [org_id, order_id]` with separator `:` produces KV keys like `orders/acme:ord-1`.
