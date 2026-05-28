# 04 — Multiple Views

Two independent materialized views ("users" and "products") sharing one engine, one KV bucket.

## Run

```bash
go run .
```

## What you'll see

- Each view has its own schema and source stream
- Both materialize concurrently from their respective streams
- Queries are routed to the correct view by the `FROM` clause
- Unknown views/columns return clear errors (not panics)
