# 01 — Hello natsql

The simplest possible natsql example. Zero infrastructure — runs entirely in-process.

## Run

```bash
go run .
```

## What you'll see

```
✓ Engine started with embedded NATS
✓ Published event: {"id": "abc123", "name": "Alice", ...}
✓ SQL query result: [{...}]
✓ HTTP query result: [{...}]
✓ Engine stats: started=true views=1 goroutines=... http=true
✅ All done! natsql works end-to-end with zero infrastructure.
```

## Concepts demonstrated

- `natsql.NewEmbedded()` — start engine with embedded NATS
- `eng.Start()` / `eng.Close()` — lifecycle
- Publishing events to a JetStream stream
- `eng.Query(sql)` — query via Go API
- HTTP query at `POST /api/v1/query`
- `eng.Stats()` — operational metrics
- `eng.NC()` — access the underlying NATS connection
