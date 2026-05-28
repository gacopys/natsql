# 03 — Malformed Events + DLQ

Demonstrates error handling: valid events materialize, invalid ones go to the dead letter queue.

## Run

```bash
go run .
```

## What you'll see

Four types of events are published:
1. **Valid JSON** → materializes to KV, queryable via SQL
2. **Invalid JSON** → DLQ'd with envelope containing original bytes + error
3. **Type mismatch** (string instead of number) → DLQ'd
4. **Missing key field** → DLQ'd

The DLQ stream (`natsql-dlq`) stores rejected events with metadata for inspection.
