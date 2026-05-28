# 05 — Library Embed

Demonstrates two patterns for embedding natsql inside your own Go application.

## Run

```bash
go run .
```

## Patterns

**Pattern A — `natsql.New(js, cfg)`:** You already manage a NATS connection and JetStream context. You pass the JS handle to natsql. You own cleanup.

**Pattern B — `natsql.NewWithNATS(nc, cfg)`:** You have a NATS connection but want natsql to manage JetStream setup. The engine handles `nc.Close()` on shutdown.

Both patterns are useful when natsql is one component in a larger NATS-based application.
