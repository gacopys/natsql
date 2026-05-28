# Phase 1: Foundation — Materializer — Context

**Gathered:** 2026-05-28
**Status:** Ready for planning

<domain>
## Phase Boundary

Config-driven JetStream consumer that materializes events from a stream into a NATS KV bucket maintaining current row state. Covers MAT-01 through MAT-04: view definition (YAML), durable stream consumption with crash recovery, KV row state storage, and graceful malformed event handling.

</domain>

<decisions>
## Implementation Decisions

### Config Schema
- **D-01:** Full config from day one — view definition includes indexes, retry config, DLQ config, bucket config, and event mapping patterns upfront.
- **D-02:** Single YAML file supports multiple views (`views:` array).
- **D-03:** Column mapping uses simple JSON path notation (e.g., `user.id` → nested field).
- **D-04:** Composite keys supported via `key_fields: [field1, field2]` with configurable separator (default `|`).
- **D-05:** Indexes field included in config schema now (forward compatibility for Phase 2), but ignored by materializer.

### KV Key Schema
- **D-06:** Single KV bucket (`natsql-views`) for all views.
- **D-07:** Row key format: `{view_name}:{pk_value}` (e.g., `users:abc123`).
- **D-08:** View schema (column definitions and types) is stored in KV on startup at a known key (e.g., `schemas:{view_name}`) so the query engine can read it from KV without config file access.

### Stream Consumption
- **D-09:** Consumer configuration is explicit in view config (batch size, MaxDeliver, ack wait) rather than hardcoded defaults.
- **D-10:** Durable consumer named `natsql-{view_name}`.
- **D-11:** Ack-after-KV-write strategy — message is acked only after the KV put succeeds, providing at-least-once delivery. Idempotent PK overwrite handles replays.

### Error Handling
- **D-12:** Tiered malformed event handling:
  - Invalid JSON → DLQ
  - Missing key column → DLQ
  - Type mismatch → DLQ
  - Valid unknown columns → skip silently (log at debug level)
- **D-13:** DLQ stream (`natsql-dlq`) is auto-created by the materializer on startup if it does not exist.
- **D-14:** Persistent/structural errors (schema mismatch, new event format) → DLQ. Never stall the consumer.

### Schema Representation
- **D-15:** Column types specified in config — `string`, `number`, `boolean`, `timestamp`.
- **D-16:** Strict type validation on ingestion — type mismatch results in DLQ, not coercion.
- **D-17:** Materializer enforces types during event processing (not deferred to query engine).

### Claude's Discretion
- Exact YAML field naming for config (e.g., `key_separator`, `batch_size` vs `batchSize`)
- Log format and verbosity levels
- DLQ message envelope format
- Metrics instrumentation approach for v1

</decisions>

<canonical_refs>
## Canonical References

### Project requirements
- `.planning/REQUIREMENTS.md` — MAT-01 through MAT-04 define Phase 1 requirements with acceptance criteria
- `.planning/PROJECT.md` — Project constraints, key decisions, architecture principles

### Research
- `.planning/research/SUMMARY.md` — Architecture approach, risk mitigation, recommended stack
- `.planning/research/STACK.md` — Technology stack decisions and rationale
- `.planning/research/ARCHITECTURE.md` — Deep architecture analysis
- `.planning/research/PITFALLS.md` — Known pitfalls and edge cases

</canonical_refs>

<code_context>
## Existing Code Insights

### Established Patterns
- Consumer lifecycle: `defer cc.Stop(); <-cc.Closed()` for clean goroutine management
- CAS-based state mutations with retry on revision mismatch
- JetStream consumer durable naming conventions

### Integration Points
- All Phase 1 code lives in a new `natsql/` package
- KV bucket `natsql-views` is the integration seam with Phase 2 (query engine reads from it)
- Schema stored at `schemas:{view_name}` is the contract between materializer and query engine

</code_context>

<specifics>
## Specific Ideas

- DLQ message envelope should include: original message bytes, view name, error reason, timestamp
- v1 should ship a bare `cmd/natsql/main.go` that loads config, starts materializer, and blocks on SIGINT
- Materializer should log a periodic "processed N events" heartbeat for operational visibility

</specifics>

<deferred>
## Deferred Ideas

- Per-view KV buckets — defer to v2 (MAT-07)
- Full rebuild/replay from source stream — defer to v2 (MAT-06)
- Config hot-reload (SIGHUP) — defer to v2 (OPS-03)
- Prometheus metrics — defer to v2 (OPS-01)

</deferred>

---

*Phase: 01-foundation-materializer*
*Context gathered: 2026-05-28*
