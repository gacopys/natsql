# natsql

## What This Is

A NATS-native materialized view engine. Define stream-to-KV materializations declaratively (YAML/JSON), and query the resulting state with read-only SQL via NATS request-reply or HTTP. Write events to JetStreams, get queryable state — no database other than NATS.

For NATS developers building event-driven systems who need simple queryable state without running Postgres, Redis, or Kafka alongside their NATS cluster.

## Core Value

A developer can define a materialized view from a stream, publish events, and query the current state with `SELECT ... WHERE ...` — zero infrastructure beyond NATS.

## Requirements

### Validated

(None yet — ship to validate)

### Active

- [ ] **DECL-01**: User can define a materialized view in a YAML/JSON config (source stream, key, column mapping, index defs)
- [ ] **STREAM-01**: Materializer consumes JetStream events (ordered, durable) and maintains KV bucket state
- [ ] **STREAM-02**: Materializer recovers from crash — durable consumer resumes from last ack
- [ ] **QUERY-01**: User can query KV state with `SELECT ... WHERE <col> = <val>` (exact match via index or PK)
- [ ] **QUERY-02**: User can query KV state with `SELECT ... WHERE <col> > <val> / <col> < <val>` (range scan)
- [ ] **QUERY-03**: `LIMIT` support on query results
- [ ] **QUERY-04**: Query via NATS request-reply (`nc.Request("natsql.query", sqlBytes)`)
- [ ] **QUERY-05**: Query via HTTP/JSON API
- [ ] **INDEX-01**: Secondary indexes on materialized views (single-column equality + range)
- [ ] **EMBED-01**: Go library importable and usable from within a Go process
- [ ] **EMBED-02**: Standalone server binary with HTTP + NATS query endpoints
- [ ] **EMBED-03**: Works with embedded NATS (single-node or 3-node cluster)
- [ ] **RESIL-01**: Graceful handling of malformed events (log + skip, don't crash)

### Out of Scope

- DML (INSERT/UPDATE/DELETE via SQL) — writes only happen through stream messages
- Multi-table JOINs in v1 — deferred
- Complex transactions / serializable isolation
- Full pgwire / PostgreSQL protocol compatibility
- Aggregations (GROUP BY, COUNT, AVG) in v1 — deferred
- Subqueries, window functions, CTEs
- Schema migration / ALTER VIEW with backfill — requires full re-materialize
- ebind integration — independent project (ebind is reference inspiration only)

## Context

This project was born from a conversation about why full PostgreSQL protocol on NATS is a bad bet — and what a realistic, valuable SQL layer on NATS would look like. The answer: SQL as a read-only query layer over JetStream KV materializations, targeting NATS developers who want queryable state without adding a second infrastructure dependency.

The project lives alongside ebind (a task queue + DAG workflow engine on NATS) in the same mono-repo. ebind demonstrates the pattern of "stream → KV" materialization for workflow state, which natsql generalizes into a configurable engine. ebind is read-only reference code — natsql is independent.

## Constraints

- **Infrastructure**: Zero external dependencies beyond NATS JetStream 2.8+
- **Language**: Go 1.22+
- **Storage**: All state in NATS JetStream streams (changelog) and KV buckets (snapshot)
- **Consistency**: CAS-based (read-committed), not serializable
- **SQL dialect**: Minimal v1 — no JOINs, no aggregations, no subqueries
- **Deployment modes**: Both Go library (embed) and standalone binary

## Key Decisions

| Decision | Rationale | Outcome |
|----------|-----------|---------|
| YAML/JSON config for schema definition | Works for both Go embed and standalone ops; trivial to parse; no DDL parser needed for v1 | — Pending |
| Read-only SQL (SELECT) | Writes go through streams; SQL is the read path. Separates concerns cleanly | — Pending |
| NATS request-reply + HTTP interfaces | NATS-native for app code, HTTP for tooling/curl | — Pending |
| Independent from ebind | ebind is reference inspiration, not a dependency. Keeps surface area minimal | — Pending |
| v1 = functional prototype | Prove the concept works end-to-end before hardening | — Pending |
| Minimal SQL (eq + range + LIMIT) | Covers the 90% use case for KV-backed state queries | — Pending |

---
*Last updated: 2026-05-22 after project initialization*
