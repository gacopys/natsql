# natsql

## What This Is

A NATS-native materialized view engine — **shipped v1.0**. Define stream-to-KV materializations declaratively (YAML/JSON), and query the resulting state with read-only SQL via NATS request-reply, HTTP, or in-process Go calls. Write events to JetStreams, get queryable state — no database other than NATS.

For NATS developers building event-driven systems who need simple queryable state without running Postgres, Redis, or Kafka alongside their NATS cluster.

## Core Value

A developer can define a materialized view from a stream, publish events, and query the current state with `SELECT ... WHERE ...` — zero infrastructure beyond NATS.

## Requirements

### Validated

- ✓ **DECL-01**: User can define a materialized view in a YAML/JSON config — v1.0
- ✓ **STREAM-01**: Materializer consumes JetStream events (ordered, durable) and maintains KV bucket state — v1.0
- ✓ **STREAM-02**: Materializer recovers from crash — durable consumer resumes from last ack — v1.0
- ✓ **QUERY-01**: User can query KV state with `SELECT ... WHERE <col> = <val>` (PK lookup) — v1.0
- ✓ **QUERY-04**: Query via NATS request-reply (`nc.Request("natsql.query", sqlBytes)`) — v1.0
- ✓ **QUERY-05**: Query via HTTP/JSON API — v1.0
- ✓ **EMBED-01**: Go library importable and usable from within a Go process — v1.0
- ✓ **EMBED-02**: Standalone server binary with HTTP + NATS query endpoints — v1.0
- ✓ **EMBED-03**: Works with embedded NATS (single-node) — v1.0
- ✓ **RESIL-01**: Graceful handling of malformed events (log + skip, don't crash) — v1.0

### Active (Next Milestone)

- [ ] **QUERY-02**: User can query KV state with `SELECT ... WHERE <col> > <val> / <col> < <val>` (range scan)
- [ ] **QUERY-03**: `LIMIT` support on query results
- [ ] **INDEX-01**: Secondary indexes on materialized views (single-column equality + range)

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

The project lives alongside ebind (a task queue + DAG workflow engine on NATS) in the same mono-repo. ebind demonstrates the pattern of "stream → KV" materialization for workflow state, which natsql generalizes into a configurable engine. natsql is independent from ebind — no code imports.

## Current State (v1.0)

Shipped **2026-05-28** with 7,339 LOC across 29 Go source files.

**Architecture:** 3-component model — Materializer (stream→KV), Query Engine (SQL→KV reads), Transport (NATS/HTTP/Embed).

**Tech stack:** Go, NATS JetStream KV, vitess sqlparser, chi HTTP router, Cobra CLI.

**Deployment modes:** Go library (import natsql), standalone binary with embedded NATS, standalone binary connecting to external NATS cluster.

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
| YAML/JSON config for schema definition | Works for both Go embed and standalone ops; trivial to parse; no DDL parser needed for v1 | ✓ Good |
| Read-only SQL (SELECT) | Writes go through streams; SQL is the read path. Separates concerns cleanly | ✓ Good |
| NATS request-reply + HTTP interfaces | NATS-native for app code, HTTP for tooling/curl | ✓ Good |
| Independent from ebind | ebind is reference inspiration, not a dependency. Keeps surface area minimal | ✓ Good |
| v1 = functional prototype | Prove the concept works end-to-end before hardening | ✓ Good |
| Minimal SQL (eq + range + LIMIT) | Covers the 90% use case for KV-backed state queries | ⚠️ Revisit: range scans deferred to v2 |
| vitess sqlparser | Battle-tested SQL parser, handles all edge cases | ✓ Good |
| Single KV bucket | Simpler to manage, fewer NATS resources | ✓ Good |
| Cobra CLI | Already in monorepo, NATS ecosystem standard | ✓ Good |

---
*Last updated: 2026-05-28 after v1.0 milestone*
