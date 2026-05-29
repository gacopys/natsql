# natsql

## What This Is

A NATS-native materialized view engine — **shipped v1.1**. Define stream-to-KV materializations declaratively (YAML/JSON), and query the resulting state with read-only SQL via NATS request-reply, HTTP, or in-process Go calls. Write events to JetStreams, get queryable state — no database other than NATS. Now hardened with type-safe query filters, PK sanitization, DLQ failure propagation, and CI-backed testing.

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
- ✓ **FIX-ENG-01**: PK equality query applies non-PK WHERE conditions as post-filter — v1.1
- ✓ **FIX-ENG-02**: Engine.Query is safe for concurrent access (data race fixed) — v1.1
- ✓ **FIX-ENG-03**: filterRow uses type-aware comparison instead of fmt.Sprint — v1.1
- ✓ **FIX-ENG-04**: SQL parser accepts boolean literals (true/false) in WHERE — v1.1
- ✓ **FIX-MAT-01**: PK values are sanitized against KV key special characters — v1.1
- ✓ **FIX-MAT-02**: DLQ publish failures are surfaced and block event acknowledgement — v1.1
- ✓ **FIX-MAT-03**: Engine.Start partial-init path cleans up correctly on failure — v1.1
- ✓ **FIX-MAT-04**: JSON integer values >2^53 preserve full precision — v1.1
- ✓ **FIX-TRN-01**: HTTP server has read/write/idle timeouts configured — v1.1
- ✓ **FIX-TRN-02**: HTTP query endpoint enforces request body size limit — v1.1
- ✓ **FIX-TRN-03**: NATS query handler uses bounded context with timeout — v1.1
- ✓ **FIX-TRN-04**: Dead code, unused parameters, and test flakiness cleaned up — v1.1

### Active

(Next milestone — to be defined)

### Deferred (Future Milestones)

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
- External project integration — each project maintains independence

## Context

This project was born from a conversation about why full PostgreSQL protocol on NATS is a bad bet — and what a realistic, valuable SQL layer on NATS would look like. The answer: SQL as a read-only query layer over JetStream KV materializations, targeting NATS developers who want queryable state without adding a second infrastructure dependency.

natsql generalizes the pattern of "stream → KV" materialization into a configurable engine, inspired by reference implementations in the NATS ecosystem.

## Current State (v1.1)

Shipped **2026-05-29** with ~9,400 LOC across 40+ Go source files.

**Architecture:** 3-component model — Materializer (stream→KV), Query Engine (SQL→KV reads), Transport (NATS/HTTP/Embed).

**Tech stack:** Go, NATS JetStream KV, vitess sqlparser, chi HTTP router, Cobra CLI, GitHub Actions CI.

**Deployment modes:** Go library (import natsql), standalone binary with embedded NATS, standalone binary connecting to external NATS cluster.

**v1.1 improvements:** All known bugs and hardening gaps from v1.0 code reviews eliminated — PK post-filter, data race fix, type-safe WHERE comparison, boolean literal support, PK sanitization, DLQ error propagation, HTTP timeouts/body limits, NATS bounded context, 750-line black-box test suite, GitHub Actions CI pipeline.

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
| Minimal dependencies | No external project dependencies; reference inspiration only. Keeps surface area minimal | ✓ Good |
| v1 = functional prototype | Prove the concept works end-to-end before hardening | ✓ Good |
| Minimal SQL (eq + range + LIMIT) | Covers the 90% use case for KV-backed state queries | ⚠️ Revisit: range scans deferred to v2 |
| vitess sqlparser | Battle-tested SQL parser, handles all edge cases | ✓ Good |
| Single KV bucket | Simpler to manage, fewer NATS resources | ✓ Good |
| Cobra CLI | Already in monorepo, NATS ecosystem standard | ✓ Good |

---
*Last updated: 2026-05-29 — after v1.1 milestone completed*
