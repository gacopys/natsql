# Requirements: natsql

**Defined:** 2026-05-22
**Core Value:** A developer can define a materialized view from a stream, publish events, and query the current state with `SELECT ... WHERE ...` — zero infrastructure beyond NATS.

## v1 Requirements

### Materialization

- [ ] **MAT-01**: User can define a materialized view in a YAML config file (source stream subject, key column, column mapping)
- [ ] **MAT-02**: Materializer consumes JetStream events from the configured stream with a durable consumer (survives restarts)
- [ ] **MAT-03**: Materializer maintains a KV bucket with current row state (PK → JSON value)
- [ ] **MAT-04**: Materializer handles malformed events gracefully (log + skip, continue processing)

### Query

- [ ] **QRY-01**: User can query by primary key: `SELECT * FROM view WHERE <pk_col> = <val>`
- [ ] **QRY-02**: Query engine validates column names exist in the view schema
- [ ] **QRY-03**: Query engine returns typed JSON results

### Interface

- [ ] **IFC-01**: Query via NATS request-reply (`nc.Request("natsql.query", sqlBytes)` → JSON response)
- [ ] **IFC-02**: Query via HTTP/JSON API endpoint (`GET /query?sql=...` or `POST /query`)
- [ ] **IFC-03**: Query via Go library API (`engine.Query(ctx, "SELECT ...")`)

### Deployment

- [ ] **DEP-01**: Standalone `natsql server` binary with embedded NATS (single-node, dev mode)
- [ ] **DEP-02**: Standalone `natsql server` binary connects to external NATS cluster
- [ ] **DEP-03**: Importable as Go library (`natsql.NewEngine(js, config)` → attach to own NATS connection)
- [ ] **DEP-04**: Graceful shutdown — drain materializer consumer, flush pending writes, stop HTTP server

## v2 Requirements

### Materialization

- **MAT-05**: Event-to-row mapping — map multiple subject patterns (e.g., `orders.created`, `orders.updated`, `orders.canceled`) to INSERT/UPDATE/DELETE operations
- **MAT-06**: Full rebuild — replay entire source stream to rebuild a materialized view from scratch
- **MAT-07**: Per-view KV buckets (vs single shared bucket)

### Query

- **QRY-04**: Index equality scan: `SELECT * FROM view WHERE <idx_col> = <val>`
- **QRY-05**: Index range scan: `SELECT * FROM view WHERE <idx_col> > <val>`
- **QRY-06**: `LIMIT N` support
- **QRY-07**: `ORDER BY <col> ASC/DESC` on indexed columns
- **QRY-08**: `COUNT(*)` aggregation
- **QRY-09**: Multi-table JOINs

### Interface

- **IFC-04**: CLI tool (`natsql query 'SELECT ...'`)
- **IFC-05**: Push queries / subscriptions (`SUBSCRIBE SELECT ... WHERE ... EMIT CHANGES`)

### Operations

- **OPS-01**: Prometheus metrics (consumer lag, query latency, KV operations)
- **OPS-02**: Structured logging
- **OPS-03**: Configuration reload (SIGHUP)

## Out of Scope

| Feature | Reason |
|---------|--------|
| DML via SQL (INSERT/UPDATE/DELETE) | Writes only through stream messages; SQL is read-only |
| Full pgwire / PostgreSQL protocol | Massive engineering investment; users can wrap the HTTP API |
| Serializable isolation / multi-key transactions | NATS KV CAS is read-committed; transaction coordinator is v3+ |
| Subqueries, window functions, CTEs | Requires full SQL planner; not needed for the 90% use case |
| Schema migration / ALTER VIEW with backfill | Requires full re-materialization; document as manual process |
| ebind integration | Independent project; ebind is reference inspiration only |

## Traceability

| Requirement | Phase | Status |
|-------------|-------|--------|
| MAT-01 | | Pending |
| MAT-02 | | Pending |
| MAT-03 | | Pending |
| MAT-04 | | Pending |
| QRY-01 | | Pending |
| QRY-02 | | Pending |
| QRY-03 | | Pending |
| IFC-01 | | Pending |
| IFC-02 | | Pending |
| IFC-03 | | Pending |
| DEP-01 | | Pending |
| DEP-02 | | Pending |
| DEP-03 | | Pending |
| DEP-04 | | Pending |

**Coverage:**
- v1 requirements: 14 total
- Mapped to phases: 0
- Unmapped: 14 ⚠️

---
*Requirements defined: 2026-05-22*
*Last updated: 2026-05-22 after initial definition*
