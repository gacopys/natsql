# Phase 2: SQL Query Engine + Interfaces — Context

**Gathered:** 2026-05-28
**Status:** Ready for planning

<domain>
## Phase Boundary

PK-lookup SQL query engine over materialized KV state, served via NATS request-reply and HTTP. Covers QRY-01 through QRY-03 and IFC-01, IFC-02: SQL parsing, PK-lookup execution, typed JSON results, NATS request-reply query interface, and HTTP query endpoint.

Index scans (QRY-04/05), ORDER BY (QRY-07), COUNT(*) (QRY-08), and JOINs (QRY-09) are deferred to v2.

</domain>

<decisions>
## Implementation Decisions

### SQL Parser
- **D-18:** Use `vitess.io/vitess/go/vt/sqlparser` for SQL parsing. Battle-tested, handles edge cases, future-proof for v2 dialect extensions.
- **D-19:** Direct dependency in `natsql/go.mod` (no separate parser module). Vitess is imported only for `go/vt/sqlparser`.

### SQL Dialect Scope (v1)
- **D-20:** Both `SELECT *` and `SELECT col1, col2` supported. Explicit column list is validated against the view schema.
- **D-21:** Supported WHERE operators: `=`, `!=`, `IN`. All comparison values use single quotes only (SQL standard).
- **D-22:** `LIMIT N` supported.
- **D-23:** Non-PK columns in WHERE clause are allowed but trigger a full bucket scan (`KV.Keys()` + client-side filter). Performance is O(n) — documented in plan.
- **D-24:** Missing WHERE clause is rejected with a clear error.

### Query Engine Architecture
- **D-25:** New `natsql/query/` package for SQL execution — parse → plan → execute → project → return.
- **D-26:** `Engine.Query(ctx, sql string) (*QueryResult, error)` method added. NATS subscription and HTTP handlers both call `engine.Query()`.
- **D-27:** View schema is loaded from KV on each query call via `kv.LoadSchema()` (no in-memory cache — always fresh).
- **D-28:** Query execution plan types for v1: `PKLookupPlan` (direct `KV.Get()`) and `FullScanPlan` (`KV.Keys()` + client filter). Only equality on PK columns produces `PKLookupPlan`.

### Response Format
- **D-29:** Minimal JSON envelope: `{"results": [...], "error": null|"message"}`.
- **D-30:** Typed JSON values — strings quoted, numbers unquoted, booleans literal, null for missing.
- **D-31:** Explicit null for null/missing column values (not omitted).
- **D-32:** Human-readable error strings (no error codes). Vitess error messages passed through for parse errors.
- **D-33:** Empty results are a valid response: `{"results": [], "error": null}` — success with zero rows.

### NATS Interface
- **D-34:** Default subject: `natsql.query`, configurable via engine config.
- **D-35:** Request body is raw SQL string (no JSON wrapper). Response is the standard JSON envelope.
- **D-36:** NATS subscription is started in `Engine.Start()` and stopped in `Engine.Close()`.

### HTTP Interface
- **D-37:** Single route: `POST /api/v1/query` with JSON body `{"sql": "..."}`. No GET endpoint.
- **D-38:** Standard JSON Content-Type. Response is the same JSON envelope as NATS interface.
- **D-39:** HTTP server is started in `Engine.Start()` and stopped in `Engine.Close()`.

### Transport Layer
- **D-40:** New `natsql/transport/` package with `nats.go` and `http.go` for NATS subscription handler and HTTP handler respectively.
- **D-41:** Both transports share the same response format and call `engine.Query()`.

### Error Handling
- **D-42:** Unknown view name → error: `view "X" not found`.
- **D-43:** Unknown column name in SELECT or WHERE → error: `column "X" not found in view "Y"`.
- **D-44:** Non-PK WHERE → allowed, but full scan is used (O(n)). Document this performance characteristic.
- **D-45:** NULL PK value in WHERE → error (PK columns cannot be null).

### Claude's Discretion
- Port configuration for HTTP server (default 8080 vs configurable)
- Exact vitess import path and version constraint
- Full scan batch size for `KV.Keys()` iteration
- Timeout configuration for query execution
- Logging verbosity for query execution

</decisions>

<canonical_refs>
## Canonical References

### Project requirements
- `.planning/REQUIREMENTS.md` — QRY-01 through QRY-03, IFC-01, IFC-02 define Phase 2 requirements
- `.planning/PROJECT.md` — Project constraints, key decisions, SQL dialect constraints

### Research
- `.planning/research/SUMMARY.md` — Architecture approach, SQL-on-KV execution pattern, query plan types
- `.planning/research/STACK.md` — vitess sqlparser rationale and usage patterns
- `.planning/research/ARCHITECTURE.md` — Query execution pipeline, plan types, KV integration patterns

### Phase 1 artifacts (live code)
- `natsql/kv/kv.go` — `LoadSchema()`, `PkKey()`, `ViewSchema` type
- `natsql/config.go` — `ViewConfig`, `ColumnConfig`, `ColumnType`
- `natsql/engine/engine.go` — `Engine` struct with `Start()`/`Close()` lifecycle
- `natsql/go.mod` — Go module with existing dependencies

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- `natsql/kv/kv.go` — `LoadSchema(ctx, kv, viewName)` loads view schema at query time (`ViewSchema` with `Columns`, `KeyFields`)
- `natsql/kv/kv.go` — `PkKey(viewName, pkValue)` produces correct KV key for PK lookup
- `natsql/config.go` — `ViewConfig`, `ColumnConfig`, `ColumnType` types for schema validation
- `natsql/engine/engine.go` — `Engine` struct with lifecycle (but query methods need to be added)
- `natsql/materialize/` — Materializer components exist but query engine is read-only, no dependency on them

### Established Patterns
- PK key format: `{view_name}/pk/{pkValue}`
- Schema stored at `{view_name}/meta/schema`
- Schema includes `KeyFields` list and `KeySeparator` for composite key handling
- Column type system: `string`, `number`, `boolean`, `timestamp`

### Integration Points
- `engine.Query(sql)` will be the integration seam between transport layer and query engine
- `kv.LoadSchema()` provides schema lookups at query time
- `kv.PkKey()` + `KV.Get()` form the PK lookup path
- `KV.Keys()` with prefix filtering forms the full scan path (for non-PK WHERE)

</code_context>

<specifics>
## Specific Ideas

- Query engine should log executed SQL at debug level for observability
- HTTP server should support CORS headers for browser-based tooling
- NATS subscription should handle both single-request and wildcard subjects for flexibility
- Error responses should include the SQL text when reporting parse errors for easier debugging

</specifics>

<deferred>
## Deferred Ideas

- Secondary index scans (QRY-04/05) — v2
- ORDER BY support (QRY-07) — v2
- COUNT(*) and aggregations (QRY-08) — v2
- JOINs (QRY-09) — v2
- CLI query tool (`natsql query 'SELECT ...'`) — v2 (IFC-04)
- Push queries / subscriptions (SUBSCRIBE SELECT) — v2 (IFC-05)

</deferred>

---

*Phase: 02-sql-query-engine-interfaces*
*Context gathered: 2026-05-28*
