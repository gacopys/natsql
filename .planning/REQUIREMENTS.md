# Requirements: natsql

**Defined:** 2026-07-17
**Core Value:** A developer can define a materialized view from a stream, publish events, and query the current state with `SELECT ... WHERE ...` — zero infrastructure beyond NATS.

## v2.1 Requirements

Code stabilization milestone. Fix all bugs and code quality issues from the cr3.md code review verification. No new features. All requirements derived from confirmed findings.

### Engine Lifecycle

- [ ] **LIFE-01**: `Engine.Start` cleans up partially-started resources on error (goroutines, drain channels, NATS sub, HTTP server) — cr3 §1.1
- [ ] **LIFE-02**: `Engine.Query` rejects queries after `Close()` with explicit error — cr3 §1.2
- [ ] **LIFE-03**: Materializer panic does not cause silent goroutine death — error surfaced and in-flight message NAK'd — cr3 §3.21
- [ ] **LIFE-04**: CLI calls `eng.Close()` on all failure paths after engine creation — cr3 §1.4
- [ ] **LIFE-05**: `Engine.Query` lazy KV init does not hold the mutex across a network RPC — cr3 §2.8

### PK Encoding

- [ ] **PK-01**: Single canonical `FormatPKPart` used by both writer and planner; numeric PKs in non-canonical JSON form (100.0, 1e10) are reachable via PK lookup — cr3 §2.2
- [ ] **PK-02**: `key_separator` restricted to `/` and `_` (chars escaped by `SanitizePK`) to prevent composite-key collisions — cr3 §1.5

### SQL Dialect Hardening

- [ ] **SQL-01**: Column aliases (`SELECT id AS x`) rejected at parse time with explicit error — cr3 §1.7
- [ ] **SQL-02**: Mixed `*` with explicit columns (`SELECT *, id`) rejected at parse time — cr3 §1.8
- [ ] **SQL-03**: `NULL` literals in WHERE rejected at parse time with explicit error — cr3 §3.12
- [ ] **SQL-04**: Contradictory WHERE predicates on any column (not just PK) produce `EmptyPlan` — cr3 §1.13

### Config Validation

- [ ] **CFG-01**: Duplicate `key_fields` entries rejected by config validation — cr3 §1.15

### Contract & Documentation

- [ ] **DOC-01**: `WithHTTPServer` honours the host component (0.0.0.0 override works as documented; T-02-06 preserved with default 127.0.0.1) — cr3 §1.10
- [ ] **DOC-02**: Duplicate `StoreSchema` call removed from `materialize.Run` (engine is the single owner) — cr3 §1.6
- [ ] **DOC-03**: Consistency constraint corrected from "CAS-based" to "idempotent upsert within sequential consumer" — cr3 §3.20
- [ ] **DOC-04**: `PKLookupPlan` has explicit `Limit` documentation (≤1 row, current behaviour preserved) — cr3 §3.14

### Transport Safety

- [ ] **TSP-01**: NATS and HTTP error envelopes use `json.Marshal` of a real struct (no hand-rolled JSON strings) — cr3 §1.9
- [ ] **TSP-02**: Write error classification uses typed NATS/jetstream sentinels before substring fallback (substring path logs Warning) — cr3 §2.6
- [ ] **TSP-03**: All mapper errors (not just `ErrMalformedEvent`) route to DLQ + Ack — cr3 §2.7

### Robustness

- [ ] **ROB-01**: DLQ stream config includes `MaxMsgs`/`MaxBytes` caps — cr3 §3.18
- [ ] **ROB-02**: Consumer config fields validated against negative values — cr3 §3.19
- [ ] **ROB-03**: DLQ envelope timestamp uses `RFC3339Nano` for sub-second precision — cr3 §3.16
- [ ] **ROB-04**: `FullScanPlan` pre-normalises condition values once per query (not per-row) — cr3 §3.11

### API Gaps

- [ ] **APIGAP-01**: User can disable HTTP transport via `WithHTTPDisabled()` / config flag — cr3 §1.11
- [ ] **APIGAP-02**: Library users can add NATS query subscription without transferring connection ownership (`WithNATSQueryConn`) — cr3 §1.12
- [ ] **APIGAP-03**: Bound HTTP port is discoverable via `HTTPAddr()` method or `Stats` field — cr3 §2.9

### Cleanup

- [ ] **CLN-01**: Dead code removed (deprecated `PKKey`, dead `float64` branches in `validateType`, redundant port defaults in `engine.New`) — cr3 §3.3, §4.1, §2.5
- [ ] **CLN-02**: Default PK separator logic centralised in `BuildSchema` (removed from `materialize.Run` and `planner.BuildPlan`) — cr3 §2.3
- [ ] **CLN-03**: CLI passes logger to engine construction; per-query log level changed to `Debug` — cr3 §2.12, §3.8
- [ ] **CLN-04**: Redundant `SetDefaults` call removed from CLI; redundant `Content-Type` headers hoisted; `oapi.QueryResult` vs `query.QueryResult` rationalised — cr3 §3.4, §3.5, §4.11
- [ ] **CLN-05**: `EmptyPlan.Columns` dead field removed; detached doc comment fixed — cr3 §3.1
- [ ] **CLN-06**: `embed.StartNode` and `testutil.StartEmbeddedNATS` share a base helper — cr3 §2.11

## v2.2 Requirements (Deferred)

Deferred features originally listed as "Next Milestone" in v1.2.

- **QUERY-02**: Range scans (`WHERE <col> > <val>` / `< <val>`)
- **INDEX-01**: Secondary indexes on materialized views
- **DELETE-01**: Delete/tombstone semantics for materialized rows
- **BUCKET-01**: Per-view KV buckets for full isolation

## Out of Scope

| Feature | Reason |
|---------|--------|
| Range scans | Deferred — cr3 code stabilization took priority |
| Secondary indexes | Deferred — cr3 code stabilization took priority |
| Delete/tombstone semantics | Deferred — cr3 code stabilization took priority |
| Per-view KV buckets | Deferred — cr3 code stabilization took priority |
| DML via SQL | Writes only happen through stream messages |
| Multi-table JOINs | High complexity, deferred indefinitely |
| Complex transactions / serializable isolation | Beyond v1 scope |
| Full pgwire / PostgreSQL protocol | Not needed for NATS-native query layer |
| Aggregations (GROUP BY, COUNT, AVG) | Deferred indefinitely |
| Subqueries, window functions, CTEs | Not in SQL dialect scope |
| Schema migration / ALTER VIEW | Requires full re-materialize |
| External project integration | Each project maintains independence |

## Traceability

| Requirement | Phase | Status |
|---|---|---|---|
| LIFE-01 | 12 | Pending |
| LIFE-02 | 12 | Pending |
| LIFE-03 | 12 | Pending |
| LIFE-04 | 12 | Pending |
| LIFE-05 | 12 | Pending |
| PK-01 | 12 | Pending |
| PK-02 | 12 | Pending |
| SQL-01 | 13 | Pending |
| SQL-02 | 13 | Pending |
| SQL-03 | 13 | Pending |
| SQL-04 | 13 | Pending |
| CFG-01 | 13 | Pending |
| DOC-01 | 15 | Pending |
| DOC-02 | 15 | Pending |
| DOC-03 | 15 | Pending |
| DOC-04 | 15 | Pending |
| TSP-01 | 14 | Pending |
| TSP-02 | 14 | Pending |
| TSP-03 | 14 | Pending |
| ROB-01 | 14 | Pending |
| ROB-02 | 14 | Pending |
| ROB-03 | 14 | Pending |
| ROB-04 | 14 | Pending |
| APIGAP-01 | 16 | Pending |
| APIGAP-02 | 16 | Pending |
| APIGAP-03 | 16 | Pending |
| CLN-01 | 16 | Pending |
| CLN-02 | 16 | Pending |
| CLN-03 | 16 | Pending |
| CLN-04 | 16 | Pending |
| CLN-05 | 16 | Pending |
| CLN-06 | 16 | Pending |

**Coverage:**
- v2.1 requirements: 32 total
- Mapped to phases: 32
- Unmapped: 0 ✓

---
*Requirements defined: 2026-07-17*
*Last updated: 2026-07-17 after initial definition*
