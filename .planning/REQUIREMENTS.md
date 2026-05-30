# Requirements: natsql

**Defined:** 2026-05-31
**Core Value:** A developer can define a materialized view from a stream, publish events, and query the current state with `SELECT ... WHERE ...` — zero infrastructure beyond NATS.

## v1.2 Requirements

### Verification

- [ ] **VER-01**: Each finding in cr.md is examined against source code and confirmed or dismissed with documented reasoning

### Foundation — PK Encoding, SQL Rejection, Config Validation

- [ ] **FND-01**: PK values are encoded by a single canonical function used by both write (materializer) and read (query planner/executor) paths, eliminating double-sanitization and lookup key mismatch (CR-02)
- [ ] **FND-02**: Unsupported SQL constructs (ORDER BY, DISTINCT, GROUP BY, HAVING, aggregations, subqueries) are explicitly rejected with an error instead of silently mishandled (CR-05)
- [ ] **FND-03**: Config validation cross-references `key_fields` against declared `primary_key` columns, verifies uniqueness of view names and column names, and rejects invalid configurations at load time (CR-08)

### Materializer — Ordered Processing, Error Classification, Consumer Config

- [ ] **MAT-01**: Materializer processes events from a single durable consumer in stream order, preserving JetStream's per-subject ordering guarantee at the KV write boundary (CR-01)
- [ ] **MAT-02**: Materializer classifies KV write errors: transient (connection/leader election) → NAK with backoff for redelivery, terminal (bad data/config) → DLQ + Ack, malformed input → DLQ + Ack (CR-10)
- [ ] **MAT-03**: Durable consumers do not have an InactiveThreshold that could cause automatic deletion after downtime (CR-11)
- [ ] **MAT-04**: Consumer BatchSize setting is renamed to MaxAckPending to match its actual behavior, or actual batched fetching via Fetch/FetchNoWait is implemented (CR-12)

### Query Engine — Predicates, Projection, Precision, Scans

- [ ] **QENG-01**: All WHERE conditions are retained as post-filters, including conditions on PK columns, preventing contradictory or duplicate PK predicates from producing wrong results (CR-03)
- [ ] **QENG-02**: `SELECT *` returns only schema-declared columns, excluding internal `_meta` fields (CR-04)
- [ ] **QENG-03**: Query executor uses `json.Decoder.UseNumber()` for consistent numeric comparison, matching the materializer's precision handling for large integers above 2^53 (CR-09)
- [ ] **QENG-04**: Full-scan queries for a single view do not pay the cost of scanning all other views' KV keys; at minimum the cross-view cost is documented (CR-13)

### Engine Lifecycle — Port Config, Startup Error Propagation

- [ ] **LIFE-01**: HTTP server port is initialized from `cfg.HTTP.Port` in engine constructors, not hardcoded to 8080 (CR-06)
- [ ] **LIFE-02**: Engine.Start propagates startup errors synchronously: HTTP listen failures, materializer init failures, and NATS handler registration failures prevent the engine from reporting as started (CR-07)

### Transport — Stream Creation, HTTP Robustness, NATS Error Handling

- [ ] **TRN-01**: CLI stream creation respects configured `source_subject` and does not mutate existing external streams without explicit opt-in (CR-14)
- [ ] **TRN-02**: HTTP JSON handler uses `*http.MaxBytesError` via `errors.As` for body-size detection and rejects trailing non-whitespace data (CR-18)
- [ ] **TRN-03**: NATS transport checks and surfaces `nc.Flush()` and `msg.Respond()` errors instead of ignoring them (CR-19)
- [ ] **TRN-04**: Error message in executor.go is corrected from "marshaling row" to "unmarshaling row" (CR-20)

### Cleanup — Dead Code, Formatting, Tests, Docs, Examples

- [ ] **CLN-01**: `$.` prefix notation in field paths is either supported or all config examples/tests use consistent plain dot notation and the restriction is documented (CR-15)
- [ ] **CLN-02**: Index configuration produces a clear validation error or documented no-op warning until secondary indexes are implemented (CR-16)
- [ ] **CLN-03**: Missing delete/tombstone semantics are documented as a known gap with planned future support (CR-17)
- [ ] **CLN-04**: All examples check errors properly and avoid lifecycle ownership hazards (CR-21)
- [ ] **CLN-05**: Unused symbols (SchemaPrefix, ErrSkipAndAck, Stats.LastError, dlqStream parameter, EncodePKValue, MustInitBucket) are removed; one canonical PK encoding path remains (CR-22)
- [ ] **CLN-06**: All Go source files are formatted with `gofmt -w` and CI enforces formatting (CR-23)
- [ ] **CLN-07**: Embedded NATS setup and stream creation helpers are deduplicated across test packages (CR-24)
- [ ] **CLN-08**: README and SQL dialect documentation are updated to accurately reflect implemented features including LIMIT support (CR-25)

## v2 Requirements (Deferred)

### Features
- **RANGE-01**: Range scans (`WHERE <col> > <val>` / `< <val>`) on PK and secondary indexes
- **INDEX-01**: Secondary index support for non-PK columns
- **DELETE-01**: Delete/tombstone semantics for materialized rows
- **BUCKET-01**: Per-view KV buckets for full isolation

## Out of Scope

| Feature | Reason |
|---------|--------|
| New feature development (range scans, indexes, delete semantics) | v1.2 is exclusively a correctness remediation milestone |
| Performance optimization beyond fixing bugs | No throughput or latency targets for this milestone |
| Architectural refactoring beyond CR fixes | Per-view buckets, materializer redesign deferred to v2 |
| PostgreSQL protocol compatibility | Explicitly out of scope per PROJECT.md |

## Traceability

| Requirement | Phase | Status |
|-------------|-------|--------|
| VER-01 | TBD | Pending |
| FND-01 | TBD | Pending |
| FND-02 | TBD | Pending |
| FND-03 | TBD | Pending |
| MAT-01 | TBD | Pending |
| MAT-02 | TBD | Pending |
| MAT-03 | TBD | Pending |
| MAT-04 | TBD | Pending |
| QENG-01 | TBD | Pending |
| QENG-02 | TBD | Pending |
| QENG-03 | TBD | Pending |
| QENG-04 | TBD | Pending |
| LIFE-01 | TBD | Pending |
| LIFE-02 | TBD | Pending |
| TRN-01 | TBD | Pending |
| TRN-02 | TBD | Pending |
| TRN-03 | TBD | Pending |
| TRN-04 | TBD | Pending |
| CLN-01 | TBD | Pending |
| CLN-02 | TBD | Pending |
| CLN-03 | TBD | Pending |
| CLN-04 | TBD | Pending |
| CLN-05 | TBD | Pending |
| CLN-06 | TBD | Pending |
| CLN-07 | TBD | Pending |
| CLN-08 | TBD | Pending |

**Coverage:**
- v1.2 requirements: 26 total
- Mapped to phases: 0
- Unmapped: 26 ⚠️

---
*Requirements defined: 2026-05-31*
*Last updated: 2026-05-31 after initial definition*
