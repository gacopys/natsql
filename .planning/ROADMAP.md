# Roadmap: natsql

## Overview

natsql is a NATS-native materialized view engine. Define stream-to-KV materializations declaratively (YAML/JSON), and query the resulting state with read-only SQL via NATS request-reply or HTTP.

The v1.2 milestone remediates all 25 findings from the comprehensive code review, achieving 100% project correctness.

## Milestones

- ✅ **v1.0 MVP** — Phases 1-3 (shipped 2026-05-28)
- ✅ **v1.1 Tech Debt Cleanup** — Phases 4-7 (shipped 2026-05-29)
- 🏗️ **v1.2 Code Review Remediation** — Phases 8-11 (in progress)

## Phases

<details>
<summary>✅ v1.0 MVP (Phases 1-3) — SHIPPED 2026-05-28</summary>

- [x] **Phase 1: Foundation — Materializer** (3/3 plans) — completed 2026-05-28
- [x] **Phase 2: SQL Query Engine + Interfaces** (2/2 plans) — completed 2026-05-28
- [x] **Phase 3: Packaging + Deployment** (2/2 plans) — completed 2026-05-28

See `.planning/milestones/v1.0-ROADMAP.md` for full phase details.
</details>

<br/>

<details>
<summary>✅ v1.1 Tech Debt Cleanup (Phases 4-7) — SHIPPED 2026-05-29</summary>

- [x] **Phase 4: Query Engine Bug Fixes** — PK post-filter, data race, type comparison, boolean literal parsing
- [x] **Phase 5: Materializer Hardening** — PK sanitization, DLQ errors, partial init cleanup, float64 precision
- [x] **Phase 6: Transport & Code Health** — HTTP timeouts, body size limit, NATS context timeout, dead code removal
- [x] **Phase 7: Integration Verification** — Full test suite, regression check, perf benchmark

See `.planning/milestones/v1.1-ROADMAP.md` for full phase details.
</details>

<br/>

<details open>
<summary>🏗️ v1.2 Code Review Remediation (Phases 8-11) — IN PROGRESS</summary>

- [ ] **Phase 8: Verification & Foundation** — Verify cr.md findings; canonical PK encoder, SQL rejection, config validation
- [ ] **Phase 9: Materializer & Engine Lifecycle** — Ordered processing, error classification, consumer durability, startup reliability
- [ ] **Phase 10: Query Engine & Transport** — Predicate handling, meta filtering, number precision, transport robustness
- [ ] **Phase 11: Cleanup & Documentation** — Dead code removal, gofmt, test dedup, docs sync, examples hygiene
</details>

## Phase Details

### Phase 8: Verification & Foundation

**Goal**: All 25 code review findings are verified against source code, and foundational correctness issues (canonical PK encoding, unsupported SQL rejection, config validation) are fixed to unblock subsequent phases.
**Depends on**: Nothing (first phase of v1.2)
**Requirements**: VER-01, FND-01, FND-02, FND-03
**Success Criteria** (what must be TRUE):
  1. Every cr.md finding has been examined against source code and documented as confirmed or dismissed with explicit reasoning
  2. PK values are encoded by a single canonical function used by both write (materializer) and read (query planner/executor) paths, eliminating double-sanitization and lookup key mismatch
  3. Unsupported SQL constructs (ORDER BY, DISTINCT, GROUP BY, HAVING, aggregations, subqueries) produce explicit error messages at parse time instead of silent mishandling
  4. Config validation cross-references `key_fields` against declared `primary_key` columns, verifies uniqueness of view names and column names, and rejects invalid configurations at load time
**Plans**: 4 plans

Plans:
- [x] 08-01-PLAN.md — Verify all 25 code review findings against source
- [x] 08-02-PLAN.md — Reject unsupported SQL constructs (DISTINCT, ORDER BY, GROUP BY, HAVING, aggregations)
- [x] 08-03-PLAN.md — Add config cross-validation (key_fields vs primary_key)
- [x] 08-04-PLAN.md — Canonical PK encoder (BuildPkKey), remove dead code

### Phase 9: Materializer & Engine Lifecycle

**Goal**: The materializer processes events safely in stream order with proper error classification, consumer durability is hardened, and engine startup reliably propagates failures synchronously.
**Depends on**: Phase 8 (requires canonical PK encoder from Foundation)
**Requirements**: MAT-01, MAT-02, MAT-03, MAT-04, LIFE-01, LIFE-02
**Success Criteria** (what must be TRUE):
  1. Materializer processes events from a single durable consumer in stream order, preserving JetStream's per-subject ordering guarantee at the KV write boundary
  2. KV write errors are classified as transient (connection/leader election → NAK with backoff) or terminal (bad data/config → DLQ + Ack), preventing temporary NATS outages from causing permanent data loss
  3. Durable consumers have no InactiveThreshold that could cause automatic deletion after downtime
  4. Consumer batch configuration is either renamed to `MaxAckPending` or actual batched fetching via `Fetch`/`FetchNoWait` is implemented
  5. HTTP server port is initialized from `cfg.HTTP.Port` in engine constructors instead of being hardcoded to 8080
  6. `Engine.Start` propagates startup errors synchronously: HTTP listen failures, materializer init failures, and NATS handler registration failures prevent the engine from reporting as started
**Plans**: TBD

### Phase 10: Query Engine & Transport

**Goal**: Query results are correct in all edge cases (contradictory predicates, meta field leakage, numeric precision loss, unbounded scans), and all transport layers handle errors robustly.
**Depends on**: Phase 8 (requires canonical PK encoder from Foundation)
**Requirements**: QENG-01, QENG-02, QENG-03, QENG-04, TRN-01, TRN-02, TRN-03, TRN-04
**Success Criteria** (what must be TRUE):
  1. All WHERE conditions are retained as post-filters (including conditions on PK columns), preventing contradictory or duplicate PK predicates from producing wrong results
  2. `SELECT *` returns only schema-declared columns, excluding internal `_meta` fields from query results
  3. Query executor uses `json.Decoder.UseNumber()` for consistent numeric comparison, matching the materializer's precision handling for large integers above 2^53
  4. Full-scan queries for a single view do not pay the cost of scanning all other views' KV keys; at minimum the cross-view cost is documented
  5. CLI stream creation respects configured `source_subject` and does not mutate existing external streams without explicit opt-in
  6. HTTP JSON handler uses `errors.As` for `MaxBytesError` body-size detection and rejects trailing non-whitespace data
  7. NATS transport checks and surfaces `nc.Flush()` and `msg.Respond()` errors instead of ignoring them
  8. Error message in executor.go is corrected from "marshaling row" to "unmarshaling row"
**Plans**: TBD

### Phase 11: Cleanup & Documentation

**Goal**: Codebase is clean — dead code removed, formatting enforced, tests deduplicated, documentation accurate, and examples follow best practices.
**Depends on**: Phase 10 (cleanup comes after all behavioral changes are verified and merged)
**Requirements**: CLN-01, CLN-02, CLN-03, CLN-04, CLN-05, CLN-06, CLN-07, CLN-08
**Success Criteria** (what must be TRUE):
  1. `$.` field prefix notation is either fully supported or all config examples and tests use consistent plain dot notation with the restriction documented
  2. Index configuration produces a clear validation error or documented no-op warning until secondary indexes are implemented
  3. Missing delete/tombstone semantics are documented as a known gap with planned future support
  4. All examples check errors properly and avoid lifecycle ownership hazards; unused symbols are removed leaving one canonical PK encoding path
  5. All Go source files are formatted with `gofmt -w` and CI enforces formatting via a format check step
  6. Embedded NATS setup and stream creation helpers are deduplicated across test packages
  7. README and SQL dialect documentation accurately reflect implemented features including LIMIT support
**Plans**: TBD

## Progress

| Phase | Milestone | Plans Complete | Status | Completed |
|-------|-----------|----------------|--------|-----------|
| 1. Foundation — Materializer | v1.0 | 3/3 | Complete | 2026-05-28 |
| 2. SQL Query Engine + Interfaces | v1.0 | 2/2 | Complete | 2026-05-28 |
| 3. Packaging + Deployment | v1.0 | 2/2 | Complete | 2026-05-28 |
| 4. Query Engine Bug Fixes | v1.1 | 1/1 | Complete | 2026-05-29 |
| 5. Materializer Hardening | v1.1 | 1/1 | Complete | 2026-05-29 |
| 6. Transport & Code Health | v1.1 | 1/1 | Complete | 2026-05-29 |
| 7. Integration Verification | v1.1 | 1/1 | Complete | 2026-05-29 |
| 8. Verification & Foundation | v1.2 | 0/4 | Not started | - |
| 9. Materializer & Engine Lifecycle | v1.2 | 0/0 | Not started | - |
| 10. Query Engine & Transport | v1.2 | 0/0 | Not started | - |
| 11. Cleanup & Documentation | v1.2 | 0/0 | Not started | - |
