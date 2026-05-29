# Roadmap: natsql

## Overview

natsql is a NATS-native materialized view engine. Define stream-to-KV materializations declaratively (YAML/JSON), and query the resulting state with read-only SQL via NATS request-reply or HTTP.

## Milestones

- ✅ **v1.0 MVP** — Phases 1-3 (shipped 2026-05-28)
- 🏗 **v1.1 Tech Debt Cleanup** — Phases 4-7

## Phases

<details>
<summary>✅ v1.0 MVP (Phases 1-3) — SHIPPED 2026-05-28</summary>

- [x] **Phase 1: Foundation — Materializer** (3/3 plans) — completed 2026-05-28
- [x] **Phase 2: SQL Query Engine + Interfaces** (2/2 plans) — completed 2026-05-28
- [x] **Phase 3: Packaging + Deployment** (2/2 plans) — completed 2026-05-28

See `.planning/milestones/v1.0-ROADMAP.md` for full phase details.
</details>

<br/>

<details open>
<summary>🏗 v1.1 Tech Debt Cleanup (Phases 4-7) — ACTIVE</summary>

- [ ] **Phase 4: Query Engine Bug Fixes** — PK post-filter, data race, type comparison, boolean literal parsing
- [ ] **Phase 5: Materializer Hardening** — PK sanitization, DLQ errors, partial init cleanup, float64 precision
- [ ] **Phase 6: Transport & Code Health** — HTTP timeouts, body size limit, NATS context timeout, dead code removal
- [ ] **Phase 7: Integration Verification** — Full test suite, regression check, perf benchmark
</details>

## Phase Details

### Phase 4: Query Engine Bug Fixes
**Goal**: PK-lookup queries correctly apply all WHERE conditions; concurrent access is race-free; type comparisons are accurate; boolean literals parse.
**Depends on**: Nothing (fixes to existing v1.0 code)
**Requirements**: FIX-ENG-01, FIX-ENG-02, FIX-ENG-03, FIX-ENG-04
**Success Criteria** (what must be TRUE):
1. Query `SELECT * FROM users WHERE id = 'abc' AND name = 'Bob'` returns only rows matching both conditions (previously dropped the `name` filter)
2. Multiple goroutines calling `Engine.Query()` concurrently pass `go test -race` without data race warnings on `Engine.kv`
3. `filterRow` compares values by their actual types (string vs string, number vs number) — no false matches from `fmt.Sprint` coercion
4. SQL parser accepts `WHERE active = true` and `WHERE active = false` and returns correct boolean-typed results
**Plans**: TBD

### Phase 5: Materializer Hardening
**Goal**: KV keys are safe from special characters; DLQ failures are surfaced; startup is resilient; large integers preserve precision.
**Depends on**: Nothing (fixes to existing v1.0 code)
**Requirements**: FIX-MAT-01, FIX-MAT-02, FIX-MAT-03, FIX-MAT-04
**Success Criteria** (what must be TRUE):
1. PK values containing `/`, `*`, `>`, `.` are sanitized into safe KV key form (no NATS subject wildcard injection)
2. When DLQ publish fails, the source event is **not** acknowledged — consumer position does not advance past the failed event
3. When `Engine.Start` fails during partial initialization, all created resources (streams, KV buckets) are cleaned up — no orphaned NATS resources
4. JSON integer values >2^53 (e.g., `9007199254740993`) stored in materialized state retain their exact value — no float64 rounding
**Plans**: TBD

### Phase 6: Transport & Code Health
**Goal**: HTTP server is hardened with proper timeouts and body limits; NATS handlers have bounded contexts; dead code, unused params, and test flakiness are eliminated.
**Depends on**: Nothing (fixes to existing v1.0 code)
**Requirements**: FIX-TRN-01, FIX-TRN-02, FIX-TRN-03, FIX-TRN-04
**Success Criteria** (what must be TRUE):
1. HTTP server enforces read/write/idle timeouts — hanging connections are terminated, not left open indefinitely
2. HTTP `/api/v1/query` endpoint rejects request bodies exceeding the configured limit with `413 Request Entity Too Large`
3. NATS query handler applies a bounded context with timeout — a slow/stuck query returns error instead of hanging forever
4. `go vet ./...` passes clean; no dead code, unused parameters, or shadowed variables remain
5. Tests run reliably 5x in a row with `-race` — no sporadic `time.Sleep`-based flakiness
**Plans**: TBD

### Phase 7: Integration Verification
**Goal**: All fixes work together without regressions; full acceptance criteria pass; performance is baseline-verified.
**Depends on**: Phase 4, Phase 5, Phase 6
**Requirements**: None (cross-cutting verification)
**Success Criteria** (what must be TRUE):
1. `go test -race ./...` passes on a clean checkout of all Phase 4-6 changes
2. Full end-to-end workflow verified: define a view → publish events → query via NATS request-reply → query via HTTP → all routes return correct results
3. Perf benchmark completes without regression against v1.0 baseline (same throughput + latency profile within noise)
4. All v1.0 acceptance criteria continue to pass (DECL-01, STREAM-01, STREAM-02, QUERY-01, QUERY-04, QUERY-05, EMBED-01, EMBED-02, EMBED-03, RESIL-01)
**Plans**: TBD

## Progress

| Phase | Milestone | Plans Complete | Status | Completed |
|-------|-----------|----------------|--------|-----------|
| 1. Foundation — Materializer | v1.0 | 3/3 | Complete | 2026-05-28 |
| 2. SQL Query Engine + Interfaces | v1.0 | 2/2 | Complete | 2026-05-28 |
| 3. Packaging + Deployment | v1.0 | 2/2 | Complete | 2026-05-28 |
| 4. Query Engine Bug Fixes | v1.1 | 0/0 | Not started | - |
| 5. Materializer Hardening | v1.1 | 0/0 | Not started | - |
| 6. Transport & Code Health | v1.1 | 0/0 | Not started | - |
| 7. Integration Verification | v1.1 | 0/0 | Not started | - |
