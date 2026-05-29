# Roadmap: natsql

## Overview

natsql is a NATS-native materialized view engine. Define stream-to-KV materializations declaratively (YAML/JSON), and query the resulting state with read-only SQL via NATS request-reply or HTTP.

## Milestones

- ✅ **v1.0 MVP** — Phases 1-3 (shipped 2026-05-28)
- ✅ **v1.1 Tech Debt Cleanup** — Phases 4-7 (shipped 2026-05-29)

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
