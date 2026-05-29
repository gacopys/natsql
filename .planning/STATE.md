---
gsd_state_version: 1.0
milestone: v1.1
milestone_name: Tech Debt Cleanup
status: completed
stopped_at: Milestone shipped
last_updated: "2026-05-29T22:30:00Z"
last_activity: 2026-05-29 -- v1.1 Tech Debt Cleanup milestone completed and archived
progress:
  total_phases: 4
  completed_phases: 4
  total_plans: 4
  completed_plans: 4
  percent: 100
---

# Project State

## Project Reference

See: .planning/PROJECT.md (updated 2026-05-29)

**Core value:** A developer can define a materialized view from a stream, publish events, and query the current state with `SELECT ... WHERE ...` — zero infrastructure beyond NATS.

**Current focus:** Next milestone (to be defined)

## Current Position

Milestone: **v1.1 Tech Debt Cleanup — COMPLETE ✅**
Status: All 12 requirements fixed, verified with `go test -race`, `go vet` clean, full black-box test suite passing, CI pipeline active.
Shipped: 2026-05-29
Archived: `.planning/milestones/v1.1-ROADMAP.md`, `.planning/milestones/v1.1-REQUIREMENTS.md`

Progress: [██████████] 100%

## Performance Metrics

**Velocity:**

- Total plans completed: 4
- Total phases: 4 (Query Engine Bug Fixes, Materializer Hardening, Transport & Code Health, Integration Verification)
- Total tasks: ~16

## Accumulated Context

### V1.1 Issues Resolved

- **FIX-ENG-01**: PKLookupPlan now applies non-PK WHERE conditions as post-filter
- **FIX-ENG-02**: Data race on Engine.kv eliminated via sync.Mutex
- **FIX-ENG-03**: filterRow uses type-aware valuesEqual instead of fmt.Sprint coercion
- **FIX-ENG-04**: SQL parser accepts boolean literals (true/false)
- **FIX-MAT-01**: PK values sanitized via SanitizePK (underscore-prefixed encoding)
- **FIX-MAT-02**: DLQ publish failure → Nak (not Ack), preventing silent data loss
- **FIX-MAT-03**: Engine.Start partial-init cleanup on failure
- **FIX-MAT-04**: JSON integer precision via UseNumber() decoder
- **FIX-TRN-01**: HTTP read/write/idle timeouts configured
- **FIX-TRN-02**: HTTP query endpoint enforces 1MB body size limit
- **FIX-TRN-03**: NATS query handler uses bounded 30s context timeout
- **FIX-TRN-04**: Dead code removed, test flakiness eliminated

### Decisions

| Decision | Rationale | Outcome |
|----------|-----------|---------|
| No new features in v1.1 | Focus exclusively on code quality for clean base | ✓ Confirmed |
| Continue phase numbering from v1.0 | Phases 4+ instead of restarting | ✓ Set |
| Group fixes by component (Engine, Materializer, Transport) | Each phase addresses a coherent set of related fixes | ✓ Adopted |
| Include dedicated integration verification phase | Ensures all fixes work together without regression | ✓ Phase 7 |
| Underscore-prefixed KV key encoding | All result chars are valid NATS key chars, avoids URL encoding issues | ✓ Good |
| Black-box tests through public API only | Validates public contract end-to-end, no internal state inspection | ✓ Good |

### Pending Todos

- None. Next: start next milestone with `/gsd-new-milestone`

## Session Continuity

Last session: 2026-05-29 (milestone completion)
Resume: `/gsd-new-milestone` to define next milestone
