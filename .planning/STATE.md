---
gsd_state_version: 1.0
milestone: v2.1
milestone_name: Code Stabilization
status: roadmapped
stopped_at: null
last_updated: "2026-07-17T18:00:00.000Z"
last_activity: 2026-07-17 -- v2.1 roadmap created (5 phases, 32 requirements)
progress:
  total_phases: 5
  completed_phases: 0
  total_plans: 0
  completed_plans: 0
  percent: 0
---

# Project State

## Project Reference

See: .planning/PROJECT.md (updated 2026-07-17 after v2.1 milestone started)

**Core value:** A developer can define a materialized view from a stream, publish events, and query the current state with `SELECT ... WHERE ...` — zero infrastructure beyond NATS.

**Current focus:** v2.1 Code Stabilization — fix all 32 bugs/code quality issues from cr3.md code review verification

## Current Position

Phase: Phase 12 — Lifecycle & Core Correctness (not started)
Plan: —
Status: Roadmapped — 5 phases defined, ready for planning
Last activity: 2026-07-17 — Roadmap created

## Performance Metrics

| Metric | Value |
|--------|-------|
| Total phases | 5 |
| Completed phases | 0 |
| Total plans | 0 |
| Completed plans | 0 |
| Requirements mapped | 32/32 (100%) |
| Overall progress | 0% |

## Accumulated Context

### Decisions

| Decision | Rationale | Outcome |
|----------|-----------|---------|
| Verify before fixing | Prevents working on non-issues; each finding confirmed or dismissed first | ✓ Set |
| All cr3 findings in one milestone | Fix all bugs and smells from the code review verification in a single hardening pass | ✓ Set |
| Phase numbering continues from 12 | Sequential numbering across milestones | ✓ Set |
| 5 phases, coarse granularity | 32 requirements grouped into 5 delivery boundaries — lifecycle, SQL/config, transport/robustness, docs, cleanup/API gaps | ✓ Set |

### Phase Sequence Rationale

1. **Phase 12 (Lifecycle & Core Correctness)** — Foundation must be solid first: engine start/stop safety, panic recovery, PK encoding correctness
2. **Phase 13 (SQL & Config Hardening)** — Builds on stable engine; adds parse-time validation and config checks
3. **Phase 14 (Transport Safety & Robustness)** — Error handling and resource bounding on stable engine
4. **Phase 15 (Documentation & Contract Alignment)** — Fix docs after code is fixed (docs must match reality)
5. **Phase 16 (API Gaps & Cleanup)** — New API surface + cleanup; depends on engine/transport being solid

### Tech Debt Deferred

- Range scans (>, <, >=, <=)
- Secondary indexes on non-PK columns
- Delete/tombstone semantics for materialized rows
- Per-view KV buckets for full isolation
