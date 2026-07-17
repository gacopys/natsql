---
gsd_state_version: 1.0
milestone: v2.1
milestone_name: Code Stabilization
status: defining
stopped_at: null
last_updated: "2026-07-17T00:00:00.000Z"
last_activity: 2026-07-17 -- v2.1 milestone started
progress:
  total_phases: 0
  completed_phases: 0
  total_plans: 0
  completed_plans: 0
  percent: 0
---

# Project State

## Project Reference

See: .planning/PROJECT.md (updated 2026-07-17 after v2.1 milestone started)

**Core value:** A developer can define a materialized view from a stream, publish events, and query the current state with `SELECT ... WHERE ...` — zero infrastructure beyond NATS.

**Current focus:** Defining requirements for v2.1 Code Stabilization

## Current Position

Phase: Not started (defining requirements)
Plan: —
Status: Defining requirements
Last activity: 2026-07-17 — Milestone v2.1 started

## Accumulated Context

### Decisions

| Decision | Rationale | Outcome |
|----------|-----------|---------|
| Verify before fixing | Prevents working on non-issues; each finding confirmed or dismissed first | ✓ Set |
| All cr3 findings in one milestone | Fix all bugs and smells from the code review verification in a single hardening pass | ✓ Set |
| Phase numbering continues from 12 | Sequential numbering across milestones | ✓ Set |

### Tech Debt Deferred

- Range scans (>, <, >=, <=)
- Secondary indexes on non-PK columns
- Delete/tombstone semantics for materialized rows
- Per-view KV buckets for full isolation
