---
phase: 09-materializer-engine-lifecycle
plan: 01
subsystem: materializer
tags: [worker-pool, sequential-processing, goroutine, event-ordering, correctness, goroutine-panic-recover, event-timeout]

requires:
  - phase: 08-verification-foundation
    provides: BuildPkKey canonical PK encoder for KV writes
provides:
  - Sequential message processing per view (no concurrent worker pool)
  - Per-subject ordering guarantee for JetStream consumer
  - Drain handler compatibility with simplified architecture
  - Per-event timeout (30s) for stuck KV writes
  - Panic recovery in processing loop
affects: [09-02, 09-03]

tech-stack:
  added: []
  patterns:
    - "Sequential per-view processing: direct msgCtx.Next() → processEvent() loop"
    - "Per-event timeout: context.WithTimeout wrapping processEvent"
    - "Deferred recover in long-running goroutine for panic resilience"
    - "Simplified drain handler: separate goroutine calls msgCtx.Drain(), main loop waits"

key-files:
  created: []
  modified:
    - internal/materialize/materializer.go
    - internal/materialize/materializer_test.go

key-decisions:
  - "D-01: Remove 16-goroutine worker pool — single goroutine per view processes messages sequentially"
  - "D-02: Remove bridge goroutine — Messages() drives processing directly"
  - "D-03: Heartbeat goroutine kept unchanged (separate concern, no ordering impact)"
  - "D-04: No throughput optimization — deferred until shown necessary"

requirements-completed: [MAT-01]

duration: 20min
completed: 2026-06-01
---

# Phase 09: Materializer Engine Lifecycle — Plan 01 Summary

**Removed 16-goroutine worker pool, bridge goroutine, and buffered channel; events process sequentially in a single msgCtx.Next() → processEvent() loop, restoring JetStream per-subject ordering guarantee**

## Performance

- **Duration:** 20 min
- **Started:** 2026-06-01T21:20:00Z
- **Completed:** 2026-06-01T21:39:53Z
- **Tasks:** 1 (TDD: RED + GREEN)
- **Files modified:** 2

## Accomplishments
- Removed 16-goroutine worker pool that caused concurrent state corruption (CR-01 fix)
- Removed bridge goroutine and buffered channel — Messages() drives processing directly
- Added direct sequential processing loop: `msgCtx.Next()` → `processEvent()` in single goroutine
- Added per-event 30-second timeout to prevent stuck KV writes from blocking indefinitely (T-09-01-01)
- Added deferred recover in processing loop for panic resilience (T-09-01-02)
- Simplified drain handler: separate goroutine calls `msgCtx.Drain()` on drain signal, main loop waits for completion via `drainDone` channel
- Heartbeat goroutine kept unchanged (D-03)
- Added 3 TDD tests: single-goroutine verification, stream ordering preservation, heartbeat independence
- All 47 tests pass (44 existing + 3 new)

## Task Commits

Each task was committed atomically following TDD flow:

1. **Task 1 RED: Add failing sequential processing tests** - `c51e1b6` (test)
2. **Task 1 GREEN: Remove worker pool, process messages sequentially** - `8bf8611` (feat)

**Plan metadata:** `08dff57` (docs: complete plan — from orchestrator)

## Files Created/Modified
- `internal/materialize/materializer.go` — Sequential processing loop replaces bridge goroutine, worker pool, buffered channel. Import `"sync"` removed. Per-event timeout and panic recovery added.
- `internal/materialize/materializer_test.go` — Added 3 new tests: `TestSequentialProcessing_SingleGoroutine`, `TestSequentialProcessing_StreamOrder`, `TestSequentialProcessing_HeartbeatIndependent`. Added `goroutineID()` helper.

## Decisions Made
- Decision D-01 (ordered processing): Remove 16-goroutine worker pool entirely. Single goroutine processes messages sequentially from the consumer.
- Decision D-02 (no bridge): The bridge goroutine that fed messages into the buffered channel is also removed — `Messages()` drives processing directly.
- Decision D-03 (keep heartbeat): The heartbeat logging goroutine is kept — separate concern, no ordering impact.
- Decision D-04 (no optimization): No throughput optimization implemented. If needed later, optimize via batching or per-key partitioning — not before.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 2 - Missing Critical] Added per-event timeout for processEvent (T-09-01-01)**
- **Found during:** Task 1 (implementation)
- **Issue:** The threat register assigned `mitigate` disposition to T-09-01-01 — a slow/stuck processEvent blocks all subsequent messages indefinitely
- **Fix:** Wrapped `processEvent` call with `context.WithTimeout(ctx, 30*time.Second)` to prevent a single stuck KV write from blocking indefinite
- **Files modified:** internal/materialize/materializer.go
- **Verification:** Code review confirms timeout context is created before and cancelled after each processEvent call
- **Committed in:** 8bf8611 (GREEN commit)

**2. [Rule 2 - Missing Critical] Added deferred panic recovery in processing loop (T-09-01-02)**
- **Found during:** Task 1 (implementation)
- **Issue:** The threat register assigned `mitigate` disposition to T-09-01-02 — if processEvent panics, the entire processing goroutine crashes without recovery
- **Fix:** Added `defer func() { if r := recover() { ... } }()` before the processing loop to log panic and allow orderly shutdown
- **Files modified:** internal/materialize/materializer.go
- **Verification:** Code review confirms deferred recover is in scope for the entire processing loop
- **Committed in:** 8bf8611 (GREEN commit)

---

**Total deviations:** 2 auto-fixed (2 missing critical — threat register mitigations)
**Impact on plan:** Both are correctness/security mitigations directly from the plan's threat model. No scope creep. Essential for production readiness.

## Issues Encountered
- None. TDD flow executed cleanly — RED tests confirmed the worker pool was processing events in different goroutines, GREEN implementation fixed it to single goroutine.

## User Setup Required

None — no external service configuration required. All changes are internal to the materializer's processing architecture.

## Next Phase Readiness
- Materializer now processes events sequentially per view — ready for error classification (09-02)
- Drain handler architecture is simplified and compatible with the new sequential loop
- Heartbeat remains operational as an independent goroutine

---
*Phase: 09-materializer-engine-lifecycle*
*Completed: 2026-06-01*
