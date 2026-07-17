---
phase: 09-materializer-engine-lifecycle
plan: 03
subsystem: engine
tags: [engine, lifecycle, startup, http, config, error-propagation]

# Dependency graph
requires:
  - phase: 09-materializer-engine-lifecycle
    provides: engine constructors, Start/Close lifecycle
provides:
  - Engine constructors read HTTP port from cfg.HTTP.Port (LIFE-01/CR-06)
  - Engine.Start() propagates all startup errors synchronously (LIFE-02/CR-07)
  - HTTP listener bound synchronously via net.Listen before goroutine
  - Materializer consumer setup errors caught via channel + 500ms best-effort wait
  - NATS handler registration failure is fatal (Start returns error)
  - e.started = true only after ALL startup steps succeed
affects: [10-query-engine-predicates]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "Startup error propagation via labeled loop with time.After timeout"
    - "Synchronous net.Listen before goroutine for HTTP server"

key-files:
  created: []
  modified:
    - internal/engine/engine.go

key-decisions:
  - "D-12/D-13: Engine constructors read cfg.HTTP.Port with 8080 fallback when zero"
  - "D-14: All three startup failure modes are fatal (HTTP bind, materializer setup, NATS handler)"
  - "D-15: HTTP uses net.Listen before goroutine; materializer errors via channel; NATS handler error is fatal"
  - "D-16: Engine not marked started unless all steps succeed"

patterns-established:
  - "Constructors initialize derived state from Config struct with defensive zero-value fallback"
  - "Startup sequence uses synchronous checks for external resources before goroutine dispatch"
  - "Materializer goroutines report setup errors via buffered channel with timeout-based health check"

requirements-completed: [LIFE-01, LIFE-02]

# Metrics
duration: 6min
completed: 2026-06-01
---

# Phase 09 Plan 03: Engine Lifecycle — HTTP Port Wiring & Startup Error Propagation

**Engine constructors now read HTTP port from config (cfg.HTTP.Port) with 8080 fallback; Start() propagates all startup errors synchronously — HTTP bind, materializer consumer setup, and NATS handler registration all cause Start() to return an error; engine is only marked started on full success.**

## Performance

- **Duration:** 6 min
- **Started:** 2026-06-01T21:36:00Z
- **Completed:** 2026-06-01T21:42:24Z
- **Tasks:** 2
- **Files modified:** 1

## Accomplishments

- **CR-06 fixed:** Engine constructors (`New`, `NewEmbedded`) now read `queryPort` from `cfg.HTTP.Port` instead of hardcoding `8080`, with defensive 8080 fallback when zero (covers direct struct construction in tests bypassing SetDefaults)
- **CR-07 fixed:** `Engine.Start()` no longer returns `nil` when core services are down — HTTP bind uses synchronous `net.Listen`, materializer consumer setup errors propagate via buffered channel with 500ms best-effort wait, NATS handler registration failure returns error immediately
- **Safety guarantee:** `e.started = true` is only set after all startup steps succeed, preventing use of a partially-initialized engine

## Task Commits

Each task was committed atomically:

1. **Task 1: Wire HTTP port from config in engine constructors** - `c7a0a1c` (feat)
2. **Task 2: Synchronous startup error propagation in Start()** - `04c6e0a` (fix)

## Files Created/Modified

- `internal/engine/engine.go` — Updated `New()`, `NewEmbedded()`, and `Start()` methods

## Decisions Made

- Followed plan exactly per D-12 through D-16 from the phase context
- NATS handler failure is fatal per D-14 (overrides ARCHITECTURE.md §2.4.1 which recommended non-fatal logging)
- Materializer startup check uses 500ms best-effort timeout per D-15 and threat model T-09-03-03

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Fixed time.After() channel blocking in startup loop with multiple views**
- **Found during:** Task 2 (startup error propagation implementation)
- **Issue:** `time.After()` creates a single-shot channel that fires once. After consuming its value in the first loop iteration, the second iteration blocked forever waiting for the `startupCh` channel. This caused `TestEngineMultipleViews` to hang for 30s until the test context expired.
- **Fix:** Replaced the plain `for` loop with a labeled loop (`startupLoop:`) and `break startupLoop` on timeout. Once the timeout fires, all remaining views are considered healthy without blocking.
- **Files modified:** `internal/engine/engine.go`
- **Verification:** `TestEngineMultipleViews` passes in ~2.5s (was timing out at 30s). All 21 engine tests pass. Race detector passes.
- **Committed in:** `04c6e0a` (Task 2 commit)

---

**Total deviations:** 1 auto-fixed (1 bug)
**Impact on plan:** Bug fix essential for correctness with multi-view configurations. No scope creep.

## Issues Encountered

- **time.After() with for-loop over multiple views:** The plan originally specified a simple `for i := 0; i < len(e.cfg.Views); i++` loop with a `select` between `<-startupCh` and `<-afterStartup`. However, `time.After()` creates a channel that delivers exactly one value. After the first iteration consumes it, subsequent iterations block on `startupCh` indefinitely. Fixed with labeled `break startupLoop`.

## Threat Flags

None — no new security-relevant surface introduced beyond what's documented in the plan's threat model.

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness

- Engine lifecycle hardened against silent startup failures
- HTTP port properly wired from config
- Ready for Phase 10 (query engine predicates) and any further engine enhancements

## Self-Check: PASSED

- [x] All files exist: `internal/engine/engine.go`, SUMMARY.md
- [x] Both commits verified: `c7a0a1c` (Task 1), `04c6e0a` (Task 2)
- [x] `net.Listen` in Start() at line 315 (synchronous bind)
- [x] `e.started = true` at line 339 (after all checks)
- [x] `queryPort: cfg.HTTP.Port` appears 2 times (New + NewEmbedded)
- [x] `go build ./internal/engine/` passes
- [x] `go vet ./internal/engine/` passes
- [x] `go test ./internal/engine/ -count=1 -timeout 120s` — all 21 tests pass (24.3s)

---
*Phase: 09-materializer-engine-lifecycle*
*Completed: 2026-06-01*
