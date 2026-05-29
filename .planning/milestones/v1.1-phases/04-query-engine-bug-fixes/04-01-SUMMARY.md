---
phase: 04-query-engine-bug-fixes
plan: 01
subsystem: query-engine
tags: [bug-fix, pk-filter, data-race, type-comparison, boolean-parsing]

requires:
  - phase: 02-sql-query-engine-interfaces
    provides: SQL parser, planner, executor, validator types
provides:
  - PKLookupPlan non-PK WHERE post-filter (FIX-ENG-01)
  - Mutex-protected Engine.kv lazy-init (FIX-ENG-02)
  - Type-aware valuesEqual in filterRow (FIX-ENG-03)
  - Boolean literal support in SQL parser (FIX-ENG-04)
affects: []

tech-stack:
  added: []
  patterns:
    - Type-safe value comparison (valuesEqual) instead of fmt.Sprint coercion

key-files:
  modified:
    - natsql/query/types.go — added Where []Condition to PKLookupPlan
    - natsql/query/planner.go — pass non-PK conditions to PKLookupPlan
    - natsql/query/executor.go — post-filter + valuesEqual type-aware comparison
    - natsql/query/parser.go — BoolVal case in extractValue
    - natsql/engine/engine.go — sync.Mutex guard on e.kv, HTTP timeouts
    - natsql/query/parser_test.go — expanded tests for BoolVal, NullVal, parse errors
    - natsql/query/executor_test.go — tests for valuesEqual, filterRow, projectRow

key-decisions:
  - "valuesEqual normalizes int64→float64 for JSON compatibility (JSON numbers decode as float64)"
  - "PKLookupPlan.Where stores only non-PK conditions — PK conditions already satisfied by key lookup"
  - "BoolVal case returns native bool type, not string, enabling proper type-safe comparison"

patterns-established:
  - "Type-safe comparison in filter layer (valuesEqual) instead of fmt.Sprint coercion"
  - "Mutex-protected lazy-init for engine components"

requirements-completed: [FIX-ENG-01, FIX-ENG-02, FIX-ENG-03, FIX-ENG-04]
duration: 1 session
completed: 2026-05-29
---

# Phase 04 Plan 01: Query Engine Bug Fixes Summary

**All four query engine bugs from the v1.0 code review fixed: PK post-filter applied, data race on Engine.kv eliminated, type-aware comparison replacing fmt.Sprint, and boolean literal support in SQL parser.**

## Performance

- **Duration:** 1 session
- **Completed:** 2026-05-29
- **Tasks:** 4 (all fix commits in `ceb57f0`)
- **Files modified:** 7 source files + test files

## Accomplishments

- **FIX-ENG-01 — PK post-filter**: Added `Where []Condition` to `PKLookupPlan`. `BuildPlan` passes non-PK conditions through. `Execute` applies them as a post-filter after the PK `Get()` succeeds. `SELECT * FROM users WHERE id = 'abc' AND name = 'Bob'` now correctly returns empty if name doesn't match.
- **FIX-ENG-02 — Data race**: Added `sync.Mutex` to `Engine` struct. Locks around `e.kv == nil` check + `InitBucket` call in `Query()`. Passes `go test -race` on concurrent access.
- **FIX-ENG-03 — Type-aware comparison**: Replaced `fmt.Sprint`-based comparison in `filterRow` with `valuesEqual` — a type-aware function that handles float64/int64/bool/string cross-type comparison. Normalizes int64↔float64 for JSON compatibility. Eliminates false matches like `30` equal to `"30"`.
- **FIX-ENG-04 — Boolean literal parsing**: Added `*sqlparser.BoolVal` case in `extractValue` returning native `bool` type. `WHERE active = true` and `WHERE active = false` now parse correctly.

## Task Commits

1. **Task 1-4: All fixes** — `ceb57f0` (feat: v1.1 tech debt cleanup)

## Files Modified

- `natsql/query/types.go` — `PKLookupPlan.Where` field
- `natsql/query/planner.go` — non-PK condition passthrough in BuildPlan
- `natsql/query/executor.go` — post-filter, valuesEqual, projectRow fixes
- `natsql/query/parser.go` — BoolVal case in extractValue
- `natsql/engine/engine.go` — sync.Mutex + HTTP timeouts
- `natsql/query/parser_test.go` — extensive parser unit test expansion
- `natsql/query/executor_test.go` — valuesEqual and filterRow unit tests

## Decisions Made

- **valuesEqual int64↔float64 normalization**: JSON numbers decode as float64, but Go code may produce int64. Both should compare equal for the same numeric value.
- **PKLookupPlan.Where only stores non-PK conditions**: PK conditions are already satisfied by the key lookup. Only non-PK conditions need post-filter.

## Deviations from Plan

None — all 4 tasks implemented as planned.

## Issues Encountered

- **BoolVal is not a pointer type**: The `*sqlparser.BoolVal` case doesn't work because `BoolVal` is a non-pointer type in vitess. Fixed by using `sqlparser.BoolVal` directly in the type switch.

## Threat Surface Scan

No new security surfaces introduced. The post-filter and type-aware comparison are internal query engine changes with no external-facing impact.

## Self-Check: PASSED

- [x] All fixes implemented in codebase
- [x] `go build ./...` passes
- [x] `go test ./... -count=1` passes
- [x] `go test -race ./...` passes (race condition fixed)
- [x] SUMMARY.md created at expected path

---

*Phase: 04-query-engine-bug-fixes*
*Plan: 01*
*Completed: 2026-05-29*
