---
phase: 08-verification-foundation
plan: 02
subsystem: query
tags: [sql-parser, vitess, validation, error-handling]

requires:
  - phase: 07-natsql-engine
    provides: existing Parse() implementation with vitess sqlparser

provides:
  - Parse-time rejection of DISTINCT, ORDER BY, GROUP BY, HAVING with explicit errors
  - Parse-time rejection of non-column SELECT expressions (aggregations, literals, arithmetic, function calls)
  - extractSelectExprs now returns error for unsupported expressions instead of silently ignoring them
  - Negative test suite covering each rejected construct

affects:
  - 09-foundation
  - 10-transport-foundation

tech-stack:
  added: []
  patterns:
    - "Rejection checks ordered by SQL clause precedence: DISTINCT → ORDER BY → GROUP BY → HAVING → SELECT expressions"
    - "Error message format: 'unsupported: {CONSTRUCT} is not supported in v1' for clauses"
    - "Error message format: 'unsupported SELECT expression: only simple column references and * are supported in v1 (got {TYPE})' for select expressions"

key-files:
  created: []
  modified:
    - internal/query/parser.go (255 lines, +24/-11)
    - internal/query/parser_test.go (608 lines, +98/-6)

key-decisions:
  - "D-08: Reject ALL unsupported constructs with explicit error messages — DISTINCT, ORDER BY, GROUP BY, HAVING, aggregations, subqueries, non-column SELECT expressions"
  - "D-09: Whitelist-only: only SELECT <col> [, <col>]* and SELECT * are accepted"
  - "D-10: Error messages name the rejected construct: 'unsupported: {CONSTRUCT} is not supported in v1'"

patterns-established:
  - "Clause rejection before SELECT expression extraction — catches DISTINCT/ORDER BY/GROUP BY/HAVING before parsing select expressions"
  - "extractSelectExprs returns ([]string, error) — callers must handle errors for unsupported expressions"

requirements-completed: [FND-02, VER-01]

duration: 8min
completed: 2026-06-01
---

# Phase 08 Plan 02: Unsupported SQL Rejection Summary

**FND-02: Parse-time rejection of DISTINCT, ORDER BY, GROUP BY, HAVING, aggregations, subqueries, and non-column SELECT expressions with explicit error messages naming the rejected construct — whitelist-only approach per D-09, D-10**

## Performance

- **Duration:** 8 min
- **Started:** 2026-06-01T14:12:00Z (approx)
- **Completed:** 2026-06-01T14:20:00Z (approx)
- **Tasks:** 2
- **Files modified:** 2

## Accomplishments

- Added DISTINCT/ORDER BY/GROUP BY/HAVING rejection checks in `Parse()` — caught before SELECT expression extraction so clause errors fire first
- Updated `extractSelectExprs` to return `([]string, error)` — returns explicit error for non-column expressions instead of silently ignoring them
- Error messages follow D-10 format: `"unsupported: {CONSTRUCT} is not supported in v1"` for clauses and `"unsupported SELECT expression: only simple column references and * are supported in v1 (got {TYPE})"` for select expressions
- 9 new test functions covering each rejected construct + valid query regression guard
- All 62 existing tests continue to pass — zero regressions

## Task Commits

Each task was committed atomically:

1. **Task 1 RED: Test phase — write failing rejection tests** - `8aeff03` (test)
2. **Task 1 GREEN: Implementation — rejection checks in Parse()** - `59c3d8b` (feat)
3. **Task 2: Negative tests** — completed inline with Task 1 RED phase (all 9 test functions present and passing)

**Plan metadata:** (committed in final docs step)

_Note: Task 1 used TDD (RED → GREEN). Task 2's test functions were written during Task 1's RED phase._

## Files Created/Modified

- `internal/query/parser.go` (`+24/-11`) — Added DISTINCT/ORDER BY/GROUP BY/HAVING checks; updated extractSelectExprs to return error for non-column expressions; moved SELECT extraction after rejection checks
- `internal/query/parser_test.go` (`+98/-6`) — Added 9 new test functions (8 rejection tests + 1 regression guard); updated 2 existing extractSelectExprs tests for new return signature; added `strings` import

## Decisions Made

- **D-08/D-09/D-10 followed exactly:** Error format matches the specified pattern; whitelist-only approach for SELECT expressions; each rejected construct named explicitly
- **Rejection order:** DISTINCT → ORDER BY → GROUP BY → HAVING → SELECT expression — clauses are checked before expression extraction so higher-level constructs are caught first

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Fixed DISTINCT type mismatch (bool vs string)**
- **Found during:** Task 1 GREEN (implementation)
- **Issue:** Plan's action section specified `sel.Distinct != ""` but vitess `Distinct` is a `bool` field — would not compile
- **Fix:** Changed to `sel.Distinct` (direct bool check)
- **Files modified:** `internal/query/parser.go`
- **Verification:** `go build` passes, tests pass
- **Committed in:** `59c3d8b` (Task 1 GREEN commit)

**2. [Rule 1 - Bug] Fixed HAVING test query that also had GROUP BY**
- **Found during:** Task 1 GREEN (test execution)
- **Issue:** `TestParse_RejectsHaving` used `SELECT name FROM users WHERE id = 'x' GROUP BY name HAVING COUNT(*) > 1` — GROUP BY check fires first, so test never reaches HAVING check. Test failed because error mentioned "GROUP BY" not "HAVING"
- **Fix:** Changed test query to `SELECT * FROM users WHERE id = 'x' HAVING COUNT(*) > 1` (no GROUP BY clause)
- **Files modified:** `internal/query/parser_test.go`
- **Verification:** HAVING test now passes, error correctly mentions "HAVING"
- **Committed in:** `59c3d8b` (Task 1 GREEN commit)

---

**Total deviations:** 2 auto-fixed (both Rule 1 — plan action code had minor issues)
**Impact on plan:** Both auto-fixes were necessary for correctness. No scope creep.

## Issues Encountered

None beyond the two auto-fixed issues documented above.

## User Setup Required

None — no external service configuration required.

## Next Phase Readiness

- Parser now rejects all unsupported v1 SQL constructs with explicit errors
- Valid queries (`SELECT *, SELECT col1, col2`, WHERE, LIMIT, IN) continue to work identically
- Ready for downstream phases (09, 10) that depend on hardened parser

---

*Phase: 08-verification-foundation*
*Completed: 2026-06-01*

## Self-Check: PASSED

| Check | Status |
|-------|--------|
| `parser.go` exists (255 ≥ 240 lines) | ✅ |
| `parser_test.go` exists (608 ≥ 520 lines) | ✅ |
| RED commit `8aeff03` exists | ✅ |
| GREEN commit `59c3d8b` exists | ✅ |
| `go test ./internal/query/` — all 62 tests pass | ✅ |
| `go build ./internal/query/` — clean | ✅ |
| `go vet ./internal/query/` — clean | ✅ |
| SUMMARY.md created | ✅ |
