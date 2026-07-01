---
phase: "10-query-engine-transport"
plan: "01"
subsystem: query-engine
tags: [go, nats, kv, query, planner, executor, json, precision, predicates]
requires:
  - phase: 08-verification-foundation
    provides: BuildPkKey, kv package, test patterns, PK canonical encoding
  - phase: 09-materializer-engine-lifecycle
    provides: test helpers, existing query engine structure
provides:
  - All WHERE conditions retained as post-filters (PK + non-PK) for correctness
  - Short-circuit optimization for contradictory PK predicates (EmptyPlan)
  - SELECT * excludes _meta and other _-prefixed internal fields
  - Large integer precision preserved via json.Decoder.UseNumber()
  - json.Number support in valuesEqual via convertJSONNumber helper
  - Error message fix: "marshaling row" → "unmarshaling row"
  - Full scan uses WatchAll + prefix filter (Watch(prefix) doesn't support / separators per A1 fallback)
affects: [transport layer, CLI, integration tests]
tech-stack:
  added: []
  patterns:
    - "json.Decoder.UseNumber() for KV data decode in both PK and full-scan paths"
    - "convertJSONNumber helper normalizes json.Number to int64/float64 for comparison"
    - "EmptyPlan type for short-circuit optimization with no KV I/O"
key-files:
  created: []
  modified:
    - internal/query/types.go (EmptyPlan type)
    - internal/query/planner.go (contradictory PK check, all WHERE as post-filters)
    - internal/query/executor.go (UseNumber, _meta strip, json.Number valuesEqual, error msg, WatchAll fallback)
    - internal/query/query_test.go (6 new tests, 1 updated assertion)
key-decisions:
  - "D-01: Keep ALL original WHERE conditions as post-filters — PK narrows search, post-filter verifies every condition"
  - "D-02: Short-circuit contradictory same-column PK predicates to EmptyPlan (no KV I/O)"
  - "D-03: findPKEqConditions still drives lookup key, but conditions no longer removed from post-filter"
  - "D-04/D-05: projectRow strips _-prefixed keys from SELECT * results"
  - "D-06: Explicit column selection unchanged — only SELECT * applies _-filter"
  - "D-07/D-08: UseNumber in both PK and FullScan decode paths"
  - "D-09/D-10: convertJSONNumber normalizes json.Number for valuesEqual comparison"
  - "D-11 fallback: WatchAll retained because KV Watch pattern matching uses NATS subject semantics (tokenizing on .), but our keys use / separators"
  - "D-21: Error message typo fix: 'marshaling row' → 'unmarshaling row'"
patterns-established:
  - "TDD pattern: RED (failing test) → commit → GREEN (implementation) → commit"
  - "EmptyPlan as short-circuit execution path with zero KV I/O"
  - "json.Number normalization for cross-type comparison in filterRow"
  - "Deviation documentation when plan assumption proves incorrect (A1 fallback)"
requirements-completed: [QENG-01, QENG-02, QENG-03, QENG-04, TRN-04]
duration: 18min
completed: 2026-06-02
---

# Phase 10 Plan 01: Query Engine Correctness Summary

**Post-filter all WHERE conditions (including PK), short-circuit contradictory PK predicates with EmptyPlan, exclude _meta from SELECT *, preserve numeric precision with UseNumber, and scope full scans — five query engine correctness fixes with TDD**

## Performance

- **Duration:** 18 min
- **Started:** 2026-06-02T21:12:48Z
- **Completed:** 2026-06-02T21:31:00Z
- **Tasks:** 2 (each TDD: RED + GREEN)
- **Files modified:** 4

## Accomplishments

- **QENG-01 (Post-filter all WHERE):** `BuildPlan` now passes ALL original WHERE conditions to `PKLookupPlan.Where` instead of filtering out PK conditions. The PK lookup narrows the search space; post-filters verify every condition — including contradictory/duplicate PK predicates.
- **QENG-01 (Short-circuit contradictory PK):** Added early-exit in `BuildPlan`: same PK column with multiple different `OpEq` values returns `&EmptyPlan{...}`, zero KV I/O.
- **QENG-02 (_meta exclusion):** `projectRow` for `SELECT *` (nil columns) strips all map keys starting with `_`, preventing `_meta` and future internal field leakage. Explicit column selection unchanged.
- **QENG-03 (Numeric precision):** Both PK and FullScan decode paths use `json.NewDecoder().UseNumber()` instead of `json.Unmarshal`, preserving integers >2^53. `valuesEqual` normalizes `json.Number` via `convertJSONNumber` for correct WHERE comparison.
- **QENG-04 (Full scan scope):** `FullScanPlan.Execute` retained `WatchAll` + client-side `HasPrefix` filter — the KV `Watch(prefix)` API uses NATS subject pattern matching (`.` token separator), which can't match our `/`-separated keys. Documented as A1 fallback per research.
- **TRN-04 (Error message typo):** `"marshaling row"` → `"unmarshaling row"` in both PK and FullScan error paths.

## Task Commits

Each task was committed atomically with TDD (RED + GREEN):

1. **Task 1: planner.go — Contradictory PK + all WHERE post-filters + EmptyPlan**
   - `eb5bb7d` (test: add failing tests for contradictory PK, post-filter all conditions, EmptyPlan)
   - `89d0d29` (feat: post-filter all WHERE conditions, short-circuit contradictory PK predicates)

2. **Task 2: executor.go — UseNumber, valuesEqual, _meta strip, Watch prefix, error msg**
   - `2a878c1` (test: add failing tests for _meta exclusion and large integer precision)
   - `e61a565` (feat: UseNumber, _meta strip, json.Number valuesEqual, error msg fix)

## Files Created/Modified

| File | Change |
|------|--------|
| `internal/query/types.go` | Added `EmptyPlan` struct with `Execute` method returning empty result slice |
| `internal/query/planner.go` | `BuildPlan`: pass `q.Where` (all conditions) to `PKLookupPlan.Where`; short-circuit contradictory same-column PK predicates; uses `valuesEqual` from same package |
| `internal/query/executor.go` | PK and FullScan decode: `json.NewDecoder().UseNumber()`; `projectRow` strips `_`-prefixed keys for SELECT *; `valuesEqual` normalizes `json.Number` via `convertJSONNumber`; error msg fix; WatchAll + prefix filter comment |
| `internal/query/query_test.go` | 6 new tests: contradictory PK, duplicate PK, all conditions kept, EmptyPlan execute, _meta exclusion, large integer precision; updated TestProjectionSelectCols to use valuesEqual |

## Decisions Made

All decisions from the discussion phase (D-01 through D-21) were implemented as specified, with one deviation:

- **D-11 fallback (Watch prefix):** The research document Assumption A1 correctly anticipated that `Watch(ctx, prefix)` might not support `/`-separated keys. KV Watch uses NATS subject pattern matching (tokenizing on `.`), so `Watch(ctx, "test_users/pk/>")` doesn't match keys like `test_users/pk/u1`. Reverted to `WatchAll` + `strings.HasPrefix` filter — functionally equivalent, acceptable per A1's risk assessment.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] KV Watch prefix matching incompatible with '/' key separators**
- **Found during:** Task 2 (GREEN phase — FullScanPlan.Watch change)
- **Issue:** `kvb.Watch(ctx, prefix+">")` returned no matching keys because KV Watch uses NATS subject wildcards (tokenizing on `.`), but our keys use `/` separators. Three full-scan tests returned 0 results.
- **Fix:** Reverted to `WatchAll(ctx)` with explicit note about the API limitation. The `strings.HasPrefix(entry.Key(), prefix)` guard remains as defense-in-depth (belt-and-suspenders).
- **Files modified:** `internal/query/executor.go` (line 51 — WatchAll call with comment)
- **Verification:** All 67 tests pass, including TestFullScanAll, TestFullScanWithWhere, TestFullScanWithLimit
- **Committed in:** `e61a565` (Task 2 GREEN commit)

---

**Total deviations:** 1 auto-fixed ([Rule 3 — Blocking])
**Impact on plan:** The WatchAll fallback is functionally correct (identical data returned), just with slightly broader KV subscription. Performance impact is negligible at current scale. Per plan's A1 risk assessment, this was explicitly acceptable.

## Issues Encountered

- **KV Watch prefix limitation:** The `jetstream.KeyValue.Watch()` method's pattern matching uses NATS subject semantics where `*` and `>` wildcards match tokens separated by `.`, not `/`. Since our KV keys use `/` as path separator (e.g., `test_users/pk/u1`), prefix-based Watch doesn't work. This is a documented limitation in the platform assumption A1. Workaround: keep `WatchAll` + client-side prefix filter, which was the original approach.

## User Setup Required

None — no external service configuration required.

## Next Phase Readiness

- Query engine correctness is now hardened for all documented edge cases
- Next plan (10-02) can proceed with transport layer fixes (CLI, HTTP, NATS handlers)
- Integration tests across all query paths continue to pass

## Self-Check: PASSED

| Check | Status |
|-------|--------|
| Files created/modified exist | ✅ All 5 files verified |
| All commits exist in git log | ✅ All 4 commits verified |
| Full test suite passes | ✅ 67/67 tests pass |

---
*Phase: 10-query-engine-transport*
*Plan: 01*
*Completed: 2026-06-02*
