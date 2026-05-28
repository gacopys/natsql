---
phase: 02-sql-query-engine-interfaces
status: issues-found
reviewed: 2026-05-28
findings: 21 (1 critical, 1 high, 10 medium, 9 low)
---

# Code Review: Phase 02 — SQL Query Engine + Interfaces

## Overall Health: ISSUES FOUND

1 critical, 1 high, 10 medium, 9 low.

## Critical

| # | File:Line | Issue |
|---|-----------|-------|
| C1 | `query/planner.go:28-45`, `executor.go:17-33` | PKLookupPlan silently drops non-PK WHERE conditions (e.g., `WHERE id = 'abc' AND name = 'Bob'` returns row regardless of `name`). Needs post-filter. |

## High

| # | File:Line | Issue |
|---|-----------|-------|
| H1 | `engine/engine.go:245-253` | Data race on `Engine.kv` — `Query()` reads/writes `e.kv` without mutex. |

## Medium (10 items)

M1-M10: No context timeout in NATS handler, `ListKeys()` watcher not stopped on early return, `fmt.Sprint` type confusion in filterRow, silent drop of unsupported SELECT expressions, JSON injection via `fmt.Sprintf`, missing HTTP body size limit, nil `e.js` panic, PK value separator handling, error results use `"results":null` instead of `[]`, `NewTestParser()` used instead of production parser.

## Low (9 items)

L1-L9: Ignored errors, test flakiness from `time.Sleep`, shadowed context param, missing `Stop()` calls.

## Recommendations

1. **Fix C1**: Add `Where []Condition` to `PKLookupPlan` and apply `filterRow()` after PK lookup
2. **Fix H1**: Add mutex protection in `Query()`
3. **Fix M3**: Use type-aware comparison in `filterRow` instead of `fmt.Sprint`
4. **Fix M5**: Use `json.Marshal` instead of `fmt.Sprintf` for JSON error responses
5. **Fix M9**: Normalize nil to `[]` in error results
