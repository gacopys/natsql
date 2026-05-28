---
phase: 02-sql-query-engine-interfaces
plan: 01
subsystem: query-engine
tags: [vitess, sql-parser, kv-query, pk-lookup, full-scan]
requires:
  - phase: 01-foundation-materializer
    provides: kv package (ViewSchema, PkKey, LoadSchema), Engine lifecycle
provides:
  - SQL parsing via vitess (SELECT-only dialect)
  - Query validation against view schema (column existence)
  - Query planning (PKLookupPlan for PK equality, FullScanPlan otherwise)
  - Query execution (O(1) PK lookup, O(n) full scan with client-side filter)
  - Typed JSON result envelope (QueryResult with typed values)
affects:
  - 02-sql-query-engine-interfaces plan 02 (Engine integration + transport)
tech-stack:
  added:
    - vitess.io/vitess v0.24.1 (go/vt/sqlparser for SQL parsing)
  patterns:
    - Parse → Validate → Plan → Execute pipeline
    - PKLookupPlan for O(1) primary key lookups via kv.Get()
    - FullScanPlan for O(n) scans via kv.ListKeys() with client-side filter
    - projectRow/filterRow helpers for result projection and WHERE filtering
key-files:
  created:
    - natsql/query/types.go (ValidatedQuery, Condition, Op, Plan interface, PKLookupPlan, FullScanPlan, QueryResult)
    - natsql/query/parser.go (Parse function using vitess sqlparser)
    - natsql/query/parser_test.go (11 parser tests)
    - natsql/query/validate.go (Validate function checking columns against schema)
    - natsql/query/planner.go (BuildPlan producing PKLookupPlan or FullScanPlan)
    - natsql/query/executor.go (Execute methods for both plan types + projection/filter helpers)
    - natsql/query/query_test.go (18 tests: unit + integration with embedded NATS)
  modified:
    - natsql/go.mod (vitess + transitive deps added, go 1.25.0 → 1.26.2)
    - natsql/go.sum (vitess dependency tree)
key-decisions:
  - Use string-based fmt.Sprint comparison for WHERE filtering (handles mismatched concrete types from JSON unmarshal)
  - Composite key support via KeySeparator joining in BuildPlan
  - FullScanPlan returns empty slice (not nil) for empty results
  - Missing columns in projection set to explicit nil (not omitted) per D-31
patterns-established:
  - One-schema-per-view in validate: column lookup via map[string]bool
  - Embedding NATS server in integration tests via nats-server/v2
  - testSchema as package-level var for reuse across test functions
requirements-completed: [QRY-01, QRY-02, QRY-03]
duration: 35min
completed: 2026-05-28
---

# Phase 2 Plan 1: SQL Query Engine Core Summary

**SELECT-only SQL query engine over NATS KV with vitess parsing, PK and full-scan execution plans, typed JSON results, and 29 verified tests (unit + integration with embedded NATS)**

## Performance

- **Duration:** ~35 min
- **Started:** 2026-05-28T18:07:30Z
- **Completed:** 2026-05-28T18:42:00Z
- **Tasks:** 2 (TDD: 4 commits)
- **Files modified:** 9 (7 new, 2 modified)
- **Tests:** 29 total (11 parser, 4 validate, 4 plan, 10 integration)

## Accomplishments

- **SQL parser** using vitess: handles `SELECT *`, `SELECT col1,col2`, WHERE with `=`, `!=`, `IN`, `LIMIT N`, AND-connected conditions, missing WHERE rejection per D-24
- **Validator** that checks SELECT and WHERE column names against view schema per D-42/D-43
- **Query planner** producing `PKLookupPlan` for PK equality (O(1)) or `FullScanPlan` for all other queries (O(n))
- **PKLookupPlan executor** using `kv.Get()` for O(1) primary key lookups
- **FullScanPlan executor** using `kv.ListKeys()` with prefix filtering and client-side WHERE evaluation
- **Typed JSON output** via `QueryResult` envelope: strings quoted, numbers unquoted, booleans literal, null for missing columns
- **Composite key support**: PK lookup requires all key fields to have equality conditions
- **29 tests** including 11 parser unit tests, 4 validator unit tests, 4 planner unit tests, and 10 integration tests with embedded NATS

## Task Commits

Each task was committed atomically via TDD red-green style:

1. **Task 1 (RED): add failing parser tests** - `171f24a` (test)
2. **Task 1 (GREEN): implement SQL parser with vitess** - `34a3f1e` (feat)
3. **Task 2 (RED): add failing tests for validator, planner, executor** - `02e3034` (test)
4. **Task 2 (GREEN): implement validator, planner, executor** - `d0a5c52` (feat)

## Files Created/Modified

- `natsql/query/types.go` - Core types: ValidatedQuery, Condition, Op constants, Plan interface, PKLookupPlan, FullScanPlan, QueryResult
- `natsql/query/parser.go` - SQL parser using `vitess.io/vitess/go/vt/sqlparser` — Parse(sql) → *ValidatedQuery
- `natsql/query/parser_test.go` - 11 tests for all parser behaviors (SELECT *, cols, WHERE operators, LIMIT, errors)
- `natsql/query/validate.go` - Validate function checking columns against ViewSchema
- `natsql/query/planner.go` - BuildPlan producing PKLookupPlan or FullScanPlan based on WHERE conditions
- `natsql/query/executor.go` - Execute methods for both plan types + projectRow/filterRow helpers
- `natsql/query/query_test.go` - 18 tests covering validator, planner, and executor (unit + integration)
- `natsql/go.mod` - Added vitess.io/vitess v0.24.1, go 1.25.0 → 1.26.2
- `natsql/go.sum` - Vitess dependency checksums

## Decisions Made

- Used `NewTestParser()` from vitess (no custom parser options needed for v1 dialect)
- String-based `fmt.Sprint` comparison for WHERE filtering to handle mismatched types from JSON unmarshal (e.g., float64 vs int64)
- Composite key lookup requires ALL key fields to have equality conditions before producing PKLookupPlan
- FullScanPlan returns `[]map[string]any{}` (empty slice, not nil) for consistency with QueryResult envelope
- Missing projected columns get explicit `nil` value per D-31 (not omitted)
- Vitess `*sqlparser.Literal` for value extraction (replaced old `SQLVal` API from earlier vitess versions)
- Test data stored with `float64` for number columns (standard Go JSON unmarshal behavior)
- Integration tests share a `setupTestData` helper creating 4 test users in a KV bucket

## Deviations from Plan

None — plan executed exactly as written, with one test adjustment:

- **TestParseSelectColumns adjusted**: Original plan had `SELECT name, age FROM users` (no WHERE) expecting success. Changed to `SELECT name, age FROM users WHERE id = 'x'` to comply with D-24 (missing WHERE is rejected). Added companion test `TestParseRejectsNoWhereWithColumns` to verify explicit columns without WHERE also error.

## Issues Encountered

- **Vitess API mismatch**: Plan referenced `sqlparser.Parse(sql)` standalone and `SQLVal` type — vitess v0.24.1 uses `*sqlparser.Parser` with `NewTestParser().Parse(sql)` method, `*sqlparser.Literal` instead of `*SQLVal`, `ComparisonExprOperator` enum (int8) instead of string operator, `ColName` instead of `ColIdent`, and `Expr` interface changes. All resolved by inspecting actual vitess AST definitions.
- **Go version upgrade**: vitess v0.24.1 requires go >= 1.26.2, auto-upgrading go.mod from 1.25.0. This is within project constraints (Go 1.22+).

## Next Phase Readiness

- `natsql/query/` package fully ready for Plan 02 consumption
- Exported API: `Parse(sql)`, `Validate(q, schema)`, `BuildPlan(q, schema)`, `Plan.Execute(ctx, kvb)`, `QueryResult`
- Plan 02 (Engine integration + transport) can call `query.Parse → query.Validate → query.BuildPlan → plan.Execute`
- Integration test pattern (embedded NATS server) established and reusable
- FullScanPlan O(n) documentation needed for user-facing docs (performance characteristic per D-44)

## Self-Check: PASSED

- [x] All 8 plan files created/exist
- [x] All 4 commits present in git history
- [x] `go build ./...` compiles without errors
- [x] `go vet ./...` passes without warnings
- [x] All 29 tests pass (`go test ./query/...`)

---
*Phase: 02-sql-query-engine-interfaces*
*Completed: 2026-05-28*
