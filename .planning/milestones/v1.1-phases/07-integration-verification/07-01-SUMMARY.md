---
phase: 07-integration-verification
plan: 01
subsystem: cross-cutting
tags: [testing, ci, integration, benchmark]

requires:
  - phase: 04-query-engine-bug-fixes
  - phase: 05-materializer-hardening
  - phase: 06-transport-code-health
provides:
  - Black-box integration test suite (natsql_blackbox_test.go)
  - GitHub Actions CI workflow
  - Coverage tests for all components
  - Performance benchmark example
affects: []

tech-stack:
  added:
    - github.com/nats-io/nats-server/v2 — embedded NATS for black-box tests
  patterns:
    - Black-box testing through public API only (no internal state inspection)
    - Deterministic test data with computed expectations

key-files:
  created:
    - natsql/natsql_blackbox_test.go — 750-line black-box test suite with 30-row dataset
    - natsql/cfg/config_test.go — 20 config coverage tests
    - natsql/engine/engine_test.go — 23 engine coverage tests
    - natsql/kv/kv_test.go — 20 KV coverage tests
    - .github/workflows/ci.yml — GitHub Actions CI pipeline
    - examples/07-perf-benchmark/main.go — performance benchmark
  modified:
    - natsql/materialize/mapper_test.go — 24 mapper tests
    - natsql/query/parser_test.go — 39 parser unit tests
    - natsql/transport/transport_test.go — 6 transport tests

key-decisions:
  - "Black-box tests go through public Query() method only — no internal state inspection"
  - "Test data is fully deterministic with 30 rows across 6 cities — expected results are known in advance"
  - "Separate black-box CI step for isolation from unit tests"

patterns-established:
  - "Black-box testing pattern for end-to-end validation"
  - "Deterministic test data with filtering expectations computed from source data"
  - "CI with build, vet, test -race, coverage, and black-box steps"

requirements-completed: []
duration: 1 session
completed: 2026-05-29
---

# Phase 07 Plan 01: Integration Verification Summary

**Full integration test suite, CI pipeline, and performance benchmark ensuring all Phase 4-6 fixes work together without regressions.**

## Performance

- **Duration:** 1 session
- **Completed:** 2026-05-29
- **Tasks:** 4
- **Files modified:** 10+ files

## Accomplishments

- **Black-box integration tests**: 750-line test suite (`natsql_blackbox_test.go`) with 30-row deterministic dataset. Covers PK lookups, projections, WHERE/AND/IN/!=/LIMIT filters, error cases (no WHERE, unknown column, unknown view, malformed SQL), composite keys (org_id|order_id), type integrity (string/float64/bool), and JSON marshal roundtrip. All assertions through public `Query()` API only.
- **GitHub Actions CI**: Full CI pipeline at `.github/workflows/ci.yml` — build, `go vet`, `go test -race ./...`, coverage reporting, dedicated black-box test step with verbose output. Runs on every PR to master.
- **Coverage tests**: 139+ new tests across all packages covering edge cases in config validation, engine lifecycle, KV operations, mapper (sanitization, precision), parser (BoolVal, NullVal, parse errors, extractConditions, literalToGo), executor (valuesEqual, filterRow, projectRow), and transport (body size limit).
- **Performance benchmark**: Example benchmark at `examples/07-perf-benchmark/main.go` for baseline verification and regression detection.
- **`go vet` clean**: No vet warnings on any package.

## Task Commits

1. **Task 1: Black-box tests** — `ceb57f0` (feat: v1.1 tech debt cleanup)
2. **Task 2: GitHub Actions CI** — `0374760` (ci: add workflow), `4265b88` (ci: black-box step), `0eefca3` (ci: skip examples), `c3d2748` (ci: combine + coverage)
3. **Task 3: Coverage tests** — `6ac7420` (test: add coverage tests)
4. **Task 4: Perf benchmark** — `ceb57f0` (feat: v1.1 tech debt cleanup)

## Files Modified

- `natsql/natsql_blackbox_test.go` — 750 lines, 7 test functions, black-box integration
- `natsql/cfg/config_test.go` — 20 coverage tests for config validation
- `natsql/engine/engine_test.go` — 23 tests for engine lifecycle
- `natsql/kv/kv_test.go` — 20 tests for KV operations + SanitizePK
- `natsql/materialize/mapper_test.go` — 24 tests including stringifyValue, sanitization, json.Number
- `natsql/query/parser_test.go` — 39 tests: BoolVal, NullVal, extractConditions, extractValue, literalToGo, parse error cases
- `natsql/transport/transport_test.go` — 6 tests including body size limit
- `.github/workflows/ci.yml` — CI pipeline
- `examples/07-perf-benchmark/main.go` — performance benchmark

## Decisions Made

- **Black-box via public API only**: Tests go through `Engine.Query()` — no access to internal state. Validates that the public contract works end-to-end.
- **Deterministic test data**: 30 rows across 6 cities (Berlin, London, Tokyo, Paris, New York, Sydney). Each city has 5 users with known active/inactive distribution. Expected results are filters over this known dataset.
- **Separate CI step for black-box**: Isolates the (slower) integration tests from unit tests for clearer failure signals.

## Deviations from Plan

Phase 7 was implemented as planned — CI, black-box tests, coverage, and benchmark all completed.

## Threat Surface Scan

No new security surfaces — this phase is testing and CI only.

## Self-Check: PASSED

- [x] `go test -race ./...` passes
- [x] `go vet ./...` passes clean
- [x] `.github/workflows/ci.yml` exists
- [x] Black-box tests exist with 30-row dataset
- [x] Coverage tests cover all packages
- [x] SUMMARY.md created at expected path

---

*Phase: 07-integration-verification*
*Plan: 01*
*Completed: 2026-05-29*
