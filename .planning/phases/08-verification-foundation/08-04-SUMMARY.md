---
phase: 08-verification-foundation
plan: 04
type: execute
wave: 1
tags: [pk-encoding, canonical, kv, materializer, query-engine, refactor]
dependency_graph:
  requires: [08-01, 08-02]
  provides: [FND-01]
  affects: [09-materializer-lifecycle, 10-query-engine, 11-transport-layer]
tech-stack:
  added: []
  patterns:
    - "BuildPkKey(viewName, pkParts, separator) as single canonical PK encoder"
    - "Raw PK parts stored in RowMutation.PkParts []string"
    - "Sanitization happens once at KV boundary, not during mapping"
key-files:
  created: []
  modified:
    - internal/kv/kv.go
    - internal/kv/kv_test.go
    - internal/materialize/mapper.go
    - internal/materialize/mapper_test.go
    - internal/materialize/writer.go
    - internal/materialize/writer_test.go
    - internal/materialize/materializer.go
    - internal/query/types.go
    - internal/query/planner.go
    - internal/query/executor.go
    - internal/query/query_test.go
metrics:
  duration: null
  completed: 2026-06-01
  commits: 4
  files_changed: 11
---

# Phase 08 Plan 04: Canonical PK Encoder Summary

**One-liner:** Eliminated the double-sanitization PK encoding bug (CR-02) by creating a single canonical `BuildPkKey` function used exactly once by both write and read paths, removing three dead convenience functions and their tests.

## Changes Made

### Task 1 — BuildPkKey + dead code removal (TDD)

**RED commit** `d1d5388`: Added table-driven `TestBuildPkKey` covering single parts, composite keys, and all special characters (`_`, `|`, `/`, `*`, `>`) plus `TestPkKey_BackwardCompat`. Tests failed to compile — expected RED behavior.

**GREEN commit** `8010892`:
- Added `BuildPkKey(viewName, pkParts, separator)` — the single canonical PK encoder
- Sanitizes each PK part individually (not the separator), preserving separator characters in KV keys
- Updated `PkKey` to delegate to `BuildPkKey` (backward compatible)
- Removed `SchemaPrefix` constant (dead code, CR-22)
- Removed `MustInitBucket` function (dead code, CR-22)
- Removed `EncodePKValue` function (dead code + dangerous, CR-22)
- Removed 11 tests for removed functions
- All 11 table-driven BuildPkKey test cases pass

### Task 2 — Materializer write path

**Commit** `9b79ded`:
- Changed `RowMutation.PK string` to `RowMutation.PkParts []string` — stores raw parts, no sanitization
- Removed `SanitizePK` call from `stringifyValue` (returns raw strings now)
- Removed unused `kv` import (replaced with `kvpkg` alias for `ViewSchema` type)
- Removed unused `ErrSkipAndAck` sentinel
- Added `separator` field to `Writer` struct
- Updated `NewWriter(jetstream.KeyValue, string, string)` with separator parameter
- Updated `Apply` to call `kv.BuildPkKey(viewName, mut.PkParts, separator)` exactly once
- Updated `materializer.go` to pass separator to `NewWriter`
- Updated all test assertions for PkParts and NewWriter signature

### Task 3 — Query engine read path

**Commit** `2e5e1a0`:
- Replaced `PkValue string` with `PkParts []string` + `Separator string` in `PKLookupPlan`
- Planner stores raw `pkValues` (not joined/sanitized) plus `separator`
- Removed unused `strings` import from planner.go
- Executor calls `kv.BuildPkKey(viewName, pkParts, separator)` exactly once
- Added `TestPKEncoding_SpecialCharacters_WriteRead` — black-box integration test proving that rows with PK values containing `_`, `|`, `/`, `*`, `>` are stored and queried under identical keys

## Key Decisions Made

| Decision | Context |
|----------|---------|
| Sanitize each PK part individually, not joined | Separator characters (`|`, `/`) must not be sanitized — they're delimiters |
| Store raw PK parts in RowMutation.PkParts | Sanitization happens only at KV boundary (BuildPkKey) |
| kvpkg alias for kv import in mapper.go | Avoids name conflicts; consistent with materializer.go pattern |
| Backward-compatible PkKey via delegation | Avoids breaking 16+ test references in materializer_test.go and engine_test.go |

## Deviations from Plan

None — plan executed as written with one correction: the plan's action pseudocode showed `strings.Join(pkParts, separator)` followed by `SanitizePK(pk)`, but the tests expected separator characters to remain unsanitized. Fixed by sanitizing each pkPart individually before joining (Rule 1 - Bug/Logic correction during TDD GREEN phase).

## Verification Results

| Check | Result |
|-------|--------|
| `go build ./internal/kv/ ./internal/materialize/ ./internal/query/` | ✅ PASS |
| `go build ./...` | ✅ PASS |
| `go test ./internal/kv/ -count=1` | ✅ PASS (11 tests) |
| `go test ./internal/materialize/ -count=1 -timeout 120s` | ✅ PASS (38 tests) |
| `go test ./internal/query/ -count=1 -timeout 120s` | ✅ PASS (39 tests including new special chars test) |
| `go vet ./internal/kv/ ./internal/materialize/ ./internal/query/` | ✅ PASS |
| `go test -race ./internal/kv/ ./internal/materialize/ ./internal/query/` | ✅ PASS |
| `EncodePKValue`, `MustInitBucket`, `SchemaPrefix` removed | ✅ Confirmed |

## Known Stubs

None.

## Threat Flags

None. All changes fall within the plan's existing threat model (T-08-04 and T-08-05). The `BuildPkKey` uses `SanitizePK` which escapes `/` to `_s`, mitigating key traversal. No new network endpoints, auth paths, or file access patterns introduced.

## Self-Check: PASSED

- [x] `internal/kv/kv.go` exists — contains `BuildPkKey`, `PkKey` (backward compat), no `EncodePKValue`/`MustInitBucket`/`SchemaPrefix`
- [x] `internal/kv/kv_test.go` exists — contains `TestBuildPkKey`, `TestPkKey_BackwardCompat`, no removed-function tests
- [x] `internal/materialize/mapper.go` — `RowMutation.PkParts []string`, no `SanitizePK` call in `stringifyValue`, no `ErrSkipAndAck`
- [x] `internal/materialize/writer.go` — `Writer` has `separator` field, `Apply` calls `BuildPkKey`
- [x] `internal/materialize/materializer.go` — passes separator to `NewWriter`
- [x] `internal/query/types.go` — `PKLookupPlan` has `PkParts []string` + `Separator string`
- [x] `internal/query/planner.go` — stores raw `pkValues` in plan, no `strings.Join`
- [x] `internal/query/executor.go` — calls `kv.BuildPkKey(p.ViewName, p.PkParts, p.Separator)`
- [x] Commit `d1d5388` exists ✓
- [x] Commit `8010892` exists ✓
- [x] Commit `9b79ded` exists ✓
- [x] Commit `2e5e1a0` exists ✓
- [x] All packages build and all tests pass
