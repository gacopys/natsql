---
phase: 11-cleanup-documentation
plan: 01
status: completed
commits:
  - db1f8bb feat(11-01): remove dead code, add index validation, support $. prefix
requirements: [CLN-01, CLN-02, CLN-05]
key-files:
  created: []
  modified:
    - internal/materialize/mapper.go
    - internal/cfg/config.go
    - internal/engine/engine.go
    - internal/materialize/materializer.go
    - internal/materialize/materializer_test.go
  deleted: []
---

# Plan 11-01: Config & Dead Code — Summary

## What Was Built

1. **$. prefix stripping (CLN-01):** Added `strings.TrimPrefix(path, "$.")` in `extractNestedField` — both `$.field` and `field` notations work.
2. **Index config validation (CLN-02):** Added loop after view validation rejecting any `indexes` block with clear error message.
3. **Dead code removal (CLN-05):**
   - Removed `Stats.LastError` field from Stats struct in engine.go
   - Removed unused `dlqStream` parameter from `materialize.Run()` signature
   - Updated all 11 callsites (engine.go:269, 10 materializer_test.go calls)

## Deviations

None — all changes per plan.

## Build/Test Status

- `go build ./...` — passes
- `go test ./internal/materialize/ ./internal/cfg/ -count=1 -timeout 120s` — passes
- `go test ./internal/engine/ -count=1 -timeout 120s` — 1 pre-existing failure (TestEngineQueryPKLookup: UseNumber type comparison)
