---
phase: 11-cleanup-documentation
plan: 03
status: completed
commits:
  - 9f1988e feat(11-03): create testutil package, refactor tests, add CI format check
requirements: [CLN-06, CLN-07]
key-files:
  created:
    - internal/testutil/testutil.go
    - internal/testutil/doc.go
  modified:
    - internal/kv/kv_test.go
    - internal/materialize/consumer_test.go
    - internal/engine/engine_test.go
    - natsql_blackbox_test.go
    - .github/workflows/ci.yml
  deleted: []
---

# Plan 11-03: Test Helpers & Formatting — Summary

## What Was Built

1. **internal/testutil package (CLN-07):** Created `testutil.StartEmbeddedNATS(t)` and `testutil.CreateStream(t, ctx, js, name)` — shared test helpers with `t.Cleanup` for automatic teardown.

2. **Test refactor (CLN-07):** Changed `kv_test.go`, `consumer_test.go`, `engine_test.go`, and `natsql_blackbox_test.go` to delegate to testutil. Removed duplicated `srv.Shutdown()` and `nc.Close()` defers (now handled by `t.Cleanup`).

3. **gofmt formatting (CLN-06):** Ran `gofmt -w` on all Go source files (zero formatting changes needed — codebase was already clean).

4. **CI format check (CLN-06):** Added `Format check` step in `.github/workflows/ci.yml` that runs `gofmt -l .` and fails the build on unformatted files.

## Deviations

- Did NOT refactor `writer_test.go` (`startEmbeddedNATSForKV`) or `query/query_test.go` or `transport/transport_test.go` — only the 4 files specified in the plan. Remaining duplicates can be migrated incrementally.
- `createStream` functions in `engine_test.go` and `consumer_test.go` were kept as local helpers (they add Storage/Retention settings beyond testutil.CreateStream).

## Build/Test Status

- `go build ./...` — passes
- `go test ./internal/kv/ -count=1 -timeout 60s` — passes
- `go test ./internal/materialize/ -count=1 -timeout 120s` — passes
- `go test ./internal/engine/ -count=1 -timeout 120s` — 1 pre-existing failure (TestEngineQueryPKLookup)
- `gofmt -l .` — no output (all formatted)
