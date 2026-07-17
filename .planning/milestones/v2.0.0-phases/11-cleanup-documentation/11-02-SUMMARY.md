---
phase: 11-cleanup-documentation
plan: 02
status: completed
commits:
  - e02c5dd feat(11-02): fix example error handling, add docs, create SQL_DIALECT.md
requirements: [CLN-03, CLN-04, CLN-08]
key-files:
  created:
    - SQL_DIALECT.md
  modified:
    - examples/01-hello-natsql/main.go
    - examples/02-composite-keys/main.go
    - examples/03-malformed-events/main.go
    - examples/04-multiple-views/main.go
    - examples/05-library-embed/main.go
    - examples/07-perf-benchmark/main.go
    - internal/kv/kv.go
    - README.md
  deleted: []
---

# Plan 11-02: Examples & Docs — Summary

## What Was Built

1. **Example error checking (CLN-04):** Fixed all unchecked `jetstream.New`, `CreateOrUpdateStream`, `Publish`, `Decode`, and `Start` errors across 6 example programs (examples 01-05, 07). Example 06 is CLI-only (no main.go).

2. **Lifecycle safety (CLN-04):** Fixed example 05's shared NATS connection hazard — Pattern B now creates a separate `nc2` connection instead of reusing `nc`.

3. **Tombstone docs (CLN-03):** Added package-level doc comment in `internal/kv/kv.go` noting tombstone/delete semantics are not yet supported (planned for v2).

4. **README update (CLN-08):** Changed LIMIT support from Planned to Shipped, added $. prefix notation as Shipped, added Known Limitations section.

5. **SQL_DIALECT.md (CLN-08):** Created with full SQL syntax reference, WHERE clause details, LIMIT behavior, field path notation, unsupported constructs table, and v2 deferred features.

## Deviations

None.

## Build/Test Status

- All 6 example directories build (`go build .`)
- `go build ./internal/kv/` passes
