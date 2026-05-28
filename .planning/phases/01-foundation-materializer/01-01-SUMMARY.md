---
phase: 01-foundation-materializer
plan: 01
subsystem: materializer
tags: [config, yaml, json, kv, nats-jetstream, key-encoding]

requires: []
provides:
  - Typed config structs (Config, ViewConfig, ColumnConfig, etc.)
  - YAML/JSON config parsing with validation
  - KV key encoding and bucket initialization
  - ViewSchema/ColumnSchema types for downstream query engine
  - Schema persistence (StoreSchema/LoadSchema)
affects:
  - phase: 02-sql-engine
    note: consumes ViewSchema from KV to validate queries
  - phase: 01-foundation-materializer
    note: plan-02 materializer component uses config+kv

tech-stack:
  added:
    - gopkg.in/yaml.v3 — YAML config parsing
    - github.com/nats-io/nats.go — JetStream KV client
    - github.com/nats-io/nats-server/v2 — embedded NATS for testing
  patterns:
    - Config structs with dual YAML/JSON tags
    - Accumulating validator (returns all errors, not just first)
    - ViewSchema in kv package to avoid circular imports

key-files:
  created:
    - natsql/go.mod — Go module definition
    - natsql/config.go — Config structs + LoadConfig + Validate + BuildSchema
    - natsql/config_test.go — 17 tests covering config parsing and validation
    - natsql/kv/kv.go — Key encoding, bucket init, schema persistence
    - natsql/kv/kv_test.go — 15 tests covering key encoding and KV ops

key-decisions:
  - "ViewSchema and ColumnSchema types defined in kv/ package to avoid circular import with config.go (kv stores these types, config builds them)"
  - "NATS KV key format uses '/' instead of ':' because NATS KV keys only support [a-zA-Z0-9_\-./=]"
  - "Row key format: {view_name}/pk/{pk_value} (adapted from architecturally planned {view_name}:{pk_value})"
  - "Schema key format: {view_name}/meta/schema (adapted from schemas:{view_name})"

patterns-established:
  - "Dual YAML/JSON config loading via file extension detection"
  - "Accumulating validator that returns all errors joined"
  - "kv package as the key encoding authority (avoids key format divergence)"
  - "Embedded NATS server per test for integration tests"

requirements-completed: [MAT-01, MAT-03]
duration: 18min
completed: 2026-05-28
---

# Phase 01 Plan 01: Config + KV Infrastructure Summary

**Materialized view config structs with YAML/JSON parsing and NATS KV key encoding — supporting single and multi-view definitions, composite keys, column type validation, and schema persistence in the natsql-views bucket.**

## Performance

- **Duration:** 18 min
- **Started:** 2026-05-28T17:15:00Z
- **Completed:** 2026-05-28T17:33:00Z
- **Tasks:** 2
- **Files modified:** 5

## Accomplishments

- Config structs with 6 exported types (Config, ViewConfig, ColumnConfig, IndexConfig, ConsumerConfig, ColumnType) supporting all 17 field decisions from CONTEXT.md
- YAML and JSON config parsing with accumulating validation (missing fields, bad types, duplicate names, no primary key)
- ViewSchema/ColumnSchema types for downstream query engine consumption, stored in kv package to avoid circular imports
- NATS KV key encoding: PkKey, SchemaKey, EncodePKValue with proper format compliance
- Bucket initialization (natsql-views) with FileStorage and configurable replicas
- Schema persistence round-trip (StoreSchema/LoadSchema) with proper missing-key handling
- 32 tests total (17 config + 15 KV), all passing with embedded NATS server for integration tests

## Task Commits

Each task was committed atomically:

1. **Task 1: Create Go module + config structs + YAML loading** — `37a456c` (feat)
2. **Task 2: KV helper package with key encoding and bucket initialization** — `5e6c52d` (feat)

**Plan metadata:** _Pending final commit_

## Files Created/Modified

- `natsql/go.mod` — Go module `natsql` (go 1.25), dependencies for yaml.v3, nats.go, nats-server
- `natsql/config.go` — Config structs, LoadConfig (YAML/JSON), Validate (accumulating), BuildSchema
- `natsql/config_test.go` — 17 tests: valid config, missing file, multiple views, composite keys, invalid types, missing fields, JSON format, indexes forward-compat, duplicates, validation error accumulation, BuildSchema
- `natsql/kv/kv.go` — InitBucket, MustInitBucket, PkKey, SchemaKey, StoreSchema, LoadSchema, EncodePKValue, ViewSchema, ColumnSchema
- `natsql/kv/kv_test.go` — 15 tests: key formatting (PkKey, SchemaKey), EncodePKValue (string/int/bool/nil/panic cases), InitBucket integration, Store/LoadSchema round-trip, missing key returns nil, overwrite updates, MustInitBucket panic

## Decisions Made

- **ViewSchema in kv package**: To avoid circular imports (config.go needs ViewSchema for BuildSchema; kv.go needs ViewSchema for StoreSchema/LoadSchema), both ViewSchema and ColumnSchema types live in the `kv` package. The root package imports them from `natsql/kv`.
- **NATS KV key format uses `/` not `:`**: Context decision D-07 specified `{view_name}:{pk_value}`, but NATS KV keys only support `[a-zA-Z0-9_\-./=]`. The actual format is `{view_name}/pk/{pk_value}` and `{view_name}/meta/schema`.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] NATS KV key format uses '/' instead of ':'**
- **Found during:** Task 2 (KV integration tests with embedded NATS)
- **Issue:** Context decision D-07 (`{view_name}:{pk_value}`) and D-08 (`schemas:{view_name}`) use `:` character which is invalid in NATS KV keys. NATS key validation rejects `:` with `nats: invalid key` error.
- **Fix:** Changed key format to use `/` separator: `PkKey("users","abc123")` returns `"users/pk/abc123"` instead of `"users:abc123"`. Schema key changed from `"schemas:users"` to `"users/meta/schema"`.
- **Files modified:** `natsql/kv/kv.go`, `natsql/kv/kv_test.go`
- **Verification:** All 15 KV tests pass with embedded NATS (no more `invalid key` errors)
- **Committed in:** `5e6c52d` (Task 2 commit)

---

**Total deviations:** 1 auto-fixed (1 bug)
**Impact on plan:** Minor — key format changed from `:` to `/` to comply with NATS KV constraints. All key encoding decisions (single bucket, per-view prefix, schema key) preserved. No scope creep.

## Issues Encountered

- **NATS KV key validation**: Discovered that NATS KV keys only support `[a-zA-Z0-9_\-./=]`. The plan's D-07/D-08 used `:` which is invalid. Fixed by switching to `/` separators matching the existing ARCHITECTURE.md key hierarchy design.
- **Circular import between config and kv**: The root `natsql` package defines Config types, while `kv/` needs ViewSchema for StoreSchema/LoadSchema, and config.go needs ViewSchema for BuildSchema. Solved by placing ViewSchema/ColumnSchema in the `kv` package.

## Known Stubs

None — all code is fully wired.

## Threat Surface Scan

No new security-relevant surfaces introduced beyond what the plan's threat model covers.

## Next Phase Readiness

- Config types and loading ready for materializer (plan-02) and query engine (Phase 2)
- KV key encoding ready for row writes and index entries
- Schema persistence ready for query engine to load view definitions from KV
- Embedded NATS test pattern established for integration tests

## Self-Check: PASSED

- [x] All 5 created files exist (go.mod, config.go, config_test.go, kv/kv.go, kv/kv_test.go)
- [x] Both commits exist (37a456c, 5e6c52d)
- [x] `go build ./...` passes (natsql module)
- [x] `go test ./... -count=1` passes (32 tests, all green)
- [x] SUMMARY.md created at expected path

---

*Phase: 01-foundation-materializer*
*Plan: 01*
*Completed: 2026-05-28*
