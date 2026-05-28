---
phase: 01-foundation-materializer
plan: 02
subsystem: materializer
tags: [nats-jetstream, consumer, json-path, type-validation, kv-writer, tdd, durable-consumer]

requires:
  - phase: 01-01
    provides: Config structs (ViewConfig, ConsumerConfig, ColumnConfig), KV key encoding (PkKey, SchemaKey)
provides:
  - Durable pull consumer setup with explicit config (batch size, MaxDeliver, AckWait)
  - Event-to-row mapper with JSON path extraction and strict type validation
  - KV writer with _meta metadata injection for row upserts
affects:
  - phase: 01-03
    note: processing loop wires consumer, mapper, and writer together — these are the three components it uses
  - phase: 02-sql-engine
    note: query engine reads rows written by Writer from KV

tech-stack:
  added: []
  patterns:
    - TDD per component (test-first RED → GREEN commits)
    - jetstream.Msg mock for testing without NATS (mapper tests)
    - Embedded NATS for integration tests (consumer, writer)
    - Depth-limited JSON path traversal (max 8 levels per T-02-02)

key-files:
  created:
    - natsql/materialize/consumer.go — Durable pull consumer setup
    - natsql/materialize/consumer_test.go — 7 consumer tests
    - natsql/materialize/mapper.go — Event-to-row mapper with JSON path extraction and type validation
    - natsql/materialize/mapper_test.go — 19 mapper tests
    - natsql/materialize/writer.go — KV row writer with _meta metadata
    - natsql/materialize/writer_test.go — 4 writer tests

key-decisions:
  - "Added sourceSubject parameter to SetupConsumer (plan signature had cfg ConsumerConfig but needed FilterSubject from view config)"

patterns-established:
  - "TDD flow: RED commit (test + stub) → GREEN commit (implementation)"
  - "Mock jetstream.Msg for mapper unit tests (testMsg struct)"
  - "Embedded NATS server per test for consumer and writer integration tests"
  - "Depth-limited JSON path traversal (max 8 levels) for DoS protection"

requirements-completed: [MAT-01, MAT-02, MAT-03, MAT-04]
duration: 7min
completed: 2026-05-28
---

# Phase 01 Plan 02: Materializer Components — Consumer, Mapper, Writer Summary

**Durable pull consumer setup, event-to-row JSON mapper with strict type validation, and KV writer with _meta metadata — the three core materializer components built with TDD, independently tested, and ready for the processing loop.**

## Performance

- **Duration:** 7 min
- **Started:** 2026-05-28T17:21:19Z
- **Completed:** 2026-05-28T17:28:24Z
- **Tasks:** 3
- **Files modified:** 6

## Accomplishments

- Durable pull consumer setup: `ConsumerName` (`natsql-{viewName}` per D-10), `SetupConsumer` creates/resumes durable consumer with explicit config (batch size, MaxDeliver, AckWait, FilterSubject, DeliverAll), idempotent on restart, 1h InactiveThreshold prevents zombie consumers, error on missing stream
- Event-to-row mapper: `NewMapper` validates config, `MapRow` extracts fields via dotted JSON path (`user.id` → nested lookup), validates types per D-15/D-16/D-17 (string accepts number conversion, number=float64 only, boolean=bool only, timestamp=RFC3339), composite keys joined by configurable separator (default `|`), max 8-level nesting depth limit
- KV writer: `NewWriter` + `Apply` builds row JSON with `_meta` (stream_seq, updated_at), writes at `kv.PkKey(viewName, pk)`, idempotent upsert via `kv.Put`, context cancellation propagation
- 31 new tests: 7 consumer (including idempotent create, config verification, FilterSubject, missing stream), 19 mapper (top-level/nested paths, all 4 types, type mismatches, composite keys, metadata, depth limit), 5 writer (key correctness, `_meta`, idempotent overwrite, context cancellation, nil mutation)

## Task Commits

Each task was committed atomically with TDD (RED → GREEN):

1. **Task 1: Durable pull consumer setup** — `a063f23` (test: add failing tests) → `8feb08f` (feat: implement consumer)
2. **Task 2: Event→Row mapper** — `3bb4b86` (test: add failing tests) → `06372ba` (feat: implement mapper)
3. **Task 3: KV Writer** — `eccc072` (test: add failing tests) → `a82577b` (feat: implement writer)

**Plan metadata:** _Pending final commit_

## Files Created/Modified

- `natsql/materialize/consumer.go` — `ConsumerName()` and `SetupConsumer()` with durable pull consumer config, defaults for zero values, FilterSubject support
- `natsql/materialize/consumer_test.go` — 7 tests: name format, creates durable, resumes existing, config fields applied, FilterSubject, DeliverAll, missing stream error
- `natsql/materialize/mapper.go` — `Mapper` struct + `NewMapper` + `MapRow`, `RowMutation` struct, sentinel errors (`ErrMalformedEvent`, `ErrSkipAndAck`), `extractNestedField` helper, `validateType` helper, depth-limited traversal
- `natsql/materialize/mapper_test.go` — 19 tests covering all behaviors + `testMsg` mock for `jetstream.Msg`
- `natsql/materialize/writer.go` — `Writer` struct + `NewWriter` + `Apply` with `_meta` injection and `kv.Put` upsert
- `natsql/materialize/writer_test.go` — 5 tests: key correctness, `_meta` field, idempotent overwrite, context cancellation, nil mutation

## Decisions Made

- **Added sourceSubject parameter to SetupConsumer**: The plan's stated signature `SetupConsumer(ctx, js, streamName, viewName, cfg ConsumerConfig)` didn't include `sourceSubject` needed for FilterSubject behavior. Added `sourceSubject string` parameter since it's a stream-level property, not part of `ConsumerConfig`. This is a deviation from the plan's aspirational function signature but required for correct FilterSubject behavior.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 2 - Missing Critical] Added sourceSubject parameter to SetupConsumer**
- **Found during:** Task 1 (SetupConsumer implementation)
- **Issue:** Plan's stated function signature `SetupConsumer(ctx, js, streamName, viewName, cfg ConsumerConfig)` omitted the `sourceSubject` needed for `FilterSubject` config. Test 6 requires FilterSubject to be applied when SourceSubject is set, but there was no way to pass it in the signature.
- **Fix:** Added `sourceSubject string` as a parameter between `viewName` and `cfg`. The parameter maps to `ViewConfig.SourceSubject` at the call site.
- **Files modified:** `natsql/materialize/consumer.go`, `natsql/materialize/consumer_test.go`
- **Verification:** `TestSetupConsumer_FilterSubjectApplied` passes — consumer's FilterSubject matches the provided sourceSubject.
- **Committed in:** `8feb08f` (Task 1 GREEN commit)

**2. [Rule 1 - Bug] jetstream.Msg interface mismatch in mapper test mock**
- **Found during:** Task 2 (writing mapper tests)
- **Issue:** The plan's mock message pattern used `Nak(opts ...NakOption)` and `Term(opts ...TermOption)`, but nats.go v1.51.0's `jetstream.Msg` interface has `Nak() error`, `NakWithDelay(delay)`, `Term() error`, `TermWithReason(reason)` instead. Also missing `DoubleAck(ctx)`, `Headers()`, `Subject()`, `Reply()`.
- **Fix:** Updated `testMsg` to match the actual v1.51.0 interface: added `DoubleAck`, `NakWithDelay`, `TermWithReason`, `Headers`, `Subject`, `Reply` methods; removed variadic `Nak`/`Term`.
- **Files modified:** `natsql/materialize/mapper_test.go`
- **Verification:** Compilation succeeds, all 19 mapper tests pass.
- **Committed in:** `06372ba` (Task 2 GREEN commit, fixed during iteration)

---

**Total deviations:** 2 auto-fixed (1 missing critical, 1 bug)
**Impact on plan:** Minor — both fixes correct function signatures and API usage to match the actual nats.go v1.51.0 API. No scope creep.

## Issues Encountered

- **nats.go v1.51.0 API differences from plan's assumptions**: The plan assumed older jetstream.Msg interface (`Nak(opts ...NakOption)`, `Term(opts ...TermOption)`) and `Stream.Consumer()` returning `*ConsumerInfo`. The actual v1.51.0 API has different method signatures. Fixed by checking actual interface definitions and adapting tests accordingly.
- **ConsumerConfig defaults**: The plan specified `MaxAckPending: cfg.BatchSize * 2` with default 100 when BatchSize is 0. Since BatchSize defaults to 50, `MaxAckPending` defaults to 100 (50*2). This works correctly.

## Known Stubs

None — all code is fully wired.

## Threat Surface Scan

No new security-relevant surfaces introduced beyond what the plan's threat model covers:
- T-02-01 (tampering): Mitigated via strict schema validation — `ErrMalformedEvent` on invalid JSON, missing fields, type mismatches
- T-02-02 (DoS): Mitigated via max 8-level nesting depth limit in `extractNestedField`
- T-02-03 (DoS): Writer returns errors to caller — processing loop handles retry
- T-02-04 (information disclosure): All in-process memory, accepted per plan

## Next Phase Readiness

- All three materializer components are ready for the processing loop (plan 01-03):
  - `consumer.go` → creates durable consumer for event ingestion
  - `mapper.go` → transforms events to typed row mutations
  - `writer.go` → persists mutations to KV with metadata
- 31 tests with embedded NATS where applicable ensure integration correctness
- Processing loop in plan 01-03 will wire these together: consume → map → write → ack

## Self-Check: PASSED

- [x] All 6 created files exist (consumer.go, consumer_test.go, mapper.go, mapper_test.go, writer.go, writer_test.go)
- [x] All 6 commits exist (a063f23, 8feb08f, 3bb4b86, 06372ba, eccc072, a82577b)
- [x] `go build ./...` passes (natsql module, clean build)
- [x] `go test ./... -count=1` passes (59 tests across 3 packages, all green)
- [x] SUMMARY.md created at expected path

---

*Phase: 01-foundation-materializer*
*Plan: 02*
*Completed: 2026-05-28*
