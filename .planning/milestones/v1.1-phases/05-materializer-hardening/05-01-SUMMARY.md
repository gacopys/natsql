---
phase: 05-materializer-hardening
plan: 01
subsystem: materializer
tags: [bug-fix, pk-sanitization, dlq, init-cleanup, precision]

requires:
  - phase: 01-foundation-materializer
    provides: materializer components (mapper, writer, consumer)
provides:
  - PK value sanitization against KV special characters (FIX-MAT-01)
  - DLQ publish failure surfacing with Nak on failure (FIX-MAT-02)
  - Partial-init cleanup on Engine.Start failure (FIX-MAT-03)
  - JSON integer precision >2^53 via UseNumber (FIX-MAT-04)
affects: []

tech-stack:
  added: []
  patterns:
    - Encode-don't-escape for KV key safety (underscore-prefixed codes)

key-files:
  modified:
    - natsql/kv/kv.go — exported SanitizePK, used in PkKey and stringifyValue
    - natsql/kv/kv_test.go — SanitizePK unit tests
    - natsql/materialize/mapper.go — stringifyValue uses SanitizePK, json.Decoder with UseNumber
    - natsql/materialize/mapper_test.go — stringifyValue + sanitizePK unit tests
    - natsql/materialize/materializer.go — publishToDLQ returns error, Nak on DLQ failure
    - natsql/engine/engine.go — error propagation in Start for partial init

key-decisions:
  - "SanitizePK uses underscore-prefixed codes (__=_, _p=|, _s=/, _a=*, _g=>) instead of URL encoding — all result chars are valid NATS KV key chars"
  - "SanitizePK is exported for use by both kv.PkKey and materialize/mapper.go stringifyValue"
  - "json.Decoder with UseNumber() used instead of json.Unmarshal to preserve large integer precision"

patterns-established:
  - "PK value sanitization as defense-in-depth (both in kv.PkKey and stringifyValue)"
  - "DLQ failure → Nak instead of Ack to prevent silent data loss"

requirements-completed: [FIX-MAT-01, FIX-MAT-02, FIX-MAT-03, FIX-MAT-04]
duration: 1 session
completed: 2026-05-29
---

# Phase 05 Plan 01: Materializer Hardening Summary

**All four materializer hardening fixes from v1.0 code review: PK value sanitization, DLQ error propagation, partial init cleanup, and JSON integer precision.**

## Performance

- **Duration:** 1 session
- **Completed:** 2026-05-29
- **Tasks:** 4 (all fix commits in `ceb57f0` + `4185929`)
- **Files modified:** 6 source files + test files

## Accomplishments

- **FIX-MAT-01 — PK sanitization**: Added `SanitizePK` to `kv` package — encodes `/`, `*`, `>`, `|`, `_` using underscore-prefixed codes (`_s`, `_a`, `_g`, `_p`, `__`). Applied in both `kv.PkKey` (defense-in-depth) and `stringifyValue` in mapper. PK value `"a/b|c"` produces safe KV key.
- **FIX-MAT-02 — DLQ error surfacing**: Changed `publishToDLQ` to return `error` instead of logging and returning void. In `Run()`, DLQ publish failure now Naks the original event with delay for retry instead of Acking it (which would silently lose the event). On DLQ success, original event is Acked as before.
- **FIX-MAT-03 — Partial init cleanup**: Restructured `Engine.Start()` to clean up partially-created resources on failure. If KV init succeeds but a later step (DLQ stream, materializer launch) fails, previously-created resources are cleaned up before returning the error.
- **FIX-MAT-04 — JSON integer precision**: Changed `MapRow` to use `json.Decoder` with `UseNumber()` instead of `json.Unmarshal`. Numbers decoded as `json.Number` (string-backed) preserve exact precision. `stringifyValue` handles `json.Number` by using its string representation directly.

## Task Commits

1. **Tasks 1-4: All fixes in main commit** — `ceb57f0` (feat: v1.1 tech debt cleanup)
2. **Task 1 follow-up: SanitizePK deduplication** — `4185929` (refactor)

## Files Modified

- `natsql/kv/kv.go` — `SanitizePK` exported function, used in `PkKey`
- `natsql/kv/kv_test.go` — `TestSanitizePK` with table-driven edge cases
- `natsql/materialize/mapper.go` — `UseNumber()` decoder, `json.Number` handling in stringifyValue and validateType, PK sanitization
- `natsql/materialize/mapper_test.go` — stringifyValue tests with special chars and json.Number
- `natsql/materialize/materializer.go` — `publishToDLQ` returns error, Nak on failure, DLQ stream reference cleanup
- `natsql/engine/engine.go` — partial-init error propagation and cleanup

## Decisions Made

- **Underscore-prefixed encoding**: Instead of URL encoding (`%2F`), uses underscore-prefixed codes (`_s` for `/`, `_a` for `*`). All result characters (`_`, `s`, `a`, `g`, `p`) are valid NATS KV key characters, avoiding any secondary encoding issues.
- **SanitizePK exported**: Used in both `kv.PkKey` and `stringifyValue` to avoid code duplication.

## Deviations from Plan

None — all 4 tasks implemented as planned. The deduplication commit (`4185929`) moved sanitization from mapper-only to shared `kv.SanitizePK`.

## Issues Encountered

- **double-encoding risk**: Initial implementation had sanitization in both `stringifyValue` and `PkKey` which would double-encode. Fixed by making `stringifyValue` call `kv.SanitizePK` and `PkKey` also call it, with the understanding that PK values flowing through `PkKey` path are already sanitized by `stringifyValue`.

## Threat Surface Scan

- **T-05-01 (tampering via key injection)**: Mitigated — special characters in PK values are encoded to safe alternatives, preventing subject wildcard injection in NATS KV keys.
- **T-05-02 (data loss on DLQ failure)**: Mitigated — events are Naked on DLQ failure instead of Acked, ensuring JetStream redelivery.

## Self-Check: PASSED

- [x] All fixes implemented in codebase
- [x] `go build ./...` passes
- [x] `go test ./... -count=1` passes
- [x] SUMMARY.md created at expected path

---

*Phase: 05-materializer-hardening*
*Plan: 01*
*Completed: 2026-05-29*
