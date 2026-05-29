---
phase: 06-transport-code-health
plan: 01
subsystem: transport
tags: [hardening, http, nats, timeouts, body-limit, dead-code]

requires:
  - phase: 02-sql-query-engine-interfaces
    provides: NATS and HTTP transport handlers
provides:
  - HTTP read/write/idle timeouts on server (FIX-TRN-01)
  - HTTP request body size limit with 413 response (FIX-TRN-02)
  - NATS query handler bounded context with timeout (FIX-TRN-03)
  - Dead code removal, unused params, test flakiness cleanup (FIX-TRN-04)
affects: []

tech-stack:
  added: []
  patterns:
    - http.MaxBytesReader for request body size enforcement
    - context.WithTimeout for bounded NATS handler execution

key-files:
  modified:
    - natsql/transport/http.go — MaxBytesReader (1MB), body drain/close, 413 response
    - natsql/transport/nats.go — 30s context.WithTimeout for query handler
    - natsql/transport/transport_test.go — TestHTTPBodyTooLarge, strings import
    - natsql/engine/engine.go — HTTPServer ReadTimeout/WriteTimeout/IdleTimeout
    - natsql/materialize/materializer.go — removed unused dlqStream variable

key-decisions:
  - "1MB max body size (maxRequestBodySize = 1 << 20) — sufficient for any reasonable SQL query"
  - "413 status code for oversized bodies (not 400) — semantically correct per HTTP spec"
  - "30s timeout for NATS query handler — generous but prevents indefinite hangs"

patterns-established:
  - "Bounded context for all request handlers (HTTP + NATS)"
  - "Body always drained and closed after decoding to prevent connection leaks"

requirements-completed: [FIX-TRN-01, FIX-TRN-02, FIX-TRN-03, FIX-TRN-04]
duration: 1 session
completed: 2026-05-29
---

# Phase 06 Plan 01: Transport & Code Health Summary

**HTTP server hardened with timeouts and body size limits, NATS handler with bounded context, dead code and test flakiness cleaned up.**

## Performance

- **Duration:** 1 session
- **Completed:** 2026-05-29
- **Tasks:** 4 (all fix commits in `ceb57f0`)
- **Files modified:** 5 source files + test files

## Accomplishments

- **FIX-TRN-01 — HTTP timeouts**: Configured `ReadTimeout: 10s`, `WriteTimeout: 10s`, `IdleTimeout: 30s` on the HTTP server in engine.go. Hanging connections are terminated instead of left open indefinitely.
- **FIX-TRN-02 — Body size limit**: Added `http.MaxBytesReader(w, r.Body, 1<<20)` in the query endpoint handler. Requests exceeding 1MB receive `413 Request Entity Too Large`. Body is drained and closed after decoding to prevent connection leaks.
- **FIX-TRN-03 — NATS context timeout**: NATS query handler now uses `context.WithTimeout(ctx, 30*time.Second)` instead of unbounded `context.Background()`. A slow/stuck query returns an error instead of hanging forever.
- **FIX-TRN-04 — Dead code cleanup**: Removed unused `dlqStream` variable from materializer `Run()`. Cleaned up unused parameters and dead code paths. Tests run reliably without `time.Sleep`-based flakiness.

## Task Commits

1. **Tasks 1-4: All fixes** — `ceb57f0` (feat: v1.1 tech debt cleanup)

## Files Modified

- `natsql/transport/http.go` — MaxBytesReader, 413 response, body drain/close
- `natsql/transport/nats.go` — 30s context timeout for query handler
- `natsql/transport/transport_test.go` — TestHTTPBodyTooLarge integration test
- `natsql/engine/engine.go` — HTTP server timeout configuration
- `natsql/materialize/materializer.go` — removed unused dlqStream variable

## Decisions Made

- **413 vs 400 for oversized body**: 413 Request Entity Too Large is semantically correct per HTTP spec. 400 Bad Request would be misleading.
- **30s NATS timeout**: Generous enough for complex queries but prevents indefinite hangs. Matches typical HTTP gateway timeouts.

## Deviations from Plan

None — all 4 tasks implemented as planned.

## Issues Encountered

None.

## Threat Surface Scan

- **T-06-01 (DoS via large request body)**: Mitigated — 1MB limit prevents memory exhaustion from oversized SQL queries.
- **T-06-02 (DoS via slow connection)**: Mitigated — read/write/idle timeouts terminate hanging connections.
- **T-06-03 (NATS handler hang)**: Mitigated — 30s bounded context prevents indefinite blocking.

## Self-Check: PASSED

- [x] All fixes implemented in codebase
- [x] `go build ./...` passes
- [x] `go test ./... -count=1` passes
- [x] `go vet ./...` passes clean
- [x] SUMMARY.md created at expected path

---

*Phase: 06-transport-code-health*
*Plan: 01*
*Completed: 2026-05-29*
