---
phase: 02-sql-query-engine-interfaces
plan: 02
subsystem: query-engine
tags: [natstransport, http, chi, json-api, nats-request-reply]
requires:
  - phase: 02-sql-query-engine-interfaces
    plan: 01
    provides: query package (Parse, Validate, BuildPlan, Execute, QueryResult)
provides:
  - Engine.Query() method with full pipeline (parse → load schema → validate → plan → execute)
  - NATS request-reply handler on "natsql.query" subject
  - HTTP JSON API at POST /api/v1/query with chi v5
  - Transport lifecycle integration (Start/Close)
affects:
  - 03-packaging-and-cli (CLI query tool, standalone binary wiring)
tech-stack:
  added:
    - github.com/go-chi/chi/v5 v5.3.0 (HTTP router with middleware)
  patterns:
    - Engine satisfies transport.QueryHandler interface for dependency inversion
    - Transport package isolates NATS/HTTP concerns from Engine
    - Both transports share the same query.QueryResult JSON envelope
    - HTTP server bound to 127.0.0.1 (dev-local only, T-02-06)
    - chi middleware stack: Logger, Recoverer, Timeout 30s
key-files:
  created:
    - natsql/transport/nats.go (RegisterNATSHandler, QueryHandler interface)
    - natsql/transport/http.go (RegisterHTTPHandler, NewRouter, QueryRequest)
    - natsql/transport/transport_test.go (5 transport integration tests)
  modified:
    - natsql/engine/engine.go (New sig, Query, Start/Close wiring)
    - natsql/engine/engine_test.go (8 new query tests, updated sig)
    - natsql/go.mod (added chi/v5)
    - natsql/cmd/natsql/main.go (updated New call with nc param)
key-decisions:
  - Engine.New() signature changed to include *nats.Conn as first param for NATS subscription
  - HTTP server binds to 127.0.0.1:8080 (not 0.0.0.0) per threat model T-02-06
  - Lazy KV bucket initialization in Query() enables query-before-Start pattern
  - Chi middleware Timeout(30s) mitigates long-running query DoS (T-02-07)
requirements-completed: [IFC-01, IFC-02]
duration: 35min
completed: 2026-05-28
---

# Phase 2 Plan 2: Engine Integration + NATS/HTTP Transport Summary

**Engine.Query() pipeline with NATS request-reply on `natsql.query` and HTTP POST `/api/v1/query` via chi v5, both typed-JSON via shared envelope, lifecycle-wired into Engine.Start()/Close()**

## Performance

- **Duration:** ~35 min
- **Started:** 2026-05-28T20:10:00Z
- **Completed:** 2026-05-28T20:45:00Z
- **Tasks:** 2 (TDD: 4 commits)
- **Files modified:** 7 (3 new, 4 modified)
- **Tests:** 19 total (5 transport, 14 engine)

## Accomplishments

- **Engine.Query(ctx, sql) *query.QueryResult** — full pipeline: parse SQL → load schema from KV → validate against schema → build plan (PK lookup or full scan) → execute → typed JSON result with nil-to-empty-slice normalization (D-33)
- **NATS request-reply handler** on `natsql.query` subject (D-34): raw SQL string in request body (D-35), JSON envelope response
- **HTTP JSON API** at `POST /api/v1/query` (D-37): `{"sql": "..."}` request body, standard JSON response with Content-Type: application/json and 400 on invalid JSON
- **Chi v5 router** with middleware stack: Logger, Recoverer, Timeout(30s) for DoS protection (T-02-07)
- **Transport lifecycle wiring**: Engine.Start() registers NATS subscription and starts HTTP server; Engine.Close() unsubscribes NATS and gracefully shuts down HTTP server
- **Lazy KV bucket init** — Query() works before Start() by initializing the KV bucket on-demand
- **Threadsafe querying** — concurrent calls verified with 10 goroutines
- **1a61c06 sig change**: Engine.New() now accepts `*nats.Conn` as first parameter; all callers updated (cmd/natsql/main.go, engine tests)

## Task Commits

Each task was committed atomically via TDD red-green or direct implementation:

1. **Task 1 (RED skipped — full impl + tests): Engine.Query()** - `1a61c06` (feat)
2. **Task 2 (RED): failing transport tests + chi dep** - `c358d3b` (test)
3. **Task 2 (GREEN): transport handlers + lifecycle wiring** - `b006f33` (feat)

## Files Created/Modified

- `natsql/transport/nats.go` - RegisterNATSHandler subscribing to `natsql.query`, QueryHandler interface
- `natsql/transport/http.go` - RegisterHTTPHandler for POST /api/v1/query, NewRouter with chi middleware
- `natsql/transport/transport_test.go` - 5 tests: NATS request-reply, NATS error, HTTP POST, invalid body 400, Content-Type
- `natsql/engine/engine.go` - New fields (nc, natsSub, httpServer, queryPort), updated New() sig, Query() method, Start/Close transport lifecycle wiring
- `natsql/engine/engine_test.go` - 8 new Query tests (PK lookup, view not found, invalid SQL, unknown column, concurrent, before Start, full scan, empty results), updated existing New() calls
- `natsql/go.mod` - Added github.com/go-chi/chi/v5 v5.3.0
- `natsql/cmd/natsql/main.go` - Updated engine.New() call to include nc

## Decisions Made

- **127.0.0.1 binding**: HTTP server binds to localhost only per T-02-06 (dev-local recommendation)
- **Lazy KV init in Query()**: KV bucket initialized on-demand if nil, enabling query without Start() — subtle benefit is that Engine can be embedded as a pure query engine without materializers
- **Chi middleware stack**: logger + recoverer + 30s timeout as standard transport safety layer
- **Goroutine capture fix**: HTTP server and logger passed as function params to the goroutine to avoid potential closure-capture issues with Go toolchain
- **Close() order**: cancel materializers → unsubscribe NATS → shutdown HTTP → Wait() — ensures clean teardown without deadlocks (HTTP server goroutine tracked by WaitGroup)

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] Missing `time` import in engine.go**
- **Found during:** Task 2 (GREEN - lifecycle wiring)
- **Issue:** `5*time.Second` in Close() caused compilation failure — `time` package not imported
- **Fix:** Added `"time"` to engine.go imports
- **Files modified:** natsql/engine/engine.go
- **Verification:** `go build ./...` passes
- **Committed in:** b006f33 (Task 2 GREEN commit)

**2. [Rule 1 - Bug] HTTP goroutine panic on nil pointer**
- **Found during:** Task 2 (GREEN - running tests)
- **Issue:** TestEngineDoubleStart panicked with SIGSEGV in the HTTP server goroutine at `e.logger.Info(...)`
- **Fix:** Changed goroutine to capture `httpServer` and `logger` as explicit function parameters instead of closing over `e`
- **Files modified:** natsql/engine/engine.go
- **Verification:** All engine tests pass, including TestEngineDoubleStart and TestEngineRestart
- **Committed in:** b006f33 (Task 2 GREEN commit)

---

**Total deviations:** 2 auto-fixed (1 blocking, 1 bug)
**Impact on plan:** Both fixes necessary for correct operation. No scope creep.

## Issues Encountered

- **Engine.New() signature change**: The signature change from `New(js, cfg, opts...)` to `New(nc, js, cfg, opts...)` is a breaking change for all callers. Updated existing tests and cmd/natsql/main.go. Documented for downstream consumers.
- **Port conflicts in tests**: All engine tests bind HTTP port 8080. Tests run sequentially, and Close() properly releases the port, so no conflicts observed. If port conflicts arise, tests would need randomized ports.

## Threat Surface Notes

- T-02-05 (NATS auth): No auth enforced in v1 — any NATS client on the same domain can query. This is by design (accepted).
- T-02-06 (HTTP binding): Server binds to 127.0.0.1:8080 by default — not exposed to network.
- T-02-07 (DoS timeout): chi middleware.Timeout(30s) applied to all HTTP requests.
- T-02-08 (NATS DoS): No explicit protection beyond NATS message size defaults (1MB). Accepted.
- T-02-09 (Info disclosure): Error messages include SQL text. Accepted for v1 debugging.

## Next Phase Readiness

- `natsql/transport/` package ready for CLI integration (Phase 3 can use transport.RegisterNATSHandler directly)
- Engine.Query() fully callable from Go library embedding
- HTTP port needs configuration support (currently hardcoded to 8080)
- NATS query subject currently hardcoded to "natsql.query" per D-34 — should be configurable

## Self-Check: PASSED

- [x] `natsql/transport/nats.go` exists with RegisterNATSHandler using QueryHandler interface
- [x] `natsql/transport/http.go` exists with RegisterHTTPHandler and chi router
- [x] `natsql/go.mod` contains `github.com/go-chi/chi/v5`
- [x] NATS subscription subject is `natsql.query` per D-34
- [x] HTTP route is `POST /api/v1/query` per D-37
- [x] `go build ./...` compiles without errors
- [x] `go vet ./...` passes without warnings
- [x] `go test ./transport/... ./engine/... -count=1 -timeout 120s` passes
- [x] All 5 transport tests pass
- [x] All 14 engine tests pass (6 lifecycle + 8 query integration)
- [x] Engine.New() signature updated
- [x] Engine.Start() registers NATS subscription and starts HTTP server
- [x] Engine.Close() unsubscribes NATS and shuts down HTTP server

---

*Phase: 02-sql-query-engine-interfaces*
*Plan: 02*
*Completed: 2026-05-28*
