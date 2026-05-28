---
phase: 03-packaging-deployment
plan: 02
subsystem: facade-cli
tags: [facade, cobra-cli, config-split, goroutine-leak, graceful-shutdown]
requires:
  - phase: 03-packaging-deployment
    plan: 01
    provides: Embedded NATS server, config extensions, NewEmbedded, WithHTTPServer, Stats, graceful shutdown
  - phase: 01-foundation-materializer
    provides: Engine struct with Start/Close/Query lifecycle
  - phase: 02-sql-query-engine-interfaces
    provides: Query API, materializer pipeline
provides:
  - Root package facade (natsql/natsql.go)
  - Three constructors: New, NewWithNATS, NewEmbedded
  - Convenience constructors own connection cleanup (D-48)
  - Config types split to natsql/cfg/ (breaks import cycle)
  - Cobra CLI with natsql server subcommand (D-50)
  - Three deployment modes: external NATS, embedded NATS, explicit URL (D-51)
  - CLI flag overrides matching config fields (D-52)
  - Facade-level integration tests with goroutine leak verification
affects: none (terminal plan in wave 2)
tech-stack:
  added:
    - "github.com/spf13/cobra v1.10.2" (direct dependency)
    - "natsql/cfg/" (new sub-package, config types)
    - "natsql/natsql.go" (root package facade)
  patterns:
    - Root package facade pattern: struct embedding + owned cleanup
    - Config sub-package to break import cycles
    - Type aliases for backward compat across package boundary
    - Cobra server subcommand with flag overrides
key-files:
  created:
    - natsql/natsql.go (facade)
    - natsql/cfg/config.go (config types sub-package)
  modified:
    - natsql/config.go (type alias re-exports)
    - natsql/engine/engine.go (import path: natsql → natsql/cfg)
    - natsql/engine/engine_test.go (imports + 2 new tests)
    - natsql/cmd/natsql/main.go (full cobra rewrite)
    - natsql/go.mod, natsql/go.sum (added cobra)
    - natsql/materialize/consumer.go, mapper.go, materializer.go (import paths)
    - natsql/materialize/consumer_test.go, mapper_test.go, materializer_test.go (import paths)
key-decisions:
  - "D-46: natsql/natsql.go root package facade"
  - "D-47: New, NewWithNATS, NewEmbedded constructors"
  - "D-48: Convenience constructors own connection cleanup on Close()"
  - "D-50: Cobra CLI with natsql server subcommand"
  - "D-51: Three-mode: external NATS, embedded NATS, explicit URL"
  - "D-52: Config file primary, CLI flags override"
patterns-established:
  - "Facade: Engine struct embedding *engine.Engine + ownedNC + embedNode"
  - "Config split: type aliases from cfg package to root for backward compat"
  - "Cobra CLI: server subcommand with --config, --embedded, --nats-url overrides"
  - "Facade integration tests: lifecycle + goroutine leak + shutdown timing"
requirements-completed:
  - IFC-03
  - DEP-01
  - DEP-02
  - DEP-03
# Metrics
duration: 12min
completed: 2026-05-28
commit_hashes:
  task1: fc678df
  task2: 43fcc4d
  task3: 70cff2d
---

# Phase 03 Plan 02: Root Package Facade & Cobra CLI Summary

**Root package facade with three constructors (New, NewWithNATS, NewEmbedded), config sub-package to break import cycle, Cobra CLI with three deployment modes, and facade-level lifecycle integration tests**

## Performance

- **Duration:** 12 min
- **Started:** 2026-05-28T21:05:00Z (approx)
- **Completed:** 2026-05-28T21:17:00Z (approx)
- **Tasks:** 3 (all auto)
- **Files created:** 2
- **Files modified:** 12

## Accomplishments

- **Config split (natsql/cfg/):** Moved all config types (Config, ViewConfig, ColumnConfig, etc.) and functions (LoadConfig, Validate, BuildSchema, SetDefaults) into `natsql/cfg/` sub-package. Root `natsql/config.go` re-exports all types via Go type aliases for backward compatibility. This broke the import cycle that would prevent the facade from importing the engine package.

- **Root facade (natsql/natsql.go):** Created `Engine` struct embedding `*engine.Engine` with `ownedNC` and `embedNode` fields for lifecycle ownership. Three constructors per D-47:
  - `New(js, cfg, opts...)` — caller-owned JetStream context
  - `NewWithNATS(nc, cfg, opts...)` — owns `nc.Close()` on `Engine.Close()`
  - `NewEmbedded(cfg, opts...)` — owns embedded NATS server shutdown
  - `Close()` shuts down engine, embedded NATS, and NATS connection in order
  - Re-exported `WithLogger`, `WithHTTPServer`, `WithQueryPort` options

- **Cobra CLI (natsql/cmd/natsql/main.go):** Full rewrite from bare main to professional CLI with:
  - `natsql server` subcommand
  - `--config` (`-c`), `--embedded` (`-e`), `--nats-url` (`-u`), `--store-dir`, `--port` (`-p`) flags
  - Three deployment modes: external NATS (default), embedded (`-e`), explicit URL (`-u`)
  - Config file is primary source; CLI flags override matching fields (D-52)
  - Startup banner logging view names, streams, listening address
  - SIGINT/SIGTERM triggers graceful shutdown via facade's `Engine.Close()`

- **Integration tests:** Added `TestEngineFullLifecycleViaFacade` (natsql.NewWithNATS lifecycle + goroutine leak check) and `TestEngineGracefulShutdown` (Close timing + data persistence verification).

## Task Commits

Each task was committed atomically:

| Task | Commit | Description |
|------|--------|-------------|
| 1 | `fc678df` | Create root package facade with New, NewWithNATS, NewEmbedded |
| 2 | `43fcc4d` | Cobra CLI with natsql server subcommand and three deployment modes |
| 3 | `70cff2d` | Add facade lifecycle and graceful shutdown integration tests |

## Files Created/Modified

- `natsql/cfg/config.go` — New sub-package with config types + LoadConfig, Validate, BuildSchema, SetDefaults
- `natsql/natsql.go` — Root package facade (New, NewWithNATS, NewEmbedded, Close, Query, Options)
- `natsql/config.go` — Type alias re-exports + LoadConfig wrapper
- `natsql/engine/engine.go` — Import changed from `natsql` to `natsql/cfg`
- `natsql/engine/engine_test.go` — Import changed + 2 new tests (+200 lines)
- `natsql/cmd/natsql/main.go` — Full cobra rewrite (129 lines)
- `natsql/go.mod` / `natsql/go.sum` — Added cobra v1.10.2
- `natsql/materialize/*.go` — Import paths updated to `natsql/cfg`

## Decisions Made

- **Config split:** Moved config types to `natsql/cfg/` to break the import cycle. This is the correct long-term architecture and aligns with Go best practices.
- **Facade ownership:** `NewWithNATS` owns `nc.Close()`; `NewEmbedded` owns `node.Shutdown()` + `nc.Close()` (both via `Engine.Close()`).
- **Cobra CLI:** Used `RunE` pattern so errors propagate to `rootCmd.Execute()`. All flag vars are package-level. Init function registers flags on server subcommand.
- **Graceful shutdown test:** Uses separate NATS connection after `NewWithNATS` closes the owned connection for post-close verification.

## Deviations from Plan

None — plan executed exactly as written.

## Issues Encountered

1. **Import cycle resolution:** The plan extensively analyzed the cycle between root `natsql` and `natsql/engine`. The implemented solution (create `natsql/cfg/` sub-package) matches the plan's "FINAL APPROACH."
2. **Graceful shutdown test fix:** Initial test failed because `NewWithNATS` closed the NATS connection, making the post-close KV verification impossible. Fixed by creating a second NATS connection for verification.

## Known Stubs

None — all implementations are functional with no placeholder values, hardcoded empty data, or TODO markers. The `IndexConfig` "placeholder" comment in `cfg/config.go` is intentional per D-05.

## Threat Surface Scan

No new threat surface introduced beyond what's documented in the plan's threat model:
- T-03-01 (Config loading): Already mitigated — `os.ReadFile` with error before engine start
- T-03-02 (Embedded NATS): Accepted — 127.0.0.1 binding
- T-03-03 (Close timeout): Mitigated — engine.Close() timed quickly in tests (~200µs)
- T-03-04 (Library facade): Accepted — same access as engine.New directly

## Self-Check: PASSED

- [x] `natsql/natsql.go` exists with `New`, `NewWithNATS`, `NewEmbedded`
- [x] `natsql/cfg/config.go` exists with all config types/functions
- [x] `natsql/config.go` re-exports all types via type aliases
- [x] `natsql/cmd/natsql/main.go` uses Cobra with `natsql server` subcommand
- [x] CLI flags: `--config`, `--embedded`, `--nats-url`, `--store-dir`, `--port` all working
- [x] Three deployment modes operational
- [x] `go build ./...` compiles without errors
- [x] `go vet ./...` passes
- [x] All tests pass (22 engine tests + 4 embed + 22 materialize + others)
- [x] Commits exist: `fc678df`, `43fcc4d`, `70cff2d`

---

*Phase: 03-packaging-deployment*
*Plan: 02*
*Completed: 2026-05-28*
