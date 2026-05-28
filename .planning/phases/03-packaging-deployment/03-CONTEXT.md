# Phase 3: Packaging + Deployment — Context

**Gathered:** 2026-05-28
**Status:** Ready for planning

<domain>
## Phase Boundary

Package natsql as a Go library (importable) and a standalone binary with both embedded and external NATS modes. Covers IFC-03, DEP-01 through DEP-04: Go library API, standalone binary with embedded NATS, external NATS support, and graceful shutdown with no resource leaks.

</domain>

<decisions>
## Implementation Decisions

### Library API Surface
- **D-46:** Root package facade via `natsql/natsql.go` that delegates to `engine.Engine`. Engine struct stays in `natsql/engine/` sub-package.
- **D-47:** Three constructors:
  - `New(js jetstream.JetStream, cfg *Config, opts ...Option)` — caller provides JetStream context (owns connection)
  - `NewWithNATS(nc *nats.Conn, cfg *Config, opts ...Option)` — creates JetStream from connection, owns `nc.Close()`
  - `NewEmbedded(cfg *Config, opts ...Option)` — starts embedded NATS server, owns `node.Shutdown()`
- **D-48:** Convenience constructors (`NewWithNATS`, `NewEmbedded`) own their connection cleanup on `Close()`.
- **D-49:** Minimal options pattern: `WithLogger`, `WithNATSOptions` (for embedded NATS), `WithHTTPServer(addr)`. All other config is in the YAML file.

### Server Binary & CLI
- **D-50:** Cobra CLI with `natsql server` subcommand. Move `cmd/natsql/main.go` → `cmd/natsql/main.go` as root command, add `server` subcommand.
- **D-51:** Three-mode design:
  - `natsql server --config=config.yaml` (external NATS via `nats.DefaultURL`)
  - `natsql server --config=config.yaml --embedded` (embedded NATS)
  - `natsql server --config=config.yaml --nats-url=nats://host:port` (explicit external NATS)
- **D-52:** Config file primary source, CLI flags override matching config fields.
- **D-53:** Default embedded NATS port is 4222 (same as NATS default).

### Embedded NATS Integration
- **D-54:** Create a new `natsql/embed/` package for embedded NATS startup.
- **D-55:** Single-node only (`StartNode`). Cluster mode deferred.
- **D-56:** JetStream store directory configurable via `--store-dir` flag (default `./data/nats`). Memory-backed only if explicitly configured.

### Graceful Shutdown
- **D-57:** Shutdown ordering in `Engine.Close()`:
  1. Stop HTTP server (5s timeout for in-flight requests)
  2. Cancel NATS subscription
  3. Drain materializer consumers (`cons.Drain()`) — finish in-flight messages and ack
  4. Cancel materializer context (signals remaining goroutines)
  5. Wait for all goroutines via WaitGroup
  6. Close embedded NATS server (if owned)
- **D-58:** Use `cons.Drain()` instead of just context cancellation to prevent unnecessary redeliveries on restart.

### Goroutine Leak Prevention
- **D-59:** Integration tests verify goroutine count before/after Engine lifecycle (no leaked goroutines).
- **D-60:** `Engine.Stats()` method returns a struct with current goroutine count, consumer count, HTTP server status, and last error.

### Claude's Discretion
- Exact goroutine leak detection approach in tests (runtime.NumGoroutine vs pprof)
- HTTP server timeout values (read/write/idle timeouts)
- Embedded NATS store directory defaults
- Cobra command structure details (flags, help text)
- Stats struct fields beyond the core ones
- Whether to support `natsql query` ad-hoc subcommand in v1

</decisions>

<canonical_refs>
## Canonical References

### Project requirements
- `.planning/REQUIREMENTS.md` — IFC-03, DEP-01 through DEP-04 define Phase 3 requirements
- `.planning/PROJECT.md` — Project constraints

### Research
- `.planning/research/SUMMARY.md` — Deployment mode patterns, graceful shutdown
- `.planning/research/STACK.md` — Cobra CLI rationale

### Phase 1 & 2 artifacts
- `natsql/engine/engine.go` — Existing Engine struct (Start/Close lifecycle to extend)
- `natsql/cmd/natsql/main.go` — Current binary entry point (to be refactored)

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- `natsql/engine/engine.go` — Engine struct with `Start()`/`Close()` lifecycle (extend for phased shutdown)
- `natsql/engine/engine_test.go` — Existing engine test patterns (extend for shutdown verification)
- `natsql/cmd/natsql/main.go` — Current SIGINT handling pattern (refactor for cobra)

### Established Patterns
- Consumer lifecycle: `defer cc.Stop(); <-cc.Closed()` for clean cleanup
- Engine uses `sync.WaitGroup` for goroutine tracking
- Functional options pattern: `type Option func(*Engine)` with `WithLogger`

### Integration Points
- Root `natsql/natsql.go` facade delegates to `engine.Engine` (sub-package)
- `embed.StartNode` provides NATS server handle for `NewEmbedded` constructor
- Cobra root command replaces current bare `main.go`
- `Engine.Close()` must integrate HTTP server shutdown + consumer drain + NATS connection cleanup

</code_context>

<specifics>
## Specific Ideas

- The `natsql server` subcommand should log a startup banner with view names, stream sources, and listening addresses
- `Engine.Stats()` should include per-view consumer status and event processing counts for operational visibility
- Shutdown timeout should be configurable (default 30s) to handle large backlogs

</specifics>

<deferred>
## Deferred Ideas

- Embedded NATS cluster mode (3-node) — future
- `natsql query` ad-hoc CLI subcommand — v2 (IFC-04)
- Prometheus metrics endpoint — v2 (OPS-01)
- Config hot-reload (SIGHUP) — v2 (OPS-03)

</deferred>

---

*Phase: 03-packaging-deployment*
*Context gathered: 2026-05-28*
