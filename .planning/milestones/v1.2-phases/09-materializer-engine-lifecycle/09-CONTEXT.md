# Phase 9: Materializer & Engine Lifecycle — Context

**Gathered:** 2026-06-01
**Status:** Ready for planning

<domain>
## Phase Boundary

Fix materializer correctness (ordered processing, error classification, consumer durability) and engine lifecycle issues (HTTP port wiring, synchronous startup error propagation). This phase does NOT address query engine predicates, transport robustness, or cleanup — those are Phases 10-11.

</domain>

<decisions>
## Implementation Decisions

### Ordered Processing (MAT-01 / CR-01)
- **D-01:** Remove the 16-goroutine worker pool. Single goroutine per view processes messages sequentially from the consumer
- **D-02:** The bridge goroutine that feeds messages into the buffered channel is also removed — the consumer's Messages() drives processing directly
- **D-03:** The heartbeat logging goroutine is kept (separate concern, no ordering impact)
- **D-04:** If throughput becomes a bottleneck in the future, optimize via batching or per-key partitioning — not before

### Error Classification (MAT-02 / CR-10)
- **D-05:** Standard classification:
  - **Transient** (NAK with backoff): context deadline exceeded, connection refused, no leader, timeout, network errors
  - **Terminal** (DLQ + Ack): key too long, value too large, invalid message data, bad config, any error after retries exhausted
- **D-06:** The `writer.Apply` error is classified at the caller site in `processEvent` based on error type/string matching. A helper function `classifyWriteError(err error) errorClass` makes the decision
- **D-07:** Malformed input (mapper errors) still goes to DLQ + Ack immediately (already correct)

### Consumer Durability (MAT-03 / CR-11)
- **D-08:** Remove `InactiveThreshold` from `SetupConsumer` consumer config. Durable consumers survive indefinitely until explicitly deleted
- **D-09:** No config field for InactiveThreshold — it's not a user-tunable parameter for durable consumers

### BatchSize Rename (MAT-04 / CR-12)
- **D-10:** Rename `batch_size` config field to `max_ack_pending` to match its actual behavior (controls MaxAckPending, not fetch batching)
- **D-11:** YAML/JSON config field `batch_size` is deprecated; `max_ack_pending` is the new name. Old `batch_size` still accepted with a deprecation warning for one version

### HTTP Port Wiring (LIFE-01 / CR-06)
- **D-12:** In `Configure()` (or engine constructors), read `cfg.HTTP.Port` and pass it as `WithQueryPort` option
- **D-13:** No separate option validation — cfg.HTTP.Port with value 0 means "use default (8080)"

### Startup Error Propagation (LIFE-02 / CR-07)
- **D-14:** All three failure modes are FATAL:
  - HTTP listener bind failure → Start returns error
  - Materializer consumer setup failure → Start returns error
  - NATS handler registration failure → Start returns error
- **D-15:** Implementation: bind HTTP with `net.Listen` before goroutine (synchronous). Materializer setup happens in goroutine but errors propagate via channel. NATS handler error is checked and returned before Start completes
- **D-16:** The engine is NOT marked as started (`e.started = true`) unless all steps succeed

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Source of Truth
- `cr.md` — CR-01 (workers), CR-10 (error classification), CR-11 (InactiveThreshold), CR-12 (BatchSize), CR-06 (HTTP port), CR-07 (startup errors)
- `.planning/research/ARCHITECTURE.md` §Materializer — Architecture design for ordered processing, error classification, consumer config
- `.planning/research/PITFALLS.md` — Detailed pitfall analysis for each issue
- `.planning/VERIFICATION_FINDINGS.md` — Current verification status of all findings (all confirmed)

### Requirements
- `.planning/REQUIREMENTS.md` §v1.2 — MAT-01 through MAT-04, LIFE-01, LIFE-02
- `.planning/ROADMAP.md` §Phase 9 — Success criteria and dependency information

### Phase 8 Prior Art
- `.planning/phases/08-verification-foundation/08-CONTEXT.md` — BuildPkKey decisions (Phase 9 uses it for KV writes)
- `.planning/phases/08-verification-foundation/08-04-SUMMARY.md` — Canonical PK encoder implementation

### Current Source
- `internal/materialize/materializer.go` — Worker pool (lines 187-202), processEvent (lines 224-277)
- `internal/materialize/consumer.go` — Consumer setup with InactiveThreshold
- `internal/materialize/writer.go` — Writer.Apply for KV operations
- `internal/engine/engine.go` — Start() with goroutine error handling (lines 245-303)

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- `internal/kv/kv.go` — `BuildPkKey()` now available (Phase 8) for KV key construction in writer
- `internal/materialize/writer.go` — `Writer.Apply` uses `BuildPkKey`; `NewWriter` accepts separator
- `internal/materialize/consumer.go` — `SetupConsumer` with configurable parameters

### Established Patterns
- Single-goroutine-per-view pattern (being introduced by this phase, replacing worker pool)
- Error accumulation in config validation (Phase 8 pattern, not directly relevant here)

### Integration Points
- `materializer.go:194-201` — Worker goroutine loop → replace with single goroutine
- `materializer.go:254-268` — Writer error handling → add classification
- `consumer.go:53` — `InactiveThreshold` — remove from config
- `engine.go:254-261` — Materializer in goroutine with logged errors → propagate via channel
- `engine.go:268-275` — NATS handler error logged → make fatal
- `engine.go:288-295` — HTTP server in goroutine with logged errors → bind sync, serve async
- `engine.go:138,194` — Hardcoded queryPort → wire from cfg.HTTP.Port

</code_context>

<specifics>
No specific requirements — standard approaches per cr.md suggested fixes and research docs.

</specifics>

<deferred>
None — discussion stayed within phase scope.

</deferred>

---

*Phase: 09-materializer-engine-lifecycle*
*Context gathered: 2026-06-01*
