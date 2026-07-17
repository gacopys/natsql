---
phase: 09-materializer-engine-lifecycle
verified: 2026-06-01T23:54:00Z
status: passed
score: 6/6 must-haves verified
gaps: []
deferred: []
human_verification: []
---

# Phase 9: Materializer & Engine Lifecycle — Verification Report

**Phase Goal:** The materializer processes events safely in stream order with proper error classification, consumer durability is hardened, and engine startup reliably propagates failures synchronously.
**Verified:** 2026-06-01T23:54:00Z
**Status:** passed
**Re-verification:** No — initial verification

## Goal Achievement

### Observable Truths (from ROADMAP Success Criteria)

| # | Truth | Status | Evidence |
|---|-------|--------|----------|
| 1 | Materializer processes events from a single durable consumer in stream order, preserving JetStream's per-subject ordering guarantee at the KV write boundary | ✓ VERIFIED | Worker pool removed (no `materializerWorkers`, `workerWg`, `msgCh`). Sequential `msgCtx.Next() → processEvent()` loop in materializer.go:163-189. `TestSequentialProcessing_StreamOrder` passes (proves stream ordering). `TestSequentialProcessing_SingleGoroutine` passes (proves single goroutine) |
| 2 | KV write errors are classified as transient (connection/leader election → NAK) or terminal (bad data/config → DLQ + Ack) | ✓ VERIFIED | `classifyWriteError()` at materializer.go:224-246. `errorClass` type with `errorClassTransient`/`errorClassTerminal`. Transient: NAK (line 291), Terminal: DLQ+Ack (lines 299-307). 12 test subtests pass for classification + 2 integration tests for routing |
| 3 | Durable consumers have no InactiveThreshold that could cause automatic deletion after downtime | ✓ VERIFIED | consumer.go:46-53 — `ConsumerConfig` struct literal has no `InactiveThreshold` field. `grep -n "InactiveThreshold"` returns no matches |
| 4 | Consumer batch configuration is renamed to `MaxAckPending` (old `batch_size` accepted with deprecation) | ✓ VERIFIED | config.go:107: `MaxAckPending int \`yaml:"max_ack_pending"\``. config.go:112: deprecated `BatchSize` field. SetDefaults migration at lines 70-77. consumer.go:41 uses `cfg.MaxAckPending`. All test files migrated (zero `BatchSize` refs in test files) |
| 5 | HTTP server port is initialized from `cfg.HTTP.Port` in engine constructors instead of hardcoded to 8080 | ✓ VERIFIED | engine.go:138: `queryPort: cfg.HTTP.Port` in `New()`. engine.go:197: `queryPort: cfg.HTTP.Port` in `NewEmbedded()`. Defensive fallback `if e.queryPort == 0 { e.queryPort = 8080 }` in both. No `queryPort: 8080` hardcoded in constructors |
| 6 | `Engine.Start` propagates startup errors synchronously: HTTP listen failures, materializer init failures, and NATS handler registration failures prevent engine from reporting as started | ✓ VERIFIED | HTTP: `net.Listen("tcp", ...)` at line 315 BEFORE goroutine, error returned at line 317. Materializer: startup error channel (lines 249-296), error accumulation (lines 297-299), labeled loop prevents blocking on multi-view timeout. NATS handler: fatal return at line 305. `e.started = true` at line 339 only after ALL steps succeed |

**Score:** 6/6 truths verified

### Required Artifacts

| Artifact | Expected | Status | Details |
| -------- | -------- | ------ | ------- |
| `internal/materialize/materializer.go` | Sequential processing (no worker pool) | ✓ VERIFIED | No `materializerWorkers`, no `workerWg`, no `msgCh` channel. Direct `msgCtx.Next() → processEvent()` loop. Per-event 30s timeout. Deferred panic recovery. Heartbeat goroutine unchanged |
| `internal/materialize/materializer.go` | Error classification (`classifyWriteError`) | ✓ VERIFIED | `errorClass` type at line 214. `classifyWriteError()` at line 224. Transient patterns: conn refused, no leader, timeout, conn closed, context cancellation. Terminal: everything else |
| `internal/materialize/materializer.go` | Transient error → NAK path | ✓ VERIFIED | `msg.Nak()` at line 291 in `errorClassTransient` branch |
| `internal/materialize/materializer.go` | Terminal error → DLQ + Ack path | ✓ VERIFIED | `publishToDLQ` at line 299 then `msg.Ack()` at line 305 in `errorClassTerminal` branch |
| `internal/materialize/consumer.go` | No InactiveThreshold | ✓ VERIFIED | consumer.go:46-53 — `InactiveThreshold` absent from consumer config |
| `internal/cfg/config.go` | MaxAckPending field | ✓ VERIFIED | config.go:107 — `MaxAckPending int` as primary field. Deprecated `BatchSize` at line 112. Migration in SetDefaults at lines 70-77 |
| `internal/engine/engine.go` | queryPort from cfg.HTTP.Port | ✓ VERIFIED | Both `New()` and `NewEmbedded()` use `queryPort: cfg.HTTP.Port` (lines 138, 197) |
| `internal/engine/engine.go` | Synchronous net.Listen in Start() | ✓ VERIFIED | Line 315: `net.Listen("tcp", ...)` before goroutine. Line 317: error returned on failure |
| `internal/engine/engine.go` | started=true only on success | ✓ VERIFIED | Line 339: `e.started = true` after HTTP bind, materializer startup check, and NATS handler registration |

### Key Link Verification

| From | To | Via | Status | Details |
| ---- | --- | --- | ------ | ------- |
| materializer.go processing loop | `processEvent(ctx, ...)` | Direct call in receive loop | ✓ WIRED | Line 187: `processEvent(eventCtx, js, mapper, writer, msg, viewCfg, logger)` |
| `processEvent` | `classifyWriteError` | Error classification before NAK/DLQ decision | ✓ WIRED | Line 287: `switch classifyWriteError(writeErr)` |
| consumer.go `SetupConsumer` | `cfg.MaxAckPending` | Config field for consumer setting | ✓ WIRED | Line 41: `maxAckPending := cfg.MaxAckPending` |
| engine constructors (`New`/`NewEmbedded`) | `cfg.HTTP.Port` | queryPort initialization | ✓ WIRED | Lines 138, 197: `queryPort: cfg.HTTP.Port` |
| `Start()` | `net.Listen` | Synchronous HTTP bind before goroutine | ✓ WIRED | Line 315: `listener, err := net.Listen("tcp", ...)` |

### Data-Flow Trace (Level 4)

| Artifact | Data Variable | Source | Produces Real Data | Status |
| -------- | ------------- | ------ | ------------------ | ------ |
| materializer.go processEvent | `msg` (from `msgCtx.Next()`) | JetStream consumer | ✓ Yes — real NATS stream messages | ✓ FLOWING |
| materializer.go processEvent (error branch) | `writeErr` | `writer.Apply(ctx, mut)` | ✓ Yes — real KV write result | ✓ FLOWING |
| engine.go constructors | `queryPort` | `cfg.HTTP.Port` | ✓ Yes — user-provided config value | ✓ FLOWING |
| consumer.go SetupConsumer | `cfg.MaxAckPending` | Config value | ✓ Yes — user-provided or defaulted to 50 | ✓ FLOWING |

### Behavioral Spot-Checks

| Behavior | Command | Result | Status |
| -------- | ------- | ------ | ------ |
| Build all packages | `go build ./...` | Exit 0 | ✓ PASS |
| Vet all packages | `go vet ./internal/materialize/ ./internal/cfg/ ./internal/engine/` | Exit 0 | ✓ PASS |
| Materializer tests | `go test ./internal/materialize/ -count=1 -timeout 120s` | 47+ tests pass (18.68s) | ✓ PASS |
| Materializer race tests | `go test -race ./internal/materialize/ -count=1 -timeout 120s` | PASS (19.89s) | ✓ PASS |
| Cfg tests | `go test ./internal/cfg/ -count=1 -timeout 60s` | 19 tests pass (0.005s) | ✓ PASS |
| Engine tests | `go test ./internal/engine/ -count=1 -timeout 120s` | 17+ tests pass (24.36s) | ✓ PASS |
| Engine race tests | `go test -race ./internal/engine/ -count=1 -timeout 120s` | PASS (25.59s) | ✓ PASS |
| Sequential processing test | `TestSequentialProcessing_StreamOrder` | PASS | ✓ PASS |
| Error classification tests | `TestClassifyWriteError_Transient` + `_Terminal` (12 subtests) | ALL PASS | ✓ PASS |
| Error routing tests | `TestProcessEvent_TransientWriteError_NAKs` + `_TerminalWriteError_DLQ` | ALL PASS | ✓ PASS |

### Requirements Coverage

| Requirement | Source Plan | Description | Status | Evidence |
| ----------- | ---------- | ----------- | ------ | -------- |
| MAT-01 | 09-01 | Materializer processes events in stream order (CR-01) | ✓ SATISFIED | Worker pool removed. Sequential `msgCtx.Next() → processEvent()` loop. `TestSequentialProcessing_StreamOrder` passes |
| MAT-02 | 09-02 | Error classification: transient→NAK, terminal→DLQ (CR-10) | ✓ SATISFIED | `classifyWriteError` function. `errorClassTransient` branches to `msg.Nak()`. `errorClassTerminal` branches to `publishToDLQ` + `msg.Ack()` |
| MAT-03 | 09-02 | No InactiveThreshold on durable consumers (CR-11) | ✓ SATISFIED | `InactiveThreshold` removed from `consumer.go`. Durable consumers survive until deleted |
| MAT-04 | 09-02 | BatchSize→MaxAckPending rename (CR-12) | ✓ SATISFIED | `MaxAckPending` primary field, deprecated `BatchSize`, SetDefaults migration, all test references updated |
| LIFE-01 | 09-03 | HTTP port from cfg.HTTP.Port (CR-06) | ✓ SATISFIED | `queryPort: cfg.HTTP.Port` in both `New()` and `NewEmbedded()`. Defensive 8080 fallback when zero |
| LIFE-02 | 09-03 | Start propagates errors synchronously (CR-07) | ✓ SATISFIED | HTTP: `net.Listen` before goroutine. Materializer: startup channel + 500ms best-effort wait. NATS handler: fatal. `e.started=true` only after all steps |

### Anti-Patterns Found

| File | Line | Pattern | Severity | Impact |
| ---- | ---- | ------- | -------- | ------ |
| `internal/cfg/config.go` | 99 | "placeholder for Phase 2" comment on `IndexConfig` | ℹ️ Info | Pre-existing code, not introduced by Phase 9. Will be addressed in Phase 11 (CLN-02). No impact on Phase 9 goal |
| `internal/materialize/materializer.go` | 320 | "Returns 0 if metadata not available" | ℹ️ Info | Documentation comment, not a stub. `getMsgSeq` helper function |

No blocking anti-patterns found. No TODO/FIXME/XXX/HACK markers in any Phase 9 modified files. All `return nil` patterns are legitimate error-return patterns, not stubs.

### Human Verification Required

None — all checks are verifiable programmatically.

### Gaps Summary

No gaps found. All 6 ROADMAP success criteria are satisfied:

1. **MAT-01 (Ordered Processing):** Worker pool completely removed. Single sequential `msgCtx.Next() → processEvent()` loop preserves JetStream per-subject ordering. Per-event 30s timeout prevents stuck KV writes. Deferred panic recovery added. Heartbeat goroutine independent.
2. **MAT-02 (Error Classification):** `classifyWriteError` correctly routes transient errors (conn refused, no leader, timeout, conn closed, context cancellation) to NAK (redelivery) and terminal errors (everything else) to DLQ + Ack. Malformed events (mapper errors) unchanged — still DLQ + Ack.
3. **MAT-03 (Consumer Durability):** `InactiveThreshold` completely removed. Durable consumers survive indefinitely.
4. **MAT-04 (Config Rename):** `BatchSize` renamed to `MaxAckPending` with backward-compatible deprecated field and silent migration in `SetDefaults`. All test files updated.
5. **LIFE-01 (HTTP Port):** Both `New()` and `NewEmbedded()` read `queryPort: cfg.HTTP.Port`. Defensive 8080 fallback when zero. No hardcoded 8080 in constructors.
6. **LIFE-02 (Startup Errors):** HTTP binds synchronously via `net.Listen`. Materializer errors propagate via channel with 500ms labeled-loop timeout. NATS handler failure is fatal. `e.started = true` only after all steps succeed.

The auto-fixed `time.After()` multi-view bug (labeled `break startupLoop`) was correctly implemented. All 47+ materializer tests, 19 cfg tests, and 17+ engine tests pass — including race detection.

---

_Verified: 2026-06-01T23:54:00Z_
_Verifier: the agent (gsd-verifier)_
