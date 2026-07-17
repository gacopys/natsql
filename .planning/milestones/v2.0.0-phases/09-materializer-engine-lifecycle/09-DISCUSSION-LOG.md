# Phase 9: Materializer & Engine Lifecycle — Discussion Log

> **Audit trail only.** Do not use as input to planning, research, or execution agents.
> Decisions are captured in CONTEXT.md — this log preserves the alternatives considered.

**Date:** 2026-06-01
**Phase:** 09-materializer-engine-lifecycle
**Areas discussed:** Worker pool removal, Error classification, BatchSize fate, HTTP port wiring, Startup error severity

---

## Worker Pool Removal (MAT-01)

| Option | Description | Selected |
|--------|-------------|----------|
| Single goroutine | Remove worker pool, one sequential goroutine per view | ✓ |
| Single goroutine + batch writes | Accumulate N events, write batch | |
| Partition by PK hash | Multiple workers per PK partition | |

**User's choice:** Single goroutine (Recommended)
**Notes:** Simplest approach, perfect ordering. Can optimize later if throughput needs it.

---

## Error Classification (MAT-02)

| Option | Description | Selected |
|--------|-------------|----------|
| Standard classification | Transient: timeout/refused/no leader → NAK. Terminal: bad data/config → DLQ | ✓ |
| Aggressive retry | All KV errors NAK'd at least once before DLQ | |

**User's choice:** Standard classification (Recommended)
**Notes:** Classify at caller site in processEvent. Helper function `classifyWriteError`.

---

## BatchSize Fate (MAT-04)

| Option | Description | Selected |
|--------|-------------|----------|
| Rename to MaxAckPending | Config field rename, no behavioral change | ✓ |
| Implement Fetch/FetchNoWait | Actual batched fetching | |

**User's choice:** Rename to MaxAckPending (Recommended)
**Notes:** Old `batch_size` accepted with deprecation warning for one version.

---

## HTTP Port Wiring (LIFE-01)

| Option | Description | Selected |
|--------|-------------|----------|
| Direct in constructors | Read cfg.HTTP.Port, pass as WithQueryPort | ✓ |
| Keep WithQueryPort option | Option-based with cfg fallback | |
| Remove option entirely | Port always from config | |

**User's choice:** Direct in constructors (Recommended)
**Notes:** cfg.HTTP.Port == 0 → default 8080.

---

## Startup Error Severity (LIFE-02)

| Option | Description | Selected |
|--------|-------------|----------|
| All fatal | HTTP bind, materializer setup, NATS handler — all prevent Start from succeeding | ✓ |
| NATS handler degraded | Only HTTP and materializer are fatal; NATS handler failure is logged | |

**User's choice:** All fatal (Recommended)
**Notes:** HTTP bind via net.Listen before goroutine. Materializer errors via channel. NATS handler error returned directly.

---

## Deferred Ideas

None.
