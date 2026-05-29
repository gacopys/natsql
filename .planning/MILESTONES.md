# Milestones

## v1.1 Tech Debt Cleanup (Shipped: 2026-05-29)

**Phases completed:** 4 phases, 3 plans, 16 tasks

**Key accomplishments:**

- All four query engine bugs from the v1.0 code review fixed: PK post-filter applied, data race on Engine.kv eliminated, type-aware comparison replacing fmt.Sprint, and boolean literal support in SQL parser.
- All four materializer hardening fixes from v1.0 code review: PK value sanitization, DLQ error propagation, partial init cleanup, and JSON integer precision.
- HTTP server hardened with timeouts and body size limits, NATS handler with bounded context, dead code and test flakiness cleaned up.
- Full integration test suite, CI pipeline, and performance benchmark ensuring all Phase 4-6 fixes work together without regressions.

---
