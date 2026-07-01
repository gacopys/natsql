# Milestones

## v1.2 Code Review Remediation (Shipped: 2026-07-01)

**Phases completed:** 4 phases, 12 plans, 66 commits

**Key accomplishments:**

1. **Verification baseline:** All 25 code review findings verified against source code — 25 confirmed, 0 dismissed. Config cross-validation, SQL parser rejection of unsupported constructs, and canonical PK encoder (`BuildPkKey`) implemented.

2. **Sequential processing:** Removed 16-goroutine worker pool — preserves JetStream per-subject ordering at the KV write boundary. Error classification (transient→NAK, terminal→DLQ), InactiveThreshold removal, BatchSize→MaxAckPending rename.

3. **Synchronous startup:** Engine.Start propagates all startup errors synchronously — HTTP listen failures, materializer init failures, and NATS handler registration failures prevent the engine from reporting as started.

4. **Query engine correctness:** All WHERE conditions retained as post-filters with contradictory PK short-circuit. `SELECT *` excludes internal `_meta` fields. Large integer precision preserved via `json.Decoder.UseNumber()`.

5. **Transport robustness:** HTTP uses `errors.As` for MaxBytesError with trailing data rejection. NATS surfaces Flush/Respond errors. CLI supports `--create-streams` flag.

6. **Cleanup & CI overhaul:** Dead code removed, `$.` prefix supported, index config validated, test helpers into `internal/testutil`, SQL_DIALECT.md created. golangci-lint with 40 linters, govulncheck, 5 parallel CI workflows, all vulnerabilities fixed (Go 1.26.4).

---

## v1.1 Tech Debt Cleanup (Shipped: 2026-05-29)

**Phases completed:** 4 phases, 3 plans, 16 tasks

**Key accomplishments:**

- All four query engine bugs from the v1.0 code review fixed: PK post-filter applied, data race on Engine.kv eliminated, type-aware comparison replacing fmt.Sprint, and boolean literal support in SQL parser.
- All four materializer hardening fixes from v1.0 code review: PK value sanitization, DLQ error propagation, partial init cleanup, and JSON integer precision.
- HTTP server hardened with timeouts and body size limits, NATS handler with bounded context, dead code and test flakiness cleaned up.
- Full integration test suite, CI pipeline, and performance benchmark ensuring all Phase 4-6 fixes work together without regressions.

---
