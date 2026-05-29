# Phase 7: Integration Verification — PLAN

**Goal:** All fixes work together without regressions; full acceptance criteria pass; performance is baseline-verified.

**Depends on:** Phase 4, Phase 5, Phase 6

**Requirements:** None (cross-cutting verification)

## Tasks

### Task 1: Black-box integration tests

Full end-to-end test suite with 30-row deterministic dataset covering PK lookups, projections, WHERE/AND/IN/!=/LIMIT filters, error cases, composite keys, type integrity, and JSON roundtrip.

### Task 2: GitHub Actions CI

Build, vet, test with -race on every PR. Coverage reporting and dedicated black-box test step.

### Task 3: Coverage tests

Unit tests for all uncovered paths: config, engine, kv, mapper, parser, executor, transport.

### Task 4: Performance benchmark

Example benchmark for baseline verification.

## Success Criteria

1. `go test -race ./...` passes on clean checkout of all Phase 4-6 changes
2. Full end-to-end workflow verified: define a view → publish events → query via NATS request-reply → query via HTTP
3. Perf benchmark completes without regression against v1.0 baseline
4. All v1.0 acceptance criteria continue to pass
