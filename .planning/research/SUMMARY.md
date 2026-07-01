# Research Summary: natsql v1.2 Code Review Remediation

**Project:** natsql — NATS-native materialized view engine
**Domain:** Stream-to-KV materialized view engine with read-only SQL query layer
**Researched:** 2026-05-31
**Mode:** Architecture remediation for 25 code review findings
**Overall confidence:** HIGH

## Executive Summary

The v1.2 code review identified 25 findings (3 Critical, 11 High, 7 Medium, 4 Low) across the natsql codebase. The fixes cluster into six architectural waves: **(1) Foundation refactoring** — canonical PK encoder, config validation, parser hardening; **(2) Materializer correctness** — ordered processing, error classification, consumer durability; **(3) Query engine correctness** — predicate handling, metadata filtering, number precision; **(4) Engine lifecycle** — synchronous startup errors, config plumbing; **(5) Transport/CLI** — stream creation, HTTP handling, NATS errors; **(6) Cleanup** — dead code, formatting, docs.

The most architecturally significant changes are: **(A)** Removing the 16-goroutine worker pool from the materializer to restore per-view stream ordering (CR-01). **(B)** Unifying PK encoding into a single `BuildPkKey` function used by both write and read paths (CR-02). **(C)** Keeping ALL WHERE predicates as post-filters to prevent contradictory predicates from producing wrong results (CR-03). No new components are needed — all fixes are targeted modifications to existing components with the 3-component model preserved.

**Build order:** Wave 1 (foundation) → Wave 2 (materializer) + Wave 4 (engine lifecycle) in parallel → Wave 3 (query engine) → Wave 5 (transport/CLI) → Wave 6 (cleanup). Waves 1-2 have the highest risk (core materializer and PK encoding changes) and benefit from integration test validation before proceeding to Wave 3.

## Key Findings

### Recommended Architecture Approach

The fixes map to six implementation waves with strict internal dependencies but freedom for parallelization across waves:

**Wave 1 — Foundation:**
- CR-08: Cross-validate `key_fields` vs `primary_key` in config validation
- CR-02: Create single `BuildPkKey()` function; remove double-sanitization
- CR-05: Reject unsupported SQL constructs (ORDER BY, DISTINCT, GROUP BY, etc.)

**Wave 2 — Materializer Correctness:**
- CR-01: Remove 16-worker goroutine pool; process messages sequentially
- CR-10: Classify errors (transient → NAK, terminal → DLQ, malformed → DLQ)
- CR-11: Remove `InactiveThreshold` from durable consumers
- CR-12: Rename `BatchSize` to `MaxAckPending`

**Wave 3 — Query Engine Correctness:**
- CR-03: Keep ALL WHERE conditions as post-filters; add contradiction detection
- CR-04: Make SELECT * return only schema columns (filter `_meta`)
- CR-09: Use `json.Decoder.UseNumber()` in executor; update `valuesEqual`
- CR-13: Document view-prefix filtering cost; add prefix constant

**Wave 4 — Engine Lifecycle:**
- CR-07: Synchronous `net.Listen` before HTTP `Serve`; materializer error propagation
- CR-06: Initialize `queryPort` from `cfg.HTTP.Port` in engine constructors

**Wave 5 — Transport/CLI:**
- CR-14: Only create streams in embedded mode; respect `source_subject`
- CR-18: Use `errors.As` for `MaxBytesError`; check for trailing JSON data
- CR-19: Check `nc.Flush()` and `msg.Respond()` errors

**Wave 6 — Cleanup:**
- CR-15/CRO-22: Dead code removal, formatting, docs, test helpers

### Critical Pitfalls Identified by Code Review

1. **Concurrent unordered writes corrupt materialized state (CR-01).** 16 goroutines processing events concurrently destroys JetStream's per-subject ordering guarantee. **Fix:** Sequential processing per view.

2. **Inconsistent PK encoding causes unreachable rows (CR-02).** Double-sanitize on write, single-sanitize on read means rows with `_`, `|`, `/`, `*`, `>` in PK values are stored under different keys than queries look for. **Fix:** Single `BuildPkKey()` function.

3. **Contradictory PK predicates produce wrong results (CR-03).** `WHERE id = 'u1' AND id != 'u1'` returns `u1` because PK conditions are removed from post-filter. **Fix:** Keep all conditions as post-filters.

4. **Startup errors silently swallowed (CR-07).** HTTP bind failures and materializer consumer setup errors are logged but `Start()` returns nil. The engine reports "started" while core services are down. **Fix:** Synchronous error propagation.

5. **Transient errors cause permanent data loss (CR-10).** All KV write errors are treated equally — publish to DLQ, ack the original message. A temporary NATS outage permanently drops valid events. **Fix:** Error classification with NAK for transient failures.

## Implications for Roadmap

Six implementation waves with the following ordering rationale:

### Wave 1: Foundation Refactoring (CR-02, CR-05, CR-08)
**Rationale:** These are refactoring-only changes with zero behavioral impact when done correctly. CR-02 (canonical PK) is especially critical because both materializer and query engine changes depend on it.
**Delivers:** Consistent PK encoding, stricter config validation, unsupported SQL rejection
**Addresses:** CR-02, CR-05, CR-08
**Build order dependency:** Must precede Waves 2 and 3

### Wave 2: Materializer Correctness (CR-01, CR-10, CR-11, CR-12)
**Rationale:** CR-01 (ordered processing) is the most critical correctness fix in the entire review. It changes the materializer's core processing loop. CR-10, CR-11, CR-12 are natural companions in the same component.
**Delivers:** Stream-ordered materialization, error classification, consumer durability
**Addresses:** CR-01, CR-10, CR-11, CR-12
**Depends on:** Wave 1 (CR-02 canonical PK)

### Wave 3: Query Engine Correctness (CR-03, CR-04, CR-09, CR-13)
**Rationale:** Query engine fixes depend on CR-02 (canonical PK) being in place first. CR-03 (predicate handling) is the second most critical correctness fix.
**Delivers:** Correct predicate filtering, metadata-free SELECT *, precise number comparison, scoped full scans
**Addresses:** CR-03, CR-04, CR-09, CR-13
**Depends on:** Wave 1 (especially CR-02)

### Wave 4: Engine Lifecycle (CR-06, CR-07)
**Rationale:** Independent of materializer and query engine internals. Can be implemented in parallel with Waves 2 and 3.
**Delivers:** HTTP port from config, synchronous error propagation on startup
**Addresses:** CR-06, CR-07

### Wave 5: Transport/CLI (CR-14, CR-18, CR-19, CR-20)
**Rationale:** Independent of all above. Transport and CLI changes are isolated to their own packages.
**Delivers:** Safe stream creation, robust HTTP handling, NATS error checking
**Addresses:** CR-14, CR-18, CR-19, CR-20

### Wave 6: Cleanup (CR-15, CR-16, CR-21 through CR-25)
**Rationale:** Should come last after all behavioral changes are verified.
**Delivers:** Dead code removal, formatting, docs sync, test helper extraction
**Addresses:** All medium/low findings

### Parallelization Opportunities

- Waves 2 and 4 have no dependency on each other — can be implemented in parallel
- Waves 2 and 5 have no dependency — can be implemented in parallel
- Wave 3 depends on Wave 1 but not on Waves 2, 4, or 5 — can start after Wave 1
- Wave 6 should be last but individual items (gofmt, dead code) can be done incrementally

### Research Flags for Phases

- **Wave 2 (CR-01):** Sequential processing changes throughput characteristics. May need performance benchmarks if latency is a concern. LOW research risk — pattern is well-understood.
- **Wave 3 (CR-09):** json.Number comparison in valuesEqual needs careful testing for edge cases (very large integers, decimals, mixed types). MEDIUM research risk — edge cases in type comparison.
- **Wave 3 (CR-13):** Full scan performance with multi-view buckets — benchmark to document cross-view cost. LOW research risk — purely documentation.

## Confidence Assessment

| Area | Confidence | Notes |
|------|------------|-------|
| Stack | **HIGH** | No stack changes needed — all fixes use existing dependencies |
| Architecture Changes | **HIGH** | All fixes verified against source code; each has clear integration points |
| Build Order | **HIGH** | Dependency chain verified through code analysis; no circular dependencies |
| Pitfalls | **HIGH** | All 25 findings documented in cr.md with line-level evidence; fixes traceable to source |

## Gaps to Address

| Gap | Impact | Resolution |
|-----|--------|------------|
| **Per-view KV buckets** (deferred) | CR-13's real fix would be per-view buckets, which is a breaking change | Defer to v2; document cross-view scan cost for v1.2 |
| **Performance impact of sequential processing** (CR-01) | Throughput may decrease without worker pool | Benchmark after implementation; add -race testing to verify |
| **Delete semantics** (CR-17) | No tombstone/delete model for materialized rows | Defer to v2; keep as open gap |
| **Index config acceptance** (CR-16) | Users can configure indexes but they're ignored | Reject at validation time with clear error message |

## Sources

### Primary (HIGH confidence)
- Code review findings: `cr.md` (226 lines, all findings with line-level evidence)
- Source code: https://github.com/gacopys/natsql (audited all 15+ files for this research)
- `internal/materialize/materializer.go` — current worker pool architecture
- `internal/materialize/mapper.go` — stringifyValue + SanitizePK chain
- `internal/materialize/writer.go` — Writer.Apply with PkKey call
- `internal/materialize/consumer.go` — consumer config with InactiveThreshold
- `internal/query/planner.go` — findPKEqConditions + BuildPlan
- `internal/query/executor.go` — projectRow, json.Unmarshal, WatchAll
- `internal/query/parser.go` — extractSelectExprs
- `internal/engine/engine.go` — Start lifecycle, HTTP server setup
- `internal/cfg/config.go` — Validate logic
- `internal/kv/kv.go` — PkKey, SanitizePK, EncodePKValue
- `internal/transport/http.go` — body drain, error detection
- `internal/transport/nats.go` — Flush error handling
- `cmd/natsql/main.go` — stream creation, HTTP port plumbing

### Secondary (MEDIUM confidence)
- NATS JetStream KV documentation — for understanding KV capabilities and limitations
- NATS consumer configuration patterns — for consumer durability best practices

---

*Research completed: 2026-05-31*
*Ready for v1.2 milestone planning: yes*
