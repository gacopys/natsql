# Phase 8: Verification & Foundation — Context

**Gathered:** 2026-06-01
**Status:** Ready for planning

<domain>
## Phase Boundary

Verify all 25 code review findings against source code and fix foundational correctness issues that unblock subsequent phases: canonical PK encoding, unsupported SQL rejection, and config validation. This phase does NOT address materializer/engine lifecycle, query predicate, transport, or cleanup fixes — those are Phases 9-11.

</domain>

<decisions>
## Implementation Decisions

### Verification Format (VER-01)
- **D-01:** Create `VERIFICATION_FINDINGS.md` — a structured document with a table per cr.md finding: ID, severity, source lines, confirmed/dismissed status, reasoning, and which phase will fix it
- **D-02:** Verification runs against current source code before any Phase 8 changes are applied — establishes the baseline
- **D-03:** Each finding is either "Confirmed" (still present in source) or "Dismissed" (already fixed in v1.1 or not applicable) with explicit reasoning

### Canonical PK Encoder (FND-01)
- **D-04:** Single canonical `BuildPkKey(viewName string, pkParts []string, separator string) string` function in `internal/kv/kv.go`
- **D-05:** Raw PK parts are stored in `RowMutation.PK` (already separated), sanitized only at the KV key construction boundary
- **D-06:** Materializer writer and query executor both call `BuildPkKey()` exactly once — no double-sanitization
- **D-07:** Remove `EncodePKValue` and `MustInitBucket` — they're unused/dangerous convenience functions that duplicate PK encoding logic

### SQL Rejection (FND-02)
- **D-08:** Parser rejects ALL unsupported constructs with explicit error messages: DISTINCT, ORDER BY, GROUP BY, HAVING, aggregations (COUNT, SUM, AVG, MIN, MAX), subqueries, non-column/non-star select expressions (SELECT 1, SELECT CONCAT(...))
- **D-09:** Whitelist-only approach: only `SELECT <col> [, <col>]*` and `SELECT *` are accepted in select expressions
- **D-10:** Error messages name the rejected construct: `"unsupported: ORDER BY is not supported"`

### Config Validation (FND-03)
- **D-11:** `key_fields` must exactly equal the set of columns with `primary_key=true` — strict equality, no ambiguity
- **D-12:** Validate uniqueness of: view names (across all views), column names (within each view), key fields (no duplicates)
- **D-13:** Each `key_field` must reference an existing column
- **D-14:** Invalid configs are rejected at load time with clear error messages

### Testing
- **D-15:** Tests co-located with fixes in the package where changes are made
- **D-16:** Black-box integration tests for PK encoding with special characters (`_`, `|`, `/`, `*`, `>`)
- **D-17:** Negative parser tests for each rejected SQL construct
- **D-18:** Config validation unit tests for each cross-validation case

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Code Review Source
- `cr.md` — The full code review report with all 25 findings, evidence lines, and suggested fixes. Source of truth for VER-01.

### Research Documents (Phase 8 scope)
- `.planning/research/ARCHITECTURE.md` — Architecture remediation design, including FND-01 PK encoder pipeline redesign (§2) and FND-02 parser hardening
- `.planning/research/PITFALLS.md` — Detailed analysis of each pitfall, prevention strategies, and recovery approaches. Critical for understanding why each fix matters
- `.planning/research/FEATURES.md` — Feature-level mapping of cr.md findings to user-visible behavior corrections
- `.planning/research/STACK.md` — Technology stack impact analysis per finding
- `.planning/research/SUMMARY.md` — Research synthesis, fix waves, dependency graph, and phase mapping

### Requirements
- `.planning/REQUIREMENTS.md` §v2.0.0 — VER-01, FND-01, FND-02, FND-03 requirement definitions
- `.planning/ROADMAP.md` §Phase 8 — Phase success criteria and dependency information

### Project Constants
- `.planning/PROJECT.md` — Core value, constraints (zero external deps, Go 1.22+, NATS JetStream 2.8+)

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- `internal/kv/kv.go` — Existing `PkKey()` and `SanitizePK()` functions. The canonical PK encoder replaces/extends these
- `internal/query/parser.go` — Existing parser with `Parse()` function. Currently silently ignores unsupported constructs
- `internal/cfg/config.go` — Existing config loading and validation at `Validate()` around line 156

### Established Patterns
- Single shared KV bucket pattern (not changing in Phase 8)
- vitess sqlparser AST traversal for WHERE clause extraction
- YAML/JSON dual config format with shared struct tags

### Integration Points
- `internal/materialize/writer.go` calls `kv.PkKey()` — must switch to `BuildPkKey()`
- `internal/materialize/mapper.go` handles PK assembly — must store raw parts, not sanitized
- `internal/query/planner.go` builds PK lookup keys — must use `BuildPkKey()`
- `internal/query/executor.go` executes KV lookups — must use `BuildPkKey()`
- `internal/query/parser.go` must reject unsupported constructs

</code_context>

<specifics>
No specific requirements — standard approaches per research docs and cr.md suggestions. The cr.md suggested fixes serve as the design template.

</specifics>

<deferred>
None — discussion stayed within phase scope.

</deferred>

---

*Phase: 08-verification-foundation*
*Context gathered: 2026-06-01*
