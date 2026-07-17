# Phase 11: Cleanup & Documentation — Context

**Gathered:** 2026-07-01
**Status:** Ready for planning

<domain>
## Phase Boundary

Clean up the codebase after all behavioral fixes (Phases 8-10) are verified and merged: remove dead code, enforce Go formatting, deduplicate test helpers, fix examples for proper error handling, and update documentation to accurately reflect the implemented feature set. This phase does NOT add new features or fix behavioral bugs — those were Phases 8-10.
</domain>

<decisions>
## Implementation Decisions

### $. Prefix Handling (CLN-01 / CR-15)
- **D-01:** Strip `$.` prefix in `extractNestedField` (mapper.go) so both `$.field` and `field` path notations work. This is a 2-line change that makes the mapper backward-compatible with existing configs that use JSONPath-style notation.

### Index Config Validation (CLN-02 / CR-16)
- **D-02:** Add explicit validation error at config load time when `indexes` field is present: "secondary indexes are not yet supported — remove indexes block from config". This rejects invalid configs early rather than silently ignoring them.

### Delete/Tombstone Documentation (CLN-03 / CR-17)
- **D-03:** Document missing delete/tombstone semantics in two places:
  - README known-limitations section
  - Package-level doc comment in `internal/kv/kv.go` noting tombstone support is planned for v2

### Example Error Checking (CLN-04 / CR-21)
- **D-04:** Fix all example programs (examples/01 through 07) to check errors properly and avoid lifecycle ownership hazards. Specifically:
  - Check errors from `engine.Start()`, `nc.Publish()`, `nc.Request()`, `kv.Get()`, etc.
  - Ensure NATS connection lifecycle is clean (defer close, not manual close)
  - No swallowed errors or unchecked returns

### Dead Code Removal (CLN-05 / CR-22)
- **D-05:** Remove only the 6 specific symbols listed in CR-22. No broader dead code hunt:
  - `SchemaPrefix` constant
  - `ErrSkipAndAck` sentinel error
  - `Stats.LastError` field
  - `dlqStream` parameter (unused function parameter)
  - `EncodePKValue` function
  - `MustInitBucket` function

### Go Formatting & CI (CLN-06 / CR-23)
- **D-06:** Run `gofmt -w` on all Go source files as a one-time format pass
- **D-07:** Add CI step that runs `gofmt -l` and fails the build if any unformatted files are detected (blocking, not warning)

### Test Helper Deduplication (CLN-07 / CR-24)
- **D-08:** Create `internal/testutil/` package with shared test helpers:
  - `StartEmbeddedNATS(t) (*nats.Conn, jetstream.JetStream)` — starts embedded NATS, returns connection and JetStream handle
  - `CreateStream(t, ctx, js, name)` — creates a test stream with standard config
- **D-09:** Refactor existing test files to import from `internal/testutil` instead of duplicating these helpers

### Documentation Updates (CLN-08 / CR-25)
- **D-10:** Update README.md with accurate feature descriptions, including LIMIT support
- **D-11:** Create `SQL_DIALECT.md` with detailed SQL syntax reference covering:
  - Supported SELECT syntax (columns, `SELECT *`)
  - WHERE clause (equality only on PK and non-PK columns)
  - LIMIT support
  - Unsupported constructs (ORDER BY, GROUP BY, DISTINCT, HAVING, aggregations, subqueries, JOINs)
  - `$.` prefix support notation
  - Range scans — deferred to v2
</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Source of Truth
- `cr.md` — CR-15 through CR-25, the full code review findings for cleanup items
- `.planning/VERIFICATION_FINDINGS.md` — Current verification status of all findings
- `.planning/research/ARCHITECTURE.md` §6 — Cleanup recommendations and architecture impact

### Requirements
- `.planning/REQUIREMENTS.md` §v2.0.0 — CLN-01 through CLN-08 requirement definitions
- `.planning/ROADMAP.md` §Phase 11 — Success criteria and dependency information

### Prior Phase Context
- `.planning/phases/08-verification-foundation/08-CONTEXT.md` — Canonical PK encoder, SQL rejection, config validation
- `.planning/phases/09-materializer-engine-lifecycle/09-CONTEXT.md` — Sequential processing, error classification
- `.planning/phases/10-query-engine-transport/10-CONTEXT.md` — Predicate handling, UseNumber, _meta stripping

### Current Source Files
- `internal/materialize/mapper.go` — extractNestedField (CLN-01)
- `internal/cfg/config.go` — Config validation (CLN-02)
- `internal/kv/kv.go` — Package-level docs (CLN-03), dead code removal (CLN-05)
- `internal/materialize/writer.go` — ErrSkipAndAck removal (CLN-05)
- `internal/materialize/materializer.go` — SchemaPrefix, Stats.LastError removal (CLN-05)
- `internal/embed/node.go` — MustInitBucket removal (CLN-05)
- `internal/kv/kv.go` — EncodePKValue removal (CLN-05)
- `examples/*/main.go` — Error checking fixes (CLN-04)
- `internal/engine/engine_test.go` — createStream duplication (CLN-07)
- `internal/materialize/consumer_test.go` — createStream duplication (CLN-07)
- `.github/workflows/ci.yml` — Format check step (CLN-06)
- `README.md` — Docs update (CLN-08)
- `internal/query/` — SQL dialect reference for docs (CLN-08)
</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- `internal/embed/node.go` — `StartNode()` function, already exists for embedded NATS startup. Should be wrapped by testutil helpers.
- `internal/cfg/config.go:168-238` — Existing validation pattern (accumulate errors, return all). Index config validation (CLN-02) follows same pattern.
- `internal/materialize/mapper.go:124-125` — `extractNestedField` splits on `.` — simple `strings.TrimPrefix(path, "$.")` fix for CLN-01.

### Established Patterns
- Error accumulation in config validation (Phase 8 pattern) — reuse for index config error
- Doc comments follow Go standard conventions
- GitHub Actions CI currently does build + vet + test + coverage

### Integration Points
- `mapper.go:124-125` — Add `$` prefix stripping before split
- `config.go:Validate()` — Add index config check alongside existing key_fields checks
- `kv.go` — Remove `EncodePKValue` function, update comments
- `writer.go` — Remove `ErrSkipAndAck` usage
- `materializer.go` — Remove `SchemaPrefix` and `Stats.LastError`
- `embed/node.go` — Remove `MustInitBucket`
- All `examples/*/main.go` — Audit error handling
- `internal/*/..._test.go` — Replace duplicated NATS startup with testutil helpers
- `.github/workflows/ci.yml` — Add `gofmt -l` check step
</code_context>

<specifics>
No specific requirements — standard approaches per cr.md suggested fixes. The CR-22 list is the authoritative source for dead code symbols to remove.
</specifics>

<deferred>
None — discussion stayed within phase scope.
</deferred>

---

*Phase: 11-cleanup-documentation*
*Context gathered: 2026-07-01*
