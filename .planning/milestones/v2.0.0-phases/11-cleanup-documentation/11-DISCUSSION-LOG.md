# Phase 11: Cleanup & Documentation — Discussion Log

**Date:** 2026-07-01
**Status:** Context captured, ready for planning

## Areas Discussed

### 1. $. Prefix Handling (CLN-01 / CR-15)
- **Question:** How to handle $. field prefix?
- **Options presented:** Strip $ in mapper (both work) | Fix tests + document restriction | Both: strip + warn
- **Selected:** Strip $ prefix in mapper (both `$.field` and `field` work)
- **Rationale:** 2-line backward-compatible fix. Research recommendation.
- **Decision:** D-01

### 2. Dead Code Removal Scope (CLN-05 / CR-22)
- **Question:** CR-22 list only or broader scan?
- **Options presented:** CR-22 list only | Broader dead code scan | Claude's discretion
- **Selected:** CR-22 list only (6 specific symbols)
- **Rationale:** Minimal scope, well-defined, no surprises.
- **Decision:** D-05

### 3. Test Helper Dedup (CLN-07 / CR-24)
- **Question:** Test helper dedup approach?
- **Options presented:** internal/testutil package | Embed pkg as home | Minimal: dedup createStream only
- **Selected:** `internal/testutil` package with shared StartEmbeddedNATS + CreateStream
- **Rationale:** Cleanest separation, all packages import from one place.
- **Decision:** D-08, D-09

### 4. Index Config (CLN-02 / CR-16)
- **Question:** Index config handling?
- **Options presented:** Validation error (recommended) | Documented no-op warning | Claude's discretion
- **Selected:** Validation error at config load time
- **Decision:** D-02

### 5. Delete Docs (CLN-03 / CR-17)
- **Question:** Documentation approach for delete semantics?
- **Options presented:** README + code comments (recommended) | README only | Claude's discretion
- **Selected:** README known-limitations + package-level doc comments
- **Decision:** D-03

### 6. Docs Format (CLN-08 / CR-25)
- **Question:** What format for docs update?
- **Options presented:** README + SQL_DIALECT.md (recommended) | README only | In-code docs + README
- **Selected:** README + SQL_DIALECT.md
- **Decision:** D-10, D-11

### 7. CI Format Check (CLN-06 / CR-23)
- **Question:** CI format check behavior?
- **Options presented:** Fail on unformatted (recommended) | Warn only
- **Selected:** Fail on unformatted (gofmt -l blocks build)
- **Decision:** D-06, D-07

## Deferred Ideas

None — discussion stayed within phase scope.
