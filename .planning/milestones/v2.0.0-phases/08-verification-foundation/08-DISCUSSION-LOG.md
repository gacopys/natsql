# Phase 8: Verification & Foundation — Discussion Log

> **Audit trail only.** Do not use as input to planning, research, or execution agents.
> Decisions are captured in CONTEXT.md — this log preserves the alternatives considered.

**Date:** 2026-06-01
**Phase:** 08-verification-foundation
**Areas discussed:** Verification format, PK encoder location, SQL rejection boundaries, Config validation scope, Testing approach

---

## Verification Format (VER-01)

| Option | Description | Selected |
|--------|-------------|----------|
| Structured findings doc | Create VERIFICATION_FINDINGS.md with per-finding table | ✓ |
| Annotate cr.md directly | Append inline annotations to existing cr.md | |
| Inline in test comments | Document verification as test cases | |

**User's choice:** Structured findings doc
**Notes:** Per-finding table with ID, severity, source lines, confirmed/dismissed, reasoning, fix phase

---

## PK Encoder Location (FND-01)

| Option | Description | Selected |
|--------|-------------|----------|
| kv package | Place BuildPkKey() in internal/kv/kv.go | ✓ |
| Dedicated internal/pkutil | New shared package | |
| Export from engine | Place in internal/engine | |

**User's choice:** kv package
**Notes:** `BuildPkKey(viewName, pkParts, separator)` in `internal/kv/kv.go`

---

## SQL Rejection Boundaries (FND-02)

| Option | Description | Selected |
|--------|-------------|----------|
| All as specified | Reject DISTINCT, ORDER BY, GROUP BY, HAVING, aggregations, subqueries, non-column selects | ✓ |
| Allow simple expressions | Same but allow SELECT 1, SELECT 'literal' | |

**User's choice:** Reject everything not supported — whitelist-only parser
**Notes:** Add explicit error per construct. Will add support when we implement features.

---

## Config Validation Scope (FND-03)

| Option | Description | Selected |
|--------|-------------|----------|
| Strict equality | key_fields must exactly equal primary_key columns | ✓ |
| Relaxed subset | key_fields must be subset of primary_key columns | |

**User's choice:** Strict equality — key_fields == primary_key columns
**Notes:** Also validate uniqueness of view names, column names, key_fields

---

## Testing Approach

| Option | Description | Selected |
|--------|-------------|----------|
| Co-located with fixes | Tests alongside code in same package | ✓ |
| Dedicated verification file | New VERIFICATION_test.go per package | |
| Deferred to test phase | Skip tests now | |
| Mixed — per-finding co-located | Minimal unit tests per fix | |

**User's choice:** Co-located with fixes
**Notes:** Black-box tests for PK encoding with special characters, negative parser tests, config validation units

---

## Deferred Ideas

None.
