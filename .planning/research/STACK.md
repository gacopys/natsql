# Stack Research: natsql v1.2 Code Review Remediation

**Project:** natsql — NATS-native materialized view engine
**Researched:** 2026-05-31
**Confidence:** HIGH — no new dependencies required; all fixes use existing stack

## Stack Assessment for Code Review Fixes

### No New Dependencies Required

All 25 code review findings are fixable within the existing technology stack. No new libraries, databases, or infrastructure components are needed. This section documents which existing stack elements are affected by each fix.

### Existing Stack (Unchanged)

| Technology | Version | Purpose | Affected by Fixes |
|------------|---------|---------|-------------------|
| Go | 1.22+ | Language | No changes needed |
| `vitess.io/vitess/go/vt/sqlparser` | latest | SQL parsing (SELECT-only AST) | CR-05: Add rejection logic for unsupported clauses |
| `github.com/nats-io/nats.go/jetstream` | v1.51+ | JetStream KV + Stream API | No API changes; different consumer config (CR-11, CR-12) |
| `github.com/go-chi/chi/v5` | v5.3.0 | HTTP router | CR-18: Better HTTP body handling (stdlib changes) |
| `github.com/spf13/cobra` | v1.10+ | CLI framework | CR-14: Optional --create-streams flag |
| `gopkg.in/yaml.v3` | v3.0.1 | YAML config | CR-08: More validation logic (no library changes) |
| `encoding/json` | stdlib | JSON config | CR-09: json.Decoder.UseNumber (already imported) |

### Standard Library Additions

The following standard library packages are already imported or trivially available:

| Package | Used For | Fix |
|---------|----------|-----|
| `net` | `net.Listen` for synchronous HTTP bind | CR-07 |
| `errors` | `errors.As` for `*http.MaxBytesError` | CR-18 |
| `strings` | `strings.HasPrefix` for internal field filtering | CR-04 |
| `bytes` | `bytes.NewReader` for `json.NewDecoder` | CR-09 |

### No External Dependencies

The zero-external-dependency constraint is maintained. No changes to `go.mod` are required for the core fixes.

### Potential go.mod Changes

| Change | Type | Fix |
|--------|------|-----|
| None | — | All fixes use existing dependencies |

---

*Stack research for: natsql v1.2 Code Review Remediation*
*Researched: 2026-05-31*
