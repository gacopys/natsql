<!-- GSD:project-start source:PROJECT.md -->
## Project

**natsql**

A NATS-native materialized view engine. Define stream-to-KV materializations declaratively (YAML/JSON), and query the resulting state with read-only SQL via NATS request-reply or HTTP. Write events to JetStreams, get queryable state — no database other than NATS.

For NATS developers building event-driven systems who need simple queryable state without running Postgres, Redis, or Kafka alongside their NATS cluster.

**Core Value:** A developer can define a materialized view from a stream, publish events, and query the current state with `SELECT ... WHERE ...` — zero infrastructure beyond NATS.

### Constraints

- **Infrastructure**: Zero external dependencies beyond NATS JetStream 2.8+
- **Language**: Go 1.22+
- **Storage**: All state in NATS JetStream streams (changelog) and KV buckets (snapshot)
- **Consistency**: CAS-based (read-committed), not serializable
- **SQL dialect**: Minimal v1 — no JOINs, no aggregations, no subqueries
- **Deployment modes**: Both Go library (embed) and standalone binary
<!-- GSD:project-end -->

<!-- GSD:stack-start source:research/STACK.md -->
## Technology Stack

## Recommended Stack
### Core Framework
| Technology | Version | Purpose | Why |
|------------|---------|---------|-----|
| Go | 1.22+ | Language | Constraint from PROJECT.md. |
| `vitess.io/vitess/go/vt/sqlparser` | latest | SQL parsing (SELECT-only AST) | Most battle-tested SQL parser in Go. Handles all edge cases for MySQL dialect. Clean AST for SELECT statements with WHERE clause extraction. |
| `github.com/nats-io/nats.go/jetstream` | v1.51+ | JetStream KV + Stream API | The official simplified JetStream client. Replaced the legacy `nats` package API. Required for KV bucket ops, stream consumption, CAS. |
| `github.com/nats-io/nats-server/v2` | v2.14+ | Embedded NATS server | Required for single-binary deployment (EMBED-02, EMBED-03). Enables no-external-dependency mode. |
### Query Engine
| Technology | Version | Purpose | Why |
|------------|---------|---------|-----|
| `vitess.io/vitess/go/vt/sqlparser` | latest | SQL → AST | Parse `SELECT ... WHERE ... LIMIT ...` into typed AST, extract table name, columns, WHERE expressions, LIMIT. |
| — (custom query planner) | — | AST → execution plan | Pattern: Parse → Analyze WHERE (indexed vs non-indexed) → Plan (PK/Index scan vs full scan) → Execute → Project → Return |
### HTTP API
| Technology | Version | Purpose | Why |
|------------|---------|---------|-----|
| `github.com/go-chi/chi/v5` | v5.3.0 | HTTP router | Lightweight (~1000 LOC), idiomatic (100% net/http compatible), no external deps. Better than gin for this project — we only need ~3 routes. Built-in middleware for logging, recovery, timeout. |
### CLI
| Technology | Version | Purpose | Why |
|------------|---------|---------|-----|
| `github.com/spf13/cobra` | v1.10+ | CLI framework | Already in the monorepo. Standard — powers Kubernetes, Hugo, GitHub CLI, NATS CLI itself. |
### Config
| Technology | Version | Purpose | Why |
|------------|---------|---------|-----|
| `gopkg.in/yaml.v3` | v3.0.1 | YAML config | Standard Go YAML library. 34k+ importers. Final release (stable, no churn). |
| `encoding/json` | stdlib | JSON config | Same struct tags as yaml.v3. Dual-format support with zero overhead. |
### Testing
| Technology | Version | Purpose | Why |
|------------|---------|---------|-----|
| `github.com/nats-io/nats-server/v2` | v2.14+ | Test harness NATS | Use `embed.StartNode` for integration tests. |
| `testing` + `net/http/httptest` | stdlib | HTTP test harness | Use with chi — serves HTTP handler directly, no server process needed. |
## Detailed Rationale
### 1. SQL Parser: vitess.io/vitess/go/vt/sqlparser
| Criteria | vitess | pingcap/tidb/parser | xwb1989/sqlparser | Hand-rolled |
|----------|--------|---------------------|--------------------|-------------|
| Maintenance | Active (Vitess/PlanetScale) | Active (PingCAP) | Dead (last commit 2019) | N/A |
| SELECT parsing | Full | Full | Full | Fragile |
| AST quality | Clean, well-documented | Detailed, MySQL-specific | Same as old vitess | Custom |
| Import weight | Larger (vitess module) | Standalone module | Standalone, deprecated | None |
| WHERE clause extraction | Trivial (Visit) | Trivial (Visit) | Trivial | Manual |
| Dependencies | More | Fewer | Fewer | None |
| Test coverage | Excellent | Excellent | Good | None |
- For a SELECT-only dialect, we don't need TiDB's extensive MySQL type system or DDL support — vitess's parser is more focused and its AST is simpler to traverse for read-only queries
- vitess's `sqlparser.Parse(sql)` returns a `Statement` that can be type-asserted to `*sqlparser.Select`, giving direct access to `SelectExprs`, `From`, `Where`, `OrderBy`, `Limit`
- The AST visitor pattern (`Walk`) makes WHERE clause analysis trivial
- Many projects use vitess just for the parser (etcd, PlanetScale, various SQL gateways)
- vitess has excellent error messages for malformed SQL
### 2. NATS JetStream KV: Capabilities and Limits
| Operation | Performance | Use for Query Engine |
|-----------|-------------|---------------------|
| `KV.Get(key)` | O(1) — direct lookup | PK equality: `WHERE id = 'abc'` → `Get("views.users.pk.abc")` |
| `KV.Create(key, val)` | O(1) — CAS | Index entry insertion (during materialization) |
| `KV.Update(key, val, rev)` | O(1) — CAS | Atomic updates during re-materialization |
| `KV.Keys()` / `KV.ListKeys()` | O(n) — returns ALL keys via watcher | Full table scan (fallback when no index matches) |
| `KV.Watch("prefix.>")` | O(n) — streams matching keys | Index lookups with wildcard pattern matching |
| `KV.Delete(key)` | O(1) — delete marker | Remove stale index entries |
- Maintain **separate KV buckets per materialized view** (or at minimum, separate key prefixes)
- Index entries are KV keys themselves: `idx.<colName>.<colValue>.<pkValue>` → `nil` (zero-byte value)
- For equality lookups: `Watch("idx.age.21.>")` → returns all keys matching the pattern, extract PKs from key suffix
- For range scans: `Watch("idx.age.>")` → stream all age index entries, filter client-side for range
- **Performance ceiling**: At ~100K keys, `Keys()` is fine. At 1M+ keys, latency becomes problematic
### 3. HTTP Framework: chi v5
| Framework | Verdict | Reason |
|-----------|---------|--------|
| `net/http` | Viable but verbose | We need middleware (logging, recovery, CORS, timeout). stdlib has no middleware chaining. |
| `chi v5` | **RECOMMENDED** | Lightweight, stdlib-compatible, built-in middleware, no external deps. |
| `gin` | Overkill | Custom context (not `http.Handler`). Heavy for 3 routes. Slower compile times. "Framework" mindset doesn't fit. |
| `echo` | Overkill | Similar to gin — more than we need. |
- 100% stdlib compatible — our handlers are plain `http.Handler`/`http.HandlerFunc`
- Middleware chaining via `r.Use()` — add logging, recovery, request-ID, timeout in 5 lines
- Sub-router mounting for versioned API (`/v1/query`)
- Go-chi ecosystem has `render` package for JSON responses
- **V5.3.0 released May 22, 2026** — actively maintained
### 4. Query Execution Pattern
- rqlite uses SQLite internally — not applicable (it's not KV-backed)
- dqlite wraps SQLite + Raft — same, not KV-backed
- Badger's query layer uses inverted indexes stored as KV pairs — same pattern we'll use
- Tigris uses FoundationDB's KV with a document layer on top — also same pattern
### 5. YAML/JSON Config: Direct, not Viper
| Approach | Pros | Cons |
|----------|------|------|
| Direct `yaml.v3` + `encoding/json` | Zero abstraction. One struct, two decoders. | Can't auto-reload config |
| spf13/viper | Env var binding, multiple sources, live reload | Heavier, more API surface, config is a known file |
### 6. CLI: Cobra
## Alternatives Considered (with reasons for rejection)
| Category | Recommended | Alternative | Why Not |
|----------|-------------|-------------|---------|
| SQL Parser | vitess | xwb1989/sqlparser | Dead project (last commit 2019). Fork of old vitess. Missing years of fixes. **Do not use.** |
| SQL Parser | vitess | pingcap/tidb/parser | Very good alternative. If vitess dependency weight becomes an issue, switch to TiDB parser. Standalone module, actively maintained. Acceptable substitute but not primary recommendation. |
| SQL Parser | vitess | Hand-rolled (participle/goyacc) | Not worth the risk. SQL parsing has many edge cases (escaping, precedence, comments). A battle-tested parser is cheap insurance. |
| HTTP | chi v5 | gin | Gin is not `http.Handler` compatible. Forces custom context. Not worth the abstraction cost for ~3 endpoints. |
| HTTP | chi v5 | net/http (stdlib) | Viable, but no middleware chaining. We'd hand-roll logging/recovery/timeout, which chi provides for free. |
| Config | yaml.v3 direct | spf13/viper | Overkill. Config is a single file at a known path. Viper's multi-source abstraction adds complexity without value. Can add later if needed. |
| CLI | cobra | urfave/cli | Cobra is the standard. Already used in monorepo. NATS CLI uses cobra. urfave/cli is well-regarded but not the NATS ecosystem convention. |
## Installation
# Core dependencies
# CLI
# Config
## Go Version Strategy
## Confidence Assessment
| Area | Confidence | Reason |
|------|------------|--------|
| SQL Parser | **HIGH** | vitess is actively maintained, proven, and the standard choice for Go SQL parsing |
| NATS Client | **HIGH** | Official NATS Go client, actively maintained, well-documented |
| HTTP Framework | **HIGH** | chi v5 is stable, idiomatic, and the right fit for minimal APIs |
| Config | **HIGH** | yaml.v3 is the standard, encoding/json is stdlib |
| CLI | **HIGH** | Cobra is the Go CLI standard, already in the monorepo |
| Query Architecture | **MEDIUM** | The pattern (parse→plan→execute) is proven, but the specific NATS KV indexing strategy needs validation in production with realistic data sizes |
## Sources
| Source | URL | Confidence |
|--------|-----|------------|
| vitess sqlparser | https://github.com/vitessio/vitess/tree/main/go/vt/sqlparser | HIGH (GitHub) |
| nats.go jetstream | https://pkg.go.dev/github.com/nats-io/nats.go/jetstream | HIGH (official docs) |
| NATS KV docs | https://docs.nats.io/nats-concepts/jetstream/key-value-store | HIGH (official docs) |
| chi v5 | https://github.com/go-chi/chi | HIGH (GitHub, v5.3.0 released May 2026) |
| cobra | https://github.com/spf13/cobra | HIGH (GitHub, v1.10.2) |
| yaml.v3 | https://pkg.go.dev/gopkg.in/yaml.v3 | HIGH (Go Packages) |
| xwb1989/sqlparser | https://github.com/xwb1989/sqlparser | MEDIUM (archived/unmaintained) |
| pingcap/tidb/parser | https://github.com/pingcap/tidb/tree/master/pkg/parser | HIGH (actively maintained) |
| rqlite architecture | https://github.com/rqlite/rqlite | MEDIUM (reference for query patterns) |

<!-- GSD:stack-end -->

<!-- GSD:conventions-start source:CONVENTIONS.md -->
## Conventions

Conventions not yet established. Will populate as patterns emerge during development.
<!-- GSD:conventions-end -->

<!-- GSD:architecture-start source:ARCHITECTURE.md -->
## Architecture

Architecture not yet mapped. Follow existing patterns found in the codebase.
<!-- GSD:architecture-end -->

<!-- GSD:skills-start source:skills/ -->
## Project Skills

No project skills found. Add skills to any of: `.claude/skills/`, `.agents/skills/`, `.cursor/skills/`, or `.github/skills/` with a `SKILL.md` index file.
<!-- GSD:skills-end -->

<!-- GSD:workflow-start source:GSD defaults -->
## GSD Workflow Enforcement

Before using Edit, Write, or other file-changing tools, start work through a GSD command so planning artifacts and execution context stay in sync.

Use these entry points:
- `/gsd-quick` for small fixes, doc updates, and ad-hoc tasks
- `/gsd-debug` for investigation and bug fixing
- `/gsd-execute-phase` for planned phase work

Do not make direct repo edits outside a GSD workflow unless the user explicitly asks to bypass it.
<!-- GSD:workflow-end -->



<!-- GSD:profile-start -->
## Developer Profile

> Profile not yet configured. Run `/gsd-profile-user` to generate your developer profile.
> This section is managed by `generate-claude-profile` -- do not edit manually.
<!-- GSD:profile-end -->
