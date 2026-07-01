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

> Full source: [`CONVENTIONS.md`](CONVENTIONS.md). The summary below is regenerated from it.

### Toolchain
- Go 1.22+ (toolchain `go 1.26.4`); module `github.com/gacopys/natsql`. Import order enforced by `gci`: stdlib / third-party / `github.com/gacopys/natsql`. No new top-level deps without justification.

### Package layout (root facade + `internal/`)
- `natsql/` public API (`New`, `NewWithNATS`, `NewEmbedded`, `Query`, `Close`) + `config.go` type aliases that break the import cycle; `internal/` packages never import the root; `cfg` holds config *types*; `engine` imports `cfg` (not the root).
- Add public symbols only to the root package. `cmd/natsql` is the cobra CLI.

### Naming
- PascalCase exported (`Engine`, `ViewConfig`); camelCase unexported; `Err`-prefixed sentinels (`ErrMalformedEvent`); config keys are `snake_case`; acronyms title-cased (`PK`).

### Error handling
- Wrap with `%w`: `fmt.Errorf("…: %w", err)`. Never swallow — assign `_` with a one-line why-comment. `Query` returns `*QueryResult` with `Error *string` (never a Go error). Constructors return `(T, error)`. Classify at the caller (`materializer.classifyWriteError`), not inside `Writer.Apply`.

### Logging
- `log/slog` only, structured k/v pairs. Pass `*slog.Logger` down; default `slog.Default()`. Tests discard output: `slog.New(slog.NewTextHandler(io.Discard, nil))`.

### Constructors & options
- `New…` names; variadic `Option func(*Engine)`; `WithLogger`/`WithHTTPServer`/`WithQueryPort`. `SetDefaults()`+`Validate()` run automatically in the facade. `Validate()` collects *all* errors and returns one joined message. Ownership is documented (`NewWithNATS` owns `nc.Close()`, `New` does not); `Close()` is idempotent.

### Concurrency
- One goroutine per view materializer, **sequential event processing** (no write-path worker pool — preserves JetStream ordering). Read-path full scan uses a 16-worker semaphore (fine). `Engine` state guarded by `sync.Mutex`; `sync.WaitGroup` for goroutines; `atomic.Int64` for counters. Graceful shutdown order (D-57): HTTP → NATS unsubscribe → drain → cancel → `wg.Wait()` — do not reorder.

### Code style (enforced by `.golangci.yml`)
`gofumpt`, `gci`, `errcheck` (+type-assertions), `exhaustive`, `errorlint`, `gosec` (excl G115), `nakedret`, `perfsprint`, `usestdlibvars`, `misspell` US, `predeclared`, `bodyclose`, `noctx`, `copyloopvar`, `sloglint`. Run `make lint-fix` before committing. `make gocyclo` flags >15; `make dupl` flags ≥50-token production dup (tests excluded).

### Comments
- Doc comment on every exported symbol; reference decisions by tag (D-07, FIX-MAT-04, CR-02) inline. No restating-code comments. TODOs need an owner/version (`// TODO(v2): …`).

### Testing
- `make test` → `go test -race -count=1 -coverprofile=coverage.out -v ./...`. **Always `-race`.** Three layers: unit (in-package `*_test.go`), integration (`internal/<pkg>/<pkg>_test.go` with embedded NATS), black-box (`natsql_blackbox_test.go`, `package natsql_test`, public `Query` only). Use `testutil.StartEmbeddedNATS(t)` — gives `t.TempDir()` store + `t.Cleanup`; **never** share a fixed store dir. Deterministic fixtures (`allTestRows`); poll-then-assert (`pollForCount`, ~200ms/30s) instead of bare `Sleep`; numbers come back as `json.Number` (assert `.String()`); keep `JSON_marshal_roundtrip`; `testHook…` package vars for rare goroutine inspection.

### Config
- One `Config` struct, dual `yaml`/`json` tags; `LoadConfig` dispatches on extension. `SetDefaults()` fills zeros (`BatchSize`→`MaxAckPending` migration); `Validate()` cross-checks `key_fields`↔`primary_key`, separator charset, duplicate names, and **rejects** unsupported `indexes`. New optional fields use `omitempty`.

### Examples
- One program per `examples/NN-name/` with own `go.mod` (built by `make examples`). Check every error; `defer eng.Close()`. Add a row to both README tables when adding one.

### Git & CI
- Trunk-based (`master`); use GSD workflow (`/gsd-quick`, `/gsd-execute-phase`) for non-trivial changes to keep `.planning/` in sync. CI: `lint`, `build`, `test`, `examples`, `vulnerability`, `cyclo`, `dupl` — all must be green. Messages: `type: subject` (`feat:`/`fix:`/`refactor:`/`test:`/`docs:`/`chore:`/`perf:`). Never commit `coverage.out`, `data/`, `example-*`, `.idea/`.

### Explicitly avoided
No `viper`; no `gin`/`echo`; no `xwb1989/sqlparser`; no write-path worker pool; no `database/sql`/Postgres/Redis; no DML (read-only SQL); no per-view KV buckets in v1.
<!-- GSD:conventions-end -->

<!-- GSD:architecture-start source:ARCHITECTURE.md -->
## Architecture

> Full source: [`ARCHITECTURE.md`](ARCHITECTURE.md). The summary below is regenerated from it.

Three components, no more. `Engine` (`internal/engine/`) wires them; root `natsql` facade re-exports for library/CLI/embedded use.

```
JetStream stream → Materializer (consumer→mapper→writer) → KV bucket → Query Engine → Transport (NATS/HTTP/Go)
                        │
                        └── malformed/terminal → DLQ stream (natsql-dlq)
```

### Components
- **Materializer** (`internal/materialize/`): durable pull consumer, `Mapper` (JSON path → typed `RowMutation`), `Writer.Apply` (`kv.Put` via `BuildPkKey`), DLQ routing, heartbeat, drain-on-shutdown.
- **Query Engine** (`internal/query/`): `Parse` (vitess) → `Validate` (against schema) → `BuildPlan` (`PKLookup`/`FullScan`/`Empty`) → `plan.Execute` (type-aware `valuesEqual`, `json.Number` precision, column projection).
- **Transport** (`internal/transport/`): `POST /api/v1/query` (chi, 1 MiB cap, reject trailing bytes), `natsql.query` subscription (raw SQL body). Identical `QueryResult{Results,Error}` envelope.

### Storage
- One KV bucket `natsql-views` (`kv.DefaultBucket`); one DLQ stream `natsql-dlq`.
- Keys: `{view}/pk/{sanitizedPk}` → row JSON (schema cols + `_meta{stream_seq,updated_at}`); `{view}/meta/schema` → `ViewSchema`.
- **Canonical PK encoding (CR-02):** `kv.BuildPkKey(view, pkParts, sep)` is the SINGLE encoder. Sanitizes once (`_→__`,`|→_p`,`/→_s`,`*→_a`,`>→_g`); separator is validated but not sanitized. Mapper returns RAW pkParts; writer/executor both call `BuildPkKey`. Re-adding sanitization in the mapper breaks read/write consistency.

### Write path
Single goroutine per view, **sequential** (no worker pool — preserves stream/PK ordering). Error policy: malformed → DLQ+Ack; transient (deadline/timeout/conn-refused/no-leader/closed) → NAK; terminal → DLQ+Ack; `ctx.Err()` → NAK. Drain channels closed before `cancel()` (D-58).

### Read path
`Query` lazy-inits KV (mutex) → `Parse` → `LoadSchema` (fresh, D-27) → `Validate` → `BuildPlan` → `Execute`. ALL `Where` conditions kept as post-filters incl. PK equalities (CR-03). Contradictory PK equalities (`id='a' AND id='b'`) → `EmptyPlan` (zero I/O). `json.Decoder.UseNumber` everywhere (CR-09 — >2^53 precision). `SELECT *` strips `_`-prefixed keys (`_meta`); explicit projection → nil for missing columns (D-31). `nil` results → `[]` (D-33).

### Lifecycle (D-57 order — do not reorder)
1. `httpServer.Shutdown(5s)`  2. `natsSub.Unsubscribe()`  3. close drain channels (each materializer `cons.Drain()`)  4. `cancel()`  5. `wg.Wait()`. Root facade then shuts down embedded NATS (`NewEmbedded`) and closes owned `nc` (`NewWithNATS`); idempotent; returns `ErrNotStarted` if not started.

### Shutdown startup (CR-07)
`Start()`: `InitBucket` → `EnsureDLQStream` → store schemas (warn on fail) → materializers with 500ms startup-error window (fail-fast) → NATS handler (fatal) → **`net.Listen` before `Serve`** (synchronous bind). `ErrAlreadyStarted` if called twice.

### Critical invariants (preserve when refactoring)
1. Sanitize PK exactly once (`BuildPkKey`). 2. `json.Number` precision in both mapper + executor. 3. `SELECT *` strips `_`-prefixed keys. 4. All `Where` conditions post-filter (incl PK). 5. Contradictory PK → `EmptyPlan`. 6. Sequential write path (no worker pool). 7. Drain before cancel. 8. No `InactiveThreshold` on durable consumers (CR-11). 9. CLI only auto-creates streams in embedded mode, respects `source_subject` (CR-14). 10. HTTP: `MaxBytesReader` + `errors.As(*http.MaxBytesError)` + reject trailing bytes + 127.0.0.1 bind (T-02-06). 11. NATS handler checks `nc.Flush()` (CR-19). 12. Schema loaded fresh per query (D-27). 13. Path depth capped at 8 (T-02-02). 14. `$.` path prefix stripped. 15. `Query` thread-safe before `Start()` (lazy KV under mutex). Full list + edge cases in ARCHITECTURE.md §8.

### Extension points
- New column type: `ColumnType` + `Valid()` + `mapper.validateType` + `valuesEqual` (`exhaustive` guards switches).
- New WHERE op: `Op` const + `comparisonToCondition` + `filterRow` switch + planner + `SQL_DIALECT.md`.
- Secondary indexes: remove `Validate` rejection + `writer.Apply` index maintenance + index-scan `Plan`.
- Deletes/tombstones, per-view KV buckets, OR, range scans: deferred to v2 (`kv` package doc, ARCHITECTURE.md §11).
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
