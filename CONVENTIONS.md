# natsql Conventions

Authoritative coding, testing, and style conventions for the natsql project. Every contributor (human or AI agent) MUST follow these. Enforcement is partly automated via `.golangci.yml` and the `Makefile` targets.

## 1. Language & Toolchain

- **Go 1.22+** (constraint). Toolchain pinned to `go 1.26.4` in `go.mod`; CI/lint runs against that line.
- **Module path**: `github.com/gacopys/natsql`. All internal imports use this prefix.
- **No new top-level dependencies** without justification. The stack is intentionally minimal (vitess sqlparser, nats.go/jetstream, nats-server/v2, chi/v5, cobra, yaml.v3). Imports are grouped in `gci` order: stdlib, third-party, `github.com/gacopys/natsql`.

## 2. Package Layout

Repository-root facade + `internal/` packages. The split exists to break an import cycle (root imports engine; engine imports config types).

```
natsql/                       # public facade: New, NewWithNATS, NewEmbedded, Query, Close
  config.go                   # type aliases re-exporting cfg.* (breaks cycle)
  natsql.go                   # Engine wrapper + lifecycle ownership
  cmd/natsql/                 # standalone cobra CLI binary
  internal/
    cfg/                      # Config structs + LoadConfig + Validate
    engine/                   # lifecycle, Start/Close, Query entry point
    embed/                    # embedded NATS server (single-node)
    kv/                       # KV bucket, schema, canonical PK encoding
    materialize/              # consumer → mapper → writer pipeline, DLQ
    query/                    # parse → validate → plan → execute
    transport/                # NATS request-reply + HTTP (chi) handlers
    testutil/                 # shared test helpers (embedded NATS, streams)
  examples/NN-name/main.go    # each example is its own module (go.mod)
```

- Public API lives only in the root `natsql` package. `internal/` never imports the root package.
- `cfg` holds config *types*; `engine` imports `cfg` (not the root) to avoid the cycle.
- The root package re-exports config types via `type … = cfg.…` aliases — do not duplicate type definitions.

## 3. Naming

- Exported types: `Engine`, `Config`, `ViewConfig`, `QueryResult`, `RowMutation`. PascalCase.
- Unexported helpers: camelCase (`extractTableName`, `filterRow`).
- Package-level sentinel errors: `Err`-prefixed (`ErrMalformedEvent`, `ErrAlreadyStarted`, `ErrKeyNotFound`). Wrap with `%w` when propagating.
- Constants: `PascalCase` for exported (`DefaultBucket`, `DLQStreamName`), `camelCase` for unexported (`fullScanWorkers`, `maxNestingDepth`).
- Acronyms are title-cased (`PK`), not all-caps — except in struct field JSON tags which use `snake_case`.
- Config YAML/JSON keys use `snake_case` (`source_stream`, `key_fields`, `primary_key`, `max_ack_pending`).

## 4. Error Handling

- **Wrap with context + `%w`**: `fmt.Errorf("creating consumer for view %q: %w", viewName, err)`.
- **Sentinel errors** for values callers compare against (`errors.Is`). Defined package-level, grouped in a `var (...)` block.
- **Never swallow errors silently.** If you intentionally ignore one, assign to `_` and add a one-line comment explaining why (see `transport/nats.go` for the pattern). Lint `errcheck` flags unchecked returns.
- **Public `Query` returns a `*QueryResult`**, never a Go error. Errors go in `QueryResult.Error` (a `*string`) so the JSON envelope is consistent across NATS/HTTP/Go callers.
- **Constructors return `(T, error)`** for real errors. Validation failures are errors, not panics.
- **Error classification at the caller**, not the callee. `Writer.Apply` returns a plain error; `materializer.classifyWriteError` decides transient vs terminal policy.

## 5. Logging

- **`log/slog`** only. No `fmt.Println` / `log` in library code. CLI `main.go` may print final user-facing output.
- Pass a `*slog.Logger` down; default to `slog.Default()` when none provided (`WithLogger` option).
- Always log structured key/value pairs: `logger.Info("materializer started", "view", vc.Name, "source_stream", vc.SourceStream)`.
- Levels: `Info` lifecycle start/stop, `Warn` recoverable issues, `Error` non-fatal failures. The `sloglint` linter enforces no-mixed-args.
- Tests use `slog.New(slog.NewTextHandler(io.Discard, nil))` to keep output clean.

## 6. Constructors & Options

- Constructors named `New…` (`New`, `NewWithNATS`, `NewEmbedded`). `New…Embedded` owns the embedded NATS lifecycle.
- Variadic options pattern: `type Option func(*Engine)`; functional options `WithLogger`, `WithHTTPServer`, `WithQueryPort`. Validate the zero state inside the constructor.
- `SetDefaults()` and `Validate()` are called automatically by the public facade constructors — callers don't have to remember. `Validate()` collects *all* errors and returns one joined message, not first-fail.
- Ownership is explicit in doc comments: `NewWithNATS` "owns `nc.Close()`"; `New` does not. `Close()` is idempotent and safe to call multiple times.

## 7. Concurrency

- Each view's materializer runs events **sequentially in one goroutine** (single durable pull consumer, no worker pool). This preserves JetStream per-subject ordering — see ARCHITECTURE.md. Do NOT reintroduce parallel event processing.
- `Engine` guards mutable state (`started`, `kv`, `httpServer`) with `sync.Mutex`. `Query` lazy-initializes the KV under the lock.
- `sync.WaitGroup` tracks materializer + HTTP goroutines; `Close()` waits on it.
- `atomic.Int64` for counters (event count).
- Graceful shutdown ordering is fixed (D-57): HTTP shutdown → NATS unsubscribe → drain signal → cancel context → `wg.Wait()`. Don't reorder.
- Full-scan query path uses a 16-worker semaphore (`fullScanWorkers`) — that's read-path parallelism, fine; write path must stay sequential.

## 8. Code Style (enforced)

`.golangci.yml` enables ~30 linters. Notable ones and what they force:

- `gofumpt` — stricter gofmt. Run `make lint-fix` before committing.
- `gci` — import grouping (stdlib / third-party / `github.com/gacopys/natsql`).
- `errcheck` with `check-type-assertions: true` — no unchecked type assertions.
- `exhaustive` — every enum switch must have a `default` or cover all cases (e.g. `ColumnType`, `Op`).
- `errorlint` — proper error wrapping/comparison (`errors.Is`, not `==`).
- `gosec` (excludes G115) — security-sensitive patterns.
- `nakedret` — no bare returns in funcs > 30 lines.
- `perfsprint` — `strconv` over `fmt.Sprintf` for hot paths.
- `usestdlibvars` — use `http.Status…`, `time.RFC3339`, etc. constants.
- `misspell` US locale; `predeclared` — don't shadow `error`, `len`, etc.
- `bodyclose`, `noctx` — close HTTP bodies, attach contexts to requests.
- `copyloopvar` — rely on Go 1.22+ loop var capture; don't copy.

**Cyclomatic complexity**: `make gocyclo` flags functions > 15. Keep functions small; extract helpers. **Code duplication**: `make dupl` flags ≥50-token duplication in production code (tests excluded).

## 9. Comments & Doc Comments

- Every exported identifier has a doc comment starting with the identifier name. Package comments go on `package` declarations, often in a separate doc-style comment block.
- Reference design decisions by tag (D-07, D-28, FIX-MAT-04, CR-02, T-02-02) inline so they trace back to `.planning/research/`.
- **Do not add comments that restate code.** Comments explain *why*, link to decisions, or warn about edge cases. The repo style avoids redundant inline comments (see the sparse comment density in `executor.go`).
- TODOs must name the owner/version: `// TODO(v2): per-view KV buckets` — not bare `// TODO`.

## 10. Testing Conventions

```
go test -race -count=1 -coverprofile=coverage.out -v ./...    # make test
```

- **Always run with `-race`**. Concurrent goroutines + NATS mean race detection catches real bugs.
- **Three layers**:
  1. **Unit tests** in `*_test.go` within the package (`package query`, `package materialize`) — fast, isolated, use mocks/embedded NATS only when needed.
  2. **Integration tests** within `internal/<pkg>/<pkg>_test.go` — full embedded NATS, real KV, real materializer (`materializer_test.go`, `engine_test.go`).
  3. **Black-box tests** at repo root (`natsql_blackbox_test.go`, `package natsql_test`) — go through the public `Query` API only, no internal state inspection. Assert *behavior*, not implementation.
- **Embedded NATS in tests**: use `internal/testutil.StartEmbeddedNATS(t)`. It gives each test an isolated `t.TempDir()` store and registers `t.Cleanup` for the server + connection. **Never** share a fixed JetStream store dir across tests — that causes flaky cross-pollution.
- **Test data is deterministic**. See `allTestRows` in `natsql_blackbox_test.go`: 30 fixed rows with known distributions across cities/ages/active flags, so expected counts can be computed by hand/slice filtering.
- **Awaiting materialization**: tests poll via the public `Query` API, not `time.Sleep` alone (sleep is a hint, poll-loop is the contract). Use a helper like `pollForCount(t, ctx, eng, "users", 30)` with ~200ms interval + 30s deadline.
- **HTTP tests** use `net/http/httptest` directly against the chi router — no server process.
- **Table-driven tests** are the norm. Group with `t.Run("subcase", …)`. Keep expected/actual compare type-aware (numbers come back as `json.Number` or `float64`).
- **Type integrity**: assert `row["age"].(json.Number)` round-trips with `.String() == "25"`, not `float64(25)`. This guards the `UseNumber` precision invariant.
- **JSON roundtrip test**: `TestBlackBox…JSON_marshal_roundtrip` marshals `QueryResult`, unmarshals, and byte-compares — keep this pattern for any new result-shaping code.
- **Test hook pattern**: production code exposes a package-level `var testHook… func()` (zero overhead when nil) that tests set to observe internal goroutines (see `materializer.testHookProcessGoroutine`). Use sparingly.
- **Lint your tests**: `make lint` runs over `./...` including tests. `dupl` excludes `*_test.go` so test boilerplate is fine, but production dup is not.

## 11. Config Conventions

- One `Config` struct, dual YAML/JSON tags (`yaml:"…" json:"…"`). `LoadConfig(path)` dispatches on file extension.
- `SetDefaults()` fills zero values; `Validate()` runs *after* defaults and cross-validates (e.g. `key_fields` ↔ `primary_key` columns must match exactly).
- **Deprecated fields migrate, then validate**: `BatchSize` → `MaxAckPending` in `SetDefaults`. Keep one release of backward-compat.
- Add new fields as `omitempty` unless required. Required fields get a non-empty check in `Validate`.
- **Reject unsupported features at config load**, not at runtime: `Indexes` block currently returns a validation error pointing the user to remove it.
- `key_separator` must match `^[-/_=.a-zA-Z0-9]+$` (NATS KV key charset). Validated by `isValidKeySeparator`.

## 12. Examples

- One self-contained Go program per directory under `examples/`, each with its own `go.mod` (built by `make examples` via `cd dir && go build .`).
- `examples/NN-name/main.go`, numbered, with a README per directory. Start with `natsql.NewEmbedded` (zero infra) unless demonstrating an external NATS mode.
- Examples must compile (`make examples` in CI) — check every error, own `eng.Close()` via `defer`.
- Add a row to the table in `README.md` and `examples/README.md` when introducing a new example.

## 13. Git & CI

- Trunk-based: commits go to `master`. Use GSD workflow (`/gsd-quick`, `/gsd-execute-phase`) for non-trivial changes — it keeps `.planning/` state in sync.
- CI workflows (`.github/workflows/`): `lint`, `build`, `test`, `examples`, `vulnerability` (govulncheck), `cyclo`, `dupl`. All must be green on PRs.
- **Never commit** `coverage.out`, `data/`, `example-*` (ephemeral runs), `.idea/` — all in `.gitignore`.
- Commit messages: `type: subject` (`feat:`, `fix:`, `refactor:`, `test:`, `docs:`, `chore:`, `perf:`). Keep the conventional history seen in `git log`.

## 14. Things Explicitly Avoided

- **No `spf13/viper`** — direct yaml/json decode. Config is one known file.
- **No `gin`/`echo`** — chi v5 only, stdlib `http.Handler`-compatible.
- **No `xwb1989/sqlparser`** — dead. vitess only. TiDB parser is an acceptable fallback if vitess weight becomes an issue.
- **No worker pool on the write path** — ordering correctness. See ARCHITECTURE.md §Materializer.
- **No `database/sql`, no Postgres/Redis** — all state in NATS JetStream KV. This is the project's defining constraint.
- **No DML (INSERT/UPDATE/DELETE/ALTER)** — SQL is read-only. Writes happen only by publishing to streams.
- **No per-view KV buckets in v1** — single `natsql-views` bucket. Planned for v2.