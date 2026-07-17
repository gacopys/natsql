# natsql

## What This Is

A NATS-native materialized view engine — **shipped v2.0 with full code review remediation and stabilization**. Define stream-to-KV materializations declaratively (YAML/JSON), and query the resulting state with read-only SQL via NATS request-reply, HTTP, or in-process Go calls. Write events to JetStreams, get queryable state — no database other than NATS. Hardened with sequential processing, error classification, synchronous startup, large-integer precision, golangci-lint/govulncheck CI enforcement, and all lifecycle/resilience bugs fixed.

For NATS developers building event-driven systems who need simple queryable state without running Postgres, Redis, or Kafka alongside their NATS cluster.

## Core Value

A developer can define a materialized view from a stream, publish events, and query the current state with `SELECT ... WHERE ...` — zero infrastructure beyond NATS.

## Current Milestone: v2.1 Code Stabilization

**Goal:** Fix all bugs and code quality issues verified in the cr3.md code review — the entire P0-P3 finding set. No new features. A pure hardening release.

**Target features:**
- **Lifecycle & resilience fixes:** Start failure cleanup, Query-after-Close guard, panic safety, CLI cleanup
- **Read/write consistency:** Canonical PK value formatter, safe key_separator validation
- **SQL dialect hardening:** Reject aliases, mixed *, NULL literals; full-column contradiction detection
- **Config validation fix:** Reject duplicate key_fields
- **Contract/doc alignment:** WithHTTPServer host semantics, deduplicate schema storage, CAS->LWW doc fix
- **Transport hardening:** Typed error sentinels, errorEnvelope helper, DLQ stream caps, consumer bounds
- **API gaps:** HTTP disable option, non-owning NATS conn option, HTTPAddr accessor
- **Cleanup & smells:** Dead code removal, default centralization, logger wiring, style fixes

## Current State (v2.0 — Shipped)

Shipped **v2.0** — all v1.x milestones (v1.0 functional prototype → v1.1 tech debt → v1.2 code review remediation) plus the cr3.md code review verification. Codebase clean, CI hardened, all vulnerabilities fixed (Go 1.26.4).

**Architecture:** 3-component model — Materializer (stream→KV, sequential ordered processing with error classification), Query Engine (SQL→KV reads with full predicate support and large-integer precision), Transport (NATS request-reply, HTTP/JSON, embedded Go API).

**Tech stack:** Go 1.26, NATS JetStream KV, vitess sqlparser, chi HTTP router, Cobra CLI, GitHub Actions CI (5 parallel workflows: lint, vulnerability, build, test, examples).

**Deployment modes:** Go library (`import natsql`), standalone binary (embedded or external NATS).

**Key delivery:**
- 25/25 CR findings verified and fixed (100% correctness)
- Sequential processing with per-event timeout (MAT-01)
- Transient→NAK / terminal→DLQ error classification (MAT-02)
- Synchronous startup error propagation (LIFE-02)
- Large integer precision via UseNumber (QENG-03)
- HTTP trailing data rejection, NATS Flush/Respond error surfacing (TRN-02/03)
- golangci-lint (40 linters), govulncheck, all vulnerabilities fixed
- Go 1.26.4 with all dependencies upgraded
- cr3.md verification — 14 real bugs, 38 smells, all confirmed against source

## Requirements

### Validated

- ✓ **DECL-01**: User can define a materialized view in a YAML/JSON config — v1.0
- ✓ **STREAM-01**: Materializer consumes JetStream events (ordered, durable) and maintains KV bucket state — v1.0
- ✓ **STREAM-02**: Materializer recovers from crash — durable consumer resumes from last ack — v1.0
- ✓ **QUERY-01**: User can query KV state with `SELECT ... WHERE <col> = <val>` (PK lookup) — v1.0
- ✓ **QUERY-04**: Query via NATS request-reply (`nc.Request("natsql.query", sqlBytes)`) — v1.0
- ✓ **QUERY-05**: Query via HTTP/JSON API — v1.0
- ✓ **EMBED-01**: Go library importable and usable from within a Go process — v1.0
- ✓ **EMBED-02**: Standalone server binary with HTTP + NATS query endpoints — v1.0
- ✓ **EMBED-03**: Works with embedded NATS (single-node) — v1.0
- ✓ **RESIL-01**: Graceful handling of malformed events (log + skip, don't crash) — v1.0
- ✓ **FIX-ENG-01**: PK equality query applies non-PK WHERE conditions as post-filter — v1.1
- ✓ **FIX-ENG-02**: Engine.Query is safe for concurrent access (data race fixed) — v1.1
- ✓ **FIX-ENG-03**: filterRow uses type-aware comparison instead of fmt.Sprint — v1.1
- ✓ **FIX-ENG-04**: SQL parser accepts boolean literals (true/false) in WHERE — v1.1
- ✓ **FIX-MAT-01**: PK values are sanitized against KV key special characters — v1.1
- ✓ **FIX-MAT-02**: DLQ publish failures are surfaced and block event acknowledgement — v1.1
- ✓ **FIX-MAT-03**: Engine.Start partial-init path cleans up correctly on failure — v1.1
- ✓ **FIX-MAT-04**: JSON integer values >2^53 preserve full precision — v1.1
- ✓ **FIX-TRN-01**: HTTP server has read/write/idle timeouts configured — v1.1
- ✓ **FIX-TRN-02**: HTTP query endpoint enforces request body size limit — v1.1
- ✓ **FIX-TRN-03**: NATS query handler uses bounded context with timeout — v1.1
- ✓ **FIX-TRN-04**: Dead code, unused parameters, and test flakiness cleaned up — v1.1
- ✓ **VER-01**: Each finding in cr.md examined against source and confirmed/dismissed — v1.2
- ✓ **FND-01**: Canonical PK encoder (BuildPkKey) used by write and read paths — v1.2
- ✓ **FND-02**: Unsupported SQL constructs rejected at parse time — v1.2
- ✓ **FND-03**: Config cross-validation (key_fields vs primary_key) — v1.2
- ✓ **MAT-01**: Sequential ordered processing, worker pool removed — v1.2
- ✓ **MAT-02**: Error classification (transient→NAK, terminal→DLQ) — v1.2
- ✓ **MAT-03**: InactiveThreshold removed, durable consumers persist — v1.2
- ✓ **MAT-04**: BatchSize renamed to MaxAckPending — v1.2
- ✓ **QENG-01**: All WHERE conditions retained as post-filters — v1.2
- ✓ **QENG-02**: SELECT * excludes internal _meta fields — v1.2
- ✓ **QENG-03**: UseNumber for large integer precision — v1.2
- ✓ **QENG-04**: WatchAll + HasPrefix prefix filter for single-view scans — v1.2
- ✓ **LIFE-01**: HTTP port from cfg.HTTP.Port — v1.2
- ✓ **LIFE-02**: Synchronous startup error propagation — v1.2
- ✓ **TRN-01**: CLI --create-streams flag with source_subject — v1.2
- ✓ **TRN-02**: errors.As for MaxBytesError, trailing data rejection — v1.2
- ✓ **TRN-03**: NATS Flush/Respond error surfacing — v1.2
- ✓ **TRN-04**: "marshaling row" → "unmarshaling row" — v1.2
- ✓ **CLN-01**: $. prefix support in extractNestedField — v1.2
- ✓ **CLN-02**: Index config validation error — v1.2
- ✓ **CLN-03**: Delete/tombstone semantics documented — v1.2
- ✓ **CLN-04**: Example error handling fixed, lifecycle hazards resolved — v1.2
- ✓ **CLN-05**: Dead code removed (Stats.LastError, dlqStream, etc.) — v1.2
- ✓ **CLN-06**: gofmt enforced in CI — v1.2
- ✓ **CLN-07**: Test helpers deduplicated into internal/testutil — v1.2
- ✓ **CLN-08**: SQL_DIALECT.md created, README updated — v1.2

### Active (v2.1 — Code Stabilization)

- [ ] **LIFE-01**: `Engine.Start` cleans up all partially-started resources on error (goroutines, drain channels, NATS sub, HTTP server)
- [ ] **LIFE-02**: `Engine.Query` rejects queries after `Close()` with explicit error
- [ ] **LIFE-03**: Materializer does not die silently on panic — error is surfaced, in-flight message is NAK'd
- [ ] **LIFE-04**: CLI calls `eng.Close()` on all failure paths
- [ ] **LIFE-05**: `Engine.Query` lazy-init does not hold the mutex across a KV RPC
- [ ] **PK-01**: A single canonical `FormatPKPart` is used by both writer and planner; number-typed PKs with non-canonical JSON forms (100.0, 1e10) are reachable via PK lookup
- [ ] **PK-02**: `key_separator` is restricted to `/` and `_` (chars escaped by `SanitizePK`) to prevent composite-key collisions
- [ ] **SQL-01**: Column aliases (`SELECT id AS x`) are rejected at parse time with an explicit error
- [ ] **SQL-02**: Mixed `*` with explicit columns (`SELECT *, id`) is rejected at parse time
- [ ] **SQL-03**: `NULL` literals in WHERE are rejected at parse time
- [ ] **SQL-04**: Contradictory WHERE predicates on any column (not just PK) produce `EmptyPlan`
- [ ] **CFG-01**: Duplicate `key_fields` entries are rejected by config validation
- [ ] **DOC-01**: `WithHTTPServer` honours the host component (0.0.0.0 override works as documented)
- [ ] **DOC-02**: Duplicate `StoreSchema` call removed from `materialize.Run` (engine is the single owner)
- [ ] **DOC-03**: Consistency constraint in docs corrected from "CAS-based" to "idempotent upsert within sequential consumer"
- [ ] **TSP-01**: NATS and HTTP error envelopes use `json.Marshal` of a real struct (no hand-rolled JSON strings)
- [ ] **TSP-02**: Write error classification uses typed NATS/jetstream sentinels where available; substring fallback logs a warning
- [ ] **TSP-03**: All mapper errors (not just `ErrMalformedEvent`) route to DLQ + Ack
- [ ] **ROB-01**: DLQ stream config includes `MaxMsgs`/`MaxBytes` caps
- [ ] **ROB-02**: Consumer config bounds (AckWaitSeconds, MaxAckPending, MaxDeliver) validated against negative values
- [ ] **ROB-03**: DLQ envelope timestamp uses `RFC3339Nano` for sub-second precision
- [ ] **ROB-04**: `FullScanPlan` pre-normalises condition values once per query (not per-row)
- [ ] **APIGAP-01**: User can disable HTTP transport via `WithHTTPDisabled()` / config
- [ ] **APIGAP-02**: Library users can add a NATS query subscription without transferring connection ownership (`WithNATSQueryConn`)
- [ ] **APIGAP-03**: Bound HTTP port is discoverable via `HTTPAddr()` / `Stats`
- [ ] **CLN-01**: Dead code removed (deprecated `PKKey`, dead `float64` branches in `validateType`, redundant port defaults)
- [ ] **CLN-02**: Default PK separator logic centralised in `BuildSchema` (removed from materializer and planner)
- [ ] **CLN-03**: CLI passes logger to engine construction; query log level is `Debug` not `Info`
- [ ] **CLN-04**: Redundant `SetDefaults` call removed from CLI; redundant `Content-Type` sets hoisted; `oapi.QueryResult` vs `query.QueryResult` rationalised
- [ ] **CLN-05**: `PKLookupPlan` has explicit `Limit` awareness (documented behaviour)
- [ ] **CLN-06**: `EmptyPlan.Columns` dead field removed; detached doc comment fixed
- [ ] **CLN-07**: `embed.StartNode` and `testutil.StartEmbeddedNATS` share a base helper

### Out of Scope

- Range scans (`>`, `<`, `>=`, `<=`) — deferred; cr3 code stabilization took priority
- Secondary indexes — deferred; cr3 code stabilization took priority
- Delete/tombstone semantics — deferred; cr3 code stabilization took priority
- Per-view KV buckets — deferred; cr3 code stabilization took priority
- DML (INSERT/UPDATE/DELETE via SQL) — writes only happen through stream messages
- Multi-table JOINs — deferred
- Complex transactions / serializable isolation
- Full pgwire / PostgreSQL protocol compatibility
- Aggregations (GROUP BY, COUNT, AVG) — deferred
- Subqueries, window functions, CTEs
- Schema migration / ALTER VIEW with backfill — requires full re-materialize
- External project integration — each project maintains independence

## Constraints

- **Infrastructure**: Zero external dependencies beyond NATS JetStream 2.8+
- **Language**: Go 1.22+
- **Storage**: All state in NATS JetStream streams (changelog) and KV buckets (snapshot)
- **Consistency**: CAS-based (read-committed), not serializable
- **SQL dialect**: Minimal v1 — no JOINs, no aggregations, no subqueries
- **Deployment modes**: Both Go library (embed) and standalone binary

## Key Decisions

| Decision | Rationale | Outcome |
|----------|-----------|---------|
| YAML/JSON config for schema definition | Works for both Go embed and standalone ops; trivial to parse; no DDL parser needed for v1 | ✓ Good |
| Read-only SQL (SELECT) | Writes go through streams; SQL is the read path. Separates concerns cleanly | ✓ Good |
| NATS request-reply + HTTP interfaces | NATS-native for app code, HTTP for tooling/curl | ✓ Good |
| Minimal dependencies | No external project dependencies; reference inspiration only. Keeps surface area minimal | ✓ Good |
| v1 = functional prototype | Prove the concept works end-to-end before hardening | ✓ Good |
| Minimal SQL (eq + range + LIMIT) | Covers the 90% use case for KV-backed state queries | ⚠️ Revisit: range scans deferred to v2 |
| vitess sqlparser | Battle-tested SQL parser, handles all edge cases | ✓ Good |
| Single KV bucket | Simpler to manage, fewer NATS resources | ✓ Good |
| Cobra CLI | Already in monorepo, NATS ecosystem standard | ✓ Good |
| UseNumber in decode paths | Preserves large integer precision above 2^53 (D-07/D-08) | ✓ Good |
| WatchAll + HasPrefix (not Watch) | KV keys use '/' separating structural from PK data — prefix Watch conflicts with NATS subject tokenization (D-11) | ✓ Good |
| Sequential processing per view | Preserves JetStream per-subject ordering (CR-01) | ✓ Good |
| Error classification (transient→NAK) | Prevents temporary NATS outages from causing permanent data loss | ✓ Good |

## Evolution

This document evolves at phase transitions and milestone boundaries.

**After each phase transition** (via `/gsd-transition`):
1. Requirements invalidated? → Move to Out of Scope with reason
2. Requirements validated? → Move to Validated with phase reference
3. New requirements emerged? → Add to Active
4. Decisions to log? → Add to Key Decisions
5. "What This Is" still accurate? → Update if drifted

**After each milestone** (via `/gsd-complete-milestone`):
1. Full review of all sections
2. Core Value check — still the right priority?
3. Audit Out of Scope — reasons still valid?
4. Update Context with current state

---

*Last updated: 2026-07-17 — after v2.1 milestone started*
