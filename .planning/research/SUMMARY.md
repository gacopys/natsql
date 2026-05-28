# Project Research Summary

**Project:** natsql — NATS-native materialized view engine
**Domain:** Stream-to-KV materialized view engine with read-only SQL query layer
**Researched:** 2026-05-23
**Confidence:** HIGH

## Executive Summary

natsql is a read-only SQL query engine over NATS JetStream KV-backed materialized views. It consumes JetStream events, maintains KV bucket snapshots with secondary indexes, and serves `SELECT ... WHERE ... LIMIT` queries via NATS request-reply, HTTP, or in-process Go calls. The product fills a clear gap: NATS users who need simple queryable state without provisioning Postgres, Redis, or Kafka alongside their NATS cluster.

The recommended architecture cleanly separates three concerns — **Materializer** (stream→KV), **Query Engine** (SQL→KV reads), and **Transport** (NATS/HTTP/Embed) — into components that share only the KV bucket name. This follows proven patterns from comparable systems (ksqlDB, Materialize, Kafka Streams Interactive Queries) but intentionally scopes down: no JOINs, no aggregations, no push queries, no pgwire for v1. The bet is that `SELECT + WHERE (eq + range) + LIMIT` on indexed KV buckets covers 90% of what developers need from a state query layer, and that the zero-infrastructure-selling point against Kafka-dependent alternatives is the real differentiator.

**Key risks to mitigate:**
1. **NATS KV `Keys()` is O(n) with no server-side filtering** — every full table scan loads ALL keys. Solution: enforce index-only query paths; error on unindexed queries rather than falling back to full scan.
2. **CAS race between PK write and index update** — crash mid-write leaves index pointing to stale data. Solution: write-then-ack ordering; accept read-committed consistency for v1; add consistency sweep in v2.
3. **Write amplification from secondary indexes** — each index doubles KV writes (×Raft replicas). Solution: limit indexes per view (max 5); benchmark the curve; document the cost.
4. **Consumer lifecycle management** — zombie consumers leak goroutines and NATS server resources. Solution: `InactiveThreshold`, `defer cc.Stop(); <-cc.Closed()` pattern.

## Key Findings

### Recommended Stack

The stack is minimal by design — five direct dependencies, no database drivers, no ORMs:

| Technology | Purpose | Rationale |
|------------|---------|-----------|
| **Go 1.22+** | Language | Already in monorepo |
| **vitess.io/vitess/go/vt/sqlparser** | SQL parsing (SELECT-only AST) | Most battle-tested Go SQL parser; clean AST for SELECT with WHERE/LIMIT extraction |
| **github.com/nats-io/nats.go/jetstream** | JetStream KV + Stream API | Official simplified client; replaces legacy `nats` package |
| **github.com/nats-io/nats-server/v2** | Embedded NATS | Single-binary deployment; integration test harness |
| **github.com/go-chi/chi/v5** | HTTP router | Lightweight (~1000 LOC), stdlib-compatible, built-in middleware |
| **github.com/spf13/cobra** | CLI framework | Already in monorepo; NATS ecosystem standard |
| **gopkg.in/yaml.v3** | Config parsing | Standard Go YAML; pairs with `encoding/json` for dual-format support |

**Alternative considered:** Hand-written SQL parser for v1 (recommended by ARCHITECTURE.md to eliminate dependency). Both viabble — defer to Phase 2 planning. vitess is the safer choice if we expect SQL dialect to grow; hand-written is simpler if we truly lock to `SELECT ... WHERE col <op> val LIMIT N`.

**SQL-on-KV execution pattern** (well-established, proven by Badger, Tigris, rqlite):
```
SQL text → [vitess parser] → AST → [planner] → Plan → [executor] → JSON rows
```
Plan types: PKLookupPlan (1 Get), IndexScanPlan (Watch prefix + Gets), RangeScanPlan (Watch prefix + client filter), FullScanPlan (Keys + client filter — error in v1).

### Expected Features

**Must have (table stakes) — from FEATURES.md ecosystem analysis of ksqlDB, rqlite, Materialize, Kafka Streams, Dgraph:**
- Declarative view definition (YAML/JSON config) — DECL-01
- Ordered, durable JetStream consumption with crash recovery — STREAM-01/02
- Primary-key point lookup (`WHERE pk = val`) — QUERY-01
- Range scan on indexed columns (`WHERE col > val / col < val`) — QUERY-02
- `LIMIT` support — QUERY-03
- NATS request-reply query interface — QUERY-04
- HTTP/JSON query interface — QUERY-05
- Secondary indexes (single-column equality + range) — INDEX-01
- Malformed event handling (log + skip, don't crash) — RESIL-01

**Should have (differentiators — competitive advantage):**
- **Zero-infrastructure Kafka alternative** — natsql vs ksqlDB/Materialize: no Kafka, no Schema Registry, no K8s
- **Secondary indexes on KV state** — ksqlDB and Kafka Streams lack this; Materialize has it but with complexity
- **Go embeddable (library mode)** — import as Go library, no sidecar process needed — EMBED-01
- **NATS-native request-reply** — query via `nc.Request("natsql.query", sqlBytes)` — fits NATS ecosystem
- **Declarative YAML/JSON config** — no DDL parser for v1; define views in a config file
- **Embedded NATS support** — works with `nats-server` in-process — EMBED-03

**Defer (v2+):**
- Full pgwire / PostgreSQL protocol (massive surface area; v1 should prove engine first)
- JOINs across views (query planning complexity / multi-bucket coordination)
- Aggregations (COUNT, SUM, GROUP BY — needs state tracking across events)
- Push queries / subscriptions (server-side complexity for maintaining query results over time)
- Schema inference (event format changes break queries; explicit schema is better)
- Multi-key transactions (NATS KV CAS doesn't support them)
- Own query language (Dgraph got this wrong — hurts adoption; stick to SQL subset)

### Architecture Approach

Three-component model with clean separation of concerns, all sharing a single NATS KV bucket (`natsql-views`):

```
Config (YAML/JSON)
    │ defines
    ▼
┌──────────────────────────────────────────────┐
│            Materializer (per-view)            │
│  Durable Pull Consumer → Row Mapper → KV     │
│  Writer (PK + index entries)                 │
│  + Rebuild Controller (purge → replay)        │
└──────────────────┬───────────────────────────┘
                   │ writes to
                   ▼
┌──────────────────────────────────────────────┐
│          NATS KV Bucket (natsql-views)        │
│  {view}/pk/{pk}        → row JSON            │
│  {view}/idx/{col}/{v}/{pk} → "" (index entry) │
│  {view}/meta/schema    → schema def           │
└──────────────────┬───────────────────────────┘
                   │ reads from
                   ▼
┌──────────────────────────────────────────────┐
│           Query Engine (stateless)            │
│  SQL Parser → Validator → Plan Builder →     │
│  KV Executor                                  │
│  Results via: NATS req-reply / HTTP / Go call │
└──────────────────────────────────────────────┘
```

**Major components:**
1. **Materializer** — owns one durable pull consumer per view; converts events to row mutations; writes PK rows + index entries to KV; acks only after successful KV writes. Crash recovery = consumer resumes from last ack.
2. **KV Bucket** — single `natsql-views` bucket; `{view}/pk/{pk}` → row JSON; `{view}/idx/{col}/{val}/{pk}` → "" (presence marker); meta keys for schema + consumer progress.
3. **Query Engine** — stateless per-request pipeline: parse SQL → validate against schema → build plan (PK lookup > index lookup > range scan > full scan) → execute against KV → return structured result. Thread-safe, context-aware.

**Key architectural properties:**
- State ownership: KV is authoritative snapshot; stream is authoritative changelog
- Materializer and Query Engine share only the KV bucket name (no direct coupling)
- Single-threaded event loop per view (simplifies consistency; no CAS conflicts within one materializer)
- NATS handles replication (JetStream + KV replicas) — zero external infrastructure
- Query engine is stateless and horizontally scalable

**KV key encoding for range-sortable numeric indexes:**
- Integers → zero-padded hex (e.g., `000000000000002a` for 42)
- This enables lexicographic ordering for prefix scans + client-side range filtering

### Critical Pitfalls

1. **Full bucket scans kill performance.** NATS KV `Keys()` loads ALL keys into memory with no server-side filtering. At 100K+ keys this OOMs the client. **Prevention:** Error on unindexed queries instead of falling back to full scan. Always query through indexes + PK lookups. Use `ListKeys()` (streaming) over `Keys()` (in-memory slice) when scanning is unavoidable.

2. **CAS race between PK write and index update.** Materializer crash between `Put(PK)` and `Put(idx/...)` leaves index pointing to non-existent or stale data. **Prevention:** Accept read-committed consistency for v1; index is eventually consistent. Idempotent event replay (check `event_sequence > stored_sequence`). Consistency sweep as future improvement.

3. **Write amplification from secondary indexes.** Each index = N+1 KV writes (1 data + N indexes). With R3 replication, that's 3×(N+1) Raft writes across the NATS cluster. **Prevention:** Limit indexes to 5 per view. Benchmark the throughput curve. Document "each index doubles write cost" for users. Consider batch flushing in future.

4. **Consumer lifecycle — goroutine leaks and zombie consumers.** Never calling `Stop()` on `ConsumeContext` leaks goroutines; durable consumers without `InactiveThreshold` accumulate on the server. **Prevention:** Always `defer cc.Stop(); <-cc.Closed()`. Set `InactiveThreshold: 1h`. Use `CreateOrUpdateConsumer` (NATS 2.11+). Follow established shutdown patterns.

5. **SQL NULL handling and type coercion.** `NULL = NULL` is false in standard SQL but naive implementations get this wrong. Reserved words as column names break parsing. **Prevention:** Define explicit type system from day 1 (string, int64, float64, bool, timestamp). No type inference from events. Document NULL-handling semantics. Support quoted identifiers. Build a SQL compliance test suite covering NULL, type coercion, reserved words, and edge cases.

## Implications for Roadmap

Based on combined research, I recommend **5 phases** with strict dependency ordering:

### Phase 1: Foundation — Config Parsing + KV Layout + Consumer Setup
**Rationale:** Every downstream component depends on having materialized state in KV. This phase proves the write path (stream→KV) end-to-end with no query engine yet.
**Delivers:**
- YAML/JSON config loader (`Config`, `ViewConfig`, `ColumnConfig` structs)
- KV helper package (key encoding for PKs + index entries, bucket initialization)
- Durable pull consumer setup (create/resume, `AckExplicit`, `DeliverAll`/`DeliverNew`)
- Event→row mapper (config-driven JSON field extraction, column type validation)
- KV Writer (PK upsert with `_meta` injection; index entry creation)
- Manual integration test: publish event → verify KV state
**Addresses FEATURES:** Declarative view config, ordered stream consumption, KV materialization, crash recovery
**Avoids PITFALLS:** Pitfall 6 (key encoding design), Pitfall 7 (history=1 config), Pitfall 19 (key length limits)
**Research flag:** Low complexity — well-documented NATS KV patterns. Skip /gsd-research-phase.

### Phase 2: Minimal SQL — PK Lookup + NATS Request-Reply
**Rationale:** Depends on Phase 1 state being available in KV. This is the first user-visible query capability — prove `SELECT ... WHERE pk = val` works before adding index complexity.
**Delivers:**
- SQL parser (hand-written or vitess — decide in planning) for: `SELECT cols FROM view WHERE pk = val LIMIT N`
- Validator (view exists, columns exist, type match)
- Plan builder (PK point lookup plan)
- Executor (single `kv.Get` + column projection)
- NATS request-reply handler (`natsql.query` subject)
**Addresses QUERY-01** (PK equality), **QUERY-03** (LIMIT), **QUERY-04** (NATS request-reply)
**Avoids PITFALLS:** Pitfall 5 (SQL edge cases — start with limited grammar), Pitfall 10 (no string interpolation — parameterized queries)
**Research flag:** Decide hand-written vs vitess parser. If hand-written, ~200 LOC using `text/scanner`. If vitess, add one dependency. Both are low-risk.

### Phase 3: Secondary Indexes + Full WHERE + HTTP API
**Rationale:** Depends on Phase 2 query engine skeleton. This is the v1 differentiator — secondary indexes on KV state stores (what ksqlDB/Kafka Streams lack).
**Delivers:**
- Index entry creation in materializer (on config change → re-materialize)
- IndexEqLookup plan (`WHERE indexed_col = val`)
- IndexRangeScan plan (`WHERE indexed_col > val` / `< val`)
- FullScan fallback plan (return error by default; opt-in with config flag)
- Multiple WHERE conditions (AND-connected)
- HTTP/JSON query API (POST /api/v1/query)
- Batch KV fetch optimization (parallel `kv.Get` with worker pool)
- Limit pushdown (stop fetching once LIMIT reached)
**Addresses INDEX-01** (secondary indexes), **QUERY-02** (range scan), **QUERY-05** (HTTP API)
**Avoids PITFALLS:** Pitfall 1 (no unindexed full scans — error), Pitfall 2 (CAS race — accept eventual consistency), Pitfall 11 (limit indexes per view), Pitfall 8 (use `Get`, not `Watch`)
**Research flag:** Medium — index write amplification needs benchmarking. No /gsd-research-phase needed, but include a spike in planning to measure throughput at 0/1/3/5 indexes.

### Phase 4: Embedding + Standalone Server
**Rationale:** The engine is feature-complete for v1. Now package it for both consumption modes: Go library and standalone binary.
**Delivers:**
- Public `Engine` API: `New(js, config)`, `Execute(ctx, query)`, `Start(ctx)`, `Close()`
- Options pattern (replicas, KV bucket name, logger)
- `cmd/natsql/` standalone binary with `serve`, `validate`, `view list`, `view status` subcommands
- Embedded NATS support (EMBED-03) — start `nats-server` in-process
- Graceful shutdown with two-phase drain (stop accepting → drain in-flight → exit)
- Cobra CLI with root command + subcommands
**Addresses EMBED-01** (Go library), **EMBED-02** (standalone server), **EMBED-03** (embedded NATS)
**Avoids PITFALLS:** Pitfall 4 (consumer lifecycle — proper shutdown), Pitfall 15 (ack race), Pitfall 17 (auth — bind HTTP to localhost by default; document access control for embed mode)
**Research flag:** Low — standard Go library packaging. Skip /gsd-research-phase.

### Phase 5: Operational Hardening
**Rationale:** After proving the engine works end-to-end, harden for production use. Can partially overlap with Phase 4.
**Delivers:**
- Full rebuild (purge + replay) — `natsqlctl rebuild --view myview`
- Malformed event handling (log + skip, metrics counter)
- Structured logging (`slog.Logger` with configurable level)
- Prometheus metrics (events consumed, KV writes, query latency, index count, bucket size)
- Integration test suite (end-to-end: publish→materialize→query→verify)
- Performance benchmarks (latency by plan type, throughput by index count, warmup time)
- Documentation: examples, config reference, FAQ
**Addresses RESIL-01** (malformed events), operational readiness for all requirements
**Avoids PITFALLS:** Pitfall 14 (warmup time — measure and document), Pitfall 9 (schema evolution — rebuild command), Pitfall 7 (delete marker pollution — PurgeDeletes sweep)
**Research flag:** Low — standard observability and testing practices. Skip /gsd-research-phase.

### Phase Ordering Rationale

- **Strict dependencies:** Phase 1 → 2 → 3 is a hard chain (no query engine without materialized state; no indexes without query engine skeleton)
- **Parallelism opportunity:** Phases 4 and 5 can partially overlap with Phase 3 (CLI skeleton and logging are independent of index logic)
- **Risk ordering:** Prove the write path first (Phase 1), then the read path (Phase 2), then the differentiator (Phase 3), then packaging (Phase 4), then hardening (Phase 5)
- **Pitfall avoidance:** Index design (Pitfall 1, 2, 11) is baked into Phase 3 from day one — adding indexes after the fact requires re-materialization

### Feature-to-Phase Mapping

| Requirement | Phase | Notes |
|-------------|-------|-------|
| DECL-01: View config | 1 | YAML/JSON → Config struct |
| STREAM-01: Stream consumption | 1 | Durable pull consumer |
| STREAM-02: Crash recovery | 1 | Consumer resumes from last ack |
| QUERY-01: PK equality | 2 | `SELECT ... WHERE pk = val` |
| QUERY-02: Range scan | 3 | Requires indexes |
| QUERY-03: LIMIT | 2 | Trivial on sorted KV iteration |
| QUERY-04: NATS req-reply | 2 | `nc.Request("natsql.query", sqlBytes)` |
| QUERY-05: HTTP API | 3 | POST /api/v1/query |
| INDEX-01: Secondary indexes | 3 | Equality + range on indexed columns |
| EMBED-01: Go library | 4 | `natsql.New(js, config).Execute(ctx, query)` |
| EMBED-02: Standalone binary | 4 | `cmd/natsql/` with cobra |
| EMBED-03: Embedded NATS | 4 | In-process `nats-server` |
| RESIL-01: Malformed events | 5 | Log + skip + metrics |

## Confidence Assessment

| Area | Confidence | Notes |
|------|------------|-------|
| Stack | **HIGH** | All technologies verified against official docs and GitHub. PATENTED patterns (parse→plan→execute) proven by Badger/Tigris. |
| Features | **MEDIUM-HIGH** | Ecosystem research covered 5 comparable systems at depth. Feature claims verified against official docs (but Dgraph docs partially unavailable). ksqlDB/Materialize/Kafka Streams patterns directly inform natsql's design. |
| Architecture | **HIGH** | 3-component model confirmed against NATS JetStream docs and go-mysql-server query pipeline. |
| Pitfalls | **HIGH** | Verified against NATS docs, nats.go issue tracker (KV bugs, ListKeysFiltered issues), and NATS community experience. |

**Overall confidence:** HIGH — all four research dimensions consistently point to the same technology choices, architecture, and phase ordering. The few tensions (hand-written vs vitess parser; single vs per-view KV buckets) are low-risk decisions that can be resolved during Phase 1/2 planning.

### Gaps to Address

| Gap | Impact | Resolution |
|-----|--------|------------|
| **SQL parser choice** — hand-written vs vitess | Medium — affects Phase 2 dependency weight | Decide in Phase 2 planning. Hand-written is simpler for v1 subset; vitess is future-proof. Both are viable. |
| **Index write amplification curve** — no benchmarks yet | Medium — affects Phase 3 index limit decisions | Include benchmark spike in Phase 3 planning. Measure throughput at 0/1/3/5 indexes with R1 and R3 replication. |
| **Single vs per-view KV buckets** — one `natsql-views` bucket vs per-view buckets | Low — affects Phase 1 bucket config | Start with single bucket (simpler). Can migrate to per-view buckets later if `Keys()` isolation or independent TTL/replication settings become necessary. |
| **KV `MaxValueSize`** — 64KB default may be limiting for large rows | Low — affects Phase 1 config | Document in FAQ. Increase `MaxValueSize` if rows exceed 64KB. For >8MB values, recommend external storage with checksum reference. |
| **Late-arriving events** — event-time ordering not enforced in v1 | Medium — affects Phase 1 event processing design | Document stream-order processing limitation. Add event-time check (skip events older than current stored value) in Phase 5 hardening. Add metric for late events. |

## Sources

### Primary (HIGH confidence)
- [NATS JetStream KV Store Docs](https://docs.nats.io/nats-concepts/jetstream/key-value-store) — KV capabilities, limits, key format
- [NATS Consumer Configuration](https://docs.nats.io/nats-concepts/jetstream/consumers) — durable consumer lifecycle, ack policies
- [nats.go jetstream](https://pkg.go.dev/github.com/nats-io/nats.go/jetstream) — KV operations, consumer API, Watch patterns
- [vitess sqlparser](https://github.com/vitessio/vitess/tree/main/go/vt/sqlparser) — SQL SELECT AST parsing

- [chi v5](https://github.com/go-chi/chi) — HTTP router, v5.3.0 released May 2026
- [ksqlDB pull queries](https://docs.ksqldb.io/en/latest/developer-guide/ksqldb-reference/select-pull-query/) — feature reference for comparable system
- [rqlite API](https://rqlite.io/docs/api/api/) — HTTP query API pattern reference
- [Materialize docs](https://materialize.com/docs/sql/create-materialized-view/) — materialized view SQL reference
- [Kafka Streams Interactive Queries](https://docs.confluent.io/platform/current/streams/developer-guide/interactive-queries.html) — KV state store query patterns

### Secondary (MEDIUM confidence)
- [Dgraph documentation](https://dgraph.io/docs/) — indexing approach (partially available; pattern reference only)
- [xwb1989/sqlparser](https://github.com/xwb1989/sqlparser) — archived fork of old vitess; fallback parser option
- [pingcap/tidb/parser](https://github.com/pingcap/tidb/tree/master/pkg/parser) — viable alternative to vitess if dependency weight is a concern
- [rqlite architecture](https://github.com/rqlite/rqlite) — reference for query patterns on replicated state

---

*Research completed: 2026-05-23*
*Ready for roadmap: yes*
