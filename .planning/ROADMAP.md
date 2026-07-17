# Roadmap: natsql

## v2.1 — Code Stabilization

**Granularity:** Coarse (compressed from 9 categories into 5 phases)

### Phase Structure

Phase numbering continues from v1.2 (Phase 11). v2.1 starts at Phase 12.

| Phase | Name | Goal | Reqs |
|-------|------|------|------|
| 12 | Lifecycle & Core Correctness | Engine lifecycle is hardened — safe start/stop, panic recovery, canonical PK encoding | 7 |
| 13 | SQL & Config Hardening | Parse-time rejection of unsupported SQL constructs and config validation errors | 5 |
| 14 | Transport Safety & Robustness | Error envelopes, error classification, DLQ/consumer hardening, query optimisations | 7 |
| 15 | Documentation & Contract Alignment | Project documentation matches actual behaviour | 4 |
| 16 | API Gaps & Cleanup | Missing API surface added, dead code removed, code organisation improved | 9 |

**Total: 5 phases | 32 requirements | 100% coverage ✓**

---

## Previous Milestones

See `MILESTONES.md` for v1.0 → v2.0 history.

---

## Phases

- [ ] **Phase 12: Lifecycle & Core Correctness** — Engine lifecycle hardened: safe startup cleanup, Query-after-Close guard, panic safety, CLI Close on failure, mutex fix, canonical PK encoding, safe key_separator
- [ ] **Phase 13: SQL & Config Hardening** — Reject aliases, mixed `*`, NULL literals; contradictory-WHERE EmptyPlan; duplicate key_fields validation
- [ ] **Phase 14: Transport Safety & Robustness** — Typed error envelopes, typed sentinel error classification, all-mapper-errors to DLQ, DLQ stream caps, consumer bounds, RFC3339Nano timestamps, FullScanPlan pre-normalisation
- [ ] **Phase 15: Documentation & Contract Alignment** — WithHTTPServer host honouring, StoreSchema deduplication, consistency doc fix, PKLookupPlan Limit doc
- [ ] **Phase 16: API Gaps & Cleanup** — HTTP disable option, non-owning NATS conn, HTTPAddr accessor, dead code removal, default centralisation, logger wiring, test helper base

---

## Phase Details

### Phase 12: Lifecycle & Core Correctness
**Goal**: Engine lifecycle is hardened — safe start/stop/close, panic recovery in materializers, and canonical PK encoding that prevents composite-key collisions

**Depends on**: Nothing (foundation fixes to existing code)

**Requirements**: LIFE-01, LIFE-02, LIFE-03, LIFE-04, LIFE-05, PK-01, PK-02

**Success Criteria** (what must be TRUE):
1. When `Engine.Start` fails partway through initialisation, all started resources (goroutines, drain channels, NATS subscriptions, HTTP server) are properly cleaned up — no goroutine or resource leaks
2. Calling `Engine.Query` after `Engine.Close()` returns an explicit "engine closed" error — never a panic, hang, or stale result
3. If a materializer panics during event processing, the panic is recovered, the in-flight message is NAK'd (not lost), and the error is logged — no silent goroutine death
4. All CLI code paths that create an engine and encounter an error call `eng.Close()` before returning (no engine handle leaks)
5. `Engine.Query`'s lazy KV bucket initialisation does not hold the engine mutex across the KV network RPC — no lock contention on read path
6. A single canonical `FormatPKPart` function is used by both the writer and planner; numeric PK values in non-canonical JSON forms (`100.0`, `1e10`) are reachable via PK lookup
7. `key_separator` is restricted to `/` and `_` (the characters already escaped by `SanitizePK`), preventing composite-key collisions from arbitrary separators

**Plans**: TBD

---

### Phase 13: SQL & Config Hardening
**Goal**: Unsupported SQL constructs are rejected at parse time (not silently ignored), and config validation catches duplicate `key_fields`

**Depends on**: Phase 12 (engine lifecycle must be stable to test SQL changes)

**Requirements**: SQL-01, SQL-02, SQL-03, SQL-04, CFG-01

**Success Criteria** (what must be TRUE):
1. Column aliases (`SELECT id AS x`) are rejected with an explicit parse error before query execution — no silent dropping of the alias
2. Mixed `*` with explicit columns (`SELECT *, id`) is rejected with an explicit parse error before execution
3. `NULL` literals in WHERE clauses are rejected with an explicit parse error before execution — not silently treated as a column reference or comparison
4. Contradictory WHERE predicates on any column (e.g. `a = 1 AND a = 2`) produce an `EmptyPlan` (zero I/O) — not just on PK columns
5. Config validation rejects views with duplicate entries in `key_fields` — caught at config load time

**Plans**: TBD

---

### Phase 14: Transport Safety & Robustness
**Goal**: Error envelopes use typed structs, error classification uses typed sentinels, DLQ/consumer config is bounded, and query execution pre-normalises conditions

**Depends on**: Phase 12 (engine must be stable for transport testing)

**Requirements**: TSP-01, TSP-02, TSP-03, ROB-01, ROB-02, ROB-03, ROB-04

**Success Criteria** (what must be TRUE):
1. NATS and HTTP error response envelopes use `json.Marshal` of a real Go struct — no hand-rolled JSON string concatenation
2. Write error classification checks typed NATS/jetstream error sentinels first; the substring-based fallback path logs a `Warn` level message when triggered
3. All mapper errors (not just `ErrMalformedEvent`) route the failed event to the DLQ stream and Ack it — no silent event loss from unexpected mapper errors
4. DLQ stream configuration includes `MaxMsgs` and `MaxBytes` caps — prevents unbounded DLQ growth
5. Consumer configuration fields (`AckWaitSeconds`, `MaxAckPending`, `MaxDeliver`) are validated against negative values at config time
6. DLQ envelope timestamps use `RFC3339Nano` format for sub-second timestamp precision
7. `FullScanPlan` normalises condition values once at plan creation time, not once per row during scan execution

**Plans**: TBD

---

### Phase 15: Documentation & Contract Alignment
**Goal**: Project documentation (doc comments, ARCHITECTURE.md, PROJECT.md) accurately reflects actual implementation behaviour

**Depends on**: Phase 12, Phase 13 (must fix the code before documenting fixed behaviour)

**Requirements**: DOC-01, DOC-02, DOC-03, DOC-04

**Success Criteria** (what must be TRUE):
1. `WithHTTPServer` actually uses the host component of the address argument for binding — `0.0.0.0:8080` binds all interfaces, default `127.0.0.1` is preserved (T-02-06 compatibility)
2. `StoreSchema` is called only once by the engine, not duplicated in `materialize.Run` — engine is the single owner of schema persistence
3. All documentation (ARCHITECTURE.md, PROJECT.md, doc comments) describes the consistency model as "idempotent upsert within sequential consumer" instead of "CAS-based"
4. `PKLookupPlan` has explicit doc comment documenting that it returns ≤1 row per PK equality

**Plans**: TBD

---

### Phase 16: API Gaps & Cleanup
**Goal**: Missing API surface added (HTTP disable, non-owning NATS conn, HTTPAddr accessor), dead code and duplication removed, code organisation improved

**Depends on**: Phase 12, Phase 14 (engine must be stable before adding API surface; transport must be solid before HTTP disable option)

**Requirements**: APIGAP-01, APIGAP-02, APIGAP-03, CLN-01, CLN-02, CLN-03, CLN-04, CLN-05, CLN-06

**Success Criteria** (what must be TRUE):
1. User can disable the HTTP transport via `WithHTTPDisabled()` option or config flag — no HTTP server started when disabled
2. Library users can register a NATS query subscription without transferring NATS connection ownership — `WithNATSQueryConn` allows sharing a connection without taking ownership
3. The bound HTTP port is discoverable via an `HTTPAddr()` accessor method or exposed in `Stats`
4. Dead code removed: deprecated `PKKey`, dead `float64` branches in `validateType`, redundant port defaults in `engine.New`, `EmptyPlan.Columns` dead field — all removed cleanly
5. Default PK separator logic exists only in `BuildSchema` — not duplicated in `materialize.Run` or `planner.BuildPlan`
6. CLI passes a logger to engine construction; the per-query log line uses `Debug` level, not `Info`
7. Redundant `SetDefaults` call removed from CLI; redundant `Content-Type` header sets consolidated; `oapi.QueryResult` vs `query.QueryResult` rationalised
8. `embed.StartNode` and `testutil.StartEmbeddedNATS` share a common base helper — test infrastructure is not duplicated

**Plans**: TBD

---

## Progress

| Phase | Plans Complete | Status | Completed |
|-------|----------------|--------|-----------|
| 12. Lifecycle & Core Correctness | 0/0 | Not started | - |
| 13. SQL & Config Hardening | 0/0 | Not started | - |
| 14. Transport Safety & Robustness | 0/0 | Not started | - |
| 15. Documentation & Contract Alignment | 0/0 | Not started | - |
| 16. API Gaps & Cleanup | 0/0 | Not started | - |

---

## Appendix: Requirement Coverage Map

| Req | Phase | Description |
|-----|-------|-------------|
| LIFE-01 | 12 | Engine.Start cleanup on partial failure |
| LIFE-02 | 12 | Engine.Query rejected after Close |
| LIFE-03 | 12 | Materializer panic safety |
| LIFE-04 | 12 | CLI Close on all failure paths |
| LIFE-05 | 12 | Query lazy-init no mutex across RPC |
| PK-01 | 12 | Canonical FormatPKPart |
| PK-02 | 12 | key_separator restricted to `/` `_` |
| SQL-01 | 13 | Column alias rejection |
| SQL-02 | 13 | Mixed * rejection |
| SQL-03 | 13 | NULL literal rejection |
| SQL-04 | 13 | Contradictory WHERE → EmptyPlan |
| CFG-01 | 13 | Duplicate key_fields rejection |
| TSP-01 | 14 | Typed error envelopes |
| TSP-02 | 14 | Typed sentinel error classification |
| TSP-03 | 14 | All mapper errors to DLQ |
| ROB-01 | 14 | DLQ stream MaxMsgs/MaxBytes caps |
| ROB-02 | 14 | Consumer config validated against negatives |
| ROB-03 | 14 | RFC3339Nano DLQ timestamps |
| ROB-04 | 14 | FullScanPlan pre-normalisation |
| DOC-01 | 15 | WithHTTPServer host honor |
| DOC-02 | 15 | Deduplicate StoreSchema |
| DOC-03 | 15 | Consistency doc fix |
| DOC-04 | 15 | PKLookupPlan Limit doc |
| APIGAP-01 | 16 | HTTP disable option |
| APIGAP-02 | 16 | Non-owning NATS conn |
| APIGAP-03 | 16 | HTTPAddr accessor |
| CLN-01 | 16 | Dead code removal |
| CLN-02 | 16 | Centralised default PK separator |
| CLN-03 | 16 | CLI logger wiring |
| CLN-04 | 16 | Redundant code removed |
| CLN-05 | 16 | EmptyPlan.Columns dead field |
| CLN-06 | 16 | Shared base test helper |

---

*Roadmap defined: 2026-07-17*
