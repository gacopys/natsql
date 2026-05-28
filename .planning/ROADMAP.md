# Roadmap: natsql

## Overview

natsql is a NATS-native materialized view engine. We build it in three phases, from the ground up:

1. **Prove the write path** — consume JetStream events and materialize row state into NATS KV
2. **Prove the read path** — query materialized state via SQL with PK lookups over NATS and HTTP
3. **Package and harden** — ship as a Go library and standalone binary with graceful lifecycle management

Each phase delivers a complete, testable capability. No phase depends on unbuilt infrastructure.

## Phases

- [x] **Phase 1: Foundation — Materializer** — Config-driven JetStream consumer materializes events into KV row state
- [ ] **Phase 2: SQL Query Engine + Interfaces** — PK-lookup SQL query engine with NATS request-reply and HTTP endpoints
- [ ] **Phase 3: Packaging + Deployment** — Go library API, standalone binary, embedded/external NATS, graceful shutdown

## Phase Details

### Phase 1: Foundation — Materializer

**Goal**: Events flow from a JetStream stream into a KV bucket, maintaining current row state that survives restarts.

**Depends on**: Nothing (first phase)

**Requirements**: MAT-01, MAT-02, MAT-03, MAT-04

**Success Criteria** (what must be TRUE):
  1. User can define a materialized view in a YAML config file (source stream subject, key column, column mapping)
  2. Materializer consumes JetStream events with a durable pull consumer that resumes from last ack after process restart
  3. KV bucket contains the current row state after processing events — PK maps to JSON value with schema metadata
  4. Malformed events (e.g., invalid JSON, missing key field, type mismatch) are logged and skipped without crashing the materializer
  5. Materializer processes events in order and acks only after successful KV writes (at-least-once delivery)

**Plans**: 3 plans in 3 waves

Plans:
- [x] 01-01-PLAN.md — Config & KV Infrastructure (Wave 1)
- [x] 01-02-PLAN.md — Materializer Components (Wave 2)
- [x] 01-03-PLAN.md — Engine + Main + Integration (Wave 3)

### Phase 2: SQL Query Engine + Interfaces

**Goal**: Users can query materialized views by primary key via NATS request-reply and HTTP, receiving typed JSON results.

**Depends on**: Phase 1

**Requirements**: QRY-01, QRY-02, QRY-03, IFC-01, IFC-02

**Success Criteria** (what must be TRUE):
   1. User can query a single row by primary key: `SELECT * FROM view WHERE <pk_col> = <val>` returns the correct row as typed JSON
   2. Query engine validates column names against the view schema — invalid columns return a clear error, not a panic
   3. Query results are returned as typed JSON (strings quoted, numbers unquoted, booleans literal, null for missing values)
   4. User can query via NATS request-reply — `nc.Request("natsql.query", sqlBytes)` returns JSON response with results or error
   5. User can query via HTTP — `POST /api/v1/query` with JSON body `{"sql": "..."}` returns JSON response

**Plans**: 2 plans in 2 waves

Plans:
- [ ] 02-01-PLAN.md — Query Engine Core (natsql/query/ package with parser, planner, executor) (Wave 1)
- [ ] 02-02-PLAN.md — Engine Integration + Transport (Engine.Query(), NATS + HTTP handlers) (Wave 2)

### Phase 3: Packaging + Deployment

**Goal**: Users can deploy natsql as a Go library within their own process or as a standalone binary with embedded or external NATS, with clean shutdown lifecycle.

**Depends on**: Phase 2

**Requirements**: IFC-03, DEP-01, DEP-02, DEP-03, DEP-04

**Success Criteria** (what must be TRUE):
  1. User can import natsql as a Go library — `engine := natsql.New(js, config)` and `engine.Query(ctx, "SELECT ...")` — and use it within their own Go process
  2. User can run `natsql server` as a standalone binary with embedded NATS (single-node, dev mode) — zero external infrastructure
  3. User can run `natsql server` configured to connect to an existing external NATS cluster (production mode)
  4. Graceful shutdown sequence works — `SIGINT`/`SIGTERM` drains the materializer consumer, flushes pending KV writes, stops the HTTP server, and exits cleanly
  5. No leaked goroutines, orphaned consumers, or unclosed NATS connections after shutdown (verifiable via goroutine profile and NATS server `nsc list consumers`)

**Plans**: TBD

## Progress

| Phase | Plans Complete | Status | Completed |
|-------|----------------|--------|-----------|
| 1. Foundation — Materializer | 3/3 | Complete | 2026-05-28 |
| 2. SQL Query Engine + Interfaces | 0/0 | Not started | - |
| 3. Packaging + Deployment | 0/0 | Not started | - |
