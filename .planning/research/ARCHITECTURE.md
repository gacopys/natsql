# Architecture: Stream-to-KV Materialized View Engine on NATS

**Project:** natsql  
**Researched:** 2026-05-23  
**Confidence:** HIGH (confirmed against NATS JetStream docs, ebind reference code, and comparable SQL-over-KV engines)

## Executive Summary

natsql is a materialized view engine that consumes JetStream events, maintains NATS KV bucket snapshots, and serves read-only SQL queries. The architecture separates three concerns — **materialization** (stream→KV), **query** (SQL→KV reads), and **transport** (NATS request-reply / HTTP / Go embed) — into independent components that share a common KV store. The design follows patterns proven in the ebind codebase (CAS-based state management, durable consumer recovery, KV key hierarchy) and adapts the parse→validate→plan→execute pipeline from go-mysql-server to the KV-backed execution model.

---

## 1. High-Level Component Diagram

```
                          ┌─────────────────────┐
                          │  Config (YAML/JSON)  │
                          │  source stream,      │
                          │  columns, PK, indexes│
                          └──────────┬───────────┘
                                     │ defines
                                     ▼
┌────────────────────────────────────────────────────────────────┐
│                     Materializer (per-view)                     │
│                                                                │
│  ┌──────────────┐  ┌─────────────┐  ┌──────────────────────┐  │
│  │ Durable Pull │──▶ Event→Row   │──▶ KV Writer            │  │
│  │ Consumer     │  │ Mapper      │  │ (PK + index entries) │  │
│  └──────────────┘  └─────────────┘  └──────────────────────┘  │
│                                                                │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │ Rebuild Controller: Purge bucket → DeliverAll → replay  │   │
│  └─────────────────────────────────────────────────────────┘   │
└─────────────────────────┬──────────────────────────────────────┘
                          │ writes to
                          ▼
┌─────────────────────────────────────────────────────────────────┐
│                    NATS KV Bucket (natsql-views)                 │
│                                                                 │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │  {view}/pk/{encoded_pk}     →  full row JSON            │   │
│  │  {view}/idx/{col}/{val}/{pk}→  "" (presence marker)     │   │
│  │  {view}/meta/schema         →  schema definition         │   │
│  │  {view}/meta/consumer       →  stream seq, state         │   │
│  └──────────────────────────────────────────────────────────┘   │
└─────────────────────────┬───────────────────────────────────────┘
                          │ reads from
                          ▼
┌──────────────────────────────────────────────────────────────────┐
│                      Query Engine (stateless)                    │
│                                                                  │
│  ┌──────────┐  ┌──────────┐  ┌───────────┐  ┌──────────────┐   │
│  │ SQL      │─▶│ Validate │─▶│ Plan      │─▶│ KV Executor  │   │
│  │ Parser   │  │          │  │ Builder   │  │              │   │
│  └──────────┘  └──────────┘  └───────────┘  └──────────────┘   │
│                                                                  │
│  Results returned through any of:                                │
│  ┌─────────────────┐  ┌─────────────────┐  ┌────────────────┐   │
│  │ NATS Req-Reply  │  │ HTTP/JSON API   │  │ In-process Go  │   │
│  │ (nc.Request)    │  │ (net/http)      │  │ (library call) │   │
│  └─────────────────┘  └─────────────────┘  └────────────────┘   │
└──────────────────────────────────────────────────────────────────┘
```

### Key design properties

| Property | Choice | Rationale |
|----------|--------|-----------|
| **State ownership** | KV is authoritative snapshot; stream is authoritative changelog | KV serves reads; stream enables rebuild. No dual-write problem |
| **Component coupling** | Materializer writes KV; Query Engine reads KV | No direct coupling — they share only the KV bucket name |
| **Processing model** | Single-threaded event loop per view | Simplifies consistency; no CAS conflicts within one materializer |
| **Replication** | NATS handles it (JetStream + KV replicas) | Zero infrastructure beyond NATS |
| **Query engine** | Stateless, per-request | No session state, no transaction state, trivial to scale |

---

## 2. Materializer Component

### 2.1 Purpose

Consume a JetStream durably, map each event to a row, and upsert the corresponding KV entries (primary row + secondary index entries).

### 2.2 Consumer Setup

```
Stream:   user-provided source stream (e.g., "ORDERS", "users")
Consumer: Durable pull consumer per view
Policy:   DeliverAll on fresh start, DeliverNew on resume
Ack:      AckExplicit — only ack after KV writes succeed
Ordering: Single consumer, single-threaded processing
Rebuild:  Delete consumer, recreate with DeliverAll
```

**Key decisions:**

- **Pull consumer over push**: Pull gives explicit flow control via `Fetch()` / `Messages()`; no DeliverSubject needed; works through NATS auth boundaries. Recommended by NATS docs for new projects.
- **Single consumer per view, not queue group**: Queue groups distribute messages across workers. For materialization, we need ordered processing (events must be applied in stream order). A single consumer guarantees order. If throughput becomes a bottleneck, shard the source stream by subject and create one materializer per shard.
- **Ack after KV write, not before**: If we ack then crash before KV write completes, we lose the event. Ack must happen after the entire `Put` sequence (PK + all indexes) succeeds. JetStream's redelivery gives at-least-once; KV's CAS gives idempotency on replay.

### 2.3 Event-to-Row Mapping

The row mapper is a configurable transformation. For v1, it supports:

```yaml
# config.yaml
views:
  - name: "users"
    source_stream: "users_events"
    source_subject: "users.>"           # optional filter
    columns:
      - name: "id"
        from: "$.user_id"               # JSON path in event payload
        type: "string"
        primary_key: true
      - name: "name"
        from: "$.name"
        type: "string"
      - name: "email"
        from: "$.email"
        type: "string"
      - name: "age"
        from: "$.age"
        type: "int"
    indexes:
      - column: "age"                   # equality lookups
      - column: "name"                   # range scans
    event_type_field: "$.type"          # optional: which field determines event type
    delete_on: "user_deleted"           # optional: event type that removes the row
```

**Mapper interface:**

```go
// RowMapper converts a JetStream message into a set of KV mutations.
// Implementations are config-defined and stateless.
type RowMapper interface {
    // MapRow produces the mutations for this event.
    // Returns nil mutations if the event should be skipped.
    // Returns ErrSkipAndAck to silently skip (no error logging).
    // Returns ErrMalformedEvent to log+skip (stream continues).
    MapRow(msg jetstream.Msg) (*RowMutation, error)
}

type RowMutation struct {
    PK           string            // encoded primary key value
    RowData      map[string]any    // the full column values
    IsDelete     bool              // true if this is a tombstone
}
```

**Schema evolution strategy for v1:**

Schema is defined at view creation time and is immutable without a full rebuild. To "evolve" the schema:

1. Stop the old materializer
2. Define a new view config with the new schema
3. Run a full rebuild (purge KV bucket, replay stream from beginning)

The v1 position: **schema evolution = re-materialization**. This matches the project requirement ("Schema migration / ALTER VIEW with backfill — requires full re-materialize").

A future v2 could support additive schema changes (new columns with defaults) but v1 keeps it simple.

### 2.4 KV Writer

The KV writer translates `RowMutation` into actual KV operations:

```go
type KVWriter struct {
    kv     jetstream.KeyValue
    view   string // view name, used as key prefix
}

func (w *KVWriter) Apply(ctx context.Context, mut *RowMutation) error {
    // 1. If delete: remove PK row + all index entries for old values
    if mut.IsDelete {
        oldRow, err := w.kv.Get(ctx, w.pkKey(mut.PK))
        if err == nil {
            oldValues := parseRow(oldRow.Value())
            // Remove each index entry for the old values
            for _, idx := range mut.Indexes {
                w.kv.Delete(ctx, w.idxKey(idx.Name, oldValues[idx.Name], mut.PK))
            }
        }
        return w.kv.Delete(ctx, w.pkKey(mut.PK))
    }

    // 2. Read existing row (if any) for index diff computation
    oldValues := map[string]any{}
    if entry, err := w.kv.Get(ctx, w.pkKey(mut.PK)); err == nil {
        oldValues = parseRow(entry.Value())
    }

    // 3. Write PK row
    rowJSON, _ := json.Marshal(mut.RowData)
    _, err := w.kv.Put(ctx, w.pkKey(mut.PK), rowJSON)
    if err != nil {
        return err
    }

    // 4. Diff indexes: remove old index entries, add new ones
    for _, idx := range mut.Indexes {
        oldVal := oldValues[idx.Name]
        newVal := mut.RowData[idx.Name]

        if oldVal != newVal {
            if oldVal != nil {
                w.kv.Delete(ctx, w.idxKey(idx.Name, fmt.Sprint(oldVal), mut.PK))
            }
            if newVal != nil {
                w.kv.Put(ctx, w.idxKey(idx.Name, fmt.Sprint(newVal), mut.PK), nil)
            }
        }
    }

    // 5. Update consumer progress marker
    // (handled by materializer controller, not the writer)
    return nil
}
```

**Atomicity note**: NATS KV does NOT support multi-key atomic writes. The sequence PK-write → index-delete → index-create has crash windows where:
- PK was written but index entries are stale
- Index entries point to a PK that doesn't exist (because of in-progress delete)

**Mitigation for v1**: These windows are acceptable given the read-committed consistency model. A periodic "index sweep" (separate goroutine) can validate index→PK consistency and repair drift. For v1, skip the sweep — document the gap.

### 2.5 Event Processing Loop

```
for {
    select {
    case msg := <-consumer.Messages():
        // 1. Deserialize headers for metadata
        seq := msg.Metadata().Sequence.Stream

        // 2. Map event → row mutation
        mut, err := mapper.MapRow(msg)

        // 3. Apply to KV
        if err == nil {
            if mut != nil {
                err = writer.Apply(ctx, mut)
            }
        }

        // 4. Ack only after successful KV write
        if err == nil {
            msg.Ack()          // at-least-once delivery
            saveProgress(seq)  // optionally persist for rebuild-from-seq
        } else if errors.Is(err, ErrMalformedEvent) {
            msg.Ack()          // skip malformed, don't block stream
            log.Warn("skipped malformed event", "seq", seq, "error", err)
        } else {
            msg.Nak()          // retry on transient errors
        }

    case <-rebuildRequest:
        // Full rebuild flow (see §7)
    }
}
```

### 2.6 Error Handling Policy

| Error | Action | Rationale |
|-------|--------|-----------|
| Malformed event JSON | Ack + skip + log | One bad event shouldn't block the stream |
| Column type mismatch | Ack + skip + log | Same reasoning as malformed |
| KV write conflict (CAS) | Retry (read-modify-write) | CAS protects against concurrent index sweep |
| NATS connection lost | Nak → redeliver | At-least-once; redeliver when connected |
| Context cancelled | Return immediately | Graceful shutdown |
| All other errors | Nak with backoff | Transient: retry with exponential backoff |

---

## 3. KV Layout Design

### 3.1 Key Space

All state for all views lives in a single KV bucket named `natsql-views`. This follows the ebind pattern of one bucket per subsystem.

```
Key hierarchy:

natsql-views/
├── {view}/
│   ├── pk/
│   │   └── {encoded_pk_value}    →  {"id":"...", "name":"...", ...}
│   ├── idx/
│   │   └── {col}/
│   │       └── {encoded_val}/
│   │           └── {encoded_pk}  →  "" (empty, just key presence)
│   └── meta/
│       ├── schema                →  JSON schema definition
│       ├── consumer-seq          →  last applied stream sequence
│       └── status                →  {"state":"running|rebuilding|paused"}
```

### 3.2 Encoding Rules

| Value type | Encoding | Example |
|-----------|----------|---------|
| `string` | As-is (KV keys support `[a-zA-Z0-9_\-./=]`) | `alice@example.com` |
| `int` / `int64` | Zero-padded hex for range-sortable keys | `000000000000002a` (42) |
| `float64` | Not supported as PK/idx in v1 | — |
| `bool` | `t` / `f` | `t` |
| `time.Time` | RFC3339Nano (sortable when padded) | `2026-05-23T12:00:00Z` |
| `null` / `nil` | Omitted from index (null values not indexed) | — |

**PK value encoding must be reversible** (to decode from a key back to a value) and **must not contain `/`** (KV key separator). Use URL-safe base64 or hex for opaque PKs.

For range-sortable numeric encodings, use fixed-width hex:

```go
func encodeInt(v int64) string {
    return fmt.Sprintf("%016x", v) // 16 hex chars = 64 bits
}

func decodeInt(s string) (int64, error) {
    return strconv.ParseInt(s, 16, 64)
}
```

### 3.3 PK Row Format

```json
{
    "id": "abc123",
    "name": "Alice",
    "email": "alice@example.com",
    "age": 30,
    "_meta": {
        "stream_seq": 42,
        "updated_at": "2026-05-23T12:00:00Z",
        "deleted": false
    }
}
```

The `_meta` field is injected by the materializer (not from the event). It enables:
- **Rebuild validation**: detect if a row was written by a stale version of the code
- **Debugging**: trace which stream event produced this row

### 3.4 Index Key Design

```
{view}/idx/{col}/{encoded_val}/{encoded_pk}
```

**Why include PK in the key**: Two index entries with the same column value (e.g., `age=30`) must not collide. Appending the PK makes each key unique within the KV bucket.

**Why empty value**: The index entry itself carries no data — the key IS the data. An empty value saves KV storage.

**Range scan mechanics**: To query `WHERE age > 25 AND age < 35`:
1. List all keys with prefix `users/idx/age/`
2. Filter client-side for keys between `25` and `35` (using the hex encoding for proper lexicographic ordering)
3. Extract PKs from the remaining keys
4. Batch-fetch PK rows
5. The hex encoding of ints gives us proper byte-order sorting for prefix scan → range filter

**Limitation**: NATS KV `ListKeys` returns ALL keys in the bucket (no prefix filtering at the server level). For large datasets, this is expensive. Mitigation: use per-index key prefixes so `ListKeys` returns only index keys, then filter client-side.

### 3.5 Bucket Configuration

```go
kvConfig := jetstream.KeyValueConfig{
    Bucket:   "natsql-views",
    Storage:  jetstream.FileStorage,
    Replicas: replicas,
    // No MaxValueSize set (default 64KB) — sufficient for row data
    // History: 1 — we only need current state
    // TTL: 0 — data lives until explicitly deleted
}
```

**Should each view get its own bucket?** No for v1. One bucket is simpler to manage, and NATS KV bucket count is limited. If isolation is needed later, the `view` key prefix provides logical separation. A future optimization could use per-view buckets for independent TTL/replication settings.

**History of 1**: The KV only keeps the latest revision per key. We don't need historical row values — the source stream IS the history. This saves storage.

---

## 4. Query Engine Architecture

### 4.1 Pipeline

```
SQL text ──▶ Parser ──▶ AST ──▶ Validator ──▶ Resolved AST ──▶ Plan Builder ──▶ Plan ──▶ Executor ──▶ Rows
```

### 4.2 SQL Parser

**Library choice**: `github.com/xwb1989/sqlparser` — a standalone extract of Vitess's MySQL parser. It handles our v1 subset (SELECT with WHERE, comparison operators, LIMIT) with no external dependencies.

```go
import "github.com/xwb1989/sqlparser"

stmt, err := sqlparser.Parse("SELECT id, name FROM users WHERE age = 30 LIMIT 10")
// stmt is *sqlparser.Select with:
//   .SelectExprs (columns)
//   .From (table refs)
//   .Where (*sqlparser.Where with comparison)
//   .Limit (*sqlparser.Limit with *sqlparser.SQLVal)
```

**Risk**: xwb1989/sqlparser was extracted in 2019 and may lag behind Vitess upstream. If it lacks features, fall back to `github.com/dolthub/vitess` (actively maintained by Dolt). Or for our tiny SQL subset, write a hand-written parser in ~200 lines of Go using `text/scanner` — this eliminates the dependency entirely and gives full control over error messages.

**Recommendation**: Start with hand-written parser for v1 (SELECT only, `WHERE col = val / col > val / col < val`, `LIMIT`). 99% of the complexity in SQL parsers comes from INSERT/UPDATE/DELETE, JOINs, and subqueries — none of which we support. A hand-written parser is simpler and has zero dependency risk.

```go
type Query struct {
    Select     []string       // column names; nil or empty = *
    From       string         // view name
    Where      []Condition   // AND-connected
    Limit      int           // 0 = no limit
    OrderBy    []OrderExpr   // v2 feature
}

type Condition struct {
    Column string
    Op     Op // Eq, Gt, Lt, Gte, Lte, Neq
    Value  any
}
```

### 4.3 Validator

Checks the parsed query against the view schema (loaded from KV `{view}/meta/schema`):

| Check | Action |
|-------|--------|
| View exists in config | Error: "materialized view 'X' not found" |
| SELECT columns exist | Error: "column 'X' not found" |
| WHERE column exists | Error: "column 'X' not found" |
| WHERE value type matches schema | Warning: implicit cast (string→int) or error |
| WHERE operator supported | Error: "operator 'LIKE' not supported in v1" |
| LIMIT is positive int | Error: "LIMIT must be positive" |

### 4.4 Plan Builder

The plan builder turns a validated query into an executable plan tree:

```
Plan types:

SelectPlan           ← top-level node
  ├─ TableScanPlan   ← full scan (all rows in view)
  │   └─ FilterPlan  ← apply WHERE conditions
  │       └─ LimitPlan  ← slice rows
  │
  ├─ IndexPlan       ← look up through index, fetch rows
  │   └─ FilterPlan  ← apply remaining conditions
  │       └─ LimitPlan
```

**Plan selection logic:**

```
For each condition in WHERE:
  If condition matches an index:
    - For EQ condition: Use IndexPlan with single-point lookup
    - For GT/LT/GTE/LTE condition: Use IndexPlan with range scan
  If condition is on PK:
    - For EQ: Use PKPointLookupPlan (single Get, cheapest)
    - For range: fall back to full TableScan

If any condition uses an index:
  Use IndexPlan for the most selective condition
  Apply remaining conditions as FilterPlan on top

If no condition uses an index or PK:
  Use TableScanPlan (list all view keys, filter in memory)
```

**Cost estimation (v2 feature, not in scope for v1 MVP):**
In v1, the planner uses simple heuristics:
1. PK equality lookup (single `kv.Get`) — cheapest
2. Index equality lookup (single `kv.Get` by index key prefix, then batch `kv.Get` for PKs) — medium
3. Index range scan (list index keys, filter, batch fetch PKs) — medium
4. Full table scan (list all PK keys, fetch each, filter) — most expensive

For v1, always prefer (1) > (2) > (3) > (4).

### 4.5 Executor

```go
type Executor struct {
    kv   jetstream.KeyValue
    view string
}

// Execute runs a plan against the KV store.
func (e *Executor) Execute(ctx context.Context, plan *SelectPlan) ([]Row, error)
```

**Execution strategies:**

| Plan type | KV operations | Complexity |
|-----------|--------------|------------|
| PKPointLookup (WHERE id = 'x') | 1× `kv.Get("{view}/pk/{pk}")` | O(1) |
| IndexEqLookup (WHERE age = 30, age indexed) | 1× list index prefix → N× `kv.Get` PK rows | O(N) where N = matching rows |
| IndexRangeScan (WHERE age > 25 AND age < 35, age indexed) | List index prefix + client filter → N× `kv.Get` PK rows | O(K + N) where K = total indexed values for column |
| FullScan (no useful index) | List all `{view}/pk/` keys → N× `kv.Get` | O(M) where M = total rows in view |

**Batch fetch optimization**: When a plan produces N PKs to fetch, batch them with parallel `kv.Get` calls using a worker pool. NATS KV gets are network round-trips; batching reduces latency.

**Limit pushdown**: If the plan has a `LimitPlan` directly above an `IndexEqLookup`, stop fetching PKs once the limit is reached. This avoids unnecessary KV reads.

### 4.6 Query Engine Interface

```go
type Engine struct {
    // Views returns the schema for a named view.
    Views() map[string]*ViewSchema
}

// Execute runs a SQL query against the materialized state.
// Returns structured rows, not wire-format bytes.
func (e *Engine) Execute(ctx context.Context, query string) (*Result, error)

type Result struct {
    Columns []Column
    Rows    []Row
    Err     error
}
```

The engine is:
- **Stateless**: no connections, no transactions, no prepared statement cache (for v1)
- **Thread-safe**: multiple goroutines can call `Execute` concurrently
- **Context-aware**: cancellation propagates to KV operations

---

## 5. Index Maintenance

### 5.1 Index Update Sequence

```
Event arrives
  │
  ├── 1. Read existing PK row from KV (if exists)
  │        kv.Get("{view}/pk/{pk}") → oldRow
  │
  ├── 2. Compute old vs new column values
  │        oldRow["age"] = 25, newRow["age"] = 30
  │
  ├── 3. Write new PK row
  │        kv.Put("{view}/pk/{pk}", newRowJSON)
  │
  ├── 4. Remove old index entries (for changed values)
  │        kv.Delete("{view}/idx/age/25/{pk}")
  │
  └── 5. Add new index entries
           kv.Put("{view}/idx/age/30/{pk}", nil)
```

**Crash windows:**

| After step | If crash happens | Result | Severity |
|-----------|-----------------|--------|----------|
| 1 | Old row read but nothing written | Event replayed on restart, applies cleanly | None |
| 2 | — | No KV change, idempotent replay | None |
| 3 | PK written, indexes stale | Row exists with old index entries | **Low**: query returns row via PK, index query misses it |
| 4 | PK written, some old indexes deleted, some new indexes missing | Row exists, old index might not find it, new index not yet set | **Low**: temporary blind spot
| 5 | All changes complete | Consistent | None |

**Mitigation for v1**: Accept the temporary inconsistency (read-committed model). A "background consistency sweep" (v2) would list all index entries, verify each PK exists, and clean up orphans. The sweep runs periodically and on startup.

### 5.2 Index Consistency Sweep (v2 design, noted for roadmap)

```go
func (s *IndexSweeper) Sweep(ctx context.Context) {
    // 1. List all index keys
    // 2. For each index entry, check PK exists
    // 3. Remove orphan index entries (PK deleted but index not cleaned)
    // 4. Find PKs that are missing expected index entries and rewrite them
}
```

The sweep is best-effort. It runs less frequently (every N events, or on a timer). CAS prevents races with the live materializer.

### 5.3 What Happens with Concurrent Writes

Since there is exactly ONE materializer per view (single consumer), there are no concurrent writes to the same view's KV keys from the materialization path. The only concurrent readers are:
- **Query engine**: reads only, no conflict
- **Index sweep** (if running): reads and deletes, uses CAS to avoid races with materializer

---

## 6. Dual Interface: NATS + HTTP from the Same Engine

### 6.1 Architecture

```
                    ┌──────────────┐
                    │  QueryEngine  │
                    │  Execute()    │
                    └──────┬───────┘
                           │
              ┌────────────┼────────────┐
              │            │            │
              ▼            ▼            ▼
        ┌──────────┐ ┌──────────┐ ┌──────────┐
        │ NATS     │ │ HTTP     │ │ In-proc  │
        │ Handler  │ │ Handler  │ │ Caller   │
        └──────────┘ └──────────┘ └──────────┘
```

The engine is unaware of the transport. All three callers:

```go
// NATS handler
nc.Subscribe("natsql.query", func(msg *nats.Msg) {
    result := engine.Execute(ctx, string(msg.Data))
    msg.Respond(result.ToJSON())
})

// HTTP handler
http.HandleFunc("POST /query", func(w http.ResponseWriter, r *http.Request) {
    body, _ := io.ReadAll(r.Body)
    result := engine.Execute(ctx, string(body))
    json.NewEncoder(w).Encode(result)
})

// In-process caller
result, err := engine.Execute(ctx, "SELECT * FROM users WHERE age = 30")
```

### 6.2 NATS Protocol

```
Subject:    natsql.query
Request:    "SELECT id, name FROM users WHERE age = 30 LIMIT 10"
Response:   JSON bytes of Result struct

Inbox-based request-reply (standard NATS pattern).
The requester gets exactly one response (or timeout).

Optional: support subject-per-view for routing:
  natsql.query.users  →  query scoped to "users" view
```

### 6.3 HTTP Protocol

```
POST /api/v1/query
Content-Type: application/json

{"query": "SELECT id, name FROM users WHERE age = 30 LIMIT 10"}

Response 200:
{
    "columns": ["id", "name"],
    "rows": [
        {"id": "abc123", "name": "Alice"},
        {"id": "def456", "name": "Bob"}
    ],
    "elapsed_ms": 4.2
}
```

### 6.4 Why This Works

The engine is pure data transformation: `(ctx, sql_string) → (columns, rows)`. Transport is just an envelope. This design:
- Eliminates code duplication
- Makes the engine testable without NATS or HTTP
- Allows embedders to use their own transport (WebSocket, gRPC, Unix socket)
- Makes it trivial to add new transports later

---

## 7. Embedding vs Standalone

### 7.1 Two Deployment Modes

```
┌──────────────────────────────────────────────────┐
│  Standalone binary (cmd/natsql/)                  │
│                                                   │
│  ┌──────────┐   ┌────────────┐   ┌─────────────┐ │
│  │ Config   │──▶│ Materialize│──▶│ Serve NATS  │ │
│  │ Loader   │   │ + Query    │   │ + HTTP      │ │
│  └──────────┘   └────────────┘   └─────────────┘ │
└──────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────┐
│  Embedded library (import "github.com/.../natsql") │
│                                                   │
│  User's Go app:                                   │
│  ┌──────────┐   ┌──────────┐                     │
│  │ My App   │──▶│ natsql   │                     │
│  │ Logic    │   │ Engine   │                     │
│  └──────────┘   │          │                     │
│                 │ Execute()│                     │
│                 └──────────┘                     │
└──────────────────────────────────────────────────┘
```

### 7.2 Library Interface

```go
package natsql

// Config defines a materialized view.
type Config struct {
    Views []ViewConfig `yaml:"views"`
}

// ViewConfig defines one materialized view.
type ViewConfig struct {
    Name        string   `yaml:"name"`
    SourceStream string `yaml:"source_stream"`
    Columns     []ColumnConfig `yaml:"columns"`
    Indexes     []IndexConfig `yaml:"indexes,omitempty"`
}

// Engine is the top-level object. It owns the materializer and query engine.
type Engine struct {
    // unexported fields
}

// New creates a new engine from a NATS JetStream connection and config.
func New(js jetstream.JetStream, cfg *Config, opts ...Option) (*Engine, error)

// Views returns registered view schemas.
func (e *Engine) Views() map[string]*ViewSchema

// Execute runs a read-only SQL query against the materialized state.
func (e *Engine) Execute(ctx context.Context, query string) (*Result, error)

// Start begins consuming all configured streams.
// Blocks until ctx is cancelled. Idempotent.
func (e *Engine) Start(ctx context.Context) error

// Close stops all consumers and releases resources.
func (e *Engine) Close() error
```

### 7.3 Options Pattern

```go
type Options struct {
    Replicas     int           // KV bucket + consumer replicas (default: 1)
    KVBucketName string        // override default "natsql-views"
    Logger       *slog.Logger
}

type Option func(*Options)
```

### 7.4 Standalone Binary

```go
// cmd/natsql/main.go
func main() {
    cfg := loadConfig()
    nc, _ := nats.Connect(cfg.NATS.URL)
    js, _ := jetstream.New(nc)

    engine, _ := natsql.New(js, cfg)

    // Start materializers in background
    go engine.Start(ctx)

    // Start HTTP server (optional)
    if cfg.HTTP.Enabled {
        http.HandleFunc("/query", func(w, r) {
            result := engine.Execute(ctx, r.FormValue("query"))
            json.NewEncoder(w).Encode(result)
        })
        http.ListenAndServe(cfg.HTTP.Addr, nil)
    }

    // Register NATS query handler (always enabled)
    nc.Subscribe("natsql.query", func(msg *nats.Msg) {
        result := engine.Execute(ctx, string(msg.Data))
        msg.Respond(result.ToJSON())
    })

    <-ctx.Done()
}
```

### 7.5 Key Design Principle

The `Engine` struct IS the component boundary. Everything else is internal:

```
natsql/
├── engine.go         → public API: New(), Execute(), Start(), Close()
├── config.go         → Config, ViewConfig, ColumnConfig
├── materialize/      → internal: consumer, mapper, KV writer
│   ├── consumer.go
│   ├── mapper.go
│   └── writer.go
├── query/            → internal: parser, planner, executor
│   ├── parser.go
│   ├── planner.go
│   └── executor.go
├── kv/               → internal: KV helper (key encoding, batch ops)
│   └── kv.go
└── cmd/
    └── natsql/       → standalone binary entrypoint
        └── main.go
```

The ebind package layout is the model: public API surface is minimal (one struct, three core methods), all implementation detail is internal. Embedders use `natsql.New(js, config).Execute(ctx, "SELECT ...")`.

---

## 8. Recovery and Rebuild

### 8.1 Normal Recovery (Crash Restart)

```
1. Process starts
2. Load config
3. Open KV bucket (CreateOrUpdateKeyValue — idempotent)
4. For each view:
   a. Create durable consumer with the same name as before
      → NATS JetStream resumes from last ack
   b. Start consuming events
   c. For each un-acked event: map → KV write → ack
5. KV bucket contains the snapshot as of the last ack
```

**Recovery is automatic.** The durable consumer name is deterministic (`natsql-{view}`). On restart, NATS resumes delivery from the last acknowledged sequence. The materializer replays the few un-acked events and catches up.

**No explicit "recovery mode" needed** — the consumer's `DeliverNew` policy means it only processes new messages. The KV bucket is always consistent with the consumer's ack point.

### 8.2 Full Rebuild (Schema Change, Corruption, First Deploy)

```
1. Delete the durable consumer (js.DeleteConsumer)
2. Purge the KV bucket keys for this view
3. Recreate consumer with DeliverAll
4. Replay all events from the beginning of the stream
5. For each event: map → KV write → ack
```

**Implementation:**

```go
func (e *Engine) RebuildView(ctx context.Context, viewName string) error {
    view := e.views[viewName]

    // 1. Delete consumer to reset state
    stream, _ := e.js.Stream(ctx, view.SourceStream)
    stream.DeleteConsumer(ctx, "natsql-"+viewName)

    // 2. Delete all KV keys for this view
    keys, _ := e.kv.ListKeys(ctx)
    for k := range keys.Keys() {
        if strings.HasPrefix(k, viewName+"/") {
            e.kv.Delete(ctx, k)
        }
    }

    // 3. Recreate consumer starting from beginning
    cons, _ := stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
        Durable:      "natsql-" + viewName,
        AckPolicy:    jetstream.AckExplicitPolicy,
        DeliverPolicy: jetstream.DeliverAllPolicy,
        FilterSubject: view.FilterSubject,
    })

    // 4. Replay
    for msg := range cons.Messages().Messages() {
        mut, err := view.Mapper.MapRow(msg)
        if err == nil && mut != nil {
            view.Writer.Apply(ctx, mut)
        }
        msg.Ack()
    }

    return nil
}
```

**Rebuild is O(N)** where N = number of events in the stream. For large streams, this takes time. In v1, rebuild is an offline operation (the view is unavailable during rebuild).

**Future optimization**: Rebuild from the source stream PLUS the current KV snapshot (replay only events newer than the snapshot). This turns a full rebuild into an incremental catch-up.

### 8.3 Graceful Degradation on Malformed Events

The materializer MUST NOT crash or halt on a single bad event. The policy:

```go
// ErrMalformedEvent signals that an event cannot be processed
// but should not block the stream.
var ErrMalformedEvent = errors.New("malformed event")

// In the mapper:
func (m *StaticMapper) MapRow(msg jetstream.Msg) (*RowMutation, error) {
    var raw map[string]any
    if err := json.Unmarshal(msg.Data(), &raw); err != nil {
        return nil, fmt.Errorf("%w: %v", ErrMalformedEvent, err)
    }
    // ...
}

// In the processing loop:
mut, err := mapper.MapRow(msg)
if errors.Is(err, ErrMalformedEvent) {
    msg.Ack()                   // skip it
    log.Warn("skipped", "seq", seq, "err", err)
    continue
}
```

---

## 9. Data Flow Paths (End-to-End)

### 9.1 Write Path (Stream → KV)

```
Producer
  │  publishes JSON event to JetStream subject
  ▼
JetStream Source Stream (e.g., "orders")
  │  durably stored, replicated
  ▼
Durable Pull Consumer ("natsql-orders")
  │  ordered, at-least-once delivery
  ▼
Row Mapper (config-defined JSON field → column mapping)
  │  extracts id, customer_id, total, items
  ▼
KV Writer
  │  1. kv.Put("orders/pk/ORDER-123", rowJSON)
  │  2. kv.Put("orders/idx/status/pending/ORDER-123", "")
  │  3. kv.Put("orders/idx/customer_id/CUST-456/ORDER-123", "")
  ▼
Ack message (only after all KV writes succeed)
```

### 9.2 Read Path (SQL → Response)

```
Client (NATS request / HTTP / Go code)
  │  "SELECT id, total FROM orders WHERE customer_id = 'CUST-456'"
  ▼
SQL Parser
  │  AST: Select{Columns: [id, total], From: orders, Where: Eq(customer_id, "CUST-456")}
  ▼
Validator
  │  Checks: view "orders" exists, columns exist, type match
  ▼
Plan Builder
  │  Index plan: customer_id is indexed → IndexEqLookup
  ▼
KV Executor
  │  1. kv.ListKeys("orders/idx/customer_id/CUST-456/") → [".../ORDER-123"]
  │  2. kv.Get("orders/pk/ORDER-123") → {"id":"ORDER-123",...}
  │  3. Filter: (no additional conditions)
  │  4. Limit: (no LIMIT clause → return all)
  ▼
Result
  │  Columns: [id, total]
  │  Rows: [[ORDER-123, 42.50]]
  ▼
Transport Encoder (JSON response for HTTP/NATS)
```

### 9.3 Rebuild Path (Stream Replay → KV)

```
Operator triggers rebuild
  ▼
Delete consumer + purge view keys
  ▼
Create consumer with DeliverAll
  ▼
For each event in stream (oldest to newest):
  Map event → KV upsert → ack
  ▼
View is queryable again (consistent at end of replay)
```

---

## 10. Scalability Considerations

| Concern | v1 Approach | Future (v2/v3) |
|---------|------------|----------------|
| **Large streams** (>1M events) | Sequential replay; O(N) on first materialize | Parallel shards per subject prefix; incremental catch-up |
| **Large KV buckets** (>100K keys) | `ListKeys` returns all keys; filter client-side | Per-view buckets; server-side prefix filtering when NATS adds it |
| **High event throughput** (>1K/s) | Single consumer, single-threaded | Shard source stream by subject; one materializer per shard |
| **Query concurrency** | Per-request stateless; scales horizontally | Connection pooling; prepared statement cache |
| **Range scans on non-indexed columns** | Full table scan (slow) | Warn in docs; require index for range queries |
| **KV value size** | 64KB max per row (NATS limit) | Split large rows; use Object Store for blobs |

**NATS KV `ListKeys` limitation**: `ListKeys` returns ALL keys in the bucket with no prefix filter. For a bucket with millions of keys, this is a blocking O(N) operation. Mitigation:
- Use narrow key prefixes (`{view}/idx/{col}/{val}/`) to bound the list
- Accept that full table scans are expensive
- Consider per-view buckets if `ListKeys` becomes a bottleneck

---

## 11. Build Order (Roadmap Implications)

The architecture implies a strict dependency order for implementation:

```
Phase 1: Foundation (no SQL yet)
  ├── Config loader (YAML/JSON → Config struct)
  ├── KV helper package (key encoding utilities)
  ├── Durable consumer setup (create/resume consumer)
  ├── Event→Row mapper (config-driven, JSON extraction)
  ├── KV Writer (PK upsert + index update)
  └── Manual integration test (publish → verify KV state)

Phase 2: Minimal SQL (SELECT with PK lookup)
  ├── SQL parser (minimal hand-written)
  ├── Validator (view + column resolution)
  ├── Plan builder (PK lookup only)
  ├── Executor (single kv.Get)
  └── NATS request-reply handler

Phase 3: Indexes + WHERE
  ├── Index entry creation in materializer
  ├── IndexEqLookup plan
  ├── IndexRangeScan plan
  ├── FullScan plan (fallback)
  ├── Limit support
  └── HTTP/JSON API handler

Phase 4: Embedding + Server
  ├── Engine public API cleanup
  ├── Options pattern
  ├── Standalone binary (cmd/natsql)
  ├── Graceful shutdown
  └── Documentation + examples

Phase 5: Operational Hardening
  ├── Full rebuild (purge + replay)
  ├── Malformed event handling
  ├── Logging + metrics
  ├── Integration test suite
  └── Performance benchmarks
```

**Key dependencies:**
- Phase 1 must precede everything else (no state without materializer)
- Phase 2 depends on Phase 1 (no queries without state)
- Phase 3 depends on Phase 2 (query engine skeleton needed before adding index plans)
- Phase 4 depends on Phase 3 (useful server needs indexes)
- Phase 5 can start alongside Phase 3 (rebuild, logging are independent)

---

## 12. Sources and References

| Source | What | Confidence |
|--------|------|------------|
| ebind `workflow/store_nats.go` | KV key hierarchy pattern (`<id>/meta`, `<id>/step/<x>`, `<id>/result/<x>`) | HIGH (local code) |
| ebind `workflow/scheduler.go` | Event-driven state machine + CAS retry pattern | HIGH (local code) |
| ebind `workflow/events_nats.go` | Durable JetStream consumer with AckExplicit | HIGH (local code) |
| ebind `stream/setup.go` | Stream configuration patterns | HIGH (local code) |
| NATS JetStream docs | Consumer types, KV store capabilities, CAS semantics | HIGH (docs.nats.io) |
| Materialize architecture blog | pTVC abstraction, incremental computation, adapter/storage/compute split | MEDIUM (architecture inspiration, overkill for v1) |
| go-mysql-server ARCHITECTURE.md | SQL pipeline: parse → analyze (resolve+optimize) → execute | HIGH (pattern adapted for v1) |
| xwb1989/sqlparser | MySQL-dialect SQL parser in Go | MEDIUM (fallback option; hand-written parser preferred for v1) |

---

*Generated for natsql project roadmap. Last updated: 2026-05-23.*
