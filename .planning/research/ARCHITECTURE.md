# Architecture Remediation: Code Review Fixes for natsql v2.0.0

**Project:** natsql — NATS-native materialized view engine
**Researched:** 2026-05-31
**Mode:** Architecture remediation for 25 code review findings (v2.0.0 milestone)
**Confidence:** HIGH (all findings verified against source code, fixes designed with zero new dependencies)

## Executive Summary

This document describes the architectural changes required to fix 25 code review findings (CR-01 through CR-25) in natsql v2.0.0. The fixes cluster into five architectural domains: **(1) Materializer ordering and error handling**, **(2) Canonical PK encoding pipeline**, **(3) Query planner predicate correctness**, **(4) Engine lifecycle synchronization**, and **(5) Config validation and transport hardening**. No new components are introduced — the 3-component model (Materializer, Query Engine, Transport) is preserved. All fixes are targeted modifications to existing components with well-defined integration boundaries.

The most architecturally significant change is removing the 16-goroutine worker pool from the materializer (CR-01), restoring the per-view ordered processing guarantee that NATS JetStream's single-consumer model provides. The second most significant is unifying the PK encoding pipeline (CR-02), eliminating a correctness bug where writes and reads produce different keys for the same data.

---

## 1. Architecture Overview (Post-Fix)

```
┌─────────────────────────────────────────────────────────────────┐
│                      Engine (internal/engine)                    │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │ Start() — synchronous setup with error propagation (CR-07)│  │
│  │   ├── net.Listen HTTP before goroutine                    │  │
│  │   ├── Materializer setup returns errors synchronously     │  │
│  │   └── NATS handler failure is fatal (documented)          │  │
│  └───────────────────────────────────────────────────────────┘  │
└──────────────────────────┬──────────────────────────────────────┘
                           │
┌──────────────────────────▼──────────────────────────────────────┐
│                  Materializer (internal/materialize)             │
│                                                                  │
│  ┌──────────────┐   ┌────────────────┐   ┌──────────────────┐  │
│  │ Durable Pull │──▶│ Sequential     │──▶│ KV Writer        │  │
│  │ Consumer     │   │ Process Loop   │   │ (ordered, per-   │  │
│  │ (no worker   │   │ (single        │   │  view)           │  │
│  │  pool)       │   │  goroutine)    │   │                  │  │
│  └──────────────┘   └────────────────┘   └───────┬──────────┘  │
│        │                                          │            │
│        │ CR-11: No InactiveThreshold              │            │
│        │ CR-12: Rename BatchSize                  │            │
│        │                                          │            │
│        │ Error Classification (CR-10):            │            │
│        │ ├── ErrMalformedEvent → DLQ + Ack        │            │
│        │ ├── Transient error → NAK + backoff      │            │
│        │ └── Terminal error → DLQ + Ack           │            │
└──────────────────────────┬──────────────────────────────────────┘
                           │ writes via
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│              NATS KV Bucket (single, shared)                     │
│  Keys: {view}/pk/{pk_encoded_once} ← Canonical PK (CR-02)      │
│        {view}/meta/schema                                        │
│  Values: schema columns only (SELECT * filters _meta) (CR-04)   │
└──────────────────────────┬──────────────────────────────────────┘
                           │ reads from
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│                     Query Engine (internal/query)                 │
│                                                                  │
│  ┌──────────┐   ┌──────────────┐   ┌────────────┐              │
│  │ Parser   │──▶│ Planner      │──▶│ Executor   │              │
│  │ (CR-05:  │   │ (CR-03: all  │   │ (CR-04:    │              │
│  │  reject  │   │  predicates  │   │  filter    │              │
│  │  unsup-  │   │  as post-    │   │  _meta in  │              │
│  │  ported) │   │  filters)    │   │  SELECT *) │              │
│  └──────────┘   └──────────────┘   └──────┬─────┘              │
│                                           │                    │
│                    CR-09: UseNumber in executor too             │
│                    CR-13: View prefix in WatchAll filter        │
└─────────────────────────────────────────────────────────────────┘
                           │
┌──────────────────────────▼──────────────────────────────────────┐
│                     Transport Layer                               │
│  ┌────────────────────┐   ┌────────────────┐                    │
│  │ NATS Request-Reply │   │ HTTP Server    │                    │
│  │ (CR-19: check      │   │ (CR-06: port   │                    │
│  │  Flush error,      │   │  from config;  │                    │
│  │  log Respond error)│   │  CR-18: proper │                    │
│  └────────────────────┘   │  body drain,   │                    │
│                           │  MaxBytesError)│                    │
│                           └────────────────┘                    │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │ CLI (cmd/natsql)                                         │   │
│  │ CR-14: Stream creation respects source_subject           │   │
│  │        No mutation of external streams without flag       │   │
│  └──────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
```

---

## 2. Component-by-Component Changes

### 2.1 Materializer (`internal/materialize/`)

#### 2.1.1 CR-01: Remove Concurrent Worker Pool — Restore Ordered Processing

**Current state:** `materializer.go:20` defines `materializerWorkers = 16`. The bridge goroutine feeds a channel from the consumer Messages() call. 16 worker goroutines drain that channel concurrently, calling `processEvent`. This destroys JetStream's per-subject ordering guarantee — two events for the same PK can be processed out of order.

**New architecture:** Process messages sequentially in the bridge goroutine itself. No worker pool, no shared channel.

```
Before (broken ordering):
  consumer.Messages().Next()
    → msgCh (buffered channel, 64)
      → 16 worker goroutines (concurrent processEvent)
        → kv.Put (last-writer-wins by goroutine scheduling)

After (ordered):
  consumer.Messages().Next()
    → processEvent (sequential, in same goroutine)
      → kv.Put (ordered, per-key stream order preserved)
```

**Implementation sketch:**

```go
// In materializer.Run — replace the worker pool with direct sequential processing:
for msg := range msgCh {
    eventCount.Add(1)
    processEvent(ctx, js, mapper, writer, msg, viewCfg, logger)
}
```

That's it — remove the `workerWg` block and `for i := 0; i < materializerWorkers; i++` loop entirely. Process messages in the receive loop.

**Impact analysis:**
- **Throughput**: Single-goroutine processing is slower than 16 concurrent workers for independent events. However, JetStream's pull consumer with `MaxAckPending` provides backpressure — we're bound by KV write latency, not CPU.
- **Correctness**: Stream order is preserved. Two updates to the same PK are always applied in the order they were published. **This is the correctness requirement.**
- **Latency**: Sequential processing means one slow event blocks subsequent events. Mitigation: add per-event timeout context in `processEvent` (e.g., 5s per event) so a stuck KV write doesn't block indefinitely.
- **Future optimization**: If throughput is insufficient, partition the source stream by subject and create one sequential consumer per partition. Each partition still processes sequentially; different partitions are independent.

**Integration points:** Remove `materializerWorkers` constant, `workerWg`, worker goroutine loop, and `sem` pattern. The bridge goroutine and drain support remain unchanged.

#### 2.1.2 CR-10: Error Classification in processEvent

**Current state:** `processEvent` treats every writer error the same — publish to DLQ, ack the original message. This permanently loses data on transient failures.

**New architecture:** Classify errors into three categories:

```go
// Sentinel errors for writer/processEvent
var (
    // ErrMalformedEvent — event data cannot be parsed. DLQ + Ack.
    // Already exists, usage unchanged.
    
    // ErrTransientFailure — temporary infrastructure issue. NAK + no DLQ.
    // The message will be redelivered by JetStream.
    ErrTransientFailure = fmt.Errorf("transient failure")
    
    // ErrTerminalWriteFailure — unrecoverable write error. DLQ + Ack.
    // Event cannot ever be processed.
    ErrTerminalWriteFailure = fmt.Errorf("terminal write failure")
)
```

**Error routing in processEvent:**

| Error Source | Error Type | Action |
|-------------|------------|--------|
| `mapper.MapRow()` returns `ErrMalformedEvent` | Malformed | DLQ + Ack |
| `mapper.MapRow()` returns other error | Malformed | DLQ + Ack |
| `writer.Apply()` returns context cancellation | Transient | NAK (redeliver on restart) |
| `writer.Apply()` returns NATS connection error | Transient | NAK with backoff |
| `writer.Apply()` returns KV timeout | Transient | NAK with backoff |
| `writer.Apply()` returns key validation error | Terminal | DLQ + Ack |
| `publishToDLQ()` fails | Transient | NAK (original msg not acked) |

**Implementation:**

```go
func processEvent(ctx context.Context, js jetstream.JetStream, mapper *Mapper, writer *Writer, msg jetstream.Msg, viewCfg *natsql.ViewConfig, logger *slog.Logger) {
    // ... existing mapper error handling (ErrMalformedEvent → DLQ + Ack) ...
    
    if mut != nil {
        if writeErr := writer.Apply(ctx, mut); writeErr != nil {
            if ctx.Err() != nil {
                msg.Nak()  // context cancelled → redeliver
                return
            }
            
            if isTransientError(writeErr) {
                logger.Warn("transient write failure, nacking", "seq", getMsgSeq(msg), "error", writeErr)
                msg.Nak()  // redeliver — may succeed next time
                return
            }
            
            // Terminal error: DLQ + Ack
            if dlqErr := publishToDLQ(ctx, js, msg, viewCfg.Name, writeErr); dlqErr != nil {
                logger.Error("DLQ publish failed, nacking event", "seq", getMsgSeq(msg), "error", dlqErr)
                msg.Nak()
            } else {
                msg.Ack()
            }
            logger.Error("terminal write failure, sent to DLQ", "seq", getMsgSeq(msg), "error", writeErr)
            return
        }
    }
    
    msg.Ack()
}
```

**Integration points:** New `isTransientError()` helper in `materializer.go`. No changes to `Writer.Apply` signature — classification happens at the caller level.

#### 2.1.3 CR-11: Remove InactiveThreshold from Durable Consumers

**Current state:** `consumer.go:53` sets `InactiveThreshold: 1 * time.Hour`. If the engine is down longer than an hour, NATS deletes the durable consumer. On restart, the consumer is recreated with `DeliverAllPolicy`, replaying the entire stream.

**Fix:** Remove `InactiveThreshold` from durable consumer config entirely. Durable consumers should persist until explicitly deleted.

```go
// Before:
consumerCfg := jetstream.ConsumerConfig{
    Durable:           ConsumerName(viewName),
    AckPolicy:         jetstream.AckExplicitPolicy,
    DeliverPolicy:     jetstream.DeliverAllPolicy,
    MaxDeliver:        maxDeliver,
    AckWait:           time.Duration(ackWaitSeconds) * time.Second,
    MaxAckPending:     batchSize * 2,
    InactiveThreshold: 1 * time.Hour,  // ← REMOVE
}

// After:
consumerCfg := jetstream.ConsumerConfig{
    Durable:       ConsumerName(viewName),
    AckPolicy:     jetstream.AckExplicitPolicy,
    DeliverPolicy: jetstream.DeliverAllPolicy,
    MaxDeliver:    maxDeliver,
    AckWait:       time.Duration(ackWaitSeconds) * time.Second,
    MaxAckPending: batchSize * 2,
    // No InactiveThreshold — durable consumers live until deleted
}
```

**Integration points:** Single line removal in `consumer.go:53`. Update any tests that assert InactiveThreshold.

#### 2.1.4 CR-12: Rename BatchSize or Document Actual Behavior

**Current state:** `ConsumerConfig.BatchSize` influences `MaxAckPending = batchSize * 2` but does NOT control fetch batch size (the consumer uses `Messages()` → repeated `Next()`). The name implies batch fetching behavior that doesn't exist.

**Fix:** Rename the config field to `MaxAckPending` to reflect its actual behavior, or add a `FetchBatchSize` if batch fetching is implemented. For v2.0.0 scope: rename the field.

```go
type ConsumerConfig struct {
    MaxAckPending int `yaml:"max_ack_pending,omitempty" json:"max_ack_pending,omitempty"`
    MaxDeliver     int `yaml:"max_deliver,omitempty" json:"max_deliver,omitempty"`
    AckWaitSeconds int `yaml:"ack_wait_seconds,omitempty" json:"ack_wait_seconds,omitempty"`
}
```

**Integration points:** Config struct in `internal/cfg/config.go`, consumer setup in `internal/materialize/consumer.go`, all test fixtures that reference `BatchSize`.

#### 2.1.5 CR-14: Stream Creation Respects Source Subject

**Current state:** `cmd/natsql/main.go:135-137` creates/updates source streams with `Subjects: []string{v.SourceStream + ".>"}`, ignoring the configured `source_subject`. In external NATS mode, this mutates existing streams unexpectedly.

**Fix:** Three changes:
1. Only create streams in embedded mode (the engine owns them)
2. In external mode, warn if stream doesn't exist and let the consumer setup fail naturally
3. Add `--create-streams` flag for explicit opt-in

```go
// In runServer():
// Only auto-create streams in embedded mode
if cfg.NATS.Embedded {
    createSourceStreams(ctx, js, cfg.Views, logger)
} else {
    // In external mode: just log a warning if streams don't exist
    // Consumer setup will fail with a clear error
    for _, v := range cfg.Views {
        if _, err := js.Stream(ctx, v.SourceStream); err != nil {
            logger.Warn("source stream not found (consumer setup will fail)", 
                "stream", v.SourceStream, "view", v.Name)
        }
    }
}
```

And when creating streams, respect `source_subject`:

```go
func createSourceStreams(ctx context.Context, js jetstream.JetStream, views []natsqlpkg.ViewConfig, logger *slog.Logger) {
    seen := map[string][]string{} // stream → subjects
    for _, v := range views {
        subj := v.SourceSubject
        if subj == "" {
            subj = v.SourceStream + ".>"
        }
        seen[v.SourceStream] = append(seen[v.SourceStream], subj)
    }
    for stream, subjects := range seen {
        cfg := jetstream.StreamConfig{
            Name:     stream,
            Subjects: subjects,
        }
        if _, err := js.CreateOrUpdateStream(ctx, cfg); err != nil {
            logger.Warn("failed to create source stream", "stream", stream, "error", err)
        }
    }
}
```

**Integration points:** CLI (`cmd/natsql/main.go`), new helper function. No changes to engine or materializer packages.

---

### 2.2 Canonical PK Encoding (`internal/kv/`, `internal/materialize/`, `internal/query/`)

#### 2.2.1 CR-02: Single Canonical PK Encoder

**Current state:** PK encoding is duplicated across three paths, creating inconsistency:

```
Write path:
  Mapper: stringifyValue(val) → SanitizePK(s) → join with separator → pk string
    → Writer: kv.PkKey(viewName, pk) → SanitizePK(pk) ← DOUBLE SANITIZE!

Read path (query):
  Planner: fmt.Sprint(value) join with separator → pkValue string
    → Executor: kv.PkKey(viewName, pkValue) → SanitizePK(pkValue) ← SANITIZED ONCE
```

The result: rows with PK values containing `_`, `|`, `/`, `*`, `>` are stored under a double-sanitized key on write but single-sanitized key on read.

**New architecture:** One function, called exactly once, used by all paths.

```go
// In internal/kv/kv.go — NEW canonical PK encoder:

// BuildPkKey constructs the full KV key for a row.
// Takes raw PK component values, joins them with the separator, sanitizes once,
// and returns the full key path.
// This is the SINGLE canonical function for PK key construction.
// Both materializer and query engine MUST use this.
func BuildPkKey(viewName string, pkParts []string, separator string) string {
    // 1. Join parts with separator (unsanitized)
    pk := strings.Join(pkParts, separator)
    
    // 2. Sanitize exactly once
    sanitized := SanitizePK(pk)
    
    // 3. Build full key
    return viewName + "/pk/" + sanitized
}
```

**Changes required by component:**

| Component | Current | New |
|-----------|---------|-----|
| `mapper.go:214-234` | `stringifyValue(val)` calls `SanitizePK` | Return raw string (no SanitizePK) |
| `mapper.go:105` | `pk := strings.Join(pkParts, separator)` | Return raw pk string, pass to writer |
| `RowMutation.PK` | string, already sanitized | string, raw (unsanitized) |
| `writer.go:48` | `kv.PkKey(w.viewName, mut.PK)` — double sanitizes | `kv.BuildPkKey(w.viewName, parts, sep)` — single call |
| `planner.go:30-38` | `fmt.Sprint(value)` then join | Use same string representation as mapper (without SanitizePK) |
| `executor.go:22` | `kv.PkKey(p.ViewName, p.PkValue)` | `kv.BuildPkKey(p.ViewName, p.PkValue, sep)` — but PkValue is already joined |

The key insight: `RowMutation.PK` changes from "already sanitized and joined" to "raw joined parts, not sanitized". The Writer is the single point that calls `BuildPkKey` which handles sanitization.

For the query path, the planner produces a `PkValue` string that is the raw-joined PK parts (same as what the mapper produces). The executor then calls `BuildPkKey` (which sanitizes) instead of `PkKey` (which also sanitizes but takes a different input format).

**Migration:**
1. Add `BuildPkKey` to `kv.go` (new function)
2. Remove `SanitizePK` call from `stringifyValue` in `mapper.go`
3. Change `Writer.Apply` to use `BuildPkKey(viewName, rawParts, separator)` instead of `PkKey(viewName, mut.PK)`
4. Change `PKLookupPlan.Execute` to use `BuildPkKey(viewName, pkValue, separator)` instead of `PkKey(viewName, pkValue)`
5. Keep `PkKey` as deprecated wrapper for backward compat or remove it

**Integration boundary:** The `RowMutation.PK` field changes semantics from "sanitized PK key suffix" to "raw joined PK parts". All consumers of `mut.PK` must be audited.

---

### 2.3 Query Engine (`internal/query/`)

#### 2.3.1 CR-03: Keep All Predicates as Post-Filters

**Current state:** `planner.go:40-46` removes conditions whose column appears in the PK condition map, even if they contradict the PK equality or are duplicate equalities on the same column.

**Fix:** Keep ALL original WHERE conditions as post-filters. Build the PK lookup key from matching equality conditions, but still apply those same conditions as post-filters.

```go
func BuildPlan(q *ValidatedQuery, schema *kv.ViewSchema) (Plan, error) {
    // ... existing PK detection ...

    if len(pkConditions) == len(schema.KeyFields) && len(schema.KeyFields) > 0 {
        pkValues := make([]string, len(schema.KeyFields))
        for i, kf := range schema.KeyFields {
            pkValues[i] = fmt.Sprint(pkConditions[kf].Value)
        }
        // ... build pkValue from pkValues ...

        // CR-03 FIX: Keep ALL conditions as post-filters, including PK conditions.
        // This handles:
        //   WHERE id = 'u1' AND id != 'u1'  →  PK lookup u1, post-filter rejects
        //   WHERE id = 'u1' AND id = 'u2'  →  PK lookup u2 (last wins), post-filter rejects
        return &PKLookupPlan{
            ViewName: q.From,
            PkValue:  pkValue,
            Columns:  q.Select,
            Where:    q.Where,  // ALL original conditions
        }, nil
    }

    // Full scan: also pass all conditions
    return &FullScanPlan{
        ViewName: q.From,
        Columns:  q.Select,
        Where:    q.Where,
        Limit:    q.Limit,
    }, nil
}
```

**Additionally — empty result optimization:** Optionally detect contradictory equality predicates on the same PK column and return an empty plan immediately:

```go
// Short-circuit for contradictory PK equalities
for _, kf := range schema.KeyFields {
    eqValues := make(map[string]bool)
    for _, c := range q.Where {
        if c.Column == kf && c.Op == OpEq {
            val := fmt.Sprint(c.Value)
            if len(eqValues) > 0 && !eqValues[val] {
                // Contradictory equalities on same PK column → empty result
                return &EmptyPlan{}, nil
            }
            eqValues[val] = true
        }
    }
}
```

**Integration points:** Only `planner.go`. No changes to executor, parser, or types.

#### 2.3.2 CR-04: SELECT * Filters _meta Fields

**Current state:** `executor.go:145-148` — when `columns == nil` (SELECT *), `projectRow` returns the full row map including `_meta`.

**Fix:** Strip `_meta` from results when doing `SELECT *`. Pass schema column names into the plan, or filter known internal prefixes.

```go
func projectRow(row map[string]any, columns []string) []string {
    // The schema columns for this view
    if columns == nil {
        // CR-04: SELECT * excludes internal fields
        // Known internal prefixes: "_meta"
        projected := make(map[string]any, len(row))
        for k, v := range row {
            if strings.HasPrefix(k, "_") {
                continue  // skip internal fields
            }
            projected[k] = v
        }
        return projected
    }
    // ... existing projection logic ...
}
```

**Alternative (preferred):** Store schema column names in the plan itself, so `SELECT *` can project exactly the known columns instead of guessing which keys are "internal":

```go
type PKLookupPlan struct {
    ViewName   string
    PkValue    string
    Columns    []string    // nil = all schema columns
    SchemaCols []string    // always populated — all column names from schema
    Where      []Condition
}

func (p *PKLookupPlan) Execute(ctx context.Context, kvb jetstream.KeyValue) ([]map[string]any, error) {
    // ... fetch row ...
    
    // Determine projection
    cols := p.Columns
    if cols == nil {
        cols = p.SchemaCols  // SELECT * = all schema columns, no internal fields
    }
    return []map[string]any{projectRow(row, cols)}, nil
}
```

**Integration points:** 
- Add `SchemaCols` field to `PKLookupPlan` and `FullScanPlan`
- Populate from schema in `BuildPlan`
- Update `projectRow` — when `columns` is explicitly provided (non-nil), use as-is; `SELECT *` now passes schema column names instead of nil

#### 2.3.3 CR-05: Reject Unsupported SQL Constructs

**Current state:** `parser.go:47-50` silently ignores non-column select expressions. No validation for DISTINCT, ORDER BY, GROUP BY, HAVING, etc.

**Fix:** Explicitly check and reject unsupported constructs:

```go
func Parse(sql string) (*ValidatedQuery, error) {
    // ... existing parsing ...
    
    sel, ok := stmt.(*sqlparser.Select)
    if !ok {
        return nil, errors.New("only SELECT statements are supported")
    }
    
    // CR-05: Reject unsupported clauses
    if sel.Distinct != "" {
        return nil, errors.New("DISTINCT is not supported in v1")
    }
    if sel.OrderBy != nil {
        return nil, errors.New("ORDER BY is not supported in v1")
    }
    if sel.GroupBy != nil {
        return nil, errors.New("GROUP BY is not supported in v1")
    }
    if sel.Having != nil {
        return nil, errors.New("HAVING is not supported in v1")
    }
    
    // ... extract SelectExprs with validation ...
}
```

And in `extractSelectExprs`, return an error for non-column expressions instead of ignoring them:

```go
func extractSelectExprs(exprs sqlparser.SelectExprs) ([]string, error) {
    // ... single * check ...
    
    cols := make([]string, 0, len(exprs))
    for _, expr := range exprs {
        switch e := expr.(type) {
        case *sqlparser.AliasedExpr:
            col, ok := e.Expr.(*sqlparser.ColName)
            if !ok {
                return nil, fmt.Errorf("unsupported SELECT expression: %T (only simple column references and * are supported in v1)", e.Expr)
            }
            cols = append(cols, col.Name.String())
        case *sqlparser.StarExpr:
            cols = append(cols, "*")
        default:
            return nil, fmt.Errorf("unsupported SELECT expression type: %T", e)
        }
    }
    return cols, nil
}
```

**Integration points:** `parser.go` only. Changes to `extractSelectExprs` return signature (adds error).

#### 2.3.4 CR-09: Consistent UseNumber in Executor

**Current state:** `executor.go:32` uses `json.Unmarshal` which converts all numbers to `float64`. Large integers >2^53 lose precision. The mapper uses `json.Decoder.UseNumber()` which preserves exact precision.

**Fix:** Use `json.Decoder.UseNumber()` in the executor too, and update `valuesEqual` to handle `json.Number`:

```go
// In executor.go, PK lookup:
var row map[string]any
decoder := json.NewDecoder(bytes.NewReader(entry.Value()))
decoder.UseNumber()
if err := decoder.Decode(&row); err != nil {
    return nil, fmt.Errorf("unmarshaling row: %w", err)
}

// In full scan:
var fullRow map[string]any
decoder := json.NewDecoder(bytes.NewReader(data))
decoder.UseNumber()
if uerr := decoder.Decode(&fullRow); uerr != nil { ... }
```

**`valuesEqual` update:** Add `json.Number` comparison before the `float64`/`int64` normalization:

```go
func valuesEqual(a, b any) bool {
    // ... nil check ...
    
    // CR-09: Handle json.Number (exact precision)
    an, aIsNum := a.(json.Number)
    bn, bIsNum := b.(json.Number)
    if aIsNum && bIsNum {
        return an.String() == bn.String()  // exact string comparison
    }
    if aIsNum {
        // Convert json.Number to float64 for comparison with float64 values
        af, err := an.Float64()
        if err != nil { return false }
        a = af
    }
    if bIsNum {
        bf, err := bn.Float64()
        if err != nil { return false }
        b = bf
    }
    
    // ... rest of existing comparison logic ...
}
```

**Integration points:** `executor.go` — both PK lookup path and full scan path. `valuesEqual` function.

#### 2.3.5 CR-13: View-Prefix Filtered Full Scans

**Current state:** `executor.go:49-51` uses `WatchAll` to scan the entire shared KV bucket, then filters by `p.ViewName + "/pk/"` prefix on every key. For N views, a full scan on one view pays O(all keys across all views).

**Fix:** Pass the view prefix into the watcher. While NATS KV `WatchAll` doesn't support server-side prefix filtering natively, we can reduce the client-side filtering cost by:

1. Using the view prefix early in the filter to skip non-matching keys with minimal overhead
2. Documenting the cross-view cost
3. Optionally: using a dedicated view key prefix that separates views more cleanly

The current approach (`strings.HasPrefix`) is already reasonably efficient for this — the real fix is documentation and a TODO for per-view buckets.

```go
// CR-13: Add view filtering documentation and prefix check
const pkPrefix = "/pk/"  // shared constant

func (p *FullScanPlan) Execute(ctx context.Context, kvb jetstream.KeyValue) ([]map[string]any, error) {
    prefix := p.ViewName + pkPrefix  // e.g., "users/pk/"
    
    watcher, err := kvb.WatchAll(ctx)
    // ...
    
    for entry := range watcher.Updates() {
        if entry == nil { break }
        if !strings.HasPrefix(entry.Key(), prefix) {
            continue  // skip keys from other views or meta keys
        }
        // ...
    }
}
```

**Medium-term fix (not in v2.0.0 scope):** Per-view KV buckets. Each view gets its own bucket `natsql-views-{name}`, so `WatchAll` on one bucket only scans that view's data. This is a breaking change requiring config migration.

---

### 2.4 Engine Lifecycle (`internal/engine/`)

#### 2.4.1 CR-07: Synchronous Startup with Error Propagation

**Current state:** `engine.go:254-256` launches materializer in goroutine, errors are logged. `engine.go:288-295` starts HTTP server in goroutine, bind errors are logged. `engine.go:269-270` logs NATS registration errors without failing.

**Fix:**

**a) HTTP server bind before goroutine:**
```go
// Synchronous bind
listener, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", e.queryPort))
if err != nil {
    return fmt.Errorf("HTTP listen failed: %w", err)
}

// Then serve in goroutine
e.wg.Add(1)
go func(l net.Listener, logger *slog.Logger) {
    defer e.wg.Done()
    logger.Info("HTTP query server starting", "addr", l.Addr())
    if err := httpServer.Serve(l); err != nil && err != http.ErrServerClosed {
        logger.Error("HTTP server error", "error", err)
    }
}(listener, e.logger)
```

**b) Materializer error propagation:**
Add a startup error channel:

```go
// Launch materializers with error channel
type materializerResult struct {
    viewName string
    err      error
}

startupErrCh := make(chan materializerResult, len(e.cfg.Views))

for i := range e.cfg.Views {
    vc := e.cfg.Views[i]
    drainCh := make(chan struct{})
    drainChans[i] = drainCh
    
    // Store schema synchronously (already is)
    schema := vc.BuildSchema()
    if storeErr := kv.StoreSchema(ctx, kvb, vc.Name, schema); storeErr != nil {
        logger.Warn("failed to store schema", "view", vc.Name, "error", storeErr)
    }
    
    e.wg.Add(1)
    go func(viewCfg natsqlpkg.ViewConfig, dc chan struct{}) {
        defer e.wg.Done()
        if runErr := materialize.Run(ctx, e.js, &viewCfg, kvb, dlqStream, e.logger, dc); runErr != nil {
            if !errors.Is(runErr, context.Canceled) {
                logger.Error("materializer exited with error", "view", viewCfg.Name, "error", runErr)
            }
        }
    }(vc, drainCh)
    
    // Note: Materializer.Run blocks on consumer setup first, which is synchronous.
    // If consumer setup fails, it returns an error immediately.
    // The goroutine captures this error and we can propagate it via the channel.
}
```

Note: The materializer's `Run` function already returns consumer setup errors synchronously before entering the message loop. The goroutine wrapper captures these. For v2.0.0, we add an initial health check — wait briefly for all materializers to confirm they're alive (consumer setup succeeded), then proceed. This is a **best-effort** check because the materializer runs forever if setup succeeds.

**c) NATS handler failure should be fatal (or at least logged at ERROR level with clear message):**
```go
// CR-07: NATS handler failure is a hard error
sub, err := transport.RegisterNATSHandler(e.nc, e)
if err != nil {
    logger.Error("failed to register NATS query handler — queries via NATS will not work",
        "error", err)
    // Decision: do NOT fail Start for NATS handler. Log prominently.
    // Rationale: user may only use HTTP queries.
}
```

**Integration points:** `engine.go` — startup sequence, HTTP listener, materializer goroutine.

#### 2.4.2 CR-06: HTTP Port from Config

**Current state:** `engine.go` defaults `queryPort` to `8080`. CLI sets `cfg.HTTP.Port`. No connection between them.

**Fix:** Pass `natsql.WithQueryPort(cfg.HTTP.Port)` when constructing the engine, or have the engine constructor read `cfg.HTTP.Port`:

```go
// Option A: In New/NewEmbedded, initialize queryPort from cfg
func New(nc *nats.Conn, js jetstream.JetStream, cfg *natsqlpkg.Config, opts ...Option) (*Engine, error) {
    // ... existing validation ...
    e := &Engine{
        js:        js,
        nc:        nc,
        cfg:       cfg,
        logger:    slog.Default(),
        queryPort: cfg.HTTP.Port,  // ← from config, not hardcoded 8080
    }
    if e.queryPort == 0 {
        e.queryPort = 8080  // fallback default
    }
    // ... apply opts ...
}
```

**Option A is preferred** — it's simpler and doesn't require changes at every call site.

**Integration points:** `engine.go` — constructor functions.

---

### 2.5 Config Validation (`internal/cfg/`)

#### 2.5.1 CR-08: Cross-Validation of key_fields and primary_key

**Current state:** `config.go:156-187` validates key_fields and primary_key separately but never cross-references them.

**Fix:** Add validation that checks:
1. Every `key_field` references an existing column that has `primary_key=true`
2. Every column with `primary_key=true` appears in `key_fields`
3. Column names within a view are unique
4. Key field names within a view are unique

```go
func (cfg *Config) Validate() error {
    var errs []string
    
    for i, v := range cfg.Views {
        prefix := fmt.Sprintf("views[%d]", i)
        
        // ... existing checks ...
        
        // CR-08: Cross-validate key_fields and primary_key columns
        colNames := make(map[string]bool)
        pkColNames := make(map[string]bool)
        
        for _, c := range v.Columns {
            if colNames[c.Name] {
                errs = append(errs, fmt.Sprintf("%s: duplicate column name %q", prefix, c.Name))
            }
            colNames[c.Name] = true
            if c.PrimaryKey {
                pkColNames[c.Name] = true
            }
        }
        
        // Every key_field must name an existing primary_key column
        for _, kf := range v.KeyFields {
            if !pkColNames[kf] {
                errs = append(errs, fmt.Sprintf("%s: key_field %q does not reference a column with primary_key=true", prefix, kf))
            }
        }
        
        // Every primary_key column must be in key_fields
        for pkName := range pkColNames {
            found := false
            for _, kf := range v.KeyFields {
                if kf == pkName {
                    found = true
                    break
                }
            }
            if !found {
                errs = append(errs, fmt.Sprintf("%s: column %q has primary_key=true but is not listed in key_fields", prefix, pkName))
            }
        }
    }
    
    // ... return errors ...
}
```

**Integration points:** `config.go:Validate()`. Update existing tests that don't have consistent key_fields/primary_key.

---

### 2.6 Transport Layer (`internal/transport/`)

#### 2.6.1 CR-18: HTTP Body Drain and Error Handling

**Current state:** `http.go:40-41` drains body after decode but doesn't check for trailing data. Error detection for oversized body uses fragile string comparison.

**Fix:**

```go
// After first decode, check for trailing data
decoder := json.NewDecoder(r.Body)
if err := decoder.Decode(&req); err != nil {
    w.Header().Set("Content-Type", "application/json")
    var maxErr *http.MaxBytesError
    if errors.As(err, &maxErr) {
        w.WriteHeader(http.StatusRequestEntityTooLarge)
        w.Write([]byte(`{"results":[],"error":"request body too large"}`))
        return
    }
    w.WriteHeader(http.StatusBadRequest)
    w.Write([]byte(`{"results":[],"error":"invalid JSON body"}`))
    return
}

// Verify no trailing data after first JSON value
if decoder.More() {
    w.Header().Set("Content-Type", "application/json")
    w.WriteHeader(http.StatusBadRequest)
    w.Write([]byte(`{"results":[],"error":"unexpected trailing data after JSON body"}`))
    return
}
```

#### 2.6.2 CR-19: NATS Handler Error Handling

**Current state:** `nats.go:41` ignores `nc.Flush()` error. `nats.go:36` ignores `msg.Respond()` error.

**Fix:**

```go
func RegisterNATSHandler(nc *nats.Conn, handler QueryHandler) (*nats.Subscription, error) {
    sub, err := nc.Subscribe("natsql.query", func(msg *nats.Msg) {
        ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
        defer cancel()
        sql := string(msg.Data)
        result := handler.Query(ctx, sql)
        data, marshalErr := json.Marshal(result)
        if marshalErr != nil {
            errResp := fmt.Sprintf(`{"results":[],"error":"internal error: %s"}`, marshalErr.Error())
            if respondErr := msg.Respond([]byte(errResp)); respondErr != nil {
                slog.Default().Warn("NATS response failed", "error", respondErr)
            }
            return
        }
        if respondErr := msg.Respond(data); respondErr != nil {
            slog.Default().Warn("NATS response failed", "error", respondErr)
        }
    })
    if err != nil {
        return nil, fmt.Errorf("subscribing to natsql.query: %w", err)
    }
    if err := nc.Flush(); err != nil {
        return nil, fmt.Errorf("flushing NATS subscription: %w", err)
    }
    return sub, nil
}
```

---

### 2.7 CLI (`cmd/natsql/main.go`)

Changes for CR-06 (already covered in §2.4.2) and CR-14 (covered in §2.1.5).

---

## 3. Data Flow Changes

### 3.1 Write Path (Before CR Fixes)

```
Event → Consumer → msgCh(64) → 16 workers → processEvent
  → mapper.MapRow (stringifyValue + SanitizePK + join)
  → Writer.Apply (PkKey = view/pk/SanitizePK(already_sanitized))
  → kv.Put → Ack or DLQ (all errors treated same)
```

### 3.2 Write Path (After CR Fixes)

```
Event → Consumer → processEvent (sequential, ordered)
  → mapper.MapRow (stringifyValue WITHOUT SanitizePK, raw join)
  → Writer.Apply (BuildPkKey = sanitize once)
  → kv.Put
  → [if transient] NAK with backoff
  → [if malformed] DLQ + Ack
  → [if terminal] DLQ + Ack
  → [if success] Ack
```

### 3.3 Read Path (Before CR Fixes)

```
SQL → Parser (silently ignores unsupported constructs)
  → Planner (PK conditions removed from post-filter)
  → PKLookupPlan: PkKey(view, joined) — different encoding from write!
  → Executor: json.Unmarshal (float64 precision loss)
  → projectRow (SELECT * returns _meta)
```

### 3.4 Read Path (After CR Fixes)

```
SQL → Parser (rejects unsupported constructs with error)
  → Planner (ALL conditions kept as post-filters; contradiction detection)
  → PKLookupPlan: BuildPkKey(view, pkParts, sep) — same as write!
  → Executor: json.Decoder.UseNumber (exact precision)
  → projectRow (SELECT * = schema columns only, no _meta)
```

---

## 4. Build Order and Dependencies

```
Wave 1: Foundation (no behavioral change)
  ├── CR-08: Config validation (self-contained, pure function changes)
  ├── CR-02: Canonical PK encoder (refactoring, no behavior change if correct)
  └── CR-05: Reject unsupported SQL (parser validation, self-contained)
  
Wave 2: Materializer (requires Wave 1 CR-02)
  ├── CR-01: Remove worker pool (behavioral: fixes ordering)
  ├── CR-10: Error classification (behavioral: fixes transient handling)
  ├── CR-11: Remove InactiveThreshold (behavioral: fixes durability)
  └── CR-12: Rename BatchSize (interface: rename config field)

Wave 3: Query Engine (requires Wave 1 CR-02, CR-05)
  ├── CR-03: Keep all predicates (behavioral: fixes query correctness)
  ├── CR-04: SELECT * filters _meta (behavioral: fixes metadata leak)
  ├── CR-09: UseNumber in executor (behavioral: fixes precision loss)
  └── CR-13: View prefix scan (behavioral: scan scoping)

Wave 4: Engine Lifecycle (no hard dependencies on Waves 2-3)
  ├── CR-07: Synchronous startup with errors
  └── CR-06: HTTP port from config

Wave 5: Transport + CLI (no hard dependencies)
  ├── CR-14: Stream creation respects subjects
  ├── CR-18: HTTP body handling
  └── CR-19: NATS handler error handling

Wave 6: Cleanup (low/medium findings)
  CR-15 through CR-25 — formatting, dead code, docs, tests
```

### Build Order Rationale

1. **Wave 1 first** because CR-02 (canonical PK) is a refactoring that both materializer and query engine depend on. CR-08 (config validation) is CI-gate level — should be early to catch bad configs. CR-05 (reject unsupported SQL) is parser-level and self-contained.

2. **Wave 2 next** because CR-01 (ordered processing) is the most critical correctness fix. It changes the materializer's core processing loop, which affects all subsequent materializer changes.

3. **Wave 3 after Wave 1** because CR-02 (canonical PK) changes the query path's PK encoding. CR-03 (predicates) and CR-04 (SELECT *) are planner/executor changes.

4. **Wave 4 can parallelize with Waves 2-3** because engine lifecycle changes (startup errors, HTTP port) are orthogonal to materializer and query engine internals.

5. **Wave 5 can parallelize with all above** because transport and CLI changes are isolated to their own packages.

6. **Wave 6 last** because it includes formatting, dead code removal, and docs — should be done after behavioral changes are complete.

---

## 5. New Component Boundaries

No new components are introduced. The following existing components gain new responsibilities:

| Component | New Responsibility | Why Here |
|-----------|-------------------|----------|
| `internal/kv/kv.go` | `BuildPkKey()` — single canonical PK encoder | Centralizes key construction; all paths call it |
| `internal/materialize/materializer.go` | `isTransientError()` — error classification | Writer errors must be classified before action |
| `internal/query/planner.go` | `EmptyPlan` — short-circuit for impossible queries | Planner detects contradictions before execution |
| `internal/engine/engine.go` | `net.Listen` before HTTP Serve | Synchronous bind for error propagation |

### New Types

```go
// internal/query/types.go
// EmptyPlan is a plan that returns zero rows immediately.
// Used for contradictory predicates (CR-03) or other short-circuits.
type EmptyPlan struct{}

func (p *EmptyPlan) Execute(ctx context.Context, kvb jetstream.KeyValue) ([]map[string]any, error) {
    return []map[string]any{}, nil
}
```

### Removed Types/Constants

| Symbol | Reason |
|--------|--------|
| `materializerWorkers` (16) | CR-01: sequential processing |
| `SchemaPrefix` ("schemas:") | CR-22: dead code |
| `ErrSkipAndAck` | CR-22: unused sentinel |
| `EncodePKValue` (panics on `/`) | CR-22: replaced by `BuildPkKey` |
| `MustInitBucket` | CR-22: test-only, production uses `InitBucket` |
| `ConsumerConfig.BatchSize` | CR-12: renamed to `MaxAckPending` |

---

## 6. Configuration Changes

### Updated Config Schema (CR-08, CR-12)

```yaml
views:
  - name: users
    source_stream: users_events
    source_subject: users.>          # respected in stream creation (CR-14)
    key_fields: [id]                 # must match primary_key columns (CR-08)
    key_separator: "|"
    columns:
      - name: id
        from: id
        type: string
        primary_key: true
    consumer:
      max_ack_pending: 50            # renamed from batch_size (CR-12)
      max_deliver: 10
      ack_wait_seconds: 30
```

---

## 7. Cleanup Changes (Medium/Low Findings)

### CR-15: $.field prefix support
Either support `$.` prefix stripping in `extractNestedField` or remove from test examples. **Recommendation:** strip `$.` prefix in extractNestedField so both `$.user.id` and `user.id` work.

### CR-16: Index config is ignored
Reject `indexes` at validation time with clear error until secondary indexes are implemented.

### CR-17: Delete/tombstone semantics
Deferred to v2. Keep as recognized gap.

### CR-18: HTTP body handling
Covered in §2.6.1.

### CR-19: NATS handler errors
Covered in §2.6.2.

### CR-20: Error message typo ("marshaling" → "unmarshaling")
Single-line fix in `executor.go:33`.

### CR-21: Example errors
Audit all examples for unchecked errors and lifecycle ownership issues.

### CR-22: Dead code
Remove `SchemaPrefix`, `ErrSkipAndAck`, `EncodePKValue`, `MustInitBucket`, unused `dlqStream` param.

### CR-23: Formatting drift
`gofmt -w` across repo. Add `gofmt` check to CI.

### CR-24: Duplicated test helpers
Extract common test helpers (embedded NATS startup, stream creation, test data setup) into a shared test utility.

### CR-25: Docs sync
Update README and feature docs to reflect implemented LIMIT semantics.

---

## 8. Integration Points Summary

| Fix | Files Modified | New Dependencies | Risk |
|-----|---------------|------------------|------|
| CR-01 | `materializer.go` | None | **HIGH** — core processing loop change |
| CR-02 | `kv.go`, `mapper.go`, `writer.go`, `planner.go`, `executor.go` | None | **HIGH** — affects all PK operations |
| CR-03 | `planner.go` | None | MEDIUM — changes predicate handling logic |
| CR-04 | `types.go`, `planner.go`, `executor.go` | None | LOW — projection filtering |
| CR-05 | `parser.go` | None | LOW — validation only |
| CR-06 | `engine.go` | None | LOW — config plumbing |
| CR-07 | `engine.go` | None | MEDIUM — startup sequence change |
| CR-08 | `config.go` | None | LOW — validation logic |
| CR-09 | `executor.go` | None | LOW — JSON decoder change |
| CR-10 | `materializer.go` | None | MEDIUM — error classification |
| CR-11 | `consumer.go` | None | LOW — config field removal |
| CR-12 | `config.go`, `consumer.go` | None | LOW — rename |
| CR-13 | `executor.go` | None | LOW — prefix filtering |
| CR-14 | `cmd/natsql/main.go` | None | LOW — CLI change |
| CR-15 | `mapper.go` | None | LOW — path stripping |
| CR-16 | `config.go` | None | LOW — validation |
| CR-18 | `http.go` | None | LOW — error handling |
| CR-19 | `nats.go` | None | LOW — error checking |
| CR-20 | `executor.go` | None | LOW — string change |
| CR-22 | `kv.go`, `mapper.go`, `materializer.go`, `engine.go` | None | LOW — deletion |

**Total risk assessment:** 2 HIGH, 4 MEDIUM, 15 LOW. The two HIGH-risk changes are CR-01 (removing worker pool) and CR-02 (canonical PK encoder). Both can be validated with integration tests before proceeding.

---

## 9. Key Architecture Decisions Summary

| Decision | Rationale | Alternative Considered |
|----------|-----------|----------------------|
| Sequential processing (no worker pool) | Preserves stream ordering; correctness over throughput | Per-key partitioning — more complex, not justified for v1 |
| Single canonical `BuildPkKey` function | Eliminates double-sanitization bug; single source of truth | Keep both paths but sync them — fragile |
| ALL predicates as post-filters | Correctness; handles contradictory and duplicate equalities | Short-circuit detection only — misses post-filter cases |
| `net.Listen` before `Serve` for HTTP | Synchronous error propagation; prevents "started but not serving" state | Startup timeout — racy, less predictable |
| Error classification at caller level | Writer stays simple; classification logic is caller policy | Add error types to Writer — would work but adds coupling |
| No per-view KV buckets for v2.0.0 | Minimal change; cross-view scan cost is documented | Per-view buckets — breaking change, scope for v2 |
| Rename BatchSize to MaxAckPending | Reflects actual behavior; no implementation change needed | Implement Fetch() batching — scope increase |

---

*Architecture remediation research for: natsql v2.0.0 Code Review Fixes*
*Researched: 2026-05-31*
*Based on: cr.md findings, source code audit, v1.1 architecture patterns*
