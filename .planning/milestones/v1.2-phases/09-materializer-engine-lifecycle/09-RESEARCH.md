# Phase 9: Materializer & Engine Lifecycle — Research

**Researched:** 2026-06-01
**Source:** Distilled from `.planning/research/ARCHITECTURE.md`, `.planning/research/PITFALLS.md`, `.planning/research/FEATURES.md`

## Scope

This phase covers CR-01, CR-06, CR-07, CR-10, CR-11, CR-12 — six code review findings clustered around materializer correctness (ordered processing, error classification, consumer durability) and engine lifecycle (HTTP port wiring, startup error propagation).

---

## 1. Ordered Processing (MAT-01 / CR-01)

### Current State
`materializer.go:20` defines `materializerWorkers = 16`. A bridge goroutine feeds messages from the consumer into a buffered channel. 16 worker goroutines drain that channel concurrently, calling `processEvent`. This destroys JetStream's per-subject ordering guarantee — two events for the same PK can be processed out of order.

### Architecture (from ARCHITECTURE.md §2.1.1)
```
Before (broken ordering):
  consumer.Messages().Next() → msgCh(64) → 16 workers → processEvent → kv.Put

After (ordered):
  consumer.Messages().Next() → processEvent (sequential, in bridge goroutine)
```

### Key Design Decisions (from CONTEXT.md D-01 through D-04)
- Remove 16-goroutine worker pool entirely
- Single goroutine per view processes messages sequentially
- Bridge goroutine removed — consumer's Messages() drives processing directly
- Heartbeat logging goroutine kept (separate concern)
- No throughput optimization until benchmarked

### Integration Points
- Remove `materializerWorkers` constant, `workerWg`, worker goroutine loop, `sem` pattern
- Process messages in the receive loop directly

---

## 2. Error Classification (MAT-02 / CR-10)

### Current State
All writer failures in `processEvent` are treated identically: publish to DLQ + Ack. Transient NATS/KV failures permanently drop valid events.

### Architecture (from ARCHITECTURE.md §2.1.2)
Three error categories:
- **Malformed** (mapper errors) → DLQ + Ack (already correct)
- **Transient** (timeout, connection refused, no leader) → NAK with backoff
- **Terminal** (key too long, value too large, bad config) → DLQ + Ack

### Error Classification Table
| Error Source | Classification | Action |
|-------------|---------------|--------|
| `mapper.MapRow()` → malformed | Malformed | DLQ + Ack |
| Context cancellation | Transient | NAK |
| NATS connection error | Transient | NAK |
| KV timeout | Transient | NAK |
| Key validation error | Terminal | DLQ + Ack |
| `publishToDLQ()` fails | Transient | NAK (original msg not acked) |

### Key Design Decisions (from CONTEXT.md D-05 through D-07)
- Standard classification: transient → NAK, terminal → DLQ + Ack
- `classifyWriteError(err error) errorClass` helper function
- Classification at caller site in `processEvent`, not in Writer

---

## 3. Consumer Durability (MAT-03 / CR-11)

### Current State
`consumer.go:53` sets `InactiveThreshold: 1 * time.Hour`. If engine is down >1h, NATS deletes the durable consumer. On restart, consumer recreated with `DeliverAllPolicy`, replaying entire stream.

### Fix (from ARCHITECTURE.md §2.1.3)
Remove `InactiveThreshold` from durable consumer config entirely. Durable consumers persist until explicitly deleted.

### Integration Points
Single line removal in `consumer.go:53`. Update tests that assert InactiveThreshold.

---

## 4. BatchSize Rename (MAT-04 / CR-12)

### Current State
ConsumerConfig.BatchSize influences MaxAckPending (= batchSize * 2) but does NOT control fetch batch size. The materializer uses `Messages()` → repeated `Next()`, so "batch size" is misleading.

### Fix (from ARCHITECTURE.md §2.1.4)
Rename `batch_size` config field to `max_ack_pending` to match actual behavior. Old `batch_size` accepted with deprecation warning.

### Integration Points
Config struct in `internal/cfg/config.go`, consumer setup in `internal/materialize/consumer.go`, all test fixtures referencing `BatchSize`.

---

## 5. HTTP Port Wiring (LIFE-01 / CR-06)

### Current State
`engine.go` defaults `queryPort` to `8080`. CLI sets `cfg.HTTP.Port` but engine constructors never read it.

### Fix (from ARCHITECTURE.md §2.4.2)
Initialize `queryPort` from `cfg.HTTP.Port` in constructors. Default `cfg.HTTP.Port == 0` → 8080.

---

## 6. Startup Error Propagation (LIFE-02 / CR-07)

### Current State
Three failure modes:
1. HTTP server: `Serve()` in goroutine → bind errors logged only
2. Materializer: `Run()` in goroutine → errors logged only
3. NATS handler: registration errors logged only

All three let `Start()` return nil while core services are down.

### Fix (from ARCHITECTURE.md §2.4.1)
- HTTP: `net.Listen` BEFORE `Serve` goroutine (synchronous bind error propagation)
- Materializer: consumer setup errors propagate via channel (or make synchronous)
- NATS handler failure: logged prominently, does NOT block Start (degraded mode)
- `e.started = true` only after ALL steps succeed

### Integration Points
`engine.go` — startup sequence, HTTP listener binding, materializer goroutine.

---

## Source References

- `.planning/research/ARCHITECTURE.md` §2.1, §2.4 — Full architecture design
- `.planning/research/PITFALLS.md` — CR-01, CR-06, CR-07, CR-10, CR-11, CR-12 pitfall analysis
- `.planning/research/FEATURES.md` — CORR-01, CORR-06, CORR-07, CORR-10, CORR-11, CORR-12
- `cr.md` — Original code review findings
- `.planning/phases/09-materializer-engine-lifecycle/09-CONTEXT.md` — Locked decisions D-01 through D-16
