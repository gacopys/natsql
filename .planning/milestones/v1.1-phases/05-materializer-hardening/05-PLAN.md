# Phase 5: Materializer Hardening — PLAN

**Goal:** KV keys are safe from special characters; DLQ failures are surfaced; startup is resilient; large integers preserve precision.

**Depends on:** Nothing (fixes to existing v1.0 code)

**Requirements:** FIX-MAT-01, FIX-MAT-02, FIX-MAT-03, FIX-MAT-04

## Tasks

### Task 1: PK value sanitization (FIX-MAT-01) — `materialize/mapper.go`

**Bug (H1 from 01-REVIEW.md):** `stringifyValue` at mapper.go:202 produces PK values containing `/`, `*`, `>`, `.` that have special meaning in NATS subject-based KV keys. These can break key prefix matching in `ListKeys()` and cause injection-like behavior.

**Fix plan:**
1. Add a `sanitizePK` helper that encodes dangerous characters using URL-safe encoding:
   - `/` → `%2F`
   - `*` → `%2A`
   - `>` → `%3E`
2. Apply `sanitizePK` to each PK part before joining in `MapRow()` (at mapper.go:95)
3. Also add sanitization in `kv.PkKey()` as a defense-in-depth safety net

**Verification:**
- PK value `"foo/bar"` produces KV key `users/pk/foo%2Fbar` instead of `users/pk/foo/bar`
- PK value `"test*>data"` produces KV key with `%2A%3E`
- `FullScanPlan.Execute` prefix filtering still works correctly
- Existing tests pass

### Task 2: DLQ publish failure surfacing (FIX-MAT-02) — `materialize/materializer.go`

**Bug (H2 from 01-REVIEW.md):** `publishToDLQ` at materializer.go:53 logs DLQ publish errors but doesn't return them. The caller in `Run()` ack's the event regardless. A DLQ failure means the event is silently lost.

**Fix plan:**
1. Change `publishToDLQ` to return an `error` instead of logging internally
2. In `Run()`, after calling `publishToDLQ`, check the error:
   - If DLQ publish succeeds: ack the original event (current behavior)
   - If DLQ publish fails: **do NOT ack the original event** — instead, Nak it with delay for retry
3. Log the DLQ failure at error level with full context

**Verification:**
- Normal case: DLQ publish succeeds → original event is acked (no change)
- DLQ failure: original event is NOT acked → JetStream redelivers
- Existing tests pass

### Task 3: Partial init cleanup (FIX-MAT-03) — `engine/engine.go`

**Bug (H3 from 01-REVIEW.md):** `Engine.Start()` at engine.go:227 sets `e.kv` early (line 237) but if a later step fails (DLQ stream creation, schema store), the partially-initialized state is left with `e.kv` set but no materializers running.

**Fix plan:**
1. Restructure `Start()` to defer cleanup on failure:
   - Initialize resources (kv, dlq stream) before modifying engine state
   - If any step after kv init fails, clean up by setting `e.kv = nil`
2. Alternatively, use a clean-up-on-failure pattern: if step N fails, undo steps 1..N-1
3. The key invariant: on `Start()` failure, `Engine` state should be indistinguishable from before `Start()` was called

**Verification:**
- If DLQ stream creation fails after KV init, `e.kv` is nil (clean state)
- If materializer launch fails, engine state is clean
- Existing tests pass (including lifecycle tests)

### Task 4: JSON integer precision (FIX-MAT-04) — `materialize/mapper.go`

**Bug (M4 from 01-REVIEW.md):** JSON `float64` loses precision for integers > 2^53. Go's `json.Unmarshal` decodes all numbers as `float64`, which can't represent integers above 2^53 exactly.

**Fix plan:**
1. Use `json.Decoder` with `UseNumber()` in `MapRow()` instead of `json.Unmarshal` to decode numbers as `json.Number` (string-backed)
2. In `validateType` for `ColumnTypeNumber`, convert `json.Number` to `float64` — but also keep the original string representation for PK construction
3. PK construction in `stringifyValue` should handle `json.Number` by using its string representation directly

**Note:** The practical impact is limited since NATS KV values are JSON strings, so the number will be stored as `{"age": 9007199254740993}` in the KV value. When read back by the query engine, it will decode to `float64` again — so queries comparing large numbers may still have precision issues. This fix ensures the stored value preserves the original representation exactly.

**Verification:**
- Event with `"big_number": 9007199254740993` is stored and retrieved with exact value
- KV value shows `9007199254740993` not `9007199254740992`
- Existing tests pass

## Success Criteria

1. PK values with `/*>.` are sanitized into safe KV keys
2. DLQ publish failure blocks event acknowledgement — consumer doesn't advance
3. Failed `Engine.Start` cleans up all partially-created resources
4. Integers > 2^53 retain exact value in stored state

## Files to modify

| File | Task | Change |
|------|------|--------|
| `materialize/mapper.go` | 1, 4 | PK sanitization + json.Number support |
| `materialize/materializer.go` | 2 | Return error from publishToDLQ, Nak on DLQ failure |
| `kv/kv.go` | 1 | Sanitize in PkKey (defense in depth) |
| `engine/engine.go` | 3 | Clean up on partial Start failure |
