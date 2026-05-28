---
phase: 01-foundation-materializer
status: issues-found
reviewed: 2026-05-28
findings: 17 (3 high, 5 medium, 9 low)
---

# Code Review: Phase 01 — Foundation — Materializer

## Overall Health: ISSUES FOUND

3 high, 5 medium, 9 low — see findings below.

## High Severity

| # | File:Line | Issue | Category |
|---|-----------|-------|----------|
| H1 | `materialize/mapper.go:200-218` | PK values not sanitized for KV key safety — `/`, `*`, `>`, `.` have special meaning in NATS subject-based KV keys | Security / Bug |
| H2 | `materialize/materializer.go:53-71` | `publishToDLQ` swallows publish errors silently; event is acked anyway | Reliability / Data loss |
| H3 | `engine/engine.go:103-114` | Partial init on `Start` failure — `e.kv` set but cleanup path broken | Resource management |

## Medium Severity

| # | File:Line | Issue |
|---|-----------|-------|
| M1 | `kv/kv.go:103-105` | `EncodePKValue` panics on special characters (dead code, no caller) |
| M2 | `materialize/materializer.go:119-140` | Bridge goroutine with unbounded retry loop on non-terminal errors |
| M3 | `materialize/materializer.go:148` | Dead `dlqStream` parameter — accepted but unused |
| M4 | `materialize/mapper.go:67` | JSON `float64` precision loss for integers >2^53 |
| M5 | `config.go:76` | No file size limit on config load |
| M6 | `materialize/materializer.go:209-214` | `getMsgSeq` silently discards metadata errors |

## Low Severity

L1-L9: Dead code, test duplication, fragile mocks, hardcoded URLs, missing liveness checks.

## Recommendations

1. **Fix H1 first**: Sanitize PK values in `stringifyValue` before Phase 2
2. Fix H2: Surface DLQ publish failures instead of acking
3. Fix H3: Fix partial-init path in `engine.go`
4. Extract duplicated `startEmbeddedNATS` into shared test package
5. Remove dead `dlqStream` parameter from `materialize.Run`
