---
phase: 03-packaging-deployment
status: issues-found
reviewed: 2026-05-28
findings: 22 (2 high, 4 medium, 7 low, 9 quality)
---

# Code Review: Phase 03 — Packaging + Deployment

## Overall Health: ISSUES FOUND

2 high, 4 medium, 7 low, 9 quality findings.

## High

| # | File:Line | Issue |
|---|-----------|-------|
| B-01 | `engine/engine.go:59,108-114` | `WithNATSOptions` sets `storeDir` that is never consumed |
| B-03 | `engine/engine.go:288-291` | HTTP server missing read/write timeouts |

## Medium

| # | File:Line | Issue |
|---|-----------|-------|
| B-04 | `transport/http.go:23` | No request body size limit |
| B-05 | `engine/engine.go:118-127` | `netSplitHostPort` loses original error |
| B-07 | `materialize/materializer.go:129-178` | Bridge/monitor goroutines not lifecycle-tracked |
| S-01 | `transport/nats.go:26` | NATS handler uses unbounded `context.Background()` |

## Low + Quality

B-02, B-06, B-08, CQ-01 through CQ-07: Dead code, duplicated schema writes, test flakiness, unused params, NoLog discarding server diagnostics.

## Recommendations

1. **Fix B-03**: Add read/write/idle timeouts to HTTP server
2. **Fix B-04**: Add `http.MaxBytesReader` to HTTP endpoint
3. **Fix B-01**: Wire `storeDir` from `WithNATSOptions` or remove it
4. **Fix S-01**: Add 30s timeout context to NATS handler
5. **Fix B-07**: Track bridge/monitor goroutines with WaitGroup
