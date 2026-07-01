# Phase 10: Query Engine & Transport — Research

**Researched:** 2026-06-02
**Domain:** Query engine correctness (predicate handling, meta filtering, numeric precision, scan architecture), transport robustness (CLI stream creation, HTTP body handling, NATS error surfacing)
**Confidence:** HIGH

## Summary

Phase 10 fixes 8 distinct correctness and robustness bugs in the query engine and transport layers, all sourced from the code review (`cr.md`). The changes span four files in `internal/query/` (planner, executor, types), two files in `internal/transport/` (HTTP, NATS), and the CLI `cmd/natsql/main.go`.

**Work breakdown:** The 8 requirements form two logical groups:
- **Query engine fixes** (QENG-01–04, TRN-04): Modify `planner.go`, `executor.go`, `types.go` — PK post-filter, short-circuit contradictory predicates, `_meta` stripping, `json.Decoder.UseNumber()`, `valuesEqual` for `json.Number`, `WatchAll`→`Watch(prefix)`, fix error message typo
- **Transport fixes** (TRN-01–03): Modify `cmd/natsql/main.go` for CLI stream creation, `http.go` for `errors.As` + trailing data check, `nats.go` for Flush/Respond error handling

**Primary recommendation:** Implement in two waves — query engine first (touches core correctness), then transport (touches CLI and API layers). Each requirement has high-confidence decisions from CONTEXT.md; no exploratory research needed.

## User Constraints (from CONTEXT.md)

<user_constraints>
### Locked Decisions

#### PK Predicate Post-Filter (QENG-01 / CR-03)
- **D-01:** Keep ALL original WHERE conditions as post-filters. The PK lookup narrows the search space; post-filters verify every condition
- **D-02:** Add short-circuit optimization: if the same PK column has multiple OpEq conditions with different values (e.g., `WHERE id = 'a' AND id = 'b'`), return an empty plan immediately (no KV lookup needed)
- **D-03:** The `findPKEqConditions` still collects PK equality conditions for the lookup key, but `BuildPlan` no longer removes matching conditions from the post-filter list

#### Meta Field Exclusion (QENG-02 / CR-04)
- **D-04:** `projectRow` for `SELECT *` (nil columns) strips all map keys starting with `_` before returning
- **D-05:** This catches `_meta`, `_stream_seq`, and any future internal fields automatically
- **D-06:** Explicit column selection (`SELECT col1, col2`) is unchanged — only `SELECT *` applies the filter

#### Numeric Precision (QENG-03 / CR-09)
- **D-07:** Switch `PKLookupPlan.Execute` to use `json.NewDecoder(resp.Data).UseNumber()` instead of `json.Unmarshal`
- **D-08:** Switch `FullScanPlan.Execute` to use `json.NewDecoder(bytes.NewReader(data)).UseNumber()` instead of `json.Unmarshal`
- **D-09:** Add `json.Number` comparison to `valuesEqual`: extract string value, if no decimal point → parse as int64 and compare against int64/float64; if has decimal → parse as float64
- **D-10:** WHERE clause literal values (from the parser) remain as `int64`/`string`/`bool`/`float64` — the comparison logic handles cross-type matching

#### Full-Scan Architecture (QENG-04 / CR-13)
- **D-11:** Replace `WatchAll` with `Watch(prefix)` using `viewName/pk/` prefix — limits KV watch scope to the target view's keys only
- **D-12:** The 16-worker full scan pool is kept for concurrent unmarshal/filter/projection

#### CLI Stream Creation (TRN-01 / CR-14)
- **D-13:** CLI only creates streams in embedded mode (engine owns them). In external mode, warn if stream doesn't exist — let consumer setup fail with clear error
- **D-14:** Add `--create-streams` flag for explicit opt-in in external mode. Respects `source_subject` when creating streams
- **D-15:** Do NOT mutate existing external streams' subject sets

#### HTTP Body Handling (TRN-02 / CR-18)
- **D-16:** Use `*http.MaxBytesError` via `errors.As` for body-size detection (replace string comparison)
- **D-17:** After first JSON decode, drain remaining body bytes. If any non-whitespace data remains, reject with 400 Bad Request
- **D-18:** Implementation: drain into buffer, `strings.TrimSpace` on result, reject if non-empty

#### NATS Error Handling (TRN-03 / CR-19)
- **D-19:** `RegisterNATSHandler` checks and returns `nc.Flush()` error after subscription
- **D-20:** Message handler logs `msg.Respond()` errors (can't return from callback)

#### Error Message Fix (TRN-04 / CR-20)
- **D-21:** Fix `executor.go:33` — change `"marshaling row"` to `"unmarshaling row"`

### the agent's Discretion
- No discretion areas — all decisions locked by discussion phase

### Deferred Ideas (OUT OF SCOPE)
- None — discussion stayed within phase scope
</user_constraints>

<phase_requirements>
## Phase Requirements

| ID | Description | Research Support |
|----|-------------|------------------|
| QENG-01 | All WHERE conditions retained as post-filters (CR-03) | Decisions D-01/02/03 fully specify; planner.go:38-44 identifies exact removal site; `types.go` `PKLookupPlan.Where` field already exists |
| QENG-02 | `SELECT *` excludes `_meta` fields (CR-04) | Decisions D-04/05/06 fully specify; executor.go:145-148 identifies `projectRow` return path; writer.go:39-43 confirms `_meta` storage pattern |
| QENG-03 | `json.Decoder.UseNumber()` for precision (CR-09) | Decisions D-07/08/09/10 fully specify; executor.go:32 and executor.go:97 identify Unmarshal call sites; materializer/mapper.go:66 confirms existing `UseNumber` pattern |
| QENG-04 | Full-scan uses prefix watch (CR-13) | Decisions D-11/12 fully specify; executor.go:51 identifies `WatchAll` site; kv.go:95 confirms key format `viewName/pk/` |
| TRN-01 | CLI stream creation respects `source_subject` (CR-14) | Decisions D-13/14/15 fully specify; main.go:135-137 identifies stream creation; config.go:83 has `SourceSubject` field |
| TRN-02 | HTTP uses `errors.As` and rejects trailing data (CR-18) | Decisions D-16/17/18 fully specify; http.go:31 has string comparison; http.go:40-42 drains without check |
| TRN-03 | NATS checks Flush/Respond errors (CR-19) | Decisions D-19/20 fully specify; nats.go:41 ignores Flush error; nats.go:36 ignores Respond error |
| TRN-04 | Error message typo fix (CR-20) | Decision D-21 fully specifies; executor.go:33 has the exact string |
</phase_requirements>

## Standard Stack

### Core
| Library | Version | Purpose | Why Standard |
|---------|---------|---------|--------------|
| Go `encoding/json` | stdlib | JSON decode with `UseNumber` | Decision D-07/D-08; the materializer already uses `decoder.UseNumber()` in mapper.go:66 [VERIFIED: mapper.go L66] |
| Go `net/http` | stdlib | HTTP server, `MaxBytesError`, `errors.As` | Decision D-16; `*http.MaxBytesError` is the standard Go mechanism for body-size limit detection [VERIFIED: stdlib] |
| Go `strings` | stdlib | TrimSpace for trailing data check | Decision D-18 [VERIFIED: stdlib] |
| Go `bytes` | stdlib | `bytes.NewReader` for json.NewDecoder | Decision D-08 for full scan path [VERIFIED: stdlib] |

### Supporting
| Library | Version | Purpose | When to Use |
|---------|---------|---------|-------------|
| `github.com/nats-io/nats.go` | v1.51+ | NATS `Conn.Flush()` error checking | TRN-03: checking Flush and Respond errors [VERIFIED: nats.go] |
| `github.com/nats-io/nats.go/jetstream` | v1.51+ | `KeyValue.Watch()` prefix filtering | QENG-04: replacing `WatchAll` with `Watch(prefix)` [VERIFIED: executor.go] |

## Architecture Patterns

### Query Engine Predicate Architecture
**Current (broken):** `planner.go:38-44` — `BuildPlan` removes PK equality conditions from post-filter list, passing only non-PK conditions to `PKLookupPlan.Where`. This means contradictory or duplicate PK predicates are not checked post-lookup.

**Target (fixed per D-01/D-03):** Pass ALL original WHERE conditions (`q.Where`) to `PKLookupPlan.Where`. The PK conditions still drive the lookup key via `findPKEqConditions`, but every condition also runs through post-filter `filterRow`.

### Short-Circuit Contradictory Predicates (D-02)
**Logic:** In `BuildPlan`, after collecting `pkConditions`, check for the same PK column appearing multiple times with different OpEq values. If found, return an empty plan immediately (no KV I/O). The check occurs before constructing the `PKLookupPlan`.

**Implementation approach for same-column detection:**
```go
// In BuildPlan, before returning PKLookupPlan:
for col, cond := range pkConditions {
    // Count how many OpEq conditions exist for this PK column
    eqCount := 0
    var firstValue any
    for _, c := range q.Where {
        if c.Column == col && c.Op == OpEq {
            eqCount++
            if eqCount == 1 {
                firstValue = c.Value
            }
        }
    }
    if eqCount > 1 {
        // Check if all values are the same — if so, deduplicate and continue
        // Check if values differ — if so, return empty result
    }
}
```

### Projection Architecture
**Current (`projectRow`, executor.go:145-148):** `SELECT *` (nil columns) returns the raw row map as-is, including `_meta` and any future internal fields.

**Target (per D-04/D-05):** When `columns == nil` (SELECT *), copy only non-`_`-prefixed keys from the row. When `columns` is non-nil, behavior is unchanged (explicit column selection already filters by name).

### Scan Architecture
**Current (executor.go:51):** `kvb.WatchAll(ctx)` streams ALL keys from the shared bucket, then client-side filters with `strings.HasPrefix(entry.Key(), prefix)`.

**Target (per D-11):** `kvb.Watch(ctx, viewName+"/pk/")` scopes the KV watch to the target view's keys only. The prefix watch is the JetStream KV API's `Watch()` method with a key prefix pattern. [VERIFIED: jetstream.KeyValue interface has `Watch(ctx, keyPattern)`]

### Transport Architecture
**HTTP handler pattern (current `http.go`):**
1. `http.MaxBytesReader` wraps body
2. `json.NewDecoder(r.Body).Decode(&req)` — decodes JSON
3. String comparison `err.Error() == "http: request body too large"` — fragile
4. `io.Copy(io.Discard, r.Body)` — drains without checking for trailing data

**Target (per D-16/D-17/D-18):**
1. Same `MaxBytesReader` setup
2. Same JSON decode
3. `errors.As(err, &http.MaxBytesError{})` for body-size detection
4. After successful decode: drain body into buffer, `strings.TrimSpace`, reject if non-empty

**NATS handler pattern (current `nats.go`):**
1. Subscribe to `natsql.query`
2. `nc.Flush()` error ignored at line 41
3. `msg.Respond()` error ignored at line 36

**Target (per D-19/D-20):**
1. Return `nc.Flush()` error from `RegisterNATSHandler`
2. Log `msg.Respond()` error in message handler

### Recommended Project Structure
```
internal/
├── query/
│   ├── executor.go    # Modified: UseNumber, projectRow _meta filter, Watch prefix, valuesEqual json.Number, error msg
│   ├── planner.go     # Modified: keep all WHERE as post-filters, short-circuit contradictory PK preds
│   ├── types.go       # No changes needed (PKLookupPlan already has Where field for all conditions)
│   └── ...
├── transport/
│   ├── http.go        # Modified: errors.As, trailing data check
│   └── nats.go        # Modified: Flush/Respond error handling
cmd/
└── natsql/
    └── main.go        # Modified: CLI stream creation respects source_subject, --create-streams flag
```

### Anti-Patterns to Avoid
- **Removing PK conditions from post-filter:** The bug that caused CR-03. Never assume PK lookup alone is sufficient — post-filter must verify every condition.
- **String comparison for sentinel errors:** Using `err.Error() == "text"` is fragile and breaks if Go changes error messages. Always use `errors.Is`/`errors.As`.
- **Silently discarding drained body bytes:** Accepting trailing non-whitespace data after JSON is a security concern (request smuggling). Always validate.

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| Body-size limit detection | String comparison on error text | `errors.As(err, &http.MaxBytesError{})` | Stdlib provides typed error; string comparison is fragile per CR-18 |
| JSON number parsing | Manual float64 with precision loss | `json.NewDecoder().UseNumber()` | Standard Go pattern; materializer already uses it (mapper.go:66) |
| NATS subscription error handling | Ignoring nc.Flush() | Check returned error | Flush confirms subscription propagated; ignoring hides failures |
| KV key stream filtering | WatchAll + client-side prefix filter | `Watch(prefix)` scoped to view | Reduces I/O; avoids scanning all views' keys per CR-13 |

## Common Pitfalls

### Pitfall 1: Per-view Bucket Assumption vs Shared Bucket
**What goes wrong:** QENG-04 replaces `WatchAll` with `Watch(prefix)`, but the project uses a single shared KV bucket (`DefaultBucket = "natsql-views"`). The prefix watch only reduces I/O within the shared bucket — all views still share one bucket namespace.
**Why it happens:** The keys are namespaced per view (`viewName/pk/`), but the bucket is global. A true isolation fix (per-view buckets) is deferred to v2.
**How to avoid:** D-11 explicitly limits scope to prefix watch. Document the cross-view cost as a known limitation (per success criterion 4).
**Warning signs:** If test assertions assume per-view bucket isolation, they'll fail.

### Pitfall 2: `UseNumber` Changes Return Types
**What goes wrong:** After switching to `json.NewDecoder().UseNumber()`, stored JSON numbers are decoded as `json.Number` (string type) instead of `float64`. This changes the type of values in query results returned to clients.
**Why it happens:** `json.Number` is a string type, not a numeric type. JSON serialization of `json.Number` preserves the original string representation (no float64 precision loss), but Go type assertions against `float64` will fail.
**How to avoid:** All comparison logic in `valuesEqual`, `filterRow`, and `projectRow` must handle `json.Number` type. Decisions D-09/D-10 specify the comparison strategy. Final JSON output via `json.Marshal(QueryResult)` should naturally handle `json.Number` correctly (it serializes as the raw number string).
**Warning signs:** Tests asserting `results[0]["age"] == float64(30)` will fail because `json.Number("30") != float64(30)`.

### Pitfall 3: Short-Circuit Logic Contradicting Same-Value Conditions
**What goes wrong:** `WHERE id = 'a' AND id = 'a'` has duplicate but non-contradictory predicates. The short-circuit (D-02) should NOT return empty for this case.
**Why it happens:** The duplicate condition has the same value, so it's logically equivalent to a single condition. Only different values (e.g., `WHERE id = 'a' AND id = 'b'`) should short-circuit.
**How to avoid:** Track the first value encountered for each PK column. If a second OpEq condition with a *different* value is found, short-circuit. If same value, deduplicate and continue.
**Warning signs:** Existing test queries that use `WHERE id = 'u1'` remain valid, but queries with the same condition duplicated should still work.

### Pitfall 4: `_meta` Filter Privacy
**What goes wrong:** The `_meta` filter (D-04) strips keys starting with `_`. If a user declares a column starting with `_`, it would also be stripped in `SELECT *`.
**Why it happens:** The heuristics-based filter doesn't consult the schema.
**How to avoid:** This is an accepted tradeoff per D-05. Column names starting with `_` should be documented as reserved for internal use. The planner checks schema columns during validation (validate.go), so explicitly selecting `SELECT _meta` would be rejected by Validate because `_meta` is not a declared column.
**Warning signs:** None in normal operation — this only affects edge cases with user-declared `_`-prefixed columns.

## Code Examples

### Pattern 1: JSON Decode with UseNumber
```go
// Source: materializer/mapper.go:66 (existing pattern in codebase)
decoder := json.NewDecoder(bytes.NewReader(data))
decoder.UseNumber()
var row map[string]any
if err := decoder.Decode(&row); err != nil {
    return nil, fmt.Errorf("unmarshaling row: %w", err)
}
```
**Where applied:** PKLookupPlan.Execute (executor.go:32) and FullScanPlan.Execute goroutine (executor.go:97)

### Pattern 2: valuesEqual with json.Number
```go
// Source: Decision D-09, adapted from existing valuesEqual (executor.go:202)
func valuesEqual(a, b any) bool {
    if a == nil || b == nil {
        return a == b
    }

    // Normalize json.Number to int64 or float64 for comparison
    an, aIsNum := a.(json.Number)
    bn, bIsNum := b.(json.Number)
    if aIsNum || bIsNum {
        aVal := a
        bVal := b
        if aIsNum {
            aVal = convertJSONNumber(an)
        }
        if bIsNum {
            bVal = convertJSONNumber(bn)
        }
        return valuesEqual(aVal, bVal)
    }

    // rest of existing logic...
}

func convertJSONNumber(n json.Number) any {
    s := n.String()
    if !strings.Contains(s, ".") {
        if i, err := strconv.ParseInt(s, 10, 64); err == nil {
            return i
        }
    }
    if f, err := strconv.ParseFloat(s, 64); err == nil {
        return f
    }
    return s // fallback
}
```

### Pattern 3: projectRow — Strip _-prefixed Keys
```go
// Source: Decision D-04
func projectRow(row map[string]any, columns []string) map[string]any {
    if columns == nil { // SELECT *
        projected := make(map[string]any, len(row))
        for k, v := range row {
            if !strings.HasPrefix(k, "_") {
                projected[k] = v
            }
        }
        return projected
    }
    // ... existing logic for explicit columns
}
```

### Pattern 4: HTTP Error Detection via errors.As
```go
// Source: Decision D-16/17/18
var maxBytesErr *http.MaxBytesError
if errors.As(err, &maxBytesErr) {
    w.Header().Set("Content-Type", "application/json")
    w.WriteHeader(http.StatusRequestEntityTooLarge)
    w.Write([]byte(fmt.Sprintf(`{"results":[],"error":"request body too large (max %d bytes)"}`, maxRequestBodySize)))
    return
}

// After successful decode, check for trailing data (D-17/18)
remaining, readErr := io.ReadAll(r.Body)
if readErr != nil {
    w.Header().Set("Content-Type", "application/json")
    w.WriteHeader(http.StatusBadRequest)
    w.Write([]byte(`{"results":[],"error":"error reading request body"}`))
    return
}
r.Body.Close()
if len(strings.TrimSpace(string(remaining))) > 0 {
    w.Header().Set("Content-Type", "application/json")
    w.WriteHeader(http.StatusBadRequest)
    w.Write([]byte(`{"results":[],"error":"unexpected data after JSON body"}`))
    return
}
```

### Pattern 5: NATS Flush/Respond Error Handling
```go
// Source: Decision D-19/20
sub, err := nc.Subscribe("natsql.query", func(msg *nats.Msg) {
    // ... handler logic ...
    if err := msg.Respond(data); err != nil {
        // Can't return from callback, log instead
        slog.Warn("failed to respond to NATS query", "error", err)
    }
})
if err != nil {
    return nil, fmt.Errorf("subscribing to natsql.query: %w", err)
}
if err := nc.Flush(); err != nil {
    sub.Unsubscribe() // clean up subscription
    return nil, fmt.Errorf("flushing NATS subscription: %w", err)
}
```

### Pattern 6: Short-Circuit Contradictory PK Predicates
```go
// Source: Decision D-02 — inserted in BuildPlan before returning PKLookupPlan
// After collecting pkConditions, before constructing PKLookupPlan:
for _, cond := range q.Where {
    if cond.Op == OpEq {
        if existing, ok := pkSeen[cond.Column]; ok {
            if !valuesEqual(existing, cond.Value) {
                // Contradictory PK conditions → empty result
                return &EmptyPlan{}, nil
            }
        } else {
            pkSeen[cond.Column] = cond.Value
        }
    }
}
```

### Pattern 7: CLI Stream Creation with SourceSubject
```go
// Source: Decision D-13/14/15 — replacing current main.go:135-137
if !cfg.NATS.Embedded {
    // In external mode, only create streams if --create-streams flag is set
    if !createStreams {
        // Warn but don't create — consumer setup will fail with clear error
        logger.Warn("external mode: not creating streams (use --create-streams to opt in)")
        return nil
    }
}

for _, v := range cfg.Views {
    if seen[v.SourceStream] {
        continue
    }
    seen[v.SourceStream] = true

    // Build subjects list respecting source_subject
    subjects := []string{v.SourceSubject}
    if v.SourceSubject == "" {
        subjects = []string{v.SourceStream + ".>"}
    }

    _, serr := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
        Name:     v.SourceStream,
        Subjects: subjects,
    })
    // ...
}
```

## State of the Art

This phase does not change the stack architecture — it fixes bugs within existing patterns. The key improvements:

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| PK conditions removed from post-filter | ALL conditions kept as post-filters | This phase | Fixes contradictory/duplicate PK predicate bugs |
| `SELECT *` returns raw map | `SELECT *` strips `_`-prefixed keys | This phase | `_meta` no longer leaks to users |
| `json.Unmarshal` in executor | `json.Decoder.UseNumber()` | This phase | Large integers preserve precision |
| `WatchAll` + client-side filter | `Watch(prefix)` for scoped scan | This phase | Reduces I/O for full scans |
| CLI creates streams unconditionally | Only in embedded or with `--create-streams` | This phase | Respects external stream ownership |
| String comparison for body size | `errors.As` with `*http.MaxBytesError` | This phase | Robust error detection |
| `nc.Flush()` error ignored | Error checked and returned | This phase | Catches subscription propagation failures |
| `msg.Respond()` error ignored | Error logged | This phase | Surfaces response failures |

## Assumptions Log

| # | Claim | Section | Risk if Wrong |
|---|-------|---------|---------------|
| A1 | `jetstream.KeyValue.Watch(ctx, prefix)` accepts a string key prefix pattern and behaves equivalently to `WatchAll` but scoped | Architecture | LOW risk — NATS Go docs confirm `Watch` takes key pattern; if `Watch` with prefix doesn't exist in the v1.51 API, fallback: use `WatchAll` + prefix filter (current behavior) is acceptable |
| A2 | `json.Marshal` on a `map[string]any` containing `json.Number` values serializes them as raw numbers | Code Examples | LOW risk — `json.Number` implements `json.Marshaler` interface, marshaling as the raw string value (a number string). Verified in Go stdlib behavior |
| A3 | The `bytes.NewReader` import is not already present in executor.go | Code Examples | LOW risk — if already imported, skip adding it; if not, add to import block |
| A4 | Multiple WHERE conditions on the same PK column all with OpEq and the same value are acceptable and should not short-circuit | Pitfall 3 | LOW risk — logically correct per D-02 intent |

## Open Questions

1. **Does `jetstream.KeyValue` interface in nats.go v1.51+ have a `Watch(ctx, prefix)` method?**
   - What we know: `WatchAll(ctx)` exists. The Go NATS docs for the JetStream KV API list `Watch(ctx context.Context, keys string) (KeyWatcher, error)` for prefix watching.
   - What's unclear: Exact method signature and behavior for prefix pattern matching.
   - Recommendation: Check the JetStream KV API during implementation. The pattern `viewName/pk/` should match all keys starting with that prefix. If `Watch` doesn't support prefix patterns, fall back to `WatchAll` + existing `strings.HasPrefix` filter (current behavior is a performance regression, not a correctness issue).

2. **Does `errors.As` with `*http.MaxBytesError` work correctly when the error is wrapped?**
   - What we know: `json.Decoder.Decode` returns the `*http.MaxBytesError` directly from `MaxBytesReader`.
   - What's unclear: Whether the error might be wrapped by json.Decoder in some circumstances.
   - Recommendation: `errors.As` unwraps the error chain, so wrapping is handled. Add a test with both too-large body and valid body to verify both paths.

## Environment Availability

> Skipped — no new external dependencies introduced. All changes use stdlib or already-imported packages.

## Validation Architecture

> Skipped — `workflow.nyquist_validation` is explicitly `false` in `.planning/config.json`.

## Security Domain

### Applicable ASVS Categories

| ASVS Category | Applies | Standard Control |
|---------------|---------|-----------------|
| V5 Input Validation | yes | `http.MaxBytesReader` for body size limit; trailing data rejection prevents request smuggling |
| V6 Cryptography | no | No new crypto operations |
| V12.3 File Upload | partial | Body size limit (`MaxBytesReader`) is relevant to request body handling |

### Known Threat Patterns

| Pattern | STRIDE | Standard Mitigation |
|---------|--------|---------------------|
| Request smuggling via trailing data | Tampering | D-17/D-18: drain body after JSON decode, reject if non-whitespace remains |
| Oversized request body DoS | Denial of Service | Existing `MaxBytesReader` (1MB limit); D-16 improves error detection robustness |
| Internal field leakage | Information Disclosure | D-04/D-05: strip `_`-prefixed keys from `SELECT *` results |

## Sources

### Primary (HIGH confidence)
- Source code files in `internal/query/`, `internal/transport/`, `cmd/natsql/main.go` — [VERIFIED: codebase]
- `cr.md` — CR-03, CR-04, CR-09, CR-13, CR-14, CR-18, CR-19, CR-20 — [VERIFIED: codebase]
- `10-CONTEXT.md` — All decisions D-01 through D-21 — [VERIFIED: discussion phase output]
- `internal/materialize/mapper.go` line 66 — Existing `UseNumber` pattern — [VERIFIED: codebase]
- `internal/materialize/writer.go` lines 39-42 — `_meta` field presence in stored rows — [VERIFIED: codebase]
- `internal/kv/kv.go` line 95 — Key format `viewName/pk/` — [VERIFIED: codebase]

### Secondary (MEDIUM confidence)
- Go `encoding/json` `UseNumber` documentation — [CITED: Go stdlib docs]
- `errors.As` and `http.MaxBytesError` docs — [CITED: Go stdlib docs]

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH — all libraries already in use or stdlib
- Architecture: HIGH — patterns are direct code modifications with no architectural changes
- Pitfalls: HIGH — identified from code review findings and discussion phase decisions

**Research date:** 2026-06-02
**Valid until:** 2026-07-02 (stable stack, no fast-moving dependencies)
