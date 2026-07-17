# Phase 10: Query Engine & Transport — Context

**Gathered:** 2026-06-01
**Status:** Ready for planning

<domain>
## Phase Boundary

Fix query engine correctness (PK predicate handling, meta field filtering, numeric precision, scan architecture) and transport robustness (CLI stream creation, HTTP body handling, NATS error surfacing, error message fix). This phase does NOT address cleanup/documentation (Phase 11).

</domain>

<decisions>
## Implementation Decisions

### PK Predicate Post-Filter (QENG-01 / CR-03)
- **D-01:** Keep ALL original WHERE conditions as post-filters. The PK lookup narrows the search space; post-filters verify every condition
- **D-02:** Add short-circuit optimization: if the same PK column has multiple OpEq conditions with different values (e.g., `WHERE id = 'a' AND id = 'b'`), return an empty plan immediately (no KV lookup needed)
- **D-03:** The `findPKEqConditions` still collects PK equality conditions for the lookup key, but `BuildPlan` no longer removes matching conditions from the post-filter list

### Meta Field Exclusion (QENG-02 / CR-04)
- **D-04:** `projectRow` for `SELECT *` (nil columns) strips all map keys starting with `_` before returning
- **D-05:** This catches `_meta`, `_stream_seq`, and any future internal fields automatically
- **D-06:** Explicit column selection (`SELECT col1, col2`) is unchanged — only `SELECT *` applies the filter

### Numeric Precision (QENG-03 / CR-09)
- **D-07:** Switch `PKLookupPlan.Execute` to use `json.NewDecoder(resp.Data).UseNumber()` instead of `json.Unmarshal`
- **D-08:** Switch `FullScanPlan.Execute` to use `json.NewDecoder(bytes.NewReader(data)).UseNumber()` instead of `json.Unmarshal`
- **D-09:** Add `json.Number` comparison to `valuesEqual`: extract string value, if no decimal point → parse as int64 and compare against int64/float64; if has decimal → parse as float64
- **D-10:** WHERE clause literal values (from the parser) remain as `int64`/`string`/`bool`/`float64` — the comparison logic handles cross-type matching

### Full-Scan Architecture (QENG-04 / CR-13)
- **D-11:** Replace `WatchAll` with `Watch(prefix)` using `viewName/pk/` prefix — limits KV watch scope to the target view's keys only
- **D-12:** The 16-worker full scan pool is kept for concurrent unmarshal/filter/projection

### CLI Stream Creation (TRN-01 / CR-14)
- **D-13:** CLI only creates streams in embedded mode (engine owns them). In external mode, warn if stream doesn't exist — let consumer setup fail with clear error
- **D-14:** Add `--create-streams` flag for explicit opt-in in external mode. Respects `source_subject` when creating streams
- **D-15:** Do NOT mutate existing external streams' subject sets

### HTTP Body Handling (TRN-02 / CR-18)
- **D-16:** Use `*http.MaxBytesError` via `errors.As` for body-size detection (replace string comparison)
- **D-17:** After first JSON decode, drain remaining body bytes. If any non-whitespace data remains, reject with 400 Bad Request
- **D-18:** Implementation: drain into buffer, `strings.TrimSpace` on result, reject if non-empty

### NATS Error Handling (TRN-03 / CR-19)
- **D-19:** `RegisterNATSHandler` checks and returns `nc.Flush()` error after subscription
- **D-20:** Message handler logs `msg.Respond()` errors (can't return from callback)

### Error Message Fix (TRN-04 / CR-20)
- **D-21:** Fix `executor.go:33` — change `"marshaling row"` to `"unmarshaling row"`

</decisions>

<canonical_refs>
## Canonical References

### Source of Truth
- `cr.md` — CR-03 (PK predicates), CR-04 (meta fields), CR-09 (precision), CR-13 (scans), CR-14 (stream creation), CR-18 (HTTP body), CR-19 (NATS errors), CR-20 (typo)
- `.planning/VERIFICATION_FINDINGS.md` — Current verification status
- `.planning/research/ARCHITECTURE.md` §3 — Query engine predicate architecture, scan architecture
- `.planning/research/PITFALLS.md` (CR-03, CR-14) — Detailed pitfall analysis

### Requirements
- `.planning/REQUIREMENTS.md` §v2.0.0 — QENG-01 through QENG-04, TRN-01 through TRN-04
- `.planning/ROADMAP.md` §Phase 10 — Success criteria

### Prior Phase Context
- `.planning/phases/08-verification-foundation/08-CONTEXT.md` — BuildPkKey for PK construction
- `.planning/phases/09-materializer-engine-lifecycle/09-CONTEXT.md` — Sequential processing, error classification

### Current Source
- `internal/query/planner.go` — BuildPlan, findPKEqConditions (QENG-01)
- `internal/query/executor.go` — PKLookupPlan.Execute, FullScanPlan.Execute, projectRow, valuesEqual (QENG-01 through QENG-04)
- `internal/transport/http.go` — RegisterHTTPHandler (TRN-02)
- `internal/transport/nats.go` — RegisterNATSHandler (TRN-03)
- `cmd/natsql/main.go` — Stream creation (TRN-01)
</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- `internal/kv/kv.go` — `BuildPkKey()` for KV key construction
- `internal/query/validate.go` — Query validation (may need schema access for _meta filtering)

### Established Patterns
- TDD pattern from Phase 8 (tests before implementation)
- Single shared KV bucket (not changing — cross-view cost mitigated by prefix watch)

### Integration Points
- `planner.go:38-44` — PK conditions removed from post-filter → keep all + short-circuit
- `executor.go:31-33` — json.Unmarshal → json.Decoder.UseNumber()
- `executor.go:97` — FullScan json.Unmarshal → json.Decoder.UseNumber()
- `executor.go:145-148` — projectRow SELECT * returns raw row → strip _-prefixed keys
- `executor.go:49` — WatchAll → Watch(prefix)
- `http.go:31` — String comparison for body size → errors.As
- `http.go:40-42` — Drain without check → whitespace-only check
- `nats.go:41` — Flush error ignored → return error
- `nats.go:36` — msg.Respond error ignored → log it

</code_context>

<specifics>
No specific requirements — standard approaches per cr.md suggested fixes.

</specifics>

<deferred>
None — discussion stayed within phase scope.

</deferred>

---

*Phase: 10-query-engine-transport*
*Context gathered: 2026-06-01*
