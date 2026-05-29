# Phase 4: Query Engine Bug Fixes — PLAN

**Goal:** PK-lookup queries correctly apply all WHERE conditions; concurrent access is race-free; type comparisons are accurate; boolean literals parse.

**Depends on:** Nothing (fixes to existing v1.0 code)

**Requirements:** FIX-ENG-01, FIX-ENG-02, FIX-ENG-03, FIX-ENG-04

## Tasks

### Task 1: PK post-filter (FIX-ENG-01) — `query/planner.go` + `query/executor.go`

**Bug (C1 from 02-REVIEW.md):** `PKLookupPlan.Execute` silently drops non-PK WHERE conditions. `WHERE id = 'abc' AND name = 'Bob'` returns the row regardless of whether `name` matches.

**Fix plan:**
1. Add `Where []Condition` field to `PKLookupPlan` struct in `query/types.go`
2. In `query/planner.go:BuildPlan`, when creating a `PKLookupPlan`, pass through any non-PK conditions from `q.Where` that weren't used for the PK lookup
3. In `query/executor.go:PKLookupPlan.Execute`, after the PK `Get()` succeeds, apply `filterRow()` with the stored `Where` conditions — reject row if non-PK conditions don't match

**Verification:**
- `SELECT * FROM users WHERE id = 'abc' AND name = 'Bob'` returns empty if name differs
- Existing PK lookup tests still pass
- Existing full scan tests still pass

### Task 2: Data race on Engine.kv (FIX-ENG-02) — `engine/engine.go`

**Bug (H1 from 02-REVIEW.md):** `Engine.Query()` at line 245 reads/writes `e.kv` without mutex protection. Concurrent `Query()` calls race on lazy-init.

**Fix plan:**
1. Add `sync.Mutex` (or `sync.RWMutex`) field to `Engine` struct in `engine/engine.go`
2. Lock around the `e.kv == nil` check + `InitBucket` call in `Query()`
3. Consider using sync.Once for the KV bucket initialization pattern

**Verification:**
- `go test -race ./engine/...` passes
- `go test -race ./query/...` passes
- Existing tests still pass without -race

### Task 3: Type-aware filterRow (FIX-ENG-03) — `query/executor.go`

**Bug (M3 from 02-REVIEW.md):** `filterRow` uses `fmt.Sprint(val) != fmt.Sprint(c.Value)` which coerces all values to strings. This can produce false matches (e.g., `true` vs `"true"`, `30` vs `"30"`).

**Fix plan:**
1. Replace `fmt.Sprint` comparison in `filterRow` (`query/executor.go:114-143`) with type-aware comparison:
   - If both values are `float64`, compare numerically
   - If both are `bool`, compare as booleans
   - If both are `string`, compare as strings
   - Otherwise, use `fmt.Sprint` as fallback (conservative — won't false-match)

**Verification:**
- `WHERE age = 30` doesn't match rows where age is string "30"
- `WHERE active = true` (once Task 4 fixed) doesn't match string "true"
- Existing tests all pass

### Task 4: Boolean literal parsing (FIX-ENG-04) — `query/parser.go`

**Bug (discovered during benchmark testing):** SQL parser's `extractValue` doesn't handle `*sqlparser.BoolVal`. `WHERE active = true` fails with "unsupported value type".

**Fix plan:**
1. In `query/parser.go:extractValue`, add a `*sqlparser.BoolVal` case that returns the boolean value
2. Ensure the executor's `filterRow` handles boolean comparisons correctly (complement to Task 3)

**Verification:**
- `Parse("SELECT * FROM t WHERE active = true")` succeeds with boolean value
- `Parse("SELECT * FROM t WHERE active = false")` succeeds with boolean value
- End-to-end: publish event with `"active": true`, query with `WHERE active = true`

## Success Criteria

1. `SELECT * FROM users WHERE id = 'abc' AND name = 'Bob'` returns only rows matching **both** conditions
2. Concurrent `Engine.Query()` calls pass `go test -race` — no race on `Engine.kv`
3. `filterRow` compares by actual types, not `fmt.Sprint` — no false matches
4. SQL parser accepts `WHERE active = true/false` with correct boolean results

## Files to modify

| File | Task | Change |
|------|------|--------|
| `query/types.go` | 1 | Add `Where []Condition` to `PKLookupPlan` |
| `query/planner.go` | 1 | Pass non-PK conditions to PKLookupPlan |
| `query/executor.go` | 1, 3 | Post-filter + type-aware comparison |
| `engine/engine.go` | 2 | Mutex on `e.kv` lazy-init |
| `query/parser.go` | 4 | BoolVal case in extractValue |
