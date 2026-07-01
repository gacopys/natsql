# natsql SQL Dialect

natsql supports a minimal read-only SQL dialect for querying materialized views.

## SELECT Syntax

```sql
SELECT [column1, column2, ... | *]
FROM view_name
[WHERE condition [AND condition ...]]
[LIMIT n]
```

### SELECT Clause

- `SELECT *` — returns all schema-declared columns. Internal fields (prefixed with `_`) are excluded.
- `SELECT col1, col2` — returns only the specified columns. Column names are validated against the view schema.

### FROM Clause

- References a materialized view name. Must match a view defined in the config.

### WHERE Clause

- Supports equality (`=`) and `IN` operators.
- `WHERE pk_column = 'value'` — primary-key lookup (efficient, O(1) KV get).
- `WHERE non_pk_column = 'value'` — full table scan over all KV keys.
- `WHERE pk_col = 'a' AND non_pk_col = 'b'` — PK narrows search, non-PK post-filters.
- `WHERE col IN ('a', 'b')` — multi-value lookup (client-side expansion).
- Multiple conditions joined with `AND`. OR is not supported.
- Contradictory predicates (`WHERE id = 'a' AND id = 'b'`) return zero results.

### LIMIT Clause

- `LIMIT n` — returns at most `n` rows.
- LIMIT without ORDER BY returns arbitrary rows (not guaranteed to be the "first" n).
- Default: no limit (returns all matching rows).

### Field Path Notation

- Columns use dot notation for nested fields: `from: "user.address.city"`.
- Paths may optionally start with `$`: `from: "$.user.address.city"` (backward-compatible).

## Unsupported Constructs

These produce a parse-time error and are NOT supported:

| Construct | Error |
|-----------|-------|
| `ORDER BY` | "unsupported clause: ORDER BY" |
| `GROUP BY` | "unsupported clause: GROUP BY" |
| `DISTINCT` | "unsupported clause: DISTINCT" |
| `HAVING` | "unsupported clause: HAVING" |
| Aggregate functions (`COUNT`, `SUM`, `AVG`, `MIN`, `MAX`) | "unsupported: aggregate function" |
| Subqueries | "unsupported: subquery" |
| JOINs | "unsupported: JOIN" |
| `OR` in WHERE | parse error (not validated — may produce unexpected results) |

## Deferred to v2

- Range scans (`>`, `<`, `>=`, `<=`) with index support
- Secondary indexes on non-PK columns
- `ORDER BY` with result ordering
- Delete/tombstone semantics
- Per-view KV buckets

## Examples

See `examples/` directory for runnable demo programs.
