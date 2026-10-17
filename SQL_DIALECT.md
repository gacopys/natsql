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

- `SELECT *` — returns exactly the schema-declared columns, on every query plan. Internal fields (such as `_meta`) and fields outside the current schema are never returned.
- `SELECT col1, col2` — returns only the specified columns. Column names are validated against the view schema.

### FROM Clause

- References a materialized view name. Must match a view defined in the config.

### WHERE Clause

- Supports equality (`=`), not-equal (`!=`, `<>`) and `IN` operators.
- `WHERE pk_column = 'value'` — primary-key lookup (efficient, O(1) KV get).
- `WHERE pk_column IN ('a', 'b')` — one primary-key lookup per value (up to 256 keys across all key fields; longer lists scan the view).
- `WHERE non_pk_column = 'value'` — full scan of the view's rows (the server only sends this view's rows).
- `WHERE pk_col = 'a' AND non_pk_col = 'b'` — PK narrows search, non-PK post-filters.
- Multiple conditions joined with `AND`. OR is not supported.
- Contradictory predicates (`WHERE id = 'a' AND id = 'b'`) and `pk_column = NULL` return zero results without reading storage.
- Numeric primary keys match by value, not spelling: `WHERE id = 1.0`, `id = 1e0` and `id IN (1)` all find the row whose event had `"id": 1`.
- A query interrupted by its timeout or by cancellation returns an error, never a partial result.

### LIMIT Clause

- `LIMIT n` — returns at most `n` rows. `LIMIT 0` returns no rows.
- LIMIT without ORDER BY returns arbitrary rows (not guaranteed to be the "first" n).
- `OFFSET` (including `LIMIT offset, n`) is rejected: "unsupported: OFFSET is not supported in v1".
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

## Examples

See `examples/` directory for runnable demo programs.
