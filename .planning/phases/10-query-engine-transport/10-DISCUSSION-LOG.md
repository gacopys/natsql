# Phase 10: Query Engine & Transport — Discussion Log

**Date:** 2026-06-01
**Phase:** 10-query-engine-transport
**Areas discussed:** PK post-filter, Meta field exclusion, json.Number handling, Full-scan approach, HTTP trailing data

---

## PK Post-Filter (QENG-01)

| Option | Description | Selected |
|--------|-------------|----------|
| Keep all + short-circuit | All WHERE as post-filters + contradictory PK → empty plan | ✓ |
| Keep all only | Simple keep all, no special optimization | |

**User's choice:** Keep all + short-circuit (Recommended)

---

## Meta Field Exclusion (QENG-02)

| Option | Description | Selected |
|--------|-------------|----------|
| Strip all `_`-prefixed | Any column starting with `_` excluded from SELECT * | ✓ |
| Known `_meta` key only | Only strip `_meta` key | |

**User's choice:** Strip all `_`-prefixed keys (Recommended)

---

## json.Number Handling (QENG-03)

| Option | Description | Selected |
|--------|-------------|----------|
| json.Number-aware | UseNumber() + json.Number in valuesEqual | ✓ |
| json.Number→float64 | UseNumber() but convert back for comparison | |

**User's choice:** json.Number-aware comparison (Recommended)

---

## Full-Scan Approach (QENG-04)

| Option | Description | Selected |
|--------|-------------|----------|
| Prefix-based Watch | Watch(prefix) instead of WatchAll | ✓ |
| Document only | Keep WatchAll, document cost | |

**User's choice:** Prefix-based Watch (Recommended)

---

## HTTP Trailing Data (TRN-02)

| Option | Description | Selected |
|--------|-------------|----------|
| Second decode + errors.As | Attempt second decode, require io.EOF | |
| Drain + whitespace check | Drain body, check remaining is only whitespace | ✓ |

**User's choice:** Stricter: drain + whitespace check
**Notes:** Also use errors.As for MaxBytesError.

---

## Deferred Ideas

None.
