# Code Review Report

Reviewed scope: all Go source, tests, examples, CLI, config, README, and CI.

Verification run:

- `go test ./...` passed
- `go vet ./...` passed
- `go test -race ./internal/query ./internal/materialize ./internal/engine` passed
- `gofmt -l $(git ls-files '*.go')` reported formatting drift

## Critical Findings

### CR-01: Materializer can corrupt state by processing ordered stream messages concurrently

Evidence: `internal/materialize/materializer.go:20`, `internal/materialize/materializer.go:188`, `internal/materialize/materializer.go:193`, `internal/materialize/materializer.go:195`, `internal/materialize/writer.go:48`, `internal/materialize/writer.go:49`

The materializer reads from one durable stream consumer but dispatches messages to 16 workers. JetStream stream order is therefore not preserved at the KV write boundary. Two updates for the same primary key can be processed out of order, and `Writer.Apply` blindly performs `kv.Put`, so an older event can overwrite a newer event.

This violates materialized-view correctness and the project constraint that state should be CAS-based/read-committed rather than last-writer-wins by goroutine scheduling.

Suggested solution: preserve per-view stream order with a single writer stage, or partition by primary key and preserve per-key order. Store `_meta.stream_seq` as authoritative version data and use a CAS/read-modify-write loop that rejects stale stream sequences. Add an integration test publishing rapid updates for one key with an artificial slow first update and assert the highest stream sequence wins.

### CR-02: Primary-key sanitization is inconsistent and breaks lookups for special-character keys

Evidence: `internal/materialize/mapper.go:214`, `internal/materialize/mapper.go:234`, `internal/materialize/writer.go:48`, `internal/kv/kv.go:57`, `internal/kv/kv.go:58`, `internal/query/planner.go:30`, `internal/query/planner.go:32`, `internal/query/executor.go:22`

The mapper sanitizes each PK component in `stringifyValue`, then the writer calls `kv.PkKey`, which sanitizes the already-sanitized joined PK again. Query planning joins raw SQL values and the executor calls `kv.PkKey`, so queries sanitize only once. A row with id `a_b`, `a|b`, `a/b`, `a*b`, or `a>b` is written under a different key from the one used for lookup.

Suggested solution: define one canonical PK encoder and use it exactly once for both writes and reads. Prefer storing raw typed PK parts until key construction, then call `kv.PkKey(view, encodeCompositePK(parts, separator))` once. Add black-box tests for PK values containing `_`, `|`, `/`, `*`, `>`, and custom separators.

### CR-03: PK fast path drops contradictory predicates on the same PK column

Evidence: `internal/query/planner.go:26`, `internal/query/planner.go:41`, `internal/query/planner.go:43`, `internal/query/planner.go:44`, `internal/query/planner.go:82`

`findPKEqConditions` stores one equality condition per PK column, and `BuildPlan` removes every condition whose column appears in that map. This makes queries like `WHERE id = 'u1' AND id != 'u1'` return `u1`, even though SQL semantics require zero rows. Duplicate conflicting equality predicates like `WHERE id = 'u1' AND id = 'u2'` are also mishandled because the last equality wins and all same-column conditions are removed from post-filtering.

Suggested solution: only remove the exact PK equality predicates used to construct the lookup key. Keep all other predicates, including predicates on the same PK column, as post-filters. Also validate duplicate equality predicates on the same column and short-circuit impossible predicates to an empty result plan.

## High Findings

### CR-04: `SELECT *` exposes internal `_meta` fields

Evidence: `internal/materialize/writer.go:38`, `internal/materialize/writer.go:39`, `internal/materialize/writer.go:40`, `internal/query/executor.go:145`, `internal/query/executor.go:146`, `internal/query/executor.go:147`

Rows are stored with internal `_meta`, and `projectRow` returns the full stored map for `SELECT *`. This leaks implementation metadata into user query results even though `_meta` is not part of the declared schema and cannot be explicitly selected through validation.

Suggested solution: make `SELECT *` project only schema columns. Pass schema column names into plans, or strip reserved internal fields before returning results. Add tests asserting `_meta` is not present in `SELECT *` results unless an explicit metadata feature is intentionally designed.

### CR-05: Unsupported SQL clauses and expressions are silently accepted

Evidence: `internal/query/parser.go:47`, `internal/query/parser.go:49`, `internal/query/parser.go:62`, `internal/query/parser.go:63`, `internal/query/parser.go:91`, `internal/query/parser.go:93`, `internal/query/parser.go:94`, `internal/query/parser.go:95`

The parser extracts only simple columns and ignores unsupported select expressions. Queries such as `SELECT COUNT(*) FROM users WHERE id = 'x'`, `SELECT 1 FROM users WHERE id = 'x'`, `SELECT DISTINCT name ...`, or queries with `ORDER BY`/`GROUP BY` can be parsed and executed with misleading results instead of rejected.

Suggested solution: make select extraction return an error when any expression is not a simple column or a single `*`. Explicitly reject `DISTINCT`, `ORDER BY`, `GROUP BY`, `HAVING`, aggregations, and other unsupported clauses in `Parse`. Add negative tests for each unsupported construct.

### CR-06: CLI `http.port` and `--port` are logged but not applied

Evidence: `cmd/natsql/main.go:97`, `cmd/natsql/main.go:98`, `cmd/natsql/main.go:110`, `cmd/natsql/main.go:117`, `cmd/natsql/main.go:161`, `internal/engine/engine.go:138`, `internal/engine/engine.go:194`, `internal/engine/engine.go:281`

The CLI applies `cfg.HTTP.Port` and logs it, but engine construction never passes `WithQueryPort`, and `engine.New`/`NewEmbedded` default to `8080`. The standalone server therefore ignores configured HTTP ports.

Suggested solution: pass `natsql.WithQueryPort(cfg.HTTP.Port)` when constructing the engine, or have `engine.New` initialize `queryPort` from `cfg.HTTP.Port`. Add a CLI-level test that starts with a non-default port and verifies the HTTP server binds there.

### CR-07: `Start` can report success even when core services failed to start

Evidence: `internal/engine/engine.go:253`, `internal/engine/engine.go:254`, `internal/engine/engine.go:256`, `internal/engine/engine.go:267`, `internal/engine/engine.go:269`, `internal/engine/engine.go:270`, `internal/engine/engine.go:288`, `internal/engine/engine.go:292`, `internal/engine/engine.go:293`, `internal/engine/engine.go:300`

Materializer setup errors happen inside goroutines and are only logged. NATS handler registration failures are logged but do not fail `Start`. HTTP listen failures also happen after the goroutine starts, so port conflicts are logged while `Start` still returns nil and sets `started=true`.

Suggested solution: perform materializer setup synchronously before returning from `Start`, or use readiness channels that propagate startup errors. Bind the HTTP listener with `net.Listen` before launching `Serve` so bind errors are returned. Decide whether NATS handler failure should be fatal or expose a degraded-mode status.

### CR-08: Config validation does not prove `key_fields` and primary-key columns are consistent

Evidence: `internal/cfg/config.go:156`, `internal/cfg/config.go:164`, `internal/cfg/config.go:180`, `internal/cfg/config.go:185`, `internal/cfg/config.go:196`, `internal/cfg/config.go:212`, `internal/cfg/config.go:215`

Validation only checks that at least one `key_field` exists and at least one column has `primary_key=true`. It does not verify that every key field names a declared column, that key fields are primary-key columns, that primary-key columns are all included in `key_fields`, or that column/key-field names are unique. Invalid configs can start and then send every event to DLQ or produce schemas that query planning cannot use correctly.

Suggested solution: validate uniqueness of view names, column names, and key fields. Require each `key_field` to reference an existing `primary_key` column. Either require the set of `primary_key` columns to exactly equal `key_fields`, or remove `primary_key` from column config and derive it from `key_fields`.

### CR-09: Large numeric values lose precision during query execution

Evidence: `internal/materialize/mapper.go:68`, `internal/materialize/mapper.go:69`, `internal/materialize/mapper.go:174`, `internal/materialize/mapper.go:180`, `internal/query/executor.go:32`, `internal/query/executor.go:97`, `internal/query/executor.go:207`

The mapper decodes events with `UseNumber` and stores `json.Number`, but the query executor unmarshals rows with plain `json.Unmarshal`, which converts JSON numbers to `float64`. Large integers above 2^53 lose precision before comparison, so equality and `IN` filters can be wrong.

Suggested solution: use `json.Decoder.UseNumber` in the executor too. Extend `valuesEqual` to compare `json.Number` exactly for integer-looking numbers and carefully for decimals. Add tests for values like `9007199254740993`.

### CR-10: Transient KV write failures are acknowledged and sent to DLQ, causing data loss

Evidence: `internal/materialize/materializer.go:250`, `internal/materialize/materializer.go:255`, `internal/materialize/materializer.go:261`, `internal/materialize/materializer.go:265`

All writer failures are treated as DLQ-worthy and the original stream message is acknowledged after the DLQ publish. That is appropriate for deterministic bad events, but not for transient NATS/KV failures. A temporary KV outage can permanently drop a valid event from the materialized view.

Suggested solution: classify errors. Malformed input should be DLQ plus ACK. Transient storage/publish/context failures should NAK with backoff or leave unacked for redelivery. Only terminal write errors should go to DLQ.

### CR-11: Durable consumers can be deleted after inactivity, undermining crash recovery

Evidence: `internal/materialize/consumer.go:46`, `internal/materialize/consumer.go:47`, `internal/materialize/consumer.go:49`, `internal/materialize/consumer.go:53`

The consumer is durable, but `InactiveThreshold` is set to one hour. If the engine is down longer than that, the server can remove the durable consumer and the next startup uses `DeliverAllPolicy`, replaying the stream from the beginning. Upserts make this mostly idempotent for current rows, but it can duplicate DLQ entries, add startup load, and violate the stated durable-consumer recovery behavior.

Suggested solution: remove `InactiveThreshold` for durable consumers or make it an explicit config with documentation. Add a restart test that simulates durable deletion and verifies intended replay behavior.

### CR-12: `ConsumerConfig.BatchSize` does not control fetch batch size

Evidence: `internal/materialize/consumer.go:41`, `internal/materialize/consumer.go:52`, `internal/materialize/materializer.go:120`, `internal/materialize/materializer.go:136`

`BatchSize` only influences `MaxAckPending`. The materializer uses `cons.Messages()` and repeatedly calls `Next`, so the configured batch size does not actually control pull batch size or application batching.

Suggested solution: either rename the setting to reflect what it does, or implement actual batched fetching with `Fetch`/`FetchNoWait` and process bounded batches. Update tests to assert runtime behavior, not only consumer config fields.

### CR-13: Full-scan queries scan the entire shared KV bucket for every view

Evidence: `internal/query/executor.go:49`, `internal/query/executor.go:51`, `internal/query/executor.go:65`, `internal/query/executor.go:69`, `internal/kv/kv.go:14`

`FullScanPlan` uses `WatchAll` over the single global bucket and filters by prefix client-side. A full scan on one view pays the cost of schemas and all rows in all other views.

Suggested solution: use a prefix watch if the JetStream KV API supports it, maintain per-view buckets, or maintain view-specific key indexes. At minimum, document the cross-view cost and add benchmarks with multiple large views.

### CR-14: CLI mutates or creates source streams without respecting configured subjects

Evidence: `cmd/natsql/main.go:123`, `cmd/natsql/main.go:135`, `cmd/natsql/main.go:136`, `cmd/natsql/main.go:137`, `internal/materialize/consumer.go:56`, `internal/materialize/consumer.go:57`

The standalone CLI creates or updates every source stream with subject `${source_stream}.>`, regardless of `source_subject`. In external NATS mode this can unexpectedly alter an existing stream's subject set or create streams with subjects that do not match the user’s configured source subject.

Suggested solution: do not mutate external streams by default. Add an explicit `--create-streams` option, respect `source_subject` when creating streams, and avoid changing existing stream subjects unless explicitly requested.

## Medium Findings

### CR-15: Config/tests imply JSONPath-style `$.field`, but mapper only supports dot paths

Evidence: `config_test.go:18`, `config_test.go:22`, `config_test.go:60`, `config_test.go:73`, `internal/materialize/mapper.go:121`, `internal/materialize/mapper.go:124`, `internal/materialize/mapper.go:125`

Several public config tests use `from: $.user_id`, but the mapper splits paths on `.` and does not strip a leading `$`. A user following that implied format would send valid events to DLQ because the mapper looks for a top-level `$` field.

Suggested solution: either support an optional `$.` prefix in `extractNestedField`, or remove all `$` examples/tests and document that only plain dot notation is accepted.

### CR-16: Index config is accepted but ignored

Evidence: `internal/cfg/config.go:79`, `internal/cfg/config.go:91`, `internal/cfg/config.go:92`, `config_test.go:200`, `config_test.go:221`

Users can configure indexes and validation accepts them, but no materialization or query code uses them. Because non-PK filters full-scan, this can create a false expectation of indexed query performance.

Suggested solution: until secondary indexes are implemented, either reject `indexes` with a clear error or mark them as experimental/no-op in user-facing documentation and runtime logs.

### CR-17: No delete/tombstone semantics for materialized rows

Evidence: `internal/materialize/mapper.go:63`, `internal/materialize/mapper.go:113`, `internal/materialize/writer.go:24`, `internal/materialize/writer.go:49`

The materializer only maps events to upserts. There is no config or event convention for deleting a row from the KV snapshot. For a “current state” materialized view, this is an obvious missing data lifecycle path.

Suggested solution: add a delete mode, such as a configured operation field, subject convention, or tombstone predicate. Implement `kv.Delete` for deletes and define query behavior for deleted rows.

### CR-18: HTTP JSON handling accepts trailing data and uses fragile body-too-large detection

Evidence: `internal/transport/http.go:26`, `internal/transport/http.go:29`, `internal/transport/http.go:31`, `internal/transport/http.go:40`, `internal/transport/http.go:41`

The handler decodes one JSON value and then drains the body without checking that the rest is whitespace. It also detects oversized bodies by comparing `err.Error()` to a string.

Suggested solution: after the first decode, attempt a second decode and require `io.EOF`. Use `*http.MaxBytesError` or `errors.As` for body-size errors.

### CR-19: NATS request-reply ignores subscription flush and response errors

Evidence: `internal/transport/nats.go:33`, `internal/transport/nats.go:36`, `internal/transport/nats.go:41`

`RegisterNATSHandler` calls `nc.Flush()` but ignores its error. It also ignores `msg.Respond` errors. This can hide broken subscriptions or failed responses.

Suggested solution: return `nc.Flush()` errors from registration and log response errors in the handler.

### CR-20: Error message in PK lookup says “marshaling” while unmarshaling

Evidence: `internal/query/executor.go:32`, `internal/query/executor.go:33`

The code is unmarshaling a stored row but returns `marshaling row`. This slows debugging of corrupt stored data.

Suggested solution: change the error text to `unmarshaling row` and add a test for corrupt JSON in KV.

### CR-21: Examples ignore important errors and contain lifecycle ownership hazards

Evidence: `examples/02-composite-keys/main.go:55`, `examples/02-composite-keys/main.go:56`, `examples/02-composite-keys/main.go:73`, `examples/04-multiple-views/main.go:66`, `examples/04-multiple-views/main.go:67`, `examples/04-multiple-views/main.go:79`, `examples/04-multiple-views/main.go:89`, `examples/05-library-embed/main.go:59`, `examples/05-library-embed/main.go:83`, `examples/05-library-embed/main.go:114`, `examples/05-library-embed/main.go:127`

Several examples ignore stream creation, publish, and engine start errors. The library example passes the same NATS connection to `New(js, cfg)` and `NewWithNATS(nc, cfg)`; `NewWithNATS` owns and closes that connection, while the first engine still depends on the same underlying connection.

Suggested solution: check all example errors. In the library example, use separate connections for owned and caller-owned patterns, or do not run both patterns in one process. Make ownership rules explicit in the example output.

## Low Findings

### CR-22: Several symbols are dead, stale, or misleading

Evidence: `internal/kv/kv.go:15`, `internal/kv/kv.go:44`, `internal/kv/kv.go:127`, `internal/materialize/mapper.go:25`, `internal/materialize/mapper.go:27`, `internal/materialize/materializer.go:96`, `internal/engine/engine.go:443`

`SchemaPrefix`, `ErrSkipAndAck`, `Stats.LastError`, and the `dlqStream` parameter in `materialize.Run` are not used. `EncodePKValue` and `MustInitBucket` are only test-covered convenience functions and are not part of production flow. `EncodePKValue` also panics on `/` and `:`, while production key construction uses sanitization instead.

Suggested solution: remove unused symbols, or wire them into real behavior. Keep one PK encoding path and delete contradictory helpers.

### CR-23: Formatting drift hurts readability

Evidence: `gofmt -l $(git ls-files '*.go')` reported `cmd/natsql/main.go`, `examples/05-library-embed/main.go`, `internal/cfg/config.go`, `internal/engine/engine.go`, `internal/engine/engine_test.go`, `internal/kv/kv_test.go`, `internal/materialize/mapper.go`, `internal/materialize/mapper_test.go`, `internal/materialize/materializer.go`, `internal/query/query_test.go`, `internal/query/types.go`, `internal/transport/transport_test.go`, and `natsql_blackbox_test.go`.

Suggested solution: run `gofmt` across the repository and add `gofmt -w`/format checks to CI. CI currently builds, vets, tests, and races but does not enforce formatting.

### CR-24: Test helpers are duplicated heavily across packages

Evidence: `internal/query/query_test.go:161`, `internal/kv/kv_test.go:306`, `internal/materialize/consumer_test.go:194`, `internal/engine/engine_test.go:1300`, `natsql_blackbox_test.go:681`

Embedded NATS setup and stream creation helpers are copied across tests. This makes test changes tedious and can cause inconsistent defaults.

Suggested solution: introduce package-local helper files where package boundaries require them, or a small internal test utility package for black-box tests that can import it safely.

### CR-25: Documentation and feature status are out of sync

Evidence: `README.md:82`, `README.md:83`, `README.md:84`, `README.md:85`, `README.md:181`, `README.md:182`, `internal/query/parser.go:62`, `internal/query/executor.go:73`, `natsql_blackbox_test.go:400`, `natsql_blackbox_test.go:404`

The README says LIMIT is planned, while parser, executor, and black-box tests implement LIMIT. That makes it unclear whether LIMIT is supported API or accidental behavior.

Suggested solution: update README and SQL dialect docs to reflect implemented LIMIT semantics, including the fact that LIMIT without ORDER BY is unordered.

## Suggested Fix Order

1. Fix CR-01, CR-02, and CR-03 first because they can return or persist incorrect data.
2. Fix CR-04, CR-05, CR-06, and CR-07 next because they affect user-visible API behavior and operational correctness.
3. Tighten config validation in CR-08 before adding more materialization features.
4. Address transient failure handling, consumer durability, and scan architecture before benchmarking or advertising production readiness.
5. Clean dead code, formatting, and docs after correctness fixes land.
