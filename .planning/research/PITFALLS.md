# Domain Pitfalls: SQL-over-KV Stream Materialization Engine

**Project:** natsql
**Researched:** 2026-05-23
**Confidence:** HIGH (verified against NATS docs, nats.go issue tracker, and domain experience)

---

## Critical Pitfalls

Mistakes that cause rewrites or major issues.

---

### Pitfall 1: Treating JetStream KV as a Queryable Database (Full Bucket Scan)

**What goes wrong:** Every SQL query that lacks a covering index forces a `Keys()` or `ListKeys()` call that loads ALL keys into memory, then fetches each value individually via `Get()`. With 10K+ rows, this becomes a multi-second operation. With 100K+ rows, it exhausts client memory.

**Why it happens:** JetStream KV is not a database — it's a subject-addressable stream with a KV abstraction on top. There is no server-side filtering, no index structure, no query engine. Every `Keys()` call lists keys by scanning stream subjects (under `$KV.<bucket>.>`), and every `Get()` is a direct lookup. There is no `SELECT WHERE` at the server level.

**Consequences:**
- O(N) full scans with N round-trips for unindexed queries
- Memory OOM on moderate datasets (10K+ rows with large values)
- Operation timeout because `Keys()` blocks until the entire list is accumulated
- Underlying stream scan backs up NATS JetStream API internal subject

**Prevention:**
- Design secondary indexes as separate KV buckets (index key → PK mapping)
- Every query path MUST have a corresponding index strategy before implementation
- Enforce that unindexed queries return an error rather than falling back to full scan
- Set `MaxBytes` on KV buckets to prevent unbounded growth
- Use `ListKeys()` (streaming channel) over `Keys()` (all-in-memory slice) when scanning is unavoidable
- Index bucket design: `idx_<view>_<column>=<value>` → `["pk1", "pk2"]` for equality, or `idx_<view>_<column>.<value>` → `pk` for range scans using subject-ordered iteration

**Phase to address:** Phase 2 (Core Materializer) — index design must be baked in from day one. Adding indexes after the fact requires a re-materialize.

**Warning signs:**
- Query latency >100ms on small datasets
- Memory grows proportionally to dataset size during queries
- Benchmarks show O(N) scaling

---

### Pitfall 2: CAS Race Between Stream Consumer and Index Writer

**What goes wrong:** The materializer consumes a JetStream event, updates the main KV bucket row, and writes to index buckets. If the materializer crashes between the main bucket write and the index write, or if two materializers race on the same key, the index points to a stale or non-existent PK.

**Why it happens:** The stream consumer processes one event at a time, but updating the main bucket + N index buckets is not atomic. There is no distributed transaction across KV buckets in NATS JetStream. CAS on each bucket only ensures per-key consistency, not cross-bucket consistency.

**Consequences:**
- Index returns results that don't exist in the main bucket (phantom reads)
- Query results are inconsistent
- CRASH recovery can leave partial index state
- Users lose trust in query correctness

**Prevention:**
- Design a write-ahead log (WAL) pattern: write the intended change to a "pending ops" stream/bucket first, then apply to main+index, then delete the pending op
- OR: use a single KV bucket for both data and index (composite keys like `data/<pk>`, `idx/<col>/<val>/<pk>`) so a single CAS can coordinate them — but this doesn't work for multiple indexes
- OR: accept eventual index consistency and document rebuild as the recovery strategy
- Best approach for this project: make the JetStream consumer the single source of truth. Process event → write to all buckets → ack. On crash recovery, replay from last ack. The replayed event re-applies to buckets (idempotent if we check revision or use create-only semantics)
- Implement a "consistency checker" sweep that validates index ↔ data consistency and repairs drift
- Track materializer epoch/sequence in each KV entry so stale entries can be detected and cleaned

**Phase to address:** Phase 3 (Index Writer) — must be designed alongside the materializer in Phase 2.

**Warning signs:**
- Queries sometimes miss results that exist in data bucket
- Crash recovery produces inconsistent query results
- Manual inspection shows index entries pointing to missing data

---

### Pitfall 3: Assuming Ordered-Stream Delivery Guarantees for Late-Materialized State

**What goes wrong:** JetStream guarantees ordered delivery within a partition (subject-ordered per producer). The materializer assumes that if event A (create PK=1, col=x=5) arrives before event B (update PK=1, col=x=10), then the materialized state reflects A then B. But if a consumer crashes and resumes from a different point — or if events arrive out of order on different subjects — the materialized state can diverge.

**Why it happens:** 
- JetStream orders messages per subject, not across subjects in a stream
- A durable consumer resumes from its last ack, not the last applied sequence
- If the materializer crashes mid-batch, it resumes from the last ack, potentially re-processing events that were partially applied
- Late-arriving events (e.g., a delayed producer retry with `Nats-Msg-Id` dedupe window expired) can materialize stale state

**Consequences:**
- Materialized view shows incorrect state after crash recovery
- "Lost update" where event B's value is overwritten by event A's re-delivery
- Query returns state that never existed in the event stream

**Prevention:**
- Store the last-applied stream sequence alongside each KV entry (part of the value, not the key)
- On event processing, check `event_sequence > stored_sequence` before applying — this makes re-delivery idempotent even without NATS dedupe
- Use `AckSync()` (double ack) to ensure the ack is persisted before considering the event processed — trades throughput for safety
- For v1: acknowledge only after all bucket writes succeed; on crash, re-processing is idempotent (same event produces same state)
- Store materializer progress separately (a KV key tracking `last_seq` per view) to resume from a consistent point
- Handle the "dedupe window expired" case by requiring idempotent event processing (same event → same state regardless of order)

**Phase to address:** Phase 2 (Core Materializer) — idempotent event handling is foundational.

**Warning signs:**
- After crash recovery, query results differ from pre-crash
- Duplicate rows in query results
- Users report "impossible" state combinations

---

### Pitfall 4: Consumer Lifecycle — Goroutine Leaks and Zombie Consumers

**What goes wrong:** JetStream consumers are created but never properly cleaned up, causing:
1. Goroutine leaks in the Go materializer (background fetch loops never exit)
2. Zombie consumers on the NATS server (accrue state, consume disk, prevent stream deletion)
3. Duplicate message delivery when old consumer is abandoned and new one is created

**Why it happens:** 
- `Consume()` returns a `ConsumeContext` that must have `Stop()` called and its `Closed()` channel drained
- If the materializer panics or exits without calling `Stop()`, the consumer lives until `InactiveThreshold` (defaults to never for durable consumers)
- For pull consumers, the fetch loop (`Fetch()` or `Messages()`) must be explicitly stopped
- NATS server doesn't GC consumers with no active subscriptions unless `InactiveThreshold` is set

**Consequences:**
- Goroutine leak: each abandoned consumer leaks at least 1-2 goroutines for dispatch
- Zombie durable consumers accumulate on the NATS server, consuming resources
- Re-creating the same durable name after unclean shutdown causes "consumer already exists" or config mismatch errors
- 100+ zombie consumers degrade NATS JetStream API performance

**Prevention:**
- Always set `InactiveThreshold` on durable consumers (e.g., 1 hour) so abandoned consumers self-clean
- Use a `defer consumerCtx.Stop()` + `<-consumerCtx.Closed()` pattern in the materializer Run loop
- Implement a graceful shutdown with two phases: (1) stop accepting new messages, (2) drain in-flight handlers, (3) close consumer
- Track consumer creation time and version in consumer metadata for debugging
- On startup, verify the consumer config matches expectations; recreate if incompatible (NATS 2.11+ supports editable consumers)
- Wrap the consume callback in a recovery middleware that catches panics and ack/nak appropriately

```go
// Correct shutdown pattern (from ebind worker):
cons, _ := stream.CreateOrUpdateConsumer(ctx, jstream.ConsumerConfig{...})
cc, _ := cons.Consume(func(msg jetstream.Msg) {
    // handle message
})
// On shutdown:
cc.Stop()
<-cc.Closed() // Wait for dispatch goroutine to exit
```

**Phase to address:** Phase 2 (Core Materializer) — consumer setup is the first thing built.

**Warning signs:**
- `nats consumer list` shows consumers without active subscriptions
- Goroutine profiles show increasing goroutine count on consumer restart
- Streams can't be deleted because "consumers exist"

---

### Pitfall 5: SQL Parser — Null Handling and Type Coercion Surprises

**What goes wrong:** SQL queries produce wrong results because null comparison semantics, type coercion, or edge cases in the SQL parser are handled incorrectly.

**Why it happens:**
- SQL NULL is not zero, not empty string — it's "unknown". `NULL = NULL` is false in standard SQL, but `NULL IS NULL` is true
- Type coercion: `WHERE age > '10'` — should this compare as number or string?
- Reserved words: column names like `group`, `order`, `select`, `from` break naive parsers
- String comparison vs numeric comparison have different ordering (ASCII vs numerical)
- Empty result sets vs zero rows: `COUNT(*)` on no rows returns 0, not NULL
- LIMIT with OFFSET: `LIMIT 5 OFFSET 10` on a 12-row dataset returns 2 rows, not an error

**Consequences:**
- Query results that violate user expectations (wrong results)
- Parse errors on valid SQL (reserved words used as column names)
- Silent data type coercion (string "100" compared numerically gives unexpected results)
- NULL-containing rows are excluded from results when they should be included (or vice versa)

**Prevention:**
- Define an explicit type system from day one: what types does natsql support? (string, int64, float64, bool, timestamp)
- Each column in the schema declaration MUST have an explicit type — no inference
- Document NULL handling semantics clearly: all comparisons with NULL return false (three-valued logic is a non-goal for v1, treat as falsy)
- For reserved words: implement quoted identifier support (`SELECT "group" FROM ...`)
- Type coercion rules: only coerce within the same type family (int → float OK, string → int NOT OK without explicit cast)
- Implement a SQL compliance test suite covering: NULL comparison, type coercion edge cases, reserved word handling, empty results, LIMIT/OFFSET boundary cases

**SQL edge cases that MUST be tested:**
```sql
SELECT * FROM view WHERE col = NULL       -- returns no rows (NULL is not = anything)
SELECT * FROM view WHERE col IS NULL      -- correct way to check null
SELECT * FROM view WHERE col = ''          -- string empty, not null
SELECT * FROM view ORDER BY col LIMIT 0    -- returns all rows (LIMIT 0 = no limit in some dialects, but should return empty in ours)
SELECT * FROM view LIMIT 10 OFFSET 1000    -- empty result, not error
SELECT "from" FROM view                    -- quoted reserved word as column
SELECT * FROM view WHERE name = 'O''Brien' -- escaped single quote
```

**Phase to address:** Phase 4 (SQL Parser & Query Executor) — but type system must be defined in Phase 1 (Schema Design).

**Warning signs:**
- SQL parsing tests reveal unexpected edge cases
- Users report queries that work in PostgreSQL but fail in natsql
- NULL-containing rows silently disappear from results

---

## Moderate Pitfalls

---

### Pitfall 6: KV Bucket Key Design — Length Limits and Character Restrictions

**What goes wrong:** Keys that exceed NATS limits or contain invalid characters cause `Put()` to fail silently or data to not be retrievable.

**Why it happens:**
- JetStream KV keys can only contain: `a-z`, `A-Z`, `0-9`, `_`, `-`, `.`, `=`, `/`
- Keys cannot start or end with `.` and cannot contain `..`
- The underlying stream subject format is `$KV.<bucket>.<key>` — the total subject length limit (typically 4KB in NATS) applies
- KV bucket name also has restrictions: alphanumeric, dashes, underscores only

**Consequences:**
- `Put()` returns `ErrInvalidKey` for keys with spaces or special characters
- Primary keys with `/` or `.` in the value get silently corrupted (interpreted as subject hierarchy)
- Very long keys cause subject too long errors
- Data written with invalid keys can't be listed or retrieved through `Keys()`

**Prevention:**
- Encode all primary key values: URL-encode or base62-encode keys that contain invalid characters
- Or better: use a deterministic hash of the PK as the KV key, store the original PK in the value
- Document the key format clearly so users don't assume arbitrary keys work
- Validate keys at schema definition time, not at write time
- Test with PKs that contain dots, slashes, spaces, mixed case, and unicode

**Phase to address:** Phase 1 (Schema Design) — key encoding strategy must be defined upfront.

**Warning signs:**
- `ErrInvalidKey` errors during materialization
- Keys with special characters are missing from query results
- Subject hierarchy confusion (key `a.b` interpreted as hierarchical)

---

### Pitfall 7: JetStream KV History Limit and Delete Marker Pollution

**What goes wrong:** Materialized views accumulate delete markers for every row mutation, causing unbounded growth in the underlying stream and eventually exceeding limits.

**Why it happens:**
- NATS KV keeps history per key (default 1, max 64 via `KeyValueMaxHistory`)
- `Delete()` places a delete marker — it does NOT remove the previous value
- `PurgeDeletes()` only removes delete markers, not the historical data
- `Purge()` removes all previous revisions but leaves a delete marker
- Over time, the stream backing the KV bucket grows without bound as rows are updated and deleted

**Consequences:**
- KV bucket hits `MaxBytes` and refuses new writes (if `DiscardNew`) or silently drops old data (if `DiscardOld`)
- `Keys()` and `ListKeys()` return stale keys (deleted keys that still have delete markers)
- Watchers receive delete marker events for keys that were deleted long ago
- Stream storage grows unbounded

**Prevention:**
- Set `History: 1` on materialized view KV buckets (v1 doesn't need history — it's a snapshot)
- Implement periodic `PurgeDeletes()` calls for the bucket
- Consider using a separate stream for the changelog and purging the KV bucket regularly (rebuild from changelog)
- Monitor KV bucket storage with `Status()` and alert on approaching `MaxBytes`
- On materializer startup, run a consistency pass that purges stale delete markers

**Phase to address:** Phase 2 (Core Materializer) — bucket configuration.
Also include Phase 6 (Operations & Maintenance) for the PurgeDeletes sweep.

**Warning signs:**
- KV bucket `Values()` count keeps growing even though row count is stable
- `nats kv history <bucket> <key>` shows many delete markers
- Stream storage for the KV bucket exceeds 2× the expected data size

---

### Pitfall 8: File Descriptor and Memory Exhaustion from KV Watches

**What goes wrong:** Using `Watch()` or `WatchAll()` on large KV buckets creates an internal consumer on the underlying stream. Each watch opens a subscription and maintains in-memory state. With many watchers (one per SQL query connection or per materialized view), resource exhaustion follows.

**Why it happens:**
- `Watch()` creates an underlying JetStream consumer (ephemeral) on `$KV.<bucket>.>`
- Each watch has in-memory buffers for pending updates
- `WatchAll()` with `IncludeHistory` delivers ALL historical values on initialization — for a 50K-row bucket with history=64, that's 3.2M messages
- `Watch()` without `UpdatesOnly` delivers current values for all matching keys on startup
- Each active query connection might open its own watch

**Consequences:**
- NATS connections hit max subscription limits
- Client memory exhaustion from buffered watch updates
- NATS server strain from many consumers on the same stream
- Slow startup as each new watcher replays the entire bucket state

**Prevention:**
- Avoid `Watch()` for query execution — use direct `Get()` calls for point lookups and indexed scans
- Reserve `Watch()` for the materializer (it needs to react to changes) and for user-facing subscription features
- When watching, always use `UpdatesOnly` — never replay current state via a watch
- Rate-limit the number of concurrent watchers per process
- For the materializer itself, consume from the source stream directly (JetStream consumer on the original stream), not from the KV bucket

**Phase to address:** Phase 3 (Query Path) — query execution should not use Watch().

**Warning signs:**
- High memory usage on startup when many views materialize
- NATS connection errors about "too many subscriptions"
- Slow watch initialization times

---

### Pitfall 9: Schema Evolution — Breaking Changes Require Full Rematerialization

**What goes wrong:** A user adds a column to their materialized view definition. Existing KV entries don't have the column. The materializer doesn't know how to backfill.

**Why it happens:**
- KV bucket entries are opaque byte arrays — there's no schema attached to stored data
- The materializer processes new events forward but doesn't rewrite existing entries
- There is no ALTER VIEW in v1 — schema is defined at view creation time
- Adding a column means old entries lack it, producing NULL/missing values in queries

**Consequences:**
- Queries on old data return NULL for new columns (user confusion)
- Users must manually delete and recreate the materialized view
- During the re-materialization window, the view is unavailable
- If indexes reference the new column, old entries are not indexed, producing incomplete query results

**Prevention:**
- Store a schema version number with each KV entry (in the value envelope, not the key)
- On read, check schema version; if old, apply upgrade logic on-the-fly (e.g., populate default value)
- Document that schema changes require re-materialization (delete bucket, recreate, replay from stream start)
- For v1: keep schema immutable after view creation. Adding a column = new view definition with new bucket name.
- Provide a `natsqlctl rebuild --view myview` command that drops and recreates from scratch
- Always store the view definition version in a well-known KV key (`__meta__`) for introspection

**Phase to address:** Phase 1 (Schema Design) — schema immutability is a v1 constraint.
Phase 6 (Operations) — rebuild command.

**Warning signs:**
- Users ask "how do I add a column to my view?"
- Schema changes produce inconsistent query results

---

### Pitfall 10: SQL Injection via Materialized Column Values

**What goes wrong:** A streaming event contains a value that, when rendered into a SQL query response, causes the SQL parser downstream to misinterpret it.

**Why it happens:**
- natsql is a read-only query engine (no DML), so classic SQL injection (DROP TABLE, etc.) is impossible
- However, if column values are embedded in query responses without proper encoding, a downstream tool that interprets the response as SQL could be vulnerable
- More critically: if the materializer stores column names derived from event data (e.g., dynamic columns), a malicious event could inject a column name that conflicts with internal metadata

**Consequences:**
- (Low risk) A SQL client tool downstream could misinterpret response data
- (Medium risk) Column names from event data could cause parse errors or shadow real columns
- (High risk for embedded use) If natsql's own query engine uses string interpolation for building filters

**Prevention:**
- Always use parameterized queries internally — never build filter strings via concatenation
- Validate and escape column names derived from event data
- Schema declarations should fix column names, not derive them from event payloads
- In the query response format, use a structured format (JSON/MessagePack) rather than rendering into SQL
- Add integration tests that verify event payloads with special characters, quotes, and backslashes are handled correctly

**Phase to address:** Phase 4 (SQL Parser & Query Executor) — query building.
Phase 1 (Schema Design) — fixed column names.

**Warning signs:**
- Column names containing special characters cause parser errors
- String values containing single quotes break query results

---

### Pitfall 11: Index Bucket Write Amplification

**What goes wrong:** A single event that updates one row causes N+1 KV bucket writes (1 for data + N for indexes). With R1=3 replication, that's 3×(N+1) Raft writes across the NATS cluster. For tables with 5+ indexes, write amplification is 18×.

**Why it happens:**
- Each KV bucket is a replicated stream. Each write goes through Raft consensus.
- NATS KV buckets default to R1 (single replica) but production deployments use R3.
- Each index is a separate KV bucket (or a subject prefix in the same bucket).
- Writing to N indexes means N independent stream publishes, each replicated.

**Consequences:**
- Throughput drops dramatically as indexes are added (5 indexes = 1/5 the write throughput)
- NATS cluster disk I/O increases proportionally to index count
- Event processing latency spikes under load
- Users add indexes to speed up reads but unknowingly slow down writes

**Prevention:**
- Measure: benchmark write throughput with 0, 1, 3, 5 indexes to establish the curve
- Document: "each index doubles write cost" — users should only index columns they actually query
- Consider batching: collect multiple events and flush indexes in a single batch (trades latency for throughput)
- Consider composite indexes over multiple single-column indexes
- For v1: limit the number of indexes per view (e.g., max 5) to prevent unbounded amplification
- Use the same KV bucket for data + index (different key prefixes like `data/<pk>` and `idx/<col>/<val>/<pk>`) to collapse writes into one stream — but this has tradeoffs (bucket size management, watch filtering)

**Phase to address:** Phase 3 (Index Writer) — index strategy must account for write amplification.

**Warning signs:**
- Write throughput drops significantly as indexes are added
- NATS server disk write IO is the bottleneck
- Event processing latency grows linearly with index count

---

### Pitfall 12: JetStream Consumer MaxDeliver vs Application-Level Retry Mismatch

**What goes wrong:** A transient processing error causes NATS to redeliver the message, but the consumer's `MaxDeliver` cap kicks in before the application's retry policy exhausts, causing premature message termination.

**Why it happens:**
- The NATS consumer has a `MaxDeliver` config that limits total delivery attempts
- The application may have its own retry logic (e.g., Nak with delay)
- If `MaxDeliver` is too low, the consumer stops redelivering before the application's retry policy would give up
- Default `MaxDeliver` is -1 (unlimited) for new consumers, but older setups or push consumers may have lower defaults

**Consequences:**
- Events are silently dropped (land on the dead letter queue or are just lost)
- Materialized view diverges from the event stream
- Users think an event was processed when it was not
- Debugging is difficult because the message was terminated at the infrastructure level

**Prevention:**
- Set `MaxDeliver: -1` (unlimited) on the materializer consumer and control retries at the application level
- OR set `MaxDeliver` to a value that is safely above the maximum expected retry attempts
- Log a critical alert when a message reaches `MaxDeliver` — this should never happen in normal operation
- Implement circuit breaker logic at the application level for non-transient errors (schema violations, etc.) rather than relying on MaxDeliver to stop retries
- When Term() is needed, always provide a descriptive error (e.g., `msg.TermWithReason("invalid schema version")`)

**Phase to address:** Phase 2 (Core Materializer) — consumer configuration.

**Warning signs:**
- Messages disappear from the stream without being processed
- Materializer logs show "max delivery attempts reached" advisories

---

### Pitfall 13: Late-Arriving Events and Out-of-Order Materialization

**What goes wrong:** An event with timestamp T1 arrives and materializes. Later, an event with timestamp T0 (earlier) arrives and overwrites the state, producing an incorrect snapshot.

**Why it happens:**
- JetStream subjects with wildcard consumers may receive events from multiple producers in non-chronological order
- Network partitions, producer retries, or clock skew cause events to arrive at the stream out of order
- The materializer processes events in delivery order, not event time order
- A late event can "rewrite history" silently

**Consequences:**
- Materialized view shows a state that never existed in real time
- Downstream consumers of the materialized view make decisions based on incorrect state
- Audit trail is broken (state regresses to an earlier time)
- Detection is very difficult — the state looks plausible but is wrong

**Prevention:**
- Use event-time ordering: include a timestamp in each event, and maintain a per-key "last updated" timestamp in the KV entry
- Skip events that are older than the current stored value for that key (last-writer-wins with known timestamp)
- For v1: document that events are processed in stream-order, and client must ensure producers send events in timestamp order per key
- If cross-key ordering matters, use a single producer or a partitioned key strategy
- Add a metric: number of late-arriving events skipped — if this is >0, alert
- Implement a "watermark" mechanism: track the maximum event time seen, reject events more than X minutes behind

**Phase to address:** Phase 2 (Core Materializer) — event ordering strategy.

**Warning signs:**
- Metrics show late events being skipped
- State appears to "go backwards" after a new event
- Users report "impossible" state transitions

---

### Pitfall 14: Full Materialization on Startup — Warmup Time Blowup

**What goes wrong:** Every time the materializer restarts, it reads the entire source stream from the earliest unacknowledged message to rebuild its in-memory state. With hundreds of thousands of events, this takes minutes.

**Why it happens:**
- Durable consumer resumes from last ack — if the last ack was early (or if the consumer was recreated), it replays the entire stream
- The materializer must apply every event to the KV bucket
- For a stream with 500K events at 1KB each, that's 500MB of data to read and 500K KV writes
- With R3 replication, that's 1.5M replicated writes

**Consequences:**
- Materializer takes 5-30 minutes to become ready after restart
- During warmup, queries return stale or empty results
- Rolling upgrades cause extended downtime
- Crash-recovery-loop: if the materializer crashes during warmup, it restarts and begins again

**Prevention:**
- Store materializer checkpoint: regularly persist the last-processed stream sequence to a dedicated KV key
- On startup, query the last-processed sequence and create the consumer with `DeliverByStartSequence`
- Implement a "snapshot + delta" approach: periodically snapshot the entire KV bucket to a single compressed object, then replay only events after the snapshot
- For large streams: use stream sources/mirrors to pre-filter events before they reach the materializer
- Add a health check endpoint that returns `not_ready` until the materializer has caught up
- Set `MaxAckPending` to control the replay burst — don't try to replay 500K events in parallel (exhausts NATS server)

**Phase to address:** Phase 2 (Core Materializer) — checkpoint/tracking mechanism.
Phase 5 (Snapshots) — snapshot-based recovery.

**Warning signs:**
- Materializer startup takes >1 minute
- Crash-restart cycles take increasingly long
- Queries during startup return stale results

---

### Pitfall 15: Go Graceful Shutdown — MessageInProgress Acknowledgment Race

**What goes wrong:** On shutdown, the materializer stops accepting new messages but still has in-flight handlers. If the process exits before handlers complete, the unacknowledged messages are re-delivered. If the handlers complete but their acks are lost, the messages are also re-delivered.

**Why it happens:**
- JetStream push consumer delivers messages asynchronously via callback
- The materializer's shutdown sequence must: stop consumer → drain in-flight → exit
- If these steps race or skip a step, messages get re-delivered
- For pull consumers, the `Fetch()` loop must be explicitly stopped
- The `Ack()` call is async — the process may crash before the ack reaches the server

**Consequences:**
- At-least-once delivery causes duplicate processing on restart
- With non-idempotent processing (see Pitfall 3), this corrupts the materialized state
- In-flight work is silently dropped (messages are neither acked nor nacked)

**Prevention (from ebind's proven pattern in worker.go):**

```go
// Correct shutdown sequence:
// 1. Set "stopping" flag so callbacks fast-path Nak instead of starting new work
// 2. Stop() the consumer (non-blocking, stops dispatch goroutine)
// 3. <-Closed() on consumer — guarantees dispatch goroutine has exited
// 4. wg.Wait() drains spawned handler goroutines
// 5. Timeout: if handlers don't finish within ShutdownGrace, log error but exit

stopping atomic.Bool
// In consume callback:
if stopping.Load() {
    msg.Nak()  // Don't start work on a shutting-down process
    return
}
sem <- struct{}{}
if stopping.Load() {
    <-sem
    msg.Nak()  // Re-check after acquiring sem
    return
}
wg.Add(1)
go func() {
    defer func() { <-sem; wg.Done() }()
    handle(msg)
}()
```

- Use `AckSync()` for critical events where duplicate processing is costly
- Set a reasonable `ShutdownGrace` timeout (30-60s) to allow in-flight handlers to complete
- Log a warning when shutdown timeout is hit — this indicates handlers are too slow or stuck

**Phase to address:** Phase 2 (Core Materializer) — shutdown handling.

**Warning signs:**
- On restart, the same events are processed again (duplicate KV writes)
- Shutdown takes longer than expected
- "Shutdown timeout" warnings in logs

---

### Pitfall 16: NATS JetStream Object Store as Alternative to KV

**What goes wrong:** Someone proposes using JetStream Object Store for materialized values (because it handles larger values). This doesn't work for the query workload because object store lacks CAS, has no `Update()`, and has extremely high latency per operation.

**Why this is a trap:**
- Object Store is designed for blob storage (images, files), not indexed data
- No CAS update — only Put/Delete
- No per-key revision tracking for conditional updates
- Much higher latency per operation (store splits objects into chunks, each stored as a separate message)
- No `Keys()` listing — only a list of object names via internal stream inspection

**Prevention:**
- Simply don't use Object Store for materialized views. KV bucket is the correct primitive.
- If values exceed the 1MB default `MaxMsgSize`, increase `MaxValueSize` on the KV bucket config (up to 8MB typically, 64MB max)
- If values are larger than 8MB, store them externally and keep a reference + checksum in KV
- Document this: "natsql uses JetStream KV buckets, data size per row is limited by MaxValueSize"

**Phase to address:** Phase 1 (Schema Design) — document this in the project FAQ.

**Warning signs:**
- Developer asks "should we use Object Store for this?"
- Values approaching the KV value size limit

---

### Pitfall 17: No Encryption or Authentication on Query Path

**What goes wrong:** A SQL query endpoint exposed via NATS request-reply or HTTP returns data without authentication or encryption.

**Why it happens:**
- The query endpoint is a NATS subscription on a request subject
- Any NATS client that can connect to the cluster can send queries
- HTTP endpoint, if enabled, may be bound to all interfaces without auth

**Consequences:**
- Unauthorized users can read all materialized data
- NATS subject permissions (if configured) are the only access control
- In embedded mode, any code with access to the NATS connection can query

**Prevention:**
- For standalone mode: always enable NATS authentication (JWT/tokens)
- For NATS subjects: restrict query subject access via NATS account permissions (the `pub` permission on the query subject)
- For HTTP: bind to localhost by default, require an API key header
- For embedded mode: the parent process controls access — document that the embedder is responsible for access control
- Never expose the query endpoint to untrusted networks without auth
- Add a configuration option to require a query token

**Phase to address:** Phase 4 (Query Path) — query endpoint security.

**Warning signs:**
- HTTP endpoint bound to `0.0.0.0:0` in production
- No authentication check on the query subject

---

## Minor Pitfalls

---

### Pitfall 18: Using KV `Keys()` for Large Buckets

The `Keys()` method loads all keys into memory (a `[]string`). For buckets with 100K+ keys, this can OOM. Use `ListKeys()` (streaming channel) instead.

**Prevention:** Always prefer `ListKeys()` over `Keys()` in production paths.

**Phase:** Phase 2 (Core Materializer) — code review point.

---

### Pitfall 19: NATS Server Subject Length Limit

The total subject for a KV entry is `$KV.<bucket>.<key>`. NATS has a subject length limit (typically 4KB). Combined with the prefix, very long keys (2KB+) will fail.

**Prevention:** Document key length limits. Hash long keys. Test with maximum-length keys.

**Phase:** Phase 1 (Schema Design).

---

### Pitfall 20: Consumer FilterSubject Changes Not Applied

Editing a consumer's `FilterSubject` requires recreating the consumer in older NATS versions (<2.11). Creating a consumer with the same durable name but different filter silently fails or merges.

**Prevention:** Use `CreateOrUpdateConsumer` (available from NATS 2.11+). For older versions, delete and recreate. In tests, always delete consumers at cleanup.

**Phase:** Phase 2 (Core Materializer) — consumer lifecycle.

---

### Pitfall 21: Nil Entry in Watcher Initialization

When using `Watch()` without `UpdatesOnly`, the watcher sends a nil entry to signal "initial values complete". Code that iterates `Updates()` must handle nil entries, not just range over the channel blindly.

**Prevention:** Always:
```go
for entry := range watcher.Updates() {
    if entry == nil {
        continue // Initial values complete
    }
    // process entry
}
```

**Phase:** Phase 2 (Core Materializer) — watch handling.

---

### Pitfall 22: Clock Skew in Timestamp-Based Operations

If events carry wall-clock timestamps from different producers, clock skew causes incorrect event ordering and materialization. Uses with 5 minutes of drift produce "future" events that get processed before "past" events from other producers.

**Prevention:** Use JetStream sequence numbers for ordering, not timestamps. If timestamps are required (event-time queries), document that producers must have synchronized clocks (NTP). Add a warning when received timestamps are >30s in the future or >5m in the past.

**Phase:** Phase 2 (Core Materializer).

---

## Phase-Specific Warning Summary

| Phase | Topic | Likely Pitfall | Mitigation |
|-------|-------|---------------|------------|
| Phase 1: Schema Design | Type system | Pitfall 5 (null/coercion) | Explicit types from day 1 |
| Phase 1: Schema Design | Key encoding | Pitfall 6 (key limits) | Hash/encode PKs |
| Phase 1: Schema Design | Schema immutability | Pitfall 9 (evolution) | Version stamp, rebuild command |
| Phase 2: Core Materializer | Consumer config | Pitfall 4 (zombie consumers) | InactiveThreshold, proper stop |
| Phase 2: Core Materializer | Shutdown | Pitfall 15 (ack race) | ebind shutdown pattern |
| Phase 2: Core Materializer | Event ordering | Pitfall 3, 13 (replay/late) | Sequence tracking, idempotent |
| Phase 2: Core Materializer | Warmup | Pitfall 14 (full replay) | Checkpoint + snapshot strategy |
| Phase 3: Index Writer | CAS races | Pitfall 2 (index-data mismatch) | Consistent write strategy |
| Phase 3: Index Writer | Write amplification | Pitfall 11 (index cost) | Measure, limit, batch |
| Phase 4: SQL Parser | Edge cases | Pitfall 5, 10 (SQL bugs) | Compliance test suite |
| Phase 4: Query Path | Watch misuse | Pitfall 8 (watch exhaustion) | Direct Get, not Watch |
| Phase 4: Query Path | Security | Pitfall 17 (no auth) | Auth gates on query path |
| Phase 5: Snapshots | Snapshot strategy | Pitfall 14 (warmup) | No blocker — Phase 5 is the fix |
| Phase 6: Operations | Delete markers | Pitfall 7 (marker pollution) | PurgeDeletes sweep |
| Phase 6: Operations | Schema rebuild | Pitfall 9 (evolution) | rebuild command |

---

## Sources

- [NATS JetStream KV Store Docs](https://docs.nats.io/nats-concepts/jetstream/key-value-store) — HIGH confidence
- [NATS KV Developer Guide](https://docs.nats.io/using-nats/developer/develop_jetstream/kv) — HIGH confidence
- [NATS Consumer Configuration](https://docs.nats.io/nats-concepts/jetstream/consumers) — HIGH confidence
- [NATS Stream Configuration](https://docs.nats.io/nats-concepts/jetstream/streams) — HIGH confidence
- [nats.go kv.go source](https://github.com/nats-io/nats.go/blob/main/jetstream/kv.go) — HIGH confidence (valid key regex, KeyValueMaxHistory=64, Keys() returns []string)
- [nats.go issues: KV keys bugs](https://github.com/nats-io/nats.go/issues?q=is%3Aissue+kv+keys) — MEDIUM confidence (ListKeysFiltered returns deleted keys, KV watcher fails on large buckets, Watch blocks forever)
- [ebind worker.go](https://github.com/f1bonacc1/ebind/blob/main/worker/worker.go) — HIGH confidence (shutdown pattern reference)
- [ebind CLAUDE.md](https://github.com/f1bonacc1/ebind/blob/main/CLAUDE.md) — HIGH confidence (CAS patterns, consumer lifecycle, dedupe window)
- [ebind scheduler.go](https://github.com/f1bonacc1/ebind/blob/main/workflow/scheduler.go) — HIGH confidence (CAS retry, sweep pattern)
- JetStream Model Deep Dive — HIGH confidence (ack types, consumer state)
- SQL standard (NULL handling, type coercion) — HIGH confidence (language standard)
- KSQL operational experience — MEDIUM confidence (general stream processing wisdom)
- rqlite operational patterns — MEDIUM confidence (Raft write amplification is well-documented)
