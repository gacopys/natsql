# Feature Landscape: SQL-over-KV Materialized View Engines on NATS

**Domain:** Stream-to-KV materialized view engine
**Researched:** 2026-05-23
**Mode:** Ecosystem — Features dimension, five comparable systems analyzed

## System-by-System Analysis

### 1. ksqlDB (Kafka Streams → SQL)

**How it works:** ksqlDB wraps Kafka Streams behind a SQL interface. `CREATE TABLE AS SELECT` defines a materialized view (backed by a Kafka Streams state store). Pull queries (`SELECT ... WHERE key=val`) serve the current state from the local RocksDB store. Push queries (`SELECT ... EMIT CHANGES`) subscribe to ongoing changes.

**SQL Dialect:**
- `CREATE STREAM` for event streams, `CREATE TABLE` for tables with primary keys
- `CREATE TABLE AS SELECT` with aggregation (COUNT, SUM, AVG), windowing, GROUP BY, JOINs
- Pull queries: `SELECT ... FROM table WHERE key=literal` — strict subset of ANSI SQL
- Pull queries support equality on key, optionally range on non-key when table scans enabled, `LIMIT`
- Push queries: `SELECT ... FROM stream EMIT CHANGES` — continuous subscription

**Key Features:**
- Dual pull/push query model (request-response for state, streaming for subscriptions)
- Incremental updates via Kafka Streams (RocksDB state stores)
- Schema Registry integration (Avro, Protobuf, JSON Schema)
- Exactly-once semantics
- REST API + CLI + Java client
- Auto-scaling by adding ksqlDB nodes

**Pain Points (to avoid):**
- Deadlocks on concurrent pull queries (documented in their own docs)
- Table scans can cause OOM on large state stores
- `CREATE TABLE` (source) cannot be pull-queried — only `CREATE TABLE AS SELECT` tables are materialized
- Complex deployment with Kafka, Schema Registry, connect cluster
- RocksDB compaction can cause latency spikes during queries
- No cross-data-center replication built in
- Pull queries are key-lookup-only by default (table scans opt-in and slow)

**Table Stakes:**
- [ ] Declarative materialized view definition (CREATE TABLE AS SELECT)
- [ ] Pull queries (request/response) on materialized state
- [ ] Primary-key lookups
- [ ] Range scans on secondary fields
- [ ] Incremental, ordered consumption from stream source

**Differentiators:**
- Push queries / continuous subscriptions (EMIT CHANGES)
- Rich aggregation (COUNT, SUM, AVG, MIN, MAX)
- Multi-stream JOINs (stream-stream, stream-table, table-table)
- Windowed aggregation (tumbling, hopping, session windows)
- Schema Registry for type safety and evolution

**Anti-Features:**
- Full SQL DML (INSERT/UPDATE/DELETE) — ksqlDB is append-only; mutations go through Kafka
- Transactional multi-key writes
- Interactive query routing (handled transparently; user doesn't locate shards)

---

### 2. rqlite (SQLite + Raft)

**How it works:** rqlite sits a Raft consensus layer on top of SQLite. Every node has a full copy of the SQLite database. Writes go through the Raft log; reads can be served locally (weak) or through the leader (strong/linearizable). HTTP API for all operations.

**Query Routing:**
- Default: Follower transparently forwards reads/writes to Leader (weak consistency)
- `level=none`: reads from local SQLite (fast, potentially stale)
- `level=linearizable`: heartbeat quorum check before local read
- `level=strong`: read goes through Raft log (slow, don't use in prod)
- `redirect`: returns HTTP 301 with Leader address instead of proxying
- Read-only nodes: full SQLite copy, never participate in Raft; serve `none` reads

**SQL Subset:**
- Full SQLite SQL (FTS5, JSON1, RTREE, STRICT tables)
- No special SQL syntax for cluster awareness
- `PRAGMA` directives supported (except journal_mode, wal_checkpoint, synchronous)
- Parameterized statements (positional and named) via HTTP JSON API
- Multi-statement transactions via `?transaction` URL param
- No materialized views, no views at all — plain SQLite tables
- CDC via streaming the Raft log to external systems

**Key Features:**
- Drop-in SQLite replacement for HA
- Hot backups, automatic cluster formation (DNS, Consul, etcd, K8s)
- TLS + RBAC
- Queued writes for throughput
- Associative response format (map per row)
- Blob support (base64, byte array)

**Limits:**
- All writes go through single Leader (Raft bottleneck)
- No sharding — every node has full data copy
- No materialized views (everything is a table)
- No push queries / subscriptions (poll-based only)
- Storage = disk (SQLite file), not KV store
- Horizontal scaling for writes is effectively not possible
- Memory proportional to working set (SQLite page cache)

**Table Stakes:**
- HTTP API for reads/writes
- Cluster-level read consistency options
- SQL compatibility (standard SQL matters)

**Anti-Features:**
- Materialized views with incremental update — rqlite doesn't offer this
- Push-based subscriptions — not in the product
- Sharding or partitioning — deliberately avoided

---

### 3. Badger / Dgraph

**How it works:** Dgraph is a distributed graph database built on Badger (a Go embeddable KV store). It uses a custom GraphQL+- / DQL dialect for queries. The mapping from KV to query is: predicates stored as KV pairs, filtered through indexing (reverse, full-text, geo, etc.), and joined via query planning.

**DQL (Dgraph Query Language):**
- Not SQL — custom GraphQL-influenced syntax
- `{ q(func: eq(name, "Alice")) { uid name age } }` — root function + filters
- Root functions: `eq`, `allofterms`, `anyofterms`, `regexp`, `ge`, `le`, etc.
- Facets: key-value metadata on edges
- Reversal: `~predicate` to traverse reverse edges
- Variables and query blocks for joins
- No STANDARD SQL (intentional — graph queries need different syntax)

**KV Layer (Badger):**
- LSM-tree with WAL, compression, bloom filters
- Transactions (SSI isolation)
- Streaming backups
- Managed memory (no manual GC in Go)
- Direct Go API (can embed)

**Key Features:**
- Graph traversal as first-class query primitive
- Full-text, geo, and regex indexing
- Distributed query execution (any node can serve)
- Hot-data is memory-mapped; cold data on disk
- Go embeddable as a library

**Pain Points:**
- Not SQL — steep learning curve for SQL users
- Query planning can be unpredictable
- JOINs are graph traversals (different mental model)
- No streaming/changelog input — batch load or mutation API
- Dgraph as a project had multiple commercial/community shifts (Dgraph Labs → Dgraph)

**Relevance to natsql:**
- Badger's KV → query layer mapping shows it's feasible to build fast point-lookups and range scans on LSM KV stores
- Dgraph's indexing approach (reverse indexes per predicate) is analogous to what natsql needs for secondary indexes
- The complexity and non-standard query language is a caution: don't invent your own query language

**Anti-Features:**
- Proprietary query language — don't do this; natsql uses SQL
- Graph traversal as primary query primitive — wrong abstraction for KV
- Complex distributed query planning — stay single-node for v1

---

### 4. Materialize (Timely + Differential Dataflow)

**How it works:** Materialize ingests streaming data (Kafka, PostgreSQL CDC, MySQL CDC, webhooks) and maintains materialized views using Timely/Differential Dataflow. Results are incrementally updated and persisted in durable storage. Queries use standard PostgreSQL-compatible SQL.

**Materialized View Model:**
- `CREATE MATERIALIZED VIEW AS SELECT ...` — standard SQL syntax
- Incrementally updated (not batch-refreshed)
- Results persisted in durable storage (can survive cluster restart)
- Cross-cluster queryable (compute cluster maintains, serving clusters index)
- Supports indexes on materialized views for in-memory speed
- `CREATE VIEW` + `CREATE INDEX` = in-memory incremental (cheaper alternative)
- `REPLACEMENT MATERIALIZED VIEW` — zero-downtime view definition updates
- `REFRESH` strategies: `ON COMMIT` (default), `AT` (specific time), `EVERY` (cron-like schedule)

**SQL Subset:**
- Full SQL-92 + PostgreSQL compatibility
- Joins: all types (inner, left, right, full, cross, lateral)
- Aggregations: GROUP BY, COUNT, SUM, AVG, MIN, MAX, stddev, etc.
- Window functions (with limitations for streaming)
- Temporal filters (`mz_now()` for temporal windows)
- Subqueries, CTEs (including recursive CTEs)
- `SUBSCRIBE` — push-based subscription to view changes
- CDC sources (PostgreSQL, MySQL, MongoDB, SQL Server)
- Kafka/Redpanda sources, webhooks, load generators

**Key Features:**
- Strict serializable isolation by default
- PostgreSQL wire protocol — compatible with any Postgres tool (dbt, Tableau, Metabase)
- Multi-cluster architecture (source, transform, serving)
- Sinks (Kafka, S3, Iceberg, Snowflake)
- `EXPLAIN PLAN` / `EXPLAIN FILTER PUSHDOWN` for optimization
- `PARTITION BY` for internal data partitioning
- History retention for durable subscriptions (private preview)

**Pain Points (to avoid):**
- `CREATE MATERIALIZED VIEW` is expensive (persists to durable storage) — prefer `CREATE VIEW` + `CREATE INDEX` for single-cluster setups
- Hydration time after restart can be long (reading from durable storage)
- Memory proportional to working set in compute cluster
- Not all PostgreSQL features supported (e.g., UPDATABLE views)
- Complex deployments need 3-tier architecture (source/transform/serve)
- Requires dedicated infrastructure (Kubernetes for self-managed)
- Community Edition limited to 24 GiB memory, 48 GiB disk

**Relevance to natsql:**
- PostgreSQL wire protocol is table stakes for broad tool compatibility — but natsql v1 explicitly defers this
- Incremental materialized views on streaming data is exactly what natsql does, but at different scale/complexity
- The `CREATE VIEW` + index pattern vs `CREATE MATERIALIZED VIEW` — natsql's KV-backed approach is naturally the "indexed view" pattern (KV bucket IS the materialized state)
- `SUBSCRIBE` pattern is the "push query" analogue — natsql could add this post-v1

**Table Stakes:**
- Standard SQL (PostgreSQL dialect is the dominant expectation)
- Incremental view maintenance (not batch recalculation)
- Durable, restart-safe materialized state
- Query-anywhere from any cluster node

**Differentiators:**
- Full JOIN support (multi-way, non-windowed)
- Complex aggregations (GROUP BY with multiple functions)
- PostgreSQL wire protocol (massive tool ecosystem)
- Strict serializable isolation
- Sinks to external systems

**Anti-Features:**
- Materialize REQUIRES significant infrastructure (3 clusters, K8s, etc.) — overkill for many use cases
- Hydration latency from durable storage — natsql's KV buckets are faster to read but slower to write
- Multi-tier architecture is unnecessary complexity for simple stream→KV use cases

---

### 5. Kafka Streams Interactive Queries

**How it works:** Kafka Streams applications maintain state stores (RocksDB embedded, or in-memory) keyed by record keys. Interactive Queries exposes these state stores via a local API: `ReadOnlyKeyValueStore.get(key)`, `range(from, to)`, `all()`. Stores can be partitioned across instances. A `StreamsMetadata` API discovers which instance hosts which partition.

**API Patterns:**
```java
// Local state store access
ReadOnlyKeyValueStore<String, Long> store = 
    streams.store(StoreQueryParameters.fromNameAndType("counts", QueryableStoreTypes.keyValueStore()));
Long count = store.get("my-key");

// Range scan
KeyValueIterator<String, Long> range = store.range("a", "z");

// Partition metadata (for distributed queries)
Collection<StreamsMetadata> metadata = streams.allMetadataForStore("counts");
// StreamsMetadata has host(), port(), partitionCount(), etc.

// Custom state stores (supported but rare)
StoreBuilder<KeyValueStore<String, Long>> storeBuilder =
    Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore("my-store"), 
        Serdes.String(), Serdes.Long());
```

**Key Features:**
- `get(key)` — O(1) point lookup
- `range(from, to)` — ordered range scan
- `all()` — full table scan (use with caution)
- `approximateNumEntries()` — approximate cardinality
- Custom state store types: key-value, windowed, session, timestamped
- Custom serdes for any data format
- Co-located computation + query (no network hop for local data)
- gRPC/REST layer often built on top by application developers

**Pain Points:**
- No built-in query language — you must write Java code or build an API layer
- No ad-hoc SQL queries — all queries are programmatic
- Partition discovery requires custom infrastructure (routing layer)
- RocksDB compaction can block queries (observed latency spikes)
- No secondary indexes — only primary-key access patterns
- State store size limited by local disk (partition count * data per key)
- Rebalancing can make state temporarily unavailable
- Cross-partition queries require custom fan-out
- No easy CLI debugging — must inspect RocksDB SST files or expose an API

**Relevance to natsql:**
- The `get(key)` + `range(from, to)` pattern IS the query interface natsql needs for v1
- natsql wraps this in SQL, eliminating the need for Java code
- NATS KV buckets already provide this same `get/range` interface — no RocksDB needed
- The pain of writing custom routing/API layers is exactly what natsql solves
- The lack of secondary indexes in Kafka Streams is the #1 reason natsql needs them

**Table Stakes:**
- Primary-key point lookups (get by key)
- Range scans over sorted keys

**Differentiators:**
- Co-located compute + query (no extra network hop)
- Partition-aware routing (query reaches the right instance)

**Anti-Features:**
- Don't expose raw RocksDB/state store primitives — wrap in SQL
- Don't require custom code to query — declarative SQL or API call
- Don't force users to manage partition routing — abstract behind NATS request-reply or HTTP

---

## Cross-System Feature Matrix

### Feature: Declarative view definition

| System | Approach | Notes |
|--------|----------|-------|
| ksqlDB | `CREATE TABLE AS SELECT` | SQL DDL, rich semantics |
| rqlite | N/A | No materialized views — plain SQL tables |
| Dgraph | Schema definitions in DQL | Custom format, not standard SQL |
| Materialize | `CREATE MATERIALIZED VIEW AS SELECT` | SQL DDL, closest to rqlite |
| Kafka Streams | `KStream.toTable()`, Processor API | Programmatic, no SQL |
| **natsql v1** | YAML/JSON config | Explicitly NOT SQL DDL for v1 — simpler |

### Feature: Query interface

| System | Pull (Request/Response) | Push (Subscription) | Protocol |
|--------|------------------------|---------------------|----------|
| ksqlDB | `SELECT ... WHERE key=val` (Pull) | `SELECT ... EMIT CHANGES` (Push) | REST, WebSocket, CLI |
| rqlite | `SELECT` via HTTP GET/POST | No | HTTP/JSON |
| Dgraph | DQL via gRPC/HTTP | Subscriptions via DQL | gRPC, HTTP |
| Materialize | `SELECT` via pgwire | `SUBSCRIBE` | PostgreSQL wire |
| Kafka Streams | `store.get(key)` / `store.range()` | Custom (KStream.forEach, etc.) | Java API |
| **natsql v1** | `SELECT ... WHERE col=val` | No — deferred | NATS request-reply, HTTP |

### Feature: Index support

| System | PK Index | Secondary Index | Composite Index | Full-text |
|--------|----------|-----------------|-----------------|-----------|
| ksqlDB | Automatic (key) | Table scan only (non-key) | No | No |
| rqlite | SQLite indexes | SQLite indexes | SQLite | SQLite FTS5 |
| Dgraph | Automatic (UID) | Per-predicate | Reverse indexes | Yes |
| Materialize | Via `CREATE INDEX` | Via `CREATE INDEX` | Multi-column index | No |
| Kafka Streams | Automatic (key) | No | No | No |
| **natsql v1** | Automatic (KV key) | Single-column equality + range | No | No |

### Feature: Aggregation support

| System | COUNT | SUM | AVG | GROUP BY | Windowed |
|--------|-------|-----|-----|----------|----------|
| ksqlDB | Yes | Yes | Yes | Yes | Yes (tumbling, hopping, session) |
| rqlite | Yes | Yes | Yes | Yes | SQLite date functions |
| Dgraph | Count (graph-level) | No | No | No | No |
| Materialize | Yes | Yes | Yes | Yes | Yes + temporal filters |
| Kafka Streams | Yes | Yes | Yes | Yes | Yes (all window types) |
| **natsql v1** | No | No | No | No | No — explicitly deferred |

### Feature: JOIN support

| System | Stream-Stream | Stream-Table | Table-Table | Multi-way |
|--------|--------------|-------------|-------------|-----------|
| ksqlDB | Yes | Yes | Yes | Yes |
| rqlite | N/A | N/A | SQLite JOINs | Yes |
| Dgraph | N/A | N/A | Graph traversal | Graph traversal |
| Materialize | Yes (via sources) | Yes | Yes | Yes |
| Kafka Streams | Yes | Yes | Yes | Yes |
| **natsql v1** | No | No | No | No — explicitly deferred |

### Feature: Operational characteristics

| System | HA | Persistence | Latency | Throughput | Tooling |
|--------|----|-------------|---------|------------|---------|
| ksqlDB | Multi-node | RocksDB (disk) | Pull: low | Stream: high | REST, CLI, Java |
| rqlite | Raft | SQLite (disk) | Read: low | Write: medium | HTTP, CLI, libs |
| Dgraph | Raft + sharding | Badger (disk) | Query: medium | High | GraphQL, gRPC |
| Materialize | Multi-cluster | Durable storage | Query: low | Stream: high | pgwire, dbt, BI tools |
| Kafka Streams | Partitioned | RocksDB (disk) | Query: low | High | JMX, Java |
| **natsql v1** | NATS cluster | KV bucket (NATS) | Query: low | JetStream-bound | NATS req-reply, HTTP |

---

## Categorized Recommendations for natsql

### Table Stakes (Must Have or Users Leave)

These are non-negotiable features users expect from any state-queryable streaming system:

| Feature | Complexity | Notes | Required by |
|---------|------------|-------|-------------|
| Declarative view definition (config) | Low | YAML/JSON v1; SQL DDL v2 | All comparable systems |
| Ordered, durable stream consumption | Medium | JetStream consumer with resume | ksqlDB, Kafka Streams, Materialize |
| Primary-key point lookup (`WHERE pk = val`) | Low | NATS KV `Get()` → SQL wrapper | All |
| Range scan on primary key (`WHERE pk > val`) | Low | NATS KV `Keys()` with prefix/range | ksqlDB (opt-in), Kafka Streams, Badger |
| Single-column equality on non-key field | Medium | Requires secondary index | ksqlDB (table scan), Dgraph, Materialize |
| Single-column range on non-key field | Medium | Requires index on that column | Dgraph, Materialize (indexed) |
| `LIMIT` support | Low | Trivial on sorted KV iteration | ksqlDB, SQL everywhere |
| Crash recovery (durable consumer + state) | Medium | JetStream durable consumer + KV bucket | ksqlDB, Kafka Streams, Materialize |
| Request-reply query interface | Low | NATS request-reply | Kafka Streams (custom API), ksqlDB (REST) |
| HTTP/JSON query interface | Low | HTTP server wrapping SQL parser | rqlite, ksqlDB, Dgraph |
| Malformed event handling (log + skip) | Low | Don't crash on bad data | All production systems |
| Secondary index definition in config | Medium | YAML specifying indexed columns | Dgraph (schema), Materialize (`CREATE INDEX`) |

### Differentiators (Competitive Advantage)

These set natsql apart from alternatives and justify its existence:

| Feature | Complexity | Why Differentiating | Comparable |
|---------|------------|--------------------|------------|
| **Zero-infrastructure Kafka alternative** | System | No Kafka, no Postgres, no Schema Registry — just NATS | Everyone else requires Kafka/K8s |
| **Secondary indexes on KV state stores** | High | ksqlDB/Kafka Streams don't support this; Materialize does but with complexity | Materialize, Dgraph |
| **Go embeddable (library mode)** | Medium | Run query engine inside your Go process — no sidecar | Badger (KV only), not ksqlDB/Mat |
| **Minimal SQL that works out of box** | Low | Not full SQL-92 — SELECT + WHERE + LIMIT is enough for 90% of KV queries | Kafka Streams (no SQL), rqlite (full SQLite) |
| **NATS-native request-reply** | Low | Query via `nc.Request("natsql.query", sqlBytes)` — fits NATS ecosystem | ksqlDB REST, not NATS-native |
| **Declarative YAML/JSON config** | Low | No DDL parser for v1 — define view in a file | All others require SQL DDL |
| **Independent from Confluent/Kafka ecosystem** | System | Single dependency: NATS | Everyone depends on Kafka |
| **Embedded NATS support** | Low | Works with nats-server in-process | ksqlDB needs external Kafka |

### Anti-Features (Deliberately NOT Build for v1)

These are features the PROJET.md already defers OR that the systems research suggests are traps for a v1 product:

| Anti-Feature | Why Avoid | What Comparable System Tried |
|--------------|-----------|------------------------------|
| **Full pgwire / PostgreSQL protocol** | Massive surface area; many edge cases; v1 should prove the query engine first | Materialize does this well but it took them years |
| **DML via SQL (INSERT/UPDATE/DELETE)** | Blurs the write path; writes should flow through JetStream | ksqlDB doesn't support it; rqlite does but it's a different product |
| **JOINs across views/tables** | Query planning complexity; needs multi-KV-bucket coordination | Every system supports it but it's complex |
| **Aggregations (COUNT, SUM, GROUP BY)** | Needs state tracking across events; window management | ksqlDB/Mat do it well but it requires differential dataflow style |
| **Window functions / CTEs / subqueries** | Significant SQL parser complexity | Materialize supports, Kafka Streams doesn't |
| **Automatic schema inference** | Event format changes break queries; better to require explicit schema | ksqlDB/Mat use Schema Registry; natsql doesn't have one |
| **Transaction support across rows** | CAS-based KV doesn't support multi-key transactions | rqlite (Raft) does; natsql (NATS KV) can't |
| **Sharding / distributed query execution** | v1 should prove single-node correctness first | Dgraph and Kafka Streams do this but it's complex |
| **Custom RocksDB or LSM storage** | NATS KV is the storage layer; adding a second storage is scope creep | ksqlDB (RocksDB), Badger (LSM) — but NATS KV exists already |
| **Push queries / subscriptions** | Adds server-side complexity for maintaining query results over time | ksqlDB (`EMIT CHANGES`), Materialize (`SUBSCRIBE`) — defer |
| **Own query language** | Bad for adoption; stick to standard SQL subset | Dgraph got this wrong — hurts adoption |
| **Multi-stream Kafka-like input** | v1 = single stream per view. Multiple streams = complex ordering | ksqlDB supports multiple Kafka topics per stream |

### Feature Dependencies

```
View Config (YAML/JSON) 
  → JetStream Consumer 
    → Event deserialization + column mapping 
      → KV bucket write (key = pk, value = column family or encoded row)
        → Secondary index updates (write-through or async)
          → Query engine (SQL parse → index lookup → KV read → result)

Minimal viable chain: 
  Config → Consumer → KV write → SQL parse → PK lookup → result
  (secondary indexes optional for v1 MVP, essential for v1 GA)
```

### Phase Dependencies

```
Phase 1: Core pipeline
  Declarative view config → JetStream consumer → KV materialization
  → SQL equality on PK → LIMIT → request-reply + HTTP

Phase 2: Secondary indexes
  Index config → index maintenance on materialization → WHERE on non-key columns → range scans

Phase 3: Operational hardening  
  Crash recovery → graceful shutdown → metrics → malformed event handling

Phase 4: Embed mode 
  Go library API → in-process NATS → docs + examples

Phase 5: Enhanced queries (post-v1)
  Aggregation → JOINs → push queries → pgwire
```

### MVP Recommendation for Feature Parity

**Must ship (table stakes for being useful):**
1. YAML/JSON view config with stream name, key column, column mapping
2. JetStream durable consumer with ordered delivery
3. KV bucket materialization (key = primary key, value = encoded row)
4. `SELECT ... WHERE pk = val` (point lookup)
5. `SELECT ... WHERE pk > val AND pk < val` (range on PK)
6. `LIMIT N`
7. NATS request-reply + HTTP query endpoints
8. Crash recovery (durable consumer resumes; KV state persists)
9. Malformed event handling (log + skip, don't crash)

**Ship next (differentiators for v1):**
10. Secondary indexes (single-column equality + range)
11. Go embeddable library mode
12. Basic WHERE clause with AND (multiple conditions)

**Defer:**
- JOINs (needs query planner across buckets)
- Aggregations (needs state machine per group)
- Push queries (needs subscription model on query results)
- pgwire protocol (needs PostgreSQL protocol parser)
- Schema inference (needs Schema Registry equivalent)
- Multi-view transactions (not possible with NATS KV CAS semantics)

## Sources

- ksqlDB documentation: https://docs.ksqldb.io/en/latest/developer-guide/ksqldb-reference/select-pull-query/
- rqlite documentation: https://rqlite.io/docs/features/, https://rqlite.io/docs/api/api/, https://rqlite.io/docs/api/read-consistency/
- Materialize documentation: https://materialize.com/docs/sql/create-materialized-view/, https://materialize.com/docs/concepts/views/, https://materialize.com/docs/get-started/
- Kafka Streams documentation: https://docs.confluent.io/platform/current/streams/developer-guide/interactive-queries.html
- Dgraph documentation: https://dgraph.io/docs/ (archived docs used for pattern reference)
- natsql PROJECT.md: /home/pawel/repo/natsdb/.planning/PROJECT.md

**Confidence:** MEDIUM-HIGH — All feature claims verified against official documentation except Dgraph (site returned 404 for specific DQL pages; Dgraph patterns come from archived knowledge). Kafka Streams Interactive Queries documentation was partially fetched; API patterns confirmed from public API documentation.
