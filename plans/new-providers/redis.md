# Plan — Redis provider (`Shiny.DocumentDb.Redis`)

## Why / fit

Redis is the ubiquitous hot/caching-tier data store, and **Redis Stack** (RedisJSON + RediSearch —
now folded into Redis 8) turns it into a genuine queryable document store. It's the only candidate of
the five where **GroupBy pushes down server-side** (RediSearch `FT.AGGREGATE`), and it brings native
**full-text**, **vector (HNSW/FLAT KNN)**, and **geo** query in one engine, plus cheap atomic
**`INCR`** counters that let us support **Int/Long autogen** where every other NoSQL provider throws.

**Archetype:** document-native / NoSQL over Redis Stack. `RedisDocumentStore : DocumentProviderBase,
IDocumentStore, …`.

> **Hard requirement:** RedisJSON + RediSearch modules (Redis Stack / Redis ≥ 8, or Azure Managed
> Redis / Redis Enterprise with the modules). At init, probe with `FT._LIST` / module list and throw
> a clear, actionable error if the modules are absent. Plain Redis is not supported.

## Dependencies & shape

- NuGet: `NRedisStack` (official, over `StackExchange.Redis`). Pin centrally. TFM net10.0. `ProjectReference` → core.
- Files: `RedisDocumentStore.cs` (+ `.ChangeFeed.cs`, `.Backup.cs` partials), `RedisDocumentQuery.cs`, `RedisDocumentStoreOptions.cs`, `RedisSearchQueryBuilder.cs` (Expression→RediSearch query + `FT.AGGREGATE`), `RedisDocument.cs` (key/schema helpers), `ServiceCollectionExtensions.cs`.
- **AOT:** `StackExchange.Redis` is reasonably trim-friendly; our JSON path stays `JsonTypeInfo`-first. Don't over-claim AOT until measured.

## Storage model

- Each document = a RedisJSON key `doc:{typeName}:{id}` set via `JSON.SET`. Body is the POCO JSON.
- One **RediSearch index per type**, created at first use: `FT.CREATE idx:{typeName} ON JSON PREFIX 1 doc:{typeName}: SCHEMA …`. The schema declares the **queryable** fields — this is exactly the `MapIndexedProperty` surface: only declared fields are filterable/sortable (TAG for exact/`in`, NUMERIC for ranges/sort, TEXT for full-text, GEO for spatial, VECTOR for KNN). Fields not in the schema are still stored but only filterable client-side.
- Version/bookkeeping fields (`__version`, `__typeName`) live inside the JSON.
- Index creation is lazy + cached per type (like Mongo's ftsIndexed/geoIndexed dictionaries).

## CRUD, ids, CAS

- **Ids**: Guid→`N`; String→caller-assigned; Custom→converter; **Int/Long autogen supported** via `INCR` on a per-type counter key `seq:{typeName}` — a real differentiator (Dynamo/Azure throw here). Document that Int/Long ids are monotonic-per-store, not globally meaningful.
- **Upsert** = `JSON.SET` at root with RFC-7396 merge computed client-side, or `JSON.MERGE` (RedisJSON 2.6+) server-side — prefer `JSON.MERGE`.
- **CAS**: `MapVersionProperty` → `__version`; guard with a small **Lua script** doing read-version → compare → `JSON.SET` atomically (or `WATCH`/`MULTI`/`EXEC`). Version drift → `ConcurrencyException`.
- **SetProperty/RemoveProperty** → `JSON.SET`/`JSON.DEL` at the JSON path (native, targeted).

## Query (`RedisDocumentQuery<T>`)

Translate `Expression` → **RediSearch query string** + options (`RedisSearchQueryBuilder`), execute via
`FT.SEARCH` / `FT.AGGREGATE`. Pushdown is strong **for schema-declared fields**:

| Surface | Pushdown | Notes |
|---|---|---|
| `Where` on indexed fields: `==`/`in` (TAG), ranges (NUMERIC), `and`/`or`/`not` | ✅ `FT.SEARCH` | e.g. `@status:{open} @age:[30 +inf]` |
| `Where` on non-indexed fields | ⚠️ client-side | after a broader `FT.SEARCH`; document that unindexed predicates need `MapIndexedProperty` for pushdown |
| `OrderBy` / `OrderByDescending` | ✅ `SORTBY` | field must be `SORTABLE` in schema |
| `Paginate(offset, take)` | ✅ `LIMIT offset num` | |
| `Count` | ✅ | `FT.SEARCH … LIMIT 0 0` returns total |
| `Sum`/`Avg`/`Min`/`Max` | ✅ | `FT.AGGREGATE … REDUCE SUM/AVG/MIN/MAX` |
| `GroupBy` + aggregates | ✅ **server-side** | `FT.AGGREGATE … GROUPBY @field REDUCE COUNT/SUM/…` — **the differentiator**; Mongo/Cosmos/LiteDB do this client-side |
| `Select` / `Project` | ✅ | `RETURN`/`LOAD` field lists; computed-shape projection stays client-side |
| `FullTextSearch` / `FullTextMatch` / OrderByScore | ✅ native | TEXT fields, BM25 scoring |
| `NearestVectors` | ✅ native | VECTOR field, `KNN` query (HNSW/FLAT) |
| `WithinRadius` / `WithinBoundingBox` | ✅ | GEO field radius; `WithinBoundingBox`/polygon via `GEOSHAPE` (`WITHIN`/`CONTAINS`) |
| Full OGC `Geo*` set | ⚠️ | GEOSHAPE covers within/contains/intersects; the rarer OGC predicates throw |
| `ToCursorPage` | ⚠️ | `FT.SEARCH` has no keyset cursor; use `FT.AGGREGATE … WITHCURSOR` or a client-side pager |
| String `Query(whereClause)` / `QueryStream` | ✅ optional | can lower `FilterExpressionParser` output to a RediSearch string; if deferred, throw |

Untranslatable predicate nodes → `NotSupportedException` (steer to `FullTextSearch`/`Geo*`/`NearestVectors`), matching Mongo.

## Capability interfaces

| Interface / feature | Support | Mechanism |
|---|---|---|
| `IUnitOfWorkEngine` | ✅ atomic-apply | buffer writes, apply via `MULTI`/`EXEC` (atomic execution, no partial application) or a single Lua script. Not read-your-writes MVCC, but atomic apply beats compensating. |
| `IExplicitTransactionEngine` | ⚠️ | Redis transactions can't read-then-branch without `WATCH`; expose only if we can honor the semantics, else leave `BeginTransaction` throwing |
| `IChangeFeedDocumentStore` | ✅ (opt-in) | **keyspace notifications** (`notify-keyspace-events`, subscribe `__keyspace@0__:doc:{type}:*`) → map SET/JSON.SET → Inserted/Updated, DEL → Removed. Requires server config; probe and document. Alternative: mirror each change into a Redis **Stream** for a durable, replayable feed. |
| `IObservableDocumentStore.NotifyOnChange` | ✅ | in-process `ChangeBroadcaster` (+ optionally keyspace notifications) |
| `SupportsFullText` / `FullTextSearch` | ✅ native | RediSearch TEXT |
| `SupportsVector` / `NearestVectors` | ✅ native | RediSearch VECTOR KNN |
| `SupportsSpatial` / geo | ✅ partial | GEO + GEOSHAPE (point radius / within / contains) |
| `ITemporalDocumentStore` | ⚠️ optional | no native versioning; **Redis Streams** as an append-only history log `hist:{type}:{id}` (XADD per change) is a clean fit — implement or defer to v2 |
| `IDocumentBackup` | ✅ | stream scan (`FT.SEARCH`/`SCAN`) export; `JSON.SET` import |
| `IDocumentMaintenance.ClearAll` | ✅ | `SCAN`+`DEL` by `doc:*` prefix (+ drop indexes) |
| `MapComputedProperty` | ✅ | `ComputedReadBack` on read; materialized → an indexed schema field |

## Implementation phases

1. **Skeleton + module probe + CRUD (`JSON.SET`/`JSON.GET`/`JSON.MERGE`) + ids (incl. `INCR`) + Lua CAS + DI** (`AddRedisDocumentStore`; accept a pre-built `IConnectionMultiplexer` or connection string). CRUD conformance green.
2. **Index management** (`FT.CREATE` from `MapIndexedProperty`/`MapFullTextProperty`/`MapVectorProperty`/`MapSpatialProperty`, lazy+cached).
3. **Query**: `FT.SEARCH` Where/OrderBy/paging/Count; client-side fallback for unindexed predicates.
4. **`FT.AGGREGATE`**: server-side GroupBy + Sum/Avg/Min/Max (headline).
5. **Full-text, vector, geo** terminators.
6. **UoW (`MULTI`/`EXEC`)**, change feed (keyspace notifications / Streams), backup + maintenance. Temporal-via-Streams optional/v2.

## Testing

- Testcontainers: `redis/redis-stack` (or `redis/redis-stack-server`) image so RediSearch + RedisJSON are present. Fixture implements `IDocumentStoreFixture`; wire into conformance suite (skip/guard the full-text/vector/GroupBy tests that plain-Redis providers can't run — here they should *pass*).
- Provider-specific: `FT.SEARCH`/`FT.AGGREGATE` pushdown (`ToQueryString` = the emitted RediSearch query), server-side GroupBy, KNN vector, geo, `INCR` autogen, Lua CAS → `ConcurrencyException`, keyspace-notification change feed.

## Four-artifact sync

1. **Code + tests** above. Release note tier: document-native over **Redis Stack** (modules required); server-side GroupBy/full-text/vector/geo; Int/Long autogen supported.
2. **Docs**: `redis.mdx` — front-load the **Redis Stack / modules requirement** and the "declare indexed fields to get pushdown" model; capability matrix updates; `<RN type="feature">`.
3. **Skill**: add Redis + `AddRedisDocumentStore` to `triggers:`/provider list; note modules requirement + native GroupBy/full-text/vector.
4. **readme.md**: provider list + capability callout (+ modules requirement).

## Risks / open questions

- **Modules requirement** is the biggest adoption caveat — must be loud in docs and at init.
- RediSearch only queries **declared schema fields** — the "why isn't my Where pushing down?" pitfall. Mitigate with docs + a debug log when a predicate falls back client-side.
- Numeric/enum/`DateTime` encoding for TAG vs NUMERIC fields must match how RediSearch parses them — test enum-as-string vs int and `DateTime`-as-epoch carefully.
- Keyspace notifications require server config (`notify-keyspace-events`) and are best-effort (not durable) — offer the Streams-based feed as the durable option.
