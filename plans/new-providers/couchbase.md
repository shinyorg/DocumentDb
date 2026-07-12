# Plan — Couchbase provider (`Shiny.DocumentDb.Couchbase`)

## Why / fit

Couchbase is a major JSON document database whose **SQL++ (N1QL)** query language gives the fullest
server-side pushdown of the five — including **GroupBy + aggregates**, projections, and rich `WHERE`
— alongside native **FTS (full-text + geo)**, native **vector search** (7.6+), a native per-document
**CAS** token, and genuine **distributed ACID transactions** (→ a *real* unit of work, not
compensating). It's the natural "enterprise document DB" addition.

**Archetype:** document-native / NoSQL with a SQL++ emitter (conceptually like the Cosmos SQL
emitter, not the relational `IDatabaseProvider`). `CouchbaseDocumentStore : DocumentProviderBase,
IDocumentStore, …`.

## Dependencies & shape

- NuGet: `CouchbaseNetClient` (official SDK 3.x; distributed transactions are built in). Pin centrally. TFM net10.0. `ProjectReference` → core.
- Files: `CouchbaseDocumentStore.cs` (+ `.Temporal.cs`, `.Geometry.cs`, `.Backup.cs` partials), `CouchbaseDocumentQuery.cs`, `CouchbaseDocumentStoreOptions.cs`, `CouchbaseSqlPlusPlusEmitter.cs` (Expression→SQL++ `WHERE`/`ORDER BY`/`GROUP BY`), `CouchbaseDocument.cs`, `ServiceCollectionExtensions.cs`.
- **AOT:** `CouchbaseNetClient` is not trim/AOT-clean — annotate, don't claim `IsAotCompatible`.

## Storage model

- Couchbase hierarchy: **cluster → bucket → scope → collection**. Map a document type to a **collection**
  (`scope.<typeName>`), created lazily; or a single collection with a `__typeName` discriminator (choose
  per-collection for clean SQL++ + GSI indexing). Document **key** = the id string (or `<typeName>::<id>`
  in single-collection mode).
- Store the POCO JSON **natively** (Couchbase is JSON-native). Bookkeeping fields `__typeName`,
  `__version` inside the doc; Couchbase's own **CAS** value is the native concurrency token.
- **Indexing:** SQL++ needs GSI indexes for anything beyond primary-key lookups. Create a primary index
  per collection at init (dev) and GSI indexes from `MapIndexedProperty` (`CREATE INDEX … ON <coll>(field)`).
  Document that production users should curate indexes rather than rely on the primary index.

## CRUD, ids, CAS

- **Ids**: Guid→`N`; String→caller-assigned; Custom→converter; **Int/Long autogen supported** via Couchbase
  atomic **counter documents** (`Binary.IncrementAsync` on `seq::{typeName}`) — a differentiator like Redis.
- **Upsert** = KV `UpsertAsync` with client-side RFC-7396 merge, or SQL++ `UPDATE … SET`; prefer KV upsert for single-doc.
- **CAS**: two-layer — map `MapVersionProperty` → `__version` field for cross-provider parity, and back it with
  Couchbase's **native CAS**: pass the loaded `Cas` into `ReplaceAsync(..., options.Cas(cas))`; a
  `CasMismatchException` → `ConcurrencyException`. This is the cleanest CAS of the five (AzureTable-ETag-like but per-doc).

## Query (`CouchbaseDocumentQuery<T>`) — SQL++ pushdown

Translate `Expression` → SQL++ via `CouchbaseSqlPlusPlusEmitter`, execute with the Query service. SQL++
is expressive, so pushdown is the broadest of the candidates:

| Surface | Pushdown | Notes |
|---|---|---|
| `Where` (comparisons, and/or/not, `IN`, `LIKE`, `ANY … SATISFIES` for arrays) | ✅ SQL++ `WHERE` | string funcs `LOWER`/`UPPER`/`SUBSTR`/`CONTAINS`, date funcs, math — map to SQL++ scalar funcs |
| `OrderBy` / `OrderByDescending` | ✅ `ORDER BY` | needs GSI for performance |
| `Paginate(offset, take)` | ✅ `LIMIT … OFFSET` | |
| `Count` / `Any` | ✅ | `SELECT COUNT(*)` / `LIMIT 1` |
| `Sum`/`Avg`/`Min`/`Max` | ✅ | SQL++ aggregates |
| `GroupBy` + `Count/Sum/Avg/Min/Max` (+ Having) | ✅ **server-side** | SQL++ `GROUP BY … HAVING` — **full pushdown**, matching the relational tier, unlike Mongo/Cosmos client-side |
| `Select` / `Project` | ✅ | native SQL++ projection |
| `ExecuteDelete` / `ExecuteUpdate` | ✅ | SQL++ `DELETE`/`UPDATE … WHERE` |
| `ToCursorPage` | ✅ | keyset via `WHERE key > @after ORDER BY key LIMIT n` |
| String `Query(whereClause)` / `QueryStream` / `Count(whereClause)` | ✅ | **lower `FilterExpressionParser` output into the same SQL++ emitter** — Couchbase can genuinely honor the string grammar (like relational providers), a step up from Mongo (which throws). Also `ToQueryString` returns the emitted SQL++. |

Untranslatable nodes throw `NotSupportedException` (steer to `FullTextSearch`/`Geo*`/`NearestVectors`).

## Capability interfaces

| Interface / feature | Support | Mechanism |
|---|---|---|
| `IUnitOfWorkEngine` | ✅ **real ACID** | Couchbase **distributed transactions** (`cluster.Transactions.RunAsync`) — atomic across the batch, *not* compensating. Headline. |
| `IExplicitTransactionEngine` (`session.BeginTransaction`) | ✅ | wrap a transaction attempt around the session's buffered writes |
| `SupportsFullText` / `FullTextSearch` / `FullTextMatch` / OrderByScore | ✅ native | Couchbase **FTS (Search service)**; `MapFullTextProperty` → search index |
| `SupportsVector` / `NearestVectors` | ✅ (7.6+) | FTS **vector search**; `MapVectorProperty` → vector-enabled search index |
| `SupportsSpatial` / `Geo*` | ✅ partial | FTS geo (point/radius, bounding box, polygon); `MapSpatialProperty`. Rare OGC predicates throw. |
| `IObservableDocumentStore.NotifyOnChange` | ✅ | in-process `ChangeBroadcaster` |
| `IChangeFeedDocumentStore` | ⚠️ v2 | Couchbase's change stream is **DCP**, not surfaced by the core .NET SDK (needs a separate/lower-level client). **Decline native change feed in v1** (`SupportsChangeFeed` effectively false); revisit with a DCP client. Document the gap. |
| `ITemporalDocumentStore` | ⚠️ sidecar | no native history; append-only `<coll>_history` collection like the Mongo sidecar — implement for parity or defer to v2 |
| `IDocumentBackup` | ✅ | streaming SQL++ scan export / KV upsert import |
| `IDocumentMaintenance.ClearAll` | ✅ | `DELETE FROM <coll>` per collection / flush |
| `MapComputedProperty` | ✅ | alias via `ComputedReadBack`; materialized → a real field + GSI |

## Implementation phases

1. **Skeleton + KV CRUD + ids (incl. counter autogen) + native-CAS + DI** (`AddCouchbaseDocumentStore`; accept a pre-built `ICluster`/`IBucket` or connection string + bucket/scope + credentials). CRUD conformance green.
2. **Index management** (primary index + GSI from `MapIndexedProperty`, lazy).
3. **SQL++ emitter**: Where/OrderBy/paging/Count/aggregates/projection/ExecuteDelete/ExecuteUpdate + **server-side GroupBy/Having** + string-grammar lowering.
4. **Real UoW / transactions** via Couchbase distributed transactions.
5. **FTS**: full-text, geo, vector terminators.
6. **Backup + maintenance**. Temporal sidecar + DCP change feed → v2.

## Testing

- Testcontainers: `couchbase` image (there's a `Testcontainers.Couchbase` module that provisions bucket + services). Fixture implements `IDocumentStoreFixture`; wire into conformance suite. FTS/vector need the Search service enabled in the container — gate those tests on service availability.
- Provider-specific: SQL++ `ToQueryString` pushdown incl. **server-side GroupBy/Having**, native-CAS → `ConcurrencyException`, distributed-transaction UoW atomicity, FTS full-text/geo/vector, counter-based Int/Long autogen, string-grammar → SQL++ parity.

## Four-artifact sync

1. **Code + tests** above. Release note tier: document-native, **SQL++ full pushdown (incl. GroupBy) + real ACID transactions + FTS/vector**; no native change feed in v1.
2. **Docs**: `couchbase.mdx` — collections/scopes + GSI indexing model, SQL++ pushdown table, transactions, FTS/vector; capability-matrix updates; `<RN type="feature">`.
3. **Skill**: add Couchbase + `AddCouchbaseDocumentStore` to `triggers:`/provider list; note SQL++ GroupBy pushdown + ACID UoW + no v1 change feed.
4. **readme.md**: provider list + capability callout.

## Risks / open questions

- **No first-class change feed** in the .NET SDK (DCP is separate) — the one notable gap; be explicit in docs and consider a follow-up DCP-based `IChangeFeedDocumentStore`.
- SQL++ **index dependency**: queries without a supporting GSI are slow or rejected — surface Couchbase's index-advisor guidance and auto-create primary index only in dev.
- SQL++ requires an eventual-consistency/scan-consistency choice (`RequestPlus` for read-your-writes) — pick a safe default (`RequestPlus` after writes) and expose it in options.
- Distributed transactions have their own consistency/retry semantics — validate they compose with our `IUnitOfWorkEngine` buffering model.
