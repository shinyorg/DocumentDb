# Architecture and provider implementation

Read this reference only when implementing a provider or custom transport, investigating storage/index internals, or diagnosing legacy data. Ordinary application CRUD, queries, and configuration do not require it. These implementation descriptions do not expand the supported application API.

## Contents

- Temporal storage layout
- Temporal storage keys and indexes
- Spatial storage and index synchronization
- Mapped geometry and index selectivity
- Vector storage and external-write drift
- SQLite vector deletion and initialization
- Writing a provider (12.0+)
- Concurrency Model
- Hosting the store yourself (Shiny.DocumentDb.Hosting, 13.2+)

## Temporal storage layout

Implemented on **every** provider. Each persists versions to its own sidecar: relational stores (SQLite, SQLCipher, PostgreSQL, SQL Server, MySQL, Oracle, DuckDB) → `{table}_history` table; LiteDB / MongoDB → `{collection}_history` collection; CosmosDB → `{container}_history` container (partitioned by `/typeName`); IndexedDB → `{store}_history` object store.

## Temporal storage keys and indexes

- On the relational providers the sidecar PK is `(Id, TypeName, Version)` with `(TypeName, ValidFrom, ValidTo)` and `(TypeName, Actor)` secondary indexes backing the fleet-wide queries; the document stores model the same versions natively and compute the selection in the provider.

## Spatial storage and index synchronization

### How It Works

- **SQLite**: Creates R*Tree sidecar tables (`{table}_spatial` and `{table}_spatial_map`) that are automatically synced on insert/update/upsert/remove/clear. Bounding box pre-filter via R*Tree, then Haversine post-filter for exact radius.
- **CosmosDB**: `GeoPoint` serializes as GeoJSON `{"type":"Point","coordinates":[lng,lat]}`. Spatial index policies are added to the container automatically. Queries use native `ST_DISTANCE` and `ST_WITHIN` functions.

### Spatial CRUD Sync

Spatial sidecar data is automatically maintained — no manual steps needed:
- **Insert/Update/Upsert**: Extracts `GeoPoint` from the document and upserts into spatial index
- **Remove**: Deletes spatial data for that document
- **Clear**: Removes all spatial data for that type

## Mapped geometry and index selectivity

- **Only the mapped geometry is queryable (v13+).** Every spatial surface — `store.Geo*`, `DocumentFunctions` in `Where`/`OrderBy`, and the string grammar — must name the property passed to `cfg.MapSpatialProperty`. A second `Geometry?` property stores and round-trips normally but is **not** spatially queryable, and naming it throws `NotSupportedException` ("… is not a mapped spatial property … Mapped: 'area'"). Most relational providers answer these predicates from the sidecar's geometry column, which holds only the mapped property, so an unmapped path cannot be served honestly. When a document really has several shapes: **one semantic slot in several pieces** (a service area of three disjoint polygons) → one property typed `GeoMultiPolygon`/`GeoGeometryCollection`, whose union envelope is the right index key; **genuinely distinct slots** (a delivery's origin *and* destination) → the union envelope would span both and destroy index selectivity, so map the one you search by and keep the other as plain data.
- **`GeoDisjoint`** is anti-selective — it scans the type (O(n)) on SQLite/refine paths. Use sparingly on large corpora.
- **Fidelity:** SQLite / refine-path distances are Haversine/planar approximations; native `ST_DISTANCE` is geodesic. Ordering can differ on near-ties across providers.

## Vector storage and external-write drift

On the relational providers the embedding is stored **twice**: in the document body (it is an ordinary serialised property) and in a per-type `{table}_vec_{type}` sidecar, which is what an ANN query actually searches. The library keeps the two in step on every write. Anything that writes the row *without* going through the library — a hand-written `UPDATE`, a restore, another tool — leaves the sidecar holding the old embedding, and the search then returns wrong answers without erroring. `ShinyDocDbMyAdmin`'s **Vectors** tab reports that drift and rebuilds the sidecar from the document bodies. Providers expose the sidecar name as `IDatabaseProvider.VectorTableName(table, type)` and its current ids as `BuildVectorDocIdsSql(table, type)`.

## SQLite vector deletion and initialization

**SQLite deletion consistency:** use the normal query, batch and session deletion APIs; the provider keeps vector sidecars in sync transactionally and repairs legacy orphan mappings when initializing the mapped tables. Do not add consumer-side vector-table SQL or delete valid documents to repair an index. The SQLite path uses commands/savepoints, not virtual-table triggers, and works with `trusted_schema=OFF`. Transactional `Clear<T>` preserves tenant/query-filter boundaries. Disabled vector support skips cleanup; enabling it later initializes sidecars but does not backfill existing embeddings. Use distinct document tables for types whose sanitized vector table names collide; all relational providers reject colliding mapped sidecar names (case-insensitive) at configuration time. SQLite additionally rejects ambiguous legacy orphan candidates before repair, without rejecting unrelated non-vector types. Transactional custom-table initialization also provisions vector storage and is retried after rollback; committed transactional initialization is cached separately from full parent initialization.

## Writing a provider (12.0+)

The nine document providers share one `IDocumentQuery<T>` implementation. A provider derives from public **`DocumentQueryBase<T>`** and implements four members — `Clone()`, `ExecuteAsync(QueryPlan<T>)`, `DeleteMatchingAsync`, `SetPropertyMatchingAsync` — and inherits builder state/immutability, query-filter resolution, all client-side terminals, grouping, cursor paging, string projection, and the set-based-write interceptor plumbing. `ExecuteAsync` returns `QueryExecution<T>.Candidates` (nothing applied server-side), `.Filtered`, `.Complete`, or `.Partial(...)`; the base applies only what the engine did not. **Report push-down honestly** — claiming ordering/paging you did not apply silently returns wrong results. Optional hooks (`SetPropertiesMatchingAsync` for multi-property `ExecuteUpdate`, `ObserveChanges`, `FullTextSearchCore`, `NearestVectorsCore`, `ToQueryString`, `ToCursorPage`) and the aggregate hooks (`CountCore`/`MaxCore`/…) have safe defaults; override only what the engine can do itself. On the options side implement **`IDocumentStoreOptions`** — `Mappings`, `TypeNameResolution`, `Capabilities`, the interceptor pair, and `SerializerOptions`/`EnsureSerializerOptions` — and `ConfigureDocument<T>` plus every cross-cutting feature (soft delete, `cfg.MapJsonSchema`, field encryption) lights up for free. All per-type mapping state lives in the shared `DocumentMappingRegistry`; `Capabilities` is what the configuration validation pass reads to reject mappings your backend cannot honor.

Store writes bracket persistence with the shared pipeline on `DocumentProviderBase` (implement `Mappings`, `IdCache`, `ResolveTypeInfo`, `ResolveDocumentTypeName`): `BeginWriteAsync(op, doc, id, typeInfo, ct)` → check `write.Proceed` (false = an interceptor replaced the write; `Remove` returns `write.CancelResult`), take `write.Doc`, get the id with `ResolveInsertId`/`ResolveInsertIdAsync` (insert) or `RequireDocumentId` (update), persist, then `CompleteWriteAsync(write, id, version, changeType, doc, ct)` which runs `AfterWrite` and publishes the change (buffered until commit inside a unit of work). Never re-implement `PublishChange` — the base owns the broadcaster.

## Concurrency Model

A single `DocumentStore` instance is safe to share across threads on every provider; what differs is how operations are serialized internally.

| Provider | Connection model | Concurrency on one store |
|---|---|---|
| SQLite, SQLCipher, DuckDB | Single long-lived `DbConnection` + `SemaphoreSlim` (shared mode) | Ops queue on the semaphore. The underlying engines lock the whole database on writes, so multi-flighting buys nothing. |
| PostgreSQL, MySQL, SQL Server, Oracle | Per-op `DbConnection` opened from the ADO.NET driver pool | Ops execute concurrently up to the pool's max size. No store-level semaphore. |
| CosmosDB, MongoDB | Provider's documented thread-safe client (`CosmosClient`, `IMongoClient`) | Ops execute concurrently. Clients are pooled internally. |
| LiteDB, IndexedDB | Single-process / single-tab engines | Concurrent multi-process or multi-tab writes are not safe. |

Providers opt into shared mode via `IDatabaseProvider.RequiresSingleConnection => true` (SQLite and DuckDB do so today). Custom providers default to pooled mode.

Table init (`CREATE TABLE IF NOT EXISTS`, index DDL, tenant column/index, spatial sidecars) is exactly-once per table per process — backed by a `ConcurrentDictionary<string, Lazy<Task>>`. Concurrent first-touch callers wait on the same init task; failures evict the cached task so the next call can retry.

## Hosting the store yourself (Shiny.DocumentDb.Hosting, 13.2+)

Only for building a **host** — a package that exposes the store over a transport ASP.NET Core cannot reach (an embedded server in a MAUI app, a custom protocol, a message pump). Application code never needs this namespace; use `Where`, `Query` and the endpoints packages instead.

```csharp
using Shiny.DocumentDb.Hosting;

// A scope must be checked against an INCOMING document (POST/PUT), before there is anything to query.
Expression<Func<Order, bool>> scope = x => x.TenantId == tenantId;
var inScope = DocumentPredicate.Compile(scope);   // delegate, and NEVER Reflection.Emit
query = query.Where(scope);                       // same expression pushed down for reads
if (!inScope(incoming)) return BadRequest();      // enforced in memory for writes

// Apply ?filter= to changes arriving on a live tail (they never pass through a query).
var filter = DocumentPredicate.Compile(DocumentFilter.Parse(f, AppJsonContext.Default.Order));
var jsonFilter = DocumentPredicate.Compile(DocumentFilter.ParseJson("score:number > 5"));   // JSON collection

// ETag / If-Match: does this type have a mapped version property?
var version = store.GetVersionMapping(typeof(Order));   // null → no ETag to publish
var etag = version?.GetVersion(order);
var mappings = store.GetMappings();                     // DocumentMappingRegistry, any provider
```

Rules:

- **Never call `Expression.Compile()` in a host.** It emits IL at runtime and forfeits the trimmed/AOT guarantee of the hosting app. `DocumentPredicate.Compile` is the AOT-safe replacement — that it generates no code is the contract.
- **Never write a second in-memory evaluator.** Push down and in-memory checks must be the same semantics, or a scope the query filters on can be waved through in memory — a scope bypass.
- **Do not ask for `InternalsVisibleTo`.** These three types are the whole supported host surface; anything else in `Shiny.DocumentDb.Internal.*` is not a contract.
- `DocumentFilter.ParseJson(filter, typeInfo: null)` needs `path:type` hints (`"score:number > 5"`); pass a `JsonTypeInfo` when the collection has a document type and names/leaf types resolve as on the typed lane.
- `GetMappings` returns `null` only for a custom `IDocumentStore` implemented outside this library — that means "unknown", not "nothing mapped".
