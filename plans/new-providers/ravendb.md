# Plan — RavenDB provider (`Shiny.DocumentDb.RavenDb`)

## Why / fit

RavenDB is a .NET-native, JSON document database whose feature set maps onto our contract more
completely than any other candidate: native **revisions** (→ temporal), a native **Changes API +
data subscriptions** (→ change feed), native **full-text**, **spatial**, and (7.x) **vector**
indexing, native per-document **change vectors/ETags** (→ CAS), and a genuinely **ACID session**
(→ a real unit of work rather than the compensating pattern Mongo/Dynamo use). Audience overlap with
a .NET library is near-total. This is the richest-fit provider of the five and the one that can
light up the most capability interfaces.

**Archetype:** document-native / NoSQL. `RavenDbDocumentStore : DocumentProviderBase, IDocumentStore, …`.

## Dependencies & shape

- NuGet: `RavenDB.Client` (pin centrally in `Directory.Packages.props`). TFM `$(BaseTargetFramework)` (net10.0). `ProjectReference` → `..\Shiny.DocumentDb`.
- Project files: `RavenDbDocumentStore.cs` (+ `.Temporal.cs`, `.ChangeFeed.cs`, `.Geometry.cs`, `.Backup.cs` partials), `RavenDbDocumentQuery.cs`, `RavenDbDocumentStoreOptions.cs`, `RavenExpressionVisitor.cs` (Expression→RQL WHERE), `RavenDbDocument.cs` (metadata field constants), `ServiceCollectionExtensions.cs`.
- **AOT caveat:** `RavenDB.Client` is reflection-heavy and not trim/AOT-clean — annotate our code as usual, but do **not** claim `IsAotCompatible`. Store JSON via our `JsonTypeInfo<T>` path and hand Raven pre-serialized documents where possible to keep our serialization deterministic (Raven's own Newtonsoft-based serializer differs from System.Text.Json).

## Storage model

RavenDB is JSON-native and organizes documents into **collections** by type. Use a collection per
document type (`@metadata.@collection = <typeName>` from `TypeNameResolver`), document id
`"<typeName>/<id>"`. Store the POCO JSON **directly** (no `data` envelope wrapper needed — Raven is a
document store, not a blob store), with our bookkeeping in metadata:

- `@metadata.@collection` = resolved type name (overridable via `MapTypeToCollection<T>`)
- `@metadata.shiny-version` = mapped optimistic-concurrency version (when `MapVersionProperty`)
- Raven's own `@metadata.@change-vector` is the native CAS token.

Serialize with our `JsonSerializerOptions`, store through the low-level API to avoid Raven's
serializer reinterpreting our types (use `session.Advanced.Defer` with a `PutCommandData` carrying a
`BlittableJsonReaderObject` built from our JSON, or `bulk-insert` for batch). Deserialize by reading
the raw JSON back through System.Text.Json.

## CRUD, ids, CAS

- **Ids**: Guid→`N`; String→caller-assigned; Custom→converter. **Int/Long**: RavenDB has a native HiLo generator — we *could* support Int/Long autogen cheaply via HiLo, but for v1 keep parity with Mongo (throw) and note HiLo as a follow-up.
- **Insert/Update/Upsert/Get/Remove**: one open session per operation (or reuse the UoW session — see below). Upsert = load-then-RFC-7396-merge like Mongo, or use Raven `Patch` for server-side merge.
- **CAS**: map `MapVersionProperty` to a stored `shiny-version` field guarded on write; additionally leverage Raven's native optimistic concurrency (`session.Advanced.UseOptimisticConcurrency = true` compares change vectors) so conflicting writes throw → translate `ConcurrencyException`. Prefer version-field guard for cross-provider parity, back it with the change vector.

## Query (`RavenDbDocumentQuery<T>`)

Translate our `Expression<Func<T,bool>>` to **RQL** (`RavenExpressionVisitor`) and execute via
`session.Advanced.RawQuery<T>` / `AsyncDocumentQuery`. RQL is expressive, so pushdown is broad:

| Surface | Pushdown | Notes |
|---|---|---|
| `Where` (comparisons, and/or/not, string Contains/StartsWith/EndsWith, `in`) | ✅ RQL `where` | strings → `startsWith()`/`endsWith()`/`search()` |
| `OrderBy` / `OrderByDescending` | ✅ RQL `order by` | |
| `Paginate(offset, take)` | ✅ `limit` | Raven caps page size; stream for large |
| `Count` / `Any` | ✅ | `count()` / limited query |
| `Select` (projection) | ✅ RQL `select` | native projections |
| `GroupBy` + `Count/Sum/Avg/Min/Max` | ✅ RQL `group by` | **can push down** — differentiator vs Mongo (client-side). Start client-side (reuse `InMemoryGroupedQuery`) to ship, then push to RQL. |
| `ToCursorPage` | ⚠️ | Raven paging is skip/take + `MoreResultsAvailable`; implement keyset over the id/order field, else client-side pager like Mongo |
| `ExecuteDelete` / `ExecuteUpdate` | ✅ | RQL `update`/`delete by query` (patch-by-query) |
| String `Query(whereClause, …)` / `QueryStream` | ✅ | RavenDB is one of the few NoSQL backends where the **string grammar can lower to RQL** — worth wiring `FilterExpressionParser` output through the same visitor. If deferred, throw like Mongo and note it. |

Untranslatable nodes throw `NotSupportedException` (steer to `FullTextSearch`/`Geo*`), matching Mongo.

## Capability interfaces

| Interface / feature | Support | Mechanism |
|---|---|---|
| `IDocumentStore` (CRUD/query/batch) | ✅ | bulk-insert for `BatchInsert` |
| `IUnitOfWorkEngine` | ✅ **real ACID** | one Raven session; `SaveChanges` is atomic across the batch — *not* compensating. Big win over Mongo/Dynamo. |
| `IExplicitTransactionEngine` (`session.BeginTransaction`) | ✅ optional | Raven cluster-wide transactions; enable if we want `session.BeginTransaction` to be real |
| `ITemporalDocumentStore` | ✅ native | enable Raven **Revisions** on the collection; `History`/`AsOf` via `session.Advanced.Revisions.GetFor/GetAsOf`; `Restore` via revert. Far cleaner than the Mongo sidecar. `ChangesByActor` needs actor captured into metadata on write. |
| `IChangeFeedDocumentStore` | ✅ native | Raven **Changes API** (`store.Changes().ForDocumentsInCollection<T>()`) or **data subscriptions** for durable feeds. Map to `DocumentChange<T>`. |
| `IObservableDocumentStore.NotifyOnChange` | ✅ | in-process `ChangeBroadcaster`; optionally back with Changes API |
| `SupportsFullText` / `FullTextSearch` | ✅ native | Raven auto/static indexes with `search()`; `MapFullTextProperty` → index fields |
| `SupportsVector` / `NearestVectors` | ✅ (Raven 7.x) | native `vector.search()`; `MapVectorProperty` → vector index field |
| `SupportsSpatial` / `Geo*` | ✅ native | Raven spatial indexes (`spatial.within`, `spatial.contains`, distance sort); `MapSpatialProperty` |
| `IDocumentBackup` | ✅ | stream export/import over collections (or defer to Raven Smuggler; prefer our streaming contract) |
| `IDocumentMaintenance.ClearAll` | ✅ | delete-by-query over all collections |
| `MapComputedProperty` | ✅ | apply via `ComputedReadBack` on read; materialized indexed variant via a Raven index field |

## Implementation phases

1. **Skeleton + CRUD + ids + CAS + DI** (`AddRavenDbDocumentStore(opts)`; accept a pre-built `IDocumentStore` (Raven) or connection URL + database + optional X.509 cert). Conformance CRUD green.
2. **Query**: `RavenExpressionVisitor` → RQL for Where/OrderBy/paging/Count/Any/ExecuteDelete/ExecuteUpdate; projections; client-side GroupBy first.
3. **Real UoW** via a single Raven session (`IUnitOfWorkEngine`), then optional `IExplicitTransactionEngine`.
4. **Temporal** via Revisions.
5. **Change feed** via Changes API + in-process broadcaster.
6. **Full-text, spatial, vector** indexes (`MapFullTextProperty`/`MapSpatialProperty`/`MapVectorProperty`).
7. **Backup + maintenance**, then push GroupBy/string-grammar down to RQL.

## Testing

- Testcontainers: `RavenDbContainer` (official image `ravendb/ravendb`). Fixture implements `IDocumentStoreFixture` + `IAsyncLifetime`; wire into the shared conformance suite.
- Provider-specific: RQL `ToQueryString` pushdown assertions, revisions/temporal, Changes-API feed, native optimistic-concurrency → `ConcurrencyException`, full-text/vector/spatial.

## Four-artifact sync (per CLAUDE.md)

1. **Code + tests** above. Note provider tier in the release note (document-native, near-full capability).
2. **Docs**: new `ravendb.mdx` provider page; update `querying.mdx`/`orleans.mdx` capability matrices; add `<RN type="feature">` under the current `version.json` version (strip prerelease suffix).
3. **Skill**: add RavenDB + `AddRavenDbDocumentStore` to `SKILL.md` `triggers:` and provider list; note it's one of the few with native temporal/change-feed/vector.
4. **readme.md**: add RavenDB to the provider list + capability callout.

Optional: Orleans grain-storage integration (`Shiny.DocumentDb.Orleans.RavenDb`) mirroring the Mongo/Cosmos Orleans packages — separate follow-up.

## Risks / open questions

- Raven's Newtonsoft serializer vs our System.Text.Json — must store/read raw JSON to stay deterministic and AOT-consistent. Prototype the low-level Put/read path first.
- `RavenDB.Client` is not AOT/trim-clean — scope out NativeAOT claims.
- Licensing: RavenDB community license terms for CI/Testcontainers — confirm the test image runs unlicensed for the conformance suite.
