# Shiny.DocumentDb

[![NuGet](https://img.shields.io/nuget/v/Shiny.DocumentDb.svg?label=Core)](https://www.nuget.org/packages/Shiny.DocumentDb/)
[![NuGet](https://img.shields.io/nuget/v/Shiny.DocumentDb.Sqlite.svg?label=SQLite)](https://www.nuget.org/packages/Shiny.DocumentDb.Sqlite/)
[![NuGet](https://img.shields.io/nuget/v/Shiny.DocumentDb.Sqlite.SqlCipher.svg?label=SQLCipher)](https://www.nuget.org/packages/Shiny.DocumentDb.Sqlite.SqlCipher/)
[![NuGet](https://img.shields.io/nuget/v/Shiny.DocumentDb.MySql.svg?label=MySQL)](https://www.nuget.org/packages/Shiny.DocumentDb.MySql/)
[![NuGet](https://img.shields.io/nuget/v/Shiny.DocumentDb.MariaDb.svg?label=MariaDB)](https://www.nuget.org/packages/Shiny.DocumentDb.MariaDb/)
[![NuGet](https://img.shields.io/nuget/v/Shiny.DocumentDb.SqlServer.svg?label=SQL+Server)](https://www.nuget.org/packages/Shiny.DocumentDb.SqlServer/)
[![NuGet](https://img.shields.io/nuget/v/Shiny.DocumentDb.PostgreSql.svg?label=PostgreSQL)](https://www.nuget.org/packages/Shiny.DocumentDb.PostgreSql/)
[![NuGet](https://img.shields.io/nuget/v/Shiny.DocumentDb.CockroachDb.svg?label=CockroachDB)](https://www.nuget.org/packages/Shiny.DocumentDb.CockroachDb/)
[![NuGet](https://img.shields.io/nuget/v/Shiny.DocumentDb.Oracle.svg?label=Oracle)](https://www.nuget.org/packages/Shiny.DocumentDb.Oracle/)
[![NuGet](https://img.shields.io/nuget/v/Shiny.DocumentDb.LiteDb.svg?label=LiteDB)](https://www.nuget.org/packages/Shiny.DocumentDb.LiteDb/)
[![NuGet](https://img.shields.io/nuget/v/Shiny.DocumentDb.CosmosDb.svg?label=CosmosDB)](https://www.nuget.org/packages/Shiny.DocumentDb.CosmosDb/)
[![NuGet](https://img.shields.io/nuget/v/Shiny.DocumentDb.MongoDb.svg?label=MongoDB)](https://www.nuget.org/packages/Shiny.DocumentDb.MongoDb/)
[![NuGet](https://img.shields.io/nuget/v/Shiny.DocumentDb.AzureTable.svg?label=AzureTable)](https://www.nuget.org/packages/Shiny.DocumentDb.AzureTable/)
[![NuGet](https://img.shields.io/nuget/v/Shiny.DocumentDb.DynamoDb.svg?label=DynamoDB)](https://www.nuget.org/packages/Shiny.DocumentDb.DynamoDb/)
[![NuGet](https://img.shields.io/nuget/v/Shiny.DocumentDb.DocumentDb.svg?label=DocumentDB)](https://www.nuget.org/packages/Shiny.DocumentDb.DocumentDb/)
[![NuGet](https://img.shields.io/nuget/v/Shiny.DocumentDb.Redis.svg?label=Redis)](https://www.nuget.org/packages/Shiny.DocumentDb.Redis/)
[![NuGet](https://img.shields.io/nuget/v/Shiny.DocumentDb.RavenDb.svg?label=RavenDB)](https://www.nuget.org/packages/Shiny.DocumentDb.RavenDb/)
[![NuGet](https://img.shields.io/nuget/v/Shiny.DocumentDb.Firestore.svg?label=Firestore)](https://www.nuget.org/packages/Shiny.DocumentDb.Firestore/)
[![NuGet](https://img.shields.io/nuget/v/Shiny.DocumentDb.DuckDb.svg?label=DuckDB)](https://www.nuget.org/packages/Shiny.DocumentDb.DuckDb/)
[![NuGet](https://img.shields.io/nuget/v/Shiny.DocumentDb.IndexedDb.svg?label=IndexedDB)](https://www.nuget.org/packages/Shiny.DocumentDb.IndexedDb/)
[![NuGet](https://img.shields.io/nuget/v/Shiny.DocumentDb.Extensions.AI.svg?label=AI+Extensions)](https://www.nuget.org/packages/Shiny.DocumentDb.Extensions.AI/)
[![NuGet](https://img.shields.io/nuget/v/Shiny.DocumentDb.Orleans.svg?label=Orleans)](https://www.nuget.org/packages/Shiny.DocumentDb.Orleans/)

A lightweight, multi-provider document store for .NET that turns relational databases into a schema-free JSON document database with LINQ querying, spatial/geo queries, and full AOT/trimming support. Supports **SQLite**, **SQLCipher** (encrypted SQLite), **LiteDB**, **CosmosDB**, **MongoDB**, **Amazon DocumentDB** (MongoDB-compatible), **Redis** (Redis Stack), **RavenDB**, **Google Firestore**, **Azure Table Storage** (and Cosmos DB Table API), **Amazon DynamoDB**, **DuckDB**, **IndexedDB** (Blazor WASM), **MySQL**, **MariaDB**, **SQL Server**, **PostgreSQL**, **CockroachDB**, and **Oracle**.

**[Documentation](https://shinylib.net/sqlite-docdb)**

## Features

- **Zero schema, zero migrations** — store entire object graphs (nested objects, child collections) as JSON documents. No `CREATE TABLE`, no `ALTER TABLE`, no JOINs.
- **Multiple database providers** — use SQLite for mobile/embedded, LiteDB for file-based NoSQL, CosmosDB or MongoDB for cloud/NoSQL workloads, Azure Table Storage or Amazon DynamoDB for serverless key-partitioned cloud stores, DuckDB for analytical workloads, IndexedDB for Blazor WebAssembly, or MySQL, MariaDB, SQL Server, PostgreSQL, CockroachDB, Oracle for server workloads. Same API, same LINQ expressions, different backend. **MariaDB** (extends the MySQL provider) and **CockroachDB** (extends the PostgreSQL provider) are wire-compatible variants that inherit their parent's feature set — MariaDB uses the portable spatial tier, drops full-text proximity, and doesn't support array-unnest queries (`Any`/`All` over a collection, collection aggregates, array `GroupBy` — MariaDB has no `JSON_TABLE`, so these throw a clear error); CockroachDB keeps native spatial, full-text, and (pgvector-compatible) vector search, and scopes away change-feed/bulk-copy/soundex. Both are wired into the Aspire integration. **Azure Table and DynamoDB** are schema-free key-partitioned NoSQL stores (`AddAzureTableDocumentStore(...)` / `AddDynamoDbDocumentStore(...)`) that map the library's `(typeName, id)` identity onto the store's native partition/sort keys. Rich LINQ queries evaluate client-side after a single-partition scan; promote hot query paths with `MapIndexedProperty<T>(x => x.Status)` and predicates over that property (plus the string `Query`/`Count`/`ToQueryString` overloads — OData `$filter` on Table, PartiQL on DynamoDB) push down server-side. Optimistic concurrency uses the native ETag / conditional-write; both support in-process change observation (`IObservableDocumentStore`) and DynamoDB adds a native **DynamoDB Streams** change feed (`IChangeFeedDocumentStore`). Int/Long Id **auto-generation is unsupported** (use Guid/string, or assign the Id) — no spatial/vector/full-text/temporal on those two. **Four more document-native providers** ship alongside: **Redis** (`AddRedisDocumentStore(...)`, requires Redis Stack — RedisJSON + RediSearch) with server-side full-text/vector(KNN)/geo, `MapIndexedProperty` push-down to `FT.SEARCH`, a keyspace-notification change feed, and the only NoSQL backend where **Int/Long Id auto-gen works** (atomic `INCR`); **RavenDB** (`AddRavenDbDocumentStore(...)`) storing an opaque System.Text.Json envelope and evaluating LINQ client-side over immediately-consistent id-prefix streams (RQL `ToQueryString`); **Google Firestore** (`AddFirestoreDocumentStore(...)`) with native-map storage (single-field push-down + full-scan fallback for missing composite indexes), native cursor pagination, transaction-guarded CAS, and a real per-query change feed via snapshot listeners; and **Amazon DocumentDB** (`AddDocumentDbDocumentStore(...)`), a thin MongoDB-provider subclass with TLS + `retryWrites=false` defaults that throws for the features DocumentDB lacks (`$text` full-text, Atlas `$vectorSearch`).
- **Fluent query builder** — `store.Query<User>().Where(u => u.Age > 30).OrderBy(u => u.Name).Paginate(0, 20).ToList()` with full LINQ expression support for nested properties, `Any()`, `Count()`, string methods, null checks, and captured variables.
- **Inspect the generated query** — `store.Query<User>().Where(u => u.Age > 30).ToQueryString()` returns the SQL (or MongoDB BSON) the query *would* run, with its parameter values, **without executing it** — for debugging and logging. Relational providers and Cosmos return SQL + parameters; MongoDB returns the rendered BSON filter; LiteDB/IndexedDB throw `NotSupportedException`.
- **Scalar function translation (all providers)** — `Where` predicates translate a library of functions to each backend's native form: string functions (`ToLower`/`ToUpper`, `Length`, `Trim`, `Substring`, `Replace`, `IndexOf`, `string.IsNullOrEmpty`), `Math.*` (`Abs`, `Round`, `Ceiling`, `Floor`, `Sqrt`, `Pow`, `Sign`), date-part access, **flag-enum tests** (`x.Permissions.HasFlag(Permissions.Write)` and `(x & flag) == flag`), and phonetic search via `DocumentFunctions.Soundex(...)`. Relational providers emit native SQL (`BITAND` on Oracle); MongoDB uses `$expr` (+ `$bitsAllSet`); CosmosDB uses native NoSQL functions; LiteDB/IndexedDB run in-memory. Register custom translations with `MapFunctionTranslation(...)`. Built on a shared, AOT-safe query IR (the whole query surface is free of `Expression.Compile()`).
- **`IAsyncEnumerable<T>` streaming** — yield results one-at-a-time with `.ToAsyncEnumerable()` instead of buffering into a list. Eliminates Gen1 GC pressure at scale with comparable throughput.
- **Expression-based JSON indexes** — `store.CreateIndexAsync<User>(u => u.Name, ctx.User)` creates a partial JSON index on the property. Up to **30x faster** queries on indexed properties. (SQLite uses `json_extract`; other providers use native JSON indexing.)
- **SQL-level projections** — project into DTOs with `json_object` at the database level via `.Select()`. No full document deserialization needed.
- **Grouped aggregation** — `store.Query<Order>().GroupBy(o => o.Status).Having(g => g.Sum(o => o.Total) > 10_000).Select(g => new StatusRollup { Status = g.Key, Count = g.Count(), Revenue = g.Sum(o => o.Total) })` rolls up one row per key with `g.Key` and the `Sql` group aggregates (`g.Count/Sum/Avg/Min/Max`). Keys can be nested, **derived** (`o => o.CreatedAt.Month`), or a multi-column anonymous type; `Having` filters groups; grouped `OrderBy`/`Paginate`/`Count`/`ToQueryString` all flow through, and the string grammar (`GroupBy("status").Project("status, count() as n, sum(total) as revenue")`) lowers to the same SQL. Push-down on the relational providers; MongoDB/Cosmos/LiteDB/IndexedDB group client-side; Azure Table/DynamoDB throw. The whole-set `Count`/`Sum`/`Average` terminals remain for ungrouped totals.
- **Full AOT/trimming support** — every API has an optional `JsonTypeInfo<T>` parameter for source-generated JSON serialization. No reflection required. Configure a `JsonSerializerContext` once and all methods auto-resolve type info — no per-call `JsonTypeInfo<T>` needed. Set `UseReflectionFallback = false` to catch missing type registrations with clear exceptions instead of opaque AOT failures.
- **Strongly-typed `DocumentContext`** — an optional EF-Core-style typed front-end (the source generator is bundled in the core package — no extra package to install). Declare aggregates once on a `partial` context (`[Document(typeof(User), JsonContext = typeof(AppJsonContext))]`) and the source generator emits a `DocumentSet<T>` per type, a `ConfigureModel` lowering, and two DI extensions — `services.AddAppContext(...)` (scoped, for ASP.NET Core) and `services.AddAppContextFactory(...)` (a singleton `IDocumentContextFactory<AppContext>` for MAUI/Blazor/desktop, mirroring EF Core's `IDbContextFactory<T>`). Work model-first — `await db.Users.Where(u => u.Age >= 18).ToList()`, `await db.Users.Insert(user)` — with `JsonTypeInfo<T>` threaded automatically, so you never re-type `<T>` or pass type metadata. Ergonomics + discoverability only (no change tracking / navigation / `Include`); immediate writes; transactions via `context.OpenSession`-style grouping (the context is itself a unit of work). Per-type serialization knob: `JsonContext` (point at your `JsonSerializerContext`) or `Generated` (the generator emits the metadata-mode `JsonTypeInfo` itself) for AOT, `Reflection` to opt out. Works over any provider — `DocumentContext` only needs an `IDocumentStore`.
- **Up to 60x faster nested inserts** vs sqlite-net (and ~18x vs EF Core) — one write per document vs multiple table inserts with foreign keys. 2-9x faster reads on nested data, beating even hand-written Dapper SQL.
- **Mandatory typed Id property** — every document type must have a `public {Guid|int|long|string} Id { get; set; }` property (or a custom Id type registered via `MapIdType`). Ids are auto-generated when default (Guid.Empty, 0, null/empty string) and written back to the object. The Id lives in both the SQLite column and the JSON blob, so query results always include it.
- **JSON Merge Patch (Upsert)** — `store.Upsert(patch)` deep-merges a partial object into an existing document using SQLite's `json_patch()` (RFC 7396). The Id comes from the object. Only patched fields are overwritten; unset nullable fields are preserved.
- **Merge-vs-replace flags** — `Update(doc, patch: true)` deep-merges instead of replacing (update-only), and `Upsert(doc, patchIfUpdate: false)` replaces wholesale instead of merging (insert-or-replace) — the same flags on the JSON lane (`Update(type, jsonObject, patch: true)`) give precise partial updates. The two defaults (`Update` replace, `Upsert` merge) work everywhere; the non-default modes are relational-provider (+ JSON lane) only.
- **Surgical field updates** — `store.SetProperty<User>("id", u => u.Age, 31)` updates a single JSON field via `json_set()` without deserializing the document. `store.RemoveProperty<User>("id", u => u.Email)` strips a field via `json_remove()`. Both support nested paths like `o => o.ShippingAddress.City`.
- **Document diff (JsonPatchDocument)** — `store.GetDiff("id", modified)` compares an object against the stored document and returns an RFC 6902 `JsonPatchDocument<T>` with deep nested-object diffing. Powered by [SystemTextJsonPatch](https://www.nuget.org/packages/SystemTextJsonPatch).
- **Typed Id lookups** — `Get`, `Remove`, `SetProperty`, and `RemoveProperty` accept the Id as `object` so you can pass a `Guid`, `int`, `long`, or `string` directly. Unsupported types throw `ArgumentException`.
- **Late-bound JSON lane (`Type` + `JsonNode`)** — for dynamic ingestion where you hold a registered document `Type` but not a CLR `T` (generic HTTP intake, message-bus payloads, ETL, gateways). `store.Insert(typeof(Order), node)` / `Update` / `Upsert` take a `JsonObject` (one doc) or `JsonArray` (many, atomic — one transaction) and store the body **as-is**; `store.Get(typeof(Order), id)` / `Query(type, whereClause, parameters)` / `QueryStream(...)` return raw `JsonNode`s with no deserialize to `T` (cheaper, AOT-clean). Unlike `IDocumentBackup`/`BulkImport`, this lane **rides the full write pipeline** — tenancy, temporal, version/CAS, spatial + vector sidecars, interceptors, and change feed all apply, and the generated Id/version is injected back onto your node like the typed `Insert<T>`. Because the body is verbatim, a registered spatial/vector mapping whose JSON path is absent throws (an explicit JSON `null` is honored as "no value"); object-mutating interceptors are a no-op (JSON-shaped ones via `ctx.GetJsonDocument()` still fire) and vector auto-embedding doesn't run. Filtering reuses the string WHERE/OData surface — no "filter by JsonNode". Relational providers only (SQLite, SQLCipher, MySQL, SQL Server, PostgreSQL, Oracle, DuckDB); document-native and key-partitioned providers throw `NotSupportedException`, and the lane is unavailable inside a session transaction.
- **Pagination** — `store.Query<User>().OrderBy(u => u.Name).Paginate(0, 20).ToList()` translates to SQL `LIMIT`/`OFFSET`. For UI/REST responses use `.PageResult(page, pageSize)` to get back a `PagedResults<T> { Records, TotalCount, Page, PageSize }` in one call — the total reflects the current `Where` filters, not just the returned slice.
- **Cursor / keyset pagination** — for infinite scroll, deep paging, or large exports, `store.Query<Order>().OrderByDescending(o => o.CreatedAt).ToCursorPage(cursor, take: 50)` returns a `CursorPage<T> { Items, NextCursor, HasMore }`: a forward-only seek that stays O(log n) per page (with an index on the sort key) and is stable under concurrent writes — no ever-growing `OFFSET`, no total-count round-trip. The keyset is derived from the query's own `OrderBy` (an `Id` tiebreaker is appended automatically); `ToCursorStream(pageSize)` walks every page as an `IAsyncEnumerable<T>`. Keyset seeks server-side on the relational providers; LiteDB/IndexedDB/MongoDB page it client-side; Cosmos/DynamoDB/Azure Table are not yet supported (throw).
- **Dynamic sort columns (AOT-safe)** — `store.Query<User>().OrderBy("Name", ctx.User)` resolves the property through `JsonTypeInfo<T>` (source-generated), so the sort column can be supplied at runtime — from a query string, a column-header click, etc. Matches CLR or JSON name (case-insensitive), supports dotted paths like `"ShippingAddress.City"`, and never uses reflection on `T`.
- **Optimistic concurrency** — `MapVersionProperty<T>(x => x.RowVersion)` enables automatic version checking on update/upsert. Version is set to 1 on insert, checked and incremented on update. Throws `ConcurrencyException` on conflict. Works across all providers — stored in the JSON blob with zero schema changes.
- **Change observation (`IObservableDocumentStore`)** — consume an `IAsyncEnumerable<DocumentChange<T>>` of insert/update/remove/clear notifications with `await foreach (var c in store.NotifyOnChange<User>(ct)) { ... }` to drive reactive UI from your own writes. Notifications are in-process (changes made through this store instance), buffered and emitted only on commit. Supported on SQLite, SQLCipher, MySQL, SQL Server, PostgreSQL, Oracle (the relational `DocumentStore`) and LiteDB. Use `WhenDocumentChanged<T>(id)` to watch a single document.
- **Per-query change monitoring** — call `.NotifyOnChange()` on any query to receive only the changes whose document matches the query's `Where` predicates: `await foreach (var c in store.Query<Order>().Where(o => o.Status == "Pending").NotifyOnChange(ct)) { ... }`. Property-level / removal / clear events that don't carry the document body are passed through so the consumer can re-check membership.
- **Global query filters** — `options.AddQueryFilter<User>(u => !u.IsDeleted)` registers a predicate that's automatically AND-applied to every query of `User` — including `Query<T>()`, single-doc paths (`Get`/`Update`/`Remove`/`SetProperty`/`RemoveProperty`/`Clear`), bulk operations (`ExecuteUpdate`/`ExecuteDelete`), and per-query change monitoring. Named filters can be disabled individually via `query.IgnoreQueryFilters("name")`; all filters can be disabled with `query.IgnoreQueryFilters()`. Use for soft-delete, row-level security, or "active only" scopes. Insert is intentionally unfiltered (matches Entity Framework Core).
- **Temporal history (system-time versioning)** — `options.MapTemporal<Order>(o => { o.Retention = TimeSpan.FromDays(90); o.MaxVersions = 50; o.CaptureActor = () => userId; })` opts a type into append-only versioning. **Scope-aware actor:** `o.ResolveActor = sp => sp.GetService<ICurrentUser>()?.Id` captures "who" from the write's request-scoped DI (per unit of work), taking precedence over `CaptureActor`. Every Insert/Update/Upsert/Remove/SetProperty/RemoveProperty/BatchInsert (including inside a session) records a snapshot to a per-type history sidecar. Read it back with `History<T>(id)`, `AsOf<T>(id, when)`, `Restore<T>(id, version)`, `GetDiffBetween<T>(id, from, to)`, plus fleet-wide `AsOfAll<T>(when)`, `ChangesByActor<T>(actor)`, and `ChangesBetween<T>(from, to)`. Opt-in per type (non-temporal types pay nothing); supported on **every** provider — all relational (SQLite, SQLCipher, PostgreSQL, SQL Server, MySQL, Oracle, DuckDB) and the document stores (LiteDB, MongoDB, CosmosDB, IndexedDB). The history methods live on the `ITemporalDocumentStore` capability interface (not `IDocumentStore`) — resolve or cast to it. Retention pruned on every write.
- **Blobs (binary payloads in a sidecar)** — attach binary data (PDFs, images, signatures) to a document without bloating it. Map a `DocumentBlob` (single) or `DocumentBlobCollection` (many) with `MapBlob<T>` / `MapBlobCollection<T>`, and the bytes go to a `{table}_blobs` sidecar table while only the metadata (length, content type, file name) rides along in the document JSON — so ordinary queries never drag the payload along, and you can `Where`/`OrderBy` on that metadata like any other property (`Where(x => x.Pdf.Length > 1_000_000)`). Payloads self-load: `await doc.Pdf.LoadAsync()` for one blob, `await doc.Attachments.LoadAllAsync()` for a whole collection in one round trip (or `store.BatchLoadBlobs(page)` across a page, `store.GetBlob<T>(id, key)` with no document in hand); `Bytes` throws until loaded, so there's no hidden I/O behind a property getter. Writes go through the document (assign the member and save — no `SetBlob`), so the inline metadata can never disagree with the stored bytes; deleting a document cascades to its blob rows. Opt-in SHA-256 hashing and a per-mapping size ceiling; `store.MaxBlobSize` reports the provider limit. Use it instead of a raw `byte[]` property (which is base64'd into the document body and read on every query). **Relational providers only** (SQLite, PostgreSQL/CockroachDB, SQL Server, MySQL/MariaDB, Oracle, DuckDB); NoSQL providers report `MaxBlobSize == 0` and throw.
- **Native change feeds (`IChangeFeedDocumentStore`)** — observe changes from *any* writer (other processes/connections), not just this instance: `await using var sub = await store.SubscribeChanges<User>(async (change, ct) => { ... });`. Backed by each database's own mechanism — **PostgreSQL** `LISTEN`/`NOTIFY` (row-level triggers, true push), **SQL Server** Change Tracking with optional `SqlDependency` query-notification wake-ups (configurable via `SqlServerChangeFeedOptions`), and **CosmosDB** Change Feed. Provisioning (triggers / enabling change tracking) is automatic and idempotent. Dispose the returned handle to stop. (SQLite, LiteDB, IndexedDB, MySQL and Oracle have no proper external-change mechanism and throw `NotSupportedException`.)
- **Concurrent operations on server SQL** — a single `DocumentStore` instance backed by PostgreSQL, MySQL, SQL Server, or Oracle opens a fresh connection per operation and lets the ADO.NET driver pool multiplex callers. No per-store semaphore. SQLite and DuckDB (embedded engines that lock the whole DB on writes) keep the long-lived shared connection + serialization model. Providers opt in to shared mode via `IDatabaseProvider.RequiresSingleConnection`. Table init is exactly-once per table across concurrent first-touch callers (`ConcurrentDictionary<string, Lazy<Task>>`).
- **Unit of work (`IDocumentSession`)** — the document store is a connection: open a short-lived session and group writes into one transaction: `await using var session = store.OpenSession(); session.Add(a).Update(b).Remove<C>(id); await session.SaveChanges();` with automatic commit/rollback. Contiguous same-type runs of inserts, upserts, updates, and removes are each coalesced into the matching batch method. Inject `IDocumentSession` (scoped) in ASP.NET via `AddScopedDocumentSession()`, or open one from the singleton `IDocumentSessionFactory` (MAUI/desktop/background — mirrors EF's `IDbContextFactory`). For finer control the session exposes an explicit transaction — `await using var tx = await session.BeginTransaction();` — for locking reads (`session.Get(id, LockMode.Update)`) and grouping `ExecuteUpdate`/`ExecuteDelete`; `SaveChanges` joins it. Pass an `IsolationLevel` (`BeginTransaction(IsolationLevel.Snapshot)`) for a **consistent-read session**. (Replaces the removed `CreateUnitOfWork()`/`UnitOfWork`.)
- **Side-effect-free writes (`SaveChanges(suppressInterceptors: true)`)** — commit a session with **no** interceptor (per-document or bulk) firing for that transaction. Bounded by the commit (writes outside the unit still fire interceptors), so it's the right tool for mirrored / authoritative writes that should carry no side effects — bulk import, seeding, migration, and the inbound apply path of `Shiny.DocumentDb.AppDataSync`. When suppressed the multi-row batch fast path is re-enabled (it's only disabled to guarantee per-doc interceptors fire — moot when none will).
- **The serialized write JSON on the interceptor context** — inside `IDocumentInterceptor.BeforeWrite` read `ctx.GetJson()` / `ctx.GetJsonDocument()` to see the exact JSON about to be persisted (the store's own options/`JsonTypeInfo`, cached, invalidated if an earlier interceptor replaces the document). Generally useful for auditing, redaction checks, and outbound capture; it's the primitive the `Shiny.DocumentDb.JsonSchema` validation package builds on.
- **Batch insert** — `store.BatchInsert(items)` inserts a collection in a single transaction with prepared command reuse. Auto-generates IDs and rolls back atomically on failure.
- **Batch upsert / update / remove** — `store.BatchUpsert(items)`, `store.BatchUpdate(items)`, and `store.BatchRemove<T>(ids)` apply many writes as one set operation — a single multi-row `INSERT … ON CONFLICT` deep-merge on SQLite/DuckDB, one `BulkWrite`/`DeleteMany` on MongoDB, parallel request waves on Cosmos, and a single `DELETE … IN (…)` for `BatchRemove` on every relational provider. All-or-nothing: the first version conflict rolls the whole batch back.
- **Spatial / geo queries** — point queries (`WithinRadius`, `WithinBoundingBox`, `NearestNeighbors`) **and full OGC geometry**. Map a `GeoPoint?` or a `Geometry?` property (`GeoLineString`, `GeoPolygon` with holes, `GeoMultiPoint`/`GeoMultiLineString`/`GeoMultiPolygon`, `GeoGeometryCollection`; GeoJSON-serialized) with `MapSpatialProperty<T>(x => x.Area)`, then query with the `Geo`-prefixed topological predicate family — `GeoIntersects`, `GeoContainedBy`, `GeoContains`, `GeoDisjoint`, `GeoTouches`, `GeoCrosses`, `GeoOverlaps`, `GeoEquals`, `GeoCovers`, `GeoCoveredBy` — plus `GeoWithinDistance(geometry, meters)`. Every predicate takes an optional `orderByDistanceFrom` (point or geometry) and returns `SpatialResult<T>` with `DistanceMeters`; `NearestNeighbors` works over geometry too. The `Geometry` model also exposes in-memory `Area`/`Length`/`Centroid`/`IsValid`/`MakeValid` (C# accessors — to filter by a measurement server-side, compute the scalar in your app and store it as a normal indexed field). **SQLite** (R\*Tree) and **PostgreSQL / MySQL / SQL Server / Oracle / DuckDB** (dependency-free envelope-sidecar — no PostGIS/`geography`/`SDO`/`spatial` extension) run the full family via a bbox prune + in-process relate; **CosmosDB** pushes `ST_INTERSECTS`/`ST_WITHIN`/`ST_DISTANCE` down; **MongoDB** uses a `2dsphere` index (`$geoIntersects`/`$geoWithin`/`$near`). The mapped property can be nullable — documents with a `null` location are left out of the spatial index (no exception on write, and clearing a location on update purges its stale entry).
- **Reference geo data** — the `Shiny.DocumentDb.Geo` package ships an embedded, provider-agnostic dataset of **US states, Canadian provinces, and US & Canadian cities** as `GeoRegion`/`GeoCity` documents. Register `AddGeoReferenceSeeder()` (idempotent) with `opts.MapGeoReferenceData()` to seed it into any store and run point-in-region / nearest-city spatial queries, or read `GeoDataSets.Regions`/`GeoDataSets.Cities` in memory. The city lists are regenerated from US Census / Statistics Canada by a dev-only tool.
- **Vector / ANN search** — `MapVectorProperty<T>(x => x.Embedding, dimensions: 1536)` + `store.Query<T>().NearestVectors(queryEmbedding, k: 10)` for cross-provider ANN over `ReadOnlyMemory<float>` embeddings. `NearestVectors<T>` is also on `IDocumentSession` (consistent-snapshot search inside a transaction). Provider-native indexes: pgvector (PostgreSQL), `VECTOR_DISTANCE` (SQL Server 2025 and Oracle 23ai), DiskANN (CosmosDB), `$vectorSearch` (MongoDB Atlas), `vss` (DuckDB), `sqlite-vec` (SQLite). Cosine / Euclidean / DotProduct everywhere; Hamming on pgvector. Pre-filter via `Where(...)` where the engine supports it. Auto-embed text properties on insert/upsert via `Shiny.DocumentDb.Extensions.AI`'s `AutoEmbedOnInsert<T>` — now a write **interceptor**, so it runs on **every** provider, inside the write transaction, and resolves `Microsoft.Extensions.AI.IEmbeddingGenerator` per-write from the DI scope (a fixed-instance overload covers the container-free `new DocumentStore(options)` path).
- **Full-text search (all providers)** — `MapFullTextProperty<T>(a => a.Body)` (or `[a => a.Title, a => a.Body]`) + `store.FullTextSearch<T>("orleans persistence")` for relevance-ranked text search, returning `FullTextResult<T>` (`Document` + normalized `Score`, higher = better) ordered by relevance, with an optional pre-filter predicate and a fluent `store.Query<T>().Where(...).FullTextMatch("...")` form. The native index is created for you and engine-maintained: FTS5 (SQLite), `tsvector`+GIN (PostgreSQL), `FULLTEXT` (MySQL), Oracle Text (Oracle), Full-Text Index (SQL Server), the `fts` extension (DuckDB), full-text policy (CosmosDB), `$text` (MongoDB), and an in-memory TF-IDF fallback on LiteDB / IndexedDB. Declarative: a type must be mapped before it can be searched. Oracle Text and SQL Server Full-Text Search are optional server components; CosmosDB full-text needs `Microsoft.Azure.Cosmos` 3.61.0+. For **composable** full-text, `DocumentFunctions.LuceneMatch(a.Body, "orleans AND grain NOT deprecated")` works inside a `Where` (and `DocumentFunctions.LuceneScore(...)` inside an `OrderBy`), translating a Lucene query — terms, phrases, `AND`/`OR`/`NOT`, grouping, `foo*`, `foo~`, `"a b"~5`, `foo^2` — to the provider's native engine over the same index (SQLite, PostgreSQL, MySQL, SQL Server, Oracle match-only, and full-grammar in-memory on LiteDB/IndexedDB; unsupported operators throw). Also available in the string grammar as `lucenematch(...)` / `lucenescore(...)`.
- **Computed properties** — `MapComputedProperty<Order, decimal>(o => o.Total, o => o.Quantity * o.UnitPrice)` maps a value *derived* from other fields that you filter, sort, and project by exactly like a stored property — though it's never written into the document JSON. Expose it as a `[JsonIgnore]` property; reference it by name in typed LINQ (`Where(o => o.Total > 100)`), the string API (`Where("total > 100").OrderBy("fullName")`), `Project("fullName as name, total")`, and OData. Default **alias mode** inlines the definition into each query (zero schema change, every relational provider); `indexed: true` **materializes** a native generated/computed column + index on the relational providers (`VIRTUAL` on SQLite/MySQL, `STORED` on PostgreSQL, `PERSISTED` on SQL Server, virtual on Oracle; DuckDB uses alias mode) so filters/sorts are index-served. The value is recomputed and written back onto the object on read. LiteDB/IndexedDB evaluate it in memory (full surface); MongoDB/Cosmos support read-back + projection (not server-side filter/sort by a computed property). Definitions cover JSON field access, string concat, the scalar functions, and **numeric arithmetic** (`+ - * /`, also now usable in ordinary `Where` clauses). Fully AOT/trim-safe — tree-walked to SQL, interpreted for read-back, never compiled.
- **Telemetry & observability (embedded, always-on)** — every store emits OpenTelemetry-native metrics (`db.client.operation.duration` + an operations counter + a returned-rows histogram, tagged per the OTel DB semantic conventions) and an `ActivitySource` client span per operation directly — no decorator, no opt-in. An `IDocumentSession` adds a `<system>.unit_of_work` **parent span** (tagged `db.session.id`) so its operations nest into one correlated trace, plus a `db.client.unit_of_work.operations` histogram (buffered writes flushed per `SaveChanges`). Covers CRUD, the fluent-query terminals, and the temporal `ITemporalDocumentStore` ops. Built on `System.Diagnostics`; subscribe with `.AddSource("Shiny.DocumentDb")` / `.AddMeter("Shiny.DocumentDb")`. Zero-cost when nobody is listening; never records document bodies or ids.
- **Hot backup** — `store.Backup("/path/to/backup.db")` copies the database to a file. Available on `SqliteDocumentStore`, `SqlCipherDocumentStore`, and `LiteDbDocumentStore` (not on the `IDocumentStore` interface).
- **Streaming bulk export / import / restore (`IDocumentBackup`)** — a separate store capability (probe with `store is IDocumentBackup`, like `IDocumentMaintenance`) for moving a whole store in and out. `ExportAsync(stream)` writes a backup document (a JSON array of `{ id, docType, data, createdAt, updatedAt }` records, body emitted as-is); `RestoreAsync(stream)` streams it back in with a forward-only reader (never fully buffered) and **preserves the original `CreatedAt`/`UpdatedAt`** across every provider (backward-compatible with older v1 backups that carry no timestamps — those re-stamp now); `BulkImportAsync(IAsyncEnumerable<RawDocument>)` is the lower-level primitive over raw UTF-8 JSON rows that `RestoreAsync` adapts. Bodies are bound **verbatim** — no `<T>`, no `JsonTypeInfo`, no reflection over the documents (AOT-friendly). `BulkWriteMode` chooses collision handling — `Insert` (fail on duplicate; fastest), `Replace` (overwrite wholesale), `Merge` (RFC 7396 deep-merge), `SkipExisting` — with `ChunkSize`, per-chunk-vs-single transaction, `ClearExistingFirst`, and `IProgress<BulkProgress>`. It is a raw restore lane: the import path skips versioning/CAS, temporal history, interceptors, tenant scoping, and global query filters (not a replacement for `BatchUpsert`). Implemented on the relational `DocumentStore` (all SQL providers), MongoDB, and Cosmos DB. Insert works everywhere; Replace/SkipExisting on all relational providers + Mongo/Cosmos; Merge only on SQLite/DuckDB/Mongo/Cosmos (throws elsewhere — use Replace). Native bulk-copy fast path (10-100×) for Insert on PostgreSQL (`COPY`), SQL Server (`SqlBulkCopy`), and DuckDB (appender). Mongo/Cosmos imports are best-effort (not atomic); Cosmos export covers the whole database.
- **Clear the whole store (`IDocumentMaintenance.ClearAll`)** — `((IDocumentMaintenance)store).ClearAll()` wipes every document type (plus temporal-history, spatial, and vector sidecars) for test/dev resets. A whole-store wipe — not type- or tenant-scoped (use `Clear<T>()` for one type) — that targets only user tables in the current database and never the system catalogs. Implemented on the relational `DocumentStore` (SQLite, SQL Server, PostgreSQL, MySQL, DuckDB, Oracle), MongoDB, and CosmosDB; `SqliteDocumentStore.ClearAllAsync()` still works and delegates to it.
- **Database seeding** — register `IDocumentSeeder`s to populate initial data once at startup. Because the store is schema-free, seeding is just idempotent writes, so seeders are provider-agnostic and work against every backend. Run-once is versioned via a `DocumentSeedMarker` (bump `Version` to re-run). Wire with `AddDocumentSeeder<T>()` / `AddDocumentSeeder(name, version, delegate)` (runs at host startup via a hosted service) or call `DocumentSeedRunner.RunAsync(store, seeders)` directly (e.g. on MAUI).
- **JSON Schema validation (`Shiny.DocumentDb.JsonSchema`)** — attach a JSON Schema (draft 2020-12, via [JsonSchema.Net](https://www.nuget.org/packages/JsonSchema.Net)) to a document type and the store validates the exact JSON about to be persisted just before the write. **No DI required** — `options.MapJsonSchema<Customer>(schemaJson)` is an extension on `DocumentStoreOptions` (repeated calls accumulate into one interceptor), so it works with a hand-built `new DocumentStore(options)`; the DI form is `services.AddDocumentJsonSchema(o => o.MapJsonSchema<Customer>(schemaJson))`. A failure throws `DocumentSchemaValidationException` with field-level errors and rolls the write back. Map schemas by `JsonSchema` object, JSON text, `Stream`, or `MapJsonSchemaFromFile<T>(path)`. Enforces what the C# type can't — `maxLength`, ranges, `pattern`, `enum`, `additionalProperties:false`, reference-type required-ness — and asserts `format` (email/uuid/date-time) by default. Schema names match the **serialized** (camelCase) JSON names. Runs on `BeforeWrite`, so it composes with `Shiny.DocumentDb.AppDataSync` for free (invalid documents never reach the outbox); suppressed writes (inbound sync, bulk import) skip validation by design.
- **Offline-first sync (`Shiny.DocumentDb.AppDataSync`)** — make the store the local cache of an offline-first app that bidirectionally syncs to an HTTP backend via [`Shiny.Data.Sync`](https://shinylib.net/client/datasync/). Register `AddDocumentStore(...)` + `AddDataSync<TDelegate>(...)` + `SyncDocumentStore(sync => sync.Sync<TodoItem>())` and an ordinary document type becomes two-way synced with no manual `Queue`/delegate plumbing: every local `Insert`/`Update`/`Upsert`/`Remove` is auto-enqueued to the sync outbox, and every pulled server change is auto-applied back into the store (Create/Update → `Upsert`, Delete → `Remove`). Inbound applies run through `SaveChanges(suppressInterceptors: true)` so they never echo back to the server (loop guard) and fire no other interceptor. Set-based writes (`ExecuteUpdate`/`ExecuteDelete`/`Clear<T>`) throw `SyncBulkWriteNotSupportedException` on synced types (use `ClearAll` for a whole-store reset); batch writes enqueue each item. Synced types implement `Shiny.Data.Sync.ISyncEntity`; the store and sync serializers are validated to share one JSON contract at startup. Client-tier providers (SQLite, LiteDB, IndexedDB).
- **OData query endpoints (`Shiny.DocumentDb.OData` + `Shiny.DocumentDb.AspNetCore.OData`)** — expose a document type as an OData v4 entity set: `$filter`/`$orderby`/`$top`/`$skip`/`$count`/`$select` are translated onto the fluent `IDocumentQuery<T>` and run against any provider. The translator engine is dependency-free and AOT-clean; the ASP.NET Core host adds EDM + endpoint wiring (JIT-only). Global query filters always apply underneath, so a client can't `$filter` its way past them. Per-entity-set governance (`ODataQueryPolicy`) locks down a public endpoint: default/max page size, allowed system options, per-property filter/sort/select allowlists, and filter-complexity limits (a violation → `400`).
- **.NET Aspire integration (`Shiny.DocumentDb.Aspire.Hosting` / `.Client` / `.Orleans`)** — make the backend a deployment decision: `builder.AddPostgresDocumentStore("orders").WithSeeder(...)` in the AppHost picks the provider (Postgres/SQL Server/MySQL/SQLite) and gates seeding; the consuming service calls `builder.AddDocumentStore("orders")` to get the keyed store wired with health checks + OpenTelemetry. The client opens up to DI-aware setup — `configureServiceOptions: (sp, o) => …` configures options with the resolved `IServiceProvider`, and a `MultiTenant` settings flag registers a shared-table multi-tenant store from a registered `ITenantResolver` in one line. A source-generated typed `DocumentContext` can be backed by an Aspire resource too — `builder.Services.AddOrdersContext(builder.AddDocumentContextProvider("orders"))`. `silo.UseAspireDocumentDb("orders")` backs Orleans grain storage/reminders/clustering/directory with the same Aspire-provisioned store. Server-tier only.
- **AI tool integration** — `Shiny.DocumentDb.Extensions.AI` exposes `IDocumentStore` operations as `Microsoft.Extensions.AI` tool functions for LLM agents. Register document types with per-type capability flags (`ReadOnly`, `All`, or individual operations), structured filter expressions with boolean combinators, field visibility control (`AllowProperties`/`IgnoreProperties`), non-removable row-level scope filters (`Where(...)`) the LLM cannot see or bypass — enforced on every tool including get/insert/update/delete — and page size caps. No DI required — call `store.CreateAITools(b => b.AddType(...))` on a hand-built `IDocumentStore` to get a `DocumentStoreAITools` directly; the DI form (`services.AddDocumentStoreAITools(...)` → resolve `DocumentStoreAITools`) builds the same thing from the container. Pass `.Tools` to any `IChatClient`.
- **Orleans persistence stack (`Shiny.DocumentDb.Orleans`)** — a full Microsoft Orleans stack — **grain storage** (+ `PubSubStore`), **reminders** (`IReminderTable`), **cluster membership/clustering** (`IMembershipTable`), and **grain directory** (`IGrainDirectory`) — built entirely on `IDocumentStore`, so one set of implementations runs on every backend. `siloBuilder.AddDocumentDbGrainStorage(...)` / `.AddDocumentDbReminders(...)` / `.AddDocumentDbClustering(...)` / `.AddDocumentDbGrainDirectory("Default", ...)`. The Orleans ETag maps to a version-checked document (atomic CAS → `InconsistentStateException` on conflict), and grain state is stored as nested, **structured JSON** — so you can **query grain state directly without activating the grains** (reporting/dashboards/admin over the persisted read model, something Orleans' point-key storage contract can't do) and opt into `MapTemporal` for a free state-history audit trail. Companion packages `Shiny.DocumentDb.Orleans.MongoDb` / `Shiny.DocumentDb.Orleans.CosmosDb` wire grain storage for those backends in one call; a `StoreFactory` escape hatch covers the rest. (Membership needs multi-document transactions — relational or MongoDB replica set, not Cosmos.)

## Comparison with alternatives

| | Shiny.DocumentDb | Microsoft.Data.Sqlite (raw ADO.NET) | sqlite-net-pcl |
|---|---|---|---|
| **Schema management** | Zero — just store objects | You write every `CREATE TABLE`, `ALTER TABLE`, migration | Auto-creates flat tables from POCOs |
| **Database providers** | SQLite, LiteDB, CosmosDB, MongoDB, Azure Table, DynamoDB, DuckDB, IndexedDB, MySQL, SQL Server, PostgreSQL, Oracle | SQLite only | SQLite only |
| **Nested objects & child collections** | Stored and queried as a single JSON document | Must design normalized tables, write JOINs, manage foreign keys | No support — flat columns only, child collections require separate tables + manual joins |
| **LINQ queries on nested data** | `store.Query<Order>().Where(o => o.Lines.Any(l => l.Price > 10)).ToList()` | Hand-written `json_extract` SQL | Not possible on nested data |
| **AOT / trimming** | First-class optional `JsonTypeInfo<T>` on every API | Manual — you control all SQL | Relies on reflection; no AOT support |
| **Migrations** | Not needed — schema-free JSON | You own every migration | You own every migration |
| **Projections** | SQL-level `json_object` projections via `.Select()` | Manual SQL | Not available |
| **Unit of work** | `store.OpenSession()` (`IDocumentSession`) + `SaveChanges()` | Manual `BeginTransaction` + `Commit`/`Rollback` | `SaveChanges` (change tracker) |
| **JSON property indexes** | `store.CreateIndexAsync<User>(u => u.Name, ctx.User)` — LINQ expression indexes on `json_extract` | Manual `CREATE INDEX` on `json_extract` | Column indexes only |
| **Best fit** | Object graphs, nested data, rapid prototyping, settings stores, caches | Full SQL control, complex reporting queries, performance-critical bulk ops | Simple flat-table CRUD |

**In short:** If your data has nested objects or child collections (orders with line items, users with addresses, configs with nested sections), this library lets you store and query the entire object graph with a single call — no table design, no JOINs, no migrations. For flat, single-table CRUD on simple POCOs, sqlite-net-pcl or raw ADO.NET may be simpler.

## Replacing EF Core on .NET MAUI

Entity Framework Core is a natural choice for server-side .NET, but it becomes a liability on .NET MAUI platforms (iOS, Android, Mac Catalyst). This library is purpose-built for the constraints mobile and desktop apps actually face.

### Why EF Core is a poor fit for MAUI

- **No AOT support.** EF Core relies heavily on runtime reflection and dynamic code generation for change tracking, query compilation, and model building. It carries `[RequiresDynamicCode]` and `[RequiresUnreferencedCode]` attributes throughout its public API. On iOS, where Apple prohibits JIT compilation entirely, this is a non-starter for fully native AOT deployments.
- **Migrations are friction, not value.** On a server you run migrations against a shared database with a known lifecycle. On a mobile device, the database ships inside the app or is created on first launch. EF Core's migration pipeline (`Add-Migration`, `Update-Database`, `__EFMigrationsHistory`) adds complexity with no real benefit — there is no DBA, no staging environment, no rollback plan. A schema-free document store eliminates migrations entirely.
- **Heavy dependency graph.** EF Core pulls in `Microsoft.EntityFrameworkCore`, its SQLite provider, design-time packages, and their transitive dependencies. This increases app bundle size — a real concern when app stores enforce download size limits and users expect fast installs.
- **Relational overhead for non-relational data.** Mobile apps typically store user preferences, cached API responses, offline data queues, and local state. This data is naturally document-shaped (nested objects, variable structure). Forcing it into normalized tables with foreign keys and JOINs adds accidental complexity.

### Why this library fits

| Concern | EF Core | Shiny.DocumentDb |
|---|---|---|
| **AOT / trimming** | Reflection-heavy; no AOT support | Every API has optional `JsonTypeInfo<T>`; zero reflection required |
| **Database support** | Many providers | SQLite, LiteDB, CosmosDB, MongoDB, DuckDB, IndexedDB, MySQL, SQL Server, PostgreSQL, Oracle |
| **Migrations** | Required for every schema change | Not needed — schema-free JSON storage |
| **Nested objects** | Normalized tables, foreign keys, JOINs | Single document, single write, single read |
| **App bundle size** | Large dependency tree | Core package + one provider dependency |
| **Startup time** | DbContext model building, migration checks | Open connection and go |
| **Offline / sync patterns** | Complex change tracking | Store and retrieve document snapshots directly |

### AOT and trimming on mobile platforms

Ahead-of-Time compilation is not optional on Apple platforms — iOS, iPadOS, tvOS, and Mac Catalyst all prohibit JIT at the OS level. Android does not prohibit JIT, but AOT deployment (`PublishAot` or `AndroidEnableProfiledAot`) delivers measurably faster startup and lower memory usage, both of which directly affect user experience.

The .NET trimmer removes unreferenced code to shrink the app binary. Libraries that depend on reflection break under trimming because the trimmer cannot statically determine which types and members are accessed at runtime. This forces developers to either disable trimming (larger binaries) or maintain complex trimmer XML files.

This library avoids both problems:

- **Source-generated JSON serialization.** The `JsonSerializerContext` pattern generates serialization code at compile time. The trimmer can see every type that will be serialized, and the AOT compiler can compile every code path ahead of time.
- **No runtime expression compilation.** LINQ expressions are translated to SQL strings by a visitor — no `Expression.Compile()`, no `Reflection.Emit`, no dynamic delegates.
- **No model building.** There is no equivalent of EF Core's `OnModelCreating` that discovers entity types and relationships through reflection at startup.

### A `DbContext`-style API that fits MAUI's lifetime model

If you like EF Core's typed `DbContext`, the optional `DocumentContext` gives you the same shape — `db.Users.Where(...)`, `db.Orders.Insert(...)` — without the AOT, migration, and bundle-size costs above. MAUI has no per-request DI scope, which is exactly why EF Core recommends `IDbContextFactory<T>` there: register `services.AddAppContextFactory(...)`, inject the singleton `IDocumentContextFactory<AppContext>` anywhere (even into a singleton page or view-model), and call `factory.Create()` per operation. Unlike a `DbContext`, the created context is a stateless facade over the shared, thread-safe store — cheap to create and needing no disposal. (`services.AddAppContext(...)` keeps the scoped registration for ASP.NET Core back-ends.)

If you are building a .NET MAUI app and need local data persistence, this library gives you a queryable document store that works under full AOT and trimming without compromise.

## Benchmarks

Measured with [BenchmarkDotNet](https://benchmarkdotnet.org/) v0.15.8 on Apple M5 Pro, .NET 10.0.8, macOS. Four-way comparison: **Shiny.DocumentDb** vs **sqlite-net-pcl** vs **EF Core** (SQLite provider, run with pre-compiled `EF.CompileAsyncQuery` + `AsNoTracking` reads — its fastest configuration) vs **Dapper** (hand-written SQL over a raw `Microsoft.Data.Sqlite` connection — the micro-ORM "floor"). Full source in [`benchmarks/`](benchmarks/).

### Flat POCO (single table)

#### Insert (loop of single inserts)

| Method | Count | Mean |
|---|---|---|
| DocumentStore Insert | 10 | 389 µs |
| EF Core Insert | 10 | 877 µs |
| Dapper Insert | 10 | 1.70 ms |
| sqlite-net Insert | 10 | 1.78 ms |
| DocumentStore Insert | 100 | 3.54 ms |
| EF Core Insert | 100 | 7.28 ms |
| Dapper Insert | 100 | 17.16 ms |
| sqlite-net Insert | 100 | 17.65 ms |
| DocumentStore Insert | 1000 | 35.14 ms |
| EF Core Insert | 1000 | 93.02 ms |
| sqlite-net Insert | 1000 | 260.62 ms |
| Dapper Insert | 1000 | 385.10 ms |

> The document store writes one row per object; the others pay a round trip per row. DocumentStore stays ~2-3x ahead of EF Core. sqlite-net and Dapper (hand-written SQL over raw ADO.NET) trail because each un-batched insert auto-commits to disk — at 1000 rows that per-row latency dominates and gets noisy (high variance). For bulk writes, use batch inserts (below).

#### Batch insert

| Method | Count | Mean | Allocated |
|---|---|---|---|
| DocumentStore BatchInsert | 10 | 184 µs | 17.20 KB |
| sqlite-net InsertAllAsync | 10 | 320 µs | 5.23 KB |
| Dapper batch (transaction) | 10 | 346 µs | 16.32 KB |
| EF Core AddRange | 10 | 552 µs | 95.38 KB |
| sqlite-net InsertAllAsync | 100 | 437 µs | 45.08 KB |
| DocumentStore BatchInsert | 100 | 542 µs | 140.55 KB |
| Dapper batch (transaction) | 100 | 546 µs | 143.59 KB |
| EF Core AddRange | 100 | 3.41 ms | 864.96 KB |
| sqlite-net InsertAllAsync | 1000 | 1.52 ms | 438.83 KB |
| Dapper batch (transaction) | 1000 | 2.80 ms | 1,416.24 KB |
| DocumentStore BatchInsert | 1000 | 8.72 ms | 1,370.61 KB |
| EF Core AddRange | 1000 | 13.63 ms | 8,077.58 KB |

> DocumentStore leads at small batches (prepared-command reuse in one transaction). At 1000 items sqlite-net's simpler row structure takes the lead, with Dapper's transactional batch close behind; both raw-SQL contenders stay far ahead of EF Core, whose change tracker allocates 5–40x more.

#### Get by ID

| Method | Mean | Allocated |
|---|---|---|
| DocumentStore GetById | 2.73 µs | 2.55 KB |
| Dapper GetById | 4.96 µs | 1.98 KB |
| EF Core GetById (compiled) | 8.69 µs | 8.34 KB |
| sqlite-net GetById | 10.35 µs | 3.70 KB |

#### Get all

| Method | Count | Mean | Allocated |
|---|---|---|---|
| Dapper GetAll | 100 | 36.32 µs | 21.40 KB |
| EF Core GetAll (compiled) | 100 | 39.14 µs | 41.48 KB |
| sqlite-net GetAll | 100 | 46.12 µs | 28.38 KB |
| DocumentStore GetAll | 100 | 50.87 µs | 55.90 KB |
| Dapper GetAll | 1000 | 315.88 µs | 197.19 KB |
| EF Core GetAll (compiled) | 1000 | 317.24 µs | 343.83 KB |
| sqlite-net GetAll | 1000 | 396.97 µs | 246.35 KB |
| DocumentStore GetAll | 1000 | 500.26 µs | 541.06 KB |

#### Query (filter by name, 1000 records)

| Method | Mean | Allocated |
|---|---|---|
| Dapper Query | 21.47 µs | 2.05 KB |
| EF Core Query (compiled) | 25.13 µs | 8.33 KB |
| sqlite-net Query | 31.19 µs | 5.33 KB |
| DocumentStore Query | 160.52 µs | 5.09 KB |

> For flat single-column reads the relational contenders — Dapper fastest, then EF Core and sqlite-net — read native indexed columns directly while the document store uses `json_extract`. Add a JSON property index and the gap closes dramatically (see Index impact). The document store's architecture pays off on nested data below.

### Nested objects with child collections (Order + Address + OrderLines + Tags)

This is where the document store architecture pays off. sqlite-net needs 3 tables, 6 inserts per order, and 3 queries per read with manual rehydration. EF Core models the graph with related entities — read here with `Include` plus pre-compiled, no-tracking queries — but still pays for multi-table JOINs and graph materialization. Dapper issues hand-written SQL across the same 3 tables with manual rehydration (the relational floor). The document store stores and loads the entire object graph as one JSON document.

#### Insert (nested)

| Method | Count | Mean |
|---|---|---|
| DocumentStore Insert (nested) | 10 | 417 µs |
| EF Core Insert (3 tables) | 10 | 4.94 ms |
| Dapper Insert (3 tables) | 10 | 11.43 ms |
| sqlite-net Insert (3 tables) | 10 | 11.73 ms |
| DocumentStore Insert (nested) | 100 | 3.63 ms |
| EF Core Insert (3 tables) | 100 | 24.75 ms |
| sqlite-net Insert (3 tables) | 100 | 123.38 ms |
| Dapper Insert (3 tables) | 100 | 174.28 ms |
| DocumentStore Insert (nested) | 1000 | 36.31 ms |
| EF Core Insert (3 tables) | 1000 | 648.55 ms |
| Dapper Insert (3 tables) | 1000 | 1.99 s |
| sqlite-net Insert (3 tables) | 1000 | 2.18 s |

> Per-order inserts are un-batched (6 statements per order, each auto-committing), so the row-at-a-time contenders (sqlite-net, Dapper) get slow and high-variance at scale — the single-document write is the whole point.

#### Get by ID (nested)

| Method | Mean | Allocated |
|---|---|---|
| DocumentStore GetById (nested) | 3.47 µs | 4.43 KB |
| Dapper GetById (3 queries) | 18.56 µs | 6.99 KB |
| sqlite-net GetById (3 queries) | 27.84 µs | 16.05 KB |
| EF Core GetById (Include, compiled) | 32.03 µs | 15.64 KB |

#### Get all (nested)

| Method | Count | Mean | Allocated |
|---|---|---|---|
| DocumentStore GetAll (nested) | 100 | 127.2 µs | 244.10 KB |
| sqlite-net GetAll (3 tables + rehydrate) | 100 | 205.7 µs | 158.92 KB |
| Dapper GetAll (3 tables + rehydrate) | 100 | 253.6 µs | 192.94 KB |
| EF Core GetAll (Include, compiled) | 100 | 1.13 ms | 734.54 KB |
| DocumentStore GetAll (nested) | 1000 | 1.36 ms | 2,423.80 KB |
| sqlite-net GetAll (3 tables + rehydrate) | 1000 | 1.76 ms | 1,438.35 KB |
| Dapper GetAll (3 tables + rehydrate) | 1000 | 2.45 ms | 1,852.05 KB |
| EF Core GetAll (Include, compiled) | 1000 | 11.88 ms | 7,273.61 KB |

#### Query (nested, filter by status)

| Method | Mean | Allocated |
|---|---|---|
| DocumentStore Query (nested, by status) | 1.01 ms | 1,215.39 KB |
| sqlite-net Query (3 tables + rehydrate) | 1.42 ms | 1,013.48 KB |
| Dapper Query (3 tables + rehydrate) | 2.13 ms | 1,461.38 KB |
| EF Core Query (Include, compiled) | 5.90 ms | 3,641.24 KB |

> For nested data the document store is the clear winner: **6–60x faster inserts** and **2–9x faster reads** than the relational contenders, because it stores and retrieves the entire object graph in a single operation instead of multiple table writes and JOINs. Even Dapper — hand-written SQL, the relational floor — stays 2–9x behind on nested reads. EF Core's change tracking and graph materialization also make it the heaviest allocator by a wide margin.

### Index impact

JSON property indexes (`CreateIndexAsync`) dramatically speed up equality queries by letting SQLite use a B-tree lookup instead of scanning every row with `json_extract`.

#### Flat POCO query (filter by name, 1000 records)

| Method | Mean | Allocated |
|---|---|---|
| Query without index | 159.33 µs | 5.33 KB |
| Query with index | 6.03 µs | 5.33 KB |

> **~26x faster** — the indexed query resolves in microseconds because SQLite uses the partial index directly.

#### Nested query (filter by ShippingAddress.City, 1000 records, ~200 matches)

| Method | Mean | Allocated |
|---|---|---|
| Nested query without index | 660.5 µs | 487.34 KB |
| Nested query with index | 257.7 µs | 487.34 KB |

> **~2.6x faster** — the index eliminates the full table scan, but read + deserialize time for ~200 matching documents dominates. Indexes give the biggest wins on selective queries that return few results.

### Streaming (IAsyncEnumerable) vs buffered

Streaming yields results one-at-a-time without building an intermediate `List<T>`. Throughput is comparable; the benefit is reduced peak memory and eliminating Gen1 GC pressure at larger scales.

#### Flat POCO

| Method | Count | Mean | Gen1 | Allocated |
|---|---|---|---|---|
| ToList (buffered) | 100 | 51.00 µs | 0.61 | 55.90 KB |
| ToAsyncEnumerable (streaming) | 100 | 51.44 µs | — | 53.83 KB |
| ToList (buffered) | 1000 | 488.77 µs | 31.25 | 541.06 KB |
| ToAsyncEnumerable (streaming) | 1000 | 496.40 µs | — | 524.92 KB |

#### Nested objects

| Method | Count | Mean | Gen1 | Allocated |
|---|---|---|---|---|
| ToList nested (buffered) | 100 | 119.0 µs | 7.20 | 244.10 KB |
| ToAsyncEnumerable nested (streaming) | 100 | 118.1 µs | 0.24 | 242.03 KB |
| ToList nested (buffered) | 1000 | 1.25 ms | 109.38 | 2,423.80 KB |
| ToAsyncEnumerable nested (streaming) | 1000 | 1.16 ms | 1.95 | 2,407.66 KB |

#### Nested query (filter by status, ~500 matches from 1000)

| Method | Mean | Gen1 | Allocated |
|---|---|---|---|
| Query Where ToList (buffered) | 998.1 µs | 74.22 | 1.19 MB |
| Query Where ToAsyncEnumerable (streaming) | 967.2 µs | — | 1.18 MB |

> Streaming eliminates Gen1 GC collections entirely at scale. Throughput is within ~2% of buffered. Use streaming when you process results incrementally rather than needing the full list upfront.

## Installation

Install the core package plus the provider for your database:

```bash
# SQLite (mobile, embedded, local)
dotnet add package Shiny.DocumentDb.Sqlite

# sqlite-vec native binaries + auto-extension registration for vector search on iOS/Android/desktop
dotnet add package Shiny.DocumentDb.Sqlite.VectorSupport

# SQLCipher (encrypted SQLite)
dotnet add package Shiny.DocumentDb.Sqlite.SqlCipher

# MySQL
dotnet add package Shiny.DocumentDb.MySql

# SQL Server
dotnet add package Shiny.DocumentDb.SqlServer

# PostgreSQL
dotnet add package Shiny.DocumentDb.PostgreSql

# Oracle (23ai+)
dotnet add package Shiny.DocumentDb.Oracle

# LiteDB
dotnet add package Shiny.DocumentDb.LiteDb

# CosmosDB
dotnet add package Shiny.DocumentDb.CosmosDb

# MongoDB
dotnet add package Shiny.DocumentDb.MongoDb

# DuckDB (embedded analytical)
dotnet add package Shiny.DocumentDb.DuckDb

# IndexedDB (Blazor WebAssembly)
dotnet add package Shiny.DocumentDb.IndexedDb

# Reference geo data (US/Canada states, provinces, cities) — seeds into any provider
dotnet add package Shiny.DocumentDb.Geo
```

Dependency-injection registration (`AddDocumentStore`, `AddDocumentContext`, seeding) and OpenTelemetry
instrumentation ship **in the core `Shiny.DocumentDb` package** — no separate package to install.

## Setup

### Direct instantiation

```csharp
// SQLite
using Shiny.DocumentDb.Sqlite;
var store = new DocumentStore(new DocumentStoreOptions
{
    DatabaseProvider = new SqliteDatabaseProvider("Data Source=mydata.db")
});

// SQLCipher (encrypted SQLite)
using Shiny.DocumentDb.Sqlite.SqlCipher;
var store = new DocumentStore(new DocumentStoreOptions
{
    DatabaseProvider = new SqlCipherDatabaseProvider("encrypted.db", "mySecretKey")
});

// MySQL
using Shiny.DocumentDb.MySql;
var store = new DocumentStore(new DocumentStoreOptions
{
    DatabaseProvider = new MySqlDatabaseProvider("Server=localhost;Database=mydb;User=root;Password=pass")
});

// SQL Server
using Shiny.DocumentDb.SqlServer;
var store = new DocumentStore(new DocumentStoreOptions
{
    DatabaseProvider = new SqlServerDatabaseProvider("Server=localhost;Database=mydb;Trusted_Connection=true")
});

// PostgreSQL
using Shiny.DocumentDb.PostgreSql;
var store = new DocumentStore(new DocumentStoreOptions
{
    DatabaseProvider = new PostgreSqlDatabaseProvider("Host=localhost;Database=mydb;Username=postgres;Password=pass")
});

// Oracle (requires Oracle Database 23ai or later)
using Shiny.DocumentDb.Oracle;
var store = new DocumentStore(new DocumentStoreOptions
{
    DatabaseProvider = new OracleDatabaseProvider("User Id=myuser;Password=pass;Data Source=localhost:1521/FREEPDB1")
});

// LiteDB
using Shiny.DocumentDb.LiteDb;
var store = new LiteDbDocumentStore(new LiteDbDocumentStoreOptions
{
    ConnectionString = "Filename=mydata.db"
});

// CosmosDB
using Shiny.DocumentDb.CosmosDb;
var store = new CosmosDbDocumentStore(new CosmosDbDocumentStoreOptions
{
    ConnectionString = "AccountEndpoint=https://...;AccountKey=...",
    DatabaseName = "mydb",
    ContainerName = "documents"
});

// MongoDB
using Shiny.DocumentDb.MongoDb;
var store = new MongoDbDocumentStore(new MongoDbDocumentStoreOptions
{
    ConnectionString = "mongodb://localhost:27017",
    DatabaseName = "mydb"
});

// DuckDB (embedded analytical store)
using Shiny.DocumentDb.DuckDb;
var store = new DocumentStore(new DocumentStoreOptions
{
    DatabaseProvider = new DuckDbDatabaseProvider("Data Source=mydata.duckdb")
});

// IndexedDB (Blazor WebAssembly)
using Shiny.DocumentDb.IndexedDb;
// Requires IJSRuntime from DI — use the DI extension method below
```

> **Note:** `SqliteDocumentStore` and `SqlCipherDocumentStore` are still available as convenience wrappers that extend `DocumentStore`. They accept a connection string directly: `new SqliteDocumentStore("Data Source=mydata.db")` or `new SqlCipherDocumentStore("encrypted.db", "mySecretKey")`.

### Options reference

| Property | Type | Default | Description |
|---|---|---|---|
| `DatabaseProvider` | `IDatabaseProvider` | (required) | The database provider to use (e.g., `SqliteDatabaseProvider`, `MySqlDatabaseProvider`, `SqlServerDatabaseProvider`, `PostgreSqlDatabaseProvider`, `OracleDatabaseProvider`, `DuckDbDatabaseProvider`). LiteDB, CosmosDB, and MongoDB use their own options classes instead. |
| `TypeNameResolution` | `TypeNameResolution` | `ShortName` | How type names are stored — `ShortName` (e.g. `User`) or `FullName` (e.g. `MyApp.Models.User`) |
| `JsonSerializerOptions` | `JsonSerializerOptions?` | `null` | JSON serialization settings. When a `JsonSerializerContext` is attached as the `TypeInfoResolver`, all methods auto-resolve type info from the context |
| `UseReflectionFallback` | `bool` | `true` | When `false`, throws `InvalidOperationException` if a type can't be resolved from the configured `TypeInfoResolver` instead of falling back to reflection. Recommended for AOT deployments |
| `TableName` | `string` | `"documents"` | Name of the default shared document table. Types not explicitly mapped via `MapTypeToTable<T>()` are stored here |
| `Logging` | `Action<string>?` | `null` | Callback invoked with every SQL statement executed |

### Dependency injection

`AddDocumentStore` (in the core `Shiny.DocumentDb` package) registers `IDocumentStore` as a singleton:

```csharp
using Shiny.DocumentDb;

// SQLite
services.AddDocumentStore(opts =>
{
    opts.DatabaseProvider = new SqliteDatabaseProvider("Data Source=mydata.db");
});

// SQLCipher (encrypted SQLite)
services.AddDocumentStore(opts =>
{
    opts.DatabaseProvider = new SqlCipherDatabaseProvider("encrypted.db", "mySecretKey");
});

// SQL Server
services.AddDocumentStore(opts =>
{
    opts.DatabaseProvider = new SqlServerDatabaseProvider("Server=localhost;Database=mydb;Trusted_Connection=true");
});

// MySQL
services.AddDocumentStore(opts =>
{
    opts.DatabaseProvider = new MySqlDatabaseProvider("Server=localhost;Database=mydb;User=root;Password=pass");
});

// PostgreSQL
services.AddDocumentStore(opts =>
{
    opts.DatabaseProvider = new PostgreSqlDatabaseProvider("Host=localhost;Database=mydb;Username=postgres;Password=pass");
});

// Oracle (requires Oracle Database 23ai or later)
services.AddDocumentStore(opts =>
{
    opts.DatabaseProvider = new OracleDatabaseProvider("User Id=myuser;Password=pass;Data Source=localhost:1521/FREEPDB1");
});

// DuckDB (embedded analytical)
services.AddDocumentStore(opts =>
{
    opts.DatabaseProvider = new DuckDbDatabaseProvider("Data Source=mydata.duckdb");
});

// Full options configuration
services.AddDocumentStore(opts =>
{
    opts.DatabaseProvider = new SqliteDatabaseProvider("Data Source=mydata.db");
    opts.TypeNameResolution = TypeNameResolution.FullName;
    opts.JsonSerializerOptions = new JsonSerializerOptions
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase
    };
});
```

#### Named stores (multiple databases)

Register multiple stores by name using .NET keyed services:

```csharp
services.AddDocumentStore("users", opts =>
{
    opts.DatabaseProvider = new SqliteDatabaseProvider("Data Source=users.db");
});
services.AddDocumentStore("analytics", opts =>
{
    opts.DatabaseProvider = new PostgreSqlDatabaseProvider("Host=...");
});

// Inject via keyed services attribute
public class MyService(
    [FromKeyedServices("users")] IDocumentStore userStore,
    [FromKeyedServices("analytics")] IDocumentStore analyticsStore) { }

// Or resolve dynamically via IDocumentSessionFactory
public class MyService(IDocumentSessionFactory stores)
{
    void DoWork() => stores.GetStore("users").Insert(...);
}
```

## Table-Per-Type Mapping

By default all document types share a single table (`"documents"`). You can map specific types to dedicated tables while unmapped types continue using the shared table.

### Basic mapping

```csharp
var store = new DocumentStore(new DocumentStoreOptions
{
    DatabaseProvider = new SqliteDatabaseProvider("Data Source=mydata.db")
}.MapTypeToTable<User>()            // auto-derived table name → "User"
 .MapTypeToTable<Order>("orders")   // explicit table name
);

// Users → "User" table, Orders → "orders" table, everything else → "documents"
```

### Custom Id property

Document types can use an alternate property as the document Id instead of the default `Id`. The Id property must be `Guid`, `int`, `long`, or `string`. Custom Ids can be combined with `MapTypeToTable`, or used on their own with `MapIdProperty` to keep the type in the default shared table.

```csharp
var store = new DocumentStore(new DocumentStoreOptions
{
    DatabaseProvider = new SqliteDatabaseProvider("Data Source=mydata.db")
}
// Dedicated table + custom Id
.MapTypeToTable<Customer>("customers", c => c.CustomerId)
.MapTypeToTable<Sensor>("sensors", s => s.DeviceKey)
// Default shared table + custom Id
.MapIdProperty<BlogPost>(p => p.Slug)
);
```

Auto-generation rules still apply — `Guid` and numeric Ids are auto-generated when default, and the value is written back to the property after insert.

### Custom Id types

Beyond the built-in `Guid`/`int`/`long`/`string`, register a converter with `MapIdType` to use any Id CLR type — a `Ulid`, or a strongly-typed wrapper such as `record struct OrderId(Guid Value)`. The Id is still stored as a string in every provider (no schema/on-disk change); the converter just defines how it round-trips. Purely additive — the built-in types need no registration and behave exactly as before.

```csharp
public readonly record struct OrderId(Guid Value)
{
    public static OrderId New() => new(Guid.NewGuid());
}

var store = new DocumentStore(new DocumentStoreOptions
{
    DatabaseProvider = new SqliteDatabaseProvider("Data Source=mydata.db")
}
.MapIdType(
    toString:  (OrderId id) => id.Value.ToString("N"),
    parse:     s => new OrderId(Guid.ParseExact(s, "N")),
    isDefault: id => id.Value == Guid.Empty,   // when to auto-generate on Insert
    generate:  OrderId.New)                    // optional; omit to require explicit Ids
);

// Insert/Get/Update/Remove all accept the strongly-typed Id
var order = new Order { Customer = "Alice" };   // class Order { public OrderId Id { get; set; } … }
await store.Insert(order);                       // Id auto-generated
var fetched = await store.Get<Order>(order.Id);
```

A `DocumentIdConverter<TId>` base class is available for reusable converters (`ToStorageString` / `FromStorageString` / `IsDefault` / `TryGenerate`), and `MapIdType` is on every provider's options. Because the Id also lives inside the JSON `Data` blob, give a custom Id type a matching `System.Text.Json` converter so LINQ predicates on the Id line up with the stored form.

For sortable Guid Ids without any extra dependency, call `options.UseGuidV7Ids()` — `Guid` Ids then auto-generate as time-ordered **version 7** GUIDs (`Guid.CreateVersion7()`) instead of random v4. Storage format is unchanged, so it is a drop-in for existing data. (`long` is also a built-in Id type if you just want a sequential integer key.)

### API reference

| Overload | Description |
|---|---|
| `MapTypeToTable<T>()` | Auto-derive table name, default `Id` property |
| `MapTypeToTable<T>(tableName)` | Explicit table name, default `Id` property |
| `MapTypeToTable<T>(idProperty)` | Auto-derive table name, custom Id property |
| `MapTypeToTable<T>(tableName, idProperty)` | Explicit table name, custom Id property |
| `MapIdProperty<T>(idProperty)` | Custom Id property only — type stays in the default shared table |
| `MapIdProperty<T>(propertyName)` | AOT-safe string overload of the above |

- **Fluent** — all overloads return `DocumentStoreOptions` for chaining
- **Duplicate protection** — mapping two types to the same table throws `ArgumentException`
- **AOT-safe** — type and property names are resolved at registration time, not at runtime
- **Id remapping is independent of table mapping** — use `MapIdProperty` to override the Id without dedicating a table, or `MapTypeToTable(idProperty)` to do both at once
- Tables are lazily created on first use with the same schema and composite primary key

## AOT Setup

For AOT/trimming compatibility, create a source-generated JSON context:

```csharp
[JsonSerializable(typeof(User))]
[JsonSerializable(typeof(Order))]
[JsonSerializable(typeof(Address))]
[JsonSerializable(typeof(OrderLine))]
public partial class AppJsonContext : JsonSerializerContext;
```

Then create an instance with your desired options:

```csharp
var ctx = new AppJsonContext(new JsonSerializerOptions
{
    PropertyNamingPolicy = JsonNamingPolicy.CamelCase
});
```

Pass `ctx.Options` to `DocumentStoreOptions.JsonSerializerOptions` so that the expression visitor and serializer share the same configuration.

### Optional JsonTypeInfo<T> parameters

All `JsonTypeInfo<T>` parameters across the entire API are optional (`= null` default). When omitted, type info is automatically resolved from the configured `JsonSerializerOptions.TypeInfoResolver`. This means you can configure a `JsonSerializerContext` once and skip passing `JsonTypeInfo<T>` on every call — while retaining full AOT safety.

#### Setup

```csharp
var ctx = new AppJsonContext(new JsonSerializerOptions
{
    PropertyNamingPolicy = JsonNamingPolicy.CamelCase
});

var store = new DocumentStore(new DocumentStoreOptions
{
    DatabaseProvider = new SqliteDatabaseProvider("Data Source=mydata.db"),
    JsonSerializerOptions = ctx.Options,
    UseReflectionFallback = false // recommended for AOT
});
```

#### Multiple JSON contexts

If your types are spread across multiple `JsonSerializerContext` classes, use `TypeInfoResolverChain` to combine them. The chain is tried in order — the first context that knows about the requested type wins.

```csharp
var options = new JsonSerializerOptions
{
    PropertyNamingPolicy = JsonNamingPolicy.CamelCase
};
options.TypeInfoResolverChain.Add(UserJsonContext.Default);
options.TypeInfoResolverChain.Add(OrderJsonContext.Default);

var store = new DocumentStore(new DocumentStoreOptions
{
    DatabaseProvider = new SqliteDatabaseProvider("Data Source=mydata.db"),
    JsonSerializerOptions = options,
    UseReflectionFallback = false
});
```

#### Before vs after

| With explicit `JsonTypeInfo<T>` | With auto-resolution (recommended) |
|---|---|
| `store.Insert(user, ctx.User)` | `store.Insert(user)` |
| `store.Get("id", ctx.User)` | `store.Get<User>("id")` |
| `store.Upsert(patch, ctx.User)` | `store.Upsert(patch)` |
| `store.SetProperty("id", (User u) => u.Age, 31, ctx.User)` | `store.SetProperty<User>("id", u => u.Age, 31)` |
| `store.RemoveProperty("id", (User u) => u.Email, ctx.User)` | `store.RemoveProperty<User>("id", u => u.Email)` |

> **Note:** `Get`, `Remove`, `SetProperty`, and `RemoveProperty` accept the Id as `object` — you can pass a `Guid`, `int`, `long`, or `string` directly. Passing an unsupported type throws `ArgumentException`.
| `store.Query(ctx.User)` | `store.Query<User>()` |
| `store.Query<User>("sql", ctx.User, parms)` | `store.Query<User>("sql", parameters: parms)` |
| `store.QueryStream<User>("sql", ctx.User, parms)` | `store.QueryStream<User>("sql", parameters: parms)` |

#### Example

```csharp
// All of these are AOT-safe when ctx.Options is configured
var user = new User { Name = "Alice", Age = 25 };
await store.Insert(user); // user.Id is auto-generated
var fetched = await store.Get<User>(user.Id);
var all = await store.Query<User>().ToList();
await store.Upsert(new User { Id = user.Id, Name = "Alice", Age = 30 });

var results = await store.Query<User>(
    "json_extract(Data, '$.age') > @minAge",
    parameters: new { minAge = 30 });

await foreach (var u in store.Query<User>().ToAsyncEnumerable())
    Console.WriteLine(u.Name);
```

#### How it works

Each method checks `JsonSerializerOptions.TryGetTypeInfo(typeof(T))` before falling back to reflection. If the resolver returns a `JsonTypeInfo<T>`, it is used for serialization. When `UseReflectionFallback = false` and no type info can be resolved, a clear `InvalidOperationException` is thrown.

#### Reflection fallback behavior

By default (`UseReflectionFallback = true`), if no `TypeInfoResolver` is configured or the type isn't registered in the context, methods fall back to reflection-based serialization. Existing code without a `JsonSerializerContext` continues to work unchanged.

**For AOT deployments, set `UseReflectionFallback = false`.** Reflection-based serialization produces hard-to-diagnose errors under trimming and AOT. With this flag disabled, you get a clear `InvalidOperationException` at the point of use:

```
InvalidOperationException: No JsonTypeInfo registered for type 'MyApp.UnregisteredType'.
Register it in your JsonSerializerContext or pass a JsonTypeInfo<UnregisteredType> explicitly.
```

This tells you exactly which type is missing and what to do about it. Every type must either be registered in your `JsonSerializerContext` via `[JsonSerializable(typeof(T))]` or passed with an explicit `JsonTypeInfo<T>` parameter.

## Document Types

Every document type must have a public `Id` property of type `Guid`, `int`, `long`, or `string`. The Id is stored in both the SQLite `Id` column and inside the JSON blob, so query results always include it.

```csharp
public class User
{
    public string Id { get; set; } = "";
    public string Name { get; set; } = "";
    public int Age { get; set; }
    public string? Email { get; set; }
}
```

### Auto-generation rules

| Id CLR Type | Default Value | Auto-Gen Strategy |
|-------------|--------------|-------------------|
| `Guid` | `Guid.Empty` | `Guid.NewGuid()` |
| `string` | `null` or `""` | `Guid.NewGuid().ToString("N")` |
| `int` | `0` | `MAX(CAST(Id AS INTEGER)) + 1` per TypeName |
| `long` | `0` | `MAX(CAST(Id AS INTEGER)) + 1` per TypeName |

When `Insert` is called with a default Id, the store auto-generates one and writes it back to the object. When a non-default Id is provided, it is used as-is. If a document with the same Id already exists, `Insert` throws an exception.

## Basic CRUD Operations

### Insert a document (auto-generated ID)

```csharp
var user = new User { Name = "Alice", Age = 25 };
await store.Insert(user);
// user.Id is now populated
```

### Insert a document (explicit ID)

```csharp
await store.Insert(new User { Id = "user-1", Name = "Alice", Age = 25 });
// Throws if "user-1" already exists
```

### Batch insert

`BatchInsert` inserts multiple documents in a single transaction with prepared command reuse for optimal performance. Returns the count inserted. If any document fails (e.g. duplicate Id), the entire batch is rolled back. Auto-generates IDs for Guid, int, and long Id types.

```csharp
var users = Enumerable.Range(1, 1000).Select(i => new User
{
    Id = $"user-{i}",
    Name = $"User {i}",
    Age = 20 + (i % 50)
});

var count = await store.BatchInsert(users); // 1000 — single transaction, prepared command reused

// Works with auto-generated IDs too
var models = Enumerable.Range(1, 500).Select(i => new GuidIdModel { Name = $"Item {i}" }).ToList();
await store.BatchInsert(models); // All Ids auto-populated

// Group writes atomically with a session (store.OpenSession()) (contiguous same-type inserts coalesce into the batch path)
await using var uow = store.OpenSession();
uow.AddRange(moreUsers).Add(singleUser);
await uow.SaveChanges(); // all committed or rolled back together
```

### Update a document (full replacement)

`Update` replaces the entire document. The document must have a non-default Id and must already exist; otherwise an exception is thrown.

```csharp
await store.Update(new User { Id = "user-1", Name = "Alice", Age = 26 });
```

### Upsert with JSON Merge Patch

`Upsert` uses SQLite's `json_patch()` (RFC 7396 JSON Merge Patch) to deep-merge a partial patch into an existing document. If the document doesn't exist, it is inserted as-is. Unlike `Update`, which replaces the entire document, `Upsert` only overwrites the fields present in the patch. The document must have a non-default Id.

```csharp
// Insert a full document
await store.Insert(new User { Id = "user-1", Name = "Alice", Age = 25, Email = "alice@test.com" });

// Merge patch — only update Name and Age, preserve Email
await store.Upsert(new User { Id = "user-1", Name = "Alice", Age = 30 });

var user = await store.Get<User>("user-1");
// user.Name == "Alice", user.Age == 30, user.Email == "alice@test.com" (preserved)
```

**How it works:**
- On **insert** (new ID): the patch is stored as the full document.
- On **conflict** (existing ID): `json_patch(existing, patch)` deep-merges the patch into the stored JSON. Objects are recursively merged; scalars and arrays are replaced.
- **Null properties are excluded** from the patch automatically. In C#, unset nullable properties (e.g. `string? Email`) serialize as `null`, which would remove the key under RFC 7396. The library strips these so that unset fields are preserved rather than deleted.

> **Tip:** For true partial updates, use nullable properties in your patch type so that unset fields are `null` and excluded from the merge. Non-nullable properties with default initializers (e.g. `string Name = ""`) will always be included in the patch.

### Update a single property (SetProperty)

`SetProperty` updates a single scalar field in-place using SQLite's `json_set()` — no deserialization, no full document replacement. Returns `true` if the document was found and updated, `false` if not found.

```csharp
// Update a scalar field
await store.SetProperty<User>("user-1", u => u.Age, 31);

// Update a string field
await store.SetProperty<User>("user-1", u => u.Email, "newemail@test.com");

// Set a field to null
await store.SetProperty<User>("user-1", u => u.Email, null);

// Nested property — update a city within a shipping address
await store.SetProperty<Order>("order-1", o => o.ShippingAddress.City, "Portland");

// Check if the document existed
bool updated = await store.SetProperty<User>("user-1", u => u.Age, 31);
if (!updated)
    Console.WriteLine("Document not found");
```

**How it works:** The expression `u => u.Age` is resolved to the JSON path `$.age` (respecting `[JsonPropertyName]` attributes and naming policies). The SQL executed is:

```sql
UPDATE documents
SET Data = json_set(Data, '$.age', json('31')), UpdatedAt = @now
WHERE Id = @id AND TypeName = @typeName;
```

**Supported value types:** `SetProperty` is designed for scalar values — `string`, `int`, `long`, `double`, `float`, `decimal`, `bool`, and `null`. It does not support setting collection or complex object values. To replace a nested object or array, use `Update` (full replacement) or `Upsert` (merge patch).

### Remove a single property (RemoveProperty)

`RemoveProperty` strips a field from the stored JSON using SQLite's `json_remove()`. Returns `true` if the document was found and updated, `false` if not found. When the document is later deserialized, the removed field will have its C# default value.

```csharp
// Remove a nullable field
await store.RemoveProperty<User>("user-1", u => u.Email);

// Remove a nested property
await store.RemoveProperty<Order>("order-1", o => o.ShippingAddress.City);

// Remove a collection property (removes the entire array from the JSON)
await store.RemoveProperty<Order>("order-1", o => o.Tags);

// Check if the document existed
bool updated = await store.RemoveProperty<User>("user-1", u => u.Email);
```

**How it works:** The SQL executed is:

```sql
UPDATE documents
SET Data = json_remove(Data, '$.email'), UpdatedAt = @now
WHERE Id = @id AND TypeName = @typeName;
```

Unlike `SetProperty`, `RemoveProperty` works on any property type — scalar, nested object, or collection — because it simply removes the key from the JSON regardless of the value's shape.

### SetProperty vs RemoveProperty vs Upsert vs Insert vs Update

| Operation | Use when | Scope | Collections |
|---|---|---|---|
| `SetProperty` | Changing one scalar field | Single field, in-place `json_set` | Scalar values only |
| `RemoveProperty` | Stripping a field from the document | Single field, in-place `json_remove` | Works on any property type |
| `Upsert` | Patching multiple fields at once | Deep merge via `json_patch` | Replaces arrays entirely (RFC 7396) |
| `Insert` | Adding a new document | Full document write; throws if Id exists | Full control |
| `Update` | Replacing an existing document | Full replacement; throws if not found | Full control |
| `GetDiff` | Diffing local changes vs stored state | Read-only; returns RFC 6902 patch | Deep nested diff; arrays replaced as whole |

### Get a document by ID

The `id` parameter accepts `Guid`, `int`, `long`, or `string`. Passing an unsupported type throws `ArgumentException`.

```csharp
var user = await store.Get<User>("user-1");

// Guid, int, and long Ids work directly — no ToString() needed
var item = await store.Get<GuidIdModel>(myGuid);
var order = await store.Get<IntIdModel>(42);
```

### Diff against stored document (GetDiff)

Compare a modified object against the stored document and get an RFC 6902 `JsonPatchDocument<T>` describing the differences. Returns `null` if no document with that ID exists.

Requires the [SystemTextJsonPatch](https://www.nuget.org/packages/SystemTextJsonPatch) package (included as a dependency).

```csharp
// Fetch the stored order, propose changes
var proposed = new Order
{
    Id = "ord-1", CustomerName = "Alice", Status = "Delivered",
    ShippingAddress = new() { City = "Seattle", State = "WA" },
    Lines = [new() { ProductName = "Widget", Quantity = 10, UnitPrice = 8.99m }],
    Tags = ["priority", "expedited"]
};

// Get a patch describing what changed
var patch = await store.GetDiff("ord-1", proposed);
// patch.Operations contains:
//   Replace /status → Delivered
//   Replace /shippingAddress/city → Seattle
//   Replace /shippingAddress/state → WA
//   Replace /lines → [...]
//   Replace /tags → [...]

// Apply the patch to any instance of the same type
var current = await store.Get<Order>("ord-1");
patch!.ApplyTo(current!);
```

The diff is deep — nested objects produce individual property-level operations (e.g. `/shippingAddress/city`), while arrays and collections are replaced as a whole.

### Remove a document

```csharp
bool deleted = await store.Remove<User>("user-1");
bool removed = await store.Remove<GuidIdModel>(myGuid);
```

### Clear all documents of a type

```csharp
int deletedCount = await store.Clear<User>();
```

## Optimistic Concurrency (Row Versioning)

Map a version property on your document type for automatic optimistic concurrency checks. The version is stored inside the JSON blob — no schema or table changes required. Works across all providers (SQLite, LiteDB, CosmosDB, IndexedDB, MySQL, SQL Server, PostgreSQL, Oracle).

### Configuration

```csharp
// Expression-based (reflection)
var store = new DocumentStore(new DocumentStoreOptions
{
    DatabaseProvider = new SqliteDatabaseProvider("Data Source=mydata.db")
}.MapVersionProperty<Order>(o => o.RowVersion));

// AOT-safe overload with explicit getter/setter
var store = new DocumentStore(new DocumentStoreOptions
{
    DatabaseProvider = new SqliteDatabaseProvider("Data Source=mydata.db")
}.MapVersionProperty<Order>("RowVersion", o => o.RowVersion, (o, v) => o.RowVersion = v));
```

All provider options classes support `MapVersionProperty`: `DocumentStoreOptions`, `LiteDbDocumentStoreOptions`, `CosmosDbDocumentStoreOptions`, and `IndexedDbDocumentStoreOptions`.

### How it works

| Operation | Behavior |
|---|---|
| `Insert` | Version is set to **1** before serialization |
| `Update` | Reads the expected version from the object, checks it against the stored version, then increments. Throws `ConcurrencyException` on mismatch |
| `Upsert` | Insert path sets version to 1. Update path checks and increments (only when the existing version > 0) |
| `BatchInsert` | Version is set to 1 for each document |

### Example

```csharp
public class Order
{
    public string Id { get; set; } = "";
    public string Status { get; set; } = "";
    public int RowVersion { get; set; }
}

// Insert — RowVersion is set to 1
var order = new Order { Id = "ord-1", Status = "Pending" };
await store.Insert(order);
// order.RowVersion == 1

// Update — RowVersion is checked and incremented
order.Status = "Shipped";
await store.Update(order);
// order.RowVersion == 2

// Concurrent update — throws ConcurrencyException
var staleOrder = new Order { Id = "ord-1", Status = "Cancelled", RowVersion = 1 };
await store.Update(staleOrder); // throws ConcurrencyException
```

### ConcurrencyException

Thrown when a version mismatch is detected. Provides diagnostic properties:

| Property | Type | Description |
|---|---|---|
| `TypeName` | `string` | The document type name |
| `DocumentId` | `string` | The document Id |
| `ExpectedVersion` | `int` | The version the caller expected |
| `ActualVersion` | `int?` | The version found in the store (when available) |

```csharp
try
{
    await store.Update(staleOrder);
}
catch (ConcurrencyException ex)
{
    Console.WriteLine($"Conflict on {ex.TypeName} {ex.DocumentId}: expected v{ex.ExpectedVersion}, found v{ex.ActualVersion}");
}
```

## Fluent Query Builder

The fluent query builder is the primary way to query, filter, sort, paginate, project, aggregate, stream, and delete documents. Start with `store.Query<T>()` and chain builder methods, then terminate with a materialization method.

### Builder methods (non-executing)

| Method | Description |
|---|---|
| `.Where(predicate)` | Filter by LINQ expression. Multiple calls combine with AND. |
| `.Where(filter[, jsonTypeInfo])` | Filter by a runtime filter string (e.g. `"Age >= 30 and Status == 'open'"`) — AOT-safe. Supports `and`/`or`/`not`, comparisons, `is [not] null`, `in (…)`, `contains/startsWith/endsWith`. `jsonTypeInfo` is optional — reused from the query when omitted. |
| `.Where($"…"[, jsonTypeInfo])` | Interpolated filter — each `{value}` hole is captured as a typed argument and bound as a parameter (no quoting, injection-safe; the Dapper/InterpolatedSql pattern), e.g. `Where($"Age >= {min} and Status == {status}")`. Same grammar as `Where(string)`; an interpolated literal binds here, a plain `string` binds to the parsed overload. |
| `.OrderBy(selector)` / `.OrderByDescending(selector)` | Sort by property (expression). |
| `.OrderBy(name[, jsonTypeInfo])` / `.OrderByDescending(name[, jsonTypeInfo])` | Sort by property name (string) — AOT-safe. Case-insensitive CLR or JSON name; supports dotted paths. `jsonTypeInfo` is optional — reused from the query when omitted. |
| `.OrderBy(name, direction[, jsonTypeInfo])` | Sort by property name with a runtime direction string (`"asc"`/`"ascending"`/`"desc"`/`"descending"`, case-insensitive; empty defaults to ascending). `jsonTypeInfo` optional. |
| `.GroupBy(selector)` | Group by property (for aggregate projections with `Sql.*` markers). |
| `.Paginate(offset, take)` | Limit results with SQL `LIMIT`/`OFFSET`. |
| `.Select(selector, resultTypeInfo?)` | Project into a different shape via `json_object`. |
| `.Project(fields[, jsonTypeInfo])` | Project a runtime-chosen field list (e.g. `"name,email"`) into `IDocumentQuery<JsonObject>` — AOT-safe. Ideal for REST sparse fieldsets; no DTO required. `jsonTypeInfo` optional. |

### Terminal methods (execute SQL)

| Method | Returns | Description |
|---|---|---|
| `.ToList()` | `Task<IReadOnlyList<T>>` | Materialize all results into a list. |
| `.ToAsyncEnumerable()` | `IAsyncEnumerable<T>` | Stream results one-at-a-time without buffering. |
| `.Count()` | `Task<long>` | Count matching documents. |
| `.Any()` | `Task<bool>` | Check if any documents match. |
| `.ExecuteDelete()` | `Task<int>` | Delete matching documents and return count deleted. |
| `.ExecuteUpdate(property, value)` | `Task<int>` | Update a property on all matching documents via `json_set()` and return count updated. |
| `.Max(selector)` | `Task<TValue>` | Maximum value of a property. |
| `.Min(selector)` | `Task<TValue>` | Minimum value of a property. |
| `.Sum(selector)` | `Task<TValue>` | Sum of a property. |
| `.Average(selector)` | `Task<double>` | Average of a property. |
| `.PageResult(page, pageSize, zeroBased?)` | `Task<PagedResults<T>>` | Run the query and return records + total count in one call. 1-based by default. |

### Get all documents of a type

```csharp
var users = await store.Query<User>().ToList();
```

### Expression-based queries

The preferred way to query. Property names are resolved from `JsonTypeInfo` metadata, so `[JsonPropertyName]` attributes and naming policies are respected automatically.

#### Equality and comparisons

```csharp
var results = await store.Query<User>().Where(u => u.Name == "Alice").ToList();
var older = await store.Query<User>().Where(u => u.Age > 30).ToList();
var young = await store.Query<User>().Where(u => u.Age <= 25).ToList();
```

#### Logical operators

```csharp
var results = await store.Query<User>().Where(u => u.Age == 25 && u.Name == "Alice").ToList();
var results = await store.Query<User>().Where(u => u.Name == "Alice" || u.Name == "Bob").ToList();
var results = await store.Query<User>().Where(u => !(u.Name == "Alice")).ToList();
```

#### Null checks

```csharp
var noEmail = await store.Query<User>().Where(u => u.Email == null).ToList();
var hasEmail = await store.Query<User>().Where(u => u.Email != null).ToList();
```

#### String methods

```csharp
var results = await store.Query<User>().Where(u => u.Name.Contains("li")).ToList();
var results = await store.Query<User>().Where(u => u.Name.StartsWith("Al")).ToList();
var results = await store.Query<User>().Where(u => u.Name.EndsWith("ob")).ToList();
```

#### Nested object properties

```csharp
var results = await store.Query<Order>().Where(o => o.ShippingAddress.City == "Portland").ToList();
```

#### Collection queries with Any()

```csharp
// Object collection — filter by child property
var results = await store.Query<Order>()
    .Where(o => o.Lines.Any(l => l.ProductName == "Widget"))
    .ToList();

// Primitive collection — filter by value
var results = await store.Query<Order>()
    .Where(o => o.Tags.Any(t => t == "priority"))
    .ToList();

// Check if a collection has any elements
var results = await store.Query<Order>().Where(o => o.Tags.Any()).ToList();
```

#### Collection queries with Count()

```csharp
// Count elements (no predicate)
var results = await store.Query<Order>().Where(o => o.Lines.Count() > 1).ToList();

// Count matching elements (with predicate)
var results = await store.Query<Order>()
    .Where(o => o.Lines.Count(l => l.Quantity >= 3) >= 1)
    .ToList();

// Property form — collection .Count and array .Length translate to the
// same array-length function as .Count(), so use whichever reads cleaner
var empty = await store.Query<Order>().Where(o => o.Lines.Count == 0).ToList();
var multi = await store.Query<Order>().Where(o => o.Tags.Count > 1).ToList();
```

#### DateTime and DateTimeOffset queries

DateTime and DateTimeOffset values are formatted to match System.Text.Json's default ISO 8601 output, so comparisons work correctly with stored JSON.

```csharp
var cutoff = new DateTime(2025, 1, 1, 0, 0, 0, DateTimeKind.Utc);
var upcoming = await store.Query<Event>().Where(e => e.StartDate > cutoff).ToList();

var start = new DateTimeOffset(2025, 1, 1, 0, 0, 0, TimeSpan.Zero);
var end = new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);
var inRange = await store.Query<Event>()
    .Where(e => e.CreatedAt >= start && e.CreatedAt < end)
    .ToList();
```

#### Captured variables

```csharp
var targetName = "Alice";
var results = await store.Query<User>().Where(u => u.Name == targetName).ToList();
```

### Counting with expressions

```csharp
var count = await store.Query<User>().Where(u => u.Age == 25).Count();

// With collection predicates
var count = await store.Query<Order>()
    .Where(o => o.Lines.Any(l => l.ProductName == "Gadget"))
    .Count();

var count = await store.Query<Order>().Where(o => o.Lines.Count() > 1).Count();
```

### Bulk delete with ExecuteDelete

Delete documents matching a predicate in a single SQL DELETE — no need to query first.

```csharp
// Simple predicate — returns number of deleted rows
int deleted = await store.Query<User>().Where(u => u.Age < 18).ExecuteDelete();

// Complex predicates with && and ||
int deleted = await store.Query<Order>()
    .Where(o => o.ShippingAddress.City == "Portland" || o.Status == "Cancelled")
    .ExecuteDelete();

// Nested properties
int deleted = await store.Query<Order>()
    .Where(o => o.ShippingAddress.State == "OR")
    .ExecuteDelete();

// Captured variables
var cutoffAge = 65;
int deleted = await store.Query<User>().Where(u => u.Age > cutoffAge).ExecuteDelete();
```

### Bulk update with ExecuteUpdate

Update a single property on all matching documents in a single SQL UPDATE via `json_set()` — no deserialization needed.

```csharp
// Update a scalar property on filtered docs
int updated = await store.Query<User>()
    .Where(u => u.Age < 18)
    .ExecuteUpdate(u => u.Age, 18);

// Update a nested property
int updated = await store.Query<Order>()
    .Where(o => o.ShippingAddress.City == "Portland")
    .ExecuteUpdate(o => o.ShippingAddress.City, "Eugene");

// Set a property to null
int updated = await store.Query<User>()
    .Where(u => u.Name == "Alice")
    .ExecuteUpdate(u => u.Email, null);

// Update all documents of a type (no Where)
int updated = await store.Query<User>().ExecuteUpdate(u => u.Age, 0);
```

### Ordering

Sort results at the SQL level using the fluent `.OrderBy()` and `.OrderByDescending()` methods.

```csharp
// Ascending
var users = await store.Query<User>().OrderBy(u => u.Age).ToList();

// Descending
var users = await store.Query<User>().OrderByDescending(u => u.Age).ToList();

// With filter
var results = await store.Query<User>()
    .Where(u => u.Age > 25)
    .OrderBy(u => u.Name)
    .ToList();

// With streaming
await foreach (var user in store.Query<User>().OrderByDescending(u => u.Age).ToAsyncEnumerable())
{
    Console.WriteLine(user.Name);
}
```

Generated SQL: `ORDER BY json_extract(Data, '$.age') ASC`

#### Sort by property name (string, AOT-safe)

When the sort column is selected at runtime (sortable table headers, REST `?sort=` query strings, etc.), use the string-based overloads. They resolve the property through `JsonTypeInfo<T>` — no `Type.GetProperty(string)` reflection on `T`, so they stay AOT/trim-safe.

> The `jsonTypeInfo` argument is **optional** on all the string overloads (`Where`/`OrderBy`/`Project`). When omitted, the query reuses the `JsonTypeInfo<T>` it resolved when it was created (from `Query(ctx.User)` or the registered `JsonSerializerContext`), so you rarely need to pass it. The examples below pass it explicitly for clarity, but `store.Query(ctx.User).OrderBy("Name")` works the same.

```csharp
// jsonTypeInfo omitted — reused from the query's context
var byName = await store.Query<User>(ctx.User).OrderBy("Name").ToList();

// Sort by CLR property name (explicit JsonTypeInfo)
var byName = await store.Query<User>().OrderBy("Name", ctx.User).ToList();

// Or by JSON name (after the configured naming policy)
var byName = await store.Query<User>().OrderBy("name", ctx.User).ToList();

// Descending
var oldest = await store.Query<User>().OrderByDescending("Age", ctx.User).ToList();

// Dotted path for nested properties
var orders = await store.Query<Order>().OrderBy("ShippingAddress.City", ctx.Order).ToList();

// Driven by external input
var results = await store.Query<User>()
    .Where(u => u.Active)
    .OrderBy(request.Sort ?? "Name", ctx.User)
    .ToList();

// Direction as a runtime string too — handy for `?sort=name&dir=desc`.
// Accepts "asc"/"ascending"/"desc"/"descending" (case-insensitive);
// an empty/null/whitespace direction defaults to ascending.
var page = await store.Query<User>()
    .OrderBy(request.Sort ?? "Name", request.Dir, ctx.User)
    .ToList();
```

Matching is case-insensitive against either the CLR property name or the JSON property name. Each nested type in a dotted path must also be registered in your `JsonSerializerContext`. Unknown property names throw `ArgumentException`.

#### Set membership — `WhereIn` / `WhereNotIn`

Pass an in-memory collection and filter to documents whose property is (or isn't) one of its values — the `IN` / `NOT IN` pattern. The collection is lowered to each store's native construct (relational `IN (…)`, Cosmos `IN`, Mongo `$in`, LiteDB/IndexedDB in-memory) rather than being expanded into the filter text, so one call works the same everywhere.

```csharp
var statuses = new[] { "Open", "Pending", "Review" };

var open = await store.Query<Order>()
    .WhereIn(o => o.Status, statuses)
    .ToList();

var rest = await store.Query<Order>()
    .WhereNotIn(o => o.Status, statuses)
    .ToList();
```

`null` handling is explicit via the `NullHandling` argument (default `Ignore`):

- `Ignore` — strip `null`s from the set (the safe default; removes the classic `NOT IN (…, NULL)` "no rows" trap).
- `Match` — a `null` in the set is explicit intent about `null` *fields*: `WhereIn` also matches rows whose field is `null`; `WhereNotIn` also excludes them.
- `Raw` — pass the set through untouched and inherit the store's native three-valued logic.

```csharp
// "alice's rows, plus rows with no assignee"
var mine = await store.Query<Order>()
    .WhereIn(o => o.AssignedTo, new string?[] { "alice", null }, NullHandling.Match)
    .ToList();
```

An empty set is well-defined: `WhereIn` matches nothing, `WhereNotIn` matches everything. A `string` property-name overload (`WhereIn("Status", values)`) mirrors the string `OrderBy`/`Where` helpers. The same lowering powers the string filter's `field in (…)` form below.

#### Filter by string (runtime filter, AOT-safe)

When the filter itself is supplied at runtime (a REST `?filter=` parameter, a saved view, an admin search box), `Where(string, JsonTypeInfo<T>)` parses a small expression language into the same expression tree a compiled predicate would produce — so it runs through the existing translator and stays AOT/trim-safe (it never calls `Compile()` and resolves fields through `JsonTypeInfo`).

```csharp
var open = await store.Query<User>()
    .Where("Age >= 30 and Status == 'open'", ctx.User)
    .ToList();

// Combines with compiled predicates
var results = await store.Query<User>()
    .Where(u => u.Active)
    .Where(request.Filter, ctx.User)
    .ToList();
```

Supported syntax:

- Logical `and`, `or`, `not`, and parentheses.
- Comparisons `==` (or `=`), `!=` (or `<>`), `>`, `>=`, `<`, `<=`. Relational operators are rejected for `string`/`bool`/`Guid` fields.
- `field is null` / `field is not null` (and `field == null` / `field != null`).
- `field in (a, b, c)`.
- String functions `contains(field, 'x')`, `startsWith(field, 'x')`, `endsWith(field, 'x')`.

Field names follow the same rules as the string `OrderBy` (case-insensitive CLR or JSON name, dotted paths). String literals use single or double quotes; double the quote to escape (`'O''Brien'`). Literals are coerced to each field's CLR type. Syntax errors and unknown fields throw `ArgumentException`.

#### Runtime field projection (sparse fieldsets)

`Project(fields, JsonTypeInfo<T>)` selects a runtime-chosen set of fields and returns `IDocumentQuery<JsonObject>` — no DTO needed. This is the natural fit for REST sparse fieldsets (`?fields=name,email`) that are serialized straight back to JSON.

```csharp
IReadOnlyList<JsonObject> rows = await store.Query<User>()
    .Where("Age >= 30", ctx.User)
    .OrderBy("Name", ctx.User)
    .Project("Name, Email", ctx.User)
    .ToList();

var name = rows[0]["name"]!.GetValue<string>();

// Pagination, Count, Any and streaming all work on the projected query.
var page = await store.Query<User>().Project("name,email", ctx.User).PageResult(1, 20);
```

It emits a `json_object('name', json_extract(Data,'$.name'), …)` projection from the resolved JSON paths. Each output key is the **leaf JSON name** (so `ShippingAddress.City` → `city`); selecting two fields that resolve to the same leaf name throws. After `Project`, the query is terminal-shaped — `ToList`/`ToAsyncEnumerable`/`Count`/`Any`/`Paginate` are supported; further `Where`/`OrderBy`/`Select`/aggregates throw. `Project` is supported on the SQL providers; other providers throw `NotSupportedException`.

### Pagination

`Paginate(offset, take)` appends `LIMIT {take} OFFSET {offset}` to the generated SQL. It is a builder method that does not execute the query — it stores state until a terminal method is called.

```csharp
// First page (items 0-19)
var page1 = await store.Query<User>()
    .OrderBy(u => u.Name)
    .Paginate(0, 20)
    .ToList();

// Second page (items 20-39)
var page2 = await store.Query<User>()
    .OrderBy(u => u.Name)
    .Paginate(20, 20)
    .ToList();

// With filtering
var page = await store.Query<User>()
    .Where(u => u.Age >= 18)
    .OrderBy(u => u.Age)
    .Paginate(0, 10)
    .ToList();

// With projection
var page = await store.Query<User>()
    .OrderBy(u => u.Name)
    .Paginate(0, 10)
    .Select(u => new UserSummary { Name = u.Name, Email = u.Email })
    .ToList();

// With streaming
await foreach (var user in store.Query<User>()
    .OrderBy(u => u.Name)
    .Paginate(0, 50)
    .ToAsyncEnumerable())
{
    Console.WriteLine(user.Name);
}
```

#### `PageResult` — records + total in one call

For UI/REST responses you usually want both the page slice *and* the total matching count. `PageResult` is a terminal extension that runs the count and the page query and returns a `PagedResults<T>` envelope.

```csharp
public record PagedResults<T>(
    IEnumerable<T> Records,
    int TotalCount,
    int Page,
    int PageSize
);
```

```csharp
// 1-based by default — page 1 is the first page
var result = await store.Query<User>()
    .Where(u => u.Active)
    .OrderBy(u => u.Name)
    .PageResult(page: 1, pageSize: 20);

return new {
    items = result.Records,
    total = result.TotalCount,
    page = result.Page,
    pageSize = result.PageSize
};

// Zero-based opt-in (page 0 is the first page)
var result = await store.Query<User>()
    .OrderBy(u => u.Name)
    .PageResult(page: 0, pageSize: 20, zeroBased: true);
```

- `TotalCount` reflects the current `Where` predicates (and global query filters) — pagination state is ignored when counting, so the total spans every page, not just the returned slice.
- Any prior `.Paginate(...)` call on the query is overridden.
- `pageSize` must be greater than zero. `page` must be `>= 1` (or `>= 0` when `zeroBased: true`). Otherwise throws `ArgumentOutOfRangeException`.

### Projections

Project query results into a different shape using `.Select()`. Only the selected properties are extracted at the SQL level via `json_object` — no full document deserialization needed.

#### Flat projection

```csharp
var results = await store.Query<User>()
    .Where(u => u.Age == 25)
    .Select(u => new UserSummary { Name = u.Name, Email = u.Email })
    .ToList();
```

#### Nested source properties

```csharp
var results = await store.Query<Order>()
    .Where(o => o.Status == "Shipped")
    .Select(o => new OrderSummary { Customer = o.CustomerName, City = o.ShippingAddress.City })
    .ToList();
```

#### All documents with projection

```csharp
var results = await store.Query<Order>()
    .Select(o => new OrderDetail { Customer = o.CustomerName, LineCount = o.Lines.Count() })
    .ToList();
```

#### Collection methods in projections

Use `Count()`, `Count(predicate)`, `Any()`, and `Any(predicate)` inside projection selectors:

```csharp
// Count() — total number of elements
o => new OrderDetail { Customer = o.CustomerName, LineCount = o.Lines.Count() }
// SQL: json_array_length(Data, '$.lines')

// Count(predicate) — filtered count
o => new OrderDetail { Customer = o.CustomerName, GadgetCount = o.Lines.Count(l => l.ProductName == "Gadget") }
// SQL: (SELECT COUNT(*) FROM json_each(Data, '$.lines') WHERE json_extract(value, '$.productName') = @pp0)

// Any() — has any elements
o => new OrderDetail { Customer = o.CustomerName, HasLines = o.Lines.Any() }
// SQL: CASE WHEN json_array_length(Data, '$.lines') > 0 THEN json('true') ELSE json('false') END

// Any(predicate) — any element matches
o => new OrderDetail { Customer = o.CustomerName, HasPriority = o.Tags.Any(t => t == "priority") }
// SQL: CASE WHEN EXISTS (SELECT 1 FROM json_each(Data, '$.tags') WHERE value = @pp0) THEN json('true') ELSE json('false') END
```

Inner predicates support the same operators as WHERE clause expressions: comparisons, logical operators, null checks, string methods (`Contains`, `StartsWith`, `EndsWith`), and captured variables.

### Scalar aggregates

Compute Max, Min, Sum, Average across documents using terminal methods on the query builder.

```csharp
var maxAge = await store.Query<User>().Max(u => u.Age);
var minAge = await store.Query<User>().Min(u => u.Age);
var totalAge = await store.Query<User>().Sum(u => u.Age);
var avgAge = await store.Query<User>().Average(u => u.Age);

// With predicate filter
var maxAge = await store.Query<User>().Where(u => u.Age < 35).Max(u => u.Age);
```

### Aggregate projections (GROUP BY)

Use `Sql` marker class for aggregate projections with automatic GROUP BY.

```csharp
var results = await store.Query<Order>()
    .Select(o => new OrderStats
    {
        Status = o.Status,            // GROUP BY column
        OrderCount = Sql.Count(),     // COUNT(*)
    })
    .ToList();

// All Sql markers: Sql.Count(), Sql.Max(x.Prop), Sql.Min(x.Prop), Sql.Sum(x.Prop), Sql.Avg(x.Prop)

// With predicate filter
var results = await store.Query<Order>()
    .Where(o => o.Status == "Shipped")
    .Select(o => new OrderStats { Status = o.Status, OrderCount = Sql.Count() })
    .ToList();

// Explicit GroupBy
var results = await store.Query<Order>()
    .GroupBy(o => o.Status)
    .Select(o => new OrderStats { Status = o.Status, OrderCount = Sql.Count() })
    .ToList();
```

### Streaming queries

Use `.ToAsyncEnumerable()` instead of `.ToList()` to stream results one-at-a-time without buffering the entire result set into memory.

```csharp
// Stream all
await foreach (var user in store.Query<User>().ToAsyncEnumerable())
{
    Console.WriteLine(user.Name);
}

// Stream with filter and sort
await foreach (var user in store.Query<User>()
    .Where(u => u.Age > 30)
    .OrderBy(u => u.Name)
    .ToAsyncEnumerable())
{
    Console.WriteLine(user.Name);
}

// Stream with projection
await foreach (var summary in store.Query<Order>()
    .Where(o => o.Status == "Shipped")
    .Select(o => new OrderSummary { Customer = o.CustomerName, City = o.ShippingAddress.City })
    .ToAsyncEnumerable())
{
    Console.WriteLine($"{summary.Customer} in {summary.City}");
}

// Stream with pagination
await foreach (var user in store.Query<User>()
    .OrderBy(u => u.Name)
    .Paginate(0, 50)
    .ToAsyncEnumerable())
{
    Console.WriteLine(user.Name);
}
```

> **Note:** On shared-connection providers (SQLite, SQLCipher, DuckDB) streaming holds the per-store semaphore for the duration of enumeration — do not call other store methods inside the same `await foreach`, they will block until it completes. On pooled providers (PostgreSQL, MySQL, SQL Server, Oracle) the streaming reader holds one connection out of the driver pool and does not block other callers, but interleaving writes can still produce surprising results for consumers expecting a stable snapshot.

### Raw SQL queries

For advanced queries not covered by expressions, use raw SQL with provider-specific JSON functions. The SQL syntax varies by provider:

| Provider | JSON extract syntax |
|---|---|
| SQLite | `json_extract(Data, '$.name')` |
| MySQL | `JSON_EXTRACT(Data, '$.name')` |
| SQL Server | `JSON_VALUE(Data, '$.name')` |
| PostgreSQL | `"Data"::jsonb->>'name'` |
| Oracle | `JSON_VALUE(Data, '$.name')` |

```csharp
// SQLite example
var results = await store.Query<User>(
    "json_extract(Data, '$.name') = @name",
    parameters: new { name = "Alice" });

// With dictionary parameters (AOT-safe)
var parms = new Dictionary<string, object?> { ["name"] = "Alice" };
var results = await store.Query<User>(
    "json_extract(Data, '$.name') = @name",
    parameters: parms);

// Count with raw SQL
var count = await store.Count<User>(
    "json_extract(Data, '$.age') > @minAge",
    new { minAge = 30 });

// Streaming with raw SQL
await foreach (var user in store.QueryStream<User>(
    "json_extract(Data, '$.name') = @name",
    parameters: new { name = "Alice" }))
{
    Console.WriteLine(user.Name);
}
```

### Dynamic query building

The fluent query builder is composable — each `.Where()` call returns a new builder, so you can conditionally chain filters, sorting, and pagination at runtime:

```csharp
// Search parameters (from user input, API request, etc.)
string? nameFilter = "A";
int? minAge = null;
bool? isActive = true;
string sortBy = "name";
int page = 0, pageSize = 10;

var query = store.Query<User>();

if (!string.IsNullOrEmpty(nameFilter))
    query = query.Where(u => u.Name.StartsWith(nameFilter));

if (minAge.HasValue)
    query = query.Where(u => u.Age >= minAge.Value);

if (isActive.HasValue)
    query = query.Where(u => u.IsActive == isActive.Value);

query = sortBy switch
{
    "name" => query.OrderBy(u => u.Name),
    "age"  => query.OrderByDescending(u => u.Age),
    _      => query
};

var results = await query.Paginate(page * pageSize, pageSize).ToList();
var totalCount = await query.Count(); // same filters, no pagination
```

Multiple `.Where()` calls are AND'd together in the generated SQL.

## Transactions (IDocumentSession)

Grouping writes into one transaction is done through an `IDocumentSession` opened from the store (`await using var session = store.OpenSession();`) — there is no `RunInTransaction`. Queue `Add`/`AddRange`/`Update`/`Upsert`/`Remove`, then `SaveChanges` (commits on success, rolls back on exception). Contiguous same-type inserts coalesce into the batch-insert fast path. A session is a write buffer, not a change tracker — reads don't see operations buffered before `SaveChanges`; for read-modify-write atomicity use ETag/CAS + retry, or an explicit `await using var tx = await session.BeginTransaction();` with a `LockMode.Update` read (relational providers). Inject `IDocumentSession` (scoped, ASP.NET — `AddScopedDocumentSession()`) or `IDocumentSessionFactory` (no ambient scope — MAUI/desktop/background).

```csharp
await using var uow = store.OpenSession();
uow.Add(new User { Id = "u1", Name = "Alice", Age = 25 })
   .Add(new User { Id = "u2", Name = "Bob", Age = 30 });
await uow.SaveChanges();
```

## Write Interceptors

Register interceptors to observe/mutate writes; the after-hook runs inside the transaction with the generated id/version (e.g. for a transactional outbox). Per-document (`IDocumentInterceptor`) fires for Insert/BatchInsert(per item)/Update/Upsert/Remove; bulk (`IDocumentBulkInterceptor`) fires once for ExecuteUpdate/ExecuteDelete/Clear. Supported across every provider.

```csharp
opts.AddInterceptor(new AuditInterceptor());
opts.OnBeforeWrite<Order>((ctx, ct) => { /* mutate ctx.Document or throw to abort */ return Task.CompletedTask; });
opts.OnAfterWrite<Order>((ctx, ct) => outbox.Enqueue(ctx.Id, ctx.Operation, ct));
```

Interceptors can also be registered in DI to get constructor-injected dependencies — `AddDocumentStore` resolves every `IDocumentInterceptor`/`IDocumentBulkInterceptor` from the container and runs them after the options-registered ones. Register them as singletons. DI interceptors fire across **every** provider (including the non-relational ones and Orleans grain storage).

```csharp
services.AddSingleton<IDocumentInterceptor, OutboxInterceptor>(); // ctor deps injected
services.AddDocumentStore(opts => opts.DatabaseProvider = new SqliteDatabaseProvider("Data Source=app.db"));
```

**Scoped services + a transaction-bound session in a hook** — an interceptor resolves **request-scoped** services from `ctx.Services` (**no marker interface** — any DI-registered interceptor gets a scope, scoped registrations included; `ctx.Services` is never null and is resolved fresh from the flowing scope, so through a scoped `IDocumentSession`/`DocumentContext` it's the caller's own request scope, otherwise a fresh child scope per unit). A single write with per-doc interceptors runs as an implicit one-op unit of work, so `ctx.Session` (and `ctx.Store`) are bound to that transaction: read this unit's uncommitted rows and write side effects (an outbox row) that commit **atomically** with the triggering write, with no shared-connection deadlock. `int Order` sequences interceptors deterministically. Full read-your-writes visibility is relational + LiteDB; other backends are committed-state.

```csharp
public sealed class OrderInterceptor : IDocumentInterceptor          // no marker; may be registered scoped too
{
    public async Task BeforeWrite(DocumentWriteContext ctx, CancellationToken ct)
        => await ctx.Services.GetRequiredService<IOrderValidator>().Validate((Order)ctx.Document!, ct);

    public async Task AfterWrite(DocumentWriteContext ctx, CancellationToken ct)
    {
        using (ctx.Session.SuppressInterceptors())                    // atomic outbox, no re-entry
        {
            ctx.Session.Add(new OutboxEntry(ctx.Id!, "OrderPlaced")); // flushes into THIS transaction
            await ctx.Session.SaveChanges(ct);
        }
    }
}
```

## Change Monitoring

Stores that implement `IObservableDocumentStore` expose an `IAsyncEnumerable<DocumentChange<T>>` that you can `await foreach` over to react to local writes. Notifications are **in-process**: they fire for inserts, updates, removes and clears performed through this store instance. Changes made by other processes or other store instances are not observed — for that, use the native change feed (`IChangeFeedDocumentStore.SubscribeChanges<T>`).

Supported on `DocumentStore` (SQLite, SQLCipher, MySQL, SQL Server, PostgreSQL, Oracle) and `LiteDbDocumentStore`. Cosmos, MongoDB, IndexedDB and DuckDB do not implement it.

### Subscribing to all changes for a type

```csharp
using var cts = new CancellationTokenSource();

_ = Task.Run(async () =>
{
    await foreach (var change in store.NotifyOnChange<User>(cts.Token))
    {
        Console.WriteLine($"{change.ChangeType} {change.Id} {change.Document?.Name}");
    }
});

await store.Insert(new User { Id = "u1", Name = "Alice", Age = 25 });
await store.Update(new User { Id = "u1", Name = "Alice", Age = 26 });
await store.Remove<User>("u1");

cts.Cancel(); // stop the loop
```

### Per-document monitoring

`WhenDocumentChanged<T>(id)` filters the stream to a single document Id:

```csharp
var observable = (IObservableDocumentStore)store;
await foreach (var change in observable.WhenDocumentChanged<Order>("ord-1", ct))
{
    UpdateUi(change);
}
```

### Per-query monitoring

Every `IDocumentQuery<T>` exposes `.NotifyOnChange(ct)` — it filters the change stream by the query's `Where` predicates. `OrderBy`, `Paginate`, and `GroupBy` are ignored (they affect result shape, not membership).

```csharp
var pending = store.Query<Order>().Where(o => o.Status == "Pending");

await foreach (var change in pending.NotifyOnChange(ct))
{
    // Only fires when an Order whose Status == "Pending" is inserted or updated.
}
```

**Caveats for property-level paths:** `SetProperty`, `RemoveProperty`, `Remove`, and `Clear` do not materialize the full document, so `DocumentChange<T>.Document` is `null` for those events. The per-query filter passes them through unconditionally so your consumer can re-check membership by re-querying.

### DocumentChange shape

| Property | Description |
|---|---|
| `ChangeType` | `Inserted`, `Updated`, `Removed`, or `Cleared` |
| `Id` | The document Id (empty for `Cleared`) |
| `Document` | The document body. Populated for `Inserted` / `Updated` (full-document path); `null` for `Removed`, `Cleared`, and property-level updates |

### Transactions

Changes performed in a session are **buffered** and emitted only after `SaveChanges` commits. A rollback discards the buffered events:

```csharp
await using var uow = store.OpenSession();
uow.Add(new User { Id = "u1", Name = "Alice" })
   .Add(new User { Id = "u2", Name = "Bob" });
// Subscribers see nothing yet.
await uow.SaveChanges();
// Subscribers receive both Inserted events here, in order.
```

### Unsubscribing

Cancel the `CancellationToken` passed to `NotifyOnChange` (or break out of the `await foreach`). The subscription's channel is unregistered automatically when the iterator exits.

## Global Query Filters

Register a predicate that's automatically AND-applied to every query of `T` — the same shape as Entity Framework Core's `HasQueryFilter`. Use this for soft-delete, row-level security, or any "active only" scope that should be transparent to consumer code.

### Registering filters

```csharp
var store = new DocumentStore(new DocumentStoreOptions
{
    DatabaseProvider = new SqliteDatabaseProvider("Data Source=mydata.db")
}
.AddQueryFilter<User>(u => !u.IsDeleted)                        // unnamed
.AddQueryFilter<Order>("tenant", o => o.TenantId == ctx.Current) // named
.AddQueryFilter<Order>("status", o => o.Status != "Archived"));
```

Filters compose with `.Where(...)` and with each other — every registered filter is AND'd, then the user's `Where` predicates are AND'd after that. Captured variables (`ctx.Current`) are re-read on every translation, so per-request values work without rebuilding the store.

### What's filtered

| Path | Filtered? |
|---|---|
| `Query<T>()` + all terminals (`ToList`, `ToAsyncEnumerable`, `Count`, `Any`, `Max`/`Min`/`Sum`/`Average`, `ExecuteUpdate`, `ExecuteDelete`) | Yes |
| `query.NotifyOnChange()` | Yes — only changes whose document matches the filter are emitted |
| `Get<T>(id)` / `GetDiff<T>(id, ...)` | Yes — returns `null` if the stored doc fails the filter |
| `Update<T>` | Yes — throws "not found" if the stored doc fails the filter |
| `SetProperty<T>` / `RemoveProperty<T>` | Yes — returns `false` if the stored doc fails the filter |
| `Remove<T>(id)` | Yes — returns `false` (no-op) if the stored doc fails the filter |
| `Clear<T>()` | Yes — only matching docs are deleted |
| `Count<T>(rawSql)` | Yes |
| `Insert<T>` / `BatchInsert<T>` | **No** — inserts always succeed (matches EF Core) |
| `Upsert<T>` | **No** — Upsert bypasses filters; use `Get` + `Update` if you need filter enforcement |
| `Query<T>(rawSql)` / `QueryStream<T>(rawSql)` | **No** — raw SQL is yours (matches EF Core's `FromSqlRaw`) |

### Opting out per query

```csharp
// Disable every filter on this query
var allUsers = await store.Query<User>().IgnoreQueryFilters().ToList();

// Disable specific named filters (others still apply)
var anyTenant = await store.Query<Order>().IgnoreQueryFilters("tenant").ToList();

// Multiple names
var dump = await store.Query<Order>().IgnoreQueryFilters("tenant", "status").ToList();
```

`IgnoreQueryFilters` must be called **before** `Select(...)` — calling it on a projected query throws.

### Captured variables

Predicates re-translate on each query, so closures pick up the current value:

```csharp
var ctx = new TenantContext();
options.AddQueryFilter<Order>("tenant", o => o.TenantId == ctx.Current);

ctx.Current = "acme";
await store.Query<Order>().ToList(); // filters by acme

ctx.Current = "globex";
await store.Query<Order>().ToList(); // re-reads, filters by globex
```

### Caveats

- **Filters require a `JsonTypeInfo<T>`** for SQL-providers — they're translated through the same expression visitor as `Where`. Configure a `JsonSerializerContext` on `DocumentStoreOptions.JsonSerializerOptions` (or pass `JsonTypeInfo<T>` to the call sites that take it). Without one, a registered filter throws `InvalidOperationException` at first use.
- **Spatial sidecar tables** are touched by `Remove`/`Clear` — when a row fails the filter, the main delete is skipped but the spatial path's bulk operations may treat un-matched rows differently. If you mix soft-delete with spatial indexing, prefer `Update` (setting the deleted flag) over `Remove`.

## Native Change Feeds

Where the in-process broadcaster only sees this instance's own writes, `IChangeFeedDocumentStore.SubscribeChanges<T>` observes the underlying database itself — including writes from other processes, other store instances, and other connections. Backed by the database's native mechanism:

| Provider | Mechanism |
|---|---|
| **PostgreSQL** | `LISTEN` / `NOTIFY` with row-level triggers (true push) |
| **SQL Server** | Change Tracking, optionally with `SqlDependency` query notifications (`SqlServerChangeFeedOptions`) |
| **Cosmos DB** | Native Change Feed API |

Provisioning (triggers, enabling Change Tracking) is automatic and idempotent. SQLite, LiteDB, IndexedDB, MySQL, Oracle, and DuckDB throw `NotSupportedException`.

```csharp
await using var sub = await store.SubscribeChanges<User>(async (change, ct) =>
{
    Console.WriteLine($"{change.ChangeType} {change.Id}");
});

// Subscription runs until `sub` is disposed.
```

## Temporal History (System-Time Versioning)

Opt a document type into append-only history with `MapTemporal<T>`. Every mutation records a versioned snapshot to a per-type history sidecar, so you can read a document's state as of any point in time, audit changes, restore prior versions, and diff between versions. Opt-in per type — only mapped types pay the extra write.

```csharp
options.MapTemporal<Order>(o =>
{
    o.Retention    = TimeSpan.FromDays(90);   // prune expired versions older than this
    o.MaxVersions  = 50;                      // …or cap versions per document
    o.CaptureActor = () => currentUser.Id;    // optional "who" recorded per version
});
```

Tracked operations: `Insert`, `Update`, `Upsert`, `Remove`, `SetProperty`, `RemoveProperty`, and `BatchInsert`, including writes in a session (buffered and committed atomically). `Clear<T>` is a bulk delete and is **not** tracked.

Supported on **every** provider — the relational stores (SQLite, SQLCipher, PostgreSQL, SQL Server, MySQL, Oracle, DuckDB) and the document stores (LiteDB, MongoDB, CosmosDB, IndexedDB). Each persists versions to its own sidecar: a `{table}_history` table (relational), a `{collection}_history` collection (LiteDB, MongoDB), a `{container}_history` container (CosmosDB, partitioned by `/typeName`), or a `{store}_history` object store (IndexedDB).

#### Why the history methods aren't on `IDocumentStore`

History is an **optional capability**, not part of the universal CRUD contract — so it lives on its own interface, `ITemporalDocumentStore : IDocumentStore`, the same way observation lives on `IObservableDocumentStore` and the native change feed on `IChangeFeedDocumentStore`. Putting `History`/`AsOf`/`Restore`/… on `IDocumentStore` would force every consumer of a plain store to see seven methods that throw far more often than they work (they require the type to be `MapTemporal`-mapped), and force every backend to implement them. Asking for `ITemporalDocumentStore` instead makes "this store does history" a compile-time, discoverable fact. Resolve or cast to it:

```csharp
var store = serviceProvider.GetRequiredService<ITemporalDocumentStore>();
```

Calling a history method for a type that wasn't passed to `MapTemporal<T>` throws `InvalidOperationException`.

### Reading history

```csharp
// Every version of one document (oldest first)
IReadOnlyList<DocumentVersion<Order>> history = await store.History<Order>(orderId);

// State at a point in time (null if it didn't exist / was removed then)
Order? then = await store.AsOf<Order>(orderId, lastTuesday);

// Restore a prior version as the new current state (re-inserts if it had been removed)
Order? restored = await store.Restore<Order>(orderId, version: 7);

// RFC 6902 patch between two versions (temporal analogue of GetDiff)
JsonPatchDocument<Order>? patch = await store.GetDiffBetween<Order>(orderId, 3, 7);
```

### Fleet-wide queries

Backed by secondary indexes on the history table:

```csharp
// Point-in-time snapshot of every live document of the type
IReadOnlyList<Order> snapshot = await store.AsOfAll<Order>(endOfQuarter);

// Per-user audit trail (requires CaptureActor)
IReadOnlyList<DocumentVersion<Order>> byAlice = await store.ChangesByActor<Order>("alice@corp.com");

// Audit log over a time window (ValidFrom in [from, to))
IReadOnlyList<DocumentVersion<Order>> log = await store.ChangesBetween<Order>(weekStart, weekEnd);
```

### DocumentVersion&lt;T&gt;

| Property | Description |
|---|---|
| `Id` | The document's string Id. |
| `Version` | Monotonic version number, starting at 1. |
| `ValidFrom` / `ValidTo` | The interval this version was current. `ValidTo` is `null` for the version in effect now. |
| `Operation` | `Inserted`, `Updated`, or `Removed`. |
| `Actor` | The captured actor, when `CaptureActor` was configured. |
| `Document` | The state at this version. `null` for `Removed` tombstones. |

### Retention

Both prune on every write; the current version is never pruned. Set at least one on SQLite/mobile to keep the file bounded.

| Option | Behaviour |
|---|---|
| `Retention` (`TimeSpan?`) | Deletes closed versions whose `ValidTo` is older than `now - Retention`. |
| `MaxVersions` (`int?`) | Keeps only the newest N versions per document. |

On the relational providers the sidecar carries a `(Id, TypeName, Version)` primary key plus `(TypeName, ValidFrom, ValidTo)` and `(TypeName, Actor)` secondary indexes; the document stores model the same versions in their native sidecar collection/container/object store and compute the point-in-time selection in the provider. For merge/partial writes (`Upsert`/`SetProperty`/`RemoveProperty`) the resulting document is read back so history stores the true post-image — a cost incurred only for temporal-mapped types.

> **IndexedDB:** because temporal adds new object stores, an existing database must be opened at a higher `Version` so the schema upgrade creates them — bump `options.Version` when adding `MapTemporal` to an already-deployed store. A fresh database needs no change.

## Telemetry & Observability

The core `Shiny.DocumentDb` package emits OpenTelemetry-native metrics and distributed tracing for every store operation — **embedded and always-on**, on every provider and every construction path (DI or `new …DocumentStore(options)`). Built on the standard .NET primitives (`System.Diagnostics.Metrics.Meter` and `ActivitySource`), it plugs straight into OpenTelemetry, the .NET Aspire dashboard, Application Insights, or Prometheus/Grafana. It's **zero-cost when nobody is listening** — instruments no-op with no meter subscriber and spans aren't allocated with no `ActivityListener` — so there's nothing to opt into and no decorator to register.

**Structured `ILogger` logging** comes for free when a store is created from the container (any provider's `Add…DocumentStore` or `IServiceProvider` constructor) with an `ILoggerFactory` registered: every SQL / operation statement is logged at `Debug` under the `Shiny.DocumentDb` category (control it with `Logging:LogLevel:Shiny.DocumentDb`), flowing into Serilog / OTel logs / App Insights — the `options.Logging` string callback still fires alongside it. Works across the relational core and all six non-relational providers.

```csharp
services.AddDocumentStore(o => o.DatabaseProvider = new SqliteDatabaseProvider("Data Source=app.db"));

// Nothing to register on the store — just subscribe your OTel pipeline to its meter/source:
services.AddOpenTelemetry()
    .WithMetrics(m => m.AddMeter("Shiny.DocumentDb"))
    .WithTracing(t => t.AddSource("Shiny.DocumentDb"));
```

The `db.system.name` tag is derived from the store's backend, so it works across every provider with no per-provider config. A store registered with the keyed overload `AddDocumentStore("orders", …)` automatically tags its signals with `db.namespace = "orders"` so multiple stores are distinguishable. (The old `AddDocumentStoreInstrumentation()`/`InstrumentedDocumentStore`/`o.Instrumentation` decorator API was removed in 11.0 — instrumentation is embedded now.)

### What it emits

Instrument and tag names follow the OpenTelemetry database client semantic conventions:

| Instrument | Kind | Meaning |
|---|---|---|
| `db.client.operation.duration` | Histogram (`s`) | Per-operation latency — the primary signal. |
| `db.client.operations` | Counter | Operation count. |
| `db.client.response.returned_rows` | Histogram | Documents returned / affected. |

Tagged with `db.system.name` (`sqlite`, `postgresql`, `mongodb`, …), `db.operation.name` (`insert`, `get`, `query.to_list`, `history`, …), `db.collection.name` (the document type), `outcome` (`success`/`error`), `error.type` on failures, and `db.namespace` (the store name, on keyed/named stores only). Each operation also starts a `{system}.{operation}` `ActivityKind.Client` span carrying the same tags, with `Error` status + exception capture on failure.

### Coverage

CRUD, string + fluent-query terminals (`ToList`/`Count`/`Any`/`ExecuteDelete`/`ExecuteUpdate`/aggregates), spatial/vector, all `ITemporalDocumentStore` operations, and a session `SaveChanges` — where the queued operations become **child spans** of the transaction span. `NotifyOnChange`/`SubscribeChanges` are long-lived subscriptions and pass through untraced. Only metadata is recorded — never document bodies, ids, or parameter values.

`InstrumentedDocumentStore` is a faithful decorator (also surfaces `ITemporalDocumentStore`/`IObservableDocumentStore`/`IChangeFeedDocumentStore`); the wrapped store is reachable via its `Inner` property. Keyed registrations (the named `AddDocumentStore(name, …)` overload) are not auto-decorated — wrap those manually.

## Rekeying (SQLCipher only)

Change the encryption key of an existing SQLCipher database using the `RekeyAsync` extension method on `IDocumentStore`. This issues `PRAGMA rekey` under the hood. Throws `InvalidOperationException` if the store is not using `SqlCipherDatabaseProvider`.

```csharp
using Shiny.DocumentDb.Sqlite.SqlCipher;

await store.RekeyAsync("newPassword");
```

> **Important:** After rekeying, the store still holds the old password internally. Create a new store with the new password for subsequent operations.

## Backup (SQLite/SQLCipher/LiteDB only)

Creates a hot backup of the database to a file. Only available on concrete types — not on `IDocumentStore`. The store remains fully usable during the backup.

- **SQLite** (`SqliteDocumentStore`): Uses the SQLite Online Backup API
- **SQLCipher** (`SqlCipherDocumentStore`): Backup is automatically encrypted with the same password
- **LiteDB** (`LiteDbDocumentStore`): Requires a file-based connection string with a `Filename` parameter

```csharp
// SQLite
var sqliteStore = new SqliteDocumentStore("Data Source=mydata.db");
await sqliteStore.Backup("/path/to/backup.db");

// SQLCipher
var cipherStore = new SqlCipherDocumentStore("encrypted.db", "mySecretKey");
await cipherStore.Backup("/path/to/backup.db"); // encrypted with same password

// LiteDB
var liteStore = new LiteDbDocumentStore(new LiteDbDocumentStoreOptions { ConnectionString = "Filename=mydata.db" });
await liteStore.Backup("/path/to/backup.db");
```

## ClearAllAsync (SQLite only)

Deletes all documents across all tables in the SQLite database, including spatial sidecar tables. Only available on `SqliteDocumentStore`.

```csharp
var sqliteStore = new SqliteDocumentStore("Data Source=mydata.db");
await sqliteStore.ClearAllAsync();
```

## Spatial / Geo Queries

Spatial queries are supported on **SQLite** (R*Tree bbox), **PostgreSQL / MySQL / SQL Server / Oracle / DuckDB** (dependency-free envelope-sidecar bbox — no PostGIS/`geography`/`SDO`/`spatial` extension), all with an in-process relate/refine — plus **CosmosDB** (native GeoJSON `ST_INTERSECTS`/`ST_WITHIN`/`ST_DISTANCE`) and **MongoDB** (`2dsphere` + `$geoIntersects`/`$geoWithin`/`$near`). The fallback stores (LiteDB, IndexedDB, Azure Table, DynamoDB) throw `NotSupportedException`. Check support at runtime with `store.SupportsSpatial`.

Both **point** queries (`WithinRadius`/`WithinBoundingBox`/`NearestNeighbors` with `GeoPoint`) and **full OGC geometry** are supported. For the geometry model (`GeoLineString`/`GeoPolygon`/`GeoMultiPoint`/`GeoMultiLineString`/`GeoMultiPolygon`/`GeoGeometryCollection`), the `Geo`-prefixed predicate family (`GeoIntersects`, `GeoContainedBy`, `GeoContains`, `GeoDisjoint`, `GeoTouches`, `GeoCrosses`, `GeoOverlaps`, `GeoEquals`, `GeoCovers`, `GeoCoveredBy`, `GeoWithinDistance`), and measurement/validity accessors, see the [Spatial documentation](https://shinylib.net/documentdb/spatial/). Map a geometry with `MapSpatialProperty<T>(x => x.Area)` where `Area` is a `Geometry?`. Each predicate takes an optional `orderByDistanceFrom` and returns `SpatialResult<T>`.

**Spatial predicates in LINQ (`DocumentFunctions`, v11+):** compose a spatial predicate with the rest of a query (other `Where` clauses, `OrderBy`, `Count`, paging), all server-side — `store.Query<Zone>().Where(z => DocumentFunctions.Intersects(z.Area!, area) && z.Active).OrderBy(z => DocumentFunctions.Distance(z.Area!, origin))`. Lowers to each engine's native spatial function: SQLite (R\*Tree + `docdb_st_*` UDF), MySQL/DuckDB/PostgreSQL (`ST_*`; PostgreSQL needs PostGIS), SQL Server (native `geometry` + `.ST*`), Oracle (`SDO_GEOM`, needs Oracle Spatial), CosmosDB (`ST_INTERSECTS`/`ST_WITHIN`/`ST_DISTANCE`), MongoDB (`$geoIntersects`/`$geoWithin`). Cosmos/Mongo expose the intersect/within/distance subset in a `Where`; the finer predicates throw there — the dedicated `store.Geo*` methods support every predicate on every spatial-capable provider. `PortableSpatial = true` on a relational provider forces the dependency-free envelope tier.

### Spatial types

```csharp
// Geographic point (WGS84), serializes as GeoJSON
public readonly record struct GeoPoint(double Latitude, double Longitude);

// Bounding box for area queries
public readonly record struct GeoBoundingBox(
    double MinLatitude, double MinLongitude,
    double MaxLatitude, double MaxLongitude);

// Query result with computed distance
public class SpatialResult<T> where T : class
{
    public required T Document { get; init; }
    public double DistanceMeters { get; init; }
}
```

### Configuration

Register which `GeoPoint` property to use for spatial indexing per document type:

```csharp
public class Restaurant
{
    public string Id { get; set; } = "";
    public string Name { get; set; } = "";
    public GeoPoint Location { get; set; }
    public string Cuisine { get; set; } = "";
}

var store = new DocumentStore(new DocumentStoreOptions
{
    DatabaseProvider = new SqliteDatabaseProvider("Data Source=mydata.db")
}
.MapSpatialProperty<Restaurant>(r => r.Location)
);
```

For full AOT safety, use the delegate overload:

```csharp
.MapSpatialProperty<Restaurant>("Location", r => r.Location)
```

The mapped property may be a nullable `GeoPoint?` for records with optional coordinates. A document whose
location is `null` is skipped by the spatial index — it does not throw on insert/update and is never
returned by spatial queries; setting a previously-populated location back to `null` on update purges its
stale index entry.

### Spatial queries

```csharp
// Find documents within a radius (meters), ordered by distance ascending
var nearby = await store.WithinRadius<Restaurant>(
    new GeoPoint(45.5231, -122.6765), // Portland, OR
    5000, // 5km
    filter: r => r.Cuisine == "Italian");

foreach (var result in nearby)
    Console.WriteLine($"{result.Document.Name} — {result.DistanceMeters:N0}m away");

// Find documents within a bounding box
var inArea = await store.WithinBoundingBox<Restaurant>(
    new GeoBoundingBox(45.0, -123.0, 46.0, -122.0));

// Find K nearest neighbors, ordered by distance
var closest = await store.NearestNeighbors<Restaurant>(
    new GeoPoint(45.5231, -122.6765),
    count: 10,
    filter: r => r.Cuisine == "Italian");
```

### How it works

- **SQLite**: Creates R*Tree sidecar tables that are automatically synced on every insert/update/upsert/remove/clear. Uses bounding box pre-filter via R*Tree, then Haversine post-filter for exact radius.
- **CosmosDB**: `GeoPoint` serializes as GeoJSON. Spatial index policies are added to the container automatically. Queries use native `ST_DISTANCE` and `ST_WITHIN` functions.

## Vector / ANN Search

Map a `ReadOnlyMemory<float>` embedding property and query by similarity:

```csharp
public class Document
{
    public Guid Id { get; set; }
    public string Content { get; set; } = "";
    public ReadOnlyMemory<float> Embedding { get; set; }
}

var store = new DocumentStore(new DocumentStoreOptions
{
    DatabaseProvider = new SqliteDatabaseProvider("Data Source=mydata.db")
    {
        EnableVectorExtension = true   // load sqlite-vec on every connection
    }
}.MapVectorProperty<Document>(
    d => d.Embedding,
    dimensions: 1536,
    metric: VectorDistance.Cosine,
    indexKind: VectorIndexKind.Hnsw));

// Top-10 nearest to a query embedding
var hits = await store.Query<Document>()
    .Where(d => d.Content.Contains("invoice"))   // pre-filter where supported
    .NearestVectors(queryEmbedding, k: 10);

foreach (var hit in hits)
    Console.WriteLine($"{hit.Score:F4}  {hit.Document.Content}");
```

### Provider matrix

| Provider | Storage | Index kinds | Filter strategy |
|---|---|---|---|
| **PostgreSQL** | `pgvector` sidecar table | HNSW, IVF, None | Pre-filter via JOIN |
| **SQL Server 2025** | Native `VECTOR(n)` sidecar | DiskANN, None | Pre-filter via JOIN |
| **Oracle 23ai** | Native `VECTOR(n, FLOAT32)` sidecar | HNSW, IVF, None | Pre-filter via JOIN |
| **CosmosDB** | Embedded in document JSON | DiskANN, QuantizedFlat, Flat | `WHERE` + `ORDER BY VectorDistance(...)` |
| **MongoDB** (Atlas) | `$vectorSearch` aggregation | HNSW (Atlas-managed) | Filter clause inside `$vectorSearch` |
| **DuckDB** | `vss` sidecar table | HNSW, None | Pre-filter via JOIN |
| **SQLite** | `sqlite-vec` virtual table | None (flat scan) | Post-filter join back |
| **MySQL** / **LiteDB** / **IndexedDB** | — | — | Throws `NotSupportedException` |

> **Oracle note:** `VECTOR_DISTANCE` (exact search) works out of the box. Creating an HNSW/IVF vector index additionally requires the database's vector pool — set `vector_memory_size` (`ALTER SYSTEM SET vector_memory_size = 1G SCOPE=SPFILE;` then restart). If the pool isn't configured, index creation is silently skipped and queries fall back to an exact sequential scan (still correct, just unindexed).

> **SQLite on iOS/Android — use `Shiny.DocumentDb.Sqlite.VectorSupport`:** `EnableVectorExtension` calls `sqlite3_load_extension`, which **cannot work on iOS** — Apple forbids `dlopen` of loose libraries and the bundled `e_sqlite3` disables runtime extension loading (Android usually fails too). The **`Shiny.DocumentDb.Sqlite.VectorSupport`** package ships the `sqlite-vec` native binaries (iOS `xcframework`, Android `.so`, desktop loadables) and a one-call helper — `opts.DatabaseProvider = SqliteVec.CreateProvider(connStr)` — that registers `vec0` as an auto-extension and sets `VectorExtensionPreloaded`. To wire it by hand instead, statically link `sqlite-vec`, register it once with `sqlite3_auto_extension(sqlite3_vec_init)`, and set `VectorExtensionPreloaded = true` (the provider then uses `vec0` without calling `LoadExtension`; if both flags are set, preloaded wins).

### Score semantics

| Metric | Surfaced as | Direction |
|---|---|---|
| Cosine | Distance in [0, 2] | Lower = closer |
| Euclidean (L2) | Distance | Lower = closer |
| DotProduct | Raw inner product | Higher = closer (negated internally where needed so `ORDER BY score ASC` works) |
| Hamming | Bit count | Lower = closer (PostgreSQL only) |

### Auto-embed on insert

Wire a `Microsoft.Extensions.AI.IEmbeddingGenerator` so the vector is populated automatically when the source text is set:

```csharp
using Shiny.DocumentDb.Extensions.AI;

opts.MapVectorProperty<Document>(d => d.Embedding, dimensions: 1536)
    .AutoEmbedOnInsert<Document>(
        embeddingGenerator,
        sourceSelector: d => d.Content,
        targetSetter: (d, vec) => d.Embedding = vec,
        targetGetter: d => d.Embedding);   // optional: skip when already set

// User writes the text only; the embedding lands in the document on Insert/Upsert/BatchInsert.
await store.Insert(new Document { Content = "hello world" });
```

### Tuning knobs

`VectorIndexOptions` exposes the common ANN parameters and a `ProviderHints` dictionary for the long tail:

```csharp
opts.MapVectorProperty<Document>(
    d => d.Embedding,
    dimensions: 1536,
    metric: VectorDistance.Cosine,
    indexKind: VectorIndexKind.Hnsw,
    configureIndex: i =>
    {
        i.HnswM = 16;
        i.HnswEfConstruction = 64;
        i.HnswEfSearch = 40;
        i.ProviderHints["sqlite.postFilterMultiplier"] = 4;
        i.ProviderHints["atlas.indexName"] = "my-vec-index";
    });
```

Recognized hints:

- `sqlite.postFilterMultiplier` *(int)*: candidate count multiplier when a `Where` post-filter is applied.
- `atlas.indexName` *(string)*: Atlas Vector Search index name (default `vector_index_{type}`).
- `atlas.numCandidates` *(int)*: Atlas `numCandidates` (default `10 * k`).

Full design document: [`docs/vector-support.md`](docs/vector-support.md).

## Index Management

For frequently queried JSON properties, you can create expression indexes to speed up lookups. These methods are on the concrete `DocumentStore` (not on `IDocumentStore`) since index management is DDL, not document CRUD. Each provider generates the appropriate index DDL for its database engine.

### Create an index on a property

```csharp
await store.CreateIndexAsync<User>(u => u.Name, ctx.User);
```

This generates a partial index scoped to the document type:

```sql
CREATE INDEX IF NOT EXISTS idx_json_User_name
ON documents (json_extract(Data, '$.name'))
WHERE TypeName = 'User';
```

### Nested properties

```csharp
await store.CreateIndexAsync<Order>(o => o.ShippingAddress.City, ctx.Order);
```

### Drop a specific index

```csharp
await store.DropIndexAsync<User>(u => u.Name, ctx.User);
```

### Drop all JSON indexes for a type

Removes all `idx_json_` indexes for the given type while preserving built-in indexes and indexes on other types.

```csharp
await store.DropAllIndexesAsync<User>();
```

Index names are deterministic (`idx_json_{typeName}_{jsonPath}` with dots replaced by underscores), so `CreateIndexAsync` and `DropIndexAsync` always agree on the name for a given expression. `CreateIndexAsync` uses `IF NOT EXISTS`, so calling it multiple times is safe.

## Supported Expression Reference

The following LINQ expressions are supported across all providers. SQL output shown uses SQLite syntax; other providers generate equivalent SQL using their native JSON functions.

| Expression | SQL Output (SQLite) |
|---|---|
| `u.Name == "Alice"` | `json_extract(Data, '$.name') = @p0` |
| `u.Age > 25` | `json_extract(Data, '$.age') > @p0` |
| `u.Age == 25 && u.Name == "Alice"` | `(... AND ...)` |
| `u.Name == "A" \|\| u.Name == "B"` | `(... OR ...)` |
| `!(u.Name == "Alice")` | `NOT (...)` |
| `u.Email == null` | `... IS NULL` |
| `u.Email != null` | `... IS NOT NULL` |
| `u.Name.Contains("li")` | `... LIKE '%' \|\| @p0 \|\| '%'` |
| `u.Name.StartsWith("Al")` | `... LIKE @p0 \|\| '%'` |
| `u.Name.EndsWith("ob")` | `... LIKE '%' \|\| @p0` |
| `o.ShippingAddress.City == "X"` | `json_extract(Data, '$.shippingAddress.city') = @p0` |
| `o.Lines.Any(l => l.Name == "X")` | `EXISTS (SELECT 1 FROM json_each(...) WHERE ...)` |
| `o.Tags.Any(t => t == "priority")` | `EXISTS (SELECT 1 FROM json_each(...) WHERE value = @p0)` |
| `o.Tags.Any()` | `json_array_length(Data, '$.tags') > 0` |
| `o.Lines.Count() > 1` | `json_array_length(Data, '$.lines') > 1` |
| `o.Lines.Count(l => l.Qty > 2)` | `(SELECT COUNT(*) FROM json_each(...) WHERE ...) > 2` |
| `e.StartDate > cutoff` | `json_extract(Data, '$.startDate') > @p0` (ISO 8601 formatted) |
| `e.CreatedAt >= start` | `json_extract(Data, '$.createdAt') > @p0` (DateTimeOffset supported) |
| Captured variables | Extracted from closure at translate time |

### Projection expressions

| Expression | SQL Output |
|---|---|
| `x => new R { A = x.Name }` | `json_object('name', json_extract(Data, '$.name'))` |
| `x => new R { C = x.Nav.Prop }` | `json_object('c', json_extract(Data, '$.nav.prop'))` |
| `x => new R { N = x.Lines.Count() }` | `json_array_length(Data, '$.lines')` |
| `x => new R { N = x.Lines.Count(l => ...) }` | `(SELECT COUNT(*) FROM json_each(Data, '$.lines') WHERE ...)` |
| `x => new R { B = x.Tags.Any() }` | `CASE WHEN json_array_length(...) > 0 THEN json('true') ELSE json('false') END` |
| `x => new R { B = x.Tags.Any(t => ...) }` | `CASE WHEN EXISTS (SELECT 1 FROM json_each(...) WHERE ...) THEN json('true') ELSE json('false') END` |
