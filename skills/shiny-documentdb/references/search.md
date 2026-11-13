# Spatial, vector, and full-text search

Read only the sections relevant to the current task. These are application-facing API details.

## Contents

- Spatial / Geo Queries
- Vector / Similarity Search
- Full-Text Search

## Spatial / Geo Queries

Spatial queries are supported on **SQLite, PostgreSQL, MySQL, SQL Server, Oracle, DuckDB, CosmosDB, and MongoDB**. The fallback stores (LiteDB, IndexedDB, Azure Table, DynamoDB) throw `NotSupportedException`. Both **point** queries and **full OGC geometry** are supported.

### Full geometry (v11+)

Map a `Geometry?` property (not just `GeoPoint`) and query with the `Geo`-prefixed predicate family. The geometry model — `GeoLineString`, `GeoPolygon` (exterior ring + optional holes), `GeoMultiPoint`, `GeoMultiLineString`, `GeoMultiPolygon`, `GeoGeometryCollection` — serializes as GeoJSON; `GeoPoint` implicitly converts to a point geometry so you can pass a bare point.

```csharp
public class Zone { public string Id { get; set; } = ""; public Geometry? Area { get; set; } }

options.ConfigureDocument<Zone>(cfg => cfg.MapSpatialProperty(z => z.Area));   // or ("Area", z => z.Area) for AOT

// stored-geometry <predicate> query-geometry; optional orderByDistanceFrom + filter; returns SpatialResult<T>
var containing = await store.GeoIntersects<Zone>(new GeoPoint(45.5, -122.6));   // "which zones contain this point?"
var inside     = await store.GeoContainedBy<Zone>(searchPolygon, orderByDistanceFrom: origin);
var near       = await store.GeoWithinDistance<Zone>(routeLine, meters: 500);
```

Predicate methods: `GeoIntersects`, `GeoContainedBy`, `GeoContains`, `GeoDisjoint`, `GeoTouches`, `GeoCrosses`, `GeoOverlaps`, `GeoEquals`, `GeoCovers`, `GeoCoveredBy`, `GeoWithinDistance(geometry, meters)`. Each takes `(Geometry, Geometry? orderByDistanceFrom = null, Expression<Func<T,bool>>? filter = null)` and returns `IReadOnlyList<SpatialResult<T>>` (`DistanceMeters` populated when `orderByDistanceFrom` is given). `NearestNeighbors` works over geometry-mapped types too.

- **Measurement/validity:** `Geometry` exposes **in-memory** `Area` (m²), `Length`/`Perimeter`, `Centroid`, `NumPoints`, `NumGeometries`, `IsValid`, `IsSimple`, `MakeValid()`. These are C# accessors — they do **not** translate to SQL and do **not** compose with `cfg.MapComputedProperty` (computed properties are lowered to SQL). To filter/sort by a measurement server-side, compute the scalar in your app, store it as a normal property, and query that field. Use `MakeValid()` as a pre-insert guard so native Mongo/Cosmos indexes don't reject a shape.
- **LINQ composition (`DocumentFunctions`, v11+):** to compose a spatial predicate with other `Where` clauses / `OrderBy` / `Count` / paging server-side, use `DocumentFunctions` inside `Query<T>().Where(...)`:
  ```csharp
  store.Query<Zone>()
       .Where(z => DocumentFunctions.Intersects(z.Area!, area) && z.Active)
       .OrderBy(z => DocumentFunctions.Distance(z.Area!, origin));
  ```
  Family: `Intersects`/`Disjoint`/`Contains`/`Within`/`Covers`/`CoveredBy`/`Touches`/`Crosses`/`Overlaps`/`GeoEquals`/`WithinDistance` (in `Where`) + `Distance` (in `OrderBy`). Read as `field <predicate> query`. Lowers to native per provider: **SQLite** (R\*Tree + `docdb_st_*` UDF — all predicates), **MySQL/PostgreSQL** (native `ST_*`; PostgreSQL needs PostGIS — all predicates), **DuckDB** (native `ST_*`, auto-loads `spatial` — all except `WithinDistance`), **SQL Server** (native planar `geometry` column + `.ST*` — all except `Covers`/`CoveredBy` and `WithinDistance`), **Oracle** (native `SDO_GEOMETRY` column + MDSYS spatial index + `SDO_RELATE` operators, needs Oracle Spatial — all except `Crosses`), **CosmosDB** (`ST_INTERSECTS`/`ST_WITHIN`/`ST_DISTANCE` — intersects/within/disjoint/withindistance), **MongoDB** (`$geoIntersects`/`$geoWithin` — intersects/within/point-withindistance). `WithinDistance` in a `Where` needs a geodesic distance function, which SQL Server (planar `geometry`) and DuckDB (no polygon geodesic) lack — they **throw** rather than approximate wrongly; use `store.GeoWithinDistance(...)` (exact Haversine, every provider). `Distance`-in-`OrderBy` is native on SQLite/PostgreSQL/MySQL/DuckDB/SQL Server (SQL Server sorts by planar `STDistance` over the indexed column); on MongoDB use `store.NearestNeighbors`/`orderByDistanceFrom`. Where a predicate isn't native, the `DocumentFunctions` call in a `Where` **throws** — use the dedicated `store.Geo*` method (all predicates, every spatial provider). `PortableSpatial = true` on a relational provider — or `settings.PortableSpatial` via Aspire's `AddDocumentStore` — forces the dependency-free envelope tier. The native tier provisions an extension on **PostgreSQL** (`CREATE EXTENSION postgis`; the stock `postgres` image lacks it — use `postgis/postgis`) and **DuckDB** (`spatial`); when that fails the store throws `DocumentConfigurationException` naming the remedy, so recommend `PortableSpatial` when the user can't install the extension.
  **String-expression parity:** the same geo functions work in the string surface — `Where("…")`, interpolated `Where($"…")`, `OrderBy("…")`, `Project("…")` — using the same names (`intersects`/`within`/`withindistance`/`distance`/…). Supply the query geometry as an **interpolated `{value}`** (a `Geometry`/`GeoPoint`, bound as a parameter — only where the string carries args, i.e. `Where($"…")`) or an inline **GeoJSON string literal** (works everywhere incl. `OrderBy`/`Project`). E.g. `store.Query<Zone>().Where($"intersects(area, {poly}) and active == true")`, `.OrderBy("distance(area, '<geojson>')")`. `contains(field, …)` is geo when `field` is a `Geometry` property, else the string `Contains`.
- **Only the mapped geometry is queryable (v13+).** Spatial APIs, `DocumentFunctions`, and string filters must name the property configured with `cfg.MapSpatialProperty`. Other geometry properties round-trip as ordinary data but cannot be spatially queried. For several pieces of one shape, use `GeoMultiPolygon` or `GeoGeometryCollection`.
- `GeoDisjoint` can scan the entire type; use it sparingly on large datasets.
- Distance approximations and near-tie ordering can differ across providers.

### Spatial Types

```csharp
// Geographic point (WGS84), serializes as GeoJSON
[JsonConverter(typeof(GeoPointJsonConverter))]
public readonly record struct GeoPoint(double Latitude, double Longitude);

// Bounding box for area queries
public readonly record struct GeoBoundingBox(
    double MinLatitude, double MinLongitude,
    double MaxLatitude, double MaxLongitude);

// Query result with distance
public class SpatialResult<T> where T : class
{
    public required T Document { get; init; }
    public double DistanceMeters { get; init; }
}
```

### Configuration

Register which `GeoPoint` property to use for spatial indexing:

```csharp
public class Restaurant
{
    public string Id { get; set; } = "";
    public string Name { get; set; } = "";
    public GeoPoint Location { get; set; }
    public string Cuisine { get; set; } = "";
}

var options = new DocumentStoreOptions
{
    DatabaseProvider = new SqliteDatabaseProvider("Data Source=mydata.db")
};
options.ConfigureDocument<Restaurant>(cfg => cfg.MapSpatialProperty(r => r.Location));

// ...or the AOT-safe overload
options.ConfigureDocument<Restaurant>(cfg => cfg.MapSpatialProperty("Location", r => r.Location));

var store = new DocumentStore(options);
```

The mapped property may be nullable (`GeoPoint?`). A document whose location is `null` is skipped by the
spatial index — it does not throw on insert/update and never appears in spatial query results; setting a
previously-populated location back to `null` on update purges its stale index entry. Use this for optional
coordinates (e.g. an event that may not have a place):

```csharp
public class CalendarEvent
{
    public string Id { get; set; } = "";
    public string Title { get; set; } = "";
    public GeoPoint? Location { get; set; }   // optional — null docs are simply not indexed
}

options.ConfigureDocument<CalendarEvent>(cfg => cfg.MapSpatialProperty(e => e.Location));
```

### Querying

```csharp
// Check if provider supports spatial
if (store.SupportsSpatial) { ... }

// Find within radius (meters), ordered by distance
var nearby = await store.WithinRadius<Restaurant>(
    new GeoPoint(45.5231, -122.6765), // Portland, OR
    5000, // 5km radius
    filter: r => r.Cuisine == "Italian");

foreach (var result in nearby)
    Console.WriteLine($"{result.Document.Name} — {result.DistanceMeters:N0}m away");

// Find within bounding box
var inArea = await store.WithinBoundingBox<Restaurant>(
    new GeoBoundingBox(45.0, -123.0, 46.0, -122.0));

// Find K nearest neighbors, ordered by distance
var closest = await store.NearestNeighbors<Restaurant>(
    new GeoPoint(45.5231, -122.6765),
    count: 10,
    filter: r => r.Cuisine == "Italian");
```

Spatial indexes are maintained automatically for normal document writes and deletes; no manual index synchronization is needed.

## Vector / Similarity Search

Embedding-similarity search via `store.NearestVectors<T>(query, k)` — also on `IDocumentSession` (`session.NearestVectors<T>(...)`), where inside `BeginTransaction` it reads the transaction's consistent snapshot. Capability is a store property: check `session.Store.SupportsVector` (not duplicated on the session). Supported on PostgreSQL (`pgvector`), SQL Server 2025, Oracle 23ai, CosmosDB (DiskANN), MongoDB (Atlas `$vectorSearch`), DuckDB (`vss`), and **SQLite** (`sqlite-vec`). LiteDB, IndexedDB, and MySQL throw `NotSupportedException`.

```csharp
options.ConfigureDocument<Doc>(cfg => cfg.MapVectorProperty(d => d.Embedding, dimensions: 1536, metric: VectorDistance.Cosine));
var hits = await store.NearestVectors<Doc>(queryEmbedding, k: 5);
```

Use DocumentDB write APIs when changing embeddings. Direct database writes can leave vector search out of sync; the **Vectors** tab in `ShinyDocDbMyAdmin` can diagnose drift and rebuild the index.

**Deletion and existing embeddings:** normal query, batch, and session deletion APIs maintain vector indexes automatically. Do not add index-table SQL or delete valid documents to repair an index. Enabling vector support later does not backfill existing embeddings. If vector mapping reports a generated-name collision, assign distinct document tables to the affected types.

`VectorResult<T>.Score` semantics are **provider-specific by design** (no lossless canonical scale): for Cosine/Euclidean the relational providers return a *distance* (lower = closer) while MongoDB/CosmosDB return a normalized *similarity* (higher = closer). Results are always ordered **nearest-first regardless of provider**, so rely on the ordering — not the raw `Score` value — for portable ranking, and don't compare scores or apply a fixed threshold across providers.

### Auto-embed on insert (`Shiny.DocumentDb.Extensions.AI`)

Populate the vector automatically from a text property. `AutoEmbedOnInsert<T>` is a **write interceptor**, so it runs on **every** provider (relational and document-native — Cosmos/Mongo/Redis/…), inside the write's transaction, on `Insert`/`BatchInsert`/`Upsert`. Two overloads:

```csharp
using Shiny.DocumentDb.Extensions.AI;

// DI overload (recommended): resolves IEmbeddingGenerator per-write from the caller's scope (ctx.Services),
// so a scoped session picks the caller's own generator. Register the generator in DI.
services.AddSingleton<IEmbeddingGenerator<string, Embedding<float>>>(/* ... */);
services.AddDocumentStore(o =>
    o.ConfigureDocument<Doc>(cfg => cfg.MapVectorProperty(d => d.Embedding, dimensions: 1536));
     .AutoEmbedOnInsert<Doc>(
         sourceSelector: d => d.Content,
         targetSetter:   (d, v) => d.Embedding = v,
         targetGetter:   d => d.Embedding));   // optional: skip when already set

// Explicit-generator overload: a fixed instance, for the container-free `new DocumentStore(options)` path.
opts.ConfigureDocument<Doc>(cfg => cfg.MapVectorProperty(d => d.Embedding, dimensions: 1536));
    .AutoEmbedOnInsert<Doc>(generator, d => d.Content, (d, v) => d.Embedding = v, d => d.Embedding);
```

Skips when the source text is null/empty or the target vector is already set. With the DI overload, a missing generator throws `InvalidOperationException` (register one, or use the explicit overload). There is **no** `OnBeforeInsert` hook anymore — for non-embedding "compute a derived field" needs use `OnBeforeWrite<T>` (an `IDocumentInterceptor` lambda over `ctx.Document`).

### `Microsoft.Extensions.VectorData` connector (`Shiny.DocumentDb.Extensions.VectorData`)

Use this when the surrounding code expects **MEVD's** `VectorStore` / `VectorStoreCollection<TKey, TRecord>` (MEAI, the Microsoft Agent Framework, Semantic Kernel) rather than `IDocumentStore`. For DocumentDb-native code, keep using `NearestVectors` above — the connector is interop, not the default.

```csharp
using Microsoft.Extensions.VectorData;
using Shiny.DocumentDb.Extensions.VectorData;

public class Note
{
    [VectorStoreKey] public string Id { get; set; } = "";          // MUST also be the document id
    [VectorStoreData(IsIndexed = true)] public string Tag { get; set; } = "";
    [VectorStoreVector(1536, DistanceFunction = DistanceFunction.CosineDistance)]
    public ReadOnlyMemory<float> Embedding { get; set; }
}

services.AddDocumentDbVectorStore(o =>
{
    o.DatabaseProvider = new SqliteDatabaseProvider("Data Source=app.db") { EnableVectorExtension = true };
    o.MapVectorRecord<Note>();     // reads the MEVD attributes → MapVectorProperty<Note>
});

var notes = sp.GetRequiredService<VectorStore>().GetCollection<string, Note>("Note");
await notes.UpsertAsync(note);
await foreach (var hit in notes.SearchAsync(queryEmbedding, top: 5,
    new VectorSearchOptions<Note> { Filter = n => n.Tag == "release" })) { }
```

Rules that decide whether generated code works:

- **`MapVectorRecord<T>()` must be called while the options are being configured** — DocumentDb fixes vector mappings when the store is constructed. `GetCollection<TKey, TRecord>(name)` only builds a facade and throws for an unmapped record. A `VectorStoreCollectionDefinition` therefore goes to `MapVectorRecord<T>(name, definition)`, **not** to `GetCollection`.
- **The collection name must match the registration** — `MapVectorRecord<Note>()` defaults to `nameof(Note)`; pass a name to both, or to neither.
- **`[VectorStoreKey]` must be the document's id** (`Id` by convention, otherwise `MapIdProperty<T>`), and be `string`/`Guid`/`int`/`long`. `TKey` on the collection must match that CLR type.
- **One `[VectorStoreVector]` per record.** Multi-vector records throw at mapping time, and `VectorSearchOptions.VectorProperty` may only select the mapped one.
- **Never emit `StorageName`** on `[VectorStoreKey]`/`[VectorStoreData]`/`[VectorStoreVector]` — it throws. DocumentDb's stored name is System.Text.Json's; use `[JsonPropertyName("...")]`.
- **Filters pass through untouched** — MEVD's `Expression<Func<T, bool>>` is the exact shape `NearestVectors` takes, so push-down behaves as documented per provider.
- **Text queries** (`SearchAsync("some text", …)`) need an `IEmbeddingGenerator<string, Embedding<float>>` from the definition, the `DocumentDbVectorStore` ctor, or DI; otherwise pass a `ReadOnlyMemory<float>` / `float[]` / `Embedding<float>`. For embedding on **write**, that is still `AutoEmbedOnInsert` above — the two packages compose and neither depends on the other.
- **`AddDocumentDbVectorStore(configure)`** registers one store behind both `IDocumentStore` and `VectorStore` (relational/SQLite options). For a provider with its own options class (Mongo/Cosmos/Redis), register the store yourself and call the no-argument `AddDocumentDbVectorStore()`.
- **Vector-capable providers only** — the constructor throws `NotSupportedException` on LiteDB/IndexedDB/MySQL/Azure Table/DynamoDB/Firestore/RavenDB.
- **Not supported**: `GetDynamicCollection` (`Dictionary<string, object?>` records) and hybrid keyword+vector search.
- `UpsertAsync` is a full replace (not merge-patch); `IncludeVectors` defaults to `false` so returned records have a cleared embedding unless asked; `ScoreThreshold` is a floor (`score >= threshold`), which only reads as "more relevant" on providers whose score is a similarity.

`cfg.MapVectorProperty` also exists on **`IDocumentStoreOptions`** now (AOT-safe getter/setter form, nullable `indexKind` meaning "provider default"), which is what lets `MapVectorRecord<T>` work on every backend from one extension method. Prefer the concrete options class's strongly-typed overload in ordinary code.

### SQLite — loading `sqlite-vec`

The SQLite provider needs the `sqlite-vec` native binary. **Recommended: add the `Shiny.DocumentDb.Sqlite.VectorSupport` package** — it ships the binaries for iOS (static `xcframework`), Android (`.so` per ABI), and desktop/Mac Catalyst, plus a one-call registration helper. This is the default answer for any iOS/Android/MAUI vector question.

```csharp
using Shiny.DocumentDb.Sqlite.VectorSupport;

// Call once at startup, before opening a connection. Works on iOS, Android, and desktop alike.
opts.DatabaseProvider = SqliteVec.CreateProvider($"Data Source={dbPath}");
// (or call SqliteVec.RegisterAutoExtension() yourself, then set VectorExtensionPreloaded = true)
```

The Android `.so` are compiled with 16 KB page alignment, so they load on Android 15+ devices with 16 KB memory pages (a Play Store requirement for apps targeting Android 15+). Don't substitute the official `sqlite-vec` release `.so` — those are 4 KB aligned and fail with `dlopen failed: ... program alignment (4096) cannot be smaller than system page size (16384)`.

`RegisterAutoExtension()` is engine-aware, so it also works with **SQLCipher**: it registers `vec0` against whichever engine SQLitePCLRaw loaded (`e_sqlite3` or `e_sqlcipher`). For SQLCipher, call `SqliteVec.RegisterAutoExtension()` and set `VectorExtensionPreloaded = true` on your `SqlCipherDatabaseProvider` (`CreateProvider` returns a plain `SqliteDatabaseProvider`, so don't use it for the encrypted case).

If you supply your own binary, two mutually complementary flags on `SqliteDatabaseProvider`:

- **`EnableVectorExtension = true`** — loads at runtime via `SqliteConnection.LoadExtension("vec0")`. Ship the native binary on the load path. **Desktop/server only** — this path **cannot work on iOS** (Apple forbids `dlopen` of loose libraries; bundled `e_sqlite3` disables runtime loading) and usually fails on Android.
- **`VectorExtensionPreloaded = true`** — assumes the extension is already registered on every connection (statically linked + `sqlite3_auto_extension(sqlite3_vec_init)`), so it **skips the runtime load**. The only approach that works on iOS. If both flags are set, preloaded wins.

Either flag (or the package helper) makes `SupportsVector` return `true`. Without one, `NearestVectors` throws `NotSupportedException`. vec0 is flat-scan (no HNSW); when a `.Where(...)` filter is combined with the search, the library over-fetches `k * postFilterMultiplier` (default 4) candidates.

## Full-Text Search

Relevance-ranked text search over one or more string properties. **Declarative and up-front**: map the searchable property with `cfg.MapFullTextProperty(...)` and the library creates the native index for you at startup. A type **must be mapped before it can be searched** — there is no ad-hoc full-text (unlike `.Where(x => x.Body.Contains(...))`, which works on any field). Supported on **every provider**: FTS5 (SQLite), `tsvector`+GIN (PostgreSQL), `FULLTEXT` (MySQL), Oracle Text (Oracle), Full-Text Index (SQL Server), the `fts` extension (DuckDB), full-text policy (Cosmos), `$text` (MongoDB), and an in-memory TF-IDF scan on LiteDB / IndexedDB.

```csharp
// single field, or several combined into one index
options.ConfigureDocument<Article>(cfg =>
{
    cfg.MapFullTextProperty(a => a.Body);
    cfg.MapFullTextProperty([a => a.Title, a => a.Body]);
});

// terminal API — ordered by relevance descending, each with a Score (higher = better)
IReadOnlyList<FullTextResult<Article>> hits =
    await store.FullTextSearch<Article>("orleans persistence", maxResults: 20);

// optional pre-filter predicate (tenant/category scoping)
var tech = await store.FullTextSearch<Article>("orleans", filter: a => a.Category == "tech");

// fluent form — folds the query's Where predicates into the pre-filter
var hits2 = await store.Query<Article>()
    .Where(a => a.Category == "tech")
    .FullTextMatch("orleans", maxResults: 10);
```

`FullTextResult<T>` carries `Document` and a normalized `double Score` (higher = more relevant; absolute scale is provider-specific — compare only within one result set). `cfg.MapFullTextProperty` also has an AOT-safe overload taking `propertyNames` + a `Func<T, IEnumerable<string?>>` selector (for combining fields or indexing a string collection), and an optional `FullTextLanguage` (controls stemming where the backend supports it). The index is engine-maintained, so `Insert`/`Update`/`Remove`/`Clear` keep it in sync automatically. Notes: engines with one full-text index per table (SQL Server, MongoDB) support a single mapped type per table/collection; **Oracle Text** and **SQL Server Full-Text Search** are optional server components that must be installed; Cosmos full-text needs `Microsoft.Azure.Cosmos` 3.61.0+.

### Composable full-text with Lucene syntax (`DocumentFunctions.LuceneMatch` / `LuceneScore`)

For full-text as a **composable predicate** (not a separate ranked call), use `DocumentFunctions.LuceneMatch(field, luceneQuery)` inside a `Where`, and `DocumentFunctions.LuceneScore(field, luceneQuery)` inside an `OrderBy`/projection. They translate to the provider's native full-text engine over the **same `cfg.MapFullTextProperty` index** — so the type must still be mapped first. The `field` argument identifies the mapping (pass a mapped property); the search spans the whole combined index for the type.

```csharp
// AND with an ordinary predicate, page, and sort by relevance — one query
var hits = await store.Query<Article>()
    .Where(a => a.Category == "tech" && DocumentFunctions.LuceneMatch(a.Body, "orleans AND grain NOT deprecated"))
    .OrderByDescending(a => DocumentFunctions.LuceneScore(a.Body, "orleans grain"))
    .Skip(0).Take(20)
    .ToList();

// String-expression grammar (AOT-safe) — same IR:
store.Query<Article>().Where("lucenematch(body, 'title:quick AND brown~')");
store.Query<Article>().Where($"lucenematch(body, {userQuery})").OrderBy("lucenescore(body, 'orleans') desc");
```

**Lucene grammar:** terms, `"phrases"`, `AND`/`OR`/`NOT` (also `&&`/`||`/`!` and `+`/`-`), `(` grouping `)`, prefix `foo*`, fuzzy `foo~`/`foo~1`, proximity `"a b"~5`, boost `foo^2`. Ranges (`[a TO b]`) and non-trailing wildcards are rejected.

**Provider support (v1)** — operators a backend can't express throw `NotSupportedException` (they never silently degrade):

| Provider | LuceneMatch | LuceneScore | Advanced operators |
|---|---|---|---|
| SQLite (FTS5) | ✅ | ✅ | prefix, proximity |
| PostgreSQL | ✅ | ✅ | prefix |
| MySQL | ✅ | ✅ | prefix, proximity |
| SQL Server | ✅ | ✅ | prefix, proximity |
| Oracle Text | ✅ | ❌ (use `FullTextSearch`) | prefix, fuzzy, proximity |
| LiteDB / IndexedDB (in-memory) | ✅ | ✅ | **all** (incl. fuzzy) |
| DuckDB, CosmosDB, MongoDB | ❌ — use `store.FullTextSearch(...)` | ❌ | — |

Baseline (terms, phrases, AND/OR/NOT, grouping) works on every supported provider. Field-scoped terms (`title:foo`) are **not** supported in v1 on any provider (the index is a single combined field) and throw. Use `store.FullTextSearch<T>(...)` for ranking on the providers that don't support composable queries.
