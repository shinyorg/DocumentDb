# Vector Support — Design

This document is the design contract for vector / embedding support in `Shiny.DocumentDb`. The shape mirrors how the library already handles spatial: a `Map*Property` registration on options, a per-provider capability flag, a default-throwing implementation on `IDocumentStore`, and a fluent surface on `IDocumentQuery<T>`.

## Goals

1. **Cross-provider ANN search** with a single API: `store.Query<T>().NearestVectors(embedding, k)`.
2. **AOT- and trimming-safe** — no expression compilation, no reflection on the vector field at query time.
3. **Auto-embed-on-write** — when a `Microsoft.Extensions.AI.IEmbeddingGenerator` is registered, populating the vector field on insert/upsert is one line of config.
4. **Reuse the spatial precedent verbatim** — same capability flag pattern, same default-throw model, same sidecar-table mechanism on relational providers.

## Non-goals (v1)

- Hybrid (vector + text) search. Each provider's hybrid story is different enough that it deserves its own design pass.
- Multi-vector-per-document. Start with one mapped vector per document type.
- Re-ranking layers. Consumers can re-rank in process.
- Sparse / BM25. Out of scope.

## Type model

The vector value is `ReadOnlyMemory<float>` — matches `Microsoft.Extensions.AI.Embedding<float>.Vector`, marshals cleanly to native vector columns, and avoids forcing every document load to allocate a `float[]`.

```csharp
public class Document
{
    public Guid Id { get; set; }
    public string Content { get; set; } = "";
    public ReadOnlyMemory<float> Embedding { get; set; }
}
```

JSON serialization of `ReadOnlyMemory<float>` is supported out of the box by `System.Text.Json` since .NET 8 — it round-trips as a JSON array, which is what we want for the SQLite/Mongo/Cosmos paths anyway.

## Mapping API

A new pair of overloads on `DocumentStoreOptions` (and the provider-specific options classes that don't inherit from it: `CosmosDbDocumentStoreOptions`, `MongoDbDocumentStoreOptions`, `LiteDbDocumentStoreOptions`, `IndexedDbDocumentStoreOptions`).

```csharp
public DocumentStoreOptions MapVectorProperty<T>(
    Expression<Func<T, ReadOnlyMemory<float>>> property,
    int dimensions,
    VectorDistance metric = VectorDistance.Cosine,
    VectorIndexKind indexKind = VectorIndexKind.Hnsw,
    Action<VectorIndexOptions>? configureIndex = null) where T : class;

// AOT-safe overload, parallels MapSpatialProperty
public DocumentStoreOptions MapVectorProperty<T>(
    string propertyName,
    Func<T, ReadOnlyMemory<float>> getter,
    Action<T, ReadOnlyMemory<float>> setter,
    int dimensions,
    VectorDistance metric = VectorDistance.Cosine,
    VectorIndexKind indexKind = VectorIndexKind.Hnsw,
    Action<VectorIndexOptions>? configureIndex = null) where T : class;
```

A setter is required (unlike spatial, which is read-only) because the auto-embed hook needs to populate it.

### Enums

```csharp
public enum VectorDistance
{
    Cosine,
    Euclidean,       // L2
    DotProduct,      // negative inner product, i.e. higher = closer
    Hamming          // bit vectors, providers that support it
}

public enum VectorIndexKind
{
    None,            // no index — flat scan
    Flat,            // explicit flat index where supported (e.g. Cosmos)
    Hnsw,            // pgvector, DuckDB vss, Atlas; preferred default
    Ivf,             // pgvector ivfflat
    DiskAnn,         // SQL Server, CosmosDB
    QuantizedFlat    // CosmosDB
}
```

### Index tuning — strongly-typed common + ProviderHints

```csharp
public class VectorIndexOptions
{
    /// <summary>HNSW parameter M — graph node degree. pgvector default 16, DuckDB default 16.</summary>
    public int? HnswM { get; set; }

    /// <summary>HNSW efConstruction — build-time accuracy/speed tradeoff. pgvector default 64.</summary>
    public int? HnswEfConstruction { get; set; }

    /// <summary>HNSW efSearch — query-time accuracy/speed tradeoff. pgvector default 40.</summary>
    public int? HnswEfSearch { get; set; }

    /// <summary>IVF list count — for pgvector ivfflat.</summary>
    public int? IvfLists { get; set; }

    /// <summary>
    /// Provider-specific hints not covered by strongly-typed properties.
    /// Examples:
    ///   "cosmos.quantizedFlatVectorIndexShardKey" : "/tenant"
    ///   "sqlserver.metric_tail_size" : 64
    /// Unknown keys are ignored per provider.
    /// </summary>
    public Dictionary<string, object> ProviderHints { get; } = new();
}
```

Rationale: the three HNSW knobs (M, efConstruction, efSearch) and IVF lists cover the 90% of real-world tuning. Everything else — Cosmos-specific quantization, SQL Server DiskANN tail size, Atlas-specific `numCandidates` heuristics — lives in `ProviderHints` and the per-provider code consumes only the keys it knows.

## Query API

### On `IDocumentStore` (capability + simple form)

```csharp
bool SupportsVector => false;

Task<IReadOnlyList<VectorResult<T>>> NearestVectors<T>(
    ReadOnlyMemory<float> query,
    int k,
    Expression<Func<T, bool>>? filter = null,
    CancellationToken cancellationToken = default) where T : class
    => throw new NotSupportedException("Vector queries are not supported by this provider.");

public record VectorResult<T>(T Document, float Score) where T : class;
```

`Score` semantics: **lower = closer** for Cosine and Euclidean (we surface a distance); **higher = closer** for DotProduct (we surface the raw inner product). The mapping is provider-internal — `Cosine` is always exposed as distance in `[0, 2]` even if the underlying provider returns similarity. This is documented behavior so users don't need to learn each provider's score convention.

### On `IDocumentQuery<T>` (fluent form)

```csharp
public interface IDocumentQuery<T> where T : class
{
    // ...existing members...

    /// <summary>
    /// Terminates the query with an ANN search against the vector mapped via
    /// <c>MapVectorProperty</c>. The current <c>Where</c> predicates act as a
    /// pre-filter where the provider supports it. <c>OrderBy</c>, <c>GroupBy</c>,
    /// and <c>Paginate</c> are ignored — k controls the result count.
    /// </summary>
    Task<IReadOnlyList<VectorResult<T>>> NearestVectors(
        ReadOnlyMemory<float> query,
        int k,
        CancellationToken ct = default);
}
```

Filter semantics: **pre-filter where possible, post-filter otherwise**. The contract is "matches the `Where` predicates AND is in the top-k nearest". The order of operations is provider-dependent:

- **Cosmos, pgvector, SQL Server, MongoDB Atlas**: pre-filter native to the engine. Top-k is computed against the filtered subset. Result count == k (unless fewer documents match).
- **SQLite (sqlite-vec)**: ANN runs first, then post-filter. The vec0 query is given `k * post_filter_multiplier` candidates so the post-filter doesn't starve the result set. Default multiplier 4 — overridable via `VectorIndexOptions.ProviderHints["sqlite.postFilterMultiplier"]`.
- **DuckDB**: vss extension supports pre-filter via `WHERE` against the joined table.

When the call returns fewer than `k` results, it means the dataset was smaller than `k`, the pre-filter excluded enough rows, or the post-filter on SQLite ran out of candidates. The behavior is documented but not exception-y.

## Provider matrix

| Provider | Storage strategy | Index kinds | Filter strategy | Notes |
|---|---|---|---|---|
| **PostgreSQL** | Sidecar table `{table}_vec_{type}` with `vector(n)` column. `pgvector` extension installed via `CREATE EXTENSION IF NOT EXISTS vector` on first use. | HNSW, IVF, None | Pre-filter via JOIN | All four metrics supported (`<=>`, `<->`, `<#>`, `<+>` for Hamming on bit vectors). |
| **SQL Server 2025** | Sidecar table with `VECTOR(n)` column. `VECTOR_DISTANCE` for distance. | DiskANN, None | Pre-filter via JOIN | Requires SQL Server 2025 or Azure SQL with vector preview. If feature missing, throws on table init with a clear message. |
| **CosmosDB** | Same container as document. Vector embedding policy + indexing policy configured on first touch per type. `VectorDistance()` in `ORDER BY`. | DiskANN, QuantizedFlat, Flat | `WHERE` + `ORDER BY VectorDistance(...) ASC` | Cosine, Euclidean, DotProduct only. Hamming throws. |
| **MongoDB** | Atlas-only. Vector search index created via `db.runCommand({ createSearchIndexes: ... })`. `$vectorSearch` aggregation stage. | HNSW only (Atlas-managed) | Filter clause inside `$vectorSearch` | `numCandidates` defaults to `k * 10`, overridable via hint. Non-Atlas connections throw at `NearestVectors` with a clear message. |
| **DuckDB** | Sidecar table `{table}_vec_{type}` with `FLOAT[n]` column. `INSTALL vss; LOAD vss;` on connection init when any vector mapping is registered. | HNSW, None | Pre-filter via JOIN | `array_distance`, `array_cosine_similarity`, `array_inner_product`. |
| **SQLite** | Sidecar `vec0` virtual table per mapped type. `sqlite-vec` extension loaded via `SqliteConnection.LoadExtension("vec0")`. | None (flat scan, no HNSW) | Post-filter join back to documents | Mobile-friendly. Requires extension load — toggled via `SqliteVectorOptions.LoadExtension` (defaults to `true`). |
| **MySQL** | Sidecar table with `VECTOR(n)` column (MySQL 9+). Flat scan only — no native ANN index in 9.x. | None | Pre-filter via WHERE | Throws on `MapVectorProperty` if connection MySQL version < 9.0 at first use. |
| **LiteDB** | — | — | — | Throws `NotSupportedException` on `MapVectorProperty` registration. |
| **IndexedDB** | — | — | — | Throws `NotSupportedException` on `MapVectorProperty` registration. |

### Sidecar table schema (relational providers)

For PostgreSQL / SQL Server / DuckDB / MySQL, the sidecar table is one per `(documents-table, document-type)` pair:

```sql
CREATE TABLE {tableName}_vec_{typeNameSanitized} (
    docId   <id-type>     NOT NULL,
    typeName <varchar>    NOT NULL,
    embedding <vector(n)> NOT NULL,
    PRIMARY KEY (docId, typeName)
);
-- + provider-specific index DDL
```

`docId` joins back to the primary `documents.Id` column. `typeName` is included for symmetry with the documents-table composite key.

This isolates each `(type, vector)` mapping into its own table so the vector column type, dimension, and index parameters are unambiguous — no `dim` polymorphism within one table.

### SQLite vec0 schema

```sql
CREATE VIRTUAL TABLE {tableName}_vec_{typeNameSanitized} USING vec0(
    embedding float[N]
);
CREATE TABLE {tableName}_vec_map_{typeNameSanitized} (
    rowid    INTEGER PRIMARY KEY AUTOINCREMENT,
    docId    TEXT NOT NULL UNIQUE
);
```

The `_map` table bridges vec0's integer rowid to the document's string Id, mirroring the existing R*Tree spatial pattern.

## Provider abstraction additions

The `IDatabaseProvider` interface picks up vector hooks alongside the existing spatial ones:

```csharp
// Vector (optional)
bool SupportsVector => false;

string? BuildCreateVectorTablesSql(string tableName, string typeName, VectorMapping mapping) => null;
string? BuildVectorUpsertSql(string tableName, string typeName) => null;
string? BuildVectorDeleteSql(string tableName, string typeName) => null;
string? BuildVectorClearSql(string tableName, string typeName) => null;

/// Returns SQL plus the parameter dictionary used to bind the query vector. Provider-specific
/// because the parameter syntax for vector literals differs.
(string Sql, IReadOnlyDictionary<string, object> Parameters) BuildVectorSearchSql(
    string tableName,
    string typeName,
    VectorMapping mapping,
    ReadOnlyMemory<float> query,
    int k,
    string? additionalWhere)
    => throw new NotSupportedException("Vector queries are not supported by this provider.");

/// Hook for engines that need a load step on every connection (e.g. SQLite vec0, DuckDB vss).
Task LoadVectorExtensionAsync(DbConnection connection, CancellationToken ct) => Task.CompletedTask;
```

The non-relational providers (Cosmos, Mongo) don't fit this SQL-shaped interface; their `*DocumentStore` classes implement `NearestVectors` directly using their native client APIs.

## Auto-embed-on-insert

Goal: when a `Microsoft.Extensions.AI.IEmbeddingGenerator<string, Embedding<float>>` is registered in DI, the user only writes the text — the vector is populated automatically.

### Mapping

```csharp
options.MapVectorProperty<Document>(d => d.Embedding, dimensions: 1536)
       .AutoEmbedOnInsert<Document>(d => d.Content, d => d.Embedding);
```

`AutoEmbedOnInsert<T>` takes a `Func<T, string?>` source selector and the same `Expression<Func<T, ReadOnlyMemory<float>>>` target as the vector mapping. The store resolves the registered embedding generator at first use, holds a reference, and invokes it on:

- `Insert(T)` — when the target field is default and the source field is non-null/non-empty.
- `BatchInsert(IEnumerable<T>)` — same condition; batched into a single generator call where the generator supports it (the M.E.AI `GenerateAsync(IEnumerable<string>, ...)` overload).
- `Upsert(T)` — same condition.

If the target field already has a non-default value, auto-embed is skipped. If the source field is null/empty, auto-embed is skipped (the vector is left at default).

If `IEmbeddingGenerator<string, Embedding<float>>` is not registered, the first auto-embed call throws an `InvalidOperationException` with the offending type — caught early in dev, not a silent failure.

### DI wiring

The auto-embed feature lives in `Shiny.DocumentDb.Extensions.AI` (existing package) — the core `Shiny.DocumentDb` package keeps its zero-AI dependency profile. `Shiny.DocumentDb.Extensions.AI` already references `Microsoft.Extensions.AI.Abstractions`, so the additional surface (`AutoEmbedOnInsert`, generator resolution) lives there.

The pattern:

```csharp
services
    .AddOpenAIClient(...)
    .AddEmbeddingGenerator(...)
    .AddDocumentStore(opts =>
    {
        opts.DatabaseProvider = new SqliteDatabaseProvider("Data Source=mydata.db");
        opts.MapVectorProperty<Document>(d => d.Embedding, dimensions: 1536);
    })
    .AddDocumentStoreAutoEmbed<Document>(d => d.Content, d => d.Embedding);
```

`AddDocumentStoreAutoEmbed` is a `Shiny.DocumentDb.Extensions.AI` extension method. Implementation: at `DocumentStore` construction time, the DI extension chains an interceptor onto the insert path via a new `DocumentStoreOptions.OnBeforeInsert` hook (also added by this design — see "Required core hooks" below).

### Required core hooks

To keep the AI bits in the AI package, the core needs three things:

1. `DocumentStoreOptions.OnBeforeInsert<T>(Func<T, CancellationToken, Task> handler)` — fires inside `Insert`, `BatchInsert`, `Upsert` before serialization. Multiple handlers allowed; runs in registration order.
2. `VectorMapping.Setter` (already in the API contract above) — so the auto-embed handler can write `ReadOnlyMemory<float>` back without reflection at hot path.
3. Exposing the registered `VectorMapping` set via an internal accessor — already mirrored from `spatialMappings`.

This is the minimum surface change to keep `Shiny.DocumentDb` AI-agnostic.

## Error semantics

| Scenario | Behavior |
|---|---|
| Provider doesn't support vectors at all | `MapVectorProperty` throws immediately at registration time (LiteDB, IndexedDB). |
| Provider supports vectors but the runtime is too old | First operation that touches the sidecar table throws — clear message naming the required version (MySQL 9, SQL Server 2025, Atlas Vector Search). |
| Query vector length ≠ mapped dimensions | `ArgumentException` at the call site of `NearestVectors`. |
| Vector field has wrong length in a doc on `Insert` | `ArgumentException` from the upsert SQL with the document Id and expected/actual dimension. |
| `IEmbeddingGenerator` not registered but `AutoEmbedOnInsert` configured | First insert throws `InvalidOperationException`. |
| Empty / missing extension on SQLite or DuckDB | First operation throws a `NotSupportedException` with the install instruction in the message. |

## Open questions (worth revisiting)

- **Should `NearestVectors` participate in change feeds?** Probably not in v1 — change feed → re-rank is a layer above the store.
- **Should the fluent builder reject calls like `OrderBy` followed by `NearestVectors`?** Yes; the call throws with a clear "OrderBy is ignored on vector search; remove it" message.
- **`ProviderHints` discoverability.** Document the recognized keys per provider in the readme. No string-typed key validation in v1 — unknown keys silently ignored, matching how most query-language hints work.

## What ships in v1 vs later

**v1 (this build):**
- All core abstractions and enums.
- `MapVectorProperty` on `DocumentStoreOptions`, `CosmosDbDocumentStoreOptions`, `MongoDbDocumentStoreOptions`, `LiteDbDocumentStoreOptions`, `IndexedDbDocumentStoreOptions`.
- `NearestVectors` on `IDocumentStore` + `IDocumentQuery<T>`.
- Provider impls: SQLite, PostgreSQL, SQL Server, CosmosDB, MongoDB (Atlas), DuckDB.
- LiteDB / MySQL / IndexedDB throw at `MapVectorProperty` time.
- Auto-embed-on-insert in `Shiny.DocumentDb.Extensions.AI`.

**Deferred:**
- Hybrid search.
- Multi-vector-per-document.
- MySQL impl (wait for HNSW in MySQL).
- Vector-aware change feeds.
