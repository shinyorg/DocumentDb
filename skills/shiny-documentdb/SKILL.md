--
name: shiny-documentdb
description: Generate code using Shiny.DocumentDb, a schema-free multi-provider JSON document store for .NET supporting SQLite, LiteDB, CosmosDB, IndexedDB (Blazor WASM), MySQL, SQL Server, and PostgreSQL with LINQ queries, spatial/geo queries, and AOT support
auto_invoke: true
triggers:
  - document store
  - document db
  - DocumentStore
  - SqliteDocumentStore
  - IDocumentStore
  - IDocumentQuery
  - IDatabaseProvider
  - json document
  - schema-free
  - sqlite document
  - document database
  - json store
  - Shiny.DocumentDb
  - Shiny.DocumentDb
  - SqliteDatabaseProvider
  - SqlCipherDatabaseProvider
  - SqlCipherDocumentStore
  - sqlcipher
  - encrypted sqlite
  - MySqlDatabaseProvider
  - SqlServerDatabaseProvider
  - PostgreSqlDatabaseProvider
  - json_extract
  - document query
  - fluent query
  - paginate
  - MapTypeToTable
  - table per type
  - GetDiff
  - JsonPatchDocument
  - document diff
  - BatchInsert
  - batch insert
  - LiteDbDocumentStore
  - LiteDbDocumentStoreOptions
  - Shiny.DocumentDb.LiteDb
  - litedb
  - CosmosDbDocumentStore
  - CosmosDbDocumentStoreOptions
  - Shiny.DocumentDb.CosmosDb
  - cosmosdb
  - cosmos db
  - GeoPoint
  - GeoBoundingBox
  - SpatialResult
  - WithinRadius
  - WithinBoundingBox
  - NearestNeighbors
  - MapSpatialProperty
  - spatial query
  - geo query
  - geolocation
  - ClearAllAsync
  - Backup
  - IndexedDbDocumentStore
  - IndexedDbDocumentStoreOptions
  - Shiny.DocumentDb.IndexedDb
  - indexeddb
  - indexed db
  - blazor wasm
  - blazor webassembly
  - browser storage
  - AddDocumentStore
  - IDocumentStoreProvider
  - FromKeyedServices
  - keyed service
  - named store
  - multiple databases
  - Shiny.DocumentDb.Extensions.DependencyInjection
  - Shiny.DocumentDb.Extensions.AI
  - DocumentStoreAITools
  - DocumentAICapabilities
  - AddDocumentStoreAITools
  - IDocumentAIToolBuilder
  - AI tool
  - ai tools
  - LLM tool
  - function calling
---

# Shiny DocumentDb Skill

You are an expert in Shiny.DocumentDb, a lightweight multi-provider document store for .NET that turns relational databases into a schema-free JSON document database with LINQ querying, spatial/geo queries, and full AOT/trimming support. Supports **SQLite**, **SQLCipher** (encrypted SQLite), **LiteDB**, **CosmosDB**, **IndexedDB** (Blazor WebAssembly), **MySQL**, **SQL Server**, and **PostgreSQL**.

## When to Use This Skill

Invoke this skill when the user wants to:
- Store and retrieve .NET objects as JSON documents in SQLite, IndexedDB, MySQL, SQL Server, or PostgreSQL
- Query JSON documents with LINQ expressions or raw SQL
- Set up a schema-free document database without migrations
- Use AOT-safe document storage with `JsonTypeInfo<T>` overloads
- Stream query results with `IAsyncEnumerable<T>`
- Create JSON property indexes for faster queries
- Project query results into DTOs at the SQL level
- Compute aggregates (Max, Min, Sum, Average) across documents
- Use aggregate projections with GROUP BY via `Sql.*` markers
- Sort query results with expression-based OrderBy/OrderByDescending
- Paginate query results with LIMIT/OFFSET
- Use transactions for atomic document operations
- Work with nested objects and child collections without table design
- Map document types to dedicated tables (table-per-type)
- Use a custom Id property instead of the default `Id`
- Diff a modified object against a stored document (`GetDiff`)
- Batch insert multiple documents efficiently (`BatchInsert`)
- Choose between database providers (SQLite, IndexedDB, MySQL, SQL Server, PostgreSQL)
- Use IndexedDB for client-side storage in Blazor WebAssembly apps
- Query documents by geographic proximity (within radius, bounding box, nearest neighbors)
- Configure spatial indexing for `GeoPoint` properties (`MapSpatialProperty`)
- Use SQLite R*Tree spatial indexes or CosmosDB native GeoJSON queries
- Back up SQLite, SQLCipher, or LiteDB databases to a file (`Backup`)
- Clear all documents across all tables in SQLite (`ClearAllAsync`)
- Expose document types as AI tools for LLM agents (`AddDocumentStoreAITools`)
- Configure AI tool capabilities per type (ReadOnly, All, or individual flags)
- Control field visibility for LLM access (AllowProperties, IgnoreProperties)
- Use structured filter expressions in AI tool queries

## Library Overview

- **Repository**: https://github.com/shinyorg/DocumentDb
- **Core namespace**: `Shiny.DocumentDb`
- **NuGet packages**:
  - `Shiny.DocumentDb` — core (abstractions, `DocumentStore`, `IDocumentStore`, expression visitor)
  - `Shiny.DocumentDb.Sqlite` — SQLite provider + DI extensions
  - `Shiny.DocumentDb.Sqlite.SqlCipher` — SQLCipher (encrypted SQLite) provider + DI extensions
  - `Shiny.DocumentDb.MySql` — MySQL provider + DI extensions
  - `Shiny.DocumentDb.SqlServer` — SQL Server provider + DI extensions
  - `Shiny.DocumentDb.PostgreSql` — PostgreSQL provider + DI extensions
  - `Shiny.DocumentDb.LiteDb` — LiteDB provider + DI extensions
  - `Shiny.DocumentDb.CosmosDb` — Azure Cosmos DB provider + DI extensions
  - `Shiny.DocumentDb.IndexedDb` — IndexedDB provider for Blazor WebAssembly + DI extensions
  - `Shiny.DocumentDb.Extensions.DependencyInjection` — generic (provider-agnostic) DI extensions
  - `Shiny.DocumentDb.Extensions.AI` — Microsoft.Extensions.AI tool surface (AIFunction tools for LLM agents)
- **Provider dependencies**:
  - SQLite: `Microsoft.Data.Sqlite`
  - SQLCipher: `Microsoft.Data.Sqlite.Core` + `SQLitePCLRaw.bundle_e_sqlcipher`
  - MySQL: `MySqlConnector`
  - SQL Server: `Microsoft.Data.SqlClient`
  - PostgreSQL: `Npgsql`
  - LiteDB: `LiteDB`
  - CosmosDB: `Microsoft.Azure.Cosmos`
  - IndexedDB: `Microsoft.JSInterop` (browser JS interop)
- **AI dependency**: `Microsoft.Extensions.AI.Abstractions`
- **Target**: `net10.0`

## Setup

### Direct Instantiation

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
```

> **Note:** `SqliteDocumentStore` and `SqlCipherDocumentStore` are still available as convenience wrappers: `new SqliteDocumentStore("Data Source=mydata.db")` or `new SqlCipherDocumentStore("encrypted.db", "mySecretKey")`.

### Dependency Injection

Install `Shiny.DocumentDb.Extensions.DependencyInjection` and use `AddDocumentStore` to register `IDocumentStore` as a singleton:

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

> **Note:** LiteDB, CosmosDB, and IndexedDB have their own store and options types. Register them directly with the DI container (e.g. `services.AddSingleton<IDocumentStore, LiteDbDocumentStore>()`).

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
```

Inject via `[FromKeyedServices("name")]` attribute or resolve dynamically via `IDocumentStoreProvider`:

```csharp
// Attribute injection
public class MyService(
    [FromKeyedServices("users")] IDocumentStore userStore,
    [FromKeyedServices("analytics")] IDocumentStore analyticsStore) { }

// Dynamic resolution
public class MyService(IDocumentStoreProvider stores)
{
    void DoWork() => stores.GetStore("users").Insert(...);
}
```

### DocumentStoreOptions

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `DatabaseProvider` | `IDatabaseProvider` (required) | — | The database provider (`SqliteDatabaseProvider`, `SqlCipherDatabaseProvider`, `MySqlDatabaseProvider`, `SqlServerDatabaseProvider`, `PostgreSqlDatabaseProvider`) |
| `TableName` | `string` | `"documents"` | Default table name for all document types not mapped via `MapTypeToTable` |
| `TypeNameResolution` | `TypeNameResolution` | `ShortName` | How type names are stored (`ShortName` or `FullName`) |
| `JsonSerializerOptions` | `JsonSerializerOptions?` | `null` | JSON serialization settings. When a `JsonSerializerContext` is attached as the `TypeInfoResolver`, all methods auto-resolve type info from the context |
| `UseReflectionFallback` | `bool` | `true` | When `false`, throws `InvalidOperationException` if a type can't be resolved from the configured `TypeInfoResolver` instead of falling back to reflection. Recommended for AOT deployments |
| `Logging` | `Action<string>?` | `null` | Callback invoked with every SQL statement executed |

## Table-Per-Type Mapping

By default all document types share a single table. Use `MapTypeToTable` to give a type its own dedicated table. Tables are lazily created on first use. Two types cannot map to the same custom table.

### Basic mapping

```csharp
var store = new DocumentStore(new DocumentStoreOptions
{
    DatabaseProvider = new SqliteDatabaseProvider("Data Source=mydata.db"),
    TableName = "docs"                 // change the default table name (optional)
}
.MapTypeToTable<Order>("orders")       // explicit table name
.MapTypeToTable<AuditLog>()            // auto-derived table name "AuditLog"
// User stays in the default "docs" table
);
```

### Custom Id property

By default every document type must have a property named `Id`. When mapping a type to a table, you can also specify a custom Id property via an expression. Custom Id requires a table mapping.

```csharp
var store = new DocumentStore(new DocumentStoreOptions
{
    DatabaseProvider = new SqliteDatabaseProvider("Data Source=mydata.db")
}
.MapTypeToTable<Sensor>("sensors", s => s.DeviceKey)      // Guid DeviceKey as Id
.MapTypeToTable<Tenant>("tenants", t => t.TenantCode)     // string TenantCode as Id
);
```

### MapTypeToTable overloads

| Overload | Description |
|----------|-------------|
| `MapTypeToTable<T>()` | Auto-derive table name from type name |
| `MapTypeToTable<T>(string tableName)` | Explicit table name |
| `MapTypeToTable<T>(Expression<Func<T, object>> idProperty)` | Auto-derive table + custom Id |
| `MapTypeToTable<T>(string tableName, Expression<Func<T, object>> idProperty)` | Explicit table + custom Id |

All overloads return `DocumentStoreOptions` for fluent chaining. Duplicate table names throw `InvalidOperationException`.

## AOT Setup

For AOT/trimming compatibility, create a source-generated JSON context:

```csharp
[JsonSerializable(typeof(User))]
[JsonSerializable(typeof(Order))]
[JsonSerializable(typeof(Address))]
[JsonSerializable(typeof(OrderLine))]
public partial class AppJsonContext : JsonSerializerContext;
```

**Important:** Do NOT add `[JsonSerializerContext]` attribute — it is abstract and inherited automatically.

Create an instance with your desired options:

```csharp
var ctx = new AppJsonContext(new JsonSerializerOptions
{
    PropertyNamingPolicy = JsonNamingPolicy.CamelCase
});
```

Pass `ctx.Options` to `DocumentStoreOptions.JsonSerializerOptions` so the expression visitor and serializer share the same configuration.

### Optional JsonTypeInfo<T> Parameters

All `JsonTypeInfo<T>` parameters are optional (`= null` default). When omitted, type info is resolved automatically from the configured `JsonSerializerOptions.TypeInfoResolver`. This means you can configure a `JsonSerializerContext` once at setup and skip passing `JsonTypeInfo<T>` on every call.

```csharp
// Configure once
var store = new DocumentStore(new DocumentStoreOptions
{
    DatabaseProvider = new SqliteDatabaseProvider("Data Source=mydata.db"),
    JsonSerializerOptions = ctx.Options,
    UseReflectionFallback = false // recommended for AOT
});

// All calls auto-resolve type info — no explicit JsonTypeInfo needed
var user = new User { Name = "Alice", Age = 25 };
await store.Insert(user);
var fetched = await store.Get<User>(user.Id);
var users = await store.Query<User>().Where(u => u.Age > 25).ToList();
```

You can still pass `JsonTypeInfo<T>` explicitly when needed (e.g., for types not registered in the context):

```csharp
await store.Insert(new User { Id = "alice-1", Name = "Alice" }, ctx.User);
```

## Document Types

Every document type must have a public `Id` property of type `Guid`, `int`, `long`, or `string`. The Id is stored in both the database `Id` column and inside the JSON blob, so query results always include it.

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
| `string` | `null` or `""` | **Throws** — an explicit Id is required |
| `int` | `0` | `MAX(CAST(Id AS INTEGER)) + 1` per TypeName |
| `long` | `0` | `MAX(CAST(Id AS INTEGER)) + 1` per TypeName |

When `Insert` is called with a default Id, the store auto-generates one and writes it back to the object (except for `string` Ids, which throw if the value is `null` or `""`). When a non-default Id is provided, it is used as-is.

## Core API Reference (IDocumentStore)

### Insert / Update / Upsert

```csharp
// Auto-generated ID — written back to the object
var user = new User { Name = "Alice", Age = 25 };
await store.Insert(user);
// user.Id is now populated

// Explicit ID
await store.Insert(new User { Id = "user-1", Name = "Alice", Age = 25 });
```

### Batch insert

`BatchInsert` inserts multiple documents in a single transaction with prepared command reuse. Returns the count inserted. Rolls back atomically on failure. Auto-generates IDs for Guid, int, and long Id types.

```csharp
var users = Enumerable.Range(1, 1000).Select(i => new User
{
    Id = $"user-{i}", Name = $"User {i}", Age = 20 + i
});
var count = await store.BatchInsert(users); // single transaction, prepared command reused

// Inside a transaction — uses the existing transaction
await store.RunInTransaction(async tx =>
{
    await tx.BatchInsert(moreUsers);
    await tx.Insert(singleUser);
});
```

### Get

The `id` parameter accepts `Guid`, `int`, `long`, or `string`. Passing an unsupported type throws `ArgumentException`.

```csharp
var user = await store.Get<User>("user-1");

// Guid, int, and long Ids work directly — no ToString() needed
var item = await store.Get<GuidIdModel>(myGuid);
var order = await store.Get<IntIdModel>(42);
```

### GetDiff (Diff)

Compare a modified object against the stored document and get an RFC 6902 `JsonPatchDocument<T>` describing the differences. Returns `null` if no document with that ID exists. Deep diffs nested objects (individual property ops); arrays/collections are replaced as a whole.

```csharp
var modified = new Order
{
    Id = "ord-1", CustomerName = "Alice", Status = "Delivered",
    ShippingAddress = new() { City = "Seattle", State = "WA" },
    Lines = [new() { ProductName = "Widget", Quantity = 10, UnitPrice = 8.99m }],
    Tags = ["priority", "expedited"]
};

// Returns JsonPatchDocument<Order> from SystemTextJsonPatch
var patch = await store.GetDiff("ord-1", modified);
// patch.Operations:
//   Replace /status → Delivered
//   Replace /shippingAddress/city → Seattle
//   Replace /shippingAddress/state → WA
//   Replace /lines → [...]
//   Replace /tags → [...]

// Apply the patch to any instance
var current = await store.Get<Order>("ord-1");
patch!.ApplyTo(current!);
```

Works with table-per-type, custom Id, and inside transactions.

### Upsert (JSON Merge Patch)

```csharp
// Deep-merges patch into existing document via json_patch (RFC 7396)
// Document must have a non-default Id
await store.Upsert(new User { Id = "user-1", Name = "Alice", Age = 30 });
```

### SetProperty / RemoveProperty

The `id` parameter accepts `Guid`, `int`, `long`, or `string`. Passing an unsupported type throws `ArgumentException`.

```csharp
// Update a single field via json_set — no deserialization
await store.SetProperty<User>("user-1", u => u.Age, 31);

// Nested property
await store.SetProperty<Order>("order-1", o => o.ShippingAddress.City, "Portland");

// Remove a field via json_remove
await store.RemoveProperty<User>("user-1", u => u.Email);
```

### Remove / Clear

The `id` parameter accepts `Guid`, `int`, `long`, or `string`. Passing an unsupported type throws `ArgumentException`.

```csharp
// By ID
bool deleted = await store.Remove<User>("user-1");
bool removed = await store.Remove<GuidIdModel>(myGuid);

// Clear all documents of a type
int deletedCount = await store.Clear<User>();
```

### Raw SQL Query

Raw SQL uses provider-specific JSON functions. The SQL syntax varies by provider:

| Provider | JSON extract syntax |
|---|---|
| SQLite | `json_extract(Data, '$.name')` |
| MySQL | `JSON_EXTRACT(Data, '$.name')` |
| SQL Server | `JSON_VALUE(Data, '$.name')` |
| PostgreSQL | `"Data"::jsonb->>'name'` |

```csharp
// SQLite example
var results = await store.Query<User>(
    "json_extract(Data, '$.name') = @name",
    parameters: new { name = "Alice" });

// Streaming
await foreach (var user in store.QueryStream<User>(
    "json_extract(Data, '$.name') = @name",
    parameters: new { name = "Alice" }))
{
    Console.WriteLine(user.Name);
}
```

### Count (Raw SQL)

```csharp
var count = await store.Count<User>(
    "json_extract(Data, '$.age') > @minAge",
    new { minAge = 30 });
```

### Transactions

```csharp
await store.RunInTransaction(async tx =>
{
    await tx.Insert(new User { Id = "u1", Name = "Alice", Age = 25 });
    await tx.Insert(new User { Id = "u2", Name = "Bob", Age = 30 });
    // Commits on success, rolls back on exception
});
```

### Rekeying (SQLCipher only)

Change the encryption key of an existing SQLCipher database. Extension method on `IDocumentStore` that issues `PRAGMA rekey` with SQL injection protection via `quote()`. Throws `InvalidOperationException` if the store is not using `SqlCipherDatabaseProvider`.

```csharp
using Shiny.DocumentDb.Sqlite.SqlCipher;

await store.RekeyAsync("newPassword");
```

> **Important:** After rekeying, the store still holds the old password internally. Create a new store with the new password for subsequent operations.

### Backup (SQLite/SQLCipher/LiteDB only)

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

### ClearAllAsync (SQLite only)

Deletes all documents across all tables in the SQLite database, including spatial sidecar tables. Only available on `SqliteDocumentStore`.

```csharp
var sqliteStore = new SqliteDocumentStore("Data Source=mydata.db");
await sqliteStore.ClearAllAsync();
```

### SQLite in Blazor WebAssembly

The SQLite provider (`Shiny.DocumentDb.Sqlite`) is compatible with Blazor WebAssembly when paired with `SQLitePCLRaw.bundle_wasm`. The provider automatically adapts at runtime:

- **WAL pragma skipped** — `SqliteDatabaseProvider` checks `OperatingSystem.IsBrowser()` and skips the WAL journal mode pragma (not applicable on the Emscripten virtual filesystem)
- **Spatial disabled** — `SupportsSpatial` returns `false` in the browser because R*Tree virtual tables are unavailable in WASM-compiled SQLite
- **Backup unsupported** — `SqliteDocumentStore.Backup()` is marked `[UnsupportedOSPlatform("browser")]` and will produce a compiler warning if called from browser-targeted code
- **Connection strings** — use `Data Source=:memory:` for in-memory storage or Emscripten OPFS-mounted paths for persistence

All other features (LINQ queries, JSON indexes, table-per-type mapping, transactions, batch insert, aggregates, projections) work identically in WASM.

> **Tip:** For most Blazor WASM client-side storage, the lighter **IndexedDB provider** (`Shiny.DocumentDb.IndexedDb`) is recommended — no native WASM binary needed. Choose SQLite-in-WASM only when you need raw SQL queries, JSON indexes, or spatial capabilities.

## Spatial / Geo Queries

Spatial queries are supported on **SQLite** (via R*Tree virtual tables) and **CosmosDB** (via native GeoJSON + `ST_DISTANCE`/`ST_WITHIN`). Other providers throw `NotSupportedException`.

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

var store = new DocumentStore(new DocumentStoreOptions
{
    DatabaseProvider = new SqliteDatabaseProvider("Data Source=mydata.db")
}
.MapSpatialProperty<Restaurant>(r => r.Location)
);

// AOT-safe overload
.MapSpatialProperty<Restaurant>("Location", r => r.Location)
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

### How It Works

- **SQLite**: Creates R*Tree sidecar tables (`{table}_spatial` and `{table}_spatial_map`) that are automatically synced on insert/update/upsert/remove/clear. Bounding box pre-filter via R*Tree, then Haversine post-filter for exact radius.
- **CosmosDB**: `GeoPoint` serializes as GeoJSON `{"type":"Point","coordinates":[lng,lat]}`. Spatial index policies are added to the container automatically. Queries use native `ST_DISTANCE` and `ST_WITHIN` functions.

### Spatial CRUD Sync

Spatial sidecar data is automatically maintained — no manual steps needed:
- **Insert/Update/Upsert**: Extracts `GeoPoint` from the document and upserts into spatial index
- **Remove**: Deletes spatial data for that document
- **Clear**: Removes all spatial data for that type

## Fluent Query Builder (IDocumentQuery<T>)

The fluent query builder is the primary way to query documents. Start with `store.Query<T>()` and chain builder methods, then terminate with a materialization method.

### Builder Methods (non-executing, return IDocumentQuery<T>)

| Method | Description |
|--------|-------------|
| `.Where(predicate)` | Filter by LINQ expression. Multiple calls combine with AND. |
| `.OrderBy(selector)` | Sort ascending by property. |
| `.OrderByDescending(selector)` | Sort descending by property. |
| `.GroupBy(selector)` | Group by property (for aggregate projections). |
| `.Paginate(offset, take)` | Limit results with SQL LIMIT/OFFSET. |
| `.Select(selector, resultTypeInfo?)` | Project into a different shape via `json_object`. |

### Terminal Methods (execute SQL)

| Method | Returns | Description |
|--------|---------|-------------|
| `.ToList()` | `Task<IReadOnlyList<T>>` | Materialize all results into a list. |
| `.ToAsyncEnumerable()` | `IAsyncEnumerable<T>` | Stream results one-at-a-time. |
| `.Count()` | `Task<long>` | Count matching documents. |
| `.Any()` | `Task<bool>` | Check if any documents match. |
| `.ExecuteDelete()` | `Task<int>` | Delete matching documents. Returns count. |
| `.ExecuteUpdate(property, value)` | `Task<int>` | Update a property on all matching documents via `json_set()`. Returns count. |
| `.Max(selector)` | `Task<TValue>` | Maximum value of a property. |
| `.Min(selector)` | `Task<TValue>` | Minimum value of a property. |
| `.Sum(selector)` | `Task<TValue>` | Sum of a property. |
| `.Average(selector)` | `Task<double>` | Average of a property. |

### Common Patterns

```csharp
// Get all documents of a type
var users = await store.Query<User>().ToList();

// Filter
var results = await store.Query<User>()
    .Where(u => u.Age > 25)
    .ToList();

// Filter + sort
var results = await store.Query<User>()
    .Where(u => u.Age > 25)
    .OrderBy(u => u.Name)
    .ToList();

// Filter + sort + paginate
var page = await store.Query<User>()
    .Where(u => u.Age > 25)
    .OrderBy(u => u.Name)
    .Paginate(0, 20)
    .ToList();

// Stream results
await foreach (var user in store.Query<User>()
    .Where(u => u.Age > 25)
    .OrderByDescending(u => u.Age)
    .ToAsyncEnumerable())
{
    Console.WriteLine(user.Name);
}

// Count
var count = await store.Query<User>()
    .Where(u => u.Age > 25)
    .Count();

// Check existence
var any = await store.Query<User>()
    .Where(u => u.Name == "Alice")
    .Any();

// Delete matching documents
int deleted = await store.Query<User>()
    .Where(u => u.Age < 18)
    .ExecuteDelete();

// Update a property on matching documents
int updated = await store.Query<User>()
    .Where(u => u.Age < 18)
    .ExecuteUpdate(u => u.Age, 18);

// Update a nested property
int updated = await store.Query<Order>()
    .Where(o => o.ShippingAddress.City == "Portland")
    .ExecuteUpdate(o => o.ShippingAddress.City, "Eugene");

// Scalar aggregates
var maxAge = await store.Query<User>().Max(u => u.Age);
var minAge = await store.Query<User>().Where(u => u.Name != "Admin").Min(u => u.Age);
var totalAge = await store.Query<User>().Sum(u => u.Age);
var avgAge = await store.Query<User>().Average(u => u.Age);
```

## Pagination

`Paginate(offset, take)` appends `LIMIT {take} OFFSET {offset}` to the generated SQL. It does not execute the query — it's a builder method that stores state until a terminal method is called.

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

// Streaming with pagination
await foreach (var user in store.Query<User>()
    .OrderBy(u => u.Name)
    .Paginate(0, 50)
    .ToAsyncEnumerable())
{
    Console.WriteLine(user.Name);
}
```

## Expression Query Patterns

The expression visitor translates LINQ expressions to `json_extract` SQL. Property names are resolved from `JsonTypeInfo` metadata, so `[JsonPropertyName]` and naming policies are respected.

### Equality and Comparisons

```csharp
u => u.Name == "Alice"       // json_extract(Data, '$.name') = @p0
u => u.Age > 25              // json_extract(Data, '$.age') > @p0
u => u.Age <= 25             // json_extract(Data, '$.age') <= @p0
```

### Logical Operators

```csharp
u => u.Age == 25 && u.Name == "Alice"          // (... AND ...)
u => u.Name == "Alice" || u.Name == "Bob"      // (... OR ...)
u => !(u.Name == "Alice")                       // NOT (...)
```

### Null Checks

```csharp
u => u.Email == null          // ... IS NULL
u => u.Email != null          // ... IS NOT NULL
```

### String Methods

```csharp
u => u.Name.Contains("li")       // ... LIKE '%' || @p0 || '%'
u => u.Name.StartsWith("Al")     // ... LIKE @p0 || '%'
u => u.Name.EndsWith("ob")       // ... LIKE '%' || @p0
```

### Nested Object Properties

```csharp
o => o.ShippingAddress.City == "Portland"
// json_extract(Data, '$.shippingAddress.city') = @p0
```

### Collection Queries with Any()

```csharp
// Object collection — filter by child property
o => o.Lines.Any(l => l.ProductName == "Widget")
// EXISTS (SELECT 1 FROM json_each(...) WHERE ...)

// Primitive collection — filter by value
o => o.Tags.Any(t => t == "priority")
// EXISTS (SELECT 1 FROM json_each(...) WHERE value = @p0)

// Check if collection has any elements
o => o.Tags.Any()
// json_array_length(Data, '$.tags') > 0
```

### Collection Queries with Count()

```csharp
// Count elements (no predicate)
o => o.Lines.Count() > 1
// json_array_length(Data, '$.lines') > 1

// Count matching elements (with predicate)
o => o.Lines.Count(l => l.Quantity >= 3) >= 1
// (SELECT COUNT(*) FROM json_each(...) WHERE ...) >= 1
```

### DateTime and DateTimeOffset

Values are formatted as ISO 8601 to match `System.Text.Json` output:

```csharp
var cutoff = new DateTime(2025, 1, 1, 0, 0, 0, DateTimeKind.Utc);
e => e.StartDate > cutoff

var start = new DateTimeOffset(2025, 1, 1, 0, 0, 0, TimeSpan.Zero);
e => e.CreatedAt >= start && e.CreatedAt < end
```

### Captured Variables

```csharp
var targetName = "Alice";
u => u.Name == targetName    // Extracted from closure at translate time
```

## Projections

Project into DTOs at the SQL level via `json_object` — no full document deserialization needed. Use `.Select()` on the query builder.

### Flat Projection

```csharp
var results = await store.Query<User>()
    .Where(u => u.Age == 25)
    .Select(u => new UserSummary { Name = u.Name, Email = u.Email })
    .ToList();
```

### Nested Source Properties

```csharp
var results = await store.Query<Order>()
    .Where(o => o.Status == "Shipped")
    .Select(o => new OrderSummary { Customer = o.CustomerName, City = o.ShippingAddress.City })
    .ToList();
```

### All Documents with Projection

```csharp
var results = await store.Query<Order>()
    .Select(o => new OrderDetail { Customer = o.CustomerName, LineCount = o.Lines.Count() })
    .ToList();
```

### Collection Methods in Projections

```csharp
// Count()
o => new OrderDetail { LineCount = o.Lines.Count() }
// SQL: json_array_length(Data, '$.lines')

// Count(predicate)
o => new OrderDetail { GadgetCount = o.Lines.Count(l => l.ProductName == "Gadget") }
// SQL: (SELECT COUNT(*) FROM json_each(...) WHERE ...)

// Any()
o => new OrderDetail { HasLines = o.Lines.Any() }
// SQL: CASE WHEN json_array_length(...) > 0 THEN json('true') ELSE json('false') END

// Any(predicate)
o => new OrderDetail { HasPriority = o.Tags.Any(t => t == "priority") }
// SQL: CASE WHEN EXISTS (...) THEN json('true') ELSE json('false') END

// Collection aggregates — Sum, Max, Min, Average
o => new R { TotalQty = o.Lines.Sum(l => l.Quantity) }
// SQL: (SELECT SUM(json_extract(value, '$.quantity')) FROM json_each(Data, '$.lines'))

o => new R { MaxPrice = o.Lines.Max(l => l.UnitPrice) }
// SQL: (SELECT MAX(json_extract(value, '$.unitPrice')) FROM json_each(Data, '$.lines'))
```

## Ordering

Sort results at the SQL level using the fluent `.OrderBy()` and `.OrderByDescending()` methods.

```csharp
// Ascending
var users = await store.Query<User>()
    .OrderBy(u => u.Age)
    .ToList();

// Descending
var users = await store.Query<User>()
    .OrderByDescending(u => u.Age)
    .ToList();

// With filter
var results = await store.Query<User>()
    .Where(u => u.Age > 25)
    .OrderBy(u => u.Name)
    .ToList();

// With projection
var results = await store.Query<User>()
    .OrderBy(u => u.Name)
    .Select(u => new UserSummary { Name = u.Name, Email = u.Email })
    .ToList();

// With streaming
await foreach (var user in store.Query<User>()
    .OrderByDescending(u => u.Age)
    .ToAsyncEnumerable())
{
    Console.WriteLine(user.Name);
}
```

Generated SQL: `ORDER BY json_extract(Data, '$.age') ASC`

## Scalar Aggregates

Compute Max, Min, Sum, Average across documents using terminal methods on the query builder.

```csharp
var maxAge = await store.Query<User>().Max(u => u.Age);
var minAge = await store.Query<User>().Min(u => u.Age);
var totalAge = await store.Query<User>().Sum(u => u.Age);
var avgAge = await store.Query<User>().Average(u => u.Age);

// With predicate filter
var maxAge = await store.Query<User>()
    .Where(u => u.Age < 35)
    .Max(u => u.Age);
```

## Aggregate Projections (GROUP BY)

Use `Sql` marker class for aggregate projections with automatic GROUP BY via `.Select()`.

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

## Streaming

Use `.ToAsyncEnumerable()` instead of `.ToList()` to stream results one-at-a-time without buffering.

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

**Note:** Streaming methods hold the internal semaphore for the duration of enumeration. Consume results promptly and avoid interleaving other store operations within the same `await foreach` loop.

## Index Management

Methods on `DocumentStore` directly (not on `IDocumentStore`) since indexes are DDL, not document CRUD. Each provider generates the appropriate index DDL for its database engine.

### Create an Index

```csharp
await store.CreateIndexAsync<User>(u => u.Name, ctx.User);
// CREATE INDEX IF NOT EXISTS idx_json_User_name
// ON documents (json_extract(Data, '$.name'))
// WHERE TypeName = 'User';
```

### Nested Property Index

```csharp
await store.CreateIndexAsync<Order>(o => o.ShippingAddress.City, ctx.Order);
```

### Drop a Specific Index

```csharp
await store.DropIndexAsync<User>(u => u.Name, ctx.User);
```

### Drop All Indexes for a Type

```csharp
await store.DropAllIndexesAsync<User>();
```

Index names are deterministic (`idx_json_{typeName}_{jsonPath}`). `CreateIndexAsync` uses `IF NOT EXISTS`, so calling it multiple times is safe.

## Transactions

```csharp
await store.RunInTransaction(async tx =>
{
    await tx.Insert(new User { Id = "u1", Name = "Alice", Age = 25 });
    await tx.Insert(new User { Id = "u2", Name = "Bob", Age = 30 });
    // Commits on success, rolls back on exception
});
```

The `tx` parameter is an `IDocumentStore` scoped to the transaction. All operations within the callback share the same database transaction.

## AI Tool Integration (Shiny.DocumentDb.Extensions.AI)

Expose `IDocumentStore` operations as `Microsoft.Extensions.AI` tool functions for LLM agents.

### NuGet Package

```bash
dotnet add package Shiny.DocumentDb.Extensions.AI
```

### Registration

```csharp
using Shiny.DocumentDb.Extensions.AI;

services.AddDocumentStoreAITools(tools =>
{
    tools.AddType(
        jsonContext.Customer,
        capabilities: DocumentAICapabilities.All,
        configure: b => b
            .Description("Customer records with contact info")
            .Property(c => c.Status, "Active, Inactive, or Suspended")
            .IgnoreProperties(c => c.PasswordHash)
            .MaxPageSize(50)
    );

    tools.AddType(
        jsonContext.Order,
        capabilities: DocumentAICapabilities.ReadOnly
    );
});
```

### DocumentAICapabilities Flags

| Flag | Tool Name Pattern | Description |
|------|------------------|-------------|
| `Get` | `{slug}_get_by_id` | Fetch a single document by ID |
| `Query` | `{slug}_query` | Query with structured filter, sort, paging |
| `Count` | `{slug}_count` | Count with optional filter |
| `Aggregate` | `{slug}_aggregate` | sum/min/max/avg/count |
| `Insert` | `{slug}_insert` | Create a new document |
| `Update` | `{slug}_update` | Replace an existing document |
| `Delete` | `{slug}_delete` | Delete by ID |
| `ReadOnly` | — | Get + Query + Count + Aggregate |
| `All` | — | All seven operations |

### Per-Type Builder (IDocumentAITypeBuilder<T>)

| Method | Description |
|--------|-------------|
| `Description(string)` | Type-level description in tool descriptions and schema |
| `Property<TProp>(expr, string)` | Override description for a specific property |
| `AllowProperties(params exprs)` | Only expose listed properties (allowlist) |
| `IgnoreProperties(params exprs)` | Hide listed properties (blocklist) |
| `MaxPageSize(int)` | Cap maximum page size for query/aggregate (default 100) |

### Using the Tools

```csharp
var aiTools = serviceProvider.GetRequiredService<DocumentStoreAITools>();
var options = new ChatOptions { Tools = aiTools.Tools.ToList() };
var response = await chatClient.GetResponseAsync(messages, options);
```

### Structured Filter Format

The query, count, and aggregate tools accept a `filter` JSON object:

```json
// Leaf comparison
{ "field": "age", "op": "gt", "value": 30 }

// Boolean combinators
{ "and": [{ "field": "age", "op": "gte", "value": 18 }, { "field": "status", "op": "eq", "value": "Active" }] }
{ "or": [{ "field": "city", "op": "eq", "value": "Portland" }, { "field": "city", "op": "eq", "value": "Seattle" }] }
{ "not": { "field": "status", "op": "eq", "value": "Cancelled" } }
```

Supported operators: `eq`, `ne`, `gt`, `gte`, `lt`, `lte`, `contains`, `startsWith`, `in`.

### Query Tool Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `filter` | object | — | Structured filter (optional) |
| `orderBy` | string | — | Field name to sort by (optional) |
| `orderDirection` | string | `"asc"` | `"asc"` or `"desc"` |
| `limit` | integer | 50 | Max results (capped at MaxPageSize) |
| `offset` | integer | 0 | Results to skip |

### Aggregate Tool Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `function` | string | `"count"`, `"sum"`, `"min"`, `"max"`, or `"avg"` |
| `field` | string | Numeric field (required for sum/min/max/avg) |
| `filter` | object | Structured filter (optional) |

## Code Generation Best Practices

1. **Configure `JsonSerializerContext` once** — set `DocumentStoreOptions.JsonSerializerOptions = ctx.Options` so all `JsonTypeInfo<T>` parameters auto-resolve. No need to pass them on every call.
2. **Set `UseReflectionFallback = false` for AOT** — get clear `InvalidOperationException` instead of opaque AOT failures for unregistered types.
3. **Derive from `JsonSerializerContext`** — add `[JsonSerializable(typeof(T))]` for each type; do NOT add `[JsonSerializerContext]` attribute.
4. **Include projection and aggregate result types** in the JSON context — if using `.Select(u => new UserSummary { ... })`, register `UserSummary`.
5. **Use the fluent query builder** — `store.Query<T>().Where(...).OrderBy(...).Paginate(...).ToList()` is the primary query pattern.
6. **Use streaming for large result sets** — prefer `.ToAsyncEnumerable()` over `.ToList()` when processing results incrementally.
7. **Create indexes for frequently queried properties** — `store.CreateIndexAsync<T>(expr, jsonTypeInfo)` for up to 30x faster queries.
8. **Use `Dictionary<string, object?>` for AOT-safe raw SQL parameters** — anonymous objects work but dictionaries are fully AOT-compatible.
9. **Keep index management separate** — index methods are on `DocumentStore`, not `IDocumentStore`; cast or use the concrete type.
10. **Use `MapTypeToTable` for isolation** — when types have different lifecycles or access patterns, give them dedicated tables.
11. **Custom Id requires table mapping** — there is no overload for custom Id without `MapTypeToTable`. This is by design.
12. **DI registration uses the extensions package** — install `Shiny.DocumentDb.Extensions.DependencyInjection` and call `services.AddDocumentStore(opts => { opts.DatabaseProvider = ...; })`. There are no provider-specific DI methods.
13. **Raw SQL is provider-specific** — LINQ expressions work identically across all providers, but raw SQL queries (`store.Query<T>("sql")`) use provider-specific JSON functions. Prefer the fluent query builder for portable code.
14. **Spatial queries require `MapSpatialProperty`** — call `options.MapSpatialProperty<T>(x => x.Location)` at setup to register which `GeoPoint` property drives spatial indexing. Only SQLite and CosmosDB support spatial; other providers throw `NotSupportedException`.
15. **Backup is on concrete types, not `IDocumentStore`** — use `SqliteDocumentStore.Backup()`, `SqlCipherDocumentStore.Backup()`, or `LiteDbDocumentStore.Backup()` directly. Cast or store the concrete type.
16. **`ClearAllAsync` is SQLite-only** — available on `SqliteDocumentStore` only, deletes all documents across all tables including spatial sidecar data.
