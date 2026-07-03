--
name: shiny-documentdb
description: Generate code using Shiny.DocumentDb, a schema-free multi-provider JSON document store for .NET supporting SQLite, LiteDB, CosmosDB, MongoDB, Azure Table Storage, Amazon DynamoDB, DuckDB, IndexedDB (Blazor WASM), MySQL, SQL Server, PostgreSQL, and Oracle with LINQ queries, spatial/geo queries, and AOT support
auto_invoke: true
triggers:
  - document store
  - document db
  - DocumentStore
  - SqliteDocumentStore
  - IDocumentStore
  - IDocumentQuery
  - ToQueryString
  - DocumentQueryString
  - WhereIn
  - WhereNotIn
  - NullHandling
  - DocumentFunctions
  - Soundex
  - HasFlag
  - flag enum query
  - bitwise enum
  - scalar function
  - vector search
  - NearestVectors
  - MapVectorProperty
  - full-text search
  - FullTextSearch
  - FullTextMatch
  - MapFullTextProperty
  - FullTextResult
  - FullTextLanguage
  - FTS5
  - tsvector
  - computed property
  - computed column
  - MapComputedProperty
  - derived property
  - generated column
  - DocumentContext
  - DocumentSet
  - Document attribute
  - typed context
  - AddDocumentContext
  - IDocumentContextFactory
  - DocumentContextFactory
  - Shiny.DocumentDb.Generators
  - DocumentSerialization
  - sqlite-vec
  - VectorExtensionPreloaded
  - EnableVectorExtension
  - Shiny.DocumentDb.Sqlite.VectorSupport
  - SqliteVec
  - RegisterAutoExtension
  - vector search ios
  - vector search android
  - ToLower query
  - Math.Abs query
  - IDatabaseProvider
  - json document
  - JsonNode
  - late-bound
  - Insert(Type
  - Query(Type
  - Insert by Type
  - JsonNode write
  - raw json write
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
  - OracleDatabaseProvider
  - Shiny.DocumentDb.Oracle
  - oracle
  - json_extract
  - document query
  - fluent query
  - paginate
  - PageResult
  - PagedResults
  - paged results
  - dynamic sort
  - sort by string
  - OrderBy string
  - Where string
  - dynamic filter
  - interpolated filter
  - FilterInterpolatedStringHandler
  - MapTypeToTable
  - table per type
  - GetDiff
  - JsonPatchDocument
  - UnitOfWork
  - CreateUnitOfWork
  - SaveChanges
  - unit of work
  - transaction
  - atomic writes
  - IDocumentInterceptor
  - IDocumentBulkInterceptor
  - OnBeforeWrite
  - OnAfterWrite
  - interceptor
  - write interceptor
  - document diff
  - BatchInsert
  - batch insert
  - BatchUpsert
  - BatchUpdate
  - BatchRemove
  - batch upsert
  - batch update
  - batch remove
  - LiteDbDocumentStore
  - LiteDbDocumentStoreOptions
  - Shiny.DocumentDb.LiteDb
  - litedb
  - CosmosDbDocumentStore
  - CosmosDbDocumentStoreOptions
  - Shiny.DocumentDb.CosmosDb
  - cosmosdb
  - cosmos db
  - MongoDbDocumentStore
  - MongoDbDocumentStoreOptions
  - Shiny.DocumentDb.MongoDb
  - mongodb
  - mongo db
  - AzureTableDocumentStore
  - AzureTableDocumentStoreOptions
  - Shiny.DocumentDb.AzureTable
  - AddAzureTableDocumentStore
  - AzureTable
  - Azure Table
  - TableStorage
  - Table Storage
  - Cosmos Table API
  - DynamoDbDocumentStore
  - DynamoDbDocumentStoreOptions
  - Shiny.DocumentDb.DynamoDb
  - AddDynamoDbDocumentStore
  - DynamoDb
  - DynamoDB
  - AWS
  - DynamoDB Streams
  - MapIndexedProperty
  - promoted column
  - PartiQL
  - MapTypeToCollection
  - DuckDbDatabaseProvider
  - Shiny.DocumentDb.DuckDb
  - duckdb
  - duck db
  - analytical store
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
  - ClearAll
  - IDocumentMaintenance
  - seeding
  - IDocumentSeeder
  - AddDocumentSeeder
  - DocumentSeedRunner
  - DocumentSeedMarker
  - Backup
  - IDocumentBackup
  - ExportAsync
  - RestoreAsync
  - BulkImportAsync
  - BulkWriteMode
  - RawDocument
  - BulkRestoreOptions
  - BackupExportOptions
  - BulkRestoreResult
  - BulkProgress
  - bulk export
  - bulk import
  - bulk restore
  - backup
  - restore
  - export store
  - import documents
  - IndexedDbDocumentStore
  - IndexedDbDocumentStoreOptions
  - Shiny.DocumentDb.IndexedDb
  - indexeddb
  - indexed db
  - blazor wasm
  - blazor webassembly
  - browser storage
  - MapVersionProperty
  - ConcurrencyException
  - optimistic concurrency
  - row versioning
  - version property
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
  - multi-tenant
  - multi-tenancy
  - tenant
  - ITenantResolver
  - TenantIdAccessor
  - AddMultiTenantDocumentStore
  - tenant per database
  - shared table
  - tenant isolation
  - IObservableDocumentStore
  - IChangeFeedDocumentStore
  - NotifyOnChange
  - WhenDocumentChanged
  - SubscribeChanges
  - DocumentChange
  - DocumentChangeType
  - ChangeBroadcaster
  - change feed
  - change observation
  - change monitoring
  - query monitoring
  - reactive store
  - MapIdProperty
  - MapIdType
  - custom Id type
  - strongly-typed Id
  - DocumentIdConverter
  - UseGuidV7Ids
  - v7 guid
  - sortable guid
  - sequential guid
  - AddQueryFilter
  - IgnoreQueryFilters
  - QueryFilter
  - query filter
  - global query filter
  - HasQueryFilter
  - soft delete
  - row-level security
  - orleans
  - grain storage
  - grain persistence
  - IGrainStorage
  - PubSubStore
  - Shiny.DocumentDb.Orleans
  - AddDocumentDbGrainStorage
  - AddMongoDbGrainStorage
  - AddCosmosDbGrainStorage
  - DocumentDbGrainStorage
  - GrainStateRecord
  - orleans reminders
  - IReminderTable
  - AddDocumentDbReminders
  - orleans membership
  - clustering
  - IMembershipTable
  - AddDocumentDbClustering
  - grain directory
  - IGrainDirectory
  - AddDocumentDbGrainDirectory
  - suppressInterceptors
  - SaveChanges suppressInterceptors
  - side-effect-free write
  - GetJson
  - GetJsonDocument
  - JSON schema
  - JsonSchema
  - json schema validation
  - MapJsonSchema
  - MapJsonSchemaFromFile
  - AddDocumentJsonSchema
  - AddJsonSchemaValidation
  - DocumentSchemaValidationException
  - Shiny.DocumentDb.JsonSchema
  - validate document
  - Shiny.DocumentDb.AppDataSync
  - Shiny.Data.Sync
  - offline-first
  - offline sync
  - SyncDocumentStore
  - data sync
  - outbox
  - Sync<T>
  - AddDataSync
  - ISyncEntity
  - OData
  - Shiny.DocumentDb.OData
  - Shiny.DocumentDb.AspNetCore.OData
  - MapDocumentODataEntitySet
  - AddDocumentODataEndpoints
  - ODataQueryPolicy
  - ConfigureDefaultPolicy
  - odata entity set
  - odata governance
  - $filter
  - Shiny.DocumentDb.Aspire.Hosting
  - Shiny.DocumentDb.Aspire.Client
  - Shiny.DocumentDb.Aspire.Orleans
  - AddPostgresDocumentStore
  - AddSqliteDocumentStore
  - AsDocumentStore
  - UseAspireDocumentDb
  - CreateAITools
---

# Shiny DocumentDb Skill

You are an expert in Shiny.DocumentDb, a lightweight multi-provider document store for .NET that turns relational databases into a schema-free JSON document database with LINQ querying, spatial/geo queries, and full AOT/trimming support. Supports **SQLite**, **SQLCipher** (encrypted SQLite), **LiteDB**, **CosmosDB**, **MongoDB**, **Azure Table Storage** (and Cosmos DB Table API), **Amazon DynamoDB**, **DuckDB**, **IndexedDB** (Blazor WebAssembly), **MySQL**, **SQL Server**, **PostgreSQL**, and **Oracle**.

## When to Use This Skill

Invoke this skill when the user wants to:
- Store and retrieve .NET objects as JSON documents in SQLite, IndexedDB, MySQL, SQL Server, PostgreSQL, or Oracle
- Query JSON documents with LINQ expressions or raw SQL
- Set up a schema-free document database without migrations
- Use AOT-safe document storage with `JsonTypeInfo<T>` overloads
- Stream query results with `IAsyncEnumerable<T>`
- Create JSON property indexes for faster queries
- Project query results into DTOs at the SQL level
- Compute aggregates (Max, Min, Sum, Average) across documents
- Use aggregate projections with GROUP BY via `Sql.*` markers
- Sort query results with expression-based OrderBy/OrderByDescending
- Sort query results by a property name (string) — AOT-safe via `JsonTypeInfo<T>`, supports dotted paths, for dynamic UIs / REST `?sort=` query strings
- Paginate query results with LIMIT/OFFSET
- Return a `PagedResults<T> { Records, TotalCount, Page, PageSize }` envelope from a query in one call via `.PageResult(page, pageSize)`
- Use transactions for atomic document operations
- Work with nested objects and child collections without table design
- Map document types to dedicated tables (table-per-type)
- Use a custom Id property instead of the default `Id`
- Use a custom Id **type** beyond Guid/int/long/string — e.g. `Ulid` or a strongly-typed wrapper (`MapIdType`)
- Diff a modified object against a stored document (`GetDiff`)
- Batch insert / upsert / update / remove many documents efficiently (`BatchInsert`, `BatchUpsert`, `BatchUpdate`, `BatchRemove`)
- Choose between database providers (SQLite, IndexedDB, MySQL, SQL Server, PostgreSQL, Oracle)
- Use IndexedDB for client-side storage in Blazor WebAssembly apps
- Query documents by geographic proximity (within radius, bounding box, nearest neighbors)
- Configure spatial indexing for `GeoPoint` properties (`MapSpatialProperty`)
- Use SQLite R*Tree spatial indexes or CosmosDB native GeoJSON queries
- Use optimistic concurrency with document-level version properties (`MapVersionProperty`)
- Override the document Id property (`MapIdProperty`) without dedicating a table
- Observe in-process document changes as an `IAsyncEnumerable<DocumentChange<T>>` (`IObservableDocumentStore.NotifyOnChange<T>`)
- Watch a single document by Id (`WhenDocumentChanged<T>(id)`)
- Monitor changes filtered by a query's predicates (`store.Query<T>().Where(...).NotifyOnChange()`)
- Consume native database change feeds across writers (`IChangeFeedDocumentStore.SubscribeChanges<T>`)
- Register global query filters (`AddQueryFilter<T>`) — soft-delete, row-level security, "active only" scopes (EF Core's `HasQueryFilter` equivalent)
- Selectively disable filters with `IgnoreQueryFilters()` or `IgnoreQueryFilters("name")` per query
- Set up multi-tenancy with shared-table isolation (single database, `TenantId` column)
- Set up multi-tenancy with tenant-per-database isolation (separate database per tenant)
- Implement `ITenantResolver` for tenant context resolution
- Back up SQLite, SQLCipher, or LiteDB databases to a file (`Backup`)
- Stream a whole store out and back in for backup / restore / migration across providers (`IDocumentBackup.ExportAsync` / `RestoreAsync` / `BulkImportAsync`)
- Wipe the entire store across providers for test/dev resets (`IDocumentMaintenance.ClearAll`)
- Seed initial data once at startup, versioned and provider-agnostic (`IDocumentSeeder` / `AddDocumentSeeder` / `DocumentSeedRunner`)
- Expose document types as AI tools for LLM agents (`AddDocumentStoreAITools`)
- Configure AI tool capabilities per type (ReadOnly, All, or individual flags)
- Control field visibility for LLM access (AllowProperties, IgnoreProperties)
- Use structured filter expressions in AI tool queries
- Persist Microsoft Orleans grain state on any DocumentDb backend (`AddDocumentDbGrainStorage` / `AddMongoDbGrainStorage` / `AddCosmosDbGrainStorage`)

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
  - `Shiny.DocumentDb.Oracle` — Oracle (23ai+) provider + DI extensions
  - `Shiny.DocumentDb.LiteDb` — LiteDB provider + DI extensions
  - `Shiny.DocumentDb.CosmosDb` — Azure Cosmos DB provider + DI extensions
  - `Shiny.DocumentDb.MongoDb` — MongoDB provider + DI extensions
  - `Shiny.DocumentDb.AzureTable` — Azure Table Storage (and Cosmos DB Table API) provider + `AddAzureTableDocumentStore(...)`
  - `Shiny.DocumentDb.DynamoDb` — Amazon DynamoDB provider + `AddDynamoDbDocumentStore(...)`
  - `Shiny.DocumentDb.DuckDb` — DuckDB (embedded analytical) provider + DI extensions
  - `Shiny.DocumentDb.IndexedDb` — IndexedDB provider for Blazor WebAssembly + DI extensions
  - `Shiny.DocumentDb.Extensions.DependencyInjection` — generic (provider-agnostic) DI extensions
  - `Shiny.DocumentDb.Extensions.AI` — Microsoft.Extensions.AI tool surface (AIFunction tools for LLM agents)
  - `Shiny.DocumentDb.Diagnostics` — OpenTelemetry metrics + tracing (instrumentation decorator over any provider)
  - `Shiny.DocumentDb.Orleans` — Microsoft Orleans grain storage (`IGrainStorage` + `PubSubStore`) over any `IDocumentStore` backend
  - `Shiny.DocumentDb.Orleans.MongoDb` / `Shiny.DocumentDb.Orleans.CosmosDb` — first-class Orleans grain-storage registration for MongoDB / Cosmos DB
- **Provider dependencies**:
  - SQLite: `Microsoft.Data.Sqlite`
  - SQLCipher: `Microsoft.Data.Sqlite.Core` + `SQLitePCLRaw.bundle_e_sqlcipher`
  - MySQL: `MySqlConnector`
  - SQL Server: `Microsoft.Data.SqlClient`
  - PostgreSQL: `Npgsql`
  - Oracle: `Oracle.ManagedDataAccess.Core` (requires Oracle Database 23ai+)
  - LiteDB: `LiteDB`
  - CosmosDB: `Microsoft.Azure.Cosmos`
  - MongoDB: `MongoDB.Driver`
  - DuckDB: `DuckDB.NET.Data.Full`
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

// Azure Table Storage (and Cosmos DB Table API)
using Shiny.DocumentDb.AzureTable;
var store = new AzureTableDocumentStore(new AzureTableDocumentStoreOptions
{
    ConnectionString = "UseDevelopmentStorage=true", // or a real account / SAS; or ServiceUri + TokenCredential/DefaultAzureCredential
    TableName = "Documents"                          // one table; PartitionKey=typeName, RowKey=id
});

// Amazon DynamoDB
using Shiny.DocumentDb.DynamoDb;
var store = new DynamoDbDocumentStore(new DynamoDbDocumentStoreOptions
{
    TableName = "Documents",  // one table; pk=typeName (HASH), sk=id (RANGE)
    Region = Amazon.RegionEndpoint.USEast1, // or ServiceUrl = "http://localhost:8000" for DynamoDB Local
    AutoCreateTable = true    // dev convenience; off by default
});

// DuckDB (embedded analytical store)
using Shiny.DocumentDb.DuckDb;
var store = new DocumentStore(new DocumentStoreOptions
{
    DatabaseProvider = new DuckDbDatabaseProvider("Data Source=mydata.duckdb")
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

> **Note:** LiteDB, CosmosDB, MongoDB, and IndexedDB have their own store and options types. Register them directly with the DI container (e.g. `services.AddSingleton<IDocumentStore, MongoDbDocumentStore>()`). DuckDB uses the standard `DocumentStoreOptions` / `IDatabaseProvider` pipeline like SQLite / PostgreSQL / SQL Server / MySQL / Oracle.

Azure Table and DynamoDB ship their own DI extensions (they register `IDocumentStore` + `IDocumentMaintenance` as singletons):

```csharp
using Shiny.DocumentDb;

// Azure Table Storage (or Cosmos DB Table API)
services.AddAzureTableDocumentStore(o =>
{
    o.ConnectionString = "UseDevelopmentStorage=true"; // or ServiceUri + TokenCredential/SharedKeyCredential/SasCredential
    o.TableName        = "Documents";
    o.MapVersionProperty<Order>(x => x.Version);        // opt-in optimistic concurrency (ETag-backed)
});

// Amazon DynamoDB
services.AddDynamoDbDocumentStore(o =>
{
    o.TableName       = "Documents";
    o.Region          = Amazon.RegionEndpoint.USEast1; // or o.ServiceUrl for DynamoDB Local; o.Credentials for explicit creds
    o.AutoCreateTable = true;                          // dev convenience; off by default
    o.ConsistentRead  = false;                         // default eventual consistency
    o.MapVersionProperty<Order>(x => x.Version);
});
```

**Azure Table & DynamoDB — key facts (both are NoSQL key-partitioned stores):**
- **Key model:** the library's `(typeName, id)` identity maps to `PartitionKey/RowKey` (Table) or `pk/sk` HASH/RANGE (DynamoDB). One table holds every type; `Query<T>()` is always a single-partition scan.
- **Queries are client-side:** `Where`/`OrderBy`/`Paginate`/`Select`/aggregates evaluate in memory (the LiteDB `ExpressionInterpreter` model) after loading the type's partition. This is a **full type scan** — fine for modest per-type sets, plan hot paths accordingly.
- **Promoted columns for server-side pushdown:** `MapIndexedProperty<T>(x => x.Status)` writes the scalar as a native top-level column/attribute. LINQ predicates over a promoted property push down into a server-side filter (Azure Table OData `$filter`, DynamoDB `FilterExpression`) to shrink the candidate set; the full predicate still re-runs client-side so results are exact. The string `Query(whereClause)` / `QueryStream` / `Count(whereClause)` overloads and `ToQueryString()` are **supported** and target promoted columns — Azure Table takes a raw **OData** `$filter` fragment, DynamoDB a **PartiQL** `WHERE` condition (reference promoted props by their CLR/JSON name). `Project(string)` is not supported.
- **Change observation:** both implement `IObservableDocumentStore.NotifyOnChange<T>` (in-process, this instance's writes) and `Query<T>().NotifyOnChange()`. **DynamoDB also** implements `IChangeFeedDocumentStore.SubscribeChanges<T>` backed by **DynamoDB Streams** (any-writer, stream auto-enabled on table create).
- **Int/Long Id auto-generation is unsupported** — a default Int/Long Id on Insert throws `NotSupportedException` (no cheap `MAX`); use Guid or string Ids, or assign the Int/Long Id explicitly. Guid/string auto-gen works.
- **Optimistic concurrency:** `MapVersionProperty<T>` uses the Table `ETag` (If-Match) or a DynamoDB conditional write on a top-level `Version` attribute → `ConcurrencyException` on conflict. Blind (unversioned) upsert is last-write-wins.
- **Not supported:** spatial, vector, full-text, temporal (`SupportsSpatial`/`SupportsVector`/`SupportsFullText` stay `false`; no `ITemporalDocumentStore`). `IDocumentMaintenance.ClearAll` **is** supported.
- **Size limits:** Azure Table caps the JSON body at ~64 KB (per-property) and DynamoDB caps an item at 400 KB — an oversized document throws a clear `NotSupportedException`, not a raw storage error.
- **Native batch:** `BatchInsert`/`BatchRemove` use native transactions/bulk writes in bounded waves (≤100 per PartitionKey on Table, ≤25 per request on DynamoDB). `CreateUnitOfWork()` is a compensating tracker (no cross-partition atomicity), matching Cosmos.

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

### Multi-Tenancy

Two isolation strategies are supported via `Shiny.DocumentDb.Extensions.DependencyInjection`. Both use a user-implemented `ITenantResolver` to identify the current tenant.

#### ITenantResolver Interface

```csharp
namespace Shiny.DocumentDb;

public interface ITenantResolver
{
    string GetCurrentTenant();
}

// Example implementation
public class HttpContextTenantResolver(IHttpContextAccessor http) : ITenantResolver
{
    public string GetCurrentTenant()
        => http.HttpContext?.User.FindFirst("tenant_id")?.Value
           ?? throw new InvalidOperationException("No tenant context");
}
```

#### Shared-Table Multi-Tenancy (single database, TenantId column)

All tenants share one database. A dedicated `TenantId` column and index are added automatically. All queries are filtered by the current tenant transparently.

```csharp
services.AddSingleton<ITenantResolver, HttpContextTenantResolver>();

services.AddDocumentStore(opts =>
{
    opts.DatabaseProvider = new PostgreSqlDatabaseProvider("Host=...");
}, multiTenant: true);

// Named/keyed shared-table store — same multiTenant flag, resolve with [FromKeyedServices("orders")]:
services.AddDocumentStore("orders", opts =>
{
    opts.DatabaseProvider = new PostgreSqlDatabaseProvider("Host=...");
}, multiTenant: true);

// Consumer code is unchanged — tenant filter applied automatically
public class OrderService(IDocumentStore store)
{
    public Task<IReadOnlyList<Order>> GetOrders()
        => store.Query<Order>().ToList(); // only returns current tenant's orders
}
```

#### Tenant-Per-Database (separate database per tenant)

Each tenant gets a lazily-created separate database. `IDocumentStore` is registered as **scoped** and resolves to the correct tenant's store per request.

```csharp
services.AddSingleton<ITenantResolver, HttpContextTenantResolver>();

services.AddMultiTenantDocumentStore(tenantId => new DocumentStoreOptions
{
    DatabaseProvider = new SqliteDatabaseProvider($"Data Source={tenantId}.db")
});

// Same consumer code — correct database selected automatically
public class OrderService(IDocumentStore store) { ... }
```

#### Direct Usage (without DI)

Set `TenantIdAccessor` on `DocumentStoreOptions` for the shared-table model:

```csharp
var store = new DocumentStore(new DocumentStoreOptions
{
    DatabaseProvider = new SqliteDatabaseProvider("Data Source=mydata.db"),
    TenantIdAccessor = () => GetCurrentTenantId()  // your tenant resolution logic
});
```

### DocumentStoreOptions

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `DatabaseProvider` | `IDatabaseProvider` (required) | — | The database provider (`SqliteDatabaseProvider`, `SqlCipherDatabaseProvider`, `MySqlDatabaseProvider`, `SqlServerDatabaseProvider`, `PostgreSqlDatabaseProvider`, `OracleDatabaseProvider`, `DuckDbDatabaseProvider`) |
| `TableName` | `string` | `"documents"` | Default table name for all document types not mapped via `MapTypeToTable` |
| `TypeNameResolution` | `TypeNameResolution` | `ShortName` | How type names are stored (`ShortName` or `FullName`) |
| `JsonSerializerOptions` | `JsonSerializerOptions?` | `null` | JSON serialization settings. When a `JsonSerializerContext` is attached as the `TypeInfoResolver`, all methods auto-resolve type info from the context |
| `UseReflectionFallback` | `bool` | `true` | When `false`, throws `InvalidOperationException` if a type can't be resolved from the configured `TypeInfoResolver` instead of falling back to reflection. Recommended for AOT deployments |
| `Logging` | `Action<string>?` | `null` | Callback invoked with every SQL statement executed |
| `TenantIdAccessor` | `Func<string>?` | `null` | When set, enables shared-table multi-tenancy. All queries are filtered by TenantId and all inserts include the TenantId value. A dedicated TenantId column and index are created automatically |

## Optimistic Concurrency (Row Versioning)

Map a version property on your document type for automatic optimistic concurrency. The version is stored inside the JSON blob — no schema changes required. Works across all providers.

### Configuration

```csharp
// Expression-based
var store = new DocumentStore(new DocumentStoreOptions
{
    DatabaseProvider = new SqliteDatabaseProvider("Data Source=mydata.db")
}.MapVersionProperty<Order>(o => o.RowVersion));

// AOT-safe overload
.MapVersionProperty<Order>("RowVersion", o => o.RowVersion, (o, v) => o.RowVersion = v)
```

All provider options classes support `MapVersionProperty`: `DocumentStoreOptions` (covers SQLite/SQLCipher/PostgreSQL/SQL Server/MySQL/Oracle/DuckDB), `LiteDbDocumentStoreOptions`, `CosmosDbDocumentStoreOptions`, `MongoDbDocumentStoreOptions`, and `IndexedDbDocumentStoreOptions`.

### Behavior

| Operation | Behavior |
|---|---|
| `Insert` | Version set to **1** before serialization |
| `Update` | Checks expected version against stored version, increments on success. Throws `ConcurrencyException` on mismatch |
| `Upsert` | Insert path sets version to 1. Update path checks and increments |
| `BatchInsert` | Version set to 1 for each document |

### Example

```csharp
public class Order
{
    public string Id { get; set; } = "";
    public string Status { get; set; } = "";
    public int RowVersion { get; set; }
}

var order = new Order { Id = "ord-1", Status = "Pending" };
await store.Insert(order);
// order.RowVersion == 1

order.Status = "Shipped";
await store.Update(order);
// order.RowVersion == 2

// Stale update throws ConcurrencyException
var stale = new Order { Id = "ord-1", Status = "Cancelled", RowVersion = 1 };
await store.Update(stale); // throws ConcurrencyException
```

### ConcurrencyException

| Property | Type | Description |
|---|---|---|
| `TypeName` | `string` | Document type name |
| `DocumentId` | `string` | Document Id |
| `ExpectedVersion` | `int` | Version the caller expected |
| `ActualVersion` | `int?` | Version found in the store |

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

By default every document type must have a property named `Id`. Override that with a custom property — by Guid, int, long, or string — using either `MapTypeToTable<T>(...)` (when combined with a dedicated table) or `MapIdProperty<T>(...)` (when the type stays in the default shared table). The two are independent: you can use both, either, or neither.

```csharp
var store = new DocumentStore(new DocumentStoreOptions
{
    DatabaseProvider = new SqliteDatabaseProvider("Data Source=mydata.db")
}
// Dedicated table + custom Id
.MapTypeToTable<Sensor>("sensors", s => s.DeviceKey)      // Guid DeviceKey as Id
.MapTypeToTable<Tenant>("tenants", t => t.TenantCode)     // string TenantCode as Id
// Default shared table + custom Id
.MapIdProperty<BlogPost>(p => p.Slug)                     // string Slug as Id
);
```

### Custom Id types (MapIdType)

Document Ids are not limited to `Guid`/`int`/`long`/`string`. Register a converter with `MapIdType` to use any CLR type — a `Ulid` or a strongly-typed wrapper like `record struct OrderId(Guid Value)`. The Id is still stored as a string in every provider (no schema/on-disk change); the converter defines how it round-trips. Purely additive — built-in types need no registration and are unchanged.

```csharp
public readonly record struct OrderId(Guid Value)
{
    public static OrderId New() => new(Guid.NewGuid());
}

options.MapIdType(
    toString:  (OrderId id) => id.Value.ToString("N"),
    parse:     s => new OrderId(Guid.ParseExact(s, "N")),
    isDefault: id => id.Value == Guid.Empty,   // when to auto-generate on Insert
    generate:  OrderId.New);                   // optional; omit to require explicit Ids

// or a reusable class:
public sealed class OrderIdConverter : DocumentIdConverter<OrderId>
{
    public override string  ToStorageString(OrderId id) => id.Value.ToString("N");
    public override OrderId FromStorageString(string s)  => new(Guid.ParseExact(s, "N"));
    public override bool    IsDefault(OrderId id) => id.Value == Guid.Empty;
    public override bool    TryGenerate(out OrderId id) { id = OrderId.New(); return true; }
}
options.MapIdType(new OrderIdConverter());
```

- `Insert`/`Get`/`Update`/`Remove`/`Upsert` all accept the strongly-typed Id.
- Available on every provider's options class (Cosmos/Mongo/LiteDb/IndexedDb too).
- **Sortable Guid Ids**: `options.UseGuidV7Ids()` auto-generates time-ordered **version 7** GUIDs (`Guid.CreateVersion7()`) instead of random v4 — BCL only, storage format unchanged, drop-in for existing data. (`long` is already a built-in for sequential integer keys.)
- The Id also lives in the JSON `Data` blob — give the type a matching `System.Text.Json` converter so LINQ predicates on the Id (`Where(x => x.Id == value)`) line up with the stored string.
- A converter with no `generate`/`TryGenerate` throws `InvalidOperationException` on a default-Id Insert (assign explicitly).

### MapTypeToTable and MapIdProperty overloads

| Overload | Description |
|----------|-------------|
| `MapTypeToTable<T>()` | Auto-derive table name from type name |
| `MapTypeToTable<T>(string tableName)` | Explicit table name |
| `MapTypeToTable<T>(Expression<Func<T, object>> idProperty)` | Auto-derive table + custom Id |
| `MapTypeToTable<T>(string tableName, Expression<Func<T, object>> idProperty)` | Explicit table + custom Id |
| `MapIdProperty<T>(Expression<Func<T, object>> idProperty)` | Custom Id property only — type stays in the default shared table |
| `MapIdProperty<T>(string propertyName)` | AOT-safe string overload |
| `MapIdType<TId>(DocumentIdConverter<TId>)` | Register a custom Id **type** (converter instance) |
| `MapIdType<TId>(toString, parse, isDefault?, generate?)` | Register a custom Id **type** (inline delegates) |

All overloads return the options instance for fluent chaining. Duplicate table names throw `InvalidOperationException`.

## Strongly-Typed Context (DocumentContext)

Optional EF-Core-style typed front-end over `IDocumentStore`. Requires the **`Shiny.DocumentDb.Generators`**
analyzer package (`DocumentContext`/`DocumentSet<T>` are in core). Declare aggregates once on a `partial`
context; the generator emits a `DocumentSet<T>` per type, a `ConfigureModel` lowering, and two DI extensions:
`Add<Context>` (scoped context) and `Add<Context>Factory` (singleton `IDocumentContextFactory<Context>`).
`JsonTypeInfo<T>` is threaded automatically — never pass it from a set call.

```csharp
[JsonSerializable(typeof(User))]
[JsonSerializable(typeof(Order))]
public partial class AppJsonContext : JsonSerializerContext;

[Document(typeof(User),  Id = nameof(User.Email), JsonContext = typeof(AppJsonContext))]
[Document(typeof(Order), Table = "orders",        JsonContext = typeof(AppJsonContext))]
public partial class AppContext : DocumentContext;   // generator fills in the rest (incl. ctor)

// register, ASP.NET Core (scoped — has a request scope)
builder.Services.AddAppContext(o =>
{
    o.DatabaseProvider = new SqliteDatabaseProvider("Data Source=app.db");
    o.UseReflectionFallback = false;   // strict AOT
});

// use — injected scoped, like a DbContext
public class UserService(AppContext db)
{
    public Task<IReadOnlyList<User>> Adults() => db.Users.Where(u => u.Age >= 18).ToList();
    public Task Add(User u) => db.Users.Insert(u);
}

// register, MAUI / Blazor / desktop (no ambient scope) — singleton factory, like EF's IDbContextFactory<T>
builder.Services.AddAppContextFactory(o => o.DatabaseProvider = new SqliteDatabaseProvider($"Data Source={path}"));

public class UserViewModel(IDocumentContextFactory<AppContext> factory)
{
    public Task<IReadOnlyList<User>> Adults()
    {
        var db = factory.Create();   // cheap facade over the shared store; no disposal needed
        return db.Users.Where(u => u.Age >= 18).ToList();
    }
}
```

Rules / guidance:
- The context **must** be `partial` and derive from `DocumentContext` (else `DDB001`/`DDB002`).
- **Registration / lifetime**: `Add<Context>` registers the context **scoped** — use it where there's a
  request scope (ASP.NET Core). `Add<Context>Factory` registers a singleton
  `IDocumentContextFactory<Context>`; inject it anywhere (even into singletons) and call `Create()` per
  operation — use it in **MAUI / Blazor / desktop / background services** (no ambient scope). Created
  contexts are cheap (a facade over the shared, thread-safe store) and need no disposal. Each context binds
  to its **own** store keyed by context type, so multiple contexts coexist in one container; the first
  registered is also the default un-keyed `IDocumentStore`.
- `[Document]` properties: `Table`, `Id` (string property name), `Set` (override the generated set name —
  default is pluralized type name; needed for collisions → `DDB003`), `JsonContext`, `Serialization`.
- **Serialization modes** (`DocumentSerialization`): `JsonContext` (point at your `JsonSerializerContext` —
  recommended, AOT-safe), `Auto` (inherit store resolver, else reflection fallback), `Reflection` (explicit
  non-AOT opt-out), `Generated` (the generator emits the metadata-mode `JsonTypeInfo` itself — AOT-safe, no
  `JsonSerializerContext`; supports POCOs with a parameterless ctor + settable props of primitives, enums,
  nullable value types, nested objects, `List<T>`, arrays — anything else raises `DDB005`, use `JsonContext`).
- **Sets are immediate** (`Insert`/`Update`/`Upsert`/`Remove(id)`/`BatchInsert`/…) and queries return the
  store's `IDocumentQuery<T>` as-is (`Query()`/`Where(...)` → full query surface). Transactions via
  `db.CreateUnitOfWork()`. **No** change tracking, identity map, or navigation/`Include`.
- Works over **any** provider (only needs `IDocumentStore`). The generated `ConfigureModel`/`Add<Context>`
  target the relational `DocumentStoreOptions`; for LiteDB/MongoDB/Cosmos build that store yourself and pass
  it: `new AppContext(liteDbStore)`.

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

## Scalar functions in `Where` predicates

The relational providers (SQLite, SQLCipher, DuckDB, MySQL, SQL Server, PostgreSQL, Oracle) translate these to native SQL. LiteDB/IndexedDB evaluate them in-memory, so they always work there too.

- **String**: `s.ToLower()`/`ToUpper()`, `s.Length`, `s.Trim()`/`TrimStart()`/`TrimEnd()`, `s.Substring(start[, len])`, `s.Replace(a, b)`, `s.IndexOf(x)`, `string.IsNullOrEmpty(s)`, `a + b`, plus the existing `Contains`/`StartsWith`/`EndsWith`.
- **Math**: `Math.Abs/Round/Ceiling/Floor/Sqrt/Pow/Sign`. (`Ceiling`/`Floor`/`Sqrt`/`Pow` need the SQLite math extension; `Abs`/`Round` are always available.)
- **Flag enums** (stored numerically — the default): `x.Permissions.HasFlag(Permissions.Write)` or `(x.Permissions & Permissions.Write) == Permissions.Write`. Both lower to the same bitwise test (`BITAND` on Oracle) on the relational providers and Cosmos; MongoDB translates `HasFlag` to `$bitsAllSet`. Do **not** enable `JsonStringEnumConverter` if you need to query flags — bitwise tests require the numeric representation.
- **Phonetic**: `DocumentFunctions.Soundex(x.Name)` → native `SOUNDEX()` (SQL Server/MySQL/Oracle) or a registered connection UDF (SQLite). Not translatable on Cosmos/Mongo — compute a stored Soundex field there instead.

```csharp
await store.Query<Account>().Where(a => a.Name.ToLower() == "alice").ToList();
await store.Query<Account>().Where(a => a.Permissions.HasFlag(Permissions.Write)).ToList();
await store.Query<Account>().Where(a => DocumentFunctions.Soundex(a.Name) == DocumentFunctions.Soundex("Smith")).ToList();
```

## Computed Properties (MapComputedProperty)

A computed property is a value derived from other fields that is **not stored in the document JSON** but can be filtered, sorted, and projected by exactly like a stored property. Expose it as a `[JsonIgnore]` property (with a setter) and map it — the first expression is the property it backs, the second is the definition:

```csharp
public class Order
{
    public int     Quantity  { get; set; }
    public decimal UnitPrice { get; set; }
    public string  First     { get; set; } = "";
    public string  Last      { get; set; } = "";

    [JsonIgnore] public decimal Total    { get; set; }
    [JsonIgnore] public string  FullName { get; set; } = "";
}

opts.MapComputedProperty<Order, decimal>(o => o.Total,    o => o.Quantity * o.UnitPrice);
opts.MapComputedProperty<Order, string>(o => o.FullName,  o => o.First + " " + o.Last);
```

Reference it by name in typed LINQ, the string API, projection, and OData; it is also populated on read:

```csharp
await store.Query<Order>().Where(o => o.Total > 100).OrderByDescending(o => o.Total).ToList();
await store.Query<Order>().Where("total > 100").OrderBy("fullName").ToList();   // string API
await store.Query<Order>().Project("fullName as name, total").ToList();          // projection
// OData: $filter=total gt 100, $orderby=fullName, $select=total
```

- **Definitions** support JSON field access, string concatenation, the scalar functions, and numeric arithmetic (`+ - * /`).
- **Default (alias) mode** inlines the definition into each query — no schema change, every relational provider.
- **`indexed: true`** materializes a native generated/computed column + index on the relational providers (`VIRTUAL` on SQLite/MySQL, `STORED` on PostgreSQL, `PERSISTED` on SQL Server, virtual on Oracle; DuckDB uses alias mode — it can't add a generated column via `ALTER`) so filters/sorts are index-served:
  ```csharp
  opts.MapComputedProperty<Order, decimal>(o => o.Total, o => o.Quantity * o.UnitPrice, indexed: true);
  ```
- **LiteDB / IndexedDB** evaluate it in memory (full filter/sort/project/read-back). **MongoDB / Cosmos** support read-back and projection, but **not** server-side filter/sort by a computed property — filter on the underlying stored fields there.
- **AOT**: fully trim/AOT-safe (never compiled). For a pristine surface use the AOT overload with an explicit setter: `MapComputedProperty<Order, decimal>("Total", o => o.Quantity * o.UnitPrice, setter: (o, v) => o.Total = v)`.
- The backing property must be writable; a self-referential definition throws.

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
```

### Batch upsert / update / remove

`BatchUpsert`, `BatchUpdate`, and `BatchRemove` apply many writes as one set operation. They are
all-or-nothing: on a versioned type the first version conflict throws `ConcurrencyException` and the
whole batch rolls back. The fast path varies by provider — a single multi-row `INSERT … ON CONFLICT`
deep-merge on SQLite/DuckDB, one `BulkWrite`/`DeleteMany` on MongoDB, parallel request waves on Cosmos,
and a single `DELETE … IN (…)` for `BatchRemove` on every relational provider. Versioned, temporal,
spatial, vector, multi-tenant, filtered, or interceptor-bound types fall back to a per-document loop
inside one transaction (still atomic). `BatchRemove` ignores ids that don't exist and returns the count
actually deleted.

```csharp
await store.BatchUpsert(users);                                  // merge-or-insert many
await store.BatchUpdate(users);                                  // full replace; every doc must exist
int removed = await store.BatchRemove<User>(new object[] { 1, 2, 3 });
```

### Unit of work (grouping writes)

To group several writes into one transaction, create a `UnitOfWork` from the store, queue
`Add`/`AddRange`/`Update`/`Upsert`/`Remove`, then call `SaveChanges`. All commit or all roll back.
Contiguous same-type runs of inserts, upserts, updates, and removes are each coalesced into the
matching batch method. There is no `RunInTransaction` — `UnitOfWork` is the only way to open a
transaction.

```csharp
var uow = store.CreateUnitOfWork();
uow.Add(order)
   .AddRange(orderLines)   // coalesced into one batch insert
   .Update(customer)
   .Remove<Cart>(cartId);
await uow.SaveChanges();    // one transaction; rolls back entirely on failure
```

Only `IDocumentStore` is injected — the unit is created from it. A unit is a write buffer, not a
change tracker: reads don't see operations buffered in an uncommitted unit.

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

### Late-bound JSON lane (Type + JsonNode)

For dynamic ingestion where you hold a registered document `Type` but not a CLR `T` (generic HTTP intake, ETL, gateways). Writes store the JSON **as-is**; reads return raw `JsonNode`. Relational providers only (SQLite, SQLCipher, MySQL, SQL Server, PostgreSQL, Oracle, DuckDB); document-native and key-partitioned providers throw `NotSupportedException`, and it is unavailable inside `CreateUnitOfWork()`.

```csharp
using System.Text.Json.Nodes;

// Write — JsonObject = one doc, JsonArray = many (atomic, one transaction). Returns the count written.
await store.Insert(typeof(Order), JsonNode.Parse("""{ "customer": "acme" }""")!);   // Id auto-generated & injected into the node
await store.Insert(typeof(Order), JsonNode.Parse("""[ { "customer": "a" }, { "customer": "b" } ]""")!);
await store.Update(typeof(Order), node);   // full replace; target must exist
await store.Upsert(typeof(Order), node);   // RFC 7396 merge patch

// Read — raw JSON, no deserialize to T (AOT-clean; filter uses the same string WHERE as Query<T>(string))
JsonNode? doc = await store.Get(typeof(Order), "ord-7");
IReadOnlyList<JsonNode> rows = await store.Query(typeof(Order), "json_extract(Data, '$.status') = @s", new { s = "open" });
await foreach (var n in store.QueryStream(typeof(Order), "json_extract(Data, '$.status') = @s", new { s = "open" })) { }
```

Rules that matter when generating code for this lane:

- **Body is stored AS-IS** — property names must match the type's serialized shape (camelCase by default). A primitive `JsonValue` throws `ArgumentException`.
- **Full pipeline parity** — tenancy, temporal, version/CAS, spatial + vector sidecars, interceptors, and change feed all apply (unlike `IDocumentBackup`/`BulkImport`).
- **Mapped properties must be present** — a registered spatial/vector mapping whose JSON path is absent throws `InvalidOperationException` on `Insert`/`Update`; an explicit JSON `null` is allowed (skips the sidecar). `Upsert` checks this only when the element has no Id.
- **Limitations** — object-mutating interceptors are a no-op (use `ctx.GetJsonDocument()` for JSON-shaped interceptors); vector auto-embedding does not run (put the embedding in the JSON). There is no "filter by JsonNode" — filter with the string WHERE/OData surface.

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
| Oracle | `JSON_VALUE(Data, '$.name')` |
| DuckDB | `json_extract_string(Data, '$.name')` |
| MongoDB / LiteDB / IndexedDB | Raw SQL is not supported — use the LINQ-based `Query<T>()` overload |

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

### Transactions (UnitOfWork)

```csharp
var uow = store.CreateUnitOfWork();
uow.Add(new User { Id = "u1", Name = "Alice", Age = 25 })
   .Add(new User { Id = "u2", Name = "Bob", Age = 30 });
await uow.SaveChanges(); // commits on success, rolls back on exception
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

### ClearAll (whole-store reset)

`IDocumentMaintenance.ClearAll()` wipes every document type in the store, including temporal-history, spatial, and vector sidecars. It is an optional capability — probe with `is IDocumentMaintenance`. It is a whole-store wipe (NOT type- or tenant-scoped; use `Clear<T>()` for one type), targeting only user tables in the current database (system catalog schemas are never touched). Intended for test/dev resets, not production. Implemented on the relational `DocumentStore` (SQLite, SQL Server, PostgreSQL, MySQL, DuckDB, Oracle), MongoDB, and CosmosDB.

```csharp
if (store is IDocumentMaintenance maintenance)
    await maintenance.ClearAll();

// SqliteDocumentStore.ClearAllAsync() still works and now delegates to ClearAll()
```

### Bulk export / import / restore (IDocumentBackup)

`IDocumentBackup` is a streaming bulk export/import surface — a **separate capability**, NOT on `IDocumentStore`. Probe for it with `store is IDocumentBackup` (the same pattern as `IDocumentMaintenance`). Implemented by the relational `DocumentStore` (every SQL provider), MongoDB, and Cosmos DB. Both export and restore **stream** (a multi-GB backup never lands fully in memory), and import binds document bodies **verbatim** — no `<T>`, no `JsonTypeInfo`, no reflection over the documents (AOT-friendly).

Three methods:
- `ExportAsync(Stream, BackupExportOptions?)` — writes the store out as a v1 backup document (a JSON array of `{ id, docType, data }` records, body emitted as-is). `BackupExportOptions { IReadOnlyCollection<string>? DocTypes; bool Indented }`.
- `RestoreAsync(Stream, BulkRestoreOptions?)` — streams a backup back in with a forward-only reader; returns `BulkRestoreResult`.
- `BulkImportAsync(IAsyncEnumerable<RawDocument>, BulkRestoreOptions?)` — lower-level primitive over `RawDocument(string Id, string DocType, ReadOnlyMemory<byte> Data)` (raw UTF-8 JSON body). `RestoreAsync` is the JSON adapter on top of it.

```csharp
// Export the whole store
await using var file = File.Create("backup.json");
await ((IDocumentBackup)store).ExportAsync(file);

// Restore into a fresh store (streamed, bodies bound as-is)
await using var src = File.OpenRead("backup.json");
var result = await ((IDocumentBackup)store).RestoreAsync(src, new BulkRestoreOptions
{
    Mode = BulkWriteMode.Insert,
    ClearExistingFirst = true,
    ChunkSize = 5000,
    Progress = new Progress<BulkProgress>(p => Console.WriteLine($"{p.DocumentsWritten} written"))
});

// Or feed raw rows from any source
await ((IDocumentBackup)store).BulkImportAsync(MyRows(), new BulkRestoreOptions { Mode = BulkWriteMode.Replace });
```

`BulkRestoreOptions`: `BulkWriteMode Mode = Insert`; `bool ClearExistingFirst`; `int ChunkSize = 500`; `bool SingleTransaction` (false = commit per chunk — resumable, bounded WAL/log; true = one transaction); `IProgress<BulkProgress>? Progress`. Result is `BulkRestoreResult(long DocumentsRead, long DocumentsWritten, long DocumentsSkipped, int ChunksCommitted)`.

`BulkWriteMode`:
- `Insert` — fail on duplicate Id (fastest; multi-row `VALUES` everywhere; native bulk copy where available).
- `Replace` — overwrite the body wholesale on conflict.
- `Merge` — RFC 7396 deep-merge (same semantics as `BatchUpsert`).
- `SkipExisting` — insert new, silently skip existing.

**IMPORTANT — raw restore lane.** The import path deliberately SKIPS versioning/CAS, temporal history, interceptors, tenant scoping, and global query filters — that's where the speed comes from. It is NOT a replacement for `BatchUpsert`; use the normal write APIs when you need those side effects. For a full restore prefer `Insert` or `Replace` — under `Merge`, a `null` in a body deletes that field (RFC 7396).

**Provider tiers:**
- **Insert** — every provider (relational multi-row `VALUES`; Mongo `BulkWrite`; Cosmos concurrent waves).
- **Replace & SkipExisting** — all relational providers (`ON CONFLICT` on SQLite/DuckDB/PostgreSQL, `ON DUPLICATE KEY`/`INSERT IGNORE` on MySQL, `MERGE` on SQL Server & Oracle) + Mongo + Cosmos.
- **Merge** — only SQLite, DuckDB and Mongo/Cosmos. Throws `NotSupportedException` on PostgreSQL/MySQL/SQL Server/Oracle (use `Replace`).
- **Native bulk-copy fast path** (Insert, 10-100×) — PostgreSQL (binary `COPY`), SQL Server (`SqlBulkCopy`), DuckDB (appender). Others use multi-row `VALUES`.

**Caveats:** Mongo/Cosmos imports are best-effort, NOT atomic (`SingleTransaction` is ignored — those engines lack multi-doc transactions here). Oracle `Replace`/`SkipExisting` build the `MERGE` source via `SELECT … FROM DUAL UNION ALL`, which can reject documents above the VARCHAR2 bind limit (bound as CLOB). Cosmos export is whole-database (all containers); relational export covers the store's configured tables. Sidecar tables (history/spatial/vector/full-text) are not exported — they are rebuilt by the write path on restore.

## Seeding initial data

Register `IDocumentSeeder`s to populate initial data once. The store is schema-free, so seeding is just **idempotent writes** and works against every provider. Run-once is versioned via a `DocumentSeedMarker` document keyed on the seeder `Name`: a seeder runs when it has never run or when its `Version` exceeds the recorded one. Bump `Version` to re-run after changing the data. Make writes idempotent (`Upsert` on known ids).

```csharp
public class CountrySeeder : IDocumentSeeder
{
    public string Name => "countries";
    public int Version => 1;

    public async Task SeedAsync(IDocumentStore store, CancellationToken ct)
        => await store.Upsert(new Country { Id = "CA", Name = "Canada" }, cancellationToken: ct);
}
```

Register with DI (runs once at host startup via a hosted service):

```csharp
builder.Services.AddDocumentSeeder<CountrySeeder>();
// or inline:
builder.Services.AddDocumentSeeder("settings", version: 1, async (store, ct) =>
    await store.Upsert(new AppSettings { Id = "global", Theme = "dark" }, cancellationToken: ct));
// target a named/keyed store (AddDocumentStore("reporting", ...)):
builder.Services.AddDocumentSeeder<CountrySeeder>(storeName: "reporting");
```

No generic host (e.g. MAUI)? Run them yourself:

```csharp
await DocumentSeedRunner.RunAsync(store, new IDocumentSeeder[] { new CountrySeeder() });
```

Under Native AOT, pass the marker's `JsonTypeInfo` via the `markerTypeInfo` parameter of `DocumentSeedRunner.RunAsync`.

## Temporal History (System-Time Versioning)

Opt-in append-only versioning per type. Enable with `MapTemporal<T>` on the options; every `Insert`/`Update`/`Upsert`/`Remove`/`SetProperty`/`RemoveProperty`/`BatchInsert` (including writes inside a `UnitOfWork`) records a versioned snapshot to a per-type history sidecar. Only mapped types incur the extra write.

```csharp
options.MapTemporal<Order>(o =>
{
    o.Retention    = TimeSpan.FromDays(90);   // prune expired (closed) versions older than this
    o.MaxVersions  = 50;                      // …or keep only the newest N versions per document
    o.CaptureActor = () => currentUser.Id;    // optional "who" recorded per version
});
```

### Provider support

Implemented on **every** provider. Each persists versions to its own sidecar: relational stores (SQLite, SQLCipher, PostgreSQL, SQL Server, MySQL, Oracle, DuckDB) → `{table}_history` table; LiteDB / MongoDB → `{collection}_history` collection; CosmosDB → `{container}_history` container (partitioned by `/typeName`); IndexedDB → `{store}_history` object store.

The history-query methods live on the **`ITemporalDocumentStore`** capability interface (`ITemporalDocumentStore : IDocumentStore`), **not** the base `IDocumentStore` — the same pattern as `IObservableDocumentStore` / `IChangeFeedDocumentStore`, and the `Backup`/`ClearAllAsync` precedent. History is an optional capability, not universal CRUD: promoting it to `IDocumentStore` would force every consumer to see methods that throw unless the type is `MapTemporal`-mapped, and force every backend to implement them. Resolve or cast to `ITemporalDocumentStore` (every store, relational and NoSQL, implements it). A history call for a type not passed to `MapTemporal<T>` throws `InvalidOperationException`.

> **IndexedDB:** temporal adds new object stores, which IndexedDB only creates during a schema upgrade. Bump `options.Version` when adding `MapTemporal` to an already-deployed database (a fresh database needs no change).

### Reading history

```csharp
// Per-document
IReadOnlyList<DocumentVersion<Order>> history = await store.History<Order>(orderId);   // all versions, oldest first
Order? then     = await store.AsOf<Order>(orderId, when);                              // state at a point in time (null if absent/removed)
Order? restored = await store.Restore<Order>(orderId, version: 7);                     // reinstate a prior version as new current
JsonPatchDocument<Order>? patch = await store.GetDiffBetween<Order>(orderId, 3, 7);    // RFC 6902 patch between versions

// Fleet-wide (across all documents of the type)
IReadOnlyList<Order> snapshot = await store.AsOfAll<Order>(when);                       // point-in-time snapshot of all live docs
IReadOnlyList<DocumentVersion<Order>> byUser = await store.ChangesByActor<Order>("alice");
IReadOnlyList<DocumentVersion<Order>> log    = await store.ChangesBetween<Order>(from, to);
```

`DocumentVersion<T>`: `Id`, `Version` (long, from 1), `ValidFrom`, `ValidTo` (null = current), `Operation` (`TemporalOperation.Inserted`/`Updated`/`Removed`), `Actor` (string?), `Document` (T?, **null** for `Removed` tombstones). All history methods accept an optional `JsonTypeInfo<T>` for AOT.

### Behavior & limitations

- `Remove` records a null-body tombstone, so `AsOf`/`AsOfAll` correctly exclude deleted documents.
- For merge/partial writes (`Upsert`/`SetProperty`/`RemoveProperty`) the resulting document is read back so history stores the true post-image — incurred only for temporal-mapped types.
- `Restore` writes a **new** current version (re-inserts if removed); it does not rewrite history. Aligns the version token when optimistic concurrency is mapped.
- `Clear<T>` is a bulk delete and is **not** history-tracked — use `Remove<T>` per document when deletions must be tracked.
- Retention (`Retention` by age, `MaxVersions` by count) prunes on every write; the current version is never pruned. Set at least one on SQLite/mobile.
- On the relational providers the sidecar PK is `(Id, TypeName, Version)` with `(TypeName, ValidFrom, ValidTo)` and `(TypeName, Actor)` secondary indexes backing the fleet-wide queries; the document stores model the same versions natively and compute the selection in the provider.

## Telemetry & Diagnostics

`Shiny.DocumentDb.Diagnostics` adds OpenTelemetry-native metrics + tracing to any provider via a drop-in decorator. Register a store, then call `AddDocumentStoreInstrumentation()` **after** it, and subscribe from OTel with the meter/source name `Shiny.DocumentDb`:

```csharp
services.AddDocumentStore(o => o.DatabaseProvider = new SqliteDatabaseProvider("Data Source=app.db"));
services.AddDocumentStoreInstrumentation();

services.AddOpenTelemetry()
    .WithMetrics(m => m.AddMeter("Shiny.DocumentDb"))
    .WithTracing(t => t.AddSource("Shiny.DocumentDb"));
```

Built on `System.Diagnostics.Metrics.Meter` (via `IMeterFactory`) and `ActivitySource`. Emits, per the OTel database client semantic conventions: a `db.client.operation.duration` histogram (plus a `db.client.operations` counter and a `db.client.response.returned_rows` histogram), tagged `db.system.name` / `db.operation.name` / `db.collection.name` / `outcome` / `error.type`; and a `{system}.{operation}` `ActivityKind.Client` span per call with error status + exception capture. `db.system.name` is derived from the wrapped store, so one decorator covers all providers.

- **Decorator type**: `InstrumentedDocumentStore` implements `IDocumentStore` + `ITemporalDocumentStore` + `IObservableDocumentStore` + `IChangeFeedDocumentStore` (faithful — casts/pattern-matches keep working); wrapped store is on `.Inner`. Construct directly (`new InstrumentedDocumentStore(inner, new DocumentStoreMetrics(meterFactory))`) when not using DI.
- **Coverage**: CRUD, string `Query`/`QueryStream`, the fluent-query terminals (`ToList`/`ToAsyncEnumerable`/`Count`/`Any`/`ExecuteDelete`/`ExecuteUpdate`/`Max`/`Min`/`Sum`/`Average`/`NearestVectors`), spatial/vector, all `ITemporalDocumentStore` ops, and `UnitOfWork.SaveChanges` (inner ops become child spans of the transaction span).
- **Not traced**: `NotifyOnChange`/`SubscribeChanges` (long-lived subscriptions, passed through); the fluent **builder** operators (no I/O); provider internals (raw SQL, RU, pool) — use the per-provider `Logging` option for raw SQL.
- **Zero-cost** when nothing is listening. **Privacy**: only metadata (op, type name, outcome, counts) — never document bodies, ids, or parameter values. Keyed `AddDocumentStore(name, …)` registrations are not auto-decorated.

### MongoDB-Specific Notes

The `Shiny.DocumentDb.MongoDb` provider implements `IDocumentStore` natively over `MongoDB.Driver`. Documents are stored as a typed BSON envelope (`_id`, `id`, `typeName`, `data`, `createdAt`, `updatedAt`) inside a collection that defaults to `"documents"`. Map types to dedicated collections with `MapTypeToCollection`.

- **Predicates evaluated in C#** — LINQ expressions are translated to a MongoDB filter at the type/sort/skip/take level; complex predicates are evaluated client-side after a typed find.
- **Raw SQL throws** — `Query<T>(string)` and `QueryStream<T>(string)` throw `NotSupportedException`. Use the LINQ-based `Query<T>()` overload.
- **`Upsert` deep-merges in C#** — null properties are stripped recursively (RFC 7396 semantics).
- **`UnitOfWork` uses a compensating model** — single-node MongoDB cannot use ACID multi-document transactions without a replica set. The provider tracks inserts and deletes them on failure (matches the CosmosDB provider).
- **`MapTypeToCollection<T>(...)`** — fluent options API with overloads for auto-derived collection names, explicit names, and custom Id expressions.
- **No spatial** — MongoDB supports native geospatial indexing but the provider does not currently expose `WithinRadius`/`WithinBoundingBox`/`NearestNeighbors`.
- **Pre-configured client** — set `MongoDbDocumentStoreOptions.MongoClient` to share an existing `IMongoClient` (pooled, process-wide). When null, the provider creates one from `ConnectionString`.

```csharp
var store = new MongoDbDocumentStore(new MongoDbDocumentStoreOptions
{
    ConnectionString = "mongodb://localhost:27017",
    DatabaseName = "mydb",
    CollectionName = "documents", // default; only used for unmapped types
    JsonSerializerOptions = ctx.Options,
    UseReflectionFallback = false
}
.MapTypeToCollection<User>()
.MapTypeToCollection<Order>("orders")
.MapTypeToCollection<Sensor>("sensors", s => s.DeviceKey)
.MapVersionProperty<Order>(o => o.RowVersion));
```

### DuckDB-Specific Notes

The `Shiny.DocumentDb.DuckDb` provider uses [DuckDB](https://duckdb.org/) — an embedded analytical database — through the standard `IDatabaseProvider` pipeline. Documents are stored as `JSON` column rows alongside `Id`, `TypeName`, `CreatedAt`, `UpdatedAt`.

- **Full LINQ → SQL translation** — same expression visitor used by the SQL providers, emitting `json_extract_string(Data, '$.path')` for property access and `json_merge_patch` for upsert.
- **Native RFC 7396 merge** — DuckDB 0.10+ exposes `json_merge_patch`, so `Upsert` runs entirely server-side with deep-merge semantics (no read-merge-write round trip).
- **`SetProperty`/`RemoveProperty`** — implemented via `json_merge_patch` because DuckDB has no `json_set`/`json_remove`. Path parts are folded into a merge-patch document on the server.
- **JSON extension auto-loaded** — `InitializeConnectionAsync` runs `INSTALL json; LOAD json;` on every connection.
- **Raw SQL supported** — use `json_extract_string(Data, '$.path')` in `Query<T>("...", parameters)` calls.
- **No spatial** — the DuckDB `spatial` extension exists but the provider does not currently wire it into `WithinRadius`/`WithinBoundingBox`/`NearestNeighbors`.
- **Best fit** — analytical workloads, on-device aggregates, embedded reporting, file-based collaboration with Parquet/CSV import via DuckDB's native ingestion (outside the document API).

```csharp
var store = new DocumentStore(new DocumentStoreOptions
{
    DatabaseProvider = new DuckDbDatabaseProvider("Data Source=mydata.duckdb"),
    JsonSerializerOptions = ctx.Options,
    UseReflectionFallback = false
});

// Same fluent query API as every other SQL provider
var top = await store.Query<Order>()
    .Where(o => o.Status == "Shipped")
    .OrderByDescending(o => o.Total)
    .Paginate(0, 100)
    .ToList();
```

## Orleans Grain Storage

`Shiny.DocumentDb.Orleans` is a Microsoft Orleans `IGrainStorage` (+ `PubSubStore`) provider implemented entirely against `IDocumentStore`, so one implementation runs on every DocumentDb backend. Grain state is persisted as a nested, **queryable** `JsonElement` (not an opaque blob), and the envelope can opt into `MapTemporal` for a free audit trail of state mutations.

**Headline feature — query grain state without activating grains.** Orleans grain storage is a point key/value contract (Read/Write/Clear by grain id) with no query surface; normally you must activate a grain to read its state. Because this provider stores state as structured JSON under `$.state`, you can point a read-only `IDocumentStore` at the same grain-state table (`DocumentDbGrainStorage.ConfigureGrainState(opts, "orleans_default")`) and query it directly — no activation, no silo round-trip:

```csharp
var bigCarts = await readStore.Query<GrainStateRecord>(
    "json_extract(Data, '$.state.total') > @min",   // path follows your JsonSerializerOptions casing
    parameters: new { min = 1000 });
```

Caveat: this reads the **last-persisted** state (a live grain may hold unflushed in-memory changes until `WriteStateAsync`), takes no grain locks, and is an eventually-consistent read model — ideal for reporting/dashboards/admin/analytics, not authoritative live state. Set `JsonSerializerOptions` (e.g. camelCase) on the grain-storage options so the JSON path casing is predictable; use the LINQ `Query<T>()` overload on backends without raw SQL (MongoDB/LiteDB/IndexedDB).

### How it maps

| Orleans | Shiny.DocumentDb |
|---|---|
| document key | `Id = "{stateName}\|{grainId}"` |
| ETag | `GrainStateRecord.Version` (mapped via `MapVersionProperty`) |
| concurrency conflict | `ConcurrencyException` → `InconsistentStateException` |
| state blob | nested `JsonElement` in `GrainStateRecord.State` (queryable) |

The Orleans ETag is honored by each provider's atomic compare-and-swap (relational `UPDATE … WHERE version=@expected`, MongoDB version-predicate filter, Cosmos native `IfMatchEtag`), so a stale write loses the race even during a failover duplicate-activation window.

### Registration

```csharp
// Relational backends — built-in path (provider builds & owns its DocumentStore)
siloBuilder.AddDocumentDbGrainStorage("Default", o =>
{
    o.DatabaseProvider = new PostgreSqlDatabaseProvider(connectionString);
    // o.TableName = "orleans_default";   // default: "orleans_{providerName}"
    // o.DeleteStateOnClear = true;        // default; false writes a versioned tombstone
});

// MongoDB / Cosmos — companion packages wire store + grain-state mapping for you
siloBuilder.AddMongoDbGrainStorage("Default", connectionString, databaseName: "orleans");
siloBuilder.AddCosmosDbGrainStorage("Default", connectionString, databaseName: "orleans");

// Any other backend — generic escape hatch; you map GrainStateRecord + version property
siloBuilder.AddDocumentDbGrainStorage("Default", o =>
{
    o.StoreFactory = sp =>
    {
        var opts = new LiteDbDocumentStoreOptions { ConnectionString = "Filename=grains.db" };
        opts.MapVersionProperty<GrainStateRecord>(x => x.Version);
        return new LiteDbDocumentStore(opts);
    };
});
```

`AddDocumentDbGrainStorageAsDefault(...)` registers under Orleans' default provider name. `DocumentDbGrainStorage.ConfigureGrainState(options, tableName)` applies the type→table + version mappings on a relational options instance in one call.

### Options & compatibility

- **`DocumentDbGrainStorageOptions`**: `DatabaseProvider` (relational built-in path) **or** `StoreFactory` (any backend); `TableName` (default `"orleans_{providerName}"`); `DeleteStateOnClear` (true = delete row, false = versioned tombstone); `JsonSerializerOptions`; `InitStage`.
- **Compatibility tiers** — **Recommended**: PostgreSQL ✅, SQL Server, MySQL, Oracle (atomic `UPDATE … WHERE` CAS). **Supported**: MongoDB ✅ (atomic version-predicate filter; `_id` embeds the grain key). **Limited/dev**: SQLite, LiteDB, IndexedDB, DuckDB (single-writer/embedded). **Use with care**: Cosmos DB (CAS correct, but partitions by `typeName` → 20 GB logical-partition cap for large single-type grain populations). ✅ = covered by integration tests.
- **Serialization** — the internal envelope types are always source-generated (reflection-free). Grain state `T` becomes source-generated too when you assign a `JsonSerializerContext` as `o.JsonSerializerOptions.TypeInfoResolver`; set `o.UseReflectionFallback = false` to hard-fail on an unregistered state type instead of reflecting. Defaults (`UseReflectionFallback = true`, no context) keep the prior reflection behavior. The silo host itself is still not an AOT target (Orleans runtime is reflection-heavy).

### Orleans system stores (reminders, clustering, grain directory)

Beyond grain storage, the same `IDocumentStore` foundation backs the rest of the Orleans persistence stack. All three share the `OrleansStoreOptions` shape — a relational `DatabaseProvider` (built-in path, mappings wired for you) **or** a `StoreFactory` returning a fully-configured store (MongoDB / Cosmos / others) — and per-row optimistic concurrency rides on the same version-property CAS.

```csharp
siloBuilder
    .AddDocumentDbReminders(o      => o.DatabaseProvider = new PostgreSqlDatabaseProvider(cs))
    .AddDocumentDbClustering(o     => o.DatabaseProvider = new PostgreSqlDatabaseProvider(cs))
    .AddDocumentDbGrainDirectory("Default", o => o.DatabaseProvider = new PostgreSqlDatabaseProvider(cs));
```

- **Reminders (`IReminderTable`)** — `AddDocumentDbReminders(...)` (also calls Orleans' `AddReminders()`); default table `orleans_reminders`. Hash-ring range reads via a fluent query on the stored `GrainHash`; per-row version CAS. No multi-document transaction → works on **any** backend.
- **Clustering / membership (`IMembershipTable`)** — `AddDocumentDbClustering(...)`; default table `orleans_membership`. Per-silo rows + a global table-version row are updated together in a single `UnitOfWork`, each CAS-gated. **Requires multi-document transactions → relational or MongoDB replica set; Cosmos is NOT supported** (single-partition batches only).
- **Grain directory (`IGrainDirectory`)** — `AddDocumentDbGrainDirectory("Default", ...)`; default table `orleans_graindirectory`. Per-row version CAS for register/unregister races; no transaction required.
- Companion `.MongoDb`/`.CosmosDb` packages currently add grain-storage registration only; use `StoreFactory` to point reminders/membership/directory at a Mongo/Cosmos store. All three are covered by PostgreSQL integration tests.

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

## Vector / Similarity Search

Embedding-similarity search via `store.NearestVectors<T>(query, k)`. Supported on PostgreSQL (`pgvector`), SQL Server 2025, Oracle 23ai, CosmosDB (DiskANN), MongoDB (Atlas `$vectorSearch`), DuckDB (`vss`), and **SQLite** (`sqlite-vec`). LiteDB, IndexedDB, and MySQL throw `NotSupportedException`.

```csharp
options.MapVectorProperty<Doc>(d => d.Embedding, dimensions: 1536, metric: VectorDistance.Cosine);
var hits = await store.NearestVectors<Doc>(queryEmbedding, k: 5);
```

### SQLite — loading `sqlite-vec`

The SQLite provider needs the `sqlite-vec` native binary. **Recommended: add the `Shiny.DocumentDb.Sqlite.VectorSupport` package** — it ships the binaries for iOS (static `xcframework`), Android (`.so` per ABI), and desktop/Mac Catalyst, plus a one-call registration helper. This is the default answer for any iOS/Android/MAUI vector question.

```csharp
using Shiny.DocumentDb.Sqlite.VectorSupport;

// Call once at startup, before opening a connection. Works on iOS, Android, and desktop alike.
opts.DatabaseProvider = SqliteVec.CreateProvider($"Data Source={dbPath}");
// (or call SqliteVec.RegisterAutoExtension() yourself, then set VectorExtensionPreloaded = true)
```

`RegisterAutoExtension()` is engine-aware, so it also works with **SQLCipher**: it registers `vec0` against whichever engine SQLitePCLRaw loaded (`e_sqlite3` or `e_sqlcipher`). For SQLCipher, call `SqliteVec.RegisterAutoExtension()` and set `VectorExtensionPreloaded = true` on your `SqlCipherDatabaseProvider` (`CreateProvider` returns a plain `SqliteDatabaseProvider`, so don't use it for the encrypted case).

If you supply your own binary, two mutually complementary flags on `SqliteDatabaseProvider`:

- **`EnableVectorExtension = true`** — loads at runtime via `SqliteConnection.LoadExtension("vec0")`. Ship the native binary on the load path. **Desktop/server only** — this path **cannot work on iOS** (Apple forbids `dlopen` of loose libraries; bundled `e_sqlite3` disables runtime loading) and usually fails on Android.
- **`VectorExtensionPreloaded = true`** — assumes the extension is already registered on every connection (statically linked + `sqlite3_auto_extension(sqlite3_vec_init)`), so it **skips the runtime load**. The only approach that works on iOS. If both flags are set, preloaded wins.

Either flag (or the package helper) makes `SupportsVector` return `true`. Without one, `NearestVectors` throws `NotSupportedException`. vec0 is flat-scan (no HNSW); when a `.Where(...)` filter is combined with the search, the library over-fetches `k * postFilterMultiplier` (default 4) candidates.

## Full-Text Search

Relevance-ranked text search over one or more string properties. **Declarative and up-front**: map the searchable property with `MapFullTextProperty<T>(...)` and the library creates the native index for you at startup. A type **must be mapped before it can be searched** — there is no ad-hoc full-text (unlike `.Where(x => x.Body.Contains(...))`, which works on any field). Supported on **every provider**: FTS5 (SQLite), `tsvector`+GIN (PostgreSQL), `FULLTEXT` (MySQL), Oracle Text (Oracle), Full-Text Index (SQL Server), the `fts` extension (DuckDB), full-text policy (Cosmos), `$text` (MongoDB), and an in-memory TF-IDF scan on LiteDB / IndexedDB.

```csharp
// single field, or several combined into one index
options.MapFullTextProperty<Article>(a => a.Body);
options.MapFullTextProperty<Article>([a => a.Title, a => a.Body]);

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

`FullTextResult<T>` carries `Document` and a normalized `double Score` (higher = more relevant; absolute scale is provider-specific — compare only within one result set). `MapFullTextProperty` also has an AOT-safe overload taking `propertyNames` + a `Func<T, IEnumerable<string?>>` selector (for combining fields or indexing a string collection), and an optional `FullTextLanguage` (controls stemming where the backend supports it). The index is engine-maintained, so `Insert`/`Update`/`Remove`/`Clear` keep it in sync automatically. Notes: engines with one full-text index per table (SQL Server, MongoDB) support a single mapped type per table/collection; **Oracle Text** and **SQL Server Full-Text Search** are optional server components that must be installed; Cosmos full-text needs `Microsoft.Azure.Cosmos` 3.61.0+.

## Fluent Query Builder (IDocumentQuery<T>)

The fluent query builder is the primary way to query documents. Start with `store.Query<T>()` and chain builder methods, then terminate with a materialization method.

### Builder Methods (non-executing, return IDocumentQuery<T>)

| Method | Description |
|--------|-------------|
| `.Where(predicate)` | Filter by LINQ expression. Multiple calls combine with AND. |
| `.Where(filter[, jsonTypeInfo])` | Filter by a runtime filter string (e.g. `"Age >= 30 and Status == 'open'"`) — AOT-safe. `and`/`or`/`not`, comparisons, `is [not] null`, `in (…)`, `contains/startsWith/endsWith`. |
| `.WhereIn(selector, values[, nulls])` / `.WhereNotIn(selector, values[, nulls])` | Set-membership filter (`IN` / `NOT IN`) from an in-memory collection. The collection is lowered to the store's native construct (`IN` / `$in`), not expanded into the filter text. `nulls` is a `NullHandling` (`Ignore` default / `Raw` / `Match`). Empty set ⇒ `WhereIn` matches nothing, `WhereNotIn` matches everything. Also takes a string property name overload. |
| `.OrderBy(selector)` / `.OrderByDescending(selector)` | Sort by property (expression). |
| `.OrderBy(name[, jsonTypeInfo])` / `.OrderByDescending(name[, jsonTypeInfo])` | Sort by property name (string) — AOT-safe via `JsonTypeInfo<T>`. Supports dotted paths. |
| `.OrderBy(name, direction[, jsonTypeInfo])` | Sort by property name with a runtime direction string (`asc`/`ascending`/`desc`/`descending`, case-insensitive; empty → ascending). |
| `.GroupBy(selector)` | Group by property (for aggregate projections). |
| `.Paginate(offset, take)` | Limit results with SQL LIMIT/OFFSET. |
| `.Select(selector, resultTypeInfo?)` | Project into a different shape via `json_object`. |
| `.Project(fields[, jsonTypeInfo])` | Project a runtime-chosen field list (e.g. `"name,email"`) into `IDocumentQuery<JsonObject>` — AOT-safe. For REST sparse fieldsets; no DTO required. Supports scalar functions with an alias (`"lower(email) as email"`) on every provider. |

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
| `.PageResult(page, pageSize, zeroBased?)` | `Task<PagedResults<T>>` | Run the query and return records + total count in one envelope. 1-based by default. |
| `.ToQueryString()` | `DocumentQueryString` | Build the query the configuration **would** run **without executing it** — for debugging/logging. See below. |

### Inspecting the generated query — `.ToQueryString()`

`.ToQueryString()` returns a `DocumentQueryString { string Sql; IReadOnlyDictionary<string, object?> Parameters; }` describing the `ToList()` form of the query without executing it. Its `ToString()` renders the parameter values as a `-- @name=value` comment header above the SQL. Works for both LINQ and string-expression `Where` (same pipeline) and includes `Where`/`OrderBy`/`Paginate`/`Select`/`Project`.

```csharp
var qs = store.Query<User>().Where(u => u.Age > 28).ToQueryString();
qs.Sql;          // "SELECT Data FROM ... WHERE TypeName = @typeName AND (json_extract(Data, '$.age') > @p0);"
qs.Parameters;   // { ["@typeName"] = "User", ["@p0"] = 28 }
Console.WriteLine(qs); // comment header + SQL
```

Provider support: relational providers (SQLite, SQL Server, PostgreSQL, MySQL, Oracle, DuckDB) and Cosmos return SQL + parameters; MongoDB returns its rendered BSON filter (or full find command) as JSON with empty `Parameters`. LiteDB and IndexedDB — and client-side projections after `Select`/`Project` on the document providers — throw `NotSupportedException`.

### Common Patterns

```csharp
// Get all documents of a type
var users = await store.Query<User>().ToList();

// Filter
var results = await store.Query<User>()
    .Where(u => u.Age > 25)
    .ToList();

// Set membership (IN / NOT IN) from an in-memory collection
var statuses = new[] { "Open", "Pending", "Review" };
var open = await store.Query<Order>()
    .WhereIn(o => o.Status, statuses)
    .ToList();

// NOT IN, treating a null in the set as "also exclude null fields"
var assigned = await store.Query<Order>()
    .WhereNotIn(o => o.AssignedTo, new string?[] { "alice", null }, NullHandling.Match)
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

### `PageResult` — records + total count

For UI/REST responses use `.PageResult(page, pageSize)` to materialize the page slice *and* the total matching count in a single envelope. 1-based by default; pass `zeroBased: true` for 0-based indexing.

```csharp
public record PagedResults<T>(
    IEnumerable<T> Records,
    int TotalCount,
    int Page,
    int PageSize
);

// 1-based (default)
var result = await store.Query<User>()
    .Where(u => u.Active)
    .OrderBy(u => u.Name)
    .PageResult(page: 1, pageSize: 20);

// 0-based opt-in
var result = await store.Query<User>()
    .OrderBy(u => u.Name)
    .PageResult(page: 0, pageSize: 20, zeroBased: true);
```

- `TotalCount` reflects the current `Where` predicates (and any global query filters) — pagination state is ignored when counting.
- Overrides any prior `.Paginate(...)` call on the query.
- `pageSize` must be > 0; `page` must be `>= 1` (or `>= 0` when `zeroBased: true`). Otherwise throws `ArgumentOutOfRangeException`.

### Dynamic sort columns (string-based OrderBy)

When the sort column is determined at runtime (e.g. a column-header click, a `?sort=` query string), use the string-based overloads. They are AOT-safe: resolution walks `JsonTypeInfo.Properties` and synthesizes an `Expression.Property(parameter, PropertyInfo)` tree — no `Type.GetProperty(string)` reflection on `T`, no `Expression.Compile()`.

> **`jsonTypeInfo` is optional** on every string overload (`Where`, `OrderBy`, `OrderByDescending`, `Project`). When omitted, the query reuses the `JsonTypeInfo<T>` it resolved at creation (from `Query(ctx.User)` or the registered context), so `store.Query(ctx.User).OrderBy("Name")` works without re-passing it.

```csharp
// Sort by CLR name
var results = await store.Query<User>().OrderBy("Name", ctx.User).ToList();

// Or by JSON name (after the naming policy)
var results = await store.Query<User>().OrderBy("name", ctx.User).ToList();

// Descending
var results = await store.Query<User>().OrderByDescending("Age", ctx.User).ToList();

// Dotted path for nested properties
var orders = await store.Query<Order>().OrderBy("ShippingAddress.City", ctx.Order).ToList();

// Driven by an external value
string sort = request.Query["sort"]; // e.g. "Name", "Age", "ShippingAddress.City"
var results = await store.Query<User>()
    .Where(u => u.Active)
    .OrderBy(sort, ctx.User)
    .ToList();

// Direction as a runtime string too (e.g. "?sort=name&dir=desc").
// Accepts "asc"/"ascending"/"desc"/"descending" (case-insensitive);
// an empty/null/whitespace direction defaults to ascending.
string dir = request.Query["dir"];
var results = await store.Query<User>()
    .OrderBy(sort, dir, ctx.User)
    .ToList();
```

Matching rules:
- Case-insensitive match against either the CLR property name (`PropertyInfo.Name`) or the JSON property name (`JsonPropertyInfo.Name` after the naming policy).
- Dotted segments traverse nested types; each nested type must also be registered in your `JsonSerializerContext`.
- Unknown segments throw `ArgumentException`. Null / empty / whitespace paths throw `ArgumentNullException` / `ArgumentException`.
- The `OrderBy(name, direction, jsonTypeInfo)` overload parses the direction string and delegates to the `OrderBy` / `OrderByDescending` string overloads — same AOT-safe resolution. An unrecognized direction throws `ArgumentException`.

### Dynamic filter strings (string-based Where)

When the filter is supplied at runtime (a REST `?filter=`, a saved view, an admin search), use `Where(string, JsonTypeInfo<T>)`. It parses a small expression language into the same expression tree a compiled predicate produces, so it runs through the normal translator and stays AOT/trim-safe (no `Compile()`; fields resolved through `JsonTypeInfo`).

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

Grammar:
- `and` / `or` / `not` and parentheses.
- Comparisons `==` (or `=`), `!=` (or `<>`), `>`, `>=`, `<`, `<=`. Relational ops are rejected for `string`/`bool`/`Guid`.
- `field is null` / `field is not null` (and `field == null`).
- `field in (a, b, c)`.
- String functions `contains(field, 'x')`, `startsWith(field, 'x')`, `endsWith(field, 'x')`.
- Field names follow the string-`OrderBy` rules (case-insensitive CLR/JSON name, dotted paths). String literals use single/double quotes; double the quote to escape. Literals are coerced to the field's CLR type. Syntax errors / unknown fields throw `ArgumentException`.

When the filter *shape* is fixed but its *values* come from code, prefer the interpolated overload `Where(FilterInterpolatedStringHandler, JsonTypeInfo<T>)` — write `Where($"…")`. Each `{value}` hole is captured as a typed argument and bound as a parameter (never formatted into the text), so **don't quote interpolated string values and don't build the filter with string concatenation** — that reintroduces the injection/quoting problems this overload removes.

```csharp
var status = request.Query["status"];
var minAge = 30;

var open = await store.Query<User>()
    .Where($"Age >= {minAge} and Status == {status}", ctx.User)  // {status} needs no quotes; injection-safe
    .ToList();
```

- An interpolated `$"..."` literal binds to this overload; a plain `string` variable binds to the raw `Where(string)` overload. So pass the raw `?filter=` text as a `string` to parse it, and use `$"..."` only to inject values.
- Holes are valid only where a literal would appear — comparison RHS, `in (...)` list, or string-function argument (`contains(Email, {fragment})`) — never as a field name. Values coerce to the field's CLR type; a `null` value becomes an `is null` check.

### Runtime field projection (string-based Project)

`Project(fields, JsonTypeInfo<T>)` selects a runtime-chosen field list and returns `IDocumentQuery<JsonObject>` — no DTO needed. Ideal for REST sparse fieldsets (`?fields=name,email`).

```csharp
IReadOnlyList<JsonObject> rows = await store.Query<User>()
    .Where("Age >= 30", ctx.User)
    .OrderBy("Name", ctx.User)
    .Project("Name, Email", ctx.User)
    .ToList();

var name = rows[0]["name"]!.GetValue<string>();

// Pagination / Count / Any / streaming work on the projected query.
var page = await store.Query<User>().Project("name,email", ctx.User).PageResult(1, 20);

// Scalar functions are allowed and require an alias.
var shaped = await store.Query<User>()
    .Project("name, lower(email) as email, length(name) as len, year(created) as yr", ctx.User)
    .ToList();
```

- Relational providers emit `json_object('name', json_extract(Data,'$.name'), …)`; CosmosDB/MongoDB/LiteDB/IndexedDB project client-side via the compile-free interpreter. Supported on **every** provider.
- Output keys are the **leaf JSON name** (`ShippingAddress.City` → `city`) unless overridden with `as alias`; functions require an alias. Duplicate keys throw `ArgumentException`.
- Functions are the same set as the string `Where` grammar (`lower`/`upper`/`length`/`trim`/`substring`/`replace`/`indexof`, `abs`/`round`/`ceiling`/`floor`/`sqrt`/`sign`, `year`/`month`/`day`/…, `soundex`).
- After `Project` the query is terminal-shaped: `ToList`/`ToAsyncEnumerable`/`Count`/`Any`/`Paginate` work; `Where`/`OrderBy`/`Select`/aggregates throw.

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

// Property form — collection .Count / array .Length map to the same
// array-length function as .Count() (works on every provider)
o => o.Lines.Count == 0
o => o.Tags.Count > 1
// json_array_length(Data, '$.lines') = 0   /   ... > 1
```

`string.Length` and dictionary `.Count` are **not** array lengths and throw `NotSupportedException` (instead of silently generating a dead query) — use `.Count()` / `.Any()` for collection length.

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

**Note:** Streaming on shared-connection providers (SQLite, SQLCipher, DuckDB) holds the per-store semaphore for the lifetime of enumeration — calling other store methods inside the same `await foreach` will block until it completes. On pooled providers (PostgreSQL, MySQL, SQL Server, Oracle) the streaming reader uses one connection from the driver pool and does not block concurrent ops on the same store, but interleaving writes can still surprise consumers expecting a stable snapshot.

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

## Transactions (UnitOfWork)

Grouping writes into one transaction is done through a `UnitOfWork` created from the store — there is
no `RunInTransaction`. Queue `Add`/`AddRange`/`Update`/`Upsert`/`Remove`, then `SaveChanges`.

```csharp
var uow = store.CreateUnitOfWork();
uow.Add(new User { Id = "u1", Name = "Alice", Age = 25 })
   .Add(new User { Id = "u2", Name = "Bob", Age = 30 });
await uow.SaveChanges(); // commits on success, rolls back on exception
```

`SaveChanges` opens one transaction (pinning a single connection), applies all queued operations in
order, and commits — coalescing contiguous same-type inserts into the batch-insert fast path. A unit is
a write buffer, not a change tracker: reads don't see operations buffered in an uncommitted unit. For
read-modify-write atomicity, use ETag/CAS (`IfMatch`) + retry.

**Side-effect-free writes:** commit with `await uow.SaveChanges(suppressInterceptors: true)` to apply
the unit with **no** interceptor firing — neither per-document nor bulk. The suppression is bounded by
that commit (writes outside the unit still fire interceptors), so it's the right tool for *mirrored /
authoritative* data that should carry no side effects: bulk import, seeding, migration, and the inbound
apply path of `Shiny.DocumentDb.AppDataSync`. While suppressed, the multi-row batch fast path is
re-enabled (it's only disabled to guarantee per-doc interceptors fire — moot when none will).

## Write Interceptors

Register interceptors to observe/mutate writes; the after-hook runs inside the transaction with the
generated id/version. Per-document (`IDocumentInterceptor`) fires for Insert/BatchInsert(per item)/
Update/Upsert/Remove; bulk (`IDocumentBulkInterceptor`) fires once for ExecuteUpdate/ExecuteDelete/Clear.

```csharp
opts.AddInterceptor(new AuditInterceptor());
opts.OnBeforeWrite<Order>((ctx, ct) => { /* mutate ctx.Document or throw to abort */ return Task.CompletedTask; });
opts.OnAfterWrite<Order>((ctx, ct) => outbox.Enqueue(ctx.Id, ctx.Operation, ct));
```

Interceptors can also be **registered in DI** to get constructor-injected dependencies. `AddDocumentStore` resolves every `IDocumentInterceptor` / `IDocumentBulkInterceptor` from the container and runs them after the options-registered ones (deterministic order). Resolved once from the store's provider — register as singletons (use `IServiceScopeFactory` inside the hook for scoped services).

```csharp
public sealed class OutboxInterceptor(IOutbox outbox) : IDocumentInterceptor
{
    public Task BeforeWrite(DocumentWriteContext ctx, CancellationToken ct) => Task.CompletedTask;
    public Task AfterWrite(DocumentWriteContext ctx, CancellationToken ct) => outbox.Enqueue(ctx.TypeName, ctx.Id, ctx.Operation, ct);
}

services.AddSingleton<IDocumentInterceptor, OutboxInterceptor>();
services.AddDocumentStore(opts => opts.DatabaseProvider = new SqliteDatabaseProvider("Data Source=app.db"));
```

**The serialized JSON on the context:** inside `BeforeWrite`, `ctx.GetJson()` returns the exact JSON
about to be persisted (serialized with the store's own options/`JsonTypeInfo`, cached, and invalidated
if an earlier interceptor replaces `ctx.Document`); `ctx.GetJsonDocument()` returns a parsed
`JsonDocument` (dispose it). Both return `null` for delete-by-id. Useful for auditing/redaction and the
primitive the JSON-Schema package builds on.

## JSON Schema Validation (Shiny.DocumentDb.JsonSchema)

Validate the exact JSON about to be persisted against a JSON Schema (draft 2020-12, via `JsonSchema.Net`)
just before the write. A failure throws `DocumentSchemaValidationException` (field-level `Errors`) and
rolls the write back. Per-type opt-in; unmapped types, deletes, and set-based writes pass through.

```csharp
// On DocumentStoreOptions — no DI required, works with new DocumentStore(options). Repeated calls
// accumulate into one interceptor. Reads like the other Map* methods.
options
    .MapJsonSchema<Customer>("""
    {
      "type": "object",
      "additionalProperties": false,
      "required": ["name", "email"],
      "properties": {
        "name":  { "type": "string", "minLength": 1, "maxLength": 100 },
        "email": { "type": "string", "format": "email" }
      }
    }
    """)
    .MapJsonSchema<Order>(orderSchema)
    .ConfigureJsonSchemaValidation(s => s.EnableFormatAssertion = false); // optional

// DI flavour:
services.AddDocumentJsonSchema(o => o.MapJsonSchema<Customer>(customerSchema));
// Bulk options form: options.AddJsonSchemaValidation(o => o.MapJsonSchema<Customer>(schemaJson));
```

Rules that matter:
- **Schema names are the SERIALIZED names — camelCase by default.** Author schemas in camelCase.
- **Validate what the C# type can't** — `maxLength`, ranges, `pattern`, `enum`, `additionalProperties:false`,
  required-ness of reference-type properties.
- **`format` is asserted by default** (email/uuid/date-time). Set `EnableFormatAssertion = false` for
  annotation-only.
- Map schemas by `JsonSchema` object, JSON text, `Stream`, or `MapJsonSchemaFromFile<T>(path)` — parsed once.
- Composes with `Shiny.DocumentDb.AppDataSync`: validation is `BeforeWrite`, so an invalid document throws
  before it can reach the sync outbox. Suppressed writes (inbound sync / bulk import) skip validation.

## Offline-First Sync (Shiny.DocumentDb.AppDataSync)

Glue package that makes the document store the local cache of an offline-first app syncing to an HTTP
backend via `Shiny.Data.Sync`. Local writes auto-enqueue to the sync outbox; pulled server changes
auto-apply back into the store. No manual `Queue`/`IDataSyncDelegate` plumbing. Client-tier providers
only (SQLite, LiteDB, IndexedDB).

Synced types must implement `Shiny.Data.Sync.ISyncEntity` (string `Identifier`, conventionally
`Id.ToString()`) — required by `Shiny.Data.Sync`'s `RegisterEndpoint<T>` / `Queue<T>`.

```csharp
public sealed class TodoItem : ISyncEntity
{
    public Guid Id { get; set; }
    public string? Title { get; set; }
    public bool Completed { get; set; }
    public string Identifier => this.Id.ToString();
}

builder.Services
    .AddDocumentStore(o => o.UseSqlite("app.db").MapTypeToTable<TodoItem>())
    .AddDataSync<MyDataSyncDelegate>(opts => opts.RegisterEndpoint<TodoItem>("https://api.example.com/todos"))
    .SyncDocumentStore(sync => sync.Sync<TodoItem>());
```

- Inbound applies run under `SaveChanges(suppressInterceptors: true)` → no echo back, no other interceptor.
- Set-based writes (`ExecuteUpdate`/`ExecuteDelete`/`Clear<T>`) throw `SyncBulkWriteNotSupportedException`
  on synced types; use `ClearAll` for a local whole-store reset. Batch writes enqueue per item.
- The store + sync serializers are validated to share one JSON contract at startup.

## OData Endpoints (Shiny.DocumentDb.OData / Shiny.DocumentDb.AspNetCore.OData)

Expose a document type as an OData v4 entity set. `$filter`/`$orderby`/`$top`/`$skip`/`$count`/`$select`
translate onto `IDocumentQuery<T>` and run on any provider. `$expand` → 501 (no relationships).

```csharp
builder.Services
    .AddDocumentStore(...)
    .AddDocumentODataEndpoints(edm => edm.EntitySet<Customer>("customers"));   // key defaults to "Id"

app.MapDocumentODataEntitySet<Customer>("odata/customers");
// GET odata/customers?$filter=Country eq 'CA'&$orderby=Created desc&$top=20&$count=true
```

**Governance (lock down a public endpoint).** Each entity set has an `ODataQueryPolicy`. Set API-wide
defaults with `ConfigureDefaultPolicy(...)` and override per set via `EntitySet<T>(name, policy => …)`
(the override clones the defaults). A disallowed-but-well-formed request → `400` (with a message naming
the offender); page size is clamped to `MaxTop`. Defaults are permissive — opt in.

```csharp
.AddDocumentODataEndpoints(edm =>
{
    edm.ConfigureDefaultPolicy(p =>
    {
        p.DefaultPageSize = 25;     // applied when $top omitted → never unbounded
        p.MaxTop = 100;             // larger $top → 400; page size clamped to this
        p.MaxSkip = 10_000;
        p.MaxFilterNodeCount = 50;  // complexity / DoS guard
    });
    edm.EntitySet<Customer>("customers", p =>
    {
        p.FilterableProperties.UnionWith(["Name", "Country", "Age"]);  // empty = all allowed
        p.SortableProperties.UnionWith(["Name", "Age"]);
        p.SelectableProperties.UnionWith(["Id", "Name", "Country", "Age"]);
        // p.AllowCount = false; p.AllowArithmetic = false; p.AllowedFunctions.UnionWith(["startswith"]);
    });
});
```

- Two packages: `Shiny.DocumentDb.OData` (dependency-free, AOT-clean engine; `ODataQueryPolicy` lives
  here) and `Shiny.DocumentDb.AspNetCore.OData` (ASP.NET Core host; JIT-only).
- Global `AddQueryFilter` predicates always apply underneath `$filter`. `$count` is pre-paging.
- Status codes: `400` = policy violation or unknown property; `501` = `$expand` / spatial on a
  non-spatial provider; otherwise `200` (page size silently clamped to the cap).
- Inserts and OData reads must share one serializer (in AOT, set the store's `JsonSerializerOptions`
  to your `JsonSerializerContext.Default.Options`).

## Aspire Integration (Shiny.DocumentDb.Aspire.Hosting / .Client / .Orleans)

Make "which database backs the store" and "how it's seeded" AppHost decisions. Server-tier only
(Postgres/SQL Server/MySQL/SQLite); offline-first providers never touch an AppHost.

```csharp
// AppHost
var store = builder.AddPostgresDocumentStore("orders")   // or AddSqliteDocumentStore / AddSqlServerDocumentStore
    .WithSeeder(async (ctx, ct) => { /* gated one-shot seed */ });
builder.AddProject<Projects.Api>("api").WithReference(store);

// Consuming service — provider-agnostic, keyed store + health + OpenTelemetry
builder.AddDocumentStore("orders", configureOptions: o => o.MapTypeToTable<Order>());

// Container-aware option setup — configureServiceOptions runs with the resolved IServiceProvider
builder.AddDocumentStore("orders",
    configureServiceOptions: (sp, o) => o.AddInterceptor(sp.GetRequiredService<AuditInterceptor>()));

// Shared-table multi-tenancy in one line — wires TenantIdAccessor from a registered ITenantResolver
builder.Services.AddSingleton<ITenantResolver, MyTenantResolver>();
builder.AddDocumentStore("orders", settings => settings.MultiTenant = true);

// Orleans-on-DocumentDb silo
builder.AddDocumentStore("orleans");
builder.UseOrleans(silo => silo.UseAspireDocumentDb("orleans")); // grain storage + reminders + clustering + directory
```

The AppHost injects the connection string + a provider discriminator (`Shiny:DocumentDb:<name>:Provider`);
the client selects the matching provider. Resolve the store keyed: `[FromKeyedServices("orders")] IDocumentStore`.
Use `configureOptions: o => …` for plain option setup and `configureServiceOptions: (sp, o) => …` when an
option needs another DI service; set `DocumentStoreSettings.MultiTenant` for the common shared-table tenancy
case. Non-Aspire callers get the same primitive via `AddDocumentStore(services, name, (sp, o) => …)`.

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

## Change Monitoring (IObservableDocumentStore)

Stores that implement `IObservableDocumentStore` expose an `IAsyncEnumerable<DocumentChange<T>>` of insert/update/remove/clear events for documents written through *this* store instance. Use it to drive reactive UI from local writes. Supported on `DocumentStore` (SQLite, SQLCipher, MySQL, SQL Server, PostgreSQL, Oracle) and `LiteDbDocumentStore`. Cosmos, MongoDB, IndexedDB, and DuckDB do not implement it.

### NotifyOnChange<T>

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

cts.Cancel(); // unsubscribes; the await foreach exits
```

### WhenDocumentChanged<T>(id) — single document

```csharp
var observable = (IObservableDocumentStore)store;
await foreach (var change in observable.WhenDocumentChanged<Order>("ord-1", ct))
{
    // Only events for ord-1 (plus Cleared, which affects every doc).
}
```

### Per-query monitoring: IDocumentQuery<T>.NotifyOnChange()

Every fluent query exposes `.NotifyOnChange(ct)` — it filters the change stream by the query's `Where` predicates. `OrderBy`, `Paginate`, and `GroupBy` are ignored. Throws after `Select(...)`.

```csharp
var pending = store.Query<Order>().Where(o => o.Status == "Pending");

await foreach (var change in pending.NotifyOnChange(ct))
{
    // Only inserts/updates where the new document matches Status == "Pending".
}
```

### DocumentChange<T>

| Property | Description |
|---|---|
| `ChangeType` | `Inserted`, `Updated`, `Removed`, or `Cleared` |
| `Id` | Affected document Id (empty for `Cleared`) |
| `Document` | Populated for `Inserted` / full-document `Updated`; `null` for `Removed`, `Cleared`, `SetProperty`, `RemoveProperty` |

### Transaction buffering

Changes performed in a `UnitOfWork` are buffered and emitted *after* `SaveChanges` commits. A rollback discards the buffered events.

```csharp
var uow = store.CreateUnitOfWork();
uow.Add(new User { Id = "u1", Name = "Alice" })
   .Add(new User { Id = "u2", Name = "Bob" });
// Subscribers see nothing yet.
await uow.SaveChanges();
// Subscribers receive both events here, in order.
```

### Property-level paths emit Document == null

`SetProperty`, `RemoveProperty`, `Remove`, and `Clear` do not materialize the document, so `DocumentChange<T>.Document` is `null` for those events. For per-query monitoring, those events are passed through unconditionally so the consumer can re-query if needed.

### Cancellation / unsubscribe

Cancel the token passed to `NotifyOnChange` (or break out of the `await foreach`). The underlying channel is unregistered automatically when the iterator exits.

## Global Query Filters

EF Core-style `HasQueryFilter` equivalent. Register a predicate on `DocumentStoreOptions` (or the provider-specific options class) and it's AND-applied to every query of `T` — including single-document operations, bulk operations, and per-query change monitoring. `Insert`/`BatchInsert`/`Upsert` are intentionally unfiltered (matches EF Core). Raw SQL (`Query<T>(string)` / `QueryStream<T>(string)`) is unfiltered.

### Registration

```csharp
var store = new DocumentStore(new DocumentStoreOptions
{
    DatabaseProvider = new SqliteDatabaseProvider("Data Source=mydata.db")
}
.AddQueryFilter<User>(u => !u.IsDeleted)                          // unnamed
.AddQueryFilter<Order>("tenant", o => o.TenantId == tenantCtx.Current) // named
.AddQueryFilter<Order>("status", o => o.Status != "Archived"));
```

`AddQueryFilter<T>` is available on `DocumentStoreOptions`, `LiteDbDocumentStoreOptions`, `CosmosDbDocumentStoreOptions`, `MongoDbDocumentStoreOptions`, and `IndexedDbDocumentStoreOptions`.

### Opting out per query

```csharp
// Disable every filter
var all = await store.Query<User>().IgnoreQueryFilters().ToList();

// Disable a specific named filter (others still apply)
var anyTenant = await store.Query<Order>().IgnoreQueryFilters("tenant").ToList();
```

`IgnoreQueryFilters` must be called **before** `Select(...)` — calling it on a projected query throws.

### Captured variables re-read per query

```csharp
options.AddQueryFilter<Order>("tenant", o => o.TenantId == tenantCtx.Current);

tenantCtx.Current = "acme";
await store.Query<Order>().ToList();   // filters by acme

tenantCtx.Current = "globex";
await store.Query<Order>().ToList();   // re-translated, filters by globex
```

### Filtered vs unfiltered paths

| Path | Filtered? |
|---|---|
| `Query<T>()` + all terminals, `query.NotifyOnChange()` | Yes |
| `Get<T>` / `GetDiff<T>` | Yes — returns null if filter rejects |
| `Update<T>` | Yes — throws "not found" if filter rejects |
| `SetProperty<T>` / `RemoveProperty<T>` / `Remove<T>` | Yes — returns false if filter rejects |
| `Clear<T>` / `Count<T>` | Yes |
| `Insert<T>` / `BatchInsert<T>` / `Upsert<T>` | **No** — matches EF Core |
| `Query<T>(rawSql)` / `QueryStream<T>(rawSql)` | **No** — matches EF Core's FromSqlRaw |

### Caveats

- **`JsonTypeInfo<T>` is required** for SQL providers (the filter is translated by the expression visitor). Configure a `JsonSerializerContext` on `JsonSerializerOptions` or the registered filter throws `InvalidOperationException` at first use.
- **Spatial sidecars** are not aware of per-row filter rejections on bulk Remove/Clear. Mix soft-delete with spatial cautiously.

## Native Change Feeds (IChangeFeedDocumentStore)

For changes from *any* writer (other processes, connections, store instances), use `IChangeFeedDocumentStore.SubscribeChanges<T>`. Backed by the database's native mechanism:

| Provider | Mechanism |
|---|---|
| PostgreSQL | `LISTEN` / `NOTIFY` with row-level triggers (true push) |
| SQL Server | Change Tracking, optionally with `SqlDependency` query notifications (`SqlServerChangeFeedOptions`) |
| Cosmos DB | Native Change Feed API |

Provisioning (triggers, enabling Change Tracking) is automatic and idempotent. SQLite, LiteDB, IndexedDB, MySQL, Oracle, and DuckDB throw `NotSupportedException`.

```csharp
await using var sub = await store.SubscribeChanges<User>(async (change, ct) =>
{
    // Handle each change as it arrives. Dispose `sub` to stop.
});
```

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

**No DI?** Build it straight off a hand-constructed store — same builder, returns `DocumentStoreAITools`:

```csharp
using var store = new DocumentStore(options);
var aiTools = store.CreateAITools(tools =>
    tools.AddType(jsonContext.Customer, capabilities: DocumentAICapabilities.ReadOnly));
var chatOptions = new ChatOptions { Tools = aiTools.Tools.ToList() };
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
5. **Use the fluent query builder** — `store.Query<T>().Where(...).OrderBy(...).Paginate(...).ToList()` is the primary query pattern. For UI/REST responses prefer `.PageResult(page, pageSize)` over `.Paginate(...).ToList()` + a separate `.Count()` — it returns records + total in one call.
5a. **For dynamic sort columns use `OrderBy(string, JsonTypeInfo<T>)`** — never build expressions from `Type.GetProperty(string)` yourself; the string overload resolves through source-generated `JsonTypeInfo.Properties` and stays AOT/trim-safe. Supports case-insensitive CLR or JSON names and dotted paths. When the direction is also runtime-driven, use `OrderBy(string name, string direction, JsonTypeInfo<T>)` (`asc`/`desc`/`ascending`/`descending`, empty → ascending).
5b. **For runtime filters/projections use `Where(string, JsonTypeInfo<T>)` and `Project(string, JsonTypeInfo<T>)`** — both resolve fields through `JsonTypeInfo` and never `Compile()`, staying AOT/trim-safe. `Where` parses a small expression DSL; `Project` returns `IDocumentQuery<JsonObject>` for DTO-less sparse fieldsets. When injecting runtime *values* into a filter, use the interpolated `Where($"…")` overload so `{value}` holes are bound as parameters — never concatenate values into the filter string.
6. **Use streaming for large result sets** — prefer `.ToAsyncEnumerable()` over `.ToList()` when processing results incrementally.
7. **Create indexes for frequently queried properties** — `store.CreateIndexAsync<T>(expr, jsonTypeInfo)` for up to 30x faster queries.
8. **Use `Dictionary<string, object?>` for AOT-safe raw SQL parameters** — anonymous objects work but dictionaries are fully AOT-compatible.
9. **Keep index management separate** — index methods are on `DocumentStore`, not `IDocumentStore`; cast or use the concrete type.
10. **Use `MapTypeToTable` for isolation** — when types have different lifecycles or access patterns, give them dedicated tables.
11. **Custom Id is independent of table mapping** — use `MapIdProperty<T>(x => x.Slug)` to override the Id while keeping the type in the default shared table, or `MapTypeToTable<T>(tableName, idProperty)` to do both at once.
21. **Change monitoring uses `IAsyncEnumerable`, not `IObservable`** — consume `store.NotifyOnChange<T>(ct)` with `await foreach` (or `query.NotifyOnChange(ct)` for per-query). Wrap the loop in a background `Task.Run` if you need to keep doing work while events arrive; cancel the token to unsubscribe.
22. **Distinguish in-process vs native change feeds** — `IObservableDocumentStore.NotifyOnChange<T>` only sees writes through this store instance. To observe other writers, use `IChangeFeedDocumentStore.SubscribeChanges<T>` (Postgres / SQL Server / Cosmos only).
12. **DI registration uses the extensions package** — install `Shiny.DocumentDb.Extensions.DependencyInjection` and call `services.AddDocumentStore(opts => { opts.DatabaseProvider = ...; })`. There are no provider-specific DI methods.
13. **Raw SQL is provider-specific** — LINQ expressions work identically across all providers, but raw SQL queries (`store.Query<T>("sql")`) use provider-specific JSON functions. Prefer the fluent query builder for portable code. MongoDB, LiteDB, and IndexedDB do not accept raw SQL at all.
14. **Spatial queries require `MapSpatialProperty`** — call `options.MapSpatialProperty<T>(x => x.Location)` at setup to register which `GeoPoint` property drives spatial indexing. Only SQLite and CosmosDB support spatial; other providers throw `NotSupportedException`.
15. **Backup is on concrete types, not `IDocumentStore`** — use `SqliteDocumentStore.Backup()`, `SqlCipherDocumentStore.Backup()`, or `LiteDbDocumentStore.Backup()` directly. Cast or store the concrete type.
16. **`ClearAllAsync` is SQLite-only** — available on `SqliteDocumentStore` only, deletes all documents across all tables including spatial sidecar data.
17. **Multi-tenancy uses the DI extensions package** — `AddDocumentStore(configure, multiTenant: true)` for shared-table, `AddMultiTenantDocumentStore(factory)` for tenant-per-database. Both require `ITenantResolver` to be registered.
18. **Shared-table tenancy is transparent** — consumer code injects `IDocumentStore` normally; the tenant filter is applied automatically to all queries, inserts, updates, and deletes.
19. **Tenant-per-database registers IDocumentStore as scoped** — unlike the default singleton registration. This is required so the correct tenant store is resolved per request.
