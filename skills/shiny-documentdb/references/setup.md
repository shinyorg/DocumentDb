# Store setup, dependency injection, tenancy, and AOT

Read only the sections relevant to the current task. These are application-facing API details.

## Contents

- Setup
- AOT Setup
- Provider packages and compatibility
- SQLite in Blazor WebAssembly

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

`AddDocumentStore` (in the core `Shiny.DocumentDb` package — DI registration is built in) registers `IDocumentStore` as a singleton:

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
    o.ConfigureDocument<Order>(cfg => cfg.MapVersionProperty(x => x.Version));   // opt-in optimistic concurrency (ETag-backed)
});

// Amazon DynamoDB
services.AddDynamoDbDocumentStore(o =>
{
    o.TableName       = "Documents";
    o.Region          = Amazon.RegionEndpoint.USEast1; // or o.ServiceUrl for DynamoDB Local; o.Credentials for explicit creds
    o.AutoCreateTable = true;                          // dev convenience; off by default
    o.ConsistentRead  = false;                         // default eventual consistency
    o.ConfigureDocument<Order>(cfg => cfg.MapVersionProperty(x => x.Version));
});
```

**Azure Table & DynamoDB — key facts (both are NoSQL key-partitioned stores):**
- **Key model:** the library's `(typeName, id)` identity maps to `PartitionKey/RowKey` (Table) or `pk/sk` HASH/RANGE (DynamoDB). One table holds every type; `Query<T>()` is always a single-partition scan.
- **Queries are client-side:** `Where`/`OrderBy`/`Paginate`/`Select`/aggregates evaluate in memory (the LiteDB `ExpressionInterpreter` model) after loading the type's partition. This is a **full type scan** — fine for modest per-type sets, plan hot paths accordingly.
- **Promoted columns for server-side pushdown:** `cfg.MapIndexedProperty(x => x.Status)` writes the scalar as a native top-level column/attribute. LINQ predicates over a promoted property push down into a server-side filter (Azure Table OData `$filter`, DynamoDB `FilterExpression`) to shrink the candidate set; the full predicate still re-runs client-side so results are exact. The string `Query(whereClause)` / `QueryStream` / `Count(whereClause)` overloads and `ToQueryString()` are **supported** and target promoted columns — Azure Table takes a raw **OData** `$filter` fragment, DynamoDB a **PartiQL** `WHERE` condition (reference promoted props by their CLR/JSON name). `Project(string)` is not supported.
- **Change observation:** both implement `IObservableDocumentStore.NotifyOnChange<T>` (in-process, this instance's writes) and `Query<T>().NotifyOnChange()`. **DynamoDB also** implements `IChangeFeedDocumentStore.SubscribeChanges<T>` backed by **DynamoDB Streams** (any-writer, stream auto-enabled on table create).
- **Int/Long Id auto-generation is unsupported** — a default Int/Long Id on Insert throws `NotSupportedException` (no cheap `MAX`); use Guid or string Ids, or assign the Int/Long Id explicitly. Guid/string auto-gen works.
- **Optimistic concurrency:** `MapVersionProperty<T>` uses the Table `ETag` (If-Match) or a DynamoDB conditional write on a top-level `Version` attribute → `ConcurrencyException` on conflict. Blind (unversioned) upsert is last-write-wins.
- **Not supported:** spatial, vector, full-text, temporal (`SupportsSpatial`/`SupportsVector`/`SupportsFullText` stay `false`; no `ITemporalDocumentStore`). `IDocumentMaintenance.ClearAll` **is** supported.
- **Size limits:** Azure Table caps the JSON body at ~64 KB (per-property) and DynamoDB caps an item at 400 KB — an oversized document throws a clear `NotSupportedException`, not a raw storage error.
- **Native batch:** `BatchInsert`/`BatchRemove` use native transactions/bulk writes in bounded waves (≤100 per PartitionKey on Table, ≤25 per request on DynamoDB). `store.OpenSession()` is a compensating tracker (no cross-partition atomicity), matching Cosmos.

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

Inject via `[FromKeyedServices("name")]` attribute or resolve dynamically via `IDocumentSessionFactory`:

```csharp
// Attribute injection
public class MyService(
    [FromKeyedServices("users")] IDocumentStore userStore,
    [FromKeyedServices("analytics")] IDocumentStore analyticsStore) { }

// Dynamic resolution
public class MyService(IDocumentSessionFactory stores)
{
    void DoWork() => stores.GetStore("users").Insert(...);
}
```

### Multi-Tenancy

Two isolation strategies are supported. Both use a user-implemented `ITenantResolver` to identify the current tenant. **Scope-aware (11.0):** register `ITenantResolver` **scoped** (`services.AddScoped<ITenantResolver, …>()`) and it resolves from the caller's **session DI scope** when writing/reading through a scoped `IDocumentSession`/`DocumentContext` — the request's own tenant, no ambient `IHttpContextAccessor` needed. The immediate path (`store.Insert`, no scope) falls back to the root.

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

Each tenant gets its own store, built on first use and held in a **bounded** cache. `IDocumentStore`, `IDocumentSession` and `IDocumentSessionFactory` are all registered **scoped/wired to the current tenant**.

```csharp
services.AddSingleton<ITenantResolver, HttpContextTenantResolver>();

// Relational convenience — you return options, the store is built for you
services.AddMultiTenantDocumentStore(tenantId => new DocumentStoreOptions
{
    DatabaseProvider = new SqliteDatabaseProvider($"Data Source={tenantId}.db")
});

// Any provider — you return a BUILT store
services.AddMultiTenantDocumentStore(tenantId => new MongoDbDocumentStore(new MongoDbDocumentStoreOptions
{
    ConnectionString = "mongodb://...",
    DatabaseName = $"tenant_{tenantId}"
}));

// Same consumer code — correct database selected automatically
public class OrderService(IDocumentStore store) { ... }
```

Both overloads take an optional `Action<TenantStoreOptions>`:

```csharp
services.AddMultiTenantDocumentStore(OptionsFor, o =>
{
    o.MaxCachedStores = 250;                    // default 100; LRU eviction beyond it
    o.IdleTimeout = TimeSpan.FromMinutes(30);   // default 20 min; null disables idle eviction
    o.StoreNameFactory = t => t.Split('-')[0];  // db.namespace tag; defaults to the tenant id
    o.SeedFromRegisteredSeeders();              // run AddDocumentSeeder seeders per tenant, on first touch
    o.OnTenantStoreCreated = async ctx => { /* ctx.TenantId, ctx.Store, ctx.Services, ctx.CancellationToken */ };
});
```

Rules when generating tenant-per-database code:

- **Resolve `IDocumentStore` per scope.** The cache evicts, so a store captured in a singleton can be disposed. Eviction itself is safe mid-request: the DI scope holds a lease and disposal waits for it.
- **Seeding must be per tenant.** Startup seeding is skipped for the default store under tenant routing; use `o.SeedFromRegisteredSeeders()` (versioned run-once per tenant database).
- **Provisioning is the caller's job.** DocumentDb creates tables inside a database that already exists.
- **No cross-tenant queries** — iterate tenants yourself.
- A source-generated `DocumentContext` keeps its own keyed store and is **not** tenant-routed.

Operational control via `ITenantStoreManager` (registered automatically):

```csharp
public class TenantAdmin(ITenantStoreManager tenants)
{
    public IReadOnlyCollection<string> Open => tenants.ActiveTenants;
    public Task Onboard(string id) => tenants.WarmAsync(id);    // build + initialize off the request path
    public Task Offboard(string id) => tenants.EvictAsync(id);  // close, waiting for in-flight requests
}
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
| `TableName` | `string` | `"documents"` | Default table name for all document types that do not set `cfg.Table` |
| `SkipTableInitialization` | `bool` | `false` | When `true`, never issues the lazy `CREATE TABLE IF NOT EXISTS` / index DDL that otherwise runs once per table on first touch, **including on reads**. For a read replica, an account without DDL rights, or a tool pointed at a database it must not change. The table must already exist — the first query otherwise fails with the provider's "no such table" error |
| `TypeNameResolution` | `TypeNameResolution` | `ShortName` | How type names are stored (`ShortName` or `FullName`) |
| `JsonSerializerOptions` | `JsonSerializerOptions?` | `null` | JSON serialization settings. When a `JsonSerializerContext` is attached as the `TypeInfoResolver`, all methods auto-resolve type info from the context |
| `UseReflectionFallback` | `bool` | `true` | When `false`, throws `InvalidOperationException` if a type can't be resolved from the configured `TypeInfoResolver` instead of falling back to reflection. Recommended for AOT deployments |
| `Logging` | `Action<string>?` | `null` | Callback invoked with every SQL statement executed |
| `TenantIdAccessor` | `Func<string>?` | `null` | When set, enables shared-table multi-tenancy. All queries are filtered by TenantId and all inserts include the TenantId value. A dedicated TenantId column and index are created automatically |

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

### AOT rules that change the code you generate

Every shipping package is trim/AOT warning-free and marked `IsAotCompatible`. Three rules affect generated code:

**1. Grouped projections must target a named type.** A grouped `Select` needs a `JsonTypeInfo`, and an anonymous type cannot have one — so the anonymous-type overload throws at runtime under AOT. Never generate `GroupBy(...).Select(g => new { ... })` for an AOT target:

```csharp
// WRONG under AOT — throws "This operation requires a JsonTypeInfo<<>f__AnonymousType…>"
.GroupBy(x => x.Status).Select(g => new { g.Key, Count = g.Count() })

// RIGHT — named type registered in the context, its type info passed to Select
public class StatusCount { public string Status { get; set; } = ""; public int Count { get; set; } }
// [JsonSerializable(typeof(StatusCount))] on the context

.GroupBy(x => x.Status)
.Select(g => new StatusCount { Status = g.Key, Count = g.Count() }, AppJsonContext.Default.StatusCount)
```

The projection still raises IL2026 in *caller* code (`Expression.Bind`/`Expression.New` are `RequiresUnreferencedCode` for every LINQ expression tree — EF Core is the same). It is safe once the type is in the context; suppress on the smallest enclosing method.

**2. Use a dictionary for string-query parameters.** The anonymous-type bag reads values via `GetType().GetProperties()` and is not trim-safe (`DynamicallyAccessedMembers` cannot be applied to an `object` parameter):

```csharp
// not trim-safe
parameters: new { minAge = 30 }
// AOT-safe
parameters: new Dictionary<string, object?> { ["minAge"] = 30 }
```

**3. Forwarding a generic parameter into a mapping API needs the annotation.** `cfg.MapVersionProperty`, `cfg.MapSpatialProperty`, `cfg.MapVectorProperty`, `cfg.MapFullTextProperty`, `cfg.MapComputedProperty`, `cfg.MapBlob`, and `cfg.MapBlobCollection` declare `[DynamicallyAccessedMembers(PublicProperties)]` on `T`. Passing a concrete type needs nothing; a generic helper must propagate it or it gets IL2091:

```csharp
static void Configure<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)] T>(
    DocumentStoreOptions o, Expression<Func<T, int>> version) where T : class
    => o.ConfigureDocument<T>(cfg => cfg.MapVersionProperty(version));
```

Each mapping API also has an AOT-clean overload taking a property name plus accessor delegates — prefer it when generating code that must avoid the annotation entirely.


## Provider packages and compatibility

- **Repository**: https://github.com/shinyorg/DocumentDb
- **Core namespace**: `Shiny.DocumentDb`
- **NuGet packages**:
  - `Shiny.DocumentDb` — core (abstractions, `DocumentStore`, `IDocumentStore`, expression visitor)
  - `Shiny.DocumentDb.Sqlite` — SQLite provider + DI extensions
  - `Shiny.DocumentDb.Sqlite.SqlCipher` — SQLCipher (encrypted SQLite) provider + DI extensions
  - `Shiny.DocumentDb.MySql` — MySQL provider + DI extensions
  - `Shiny.DocumentDb.MariaDb` — MariaDB provider (extends the MySQL provider; portable spatial tier, no full-text proximity; **no array-unnest queries** — `Any`/`All` over a collection, collection aggregates, and `GroupBy` over an array element throw `NotSupportedException` because MariaDB has no `JSON_TABLE`)
  - `Shiny.DocumentDb.SqlServer` — SQL Server provider + DI extensions
  - `Shiny.DocumentDb.PostgreSql` — PostgreSQL provider + DI extensions
  - `Shiny.DocumentDb.CockroachDb` — CockroachDB provider (extends the PostgreSQL provider; native spatial, full-text, and pgvector-compatible vector search — brute-force; no change-feed/bulk-copy/soundex)
  - `Shiny.DocumentDb.Oracle` — Oracle (23ai+) provider + DI extensions
  - `Shiny.DocumentDb.LiteDb` — LiteDB provider + DI extensions
  - `Shiny.DocumentDb.CosmosDb` — Azure Cosmos DB provider + DI extensions
  - `Shiny.DocumentDb.MongoDb` — MongoDB provider + DI extensions
  - `Shiny.DocumentDb.AzureTable` — Azure Table Storage (and Cosmos DB Table API) provider + `AddAzureTableDocumentStore(...)`
  - `Shiny.DocumentDb.DynamoDb` — Amazon DynamoDB provider + `AddDynamoDbDocumentStore(...)`
  - `Shiny.DocumentDb.DocumentDb` — Amazon DocumentDB provider (thin MongoDB-provider subclass; TLS + `retryWrites=false` defaults; no `$text` full-text / no vector) + `AddDocumentDbDocumentStore(...)`
  - `Shiny.DocumentDb.Redis` — Redis Stack (RedisJSON + RediSearch) provider — server-side full-text/vector(KNN)/geo, `cfg.MapIndexedProperty` push-down to `FT.SEARCH`, `INCR`-based Int/Long Id auto-gen, keyspace-notification change feed + `AddRedisDocumentStore(...)`
  - `Shiny.DocumentDb.RavenDb` — RavenDB provider (opaque STJ envelope, client-side LINQ over id-prefix streams, RQL `ToQueryString`) + `AddRavenDbDocumentStore(...)`
  - `Shiny.DocumentDb.Firestore` — Google Firestore provider (native-map storage, single-field push-down + full-scan fallback, native cursor paging, snapshot-listener change feed) + `AddFirestoreDocumentStore(...)`
  - `Shiny.DocumentDb.DuckDb` — DuckDB (embedded analytical) provider + DI extensions
  - `Shiny.DocumentDb.IndexedDb` — IndexedDB provider for Blazor WebAssembly + DI extensions
  - `Shiny.DocumentDb.Extensions.AI` — Microsoft.Extensions.AI tool surface (AIFunction tools for LLM agents)
  - **DI registration and telemetry** ship in core `Shiny.DocumentDb`; do not install the former DI-extensions or Diagnostics packages or call the removed `AddDocumentStoreInstrumentation` API.
  - `Shiny.DocumentDb.Orleans` — Microsoft Orleans grain storage (`IGrainStorage` + `PubSubStore`) over any `IDocumentStore` backend
  - `Shiny.DocumentDb.Orleans.MongoDb` / `Shiny.DocumentDb.Orleans.CosmosDb` — first-class Orleans grain-storage registration for MongoDB / Cosmos DB
- **Provider dependencies**:
  - SQLite: `Microsoft.Data.Sqlite`; the repository uses the SQLitePCLRaw 2.1.x dependency line.
  - SQLCipher: `Microsoft.Data.Sqlite.Core` + `SQLitePCLRaw.bundle_e_sqlcipher` (2.1.x). **Never add a SQLitePCLRaw 3.x package to an app that uses SQLCipher**: 3.x has no SQLCipher bundle and an app gets one SQLitePCLRaw core, so `e_sqlite3` gets installed and encrypted opens fail with "doesn't support encryption". Apps without SQLCipher may opt into 3.x via `SQLitePCLRaw.bundle_e_sqlite3` 3.0.5
  - MySQL: `MySqlConnector`
  - MariaDB: `MySqlConnector` (via the MySQL provider)
  - SQL Server: `Microsoft.Data.SqlClient`
  - PostgreSQL: `Npgsql`
  - CockroachDB: `Npgsql` (via the PostgreSQL provider)
  - Oracle: `Oracle.ManagedDataAccess.Core` (requires Oracle Database 23ai+)
  - LiteDB: `LiteDB`
  - CosmosDB: `Microsoft.Azure.Cosmos`
  - MongoDB: `MongoDB.Driver`
  - DuckDB: `DuckDB.NET.Data.Full`
  - IndexedDB: `Microsoft.JSInterop` (browser JS interop)
- **AI dependency**: `Microsoft.Extensions.AI.Abstractions`
- **Target**: `net10.0`

## SQLite in Blazor WebAssembly

The SQLite provider (`Shiny.DocumentDb.Sqlite`) is compatible with Blazor WebAssembly when paired with `SQLitePCLRaw.bundle_wasm`. The provider automatically adapts at runtime:

- **WAL pragma skipped** — `SqliteDatabaseProvider` checks `OperatingSystem.IsBrowser()` and skips the WAL journal mode pragma (not applicable on the Emscripten virtual filesystem)
- **Spatial disabled** — `SupportsSpatial` returns `false` in the browser because R*Tree virtual tables are unavailable in WASM-compiled SQLite
- **Backup unsupported** — `SqliteDocumentStore.Backup()` is marked `[UnsupportedOSPlatform("browser")]` and will produce a compiler warning if called from browser-targeted code
- **Connection strings** — use `Data Source=:memory:` for in-memory storage or Emscripten OPFS-mounted paths for persistence

All other features (LINQ queries, JSON indexes, table-per-type mapping, transactions, batch insert, aggregates, projections) work identically in WASM.

> **Tip:** For most Blazor WASM client-side storage, the lighter **IndexedDB provider** (`Shiny.DocumentDb.IndexedDb`) is recommended — no native WASM binary needed. Choose SQLite-in-WASM only when you need raw SQL queries or JSON indexes; spatial queries are unavailable in this browser configuration.
