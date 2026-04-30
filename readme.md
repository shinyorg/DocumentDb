# Shiny.DocumentDb

[![NuGet](https://img.shields.io/nuget/v/Shiny.DocumentDb.svg?label=Core)](https://www.nuget.org/packages/Shiny.DocumentDb/)
[![NuGet](https://img.shields.io/nuget/v/Shiny.DocumentDb.Sqlite.svg?label=SQLite)](https://www.nuget.org/packages/Shiny.DocumentDb.Sqlite/)
[![NuGet](https://img.shields.io/nuget/v/Shiny.DocumentDb.Sqlite.SqlCipher.svg?label=SQLCipher)](https://www.nuget.org/packages/Shiny.DocumentDb.Sqlite.SqlCipher/)
[![NuGet](https://img.shields.io/nuget/v/Shiny.DocumentDb.MySql.svg?label=MySQL)](https://www.nuget.org/packages/Shiny.DocumentDb.MySql/)
[![NuGet](https://img.shields.io/nuget/v/Shiny.DocumentDb.SqlServer.svg?label=SQL+Server)](https://www.nuget.org/packages/Shiny.DocumentDb.SqlServer/)
[![NuGet](https://img.shields.io/nuget/v/Shiny.DocumentDb.PostgreSql.svg?label=PostgreSQL)](https://www.nuget.org/packages/Shiny.DocumentDb.PostgreSql/)
[![NuGet](https://img.shields.io/nuget/v/Shiny.DocumentDb.Extensions.DependencyInjection.svg?label=DI+Extensions)](https://www.nuget.org/packages/Shiny.DocumentDb.Extensions.DependencyInjection/)
[![NuGet](https://img.shields.io/nuget/v/Shiny.DocumentDb.Extensions.AI.svg?label=AI+Extensions)](https://www.nuget.org/packages/Shiny.DocumentDb.Extensions.AI/)

A lightweight, multi-provider document store for .NET that turns relational databases into a schema-free JSON document database with LINQ querying and full AOT/trimming support. Supports **SQLite**, **SQLCipher** (encrypted SQLite), **MySQL**, **SQL Server**, and **PostgreSQL**.

**[Documentation](https://shinylib.net/sqlite-docdb)**

## Features

- **Zero schema, zero migrations** — store entire object graphs (nested objects, child collections) as JSON documents. No `CREATE TABLE`, no `ALTER TABLE`, no JOINs.
- **Multiple database providers** — use SQLite for mobile/embedded, MySQL, SQL Server, or PostgreSQL for server workloads. Same API, same LINQ expressions, different backend.
- **Fluent query builder** — `store.Query<User>().Where(u => u.Age > 30).OrderBy(u => u.Name).Paginate(0, 20).ToList()` with full LINQ expression support for nested properties, `Any()`, `Count()`, string methods, null checks, and captured variables.
- **`IAsyncEnumerable<T>` streaming** — yield results one-at-a-time with `.ToAsyncEnumerable()` instead of buffering into a list. Eliminates Gen1 GC pressure at scale with comparable throughput.
- **Expression-based JSON indexes** — `store.CreateIndexAsync<User>(u => u.Name, ctx.User)` creates a partial JSON index on the property. Up to **30x faster** queries on indexed properties. (SQLite uses `json_extract`; other providers use native JSON indexing.)
- **SQL-level projections** — project into DTOs with `json_object` at the database level via `.Select()`. No full document deserialization needed.
- **Full AOT/trimming support** — every API has an optional `JsonTypeInfo<T>` parameter for source-generated JSON serialization. No reflection required. Configure a `JsonSerializerContext` once and all methods auto-resolve type info — no per-call `JsonTypeInfo<T>` needed. Set `UseReflectionFallback = false` to catch missing type registrations with clear exceptions instead of opaque AOT failures.
- **10-30x faster nested inserts** vs sqlite-net — one write per document vs multiple table inserts with foreign keys. 2-10x faster reads on nested data.
- **Mandatory typed Id property** — every document type must have a `public {Guid|int|long|string} Id { get; set; }` property. Ids are auto-generated when default (Guid.Empty, 0, null/empty string) and written back to the object. The Id lives in both the SQLite column and the JSON blob, so query results always include it.
- **JSON Merge Patch (Upsert)** — `store.Upsert(patch)` deep-merges a partial object into an existing document using SQLite's `json_patch()` (RFC 7396). The Id comes from the object. Only patched fields are overwritten; unset nullable fields are preserved.
- **Surgical field updates** — `store.SetProperty<User>("id", u => u.Age, 31)` updates a single JSON field via `json_set()` without deserializing the document. `store.RemoveProperty<User>("id", u => u.Email)` strips a field via `json_remove()`. Both support nested paths like `o => o.ShippingAddress.City`.
- **Document diff (JsonPatchDocument)** — `store.GetDiff("id", modified)` compares an object against the stored document and returns an RFC 6902 `JsonPatchDocument<T>` with deep nested-object diffing. Powered by [SystemTextJsonPatch](https://www.nuget.org/packages/SystemTextJsonPatch).
- **Typed Id lookups** — `Get`, `Remove`, `SetProperty`, and `RemoveProperty` accept the Id as `object` so you can pass a `Guid`, `int`, `long`, or `string` directly. Unsupported types throw `ArgumentException`.
- **Pagination** — `store.Query<User>().OrderBy(u => u.Name).Paginate(0, 20).ToList()` translates to SQL `LIMIT`/`OFFSET`.
- **Transactions** — `store.RunInTransaction(async tx => { ... })` with automatic commit/rollback.
- **Batch insert** — `store.BatchInsert(items)` inserts a collection in a single transaction with prepared command reuse. Auto-generates IDs and rolls back atomically on failure.
- **Hot backup** — `store.Backup("/path/to/backup.db")` copies the database to a file using the SQLite Online Backup API while the store remains usable. (SQLite only.)
- **AI tool integration** — `Shiny.DocumentDb.Extensions.AI` exposes `IDocumentStore` operations as `Microsoft.Extensions.AI` tool functions for LLM agents. Register document types with per-type capability flags (`ReadOnly`, `All`, or individual operations), structured filter expressions with boolean combinators, field visibility control (`AllowProperties`/`IgnoreProperties`), and page size caps. Resolve `DocumentStoreAITools` from DI and pass `.Tools` to any `IChatClient`.

## Comparison with alternatives

| | Shiny.DocumentDb | Microsoft.Data.Sqlite (raw ADO.NET) | sqlite-net-pcl |
|---|---|---|---|
| **Schema management** | Zero — just store objects | You write every `CREATE TABLE`, `ALTER TABLE`, migration | Auto-creates flat tables from POCOs |
| **Database providers** | SQLite, MySQL, SQL Server, PostgreSQL | SQLite only | SQLite only |
| **Nested objects & child collections** | Stored and queried as a single JSON document | Must design normalized tables, write JOINs, manage foreign keys | No support — flat columns only, child collections require separate tables + manual joins |
| **LINQ queries on nested data** | `store.Query<Order>().Where(o => o.Lines.Any(l => l.Price > 10)).ToList()` | Hand-written `json_extract` SQL | Not possible on nested data |
| **AOT / trimming** | First-class optional `JsonTypeInfo<T>` on every API | Manual — you control all SQL | Relies on reflection; no AOT support |
| **Migrations** | Not needed — schema-free JSON | You own every migration | You own every migration |
| **Projections** | SQL-level `json_object` projections via `.Select()` | Manual SQL | Not available |
| **Transactions** | `store.RunInTransaction(async tx => ...)` | Manual `BeginTransaction` + `Commit`/`Rollback` | `RunInTransactionAsync` available |
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
| **Database support** | Many providers | SQLite, MySQL, SQL Server, PostgreSQL |
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

If you are building a .NET MAUI app and need local data persistence, this library gives you a queryable document store that works under full AOT and trimming without compromise.

## Benchmarks

Measured with [BenchmarkDotNet](https://benchmarkdotnet.org/) v0.15.8 on Apple M2, .NET 10.0.3, macOS. Full source in [`benchmarks/`](benchmarks/).

### Flat POCO (single table)

#### Insert

| Method | Count | Mean |
|---|---|---|
| DocumentStore Insert | 10 | 572 us |
| sqlite-net Insert | 10 | 3.02 ms |
| DocumentStore Insert | 100 | 5.24 ms |
| sqlite-net Insert | 100 | 26.36 ms |
| DocumentStore Insert | 1000 | 52.52 ms |
| sqlite-net Insert | 1000 | 260.29 ms |

#### Batch insert

| Method | Count | Mean | Allocated |
|---|---|---|---|
| DocumentStore BatchInsert | 10 | 218 us | 14.02 KB |
| sqlite-net InsertAllAsync | 10 | 412 us | 5.16 KB |
| DocumentStore BatchInsert | 100 | 585 us | 117.14 KB |
| sqlite-net InsertAllAsync | 100 | 735 us | 44.92 KB |
| DocumentStore BatchInsert | 1000 | 6,399 us | 1,104 KB |
| sqlite-net InsertAllAsync | 1000 | 3,072 us | 439 KB |

> BatchInsert is ~2x faster at small-to-medium batch sizes (10–100 items) thanks to prepared command reuse in a single transaction. At 1000 items, sqlite-net's simpler row structure and lower serialization overhead give it the edge.

#### Get by ID

| Method | Mean | Allocated |
|---|---|---|
| DocumentStore GetById | 3.75 us | 1.99 KB |
| sqlite-net GetById | 16.18 us | 3.73 KB |

#### Get all

| Method | Count | Mean | Allocated |
|---|---|---|---|
| DocumentStore GetAll | 100 | 46.10 us | 48.47 KB |
| sqlite-net GetAll | 100 | 80.27 us | 28.37 KB |
| DocumentStore GetAll | 1000 | 437.16 us | 470.35 KB |
| sqlite-net GetAll | 1000 | 464.82 us | 246.35 KB |

#### Query (filter by name, 1000 records)

| Method | Mean | Allocated |
|---|---|---|
| DocumentStore Query | 269.75 us | 4.86 KB |
| sqlite-net Query | 59.20 us | 5.33 KB |

> sqlite-net is faster for simple indexed-column queries because it queries column values directly, while the document store must use `json_extract`. The document store shines with nested data (see below).

### Nested objects with child collections (Order + Address + OrderLines + Tags)

This is where the document store architecture pays off. sqlite-net requires 3 tables, 6 inserts per order, and 3 queries per read with manual rehydration.

#### Insert (nested)

| Method | Count | Mean |
|---|---|---|
| DocumentStore Insert (nested) | 10 | 686 us |
| sqlite-net Insert (3 tables) | 10 | 17.26 ms |
| DocumentStore Insert (nested) | 100 | 5.69 ms |
| sqlite-net Insert (3 tables) | 100 | 176.48 ms |
| DocumentStore Insert (nested) | 1000 | 55.62 ms |
| sqlite-net Insert (3 tables) | 1000 | 2.58 s |

#### Get by ID (nested)

| Method | Mean | Allocated |
|---|---|---|
| DocumentStore GetById (nested) | 5.04 us | 3.88 KB |
| sqlite-net GetById (3 queries) | 48.26 us | 16.05 KB |

#### Get all (nested)

| Method | Count | Mean | Allocated |
|---|---|---|---|
| DocumentStore GetAll (nested) | 100 | 148 us | 237 KB |
| sqlite-net GetAll (3 tables + rehydrate) | 100 | 326 us | 159 KB |
| DocumentStore GetAll (nested) | 1000 | 1.67 ms | 2,353 KB |
| sqlite-net GetAll (3 tables + rehydrate) | 1000 | 2.75 ms | 1,438 KB |

#### Query (nested, filter by status)

| Method | Mean | Allocated |
|---|---|---|
| DocumentStore Query (nested, by status) | 1.45 ms | 1,180 KB |
| sqlite-net Query (3 tables + rehydrate) | 2.27 ms | 1,013 KB |

> For nested data, the document store is **10-30x faster on inserts** and **2-10x faster on reads** because it stores/retrieves the entire object graph in a single operation vs. multiple table writes and JOINs.

### Index impact

JSON property indexes (`CreateIndexAsync`) dramatically speed up equality queries by letting SQLite use a B-tree lookup instead of scanning every row with `json_extract`.

#### Flat POCO query (filter by name, 1000 records)

| Method | Mean | Allocated |
|---|---|---|
| Query without index | 270 us | 4.71 KB |
| Query with index | 8.52 us | 4.71 KB |

> **~32x faster** — the indexed query resolves in microseconds because SQLite uses the partial index directly.

#### Nested query (filter by ShippingAddress.City, 1000 records, ~200 matches)

| Method | Mean | Allocated |
|---|---|---|
| Nested query without index | 992 us | 473 KB |
| Nested query with index | 326 us | 473 KB |

> **~3x faster** — the index eliminates the full table scan, but read + deserialize time for ~200 matching documents dominates. Indexes give the biggest wins on selective queries that return few results.

### Streaming (IAsyncEnumerable) vs buffered

Streaming yields results one-at-a-time without building an intermediate `List<T>`. Throughput is comparable; the benefit is reduced peak memory and eliminating Gen1 GC pressure at larger scales.

#### Flat POCO

| Method | Count | Mean | Gen1 | Allocated |
|---|---|---|---|---|
| ToList (buffered) | 100 | 46.26 us | 0.49 | 48.47 KB |
| ToAsyncEnumerable (streaming) | 100 | 47.07 us | — | 46.35 KB |
| ToList (buffered) | 1000 | 439.63 us | 21.00 | 470.35 KB |
| ToAsyncEnumerable (streaming) | 1000 | 456.58 us | — | 454.16 KB |

#### Nested objects

| Method | Count | Mean | Gen1 | Allocated |
|---|---|---|---|---|
| ToList nested (buffered) | 100 | 147.80 us | 6.84 | 236.67 KB |
| ToAsyncEnumerable nested (streaming) | 100 | 150.50 us | 0.24 | 234.55 KB |
| ToList nested (buffered) | 1000 | 1.62 ms | 134.77 | 2,353 KB |
| ToAsyncEnumerable nested (streaming) | 1000 | 1.43 ms | 1.95 | 2,337 KB |

#### Nested query (filter by status, ~500 matches from 1000)

| Method | Mean | Gen1 | Allocated |
|---|---|---|---|
| Query Where ToList (buffered) | 1.41 ms | 70.31 | 1,180 KB |
| Query Where ToAsyncEnumerable (streaming) | 1.39 ms | — | 1,172 KB |

> Streaming eliminates Gen1 GC collections entirely at scale. Throughput is within ~2% of buffered. Use streaming when you process results incrementally rather than needing the full list upfront.

## Installation

Install the core package plus the provider for your database:

```bash
# SQLite (mobile, embedded, local)
dotnet add package Shiny.DocumentDb.Sqlite

# SQLCipher (encrypted SQLite)
dotnet add package Shiny.DocumentDb.Sqlite.SqlCipher

# MySQL
dotnet add package Shiny.DocumentDb.MySql

# SQL Server
dotnet add package Shiny.DocumentDb.SqlServer

# PostgreSQL
dotnet add package Shiny.DocumentDb.PostgreSql
```

Each provider package includes dependency injection extensions. A standalone DI package is also available for provider-agnostic registration:

```bash
# Generic DI extensions (bring your own provider)
dotnet add package Shiny.DocumentDb.Extensions.DependencyInjection
```

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
```

> **Note:** `SqliteDocumentStore` and `SqlCipherDocumentStore` are still available as convenience wrappers that extend `DocumentStore`. They accept a connection string directly: `new SqliteDocumentStore("Data Source=mydata.db")` or `new SqlCipherDocumentStore("encrypted.db", "mySecretKey")`.

### Options reference

| Property | Type | Default | Description |
|---|---|---|---|
| `DatabaseProvider` | `IDatabaseProvider` | (required) | The database provider to use (e.g., `SqliteDatabaseProvider`, `MySqlDatabaseProvider`, `SqlServerDatabaseProvider`, `PostgreSqlDatabaseProvider`) |
| `TypeNameResolution` | `TypeNameResolution` | `ShortName` | How type names are stored — `ShortName` (e.g. `User`) or `FullName` (e.g. `MyApp.Models.User`) |
| `JsonSerializerOptions` | `JsonSerializerOptions?` | `null` | JSON serialization settings. When a `JsonSerializerContext` is attached as the `TypeInfoResolver`, all methods auto-resolve type info from the context |
| `UseReflectionFallback` | `bool` | `true` | When `false`, throws `InvalidOperationException` if a type can't be resolved from the configured `TypeInfoResolver` instead of falling back to reflection. Recommended for AOT deployments |
| `TableName` | `string` | `"documents"` | Name of the default shared document table. Types not explicitly mapped via `MapTypeToTable<T>()` are stored here |
| `Logging` | `Action<string>?` | `null` | Callback invoked with every SQL statement executed |

### Dependency injection

Each provider package includes its own DI extension method:

```csharp
// SQLite
using Shiny.DocumentDb.Sqlite;
services.AddSqliteDocumentStore("Data Source=mydata.db");

// SQLCipher (encrypted SQLite)
using Shiny.DocumentDb.Sqlite.SqlCipher;
services.AddSqlCipherDocumentStore("encrypted.db", "mySecretKey");

// MySQL
using Shiny.DocumentDb.MySql;
services.AddMySqlDocumentStore("Server=localhost;Database=mydb;User=root;Password=pass");

// SQL Server
using Shiny.DocumentDb.SqlServer;
services.AddSqlServerDocumentStore("Server=localhost;Database=mydb;Trusted_Connection=true");

// PostgreSQL
using Shiny.DocumentDb.PostgreSql;
services.AddPostgreSqlDocumentStore("Host=localhost;Database=mydb;Username=postgres;Password=pass");

// All providers support full options configuration
services.AddSqliteDocumentStore(opts =>
{
    opts.DatabaseProvider = new SqliteDatabaseProvider("Data Source=mydata.db");
    opts.TypeNameResolution = TypeNameResolution.FullName;
    opts.JsonSerializerOptions = new JsonSerializerOptions
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase
    };
});
```

#### Generic (provider-agnostic) registration

If you want to register the document store without depending on a specific provider package, use the standalone DI extensions package:

```csharp
using Shiny.DocumentDb;

services.AddDocumentStore(opts =>
{
    opts.DatabaseProvider = new SqliteDatabaseProvider("Data Source=mydata.db");
    opts.TypeNameResolution = TypeNameResolution.FullName;
});
```

This is useful when the provider is determined at runtime or when building libraries that accept any `IDatabaseProvider`.

All DI methods register `IDocumentStore` as a singleton.

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

Types mapped to a dedicated table can use an alternate property as the document Id instead of the default `Id`. The Id property must be `Guid`, `int`, `long`, or `string`.

```csharp
var store = new DocumentStore(new DocumentStoreOptions
{
    DatabaseProvider = new SqliteDatabaseProvider("Data Source=mydata.db")
}.MapTypeToTable<Customer>("customers", c => c.CustomerId)
 .MapTypeToTable<Sensor>("sensors", s => s.DeviceKey)
);
```

Auto-generation rules still apply — `Guid` and numeric Ids are auto-generated when default, and the value is written back to the property after insert.

### API reference

| Overload | Description |
|---|---|
| `MapTypeToTable<T>()` | Auto-derive table name, default `Id` property |
| `MapTypeToTable<T>(tableName)` | Explicit table name, default `Id` property |
| `MapTypeToTable<T>(idProperty)` | Auto-derive table name, custom Id property |
| `MapTypeToTable<T>(tableName, idProperty)` | Explicit table name, custom Id property |

- **Fluent** — all overloads return `DocumentStoreOptions` for chaining
- **Duplicate protection** — mapping two types to the same table throws `ArgumentException`
- **AOT-safe** — type and property names are resolved at registration time, not at runtime
- **Id remapping requires a table mapping** — custom Id properties are only available through `MapTypeToTable`
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

// Inside a transaction — uses the existing transaction (no nesting)
await store.RunInTransaction(async tx =>
{
    await tx.BatchInsert(moreUsers);
    await tx.Insert(singleUser);
    // All committed or rolled back together
});
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

## Fluent Query Builder

The fluent query builder is the primary way to query, filter, sort, paginate, project, aggregate, stream, and delete documents. Start with `store.Query<T>()` and chain builder methods, then terminate with a materialization method.

### Builder methods (non-executing)

| Method | Description |
|---|---|
| `.Where(predicate)` | Filter by LINQ expression. Multiple calls combine with AND. |
| `.OrderBy(selector)` | Sort ascending by property. |
| `.OrderByDescending(selector)` | Sort descending by property. |
| `.GroupBy(selector)` | Group by property (for aggregate projections with `Sql.*` markers). |
| `.Paginate(offset, take)` | Limit results with SQL `LIMIT`/`OFFSET`. |
| `.Select(selector, resultTypeInfo?)` | Project into a different shape via `json_object`. |

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

> **Note:** The streaming methods hold the internal semaphore (or database reader) for the duration of enumeration. Consume the results promptly and avoid interleaving other store operations within the same `await foreach` loop.

### Raw SQL queries

For advanced queries not covered by expressions, use raw SQL with provider-specific JSON functions. The SQL syntax varies by provider:

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

## Transactions

```csharp
await store.RunInTransaction(async tx =>
{
    await tx.Insert(new User { Id = "u1", Name = "Alice", Age = 25 });
    await tx.Insert(new User { Id = "u2", Name = "Bob", Age = 30 });
    // Commits on success, rolls back on exception
});
```

## Rekeying (SQLCipher only)

Change the encryption key of an existing SQLCipher database using the `RekeyAsync` extension method on `IDocumentStore`. This issues `PRAGMA rekey` under the hood. Throws `InvalidOperationException` if the store is not using `SqlCipherDatabaseProvider`.

```csharp
using Shiny.DocumentDb.Sqlite.SqlCipher;

await store.RekeyAsync("newPassword");
```

> **Important:** After rekeying, the store still holds the old password internally. Create a new store with the new password for subsequent operations.

## Backup (SQLite/SQLCipher only)

Creates a hot backup of the database to a file using the SQLite Online Backup API. The store remains fully usable during the backup. Not supported inside a transaction. Only available when using `SqliteDocumentStore` or `SqlCipherDocumentStore`. When using SQLCipher, the backup database is automatically encrypted with the same password.

```csharp
await store.Backup("/path/to/backup.db");
```

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
