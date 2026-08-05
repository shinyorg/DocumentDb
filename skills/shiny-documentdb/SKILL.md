--
name: shiny-documentdb
description: Generate code using Shiny.DocumentDb, a schema-free multi-provider JSON document store for .NET supporting SQLite, LiteDB, CosmosDB, MongoDB, Azure Table Storage, Amazon DynamoDB, Amazon DocumentDB, Redis, RavenDB, Google Firestore, DuckDB, IndexedDB (Blazor WASM), MySQL, MariaDB, SQL Server, PostgreSQL, CockroachDB, and Oracle with LINQ queries, spatial/geo queries, and AOT support
auto_invoke: true
triggers:
  - document store
  - outbox
  - transactional outbox
  - AddOutbox
  - AddDocumentOutbox
  - OutboxMessage
  - IOutboxDispatcher
  - IOutboxAdmin
  - OutboxRunner
  - OutboxOptions
  - OutboxFields
  - WatchOutbox
  - PublishToOutbox
  - Enqueue
  - domain events
  - integration events
  - dead letter
  - requeue
  - SupportsTransactions
  - ConfigureDocument
  - DocumentTypeBuilder
  - DocumentModelBuilder
  - DocumentConfigurationValidator
  - field encryption mapping
  - field-level encryption
  - encrypt property
  - IDocumentEncryptor
  - AesGcmDocumentEncryptor
  - DocumentEncryption
  - DocumentEncryptionFormat
  - EncryptedValueInfo
  - PlaintextView
  - FirstOrDefault
  - SingleOrDefault
  - IDocumentUpdateBuilder
  - ToJsonList
  - ToJsonAsyncEnumerable
  - FirstOrDefaultJson
  - FirstOrDefaultRawJson
  - ToRawJsonAsyncEnumerable
  - WriteJsonArrayTo
  - ToJsonCursorPage
  - RawJsonRows
  - SupportsRawJson
  - raw JSON result
  - return JSON without deserializing
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
  - VectorData
  - VectorStore
  - VectorStoreCollection
  - MapVectorRecord
  - AddDocumentDbVectorStore
  - DocumentDbVectorStore
  - VectorStoreKey
  - VectorStoreVector
  - MEVD
  - Semantic Kernel
  - AutoEmbedOnInsert
  - auto-embed
  - IEmbeddingGenerator
  - OnBeforeWrite
  - full-text search
  - FullTextSearch
  - FullTextMatch
  - MapFullTextProperty
  - FullTextResult
  - FullTextLanguage
  - LuceneMatch
  - LuceneScore
  - lucene
  - FTS5
  - tsvector
  - computed property
  - computed column
  - MapComputedProperty
  - derived property
  - generated column
  - blob
  - DocumentBlob
  - DocumentBlobCollection
  - MapBlob
  - MapBlobCollection
  - attachment
  - binary payload
  - IBlobDocumentStore
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
  - MariaDbDatabaseProvider
  - SqlServerDatabaseProvider
  - PostgreSqlDatabaseProvider
  - CockroachDbDatabaseProvider
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
  - ToCursorPage
  - CursorPage
  - ToCursorStream
  - cursor pagination
  - keyset pagination
  - seek pagination
  - infinite scroll
  - dynamic sort
  - sort by string
  - OrderBy string
  - Where string
  - dynamic filter
  - GroupBy
  - grouped aggregation
  - IGroupedDocumentQuery
  - IDocumentGroup
  - Having
  - Sql.Count
  - Sql.Sum
  - Sql.Avg
  - interpolated filter
  - FilterInterpolatedStringHandler
  - cfg.Table
  - table per type
  - GetDiff
  - JsonPatchDocument
  - IDocumentSession
  - OpenSession
  - IDocumentSessionFactory
  - AddScopedDocumentSession
  - IDocumentTransaction
  - BeginTransaction
  - LockMode
  - SaveChanges
  - unit of work
  - transaction
  - atomic writes
  - IDocumentInterceptor
  - IDocumentBulkInterceptor
  - ctx.Services
  - ctx.Store
  - ctx.Session
  - OnBeforeWrite
  - OnAfterWrite
  - interceptor
  - write interceptor
  - ctx.Cancel
  - DocumentQueryBase
  - QueryPlan
  - QueryExecution
  - IDocumentStoreOptions
  - custom provider
  - write a provider
  - Cancel interceptor
  - cancel a write
  - replace a write
  - QueryAs
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
  - DocumentDbDocumentStore
  - DocumentDbDocumentStoreOptions
  - Shiny.DocumentDb.DocumentDb
  - AddDocumentDbDocumentStore
  - Amazon DocumentDB
  - RedisDocumentStore
  - RedisDocumentStoreOptions
  - Shiny.DocumentDb.Redis
  - AddRedisDocumentStore
  - Redis
  - Redis Stack
  - RedisJSON
  - RediSearch
  - RavenDbDocumentStore
  - RavenDbDocumentStoreOptions
  - Shiny.DocumentDb.RavenDb
  - AddRavenDbDocumentStore
  - RavenDB
  - FirestoreDocumentStore
  - FirestoreDocumentStoreOptions
  - Shiny.DocumentDb.Firestore
  - AddFirestoreDocumentStore
  - Firestore
  - Google Firestore
  - DynamoDB Streams
  - MapIndexedProperty
  - promoted column
  - PartiQL
  - ToCollection
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
  - reference geo data
  - GeoRegion
  - GeoCity
  - GeoDataSets
  - AddGeoReferenceSeeder
  - Shiny.DocumentDb.Geo
  - cities states provinces
  - Geometry
  - GeoLineString
  - GeoPolygon
  - GeoMultiPoint
  - GeoMultiLineString
  - GeoMultiPolygon
  - GeoGeometryCollection
  - GeoIntersects
  - GeoContainedBy
  - GeoContains
  - GeoDisjoint
  - GeoTouches
  - GeoCrosses
  - GeoOverlaps
  - GeoEquals
  - GeoCovers
  - GeoCoveredBy
  - GeoWithinDistance
  - DocumentFunctions
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
  - IDocumentSessionFactory
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
  - AI access filter
  - non-removable filter
  - multi-tenant
  - multi-tenancy
  - tenant
  - ITenantResolver
  - TenantIdAccessor
  - AddMultiTenantDocumentStore
  - MCP
  - Model Context Protocol
  - Claude Desktop
  - agent access
  - MapDocuments
  - MapDocumentCollection
  - minimal API
  - REST endpoints
  - SSE
  - server-sent events
  - live query
  - TenantStoreOptions
  - ITenantStoreManager
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
  - AddSoftDelete
  - IncludeDeleted
  - OnlyDeleted
  - PurgeDeleted
  - HardDelete
  - Restore deleted
  - IsDeleted flag
  - DeletedAt
  - undelete
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
  - AddDocumentContextProvider
  - CreateAITools
  - Collection(
  - IJsonDocumentCollection
  - IJsonDocumentQuery
  - json collection
  - schema-free collection
  - dynamic document
  - untyped document
  - no CLR type
  - name-keyed collection
  - type hint
  - AddDocumentDbAdmin
  - ShinyDocDbMyAdmin
  - shiny-docdb-myadmin
  - admin ui
  - ShinyDocDbMyAdmin.Tui
  - shinydocdb
  - terminal ui
  - admin tui
  - docker desktop extension
  - shiny-docdb-myadmin-extension
  - outbox screen
  - outbox_status
  - vector sidecar
  - VectorTableName
  - stale embedding
  - SkipTableInitialization
  - read replica
  - filter console
  - BuildExplainSql
  - query plan
  - composite index
---

# Shiny DocumentDb Skill

You are an expert in Shiny.DocumentDb, a lightweight multi-provider document store for .NET that turns relational databases into a schema-free JSON document database with LINQ querying, spatial/geo queries, and full AOT/trimming support. Supports **SQLite**, **SQLCipher** (encrypted SQLite), **LiteDB**, **CosmosDB**, **MongoDB**, **Amazon DocumentDB** (MongoDB-compatible), **Redis** (Redis Stack), **RavenDB**, **Google Firestore**, **Azure Table Storage** (and Cosmos DB Table API), **Amazon DynamoDB**, **DuckDB**, **IndexedDB** (Blazor WebAssembly), **MySQL**, **MariaDB**, **SQL Server**, **PostgreSQL**, **CockroachDB**, and **Oracle**.

## Upgrading from v10 → v11

**Only relevant when a codebase is on v10** — do this migration when the user asks to upgrade, or when you see
v10-only signals in the code: `CreateUnitOfWork(`, the public `UnitOfWork` type in a signature,
`IDocumentStoreProvider`, `AddDocumentStoreInstrumentation(` / `o.Instrumentation = true`, or a `PackageReference`
to `Shiny.DocumentDb.Extensions.DependencyInjection` / `Shiny.DocumentDb.Diagnostics`. (Do **not** treat normal
v11 API — `IDocumentSession`, `OpenSession`, `AddScopedDocumentSession` — as a migration signal.)

v11 is a clean break (no `[Obsolete]` shims). Apply these mechanical transforms:

| v10 | v11 |
|---|---|
| `PackageReference` to `…Extensions.DependencyInjection` / `…Diagnostics` | **remove both** — folded into core `Shiny.DocumentDb`, namespaces unchanged |
| `var uow = store.CreateUnitOfWork();` | `await using var session = store.OpenSession();` (buffered `Add`/`Update`/`Upsert`/`Remove` + `SaveChanges` identical) |
| `uow.Clear()` | `session.ClearPending()` |
| `UnitOfWork` in a signature (`Action<UnitOfWork,…>`, `void F(UnitOfWork uow)`) | `IDocumentSession` (the public type is now internal) |
| `IDocumentStoreProvider` | `IDocumentSessionFactory` (`GetStore(name)` unchanged; `OpenSession(name)` added) |
| `new AppContext(store)` (generated `DocumentContext`) | `new AppContext(store.OpenSession())`; the context is now `IAsyncDisposable` — `await using` it; `context.CreateUnitOfWork()` → `context.Add`/`SaveChanges` |
| `services.AddDocumentStoreInstrumentation();` / `o.Instrumentation = true` | remove — telemetry is embedded/always-on; subscribe OTel to `.AddSource("Shiny.DocumentDb")` / `.AddMeter("Shiny.DocumentDb")` |

**Session registration by host:** ASP.NET Core → add `.AddScopedDocumentSession()` and inject scoped
`IDocumentSession`; MAUI/desktop/background/Orleans → inject the singleton `IDocumentSessionFactory` and
`OpenSession()` per unit of work (`await using`); immediate one-offs → keep injecting `IDocumentStore`. Ask the
user which host if it isn't obvious. Interceptors don't break (v11 adds `ctx.Services`/`ctx.Session` + scoped
support). **After migrating, build (expect 0 errors) and run the FULL test suite** (needs Docker for non-SQLite
providers; if Docker is off, tell the user to start it). Full guide + before/after code:
[`/documentdb/migrate-v10-to-v11`](https://shinylib.net/documentdb/migrate-v10-to-v11).

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
- Roll up one row per key with `GroupBy(keySelector).Having(…).Select(g => …)` (`g.Key`, `g.Count()`, `g.Sum`)
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
- Configure spatial indexing for `GeoPoint` properties (`cfg.MapSpatialProperty`)
- Use SQLite R*Tree spatial indexes or CosmosDB native GeoJSON queries
- Use optimistic concurrency with document-level version properties (`cfg.MapVersionProperty`)
- Override the document Id property (`cfg.MapIdProperty`) without dedicating a table
- Observe in-process document changes as an `IAsyncEnumerable<DocumentChange<T>>` (`IObservableDocumentStore.NotifyOnChange<T>`)
- Watch a single document by Id (`WhenDocumentChanged<T>(id)`)
- Monitor changes filtered by a query's predicates (`store.Query<T>().Where(...).NotifyOnChange()`)
- Consume native database change feeds across writers (`IChangeFeedDocumentStore.SubscribeChanges<T>`)
- Register global query filters (`AddQueryFilter<T>`) — row-level security, "active only" scopes (EF Core's `HasQueryFilter` equivalent)
- Selectively disable filters with `IgnoreQueryFilters()` or `IgnoreQueryFilters("name")` per query
- Turn deletes into flag updates with soft delete (`AddSoftDelete<T>` + `IncludeDeleted`/`OnlyDeleted`/`Restore`/`PurgeDeleted`/`HardDelete`)
- Replace a write from an interceptor (`ctx.Cancel()`) — e.g. delete → update — instead of only observing it
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
- Enforce a non-removable, per-type row-level scope on AI tools (`Where`) the LLM cannot bypass
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
  - `Shiny.DocumentDb.Geo` — embedded reference geography (US states, Canadian provinces, US & Canadian cities) as `GeoRegion`/`GeoCity` documents; provider-agnostic seeder (`AddGeoReferenceSeeder()` + `opts.MapGeoReferenceData()`) or in-memory `GeoDataSets.Regions`/`GeoDataSets.Cities`
  - **DI registration** (`AddDocumentStore`, `AddDocumentContext`, seeding) and **OpenTelemetry instrumentation** (`AddDocumentStoreInstrumentation`, metrics + tracing) ship **in the core `Shiny.DocumentDb` package** — no separate DI-extensions or Diagnostics package (folded into core in 11.0)
  - `Shiny.DocumentDb.Orleans` — Microsoft Orleans grain storage (`IGrainStorage` + `PubSubStore`) over any `IDocumentStore` backend
  - `Shiny.DocumentDb.Orleans.MongoDb` / `Shiny.DocumentDb.Orleans.CosmosDb` — first-class Orleans grain-storage registration for MongoDB / Cosmos DB
- **Provider dependencies**:
  - SQLite: `Microsoft.Data.Sqlite`
  - SQLCipher: `Microsoft.Data.Sqlite.Core` + `SQLitePCLRaw.bundle_e_sqlcipher`
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

## Optimistic Concurrency (Row Versioning)

Map a version property on your document type for automatic optimistic concurrency. The version is stored inside the JSON blob — no schema changes required. Works across all providers.

### Configuration

```csharp
var options = new DocumentStoreOptions
{
    DatabaseProvider = new SqliteDatabaseProvider("Data Source=mydata.db")
};

// Expression-based
options.ConfigureDocument<Order>(cfg => cfg.MapVersionProperty(o => o.RowVersion));

// ...or the AOT-safe overload
options.ConfigureDocument<Order>(cfg =>
    cfg.MapVersionProperty("RowVersion", o => o.RowVersion, (o, v) => o.RowVersion = v));

var store = new DocumentStore(options);
```

`cfg.MapVersionProperty` works on every provider — it is a member of the shared `ConfigureDocument` builder, not of any one options class.

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

## Per-Type Configuration (`ConfigureDocument<T>`)

**Everything about a document type is configured in one `ConfigureDocument<T>` block**, on every provider.
There are no flat `options.MapXxx<T>(...)` methods — they were removed in v13.

```csharp
options.ConfigureDocument<Patient>(cfg =>
{
    cfg.Table = "Patients";                       // storage unit; omit to share the store default
    cfg.MapIdProperty(x => x.Id);
    cfg.AddSoftDelete(x => x.IsDeleted);
    cfg.AddQueryFilter(u => !u.IsDeleted);
    cfg.MapSpatialProperty(r => r.Location);
    cfg.MapProperty(x => x.Ssn, p => p.Encrypt(EncryptionMode.Deterministic));
    cfg.MapFullTextProperty([a => a.Title, a => a.Body]);
    cfg.MapVectorProperty(d => d.Embedding, dimensions: 1536, metric: VectorDistance.Cosine);
    cfg.ConfigureDocument<decimal>(cfg => cfg.MapComputedProperty("Total", o => o.Quantity * o.UnitPrice, setter: (o, v) => o.Total = v));
    cfg.MapBlob(i => i.Signature, o => o.ComputeHash = true);
    cfg.MapTemporal(o => o.Retention = TimeSpan.FromDays(90));
    cfg.OnBeforeWrite((ctx, ct) => Task.CompletedTask);
});
```

### The builder surface

| Member | Description |
|---|---|
| `cfg.Table` | The type's storage unit — table / collection / container / object store. Unset ⇒ shares the store default |
| `cfg.TypeName` | The resolved type name (per `TypeNameResolution`). Assign to `cfg.Table` to name it after the type |
| `cfg.MapIdProperty(x => x.Key)` / `("Key")` | Custom Id property; the string form is the AOT-safe one |
| `cfg.MapVersionProperty(x => x.RowVersion)` | Optimistic concurrency (+ AOT accessor overload) |
| `cfg.AddQueryFilter(pred)` / `("name", pred)` | Global query filter, optionally named |
| `cfg.MapProperty(x => x.Ssn, p => p.Encrypt(mode))` | Per-property options — field-level encryption |
| `cfg.MapSpatialProperty(...)` | `GeoPoint?` or `Geometry?` — **one per type**, and it is the *only* geometry that spatial queries can name |
| `cfg.MapVectorProperty(x => x.Embedding, dimensions: n, …)` | ANN embedding — **one per type**. Leave `indexKind` unset for the provider default |
| `cfg.MapFullTextProperty(x => x.Body)` / `([a, b])` | Full-text index — **one per type** (several fields, one index) |
| `cfg.MapComputedProperty<TValue>(...)` | Derived value — **one generic argument**, not two |
| `cfg.MapBlob(...)` / `cfg.MapBlobCollection(...)` | Sidecar blob payloads |
| `cfg.MapTemporal(o => ...)` | Append-only history |
| `cfg.OnBeforeWrite(...)` / `cfg.OnAfterWrite(...)` | Write hooks scoped to this type |
| `cfg.AddSoftDelete(x => x.IsDeleted)` | Soft delete |
| `cfg.MapJsonSchema(...)` / `cfg.MapJsonSchemaFromFile(...)` | JSON Schema validation (`Shiny.DocumentDb.JsonSchema`) |

**Provider vocabulary** — same builder, the backend's own word, only visible with that package referenced:
`cfg.ToContainer(...)` (Cosmos), `cfg.ToCollection(...)` (MongoDB / LiteDB / Firestore), `cfg.ToStore(...)`
(IndexedDB), `cfg.ToPartition(...)` + `cfg.MapIndexedProperty(...)` (Azure Table / DynamoDB),
`cfg.MapIndexedProperty(...)` (Redis). `cfg.Table` is the provider-agnostic spelling of all of them.

**Store-level** options stay on the options object, not the builder: `DatabaseProvider`, `TableName`,
`TenantIdAccessor`, `TypeNameResolution`, `JsonSerializerOptions`, `UseEncryptor`, `MapIdType<TId>`,
`MapFunctionTranslation`, `AddInterceptor` / `AddBulkInterceptor`, `AutoEmbedOnInsert`.

### Rules

- `ConfigureDocument<T>` is **additive** — call it again for the same type and it adds to what is there.
  Setting `cfg.Table` twice takes the last value; two types on one name throws.
- Spatial, vector and full-text are **one per type**. A second declaration throws naming both properties.
- Mapping a feature the backend does not have is caught when the store is built:
  `DocumentConfigurationException` lists **every** problem at once. `DocumentConfigurationValidator.Collect(options)`
  returns the same list without throwing. Relational providers stay permissive for spatial/vector/full-text
  (mapping a vector on plain SQLite just skips the index until `Shiny.DocumentDb.Sqlite.VectorSupport` is added).

### Basic mapping

```csharp
var options = new DocumentStoreOptions
{
    DatabaseProvider = new SqliteDatabaseProvider("Data Source=mydata.db"),
    TableName = "docs"                 // change the default table name (optional)
};
options.ConfigureDocument<Order>(cfg => cfg.Table = "orders");           // explicit table name
options.ConfigureDocument<AuditLog>(cfg => cfg.Table = cfg.TypeName);    // named after the type
// User stays in the default "docs" table

var store = new DocumentStore(options);
```

### Custom Id property

Every document type must have a property named `Id` unless you override it. `cfg.MapIdProperty(...)` is
independent of `cfg.Table` — use either, both, or neither. The property must be `Guid`, `int`, `long`,
`string`, or a type registered with `MapIdType`.

```csharp
options.ConfigureDocument<Sensor>(cfg =>
{
    cfg.Table = "sensors";
    cfg.MapIdProperty(s => s.DeviceKey);      // Guid DeviceKey as Id, in its own table
});

options.ConfigureDocument<BlogPost>(cfg => cfg.MapIdProperty(p => p.Slug));  // default shared table
```

### Configuring from a DocumentContext

A generated context can declare its model next to its `[Document]` list — implement the generated
`OnConfiguring` partial. It runs after the attribute-derived mapping, so what it sets wins.

```csharp
[Document(typeof(Patient))]
public partial class AppContext : DocumentContext
{
    static partial void OnConfiguring(DocumentModelBuilder model)
        => model.Document<Patient>(cfg => cfg.MapTemporal(o => o.Retention = TimeSpan.FromDays(90)));
}
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

## Strongly-Typed Context (DocumentContext)

Optional EF-Core-style typed front-end over `IDocumentStore`. Everything ships in the core
**`Shiny.DocumentDb`** package — `DocumentContext`/`DocumentSet<T>` are runtime types and the source
generator is bundled as an analyzer inside the same package (under `analyzers/dotnet/cs` — there is no
separate `Shiny.DocumentDb.Generators` package to install). Declare
aggregates once on a `partial` context; the generator emits a `DocumentSet<T>` per type, a `ConfigureModel`
lowering, and two DI extensions:
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
  - Under `Generated`, a property whose **type** has a type-level `[JsonConverter]` is emitted as a value built
    from that converter, so converter-backed types work even when immutable or abstract — this is how
    `GeoPoint`, `GeoPoint?`, and `Geometry` serialize as GeoJSON. The converter must be **public**,
    non-abstract, have a public parameterless ctor, and be `JsonConverter<T>` for exactly the declared member
    type (type a spatial property as `Geometry`, **not** a derived `GeoPolygon`). **Member-level**
    `[JsonConverter]`, converter factories, and a document type carrying its own converter all raise `DDB005`.
- **Sets are immediate** (`Insert`/`Update`/`Upsert`/`Remove(id)`/`BatchInsert`/…) and queries return the
  store's `IDocumentQuery<T>` as-is (`Query()`/`Where(...)` → full query surface). The context **is** a unit of
  work (`context.Add(x)` + `await context.SaveChanges()`, or `context.BeginTransaction()`); reach the raw session
  via `context.Session`. **No** change tracking, identity map, or navigation/`Include`.
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
    => o.MapVersionProperty(version);
```

Each mapping API also has an AOT-clean overload taking a property name plus accessor delegates — prefer it when generating code that must avoid the annotation entirely.

## Scalar functions in `Where` predicates

The relational providers (SQLite, SQLCipher, DuckDB, MySQL, SQL Server, PostgreSQL, Oracle) translate these to native SQL. LiteDB/IndexedDB evaluate them in-memory, so they always work there too.

- **String**: `s.ToLower()`/`ToUpper()`, `s.Length`, `s.Trim()`/`TrimStart()`/`TrimEnd()`, `s.Substring(start[, len])`, `s.Replace(a, b)`, `s.IndexOf(x)`, `string.IsNullOrEmpty(s)`, `a + b`, plus the existing `Contains`/`StartsWith`/`EndsWith`.
- **Math**: `Math.Abs/Round/Ceiling/Floor/Sqrt/Pow/Sign`. (`Ceiling`/`Floor`/`Sqrt`/`Pow` need the SQLite math extension; `Abs`/`Round` are always available.)
- **Flag enums** (stored numerically — the default): `x.Permissions.HasFlag(Permissions.Write)` or `(x.Permissions & Permissions.Write) == Permissions.Write`. Both lower to the same bitwise test (`BITAND` on Oracle) on the relational providers and Cosmos; MongoDB translates `HasFlag` to `$bitsAllSet`. Do **not** enable `JsonStringEnumConverter` if you need to query flags — bitwise tests require the numeric representation.
- **String-stored (non-flag) enums**: plain enum `==`/`!=`/`in` comparisons work whether the enum is stored numerically (default) or as a string via `JsonStringEnumConverter` — the query layer binds the exact member name the converter persisted, on both the LINQ and string surfaces (`Where(x => x.Level == Priority.High)` and `Where("Level == 'High'")`). Only flag enums require numeric storage.
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

opts.ConfigureDocument<Order>(cfg =>
{
    cfg.ConfigureDocument<decimal>(cfg => cfg.MapComputedProperty(o => o.Total, o => o.Quantity * o.UnitPrice));
    cfg.ConfigureDocument<string>(cfg => cfg.MapComputedProperty(o => o.FullName, o => o.First + " " + o.Last));
});
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
  opts.ConfigureDocument<Order>(cfg => cfg.MapComputedProperty<decimal>(o => o.Total, o => o.Quantity * o.UnitPrice, indexed: true));
  ```
- **LiteDB / IndexedDB** evaluate it in memory (full filter/sort/project/read-back). **MongoDB / Cosmos** support read-back and projection, but **not** server-side filter/sort by a computed property — filter on the underlying stored fields there.
- **AOT**: fully trim/AOT-safe (never compiled). For a pristine surface use the AOT overload with an explicit setter: `cfg.MapComputedProperty<decimal>("Total", o => o.Quantity * o.UnitPrice, setter: (o, v) => o.Total = v)`.
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

// Update = full replace; Upsert = RFC 7396 merge-or-insert. Pick the mode explicitly with a flag:
await store.Update(doc, patch: true);          // merge into the existing doc (update-only; throws if absent)
await store.Upsert(doc, patchIfUpdate: false); // replace the body wholesale on update; insert if absent
```

**Merge vs replace flags** (`Update(patch)`, `Upsert(patchIfUpdate)`): merge modes strip null properties (unset fields are left unchanged, never deleted). A typed object serializes *every* property, so `patch: true` on it only skips `null` fields — a non-nullable default (`int Count = 0`) is still written. For a precise "change only these keys" update, make the patch type's fields nullable **or** use a JSON collection: `store.Collection(typeof(User)).Update(new JsonObject { ["id"]="u1", ["name"]="Bob" }, patch: true)`. The two defaults (`Update` replace, `Upsert` merge) work on every provider; the non-default modes (`Update(patch: true)`, `Upsert(patchIfUpdate: false)`) are relational-provider only (document-native/key-partitioned stores throw `NotSupportedException`).

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

### Unit of work — `IDocumentSession` (`store.OpenSession()`)

To group several writes into one transaction, create a `UnitOfWork` from the store, queue
`Add`/`AddRange`/`Update`/`Upsert`/`Remove`, then call `SaveChanges`. All commit or all roll back.
Contiguous same-type runs of inserts, upserts, updates, and removes are each coalesced into the
matching batch method. There is no `RunInTransaction` — `UnitOfWork` is the only way to open a
transaction.

```csharp
await using var uow = store.OpenSession();   // IDocumentSession is the unit of work
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

### JSON collections (raw JSON, keyed by name or by type)

One API for working in raw JSON, addressed two ways. **Relational providers only** (SQLite, SQLCipher, DuckDB, PostgreSQL, CockroachDB, SQL Server, MySQL, MariaDB, Oracle); everything else throws `NotSupportedException`, and it is unavailable inside a session (use `session.Store.Collection(...)`).

```csharp
using System.Text.Json.Nodes;

// (a) Schema-free — no CLR type at all. The name becomes the row's TypeName.
var orders = store.Collection("orders");                 // optional: Collection("orders", idProperty: "orderNo")
string id = await orders.Insert(jsonObject);             // returns the stored id (UUIDv7 when absent, stamped into the object)
int n     = await orders.Insert(jsonArray);              // many, atomic, returns the count
await orders.Update(node);                               // full replace; target must exist
await orders.Update(node, patch: true);                  // RFC 7396 deep-merge
await orders.Upsert(node);                               // merge-or-insert (patch by default)
await orders.BatchUpsert([a, b]);
JsonObject? doc = await orders.Get(id);
await orders.Remove(id); await orders.BatchRemove([id1, id2]); await orders.Clear();
await orders.CreateIndex(default, "status", "created");

var rows = await orders.Query()
    .Where("customer.name == 'bob' and total:number > 100")
    .OrderBy("total:number desc")
    .Paginate(0, 50)
    .ToList();                                           // IReadOnlyList<JsonObject>

// (b) Late-bound over a registered type — same API, full write pipeline, metadata-resolved paths.
var typed = store.Collection<Order>();                   // same as store.Collection(typeof(Order))
await typed.Insert(JsonNode.Parse("""{ "customer": "acme" }""")!.AsObject());
var open = await typed.Query().Where("status == 'open'").ToList();
```

Rules that matter when generating code:

- **`Insert` takes `JsonObject` or `JsonArray`, never a bare `JsonNode`** — call `.AsObject()` / `.AsArray()`. The object overload returns the **id**; the array overload returns the **count**. `Update`/`Upsert` take `JsonNode` and accept either shape.
- **String grammar only.** There is no typed LINQ on a collection — never generate `store.Collection("x").Query().Where(o => …)`. Use `Query<T>()` when you have a CLR type. If you want the *typed* builder but a raw JSON **result**, that is the [JSON terminals](#json-terminals--results-as-raw-json-instead-of-t) on `Query<T>()`, not a collection.
- **Type hints (`total:number`) are schema-free only** and are a *parse error* on a type-keyed collection. Vocabulary: `string number int long double decimal bool date guid`. **A hint is required** for `OrderBy` / `min` / `max` / `Project` over a numeric field — without one the field extracts as text and sorts lexicographically (`"100"` before `"9"`) on every provider except SQLite. `Where("total > 100")` needs no hint: the type is inferred from the literal.
- **Ids.** Schema-free: property configurable (default `"id"`), read case-insensitively, written verbatim, auto-generated as a UUIDv7 **string**; ids compare as literal strings so pass back exactly what you stored. Type-keyed: the type's own `IdKind` — `Guid` → v4, `int`/`long` → sequence, declared `string` → **refuses** to auto-generate.
- **Body is stored AS-IS** — on a type-keyed collection property names must match the type's serialized shape (camelCase by default).
- **Full pipeline parity only when type-keyed** — tenancy applies to both, but interceptors, temporal history, versioning/CAS, spatial + vector + blob sidecars, change notifications and global query filters need a CLR type. A schema-free collection has none of them, and `hasflag` / the geo predicates / the Lucene functions throw there with a pointer to `Query<T>()`.
- **Mapped properties must be present (type-keyed)** — a registered spatial/vector mapping whose JSON path is absent throws `InvalidOperationException` on `Insert`/`Update`; an explicit JSON `null` is allowed (skips the sidecar). `Upsert` checks this only when the element has no Id.
- **Names/paths must be identifiers** — `^[A-Za-z_][A-Za-z0-9_]{0,127}$`, dot-separated paths, max 16 segments. Keys with dashes/spaces/dots are storable but not addressable.
- **Raw SQL escape hatch** — `Collection(...).Query(whereClause, parameters)` / `QueryStream(...)` return `JsonNode`s; the clause is not parsed, so bind values through `parameters`.

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

### Transactions (IDocumentSession)

```csharp
await using var uow = store.OpenSession();   // IDocumentSession is the unit of work
uow.Add(new User { Id = "u1", Name = "Alice", Age = 25 })
   .Add(new User { Id = "u2", Name = "Bob", Age = 30 });
await uow.SaveChanges(); // commits on success, rolls back on exception
```

**Explicit transactions + consistent reads (11.0, relational):** `await using var tx = await session.BeginTransaction();` (one active at a time) for locking reads (`session.Get(id, LockMode.Update)`) and grouping multiple `ExecuteUpdate`/`ExecuteDelete`; `SaveChanges` joins the active tx. Pass an isolation level for a **consistent-read session** — `await session.BeginTransaction(IsolationLevel.Snapshot)` — so every read sees one snapshot. **Telemetry:** a session emits a `<system>.unit_of_work` parent span (tag `db.session.id`) so its ops nest into one correlated trace, and `SaveChanges` records the `db.client.unit_of_work.operations` histogram (buffered writes per commit). Zero-cost when unobserved.

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
- `BulkImportAsync(IAsyncEnumerable<RawDocument>, BulkRestoreOptions?)` — lower-level primitive over `RawDocument(string Id, string DocType, ReadOnlyMemory<byte> Data, DateTimeOffset? CreatedAt = null, DateTimeOffset? UpdatedAt = null)` (raw UTF-8 JSON body; optional timestamps preserved on Insert, else stamped now). `RestoreAsync` is the JSON adapter on top of it and round-trips `CreatedAt`/`UpdatedAt` (v2 envelope; older v1 backups without timestamps still import).

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

### Reference geo data (`Shiny.DocumentDb.Geo`)

The `Shiny.DocumentDb.Geo` package ships an embedded reference dataset — **US states, Canadian provinces, and US & Canadian cities** — as ordinary `GeoRegion` (state/province, `Geometry Boundary` = a simplified `GeoPolygon`) and `GeoCity` (`GeoPoint Location`) documents keyed on a deterministic string `Id` (e.g. `US-CA`, `US-CA-los-angeles`). It reuses the standard seeding + spatial machinery, so it works on any provider that supports them.

```csharp
services.AddDocumentStore(opts =>
{
    opts.DatabaseProvider = new SqliteDatabaseProvider("Data Source=app.db");
    opts.MapGeoReferenceData();     // maps GeoRegion.Boundary + GeoCity.Location for spatial queries
});
services.AddGeoReferenceSeeder();   // idempotent GeoReferenceSeeder (pass a store name for a keyed store)

// point-in-region containment
var region = (await store.GeoIntersects<GeoRegion>(new GeoPoint(39.7392, -104.9903))).FirstOrDefault();

// no host? seed directly, or read the in-memory dataset (plain LINQ)
await new GeoReferenceSeeder().SeedAsync(store, ct);
var texas = GeoDataSets.Cities.Where(c => c.RegionCode == "TX");
```

Region boundaries are intentionally low-resolution (coarse containment, not cartography). The embedded city lists are regenerated from US Census / Statistics Canada by the dev-only `tools/Shiny.DocumentDb.Geo.DataSeeder` (not part of CI).

## Blobs (MapBlob / MapBlobCollection)

Binary payloads (PDF, image, signature) attached to a document. The bytes go to a `{table}_blobs` **sidecar table**, not the document JSON; only metadata (length, content type, file name) rides along in the body. **Use `DocumentBlob` instead of a raw `byte[]` property** — a `byte[]` is base64'd into the document body and materialized on every read.

**Supported on every server-side provider** — relational (SQLite, PostgreSQL/CockroachDB, SQL Server, MySQL/MariaDB, Oracle, DuckDB) and document/NoSQL (LiteDB, MongoDB/Amazon DocumentDB, Redis, Azure Table, DynamoDB, Cosmos, Firestore, RavenDB-native-attachments). **Only IndexedDB (Blazor WASM) is unsupported** — it reports `store.MaxBlobSize == 0` and **throws `NotSupportedException`**. Per-provider caps vary (Azure Table 64KB, DynamoDB 390KB, Cosmos 1.4MB, Firestore/LiteDB 1-16MB, relational/Redis/Raven ≥512MB) — check `store.MaxBlobSize`.

```csharp
public class Invoice
{
    public string Id { get; set; } = "";
    public DocumentBlob? Pdf { get; set; }                 // single
    public DocumentBlobCollection Attachments { get; set; } = new();   // many — NOT List<DocumentBlob>
}

services.AddDocumentStore(opts =>
{
    opts.DatabaseProvider = new SqliteDatabaseProvider("Data Source=app.db");
    opts.ConfigureDocument<Invoice>(cfg =>
    {
        cfg.MapBlob(i => i.Pdf);
        cfg.MapBlobCollection(i => i.Attachments);
    });
    // options: o => { o.Key = "sig"; o.ComputeHash = true; o.MaxSize = 256*1024; }  (ComputeHash OFF by default)
});
```

**Write** — assign the member and save the document; there is NO `SetBlob`. Metadata can never disagree with the bytes because every mutation goes through the document.

```csharp
inv.Pdf = DocumentBlob.FromBytes(pdfBytes, "application/pdf", "acme.pdf");
inv.Attachments.Add(scanBytes, "image/png");           // collection.Add(bytes,…) assigns the key up front
await store.Insert(inv);
inv.Pdf = null; await store.Upsert(inv);               // null drops the row; RemoveAt prunes a collection item
```

**Read** — metadata is populated, bytes are NOT. `Bytes` throws until loaded.

```csharp
var inv = await store.Get<Invoice>(id);
inv.Pdf!.Length;      // from the body — free
inv.Pdf.IsLoaded;     // false
inv.Pdf.Bytes;        // throws until loaded

// metadata is queryable (json_extract), payload never touched:
store.Query<Invoice>().Where(x => x.Pdf!.Length > 1_000_000);
// x.Pdf.Bytes in a query throws at translation time
```

**Load on demand** — blobs self-load (the store stamped a loader during hydration):

```csharp
await inv.Pdf!.LoadAsync();               // one blob
var raw = await inv.Pdf.GetBytesAsync();  // load + read
await inv.Attachments[0].LoadAsync();     // one item of a collection
await inv.Attachments.LoadAllAsync();     // whole collection, ONE round trip

// page of results — avoid N+1 with the store batch load:
await ((IBlobDocumentStore)store).BatchLoadBlobs(page);
// no document in hand (download endpoint):
var bytes = await ((IBlobDocumentStore)store).GetBlob<Invoice>(id, "Pdf");
```

Deleting a document cascades to its blob rows. `store.MaxBlobSize` (bytes) reports the provider ceiling; `0` = unsupported. Backup includes payloads by default (`ExportAsync(stream, new BackupExportOptions { IncludeBlobs = false })` to skip). Blobs are **not** temporally versioned — `Restore` keeps current blobs. `IDocumentMaintenance.SweepOrphanedBlobs<T>()` reclaims rows whose document is gone.

## Temporal History (System-Time Versioning)

Opt-in append-only versioning per type. Enable with `cfg.MapTemporal(...)` in the type's `ConfigureDocument` block; every `Insert`/`Update`/`Upsert`/`Remove`/`SetProperty`/`RemoveProperty`/`BatchInsert` (including writes inside a `UnitOfWork`) records a versioned snapshot to a per-type history sidecar. Only mapped types incur the extra write.

```csharp
options.ConfigureDocument<Order>(cfg => cfg.MapTemporal(o =>
{
    o.Retention    = TimeSpan.FromDays(90);   // prune expired (closed) versions older than this
    o.MaxVersions  = 50;                      // …or keep only the newest N versions per document
    o.CaptureActor = () => currentUser.Id;    // optional "who" recorded per version
    // Scope-aware (11.0): resolve the actor from the write's session DI scope (a request-scoped ICurrentUser);
    // takes precedence over CaptureActor. Ideal for ASP.NET where the user is per-request.
    o.ResolveActor = sp => sp.GetService<ICurrentUser>()?.Id;
}));
```

### Provider support

Implemented on **every** provider. Each persists versions to its own sidecar: relational stores (SQLite, SQLCipher, PostgreSQL, SQL Server, MySQL, Oracle, DuckDB) → `{table}_history` table; LiteDB / MongoDB → `{collection}_history` collection; CosmosDB → `{container}_history` container (partitioned by `/typeName`); IndexedDB → `{store}_history` object store.

The history-query methods live on the **`ITemporalDocumentStore`** capability interface (`ITemporalDocumentStore : IDocumentStore`), **not** the base `IDocumentStore` — the same pattern as `IObservableDocumentStore` / `IChangeFeedDocumentStore`, and the `Backup`/`ClearAllAsync` precedent. History is an optional capability, not universal CRUD: promoting it to `IDocumentStore` would force every consumer to see methods that throw unless the type is `cfg.MapTemporal`-mapped, and force every backend to implement them. Resolve or cast to `ITemporalDocumentStore` (every store, relational and NoSQL, implements it). A history call for a type not passed to `MapTemporal<T>` throws `InvalidOperationException`.

> **IndexedDB:** temporal adds new object stores, which IndexedDB only creates during a schema upgrade. Bump `options.Version` when adding `cfg.MapTemporal` to an already-deployed database (a fresh database needs no change).

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
- `Restore` writes a **new** current version (re-inserts if removed); it does not rewrite history. Aligns the version token when optimistic concurrency is mapped. Restoring a **removed** document re-creates it as a fresh live lifecycle (new `CreatedAt`, mapped version restarts at 1); history is preserved. Uniform across all temporal providers.
- `Clear<T>` is a bulk delete and is **not** history-tracked — use `Remove<T>` per document when deletions must be tracked.
- Retention (`Retention` by age, `MaxVersions` by count) prunes on every write; the current version is never pruned. Set at least one on SQLite/mobile.
- On the relational providers the sidecar PK is `(Id, TypeName, Version)` with `(TypeName, ValidFrom, ValidTo)` and `(TypeName, Actor)` secondary indexes backing the fleet-wide queries; the document stores model the same versions natively and compute the selection in the provider.

## Telemetry & Diagnostics

The core `Shiny.DocumentDb` package emits OpenTelemetry-native metrics + tracing for every operation — **embedded and always-on** on every provider and construction path (no decorator, no opt-in, no separate package; zero-cost when unobserved). Just subscribe your OTel pipeline to the meter/source `Shiny.DocumentDb`:

```csharp
services.AddDocumentStore(o => o.DatabaseProvider = new SqliteDatabaseProvider("Data Source=app.db"));

services.AddOpenTelemetry()
    .WithMetrics(m => m.AddMeter("Shiny.DocumentDb"))
    .WithTracing(t => t.AddSource("Shiny.DocumentDb"));
```

**REMOVED in 11.0** (do not generate): `AddDocumentStoreInstrumentation()`, `InstrumentedDocumentStore`, and `DocumentStoreOptions.Instrumentation`. Instrumentation is embedded; there is nothing to call. A `UnitOfWork` emits one `transaction` span (inner writes span-free).

**Structured `ILogger` logging:** when a store is created from the container (any provider's `Add…DocumentStore` or `IServiceProvider` ctor) with an `ILoggerFactory` registered, every SQL / operation statement is logged at `Debug` under the `Shiny.DocumentDb` category (control via `Logging:LogLevel:Shiny.DocumentDb`), composed with the `options.Logging` string callback — on the relational core and all six non-relational providers. Container-free `new …DocumentStore(options)` is callback-only, unchanged.

**Keyed/named stores:** a store registered with `AddDocumentStore("orders", …)` automatically tags its signals with `db.namespace = "orders"` so multiple stores are distinguishable. The non-keyed path omits `db.namespace`.

Built on `System.Diagnostics.Metrics.Meter` (via `IMeterFactory`) and `ActivitySource`. Emits, per the OTel database client semantic conventions: a `db.client.operation.duration` histogram (plus a `db.client.operations` counter and a `db.client.response.returned_rows` histogram), tagged `db.system.name` / `db.operation.name` / `db.collection.name` / `outcome` / `error.type` (plus `db.namespace` on named stores); and a `{system}.{operation}` `ActivityKind.Client` span per call with error status + exception capture. `db.system.name` is derived from the wrapped store, so one decorator covers all providers.

- **Decorator type**: `InstrumentedDocumentStore` implements `IDocumentStore` + `ITemporalDocumentStore` + `IObservableDocumentStore` + `IChangeFeedDocumentStore` (faithful — casts/pattern-matches keep working); wrapped store is on `.Inner`. Construct directly (`new InstrumentedDocumentStore(inner, new DocumentStoreMetrics(meterFactory))`) when not using DI.
- **Coverage**: CRUD, string `Query`/`QueryStream`, the fluent-query terminals (`ToList`/`ToAsyncEnumerable`/`Count`/`Any`/`ExecuteDelete`/`ExecuteUpdate`/`Max`/`Min`/`Sum`/`Average`/`NearestVectors`), spatial/vector, all `ITemporalDocumentStore` ops, and `UnitOfWork.SaveChanges` (inner ops become child spans of the transaction span).
- **Not traced**: `NotifyOnChange`/`SubscribeChanges` (long-lived subscriptions, passed through); the fluent **builder** operators (no I/O); provider internals (raw SQL, RU, pool) — use the per-provider `Logging` option for raw SQL.
- **Zero-cost** when nothing is listening. **Privacy**: only metadata (op, type name, outcome, counts) — never document bodies, ids, or parameter values. Keyed `AddDocumentStore(name, …)` registrations are not auto-decorated.

### MongoDB-Specific Notes

The `Shiny.DocumentDb.MongoDb` provider implements `IDocumentStore` natively over `MongoDB.Driver`. Documents are stored as a typed BSON envelope (`_id`, `id`, `typeName`, `data`, `createdAt`, `updatedAt`) inside a collection that defaults to `"documents"`. Map types to dedicated collections with `cfg.ToCollection(...)` inside a `ConfigureDocument<T>` block.

- **Predicates evaluated in C#** — LINQ expressions are translated to a MongoDB filter at the type/sort/skip/take level; complex predicates are evaluated client-side after a typed find.
- **Raw SQL throws** — `Query<T>(string)` and `QueryStream<T>(string)` throw `NotSupportedException`. Use the LINQ-based `Query<T>()` overload.
- **`Upsert` deep-merges in C#** — null properties are stripped recursively (RFC 7396 semantics).
- **`UnitOfWork` uses a compensating model** — single-node MongoDB cannot use ACID multi-document transactions without a replica set. The provider tracks inserts and deletes them on failure (matches the CosmosDB provider).
- **`cfg.ToCollection(...)`** — auto-derived or explicit collection name; combine with `cfg.MapIdProperty(...)` for a custom Id.
- **Spatial supported** — MongoDB implements the full spatial surface via a `2dsphere` index: point queries (`WithinRadius`/`WithinBoundingBox`/`NearestNeighbors`) and the full geometry predicate family (`GeoIntersects`/`GeoContainedBy`/… via native `$geoIntersects`/`$geoWithin`/`$near`, with finer predicates refined in-process).
- **Pre-configured client** — set `MongoDbDocumentStoreOptions.MongoClient` to share an existing `IMongoClient` (pooled, process-wide). When null, the provider creates one from `ConnectionString`.

```csharp
var options = new MongoDbDocumentStoreOptions
{
    ConnectionString = "mongodb://localhost:27017",
    DatabaseName = "mydb",
    CollectionName = "documents", // default; only used for unmapped types
    JsonSerializerOptions = ctx.Options,
    UseReflectionFallback = false
};
options.ConfigureDocument<User>(cfg => cfg.ToCollection());
options.ConfigureDocument<Order>(cfg => { cfg.ToCollection("orders"); cfg.MapVersionProperty(o => o.RowVersion); });
options.ConfigureDocument<Sensor>(cfg => { cfg.ToCollection("sensors"); cfg.MapIdProperty(s => s.DeviceKey); });

var store = new MongoDbDocumentStore(options);
```

### DuckDB-Specific Notes

The `Shiny.DocumentDb.DuckDb` provider uses [DuckDB](https://duckdb.org/) — an embedded analytical database — through the standard `IDatabaseProvider` pipeline. Documents are stored as `JSON` column rows alongside `Id`, `TypeName`, `CreatedAt`, `UpdatedAt`.

- **Full LINQ → SQL translation** — same expression visitor used by the SQL providers, emitting `json_extract_string(Data, '$.path')` for property access and `json_merge_patch` for upsert.
- **Native RFC 7396 merge** — DuckDB 0.10+ exposes `json_merge_patch`, so `Upsert` runs entirely server-side with deep-merge semantics (no read-merge-write round trip).
- **`SetProperty`/`RemoveProperty`** — implemented via `json_merge_patch` because DuckDB has no `json_set`/`json_remove`. Path parts are folded into a merge-patch document on the server.
- **JSON extension auto-loaded** — `InitializeConnectionAsync` runs `INSTALL json; LOAD json;` on every connection.
- **Raw SQL supported** — use `json_extract_string(Data, '$.path')` in `Query<T>("...", parameters)` calls.
- **Spatial supported** — via the dependency-free envelope-sidecar path (bbox prune + C# refine), not the DuckDB `spatial` extension. Full point + geometry surface.
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

`Shiny.DocumentDb.Orleans` is a Microsoft Orleans `IGrainStorage` (+ `PubSubStore`) provider implemented entirely against `IDocumentStore`, so one implementation runs on every DocumentDb backend. Grain state is persisted as a nested, **queryable** `JsonElement` (not an opaque blob), and the envelope can opt into `cfg.MapTemporal` for a free audit trail of state mutations.

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
| ETag | `GrainStateRecord.Version` (mapped via `cfg.MapVersionProperty`) |
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
        opts.ConfigureDocument<GrainStateRecord>(cfg => cfg.MapVersionProperty(x => x.Version));
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

Spatial queries are supported on **SQLite** (R*Tree bbox), **PostgreSQL / MySQL / SQL Server / Oracle / DuckDB** (dependency-free envelope-sidecar bbox), all with in-process relate/refine — plus **CosmosDB** (native GeoJSON `ST_INTERSECTS`/`ST_WITHIN`/`ST_DISTANCE`) and **MongoDB** (`2dsphere` + `$geoIntersects`/`$geoWithin`/`$near`). The fallback stores (LiteDB, IndexedDB, Azure Table, DynamoDB) throw `NotSupportedException`. Both **point** queries and **full OGC geometry** are supported.

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
  Family: `Intersects`/`Disjoint`/`Contains`/`Within`/`Covers`/`CoveredBy`/`Touches`/`Crosses`/`Overlaps`/`GeoEquals`/`WithinDistance` (in `Where`) + `Distance` (in `OrderBy`). Read as `field <predicate> query`. Lowers to native per provider: **SQLite** (R\*Tree + `docdb_st_*` UDF — all predicates), **MySQL/PostgreSQL** (native `ST_*`; PostgreSQL needs PostGIS — all predicates), **DuckDB** (native `ST_*`, auto-loads `spatial` — all except `WithinDistance`), **SQL Server** (native planar `geometry` column + `.ST*` — all except `Covers`/`CoveredBy` and `WithinDistance`), **Oracle** (native `SDO_GEOMETRY` column + MDSYS spatial index + `SDO_RELATE` operators, needs Oracle Spatial — all except `Crosses`), **CosmosDB** (`ST_INTERSECTS`/`ST_WITHIN`/`ST_DISTANCE` — intersects/within/disjoint/withindistance), **MongoDB** (`$geoIntersects`/`$geoWithin` — intersects/within/point-withindistance). `WithinDistance` in a `Where` needs a geodesic distance function, which SQL Server (planar `geometry`) and DuckDB (no polygon geodesic) lack — they **throw** rather than approximate wrongly; use `store.GeoWithinDistance(...)` (exact Haversine, every provider). `Distance`-in-`OrderBy` is native on SQLite/PostgreSQL/MySQL/DuckDB/SQL Server (SQL Server sorts by planar `STDistance` over the indexed column); on MongoDB use `store.NearestNeighbors`/`orderByDistanceFrom`. Where a predicate isn't native, the `DocumentFunctions` call in a `Where` **throws** — use the dedicated `store.Geo*` method (all predicates, every spatial provider). `PortableSpatial = true` on a relational provider forces the dependency-free envelope tier.
  **String-expression parity:** the same geo functions work in the string surface — `Where("…")`, interpolated `Where($"…")`, `OrderBy("…")`, `Project("…")` — using the same names (`intersects`/`within`/`withindistance`/`distance`/…). Supply the query geometry as an **interpolated `{value}`** (a `Geometry`/`GeoPoint`, bound as a parameter — only where the string carries args, i.e. `Where($"…")`) or an inline **GeoJSON string literal** (works everywhere incl. `OrderBy`/`Project`). E.g. `store.Query<Zone>().Where($"intersects(area, {poly}) and active == true")`, `.OrderBy("distance(area, '<geojson>')")`. `contains(field, …)` is geo when `field` is a `Geometry` property, else the string `Contains`.
- **Only the mapped geometry is queryable (v13+).** Every spatial surface — `store.Geo*`, `DocumentFunctions` in `Where`/`OrderBy`, and the string grammar — must name the property passed to `cfg.MapSpatialProperty`. A second `Geometry?` property stores and round-trips normally but is **not** spatially queryable, and naming it throws `NotSupportedException` ("… is not a mapped spatial property … Mapped: 'area'"). Most relational providers answer these predicates from the sidecar's geometry column, which holds only the mapped property, so an unmapped path cannot be served honestly. When a document really has several shapes: **one semantic slot in several pieces** (a service area of three disjoint polygons) → one property typed `GeoMultiPolygon`/`GeoGeometryCollection`, whose union envelope is the right index key; **genuinely distinct slots** (a delivery's origin *and* destination) → the union envelope would span both and destroy index selectivity, so map the one you search by and keep the other as plain data.
- **`GeoDisjoint`** is anti-selective — it scans the type (O(n)) on SQLite/refine paths. Use sparingly on large corpora.
- **Fidelity:** SQLite / refine-path distances are Haversine/planar approximations; native `ST_DISTANCE` is geodesic. Ordering can differ on near-ties across providers.

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

### How It Works

- **SQLite**: Creates R*Tree sidecar tables (`{table}_spatial` and `{table}_spatial_map`) that are automatically synced on insert/update/upsert/remove/clear. Bounding box pre-filter via R*Tree, then Haversine post-filter for exact radius.
- **CosmosDB**: `GeoPoint` serializes as GeoJSON `{"type":"Point","coordinates":[lng,lat]}`. Spatial index policies are added to the container automatically. Queries use native `ST_DISTANCE` and `ST_WITHIN` functions.

### Spatial CRUD Sync

Spatial sidecar data is automatically maintained — no manual steps needed:
- **Insert/Update/Upsert**: Extracts `GeoPoint` from the document and upserts into spatial index
- **Remove**: Deletes spatial data for that document
- **Clear**: Removes all spatial data for that type

## Vector / Similarity Search

Embedding-similarity search via `store.NearestVectors<T>(query, k)` — also on `IDocumentSession` (`session.NearestVectors<T>(...)`), where inside `BeginTransaction` it reads the transaction's consistent snapshot. Capability is a store property: check `session.Store.SupportsVector` (not duplicated on the session). Supported on PostgreSQL (`pgvector`), SQL Server 2025, Oracle 23ai, CosmosDB (DiskANN), MongoDB (Atlas `$vectorSearch`), DuckDB (`vss`), and **SQLite** (`sqlite-vec`). LiteDB, IndexedDB, and MySQL throw `NotSupportedException`.

```csharp
options.ConfigureDocument<Doc>(cfg => cfg.MapVectorProperty(d => d.Embedding, dimensions: 1536, metric: VectorDistance.Cosine));
var hits = await store.NearestVectors<Doc>(queryEmbedding, k: 5);
```

On the relational providers the embedding is stored **twice**: in the document body (it is an ordinary serialised property) and in a per-type `{table}_vec_{type}` sidecar, which is what an ANN query actually searches. The library keeps the two in step on every write. Anything that writes the row *without* going through the library — a hand-written `UPDATE`, a restore, another tool — leaves the sidecar holding the old embedding, and the search then returns wrong answers without erroring. `ShinyDocDbMyAdmin`'s **Vectors** tab reports that drift and rebuilds the sidecar from the document bodies. Providers expose the sidecar name as `IDatabaseProvider.VectorTableName(table, type)` and its current ids as `BuildVectorDocIdsSql(table, type)`.

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

## Fluent Query Builder (IDocumentQuery<T>)

The fluent query builder is the primary way to query documents. Start with `store.Query<T>()` and chain builder methods, then terminate with a materialization method.

**Builders are immutable (12.0+, every provider).** Each builder call returns a **copy** — assign the result. `q.Where(...)` as a bare statement is discarded (the relational stores mutated in place before 12.0; the document stores never did):

```csharp
var q = store.Query<User>();
q = q.Where(x => x.Age >= 18);        // correct
// q.Where(x => x.Age >= 18);         // WRONG — discarded
```

**Composing after `Select`/`Project` throws `NotSupportedException`** on every provider (12.0 unified this; the relational stores threw `InvalidOperationException` before).

### Builder Methods (non-executing, return IDocumentQuery<T>)

| Method | Description |
|--------|-------------|
| `.Where(predicate)` | Filter by LINQ expression. Multiple calls combine with AND. |
| `.Where(filter[, jsonTypeInfo])` | Filter by a runtime filter string (e.g. `"Age >= 30 and Status == 'open'"`) — AOT-safe. `and`/`or`/`not`, comparisons, `is [not] null`, `in (…)`, `contains/startsWith/endsWith`. |
| `.WhereIn(selector, values[, nulls])` / `.WhereNotIn(selector, values[, nulls])` | Set-membership filter (`IN` / `NOT IN`) from an in-memory collection. The collection is lowered to the store's native construct (`IN` / `$in`), not expanded into the filter text. `nulls` is a `NullHandling` (`Ignore` default / `Raw` / `Match`). Empty set ⇒ `WhereIn` matches nothing, `WhereNotIn` matches everything. Also takes a string property name overload. |
| `.OrderBy(selector)` / `.OrderByDescending(selector)` | Sort by property (expression). |
| `.OrderBy(name[, jsonTypeInfo])` / `.OrderByDescending(name[, jsonTypeInfo])` | Sort by property name (string) — AOT-safe via `JsonTypeInfo<T>`. Supports dotted paths. |
| `.OrderBy(name, direction[, jsonTypeInfo])` | Sort by property name with a runtime direction string (`asc`/`ascending`/`desc`/`descending`, case-insensitive; empty → ascending). |
| `.GroupBy(keySelector)` | Group into one row per key for an aggregate projection (`.Select(g => …)` with `g.Key` + `g.Count()`/`g.Sum(x => x.P)`). |
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
| `.First()` / `.FirstOrDefault()` | `Task<T>` / `Task<T?>` | First match. **Use these instead of `(await ToList())[0]`** — the row limit is pushed to the provider (`LIMIT 1`). `First` throws when nothing matched; `FirstOrDefault` returns null. Predicate and string-filter overloads exist: `.First(u => u.Age == 40)`, `.First("status == 'open'")`. |
| `.Single()` / `.SingleOrDefault()` | `Task<T>` / `Task<T?>` | The only match; throws when a second one matches (two rows are fetched to detect it, no extra round trip). |
| `.ExecuteDelete()` | `Task<int>` | Delete matching documents. Returns count. |
| `.ExecuteUpdate(property, value)` | `Task<int>` | Update a property on all matching documents via `json_set()`. Returns count. |
| `.ExecuteUpdate(build)` | `Task<int>` | Update **several** properties in ONE statement: `.ExecuteUpdate(b => b.Set(o => o.Status, "x").Set(o => o.ClosedAt, now))`. Prefer it over consecutive single-property calls — one statement, one predicate evaluation, atomic. Same property twice throws. |
| `.Max(selector)` | `Task<TValue>` | Maximum value of a property. |
| `.Min(selector)` | `Task<TValue>` | Minimum value of a property. |
| `.Sum(selector)` | `Task<TValue>` | Sum of a property. |
| `.Average(selector)` | `Task<double>` | Average of a property. |
| `.PageResult(page, pageSize, zeroBased?)` | `Task<PagedResults<T>>` | Run the query and return records + total count in one envelope. 1-based by default. |
| `.ToCursorPage(cursor, take)` | `Task<CursorPage<T>>` | One forward seek/keyset page. `null` cursor = first page; `NextCursor` null = last page. See Pagination. |
| `.ToCursorStream(pageSize?)` | `IAsyncEnumerable<T>` | Walk every cursor page automatically — resumable full scan, no deep-offset cost. |
| `.ToQueryString()` | `DocumentQueryString` | Build the query the configuration **would** run **without executing it** — for debugging/logging. See below. |

### JSON terminals — results as raw JSON instead of `T`

Same typed builder, different materialization. Generate these whenever the document is read only to be written back out (an ASP.NET endpoint returning stored documents), so nothing is deserialized just to be re-serialized.

| Method | Returns | Typed twin |
|--------|---------|-------------|
| `.ToJsonList()` | `Task<IReadOnlyList<JsonObject>>` | `.ToList()` |
| `.ToJsonAsyncEnumerable()` | `IAsyncEnumerable<JsonObject>` | `.ToAsyncEnumerable()` |
| `.FirstJson()` / `.FirstOrDefaultJson()` | `Task<JsonObject>` / `Task<JsonObject?>` | `.First()` / `.FirstOrDefault()` |
| `.SingleJson()` / `.SingleOrDefaultJson()` | `Task<JsonObject>` / `Task<JsonObject?>` | `.Single()` / `.SingleOrDefault()` |
| `.FirstOrDefaultRawJson()` | `Task<string?>` | — (no parse at all) |
| `.ToRawJsonAsyncEnumerable()` | `IAsyncEnumerable<string>` | — |
| `.WriteJsonArrayTo(stream)` | `Task<int>` (rows written) | — |
| `.ToJsonCursorPage(cursor, take)` | `Task<CursorPage<JsonObject>>` | `.ToCursorPage()` |
| `.RawJsonRows(maxRows)` | `IAsyncEnumerable<string>` | the primitive the rest are built on |
| `.SupportsRawJson` | `bool` | whether this query can use the lane at all |

```csharp
// one document — hand the stored body straight to the client, no parse
var raw = await store.Query<Order>().Where(o => o.Id == id).FirstOrDefaultRawJson();
return raw is null ? Results.NotFound() : Results.Content(raw, "application/json");

// a whole list — one JSON array written straight to the response, never buffered
ctx.Response.ContentType = "application/json";
await store.Query<Order>()
    .Where(o => o.Status == "open")
    .OrderByDescending(o => o.CreatedAt)
    .WriteJsonArrayTo(ctx.Response.Body, ct);
```

Rules that matter when generating code:

- **Terminals only — build first, then take JSON.** `Where` / `OrderBy` / `Paginate` / `IgnoreQueryFilters` all still apply and are all typed; there is no string-grammar building after a JSON terminal (that is `store.Collection(...)`).
- **Works on every provider.** Relational + Cosmos hand back the persisted body untouched; everywhere else the provider materializes `T` and re-serializes through the type's `JsonTypeInfo` — same JSON, same API, but no saving. Don't promise a perf win on MongoDB/LiteDB/IndexedDB/Redis/RavenDB/Firestore/AzureTable/DynamoDB.
- **Encrypted types throw `NotSupportedException`** — the stored body holds ciphertext and only the typed terminals decrypt. Never generate a raw terminal for a type with `cfg.MapProperty(x => x.P, p => p.Encrypt())`.
- **When JSON is an optimization and the typed path is also correct, probe — don't catch.** `query.SupportsRawJson` is `false` for an encrypted type and after `Select`/`Project`/`GroupBy`; generate `query.SupportsRawJson ? await query.ToJsonList(ct) : <typed path>`. That is what the built-in OData engine and the AI `query` tool do. Generate a bare raw terminal (no probe) only when the raw JSON *is* the requirement.
- **Shape differs from the object** — materialized computed properties live outside the body and blob payloads are metadata envelopes, so neither appears. Use the typed terminals when you need them.
- **`WriteJsonArrayTo` writes `[]` when nothing matches** and returns the row count; set `Response.ContentType` yourself.
- **Not on projections.** After `Select` / `Project` / `GroupBy` the raw terminals throw — the result is no longer a stored document. `Project(...)` already returns `IDocumentQuery<JsonObject>`; use its `ToList()`.

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

// Several properties in one statement (preferred over consecutive ExecuteUpdate calls)
int expired = await store.Query<Order>()
    .Where(o => o.Status == "open" && o.CreatedAt < cutoff)
    .ExecuteUpdate(b => b
        .Set(o => o.Status, "expired")
        .Set(o => o.ClosedAt, DateTimeOffset.UtcNow));

// One document — never materialize a list to take its first element
var youngest = await store.Query<User>().OrderBy(u => u.Age).FirstOrDefault();
var byEmail  = await store.Query<User>().Single(u => u.Email == email);

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

### `ToCursorPage` — cursor / keyset (seek) pagination

For **infinite scroll, deep paging, or large exports**, prefer cursor paging over offset paging: it stays O(log n) per page (with an index on the sort key) and doesn't skip/duplicate rows when documents change between fetches. Pass `null` for the first page; hand the previous page's `NextCursor` back for each subsequent one; a `null` `NextCursor` marks the end. The keyset is derived from the query's `OrderBy` (an `Id` tiebreaker is appended automatically).

```csharp
string? cursor = null;
do
{
    var page = await store.Query<Order>()
        .Where(o => o.Status == "open")
        .OrderByDescending(o => o.CreatedAt)   // keyset derived from THIS OrderBy
        .ToCursorPage(cursor, take: 50);       // CursorPage<T> { Items, NextCursor, HasMore }

    Render(page.Items);
    cursor = page.NextCursor;                  // null ⇒ last page
}
while (cursor != null);

// Or walk every page automatically (resumable full scan, no deep-offset cost):
await foreach (var o in store.Query<Order>().OrderByDescending(x => x.CreatedAt).ToCursorStream(pageSize: 200))
    Export(o);
```

- **Choose offset (`PageResult`) when you need a page number or a total count**; choose cursor when you only move forward, page deep, or forever-scroll.
- A cursor is valid only for the **exact same `OrderBy` + filters** that produced it — reusing it under a different sort throws `InvalidOperationException` (a shape hash catches it). Not valid after `Select`/`Project`/`GroupBy` (throws `NotSupportedException`). `take` must be `> 0` and `≤ 10,000`.
- Index the sort key (`cfg.MapIndexedProperty`) for hot cursor paths, and order by a **non-nullable** column (a `NULL` sort value at a page boundary can skip rows).
- **Provider tier:** relational providers seek server-side; LiteDB/IndexedDB/MongoDB page the keyset client-side; Cosmos/DynamoDB/Azure Table throw `NotSupportedException` (not yet supported).

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

## Grouped Aggregation (GROUP BY)

Use the explicit `GroupBy(keySelector).Select(g => …)` surface for a roll-up of one row per key.
Use `g.Key` for the group value and the `Sql` group aggregates — `g.Count()`, `g.Sum(x => x.Prop)`,
`g.Avg`, `g.Min`, `g.Max` — over the group's members. (For a single total over the **whole** filtered
set, prefer the scalar terminals `.Count()` / `.Sum()` / `.Average()` instead.) Aggregates are typed by the
selected member: `g.Sum`/`g.Avg` of a `decimal` keep full scale (exact on providers with a native decimal
type; SQLite aggregates as `REAL`), and `g.Min`/`g.Max` work over **dates and strings**, not just numbers.

```csharp
var rollup = await store.Query<Order>()
    .Where(o => o.CreatedAt >= since)
    .GroupBy(o => o.Status)                              // group key = a JSON property
    .Having(g => g.Sum(o => o.Total) > 10_000)          // optional: filter groups by an aggregate
    .Select(g => new StatusRollup
    {
        Status  = g.Key,                                // the group key
        Count   = g.Count(),
        Revenue = g.Sum(o => o.Total),
        AvgLine = g.Avg(o => o.Total)
    })
    .OrderByDescending(r => r.Revenue)                  // order the grouped rows (by an output column)
    .Paginate(0, 10)
    .ToList();

// Nested key:   .GroupBy(o => o.ShippingAddress.Country)
// Derived key:  .GroupBy(o => o.CreatedAt.Month)          // "revenue by month" — no stored column
// Multi-key:    .GroupBy(o => new { o.Status, o.Region }) // g.Key.Status / g.Key.Region

// String grammar (relational only): count()/sum(x)/avg(x)/min(x)/max(x) each need an alias.
var rows = await store.Query<Order>()
    .GroupBy("status")
    .Having("sum(total) > 10000")
    .Project("status, count() as orders, sum(total) as revenue")   // → JsonObject rows
    .ToList();
```

**Provider tier:** push-down on the relational providers (SQLite, SQLCipher, PostgreSQL, MySQL, SQL
Server, Oracle, DuckDB — `GROUP BY` + `HAVING` + grouped `ORDER BY` + multi/derived keys). MongoDB,
Cosmos, LiteDB and IndexedDB group **client-side** (typed surface only — no string grammar). Azure Table
and DynamoDB **throw** `NotSupportedException` (key-partitioned).

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

## Transactions (IDocumentSession)

Grouping writes into one transaction is done through a `UnitOfWork` created from the store — there is
no `RunInTransaction`. Queue `Add`/`AddRange`/`Update`/`Upsert`/`Remove`, then `SaveChanges`.

```csharp
await using var uow = store.OpenSession();   // IDocumentSession is the unit of work
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
`DocumentBulkContext.Assignments` is an ordered `IReadOnlyList<(string Property, object? Value)>` (13.0 — it
replaced the single nullable `Assignment` tuple): one entry for `ExecuteUpdate(property, value)`, several for
the builder overload, empty for Delete/Clear.

```csharp
opts.AddInterceptor(new AuditInterceptor());
opts.ConfigureDocument<Order>(cfg =>
{
    cfg.OnBeforeWrite((ctx, ct) => { /* mutate ctx.Document or throw to abort */ return Task.CompletedTask; });
    cfg.OnAfterWrite((ctx, ct) => outbox.Enqueue(ctx.Id, ctx.Operation, ct));
});
```

Interceptors can also be **registered in DI** to get constructor-injected dependencies. `AddDocumentStore` resolves every `IDocumentInterceptor` / `IDocumentBulkInterceptor` from the container and runs them after the options-registered ones. Since 11.0 this fires on **every** provider (previously DI interceptors silently never ran on the non-relational providers or in Orleans grain storage).

```csharp
public sealed class OutboxInterceptor(IOutbox outbox) : IDocumentInterceptor
{
    public Task BeforeWrite(DocumentWriteContext ctx, CancellationToken ct) => Task.CompletedTask;
    public Task AfterWrite(DocumentWriteContext ctx, CancellationToken ct) => outbox.Enqueue(ctx.TypeName, ctx.Id, ctx.Operation, ct);
}

services.AddSingleton<IDocumentInterceptor, OutboxInterceptor>();
services.AddDocumentStore(opts => opts.DatabaseProvider = new SqliteDatabaseProvider("Data Source=app.db"));
```

Register DI interceptors as **Singleton** (recommended) or **Transient** — the store is a singleton and resolves them once from the root provider. A **Scoped** `IDocumentInterceptor`/`IDocumentBulkInterceptor` registration makes `AddDocumentStore` throw a clear error at startup.

**Scoped services in a write — `ctx.Services` (11.0):** resolve scoped services from `ctx.Services` **inside the hook** (never the constructor). **No marker interface** — any DI-registered interceptor gets a scope, and `ctx.Services` is never null; interceptors are resolved fresh from the flowing scope per write, so **scoped interceptors are allowed** (`services.AddScoped<IDocumentInterceptor, X>()`). Through a scoped `IDocumentSession`/`DocumentContext` it's the caller's own request scope; through a raw singleton-store immediate write (MAUI/Orleans/background) the unit-of-work engine opens a fresh child scope per unit when DI interceptors are registered. **Do NOT** ship a `CreatedBy`/`UpdatedBy` audit interceptor on this — temporal (`CaptureActor` + `ChangesByActor`) already owns audit.

```csharp
public sealed class OrderValidationInterceptor : IDocumentInterceptor   // no marker; scoped registration also fine
{
    public int Order => 0;                                   // lower runs first
    public async Task BeforeWrite(DocumentWriteContext ctx, CancellationToken ct)
    {
        if (ctx.DocumentType != typeof(Order)) return;
        var validator = ctx.Services.GetRequiredService<IOrderValidator>();   // scoped; ctx.Services never null
        await validator.EnsureCanPlace((Order)ctx.Document!, ct);
    }
    public Task AfterWrite(DocumentWriteContext ctx, CancellationToken ct) => Task.CompletedTask;
}
```

**Transaction-visible store — `ctx.Store` (11.0):** a single write with per-doc interceptors runs as an implicit one-op unit of work, so `ctx.Store` is bound to that transaction. Read this unit's uncommitted rows and write side effects (an outbox row) that commit **atomically** with the triggering write. Use `ctx.Store`, NOT a DI-resolved `IDocumentStore` (that opens its own connection → not atomic + shared-connection deadlock). Wrap re-entrant side-effect writes in `ctx.Store.SuppressInterceptors()`. Full read-your-writes visibility is relational + LiteDB; other backends are committed-state. `ctx.Store` is never null; valid only within the hook.

```csharp
public async Task AfterWrite(DocumentWriteContext ctx, CancellationToken ct)
{
    using (ctx.Store.SuppressInterceptors())
        await ctx.Store.Insert(new OutboxEntry(ctx.Id!, "OrderPlaced"), ct);
}
```

**Ordering:** both interceptor interfaces expose `int Order => 0` — lower runs first; ties keep registration order (options before DI).

**Replacing a write — `ctx.Cancel()` (11.4):** in `BeforeWrite`, `ctx.Cancel(bool succeeded = true)` tells the store to issue **no** write for the operation because the interceptor performed one itself. No `AfterWrite` fires, no change notification / temporal history entry is written, and later interceptors are skipped. `Remove` returns the `succeeded` value; `Insert`/`Update`/`Upsert` return nothing, so it's ignored there. Set-based: `DocumentBulkContext.Cancel(int affected = 0)` in `BeforeBulkWrite`, with `ctx.QueryAs<T>()` giving back the originating query (same predicate + filters; `null` for `Clear`, where `ctx.Store` is the handle). Do the replacement write through `ctx.Store`/`ctx.Session` so it commits with the same unit. Valid only in the before-hook (throws elsewhere). To **fail** a write, throw — `Cancel` is not an error path. Cancelling during a provider `BatchInsert` (one set write) throws `NotSupportedException`; on relational stores a registered per-doc interceptor already makes `BatchInsert` loop the single-doc insert, so cancelling just skips that document. **Don't hand-roll soft delete on this — use `cfg.AddSoftDelete(...)` below.**

```csharp
opts.ConfigureDocument<Order>(cfg => cfg.OnBeforeWrite(async (ctx, ct) =>
{
    if (ctx.Operation != DocumentOperation.Delete) return;
    var updated = await ctx.Store.SetProperty<Order>(ctx.Id!, x => x.Status, "voided", null, ct);
    ctx.Cancel(updated);                     // no DELETE issued; Remove() returns `updated`
}));
```

## Writing a provider (12.0+)

The nine document providers share one `IDocumentQuery<T>` implementation. A provider derives from public **`DocumentQueryBase<T>`** and implements four members — `Clone()`, `ExecuteAsync(QueryPlan<T>)`, `DeleteMatchingAsync`, `SetPropertyMatchingAsync` — and inherits builder state/immutability, query-filter resolution, all client-side terminals, grouping, cursor paging, string projection, and the set-based-write interceptor plumbing. `ExecuteAsync` returns `QueryExecution<T>.Candidates` (nothing applied server-side), `.Filtered`, `.Complete`, or `.Partial(...)`; the base applies only what the engine did not. **Report push-down honestly** — claiming ordering/paging you did not apply silently returns wrong results. Optional hooks (`SetPropertiesMatchingAsync` for multi-property `ExecuteUpdate`, `ObserveChanges`, `FullTextSearchCore`, `NearestVectorsCore`, `ToQueryString`, `ToCursorPage`) and the aggregate hooks (`CountCore`/`MaxCore`/…) have safe defaults; override only what the engine can do itself. On the options side implement **`IDocumentStoreOptions`** — `Mappings`, `TypeNameResolution`, `Capabilities`, the interceptor pair, and `SerializerOptions`/`EnsureSerializerOptions` — and `ConfigureDocument<T>` plus every cross-cutting feature (soft delete, `cfg.MapJsonSchema`, field encryption) lights up for free. All per-type mapping state lives in the shared `DocumentMappingRegistry`; `Capabilities` is what the configuration validation pass reads to reject mappings your backend cannot honor.

Store writes bracket persistence with the shared pipeline on `DocumentProviderBase` (implement `Mappings`, `IdCache`, `ResolveTypeInfo`, `ResolveDocumentTypeName`): `BeginWriteAsync(op, doc, id, typeInfo, ct)` → check `write.Proceed` (false = an interceptor replaced the write; `Remove` returns `write.CancelResult`), take `write.Doc`, get the id with `ResolveInsertId`/`ResolveInsertIdAsync` (insert) or `RequireDocumentId` (update), persist, then `CompleteWriteAsync(write, id, version, changeType, doc, ct)` which runs `AfterWrite` and publishes the change (buffered until commit inside a unit of work). Never re-implement `PublishChange` — the base owns the broadcaster.

## Soft Delete (`AddSoftDelete<T>`)

A named query filter (`soft-delete`) plus a cancelling interceptor — **not** built into the stores. Map the
flag and `Remove`/`ExecuteDelete`/`Clear` set it instead of deleting, while every read hides flagged
documents. Works on every provider. `cfg.AddSoftDelete` is an **extension method**: on `DocumentStoreOptions`
for the relational stores, and on each provider's options class **in that provider's namespace** (e.g.
`using Shiny.DocumentDb.MongoDb;` for `MongoDbDocumentStoreOptions`).

```csharp
opts.ConfigureDocument<Customer>(cfg => cfg.AddSoftDelete(x => x.IsDeleted));  // bool      → true, filter !IsDeleted
opts.ConfigureDocument<Order>(cfg => cfg.AddSoftDelete(x => x.DeletedAt));     // DateTime? → now,  filter DeletedAt == null

await store.Remove<Customer>("c1");                        // UPDATE … flag = set
await store.Get<Customer>("c1");                           // null (hidden)
await store.Query<Customer>().IncludeDeleted().ToList();   // reads past the filter
await store.Query<Customer>().OnlyDeleted().ToList();      // just the flagged ones

await store.SoftDelete<Customer>("c1");                    // explicit; throws if T isn't mapped
await store.Restore<Customer>(x => x.Id == "c1");          // clears the flag — predicate, not id
await store.PurgeDeleted<Customer>();                      // real DELETE of flagged docs (optional predicate)
await store.HardDelete<Customer>("c1");                    // real DELETE of one live doc
using (store.SuppressInterceptors()) await store.Remove<Customer>("c1");   // raw write; await INSIDE the using
```

Rules: flag must be `bool` or nullable `DateTime`/`DateTimeOffset` (anything else throws); **one flag per
document type** (a second mapping on a different property throws); the mapping is process-wide per type, so
`Restore`/`PurgeDeleted`/`OnlyDeleted` work off a bare `IDocumentStore`. `Restore` takes a predicate because
a flagged document is invisible to a by-id write. `Insert`/`Upsert` are not filtered — inserting with the
flag already set yields an immediately invisible document. Change feeds emit `Updated` (not `Removed`) and
temporal records an update, because the write really is an update. The interceptor's `Order` is
`int.MaxValue`, so your own interceptors observe the `Delete` first. Requires `JsonTypeInfo<T>` on the SQL
providers, like any query filter.

**The serialized JSON on the context:** inside `BeforeWrite`, `ctx.GetJson()` returns the exact JSON
about to be persisted (serialized with the store's own options/`JsonTypeInfo`, cached, and invalidated
if an earlier interceptor replaces `ctx.Document`); `ctx.GetJsonDocument()` returns a parsed
`JsonDocument` (dispose it). Both return `null` for delete-by-id. Useful for auditing/redaction and the
primitive the JSON-Schema package builds on.

## Transactional Outbox (`AddOutbox` + `AddDocumentOutbox`, 13.0+)

Records a domain event **in the same transaction as the write that caused it**, then delivers it in the
background. Two registrations, each doing one thing:

```csharp
services.AddDocumentStore(o =>
{
    o.DatabaseProvider = new SqliteDatabaseProvider(path);
    o.AddOutbox();                 // maps OutboxMessage → its own "outbox" table + the version property
    // o.AddOutbox("my_outbox");   // custom table;  o.AddOutbox(null) leaves it in the shared table
});

services.AddDocumentOutbox<BusDispatcher>(o =>   // the BackgroundService + IOutboxAdmin
{
    o.MaxAttempts = 8;
    o.PollInterval = TimeSpan.FromSeconds(5);
    o.Retention = TimeSpan.FromDays(7);          // null keeps acknowledged messages forever
    o.OrderedPartitions = false;
});
// or, for a transport that needs no injected state:
services.AddDocumentOutbox((msg, ct) => bus.Publish(msg.MessageType, msg.Payload, ct));
```

**The rule that matters: always enqueue through the session doing the aggregate write.** A `store.Insert`
of the message in a separate call is a dual write with extra steps — the exact failure the outbox removes.

```csharp
await using var session = store.OpenSession();
session.Add(order)
       .Enqueue(new OrderPlaced(order.Id, order.Total))            // buffered, not written yet
       .Enqueue(new InventoryReserved(order.Id), partitionKey: order.Id);
await session.SaveChanges();       // aggregate + messages commit together, or neither does

session.EnqueueRaw("Shop.OrderPlaced", json);                       // pre-serialized payload
session.Enqueue(evt, MyContext.Default.OrderPlaced);                // AOT-safe overload
```

Declarative form — publishes for every write of a type, from `AfterWrite` (inside the write's transaction):

```csharp
o.ConfigureDocument<Order>(cfg => cfg.PublishToOutbox(
    ctx => new OrderChanged((string)ctx.Id!, ctx.Operation),        // return null to publish nothing
    OutboxOperations.Insert | OutboxOperations.Update,              // default: All
    partitionKey: ctx => (string)ctx.Id!));
```

Dispatcher — throw to retry (with backoff), return to acknowledge. Resolved from a **fresh DI scope per
message**, so scoped dependencies work:

```csharp
public sealed class BusDispatcher(IBus bus) : IOutboxDispatcher
{
    public Task Dispatch(OutboxMessage message, CancellationToken ct)
        => bus.Publish(message.MessageType, message.Payload, ct);
}
```

Monitoring and operations:

```csharp
await foreach (var m in store.WatchOutbox(o => o.States = OutboxStates.DeadLettered, ct)) { }  // read-only
await foreach (var e in store.WatchOutbox<OrderPlaced>(cancellationToken: ct)) { }             // decoded

var admin = sp.GetRequiredService<IOutboxAdmin>();
await admin.PendingCount();  await admin.OldestPendingAt();   // ALERT ON AGE, NOT DEPTH
await admin.DeadLetters();   await admin.Requeue(ids);        // requeue only affects dead letters
await admin.PurgeProcessed(DateTimeOffset.UtcNow.AddDays(-7));

// Host-free (MAUI, a job, a deterministic test):
var runner = new OutboxRunner(store, dispatcher, options, timeProvider);
await runner.DrainOnce();  await runner.PurgeExpired();  await runner.PendingDepth();
```

Rules and gotchas:

- **Delivery is at-least-once.** Consumers MUST be idempotent. There is no distributed transaction with the bus.
- **Provider gate.** Requires `IDocumentStore.SupportsTransactions` — relational + LiteDB only. MongoDB,
  Cosmos, Redis, RavenDB, Azure Table, DynamoDB, Firestore and IndexedDB implement a unit of work by
  *compensation*, which does nothing for a process that dies mid-unit. `AddDocumentOutbox` throws at **host
  startup** naming the provider. Suggest `IChangeFeedDocumentStore` there instead.
- `OutboxOptions` has no `TableName` — the table is a store mapping, set once on `AddOutbox`.
- Claiming is per-message optimistic concurrency (`ConcurrencyException` ⇒ another worker won). Any number of
  workers scale by just running; no leader election.
- The backoff doubles as a **visibility timeout**: claiming pushes `AvailableAt` forward, so a crashed
  processor releases its message instead of stranding it.
- `OrderedPartitions` orders within a `PartitionKey` only. A failed message blocks its own partition until it
  dead-letters, after which ordering for that key is broken by definition. No key ⇒ no ordering.
- `WatchOutbox` is a **monitor**, not a consumer: it never claims and does not promise every revision. Anyone
  who needs every message must be the dispatcher.
- `OutboxMessage` wire names are pinned with `[JsonPropertyName]` (camelCase) and mirrored by `OutboxFields`,
  because other processes read these rows. Never address them with `nameof`.
- AOT: `o.MessageTypeInfo = OutboxJsonContext.Default.OutboxMessage`.
- Telemetry: span `outbox.dispatch`, counters `db.client.outbox.dispatched` / `.dead_lettered`, histogram
  `db.client.outbox.dispatch.duration`, gauge `db.client.outbox.pending`. `traceparent` captured at enqueue,
  restored at dispatch.

## Field-Level Encryption (core, 13.0+)

Encrypt named properties at rest on **every** provider — in the core package, no extra reference. It is a
serialization-level transform (a `JsonTypeInfo` modifier installs an encrypting `JsonConverter`), NOT an
interceptor — interceptors are write-only, so nothing would decrypt on read.

```csharp
var key = AesGcmDocumentEncryptor.GenerateKey();            // 32 bytes; store it in a real secret store
opts.UseEncryptor(new AesGcmDocumentEncryptor("k1", key));
opts.ConfigureDocument<Patient>(cfg => cfg.MapProperty(x => x.Ssn, p => p.Encrypt()));                                 // Randomized (default)
opts.ConfigureDocument<Member>(cfg => cfg.MapProperty(x => x.Email, p => p.Encrypt(EncryptionMode.Deterministic)));    // equality-queryable
```

Stored value is `enc:1:<keyId>:<base64>`; `Get`/`Query` return plaintext. Rules to generate against:

- **`Randomized`** (default): different ciphertext per write. ANY predicate over the property throws — do not
  generate `Where(x => x.Ssn == …)` for a randomized property.
- **`Deterministic`**: same ciphertext per value ⇒ `==`, `!=` and `WhereIn` work (the predicate's constant is
  rewritten to ciphertext). **string properties only.** It leaks equality/frequency — recommend it only when a
  query genuinely needs it.
- Range/`StartsWith`/`Contains`/full-text/vector over an encrypted property throw. `OrderBy` is not rejected
  but sorts by ciphertext — never generate it.
- `null` stays JSON `null`, so `x.Ssn == null` works in both modes.
- Encryptable types: string, bool, int, long, double, decimal, Guid, DateTime, DateTimeOffset (+ nullable).
  Collections/complex objects are rejected at map time.
- Must be a direct property (`x => x.Ssn`), mapped while configuring the store, and the options need a
  `TypeInfoResolver`.
- Do NOT map an encrypted property as computed/indexed-for-range/full-text/vector.
- The raw JSON lane (`store.Collection(...)`) returns the envelope — it does not decrypt.
- **Encryption is at rest only.** `Get`/`ToList`, OData responses and the AI tool results all return the
  decrypted value. Mapping a property does NOT keep it out of an HTTP response — say so if the user seems to
  expect otherwise, and suggest a DTO/projection.
- **Serializing a materialized document re-encrypts it.** The converters are symmetric, so
  `JsonSerializer.Serialize(doc, storeOptions)` turns every mapped property back into an envelope (a *new* one
  each time under Randomized). Whenever you generate code that serializes documents with the store's options,
  wrap them: `JsonSerializer.Serialize(doc, DocumentEncryption.PlaintextView(storeOptions))` — it returns the
  same instance when nothing is encrypted, so it is free and safe to use unconditionally. It is a **writer**:
  never point it at a stored body, it does not decrypt.
- The JSON terminals on `Query<T>()` (`ToJsonList`, `FirstOrDefaultRawJson`, `WriteJsonArrayTo`, …) **throw
  `NotSupportedException`** for a type with any encrypted property. Read it typed.
- Rotation: add the new key to the ring → make it current → `await store.RewrapAsync<T>()` → only then retire
  the old key. Pre-encryption plaintext values keep reading, and `RewrapAsync` converts them.
- **`DocumentEncryptionFormat`** is the public, read-only contract for the stored envelope — for tooling that
  inspects stored bodies *without* a key ring. `DocumentEncryptionFormat.TryParse(value, out var info)` yields
  `info.Version`/`info.KeyId`; `TryRenderPlaintext(bytes, out var text)` renders decrypted bytes (every value
  codec is UTF-8 text, so no CLR type is needed). It never decrypts. Generate it for a backup checker, a
  repair job or a migration script — not a second hand-rolled `value.StartsWith("enc:")` test, which
  mis-classifies an ordinary value that happens to start with `enc:`.
- The **[Admin UI](https://shinylib.net/documentdb/admin/encrypted-fields/)** understands envelopes with no
  key: it badges them, reports how many values sit under each key id (so you can tell whether a `RewrapAsync`
  finished before retiring a key), and refuses a save that would replace an envelope with clear text. Its AI
  assistant never decrypts.

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
    .AddDocumentStore(o => { o.UseSqlite("app.db"); o.ConfigureDocument<TodoItem>(cfg => cfg.Table = cfg.TypeName); })
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
- Global `cfg.AddQueryFilter` predicates always apply underneath `$filter`. `$count` is pre-paging.
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
builder.AddDocumentStore("orders", configureOptions: o => o.ConfigureDocument<Order>(cfg => cfg.Table = cfg.TypeName));

// Container-aware option setup — configureServiceOptions runs with the resolved IServiceProvider
builder.AddDocumentStore("orders",
    configureServiceOptions: (sp, o) => o.AddInterceptor(sp.GetRequiredService<AuditInterceptor>()));

// Shared-table multi-tenancy in one line — wires TenantIdAccessor from a registered ITenantResolver
builder.Services.AddSingleton<ITenantResolver, MyTenantResolver>();
builder.AddDocumentStore("orders", settings => settings.MultiTenant = true);

// Orleans-on-DocumentDb silo
builder.AddDocumentStore("orleans");
builder.UseOrleans(silo => silo.UseAspireDocumentDb("orleans")); // grain storage + reminders + clustering + directory

// Typed DocumentContext on an Aspire resource — AddDocumentContextProvider resolves the injected conn string
// + provider, wires health + OTel, and returns the Action you pass to the generated Add{Context}[Factory].
// The generated method extends IServiceCollection, so call it on builder.Services:
builder.Services.AddOrdersContext(builder.AddDocumentContextProvider("orders"));            // scoped (ASP.NET Core)
builder.Services.AddOrdersContextFactory(builder.AddDocumentContextProvider("orders"));      // factory (MAUI/Blazor/desktop)
// Same optional configureSettings / configureOptions as AddDocumentStore; call once per context (own Aspire
// name) — each store is keyed by the context type, so multiple contexts coexist without shadowing.

// Admin UI (ShinyDocDbMyAdmin) as a resource — every referenced store comes up already connected.
// Outside an AppHost the same tool is `docker run ghcr.io/shinyorg/shiny-docdb-myadmin` (mirrored to
// aritchie/shiny-docdb-myadmin on Docker Hub), or on Docker Desktop:
//   docker extension install aritchie/shiny-docdb-myadmin-extension
// The extension seeds discovered database containers via the SAME env pair WithReference emits below.
builder.AddDocumentDbAdmin(port: 8085)          // name defaults to "documentdb-admin"; pass port: by name
       .WithReference(store)
       .WithDataVolume()                        // persist saved connections/queries across runs
       .WithHostPath("/host/dbs", "/databases") // needed to reach a FILE-backed store from the container
       .WithSecretKey(builder.AddParameter("admin-key", secret: true))
       .WithReadOnly()                          // blocks every write path incl. non-SELECT SQL
       .WaitFor(store);
```

The AppHost injects the connection string + a provider discriminator (`Shiny:DocumentDb:<name>:Provider`);
the client selects the matching provider. Resolve the store keyed: `[FromKeyedServices("orders")] IDocumentStore`.
Use `configureOptions: o => …` for plain option setup and `configureServiceOptions: (sp, o) => …` when an
option needs another DI service; set `DocumentStoreSettings.MultiTenant` for the common shared-table tenancy
case. Non-Aspire callers get the same primitive via `AddDocumentStore(services, name, (sp, o) => …)`. To back
a source-generated typed `DocumentContext` with an Aspire resource, use `AddDocumentContextProvider("name")`
(relational + SQLite only — same provider family as `AddDocumentStore`).

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
await using var uow = store.OpenSession();   // IDocumentSession is the unit of work
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
);
options.ConfigureDocument<User>(cfg => cfg.AddQueryFilter(u => !u.IsDeleted));   // unnamed
options.ConfigureDocument<Order>(cfg =>
{
    cfg.AddQueryFilter("tenant", o => o.TenantId == tenantCtx.Current);         // named
    cfg.AddQueryFilter("status", o => o.Status != "Archived");
});
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
options.ConfigureDocument<Order>(cfg => cfg.AddQueryFilter("tenant", o => o.TenantId == tenantCtx.Current));

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
            .Where(c => c.TenantId == "acme")   // non-removable scope — see below
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
| `Where(Expression<Func<T,bool>>)` | Non-removable scope predicate applied to **every** tool; AND-combined with the model's filter. Call multiple times to combine conditions. |
| `MaxPageSize(int)` | Cap maximum page size for query/aggregate (default 100) |

### Non-removable access filters (`Where`)

`AllowProperties`/`IgnoreProperties` gate which *fields* the LLM sees; `Where` gates which *rows* it can reach. The predicate is a hard server-side boundary the model can't see, disable, or widen past — it's AND-combined with the model's own `filter`. Enforced on every capability:

- `query` / `count` / `aggregate` — pushed into the store query (out-of-scope docs never returned or counted, even if the model names them).
- `get_by_id` / `delete` — an out-of-scope id is treated as "not found" (`found:false` / `deleted:false`); the document is untouched.
- `insert` — a document that would fall outside the filter is rejected (throws) and never written.
- `update` — the incoming document **and** the stored record it replaces must both be in scope (can't move a record out of scope, can't overwrite one the model can't see); returns `updated:false` when the stored record is out of scope.

```csharp
tools.AddType(jsonContext.Order, capabilities: DocumentAICapabilities.All, configure: b => b
    .Where(o => o.TenantId == "acme")
    .Where(o => !o.IsArchived));
```

Evaluated with compile-free, AOT-safe machinery — keep it to LINQ constructs the store can translate. Designed for **stable** scopes (a constant fixed for the singleton registration); for per-request isolation use store-level multi-tenancy / global query filters instead. Scoped `update` re-fetches by `Id`, so it throws on types whose Id is mapped to a differently-named property via `cfg.MapIdProperty` (other tools are unaffected).

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

### Request-resolved scope filters (13.0+)

`Where(predicate)` is fixed at registration. For a scope that differs per caller, use the callback overloads — resolved on **every tool call** from `AIFunctionArguments.Services`:

```csharp
tools.AddType(jsonContext.Order, configure: b => b
    .Where(o => o.TenantId == "acme")                                             // static
    .Where(ctx => o => o.Region == ctx.GetRequiredService<ICurrentUser>().Region)  // resolved per call
    .Where<ITenantContext>((tenant, _) => o => o.TenantId == tenant.TenantId)      // resolve-a-service
    .Where<IPermissionService>(async (perms, ctx) =>                               // async
    {
        var ids = await perms.GetVisibleCustomerIdsAsync(ctx.CancellationToken);
        return o => ids.Contains(o.CustomerId);
    }));
```

Rules when generating this:

- **It fails closed.** A throwing filter, an unresolvable service, a null `Services`, or a null returned predicate all fail the tool call. Never write a `try/catch` that lets a call proceed without the predicate.
- **Deny-all is `o => false`**, never `null`.
- **Set `Services` on the `IChatClient` lane** — `new AIFunctionArguments(args) { Services = scope.ServiceProvider }`. The MCP server does it for you.
- `AddDocumentStoreAITools` throws at **startup** when a `Where<TService>` service is not registered.
- Collections take the same overloads returning a string-grammar clause.

## MCP Server (Shiny.DocumentDb.Mcp / ShinyDocDbMcp, 13.0+)

The same AI tools, exposed over the Model Context Protocol. Two shapes:

```bash
# stdio tool, for a local desktop client
dotnet tool install -g ShinyDocDbMcp
shiny-documentdb-mcp --provider sqlite --connection "Data Source=app.db"
shiny-documentdb-mcp --profile prod-readonly        # a saved ShinyDocDbMyAdmin connection
shiny-documentdb-mcp --config ./documentdb-mcp.json # collections, scopes, capabilities
```

```csharp
// library, Streamable HTTP, for a shared server
builder.Services
    .AddDocumentDbMcpServer(mcp =>
    {
        mcp.AddType(AppJsonContext.Default.Order, capabilities: DocumentAICapabilities.ReadOnly, t => t
            .Where<ITenantContext>((tenant, _) => o => o.TenantId == tenant.TenantId)
            .IgnoreProperties(o => o.InternalNotes)
            .MaxPageSize(50));
        mcp.ExposeResources();      // documentdb://types, .../schema, .../sample, documentdb://stats
        mcp.ExposePrompts();        // explain-collection, build-filter
    })
    .WithHttpTransport();

app.MapDocumentDbMcp("/mcp").RequireAuthorization("mcp");
```

Rules when generating an MCP server:

- `AddDocumentDbMcpServer` **is** the SDK's `AddMcpServer()` — never call both. It returns `IMcpServerBuilder`, so the caller chains `.WithHttpTransport()` or `.WithStdioServerTransport()`.
- Its builder **is** `IDocumentAIToolBuilder` — same `AddType`/`AddCollection`, same per-type configuration.
- **Read-only is the default.** A write capability needs `mcp.AllowWrites()` *as well as* the per-type flag, or registration throws at startup.
- Always set a scope (`Where`) on anything multi-tenant, and prefer the resolved form on the HTTP transport.
- The stdio tool cannot express a lambda scope — static clauses only, declared in `--config`.

## REST + Live-Query Endpoints (Shiny.DocumentDb.AspNetCore, 13.0+)

```csharp
app.MapDocuments<Order>("/orders", o =>
{
    o.Operations = DocumentEndpoints.All;                 // default: Read | Count
    o.MaxPageSize = 100;
    o.DefaultPageSize = 25;
    o.TypeInfo = AppJsonContext.Default.Order;            // required for AOT
    o.DefaultOrderBy = x => x.Id;                         // cursor paging needs a stable order
    o.RequireIfMatch = false;
    o.AllowFilterOn(x => x.Status, x => x.CustomerId, x => x.Total);
    o.Scope<ITenantContext>((tenant, _) => x => x.TenantId == tenant.TenantId);
})
.RequireAuthorization("orders");

// schema-free lane (relational providers only)
app.MapDocumentCollection("/intake", "intake_forms", o => o.AllowFilterOn("score"));
```

Routes: `GET /`, `GET /{id}`, `GET /count`, `GET /stream`, `POST /`, `PUT /{id}`, `PATCH /{id}`, `DELETE /{id}` — each gated by its `DocumentEndpoints` flag.

Query string: `filter` (string grammar), `orderby` (`total desc`, comma-separated), `skip`/`take` (clamped to `MaxPageSize`), `cursor` (returns `{ items, nextCursor }`), `fields` (sparse fieldset).

Rules when generating endpoints:

- **Always set `Scope` on a public endpoint.** It is the only thing standing between a caller and every row. Out-of-scope is `404` (never `403`); a write that would land outside it is `400`. `DocumentScope.DenyAll<T>()` is the explicit "no access".
- **Always set `AllowFilterOn` on a public endpoint** — an empty allowlist means every field is filterable.
- `Scope<TService>` resolves from the **request** scope and is validated at startup.
- `PATCH` is RFC 7396: unspecified members preserved, explicit `null` removes.
- `ETag`/`If-Match` need a mapped version property (`cfg.MapVersionProperty`); `RequireIfMatch = true` makes a missing header `428`.
- `DocumentEndpoints.Stream` (SSE) requires a provider implementing `IObservableDocumentStore` — otherwise mapping throws at startup. Put it behind rate limiting; it is not a durable subscription.
- Do **not** map this and the OData endpoints on the same prefix. OData when the client speaks OData; this when you own both ends.

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
10. **Use `cfg.Table` for isolation** — when types have different lifecycles or access patterns, give them dedicated tables.
11. **Custom Id is independent of table mapping** — use `cfg.MapIdProperty(x => x.Slug)` to override the Id while keeping the type in the default shared table, or `cfg.Table` + `cfg.MapIdProperty(...)` to do both at once.
21. **Change monitoring uses `IAsyncEnumerable`, not `IObservable`** — consume `store.NotifyOnChange<T>(ct)` with `await foreach` (or `query.NotifyOnChange(ct)` for per-query). Wrap the loop in a background `Task.Run` if you need to keep doing work while events arrive; cancel the token to unsubscribe.
22. **Distinguish in-process vs native change feeds** — `IObservableDocumentStore.NotifyOnChange<T>` only sees writes through this store instance. To observe other writers, use `IChangeFeedDocumentStore.SubscribeChanges<T>` (Postgres / SQL Server / Cosmos only).
12. **DI registration uses the extensions package** — install `Shiny.DocumentDb.Extensions.DependencyInjection` and call `services.AddDocumentStore(opts => { opts.DatabaseProvider = ...; })`. There are no provider-specific DI methods.
13. **Raw SQL is provider-specific** — LINQ expressions work identically across all providers, but raw SQL queries (`store.Query<T>("sql")`) use provider-specific JSON functions. Prefer the fluent query builder for portable code. MongoDB, LiteDB, and IndexedDB do not accept raw SQL at all.
14. **Spatial queries require `cfg.MapSpatialProperty`** — call `cfg.MapSpatialProperty(x => x.Location)` (a `GeoPoint?`) or `cfg.MapSpatialProperty(x => x.Area)` (a `Geometry?`) at setup to register which property drives spatial indexing. The property may be nullable; documents with a `null` location are skipped by the index (no throw on write, never returned by spatial queries). All SQL providers (**SQLite, PostgreSQL, MySQL, SQL Server, Oracle, DuckDB**) plus **CosmosDB** and **MongoDB** support spatial; the fallback stores (LiteDB, IndexedDB, Azure Table, DynamoDB) throw `NotSupportedException`. Full geometry (lines/polygons + the `Geo*` predicate family) requires v11+.
15. **Backup is on concrete types, not `IDocumentStore`** — use `SqliteDocumentStore.Backup()`, `SqlCipherDocumentStore.Backup()`, or `LiteDbDocumentStore.Backup()` directly. Cast or store the concrete type.
16. **`ClearAllAsync` is SQLite-only** — available on `SqliteDocumentStore` only, deletes all documents across all tables including spatial sidecar data.
17. **Multi-tenancy uses the DI extensions package** — `AddDocumentStore(configure, multiTenant: true)` for shared-table, `AddMultiTenantDocumentStore(factory)` for tenant-per-database (options factory = relational, store factory = any provider). Both require `ITenantResolver` to be registered. Tenant stores are cached with a bound, so resolve `IDocumentStore` per scope and seed with `TenantStoreOptions.SeedFromRegisteredSeeders()`.
18. **Shared-table tenancy is transparent** — consumer code injects `IDocumentStore` normally; the tenant filter is applied automatically to all queries, inserts, updates, and deletes.
19. **Tenant-per-database registers IDocumentStore as scoped** — unlike the default singleton registration. This is required so the correct tenant store is resolved per request.
