---
name: shiny-documentdb
description: Generate or update .NET application code using Shiny.DocumentDb. Use for IDocumentStore, AddDocumentStore, IDocumentSession, document mappings and contexts, LINQ or dynamic queries, SQLite/SQLCipher and other DocumentDB providers, multi-tenancy, transactions, outbox, spatial/vector/full-text search, temporal history, and Orleans, REST, AI or MCP integrations.
auto_invoke: true
triggers:
  - document store
  - DocumentMetadata
  - document created updated timestamps
  - join documents
  - left join
  - IJoinQuery
  - IJoinResult
  - JoinKind
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
  - unique index
  - unique constraint
  - unique key
  - MapUniqueIndex
  - UniqueConstraintException
  - UniqueIndexMapping
  - UniqueIndexKeys
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
  - Shiny.DocumentDb.Hosting
  - DocumentPredicate
  - DocumentFilter
  - DocumentStoreAccessor
  - GetVersionMapping
  - GetMappings
  - host surface
  - custom host
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
  - orleans streams
  - stream provider
  - persistent streams
  - IAsyncStream
  - AddDocumentDbStreams
  - DocumentDbStreamOptions
  - IStreamAdmin
  - stream rewind
  - IsRewindable
  - IQueueAdapterFactory
  - LockMode
  - pessimistic locking
  - FOR UPDATE
  - SupportsPessimisticLocking
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
  - WithSeeder
  - WithAi
  - WithAiFor
  - WithAiWrites
  - AdminAiProvider
  - DocumentStoreSeedMode
  - DocumentStoreSeedContext
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
  - AddDocumentDbAdminTerminal
  - DocumentDbAdminTerminalTool
  - WithStartupProfile
  - aspire terminal attach
  - ASPIRETERMINAL001
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

# Shiny DocumentDb

Generate correct application code against the public DocumentDB APIs. Focus on setup, operations, observable behavior, and provider limitations. Internal table layouts, synchronization algorithms, caches, and provider write pipelines are background material in [references/architecture.md](references/architecture.md); read them only for implementation or diagnostic tasks that need them.

## Choose the relevant reference

Load only the reference and sections needed for the user's task.

| Task | Reference |
|---|---|
| Provider setup and compatibility, SQLite in WASM, DI, named stores, multi-tenancy, AOT | [setup.md](references/setup.md) |
| Document models, IDs, per-type mapping, typed contexts, computed properties, unique indexes | [documents.md](references/documents.md) |
| LINQ or dynamic filters, JSON results, projections, joins, aggregates, paging, indexes, global filters | [queries.md](references/queries.md) |
| CRUD, JSON collections, batches, sessions, transactions, pessimistic locking, optimistic concurrency, outbox, hooks, export/restore | [sessions-and-writes.md](references/sessions-and-writes.md) |
| Spatial, vector, full-text search, auto-embedding, VectorData connector | [search.md](references/search.md) |
| Seeding, blobs, temporal history, soft delete, encryption, JSON Schema | [data-lifecycle.md](references/data-lifecycle.md) |
| Orleans, offline sync, REST/SSE, OData, Aspire | [hosting-and-integrations.md](references/hosting-and-integrations.md) |
| AI tools, per-caller access scopes, MCP | [ai-and-mcp.md](references/ai-and-mcp.md) |
| Provider-specific constraints, telemetry, change monitoring, native feeds | [operations.md](references/operations.md) |
| Existing v10 code being upgraded | [migration.md](references/migration.md) |
| Provider development, custom transport hosts, index internals, legacy repair | [architecture.md](references/architecture.md) |

## Setup and lifetime

Use the provider already chosen by the application. Relational providers use `DocumentStoreOptions.DatabaseProvider`; document-native providers have their own store/options types. Read the setup reference for the selected provider rather than assuming registrations or capabilities are identical.

`AddDocumentStore` is part of core `Shiny.DocumentDb`; do not install the former `Shiny.DocumentDb.Extensions.DependencyInjection` package for current versions.

```csharp
using Shiny.DocumentDb;
using Shiny.DocumentDb.Sqlite;

services.AddDocumentStore(options =>
{
    options.DatabaseProvider = new SqliteDatabaseProvider("Data Source=app.db");
    options.JsonSerializerOptions = AppJsonContext.Default.Options;
    options.UseReflectionFallback = false;
    options.ConfigureDocument<Order>(cfg =>
    {
        cfg.MapIdProperty(x => x.Id); // optional: Id is the default convention
    });
});
```

The default store is a singleton. In ASP.NET Core add `.AddScopedDocumentSession()` and inject scoped `IDocumentSession`. For MAUI, desktop, background workers, or Orleans, inject `IDocumentSessionFactory` and open/dispose a session per unit of work. Inject `IDocumentStore` for immediate operations. Tenant-per-database stores are scoped; resolve them within the current tenant scope.

## Models, mappings, and serialization

- Put per-type configuration in `options.ConfigureDocument<T>(cfg => ...)`; the former flat `options.MapXxx<T>` methods were removed in v13. Store-level options remain on the options object.
- Leave `cfg.Table` unset for types sharing the default store table. A custom `cfg.Table` is exclusive to one document type; do not assign the same custom table to multiple types.
- Spatial, vector, and full-text mappings allow one declaration per type. Only the mapped geometry property is spatially queryable.
- Configure a source-generated `JsonSerializerContext` once through `JsonSerializerOptions`. Include document, projection, and aggregate result types. `JsonTypeInfo<T>` arguments then resolve automatically.
- For AOT set `UseReflectionFallback = false`, use named result types instead of anonymous grouped projections, and use `Dictionary<string, object?>` for raw query parameters. Derive the context from `JsonSerializerContext` and annotate it with `[JsonSerializable(typeof(T))]`; `JsonSerializerContext` itself is a base class, not an attribute.

## Reads and queries

```csharp
var order = await store.Get<Order>(orderId);
var page = await store.Query<Order>()
    .Where(x => x.Status == "Open")
    .OrderBy(x => x.Id)
    .PageResult(page: 1, pageSize: 25);
```

Prefer the fluent query API for portable queries. Raw SQL and feature availability depend on the provider. Use `ToAsyncEnumerable()` for incremental processing of large results. Use `PageResult` for records plus total count, or `ToCursorPage` for cursor paging with stable ordering.

For dynamic columns and filters use the public string-based `OrderBy`, `Where`, and `Project` APIs with source-generated `JsonTypeInfo<T>` from the configured `JsonSerializerContext`. Bind runtime values with the interpolated `Where` overload; do not concatenate them into filter strings. Use `.ToQueryString()` to inspect a query without executing it.

## Writes and transactions

- `Insert` creates; `Update` replaces an existing document by default; `Upsert` merges or inserts by default. Merge modes skip null properties. Non-nullable defaults still overwrite stored values.
- Non-default `Update(patch: true)` and `Upsert(patchIfUpdate: false)` modes are relational-only. Use a JSON collection patch or nullable patch fields when changing only selected keys.
- Normal document/query/batch/session APIs maintain related indexes. Do not write consumer SQL to synchronize vector or spatial storage.
- A session buffers writes; it is not a change tracker. Reads do not see queued changes before `SaveChanges`. Use mapped versions and handle `ConcurrencyException` for optimistic concurrency; check provider support before relying on transactions or locking.

```csharp
await using var session = store.OpenSession();
session.Add(order).Update(customer);
await session.SaveChanges();
```

Read the write reference before using explicit transactions, pessimistic locking, transactional outbox, bulk operations, interceptors, or `suppressInterceptors`. Suppression deliberately bypasses both per-document and bulk hooks.

## Behavioral constraints that affect application code

- Search features need their mappings and provider dependencies. Check capabilities; do not infer support merely because a mapping is accepted.
- Vector results are nearest-first. Raw score direction and scale vary by provider; do not reuse score thresholds across providers. Enabling vector support later does not backfill existing embeddings.
- `Clear<T>` does not record temporal deletion history. Use per-document `Remove` when deletions must be audited. Whole-store maintenance clears are not type- or tenant-scoped.
- `IObservableDocumentStore` sees changes through the current store instance. To observe other writers, use a supported native `IChangeFeedDocumentStore`; ordinary notifications are not a durable feed.
- For multi-tenant applications use tenant isolation and scoped filters. On public REST endpoints configure `Scope` and `AllowFilterOn`; an empty filter allowlist permits every field. On AI/MCP tools use access scopes and expose only required capabilities. Per-request scopes must resolve per caller; filter resolution failures must not grant access.

Keep generated code within the public application surface. Consult architecture references when the task specifically requires provider or host implementation; do not reproduce internal pipelines in application code.
