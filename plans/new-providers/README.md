# New provider plans

Five candidate backends for Shiny.DocumentDb, each with its own plan. All five are the
**document-native / NoSQL archetype** — they implement
`XDocumentStore : DocumentProviderBase, IDocumentStore` (plus opt-in capability interfaces) and
ship their own `IDocumentQuery<T>` + `Expression`→native translator. None of them implement the
relational `IDatabaseProvider` dialect (that's only for JSON-in-a-SQL-column backends).

The reference implementations to crib from:
- **Document-native, full server-side query** → `Shiny.DocumentDb.MongoDb`, `Shiny.DocumentDb.CosmosDb`
- **Key-partitioned, client-side query after a single-partition fetch** → `Shiny.DocumentDb.DynamoDb`, `Shiny.DocumentDb.AzureTable`
- **Thin subclass of an existing provider** (v11 pattern) → `Shiny.DocumentDb.MariaDb`, `Shiny.DocumentDb.CockroachDb`

| Provider | Plan | Archetype | Effort | Headline native features |
|---|---|---|---|---|
| RavenDB | [ravendb.md](ravendb.md) | document-native | Medium | Revisions→temporal, Changes API→change feed, native FTS + vector + spatial, real ACID UoW |
| Google Firestore | [firestore.md](firestore.md) | document-native | Medium | Real-time listeners→change feed, native transactions, aggregation queries, native KNN vector |
| Redis | [redis.md](redis.md) | document-native (Redis Stack) | Medium-High | RediSearch: server-side GroupBy, full-text, vector, geo; INCR→Int/Long autogen |
| Amazon DocumentDB | [amazon-documentdb.md](amazon-documentdb.md) | thin subclass of Mongo | Low | Reuses Mongo provider; capability down-flags + TLS |
| Couchbase | [couchbase.md](couchbase.md) | document-native | Medium-High | SQL++ full pushdown incl. GroupBy, FTS + vector, per-doc CAS, real ACID UoW |

Every plan follows the same shape and ends with the **four-artifact sync** required by
`CLAUDE.md` (code+tests, docs site, `SKILL.md`, `readme.md`) plus a release note.

## Cross-cutting conventions (apply to all five)

- **Base**: derive `DocumentProviderBase`; override `internal override InterceptorPipeline Interceptors => this.options.Interceptors`; call `AttachServiceProvider(sp)` from the DI ctor so container `IDocumentInterceptor`s and scoped-service interceptors resolve.
- **Options class** `XDocumentStoreOptions` with `TypeNameResolution` (default `ShortName`), optional `JsonSerializerOptions` (default camelCase/non-indented), `UseReflectionFallback=true`, `Logging`, plus the fluent mapping surface (`MapTypeToCollection`/`MapIdProperty`/`MapIdType`/`MapVersionProperty`/`MapIndexedProperty`/`AddQueryFilter`, and whichever of `MapFullTextProperty`/`MapVectorProperty`/`MapSpatialProperty`/`MapComputedProperty`/`MapTemporal` the backend can honor).
- **AOT/trim**: dual JSON path — prefer `JsonTypeInfo<T>`; reflection fallback only when `UseReflectionFallback` and no typeInfo, each fallback method carrying `[UnconditionalSuppressMessage("Trimming","IL2026")]` + `("AOT","IL3050")`. `FindTypeInfo<T>` throws in AOT-strict mode.
- **Ids**: reuse `IdAccessorCache`/`IdAccessor<T>` semantics — Guid→`N`, String→caller-assigned (Insert throws on default), Custom→`MapIdType` converter. Int/Long autogen throws *unless* the backend has a cheap atomic counter (Redis `INCR`, Couchbase counters).
- **In-process change notifications**: reuse `ChangeBroadcaster` + the `AsyncLocal` pending-changes buffer for `IObservableDocumentStore.NotifyOnChange`, flushed on UoW commit — identical to Dynamo/Azure.
- **Interceptors / query filters**: run `RunBeforeWriteAsync`/`RunAfterWriteAsync`, honor global `AddQueryFilter` predicates (apply client-side if the backend can't express them).
- **Capabilities** are declared by *which interfaces you implement* + `Supports*` bools + loud `NotSupportedException` on unimplemented terminators. There is no capabilities struct.
- **Tests**: add an `IDocumentStoreFixture : IAsyncLifetime` Testcontainers fixture and wire the provider into the shared conformance suite in `tests/Shiny.DocumentDb.Tests`, plus provider-specific tests for pushdown (`ToQueryString`) and CAS.
