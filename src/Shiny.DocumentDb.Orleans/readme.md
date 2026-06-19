# Shiny.DocumentDb.Orleans

Microsoft Orleans **storage providers** implemented entirely against the backend-agnostic
`IDocumentStore` abstraction. One implementation, every Shiny.DocumentDb backend — which backends are
production-suitable is documented below rather than forked in code.

This package covers every Orleans storage extension point that maps onto a document store:

| Orleans option | Type | Registration |
| --- | --- | --- |
| **Grain storage** (+ `PubSubStore`) | `IGrainStorage` | `AddDocumentDbGrainStorage(name, …)` |
| **Reminders** | `IReminderTable` | `AddDocumentDbReminders(…)` |
| **Clustering / membership** | `IMembershipTable` | `AddDocumentDbClustering(…)` |
| **Grain directory** | `IGrainDirectory` | `AddDocumentDbGrainDirectory(name, …)` |

Grain storage, reminders, and grain directory reduce to single-key/conditional operations and are fully
generic. Membership additionally needs **multi-document transactions** (a `UnitOfWork`) to update the
member row and the table-version row atomically — so it requires a relational backend or MongoDB on a
replica set (not Cosmos). Stream *queue adapters* are intentionally out of scope: a queue is not a
document store.

## Why this design

Orleans grain storage is a versioned key/value contract: `Read`/`Write`/`Clear` keyed by
`(stateName, grainId)` with an ETag for optimistic concurrency. That maps cleanly onto
`IDocumentStore`:

| Orleans | Shiny.DocumentDb |
| --- | --- |
| document key | `Id = "{stateName}\|{grainId}"` |
| ETag | `GrainStateRecord.Version` (mapped via `MapVersionProperty`) |
| concurrency conflict | `ConcurrencyException` → `InconsistentStateException` |
| state blob | nested `JsonElement` (stays queryable, not opaque) |

Because the runtime binds only to `IDocumentStore`, the same code path serves all backends. Grain
state is stored as **queryable JSON** and `MapTemporal` can give you a free audit trail of every state
mutation — features Orleans' built-in providers don't offer.

## Usage

Relational backends (built-in path — the provider builds and owns its `DocumentStore`):

```csharp
siloBuilder.AddDocumentDbGrainStorage("Default", o =>
{
    o.DatabaseProvider = new PostgreSqlDatabaseProvider(connectionString);
    // o.TableName = "orleans_default";        // default: "orleans_{providerName}"
    // o.DeleteStateOnClear = true;            // default
});
```

MongoDB / Cosmos DB (first-class companion packages — `Shiny.DocumentDb.Orleans.MongoDb` /
`Shiny.DocumentDb.Orleans.CosmosDb` — wire the store + grain-state mapping for you):

```csharp
siloBuilder.AddMongoDbGrainStorage("Default", connectionString, databaseName: "orleans");
siloBuilder.AddCosmosDbGrainStorage("Default", connectionString, databaseName: "orleans");
```

Any other backend (generic escape hatch — you build the store):

```csharp
siloBuilder.AddDocumentDbGrainStorage("Default", o =>
{
    o.StoreFactory = sp => /* a fully-configured IDocumentStore with GrainStateRecord
                              mapped (type→table/collection + MapVersionProperty) */;
});
```

`AddDocumentDbGrainStorageAsDefault(...)` registers under Orleans' default provider name, and the same
provider also backs streaming `PubSubStore` when named accordingly.

## Provider compatibility (grain storage)

> The CAS-sensitive backends below are covered by the integration tests in
> `tests/Shiny.DocumentDb.Orleans.Tests` (PostgreSQL + MongoDB, including the stale-write conflict test).
> Broadening to the full Orleans storage-provider conformance suite per backend is the next step.

| Tier | Backends | Notes |
| --- | --- | --- |
| **Recommended** | PostgreSQL ✅, SQL Server, MySQL, Oracle | Atomic CAS: the version check is folded into the `UPDATE … WHERE` and row-count-verified, so the ETag is honored even during failover duplicate-activation windows. |
| **Supported** | MongoDB ✅ | Good key distribution (`_id` embeds the grain key). Atomic CAS via the version-predicate update filter (the stale-write test confirms a conflict throws). |
| **Limited / dev** | SQLite, LiteDB, IndexedDB, DuckDB | Single-writer / embedded / analytical engines — fine for dev, single-silo, or edge. |
| **Use with care** | CosmosDB | CAS is now correct (native `IfMatchEtag`), but the provider partitions by `typeName`, putting all state of a grain type in one logical partition (20 GB cap + hot-partition). Fine for modest grain populations; weigh partitioning before large-scale production. |

✅ = covered by automated integration tests.

## Roadmap

- [x] `IGrainStorage` (+ `PubSubStore`)
- [x] `IReminderTable` — single-row CAS + range query, fully generic over `IDocumentStore`
- [x] `IMembershipTable` — multi-document transactional CAS (relational / Mongo replica set)
- [x] `IGrainDirectory` — register-once CAS + conditional deletes
- [x] First-class Cosmos/Mongo registration for grain storage (`.MongoDb` / `.CosmosDb` packages)
- [x] Integration tests (PostgreSQL + MongoDB): grain storage, reminders, membership, grain directory
- [x] Source-generated (reflection-free) serialization — internal envelope types always; grain state via a `JsonSerializerContext` + `UseReflectionFallback = false`
- [ ] Full Orleans storage-provider conformance suite per backend → generate the matrix from results
- [ ] Cosmos/Mongo registration overloads for reminders + grain directory
