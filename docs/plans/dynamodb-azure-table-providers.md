# Plan: DynamoDB & Azure Table Storage providers

**Status:** Designed, not started.
**Target version:** `10.1.0` (two net-new packages — additive; raw version from `version.json` is
`10.0.0-beta.{height}`, so if 10.0 has not shipped these can ride the `10.0` section instead).
Two new NuGet packages — `Shiny.DocumentDb.AzureTable` and `Shiny.DocumentDb.DynamoDb` — each a
direct `IDocumentStore` implementation. No changes to the core contract, no breaking changes to any
existing provider.

**Supported providers added:** Azure Table Storage (and Cosmos DB Table API, same SDK) and Amazon
DynamoDB. Both are **schema-free key-partitioned JSON stores**, which is exactly the library's
identity — neither dilutes it the way a pure key/value store would.

> Self-contained build spec. The implementing agent does not have the design conversation —
> everything needed is here. Read `CLAUDE.md` (repo root) for the four-artifact rule (code+tests,
> docs site, skill, readme) before considering any commit "done".

Branch off `v10` (the current working branch) before starting. **Build Azure Table first**, then
DynamoDB reusing the scaffold — see sequencing at the end.

---

## Goal

Add two backends that expose the full **required** `IDocumentStore` surface (CRUD, batch, merge
upsert, typed `Query<T>()`, count/clear/remove, unit of work) plus optimistic concurrency, mapping
the library's `(typeName, id)` document identity onto each store's native partition model:

```csharp
// Azure Table
builder.Services.AddAzureTableDocumentStore(o =>
{
    o.ConnectionString = "UseDevelopmentStorage=true"; // or a real account / SAS / DefaultAzureCredential
    o.TableName        = "Documents";                  // one table; PartitionKey=typeName, RowKey=id
    o.MapVersionProperty<Order>(x => x.Version);        // opt-in optimistic concurrency (ETag-backed)
});

// DynamoDB
builder.Services.AddDynamoDbDocumentStore(o =>
{
    o.TableName   = "Documents";        // one table; PK=typeName (HASH), SK=id (RANGE)
    o.AutoCreateTable = true;           // dev convenience; off by default
    o.MapVersionProperty<Order>(x => x.Version);
});

// identical call site to every other provider from here on
var store = sp.GetRequiredService<IDocumentStore>();
await store.Insert(new Order { Id = Guid.NewGuid(), Total = 42m });
var open = await store.Query<Order>()
                      .Where(o => o.Status == "Open")     // client-side eval (see querying)
                      .OrderByDescending(o => o.CreatedAt)
                      .Paginate(0, 25)
                      .ToList();
```

Both are **NoSQL-path providers**: they implement `IDocumentStore` + `IDocumentQuery<T>` directly by
extending `DocumentProviderBase`, exactly like `CosmosDbDocumentStore`, `MongoDbDocumentStore`, and
`LiteDbDocumentStore`. They do **not** touch the relational `IDatabaseProvider` / `DocumentStore`
engine (that path emits SQL; these backends are not SQL/JSON-function engines).

### What this is NOT

- **Not** a relational provider. No `IDatabaseProvider` implementation, no SQL emission, no
  `JsonEachFrom`/`JsonExtractTyped` seam. If a future JSON-path or SQL feature is gated on
  `IDatabaseProvider`, these providers are simply out of scope for it (same as Cosmos/Mongo/LiteDB
  today).
- **Not** a full server-side query engine on day one. Rich predicates are evaluated **client-side**
  via the shared `ExpressionInterpreter` (the LiteDB model), after a native partition query pulls the
  candidate set. Server-side pushdown is a **progressive enhancement**, not a launch requirement.
- **Not** spatial / vector / full-text capable. `SupportsSpatial`, `SupportsVector`,
  `SupportsFullText` stay `false` (default) → the paired methods inherit the `NotSupportedException`
  default interface bodies. No temporal (`ITemporalDocumentStore`) at launch.

---

## Architecture recap (why this is cheaper than it looks)

The core has two extension points; these providers use the second exclusively:

1. `IDatabaseProvider` — SQL-dialect abstraction for relational backends via the shared
   `DocumentStore` engine. **Irrelevant here.**
2. `IDocumentStore` (`src/Shiny.DocumentDb/IDocumentStore.cs`) — implemented directly by NoSQL
   providers extending `DocumentProviderBase` (`src/Shiny.DocumentDb/DocumentProviderBase.cs`) and
   shipping their own `IDocumentQuery<T>`. **This is the path.**

Everything hard is already shared in `src/Shiny.DocumentDb/Internal/` and is reused verbatim:

- **Id handling** — `IdAccessorCache` / `IdAccessor` / `IdKind` (Guid/Int/Long/String/Custom,
  auto-gen rules). String Ids are never auto-generated (Insert throws on empty) — that rule carries.
- **Merge / diff** — `JsonMergePatch` (RFC 7396, backs `Upsert`), `JsonDiff` (RFC 6902, backs
  `GetDiff`). Read-merge-write, identical to Cosmos.
- **Client-side query** — `ExpressionInterpreter` compiles `Expression<Func<T,bool>>` predicates and
  selectors to run in memory (this is what removes the need for a native translator).
- **Type naming** — `TypeNameResolver`. **Interceptors** — `InterceptorPipeline`
  (`OnBeforeWrite`/`OnAfterWrite`). **In-proc change stream** — `ChangeBroadcaster` (optional).

The two files to crib from:
- **`src/Shiny.DocumentDb.CosmosDb/CosmosDbDocumentStore.cs`** — partition-key / point-read / ETag-CAS
  / merge-upsert / compensating unit-of-work patterns (near-1:1 blueprint).
- **`src/Shiny.DocumentDb.LiteDb/LiteDbDocumentQuery.cs`** — client-side query evaluation
  (`Materialize()` → `ExpressionInterpreter.Interpret`) so no expression translator is needed at launch.

---

## The `(typeName, id)` → native partition mapping (the crux)

Both backends are natural fits because the library already partitions documents by CLR type name —
the same choice Cosmos makes with `/typeName`.

| Concept | Cosmos (reference) | **Azure Table** | **DynamoDB** |
|---|---|---|---|
| Partition | `/typeName` | `PartitionKey = typeName` | PK attr `pk = typeName` (HASH) |
| Document key | `id` | `RowKey = id.ToString()` | SK attr `sk = id.ToString()` (RANGE) |
| Payload | `Data` (raw JSON) | `Data` string column (+ metadata cols) | `Data` string attribute (+ metadata) |
| Point read | `ReadItem(id, PK)` | `GetEntity(pk, rk)` | `GetItem(pk, sk)` |
| Type-scoped query | partition query | query filter `PartitionKey eq '<typeName>'` | `Query` with `KeyConditionExpression pk = :t` |
| Concurrency (CAS) | ETag `IfMatch` | `ETag` on `UpdateEntity` (`If-Match`) | `ConditionExpression` on version attr |
| Batch | `TransactionalBatch` ≤100/partition | `SubmitTransaction` ≤100/PartitionKey | `TransactWriteItems` ≤25/request |

Because `Query<T>()` is always type-scoped, the common read path is a **single-partition query on
both backends** — efficient, no cross-partition scan, no GSI required for the base feature set.

### Storage envelope (DTO)

Mirror `CosmosDocument` (`src/Shiny.DocumentDb.CosmosDb/CosmosDocument.cs`):

- **Azure Table** — a `TableEntity` (or `ITableEntity`): `PartitionKey`, `RowKey`, `Timestamp`, `ETag`
  (SDK-managed) + custom columns `Data` (string, the serialized JSON body), `CreatedAt`, `UpdatedAt`,
  and `Version` (long, present only when a version property is mapped — used for the app-level check;
  the SDK `ETag` is the physical CAS token). **Column-size caveat:** a single Table property is capped
  at 64 KB and an entity at 1 MB — document this; oversized bodies throw a clear
  `DocumentTooLargeException`-style error (or `NotSupportedException` with guidance), not a raw
  storage 413.
- **DynamoDB** — an attribute map: `pk` (S), `sk` (S), `Data` (S), `CreatedAt` (S, ISO-8601),
  `UpdatedAt` (S), `Version` (N, when mapped). Item size cap is 400 KB — same guard.

---

## Contract surface each provider must implement

From `src/Shiny.DocumentDb/IDocumentStore.cs`. All generic `<T> where T : class`, each taking an
optional `JsonTypeInfo<T>?` (AOT) and `CancellationToken`.

**Required (no default impl — must write):**

| Method | Azure Table | DynamoDB |
|---|---|---|
| `Query<T>(JsonTypeInfo?)` → `IDocumentQuery<T>` | own query builder | own query builder |
| `Insert<T>` | `AddEntity` (fails if exists) | `PutItem` w/ `attribute_not_exists(pk)` |
| `BatchInsert<T>` | `SubmitTransaction` in ≤100 chunks (per PartitionKey) | `BatchWriteItem` in ≤25 chunks |
| `Update<T>` (full replace) | `UpdateEntity(Replace)` + ETag | `PutItem` (+ version condition when mapped) |
| `Upsert<T>` (RFC 7396 merge) | read → `JsonMergePatch.Merge` → replace | read → merge → put |
| `SetProperty<T>` / `RemoveProperty<T>` | read-merge-write single prop | read-merge-write single prop |
| `Get<T>(id)` | `GetEntity(pk, rk)` (404 → null) | `GetItem` (miss → null) |
| `GetDiff<T>` | `JsonDiff` over current vs modified | same |
| `Query<T>(string whereClause, …)` | **throw `NotSupportedException`** at launch (LiteDB does) — later back with OData filter | **throw** at launch — later back with PartiQL |
| `QueryStream<T>(string whereClause)` | throw / async wrapper | throw / async wrapper |
| `Count<T>(whereClause?)` | client-side count of type-scoped query (no filter) | same |
| `Remove<T>(id)` | `DeleteEntity` | `DeleteItem` |
| `Clear<T>()` | query PK, batch-delete in chunks | query PK, batch-delete in chunks |
| `CreateUnitOfWork()` | compensating tracker (see below) | compensating tracker |

**Default interface impls (override only if worthwhile):**
- `BatchUpsert` / `BatchUpdate` / `BatchRemove` — default loops the single-doc method. **Override**
  both providers to use native batch/transaction in bounded waves (Cosmos does this), respecting the
  ≤100 (Table) / ≤25 (DynamoDB) chunk limits.
- Capability flags — leave `SupportsSpatial` / `SupportsVector` / `SupportsFullText` at `false`; the
  paired methods keep their throwing defaults.

**Optional interfaces — implement selectively:**
- `IDocumentMaintenance.ClearAll(...)` — **implement** (delete/recreate the table, or scan-delete all
  partitions). Cheap and useful for tests.
- `IChangeFeedDocumentStore.SubscribeChanges<T>(…)` — **DynamoDB only, deferred to phase 2**: map to
  **DynamoDB Streams**. Azure Table has no change feed → do not implement (Cosmos DB Table API does,
  but treat that as out of scope). This is a genuine DynamoDB differentiator.
- `IObservableDocumentStore.NotifyOnChange<T>` — optional in-proc broadcaster (LiteDB model). Nice for
  dev; not required. Consider phase 2.
- `ITemporalDocumentStore` — **not** implemented at launch (no interface declared → feature absent).
- `IUnitOfWorkEngine.RunUnitAsync` — backs `CreateUnitOfWork()`. **Compensating tracker, not a real
  transaction** (follow `CosmosDbDocumentStore` + its `CompensatingStore.cs`): capture pre-images,
  apply, and on failure roll back via inverse ops. Cross-type atomicity is **not** guaranteed
  (documents span partitions on both backends) — same honest limitation as Cosmos. Where all writes
  in a unit share one partition and fit the limit, the native transaction (`SubmitTransaction` /
  `TransactWriteItems`) *may* be used as an optimization, but the compensating path is the contract.

---

## Querying — `IDocumentQuery<T>` (client-side, LiteDB model)

There is **no `IQueryable`**. `IDocumentQuery<T>` (`src/Shiny.DocumentDb/IDocumentQuery.cs`) is a
fluent builder over `Expression<Func<T,bool>>`. Implement it the LiteDB way
(`LiteDbDocumentQuery.cs:256` `Materialize()`):

1. **`Materialize()`** runs a native type-scoped partition query (`PartitionKey eq typeName` /
   `KeyConditionExpression pk = :t`) to pull all candidate documents of the type, deserializing each
   `Data` body to `T`.
2. Apply accumulated `Where` predicates via `ExpressionInterpreter.Interpret(expr)` (compiled
   `Func<T,bool>`), then query filters (unless `IgnoreQueryFilters`), then `OrderBy`/`OrderByDescending`,
   then `Paginate(offset, take)` — all in memory (LINQ-to-Objects).
3. **`Select<TResult>`** projects client-side via a compiled selector; return a
   `XxxProjectedDocumentQuery<T,TResult>` whose further-chaining members throw (mirror
   `LiteDbProjectedDocumentQuery` / `CosmosDbProjectedDocumentQuery`).
4. `Count` / `Any` / `Max` / `Min` / `Sum` / `Average` — LINQ-to-Objects over the materialized+filtered
   set. `ExecuteDelete` / `ExecuteUpdate` — materialize the matching set, then batch-delete / write.
5. **Throw `NotSupportedException`** (the established pattern) for: `Project(string)`,
   `ToQueryString()`, `NearestVectors`, `FullTextMatch`, `NotifyOnChange` (unless the observable
   interface is added), and chaining after `Select`.

> **Server-side pushdown (progressive enhancement, deferred).** Before materializing, translate the
> subset of the predicate the backend can evaluate natively and push it down to shrink the candidate
> set; evaluate the remainder client-side. Azure Table → OData filter (`eq/gt/ge/lt/le/and/or` on
> `Data`-shredded columns is not possible, but on promoted columns it is — see below). DynamoDB →
> `FilterExpression` on top-level attributes / PartiQL. **Launch ships pure client-side eval**; add a
> `XxxExpressionVisitor` later that returns "translatable prefix + client-side residue". Document the
> full-partition-scan cost of the client-side path loudly (same shape as any "loads all of the type"
> warning).

### Optional: promoted/indexed columns (phase 2, both backends)

To make pushdown and secondary lookups real, offer opt-in column promotion in options
(`MapIndexedProperty<T>(x => x.Status)` style): the mapped scalar is written as a **native top-level
column/attribute** alongside `Data`, so Table OData filters and DynamoDB GSIs can target it. This is
the escape hatch from full-partition scans for hot query paths. Not required for launch; call it out
as the documented scaling story.

---

## Keys, Ids & concurrency (the Cosmos playbook, transferred)

- **Id kinds.** Guid/String generated client-side work everywhere. `RowKey`/`sk` is the id's string
  form. **Int/Long auto-generation is the one real friction:** neither backend has a cheap `MAX(id)`
  (Cosmos pays for it with `SELECT VALUE MAX`). Do **not** ship a scan-based auto-increment footgun.
  Decision: **support Guid/String Ids for auto-gen; throw a clear `NotSupportedException` on Int/Long
  auto-gen** for these two providers, steering callers to Guid/String. (Explicitly-supplied Int/Long
  Ids are fine — only server-side generation is unsupported.) Document prominently.
- **Optimistic concurrency (CAS).** When `MapVersionProperty<T>` is set, follow Cosmos
  (`CosmosDbDocumentStore.cs` version-check + native precondition, throwing `ConcurrencyException`
  from `src/Shiny.DocumentDb/ConcurrencyException.cs`):
  - **Azure Table** — read current entity, compare mapped version to expected (mismatch →
    `ConcurrencyException`), increment, and write with `If-Match: <ETag>`; catch the 412
    `RequestFailedException` (Precondition Failed) → `ConcurrencyException`.
  - **DynamoDB** — write with `ConditionExpression: Version = :expected` (and
    `attribute_not_exists` on first insert); catch `ConditionalCheckFailedException` →
    `ConcurrencyException`.
  - Blind upsert (version 0 / unmapped) = last-write-wins; version > 0 = guarded. Same semantics as
    Cosmos.
- **Upsert = RFC 7396 merge.** Read existing → `JsonMergePatch.Merge(existingData, patchJson)` after
  null-strip → write. Identical to `CosmosDbDocumentStore` upsert. When versioned, the merge-write is
  the CAS-guarded write above.

---

## Package & class layout (mirror LiteDB, the simplest existing provider)

Each provider is a small package. New dirs under `src/`:

```
src/Shiny.DocumentDb.AzureTable/
  AzureTableDocumentStore.cs             : DocumentProviderBase, IDocumentStore, IDocumentMaintenance, IUnitOfWorkEngine, IAsyncDisposable
  AzureTableDocumentQuery.cs             : IDocumentQuery<T>  (+ AzureTableProjectedDocumentQuery<TSource,TResult>)
  AzureTableDocumentStoreOptions.cs      : standalone options class (see below)
  AzureTableDocument.cs                  : ITableEntity envelope
  ServiceCollectionExtensions.cs         : AddAzureTableDocumentStore(...)
  Shiny.DocumentDb.AzureTable.csproj     : ref Azure.Data.Tables

src/Shiny.DocumentDb.DynamoDb/
  DynamoDbDocumentStore.cs               : DocumentProviderBase, IDocumentStore, IDocumentMaintenance, IUnitOfWorkEngine, (phase2: IChangeFeedDocumentStore), IAsyncDisposable
  DynamoDbDocumentQuery.cs               : IDocumentQuery<T>  (+ projected inner class)
  DynamoDbDocumentStoreOptions.cs        : standalone options class
  DynamoDbDocument.cs                    : attribute-map <-> envelope marshalling
  ServiceCollectionExtensions.cs         : AddDynamoDbDocumentStore(...)
  Shiny.DocumentDb.DynamoDb.csproj       : ref AWSSDK.DynamoDBv2
```

**Options are a standalone class** (not inheriting `DocumentStoreOptions` — LiteDB re-declares its
surface; follow `LiteDbDocumentStoreOptions.cs`). Re-declare only the knobs that apply:
`ConnectionString`/credentials, `TableName`, `TypeNameResolution`, `JsonSerializerOptions`,
`UseReflectionFallback`, `Logging`, `MapTypeToPartition<T>` (override the default typeName),
`MapIdProperty`/`MapIdType`, `AddQueryFilter`, `AddInterceptor`/`OnBeforeWrite`/`OnAfterWrite`,
`MapVersionProperty`, and `AutoCreateTable`. Omit `MapTemporal`/`MapFullTextProperty`/spatial/vector
(unsupported). Phase 2 adds `MapIndexedProperty` (promoted columns).

**Registration.** Ship an `Add…DocumentStore(this IServiceCollection, Action<Options>)` extension for
each (LiteDB currently ships none — improve on that). It news up the store and registers
`AddSingleton<IDocumentStore>(...)`, plus `AddSingleton<IDocumentMaintenance>` etc. Note the generic
`AddDocumentStore(...)` in `Shiny.DocumentDb.Extensions.DependencyInjection` only builds the relational
engine, so these need their own extension (as Cosmos/Mongo do).

**Credentials.** Azure Table — support connection string, account-key, SAS, and
`TokenCredential`/`DefaultAzureCredential`. DynamoDB — standard AWS credential chain + explicit
`AWSCredentials`/region/`ServiceURL` (for DynamoDB Local in tests).

---

## Testing (`tests/Shiny.DocumentDb.Tests`, run the suite before "done")

The existing provider fixtures are the model. Both new providers must run the **shared conformance
suite** (whatever base fixture Cosmos/Mongo/LiteDB share) against a local emulator:

- **Azure Table** — **Azurite** (`azurite` / the Azurite emulator, `UseDevelopmentStorage=true`), same
  spirit as any container-backed provider test. Gate on the emulator being available.
- **DynamoDB** — **DynamoDB Local** (the `amazon/dynamodb-local` container / `ServiceURL`).

Cover, per provider:
- **CRUD round-trip** for each `IdKind` **except** Int/Long auto-gen — assert `Insert` of an
  Int/Long-Id type *without* an explicit id throws `NotSupportedException` with the steering message;
  explicit Int/Long ids round-trip fine.
- **Upsert merge** (RFC 7396): partial patch merges, null strips, nested-object merge.
- **`GetDiff`** (RFC 6902) produces a correct patch.
- **Query** (client-side): `Where` (int/decimal/bool/string/DateTime predicates), combined `Where`
  (AND), `OrderBy`/`OrderByDescending`, `Paginate`, `Count`/`Any`, aggregates, `Select` projection +
  throw-on-chain-after-`Select`, query filters + `IgnoreQueryFilters`.
- **Concurrency**: with `MapVersionProperty`, a stale `Update`/`Upsert` throws `ConcurrencyException`;
  version increments on success; blind (unversioned) upsert is last-write-wins.
- **Batch**: `BatchInsert`/`BatchUpsert`/`BatchRemove` across chunk boundaries (>100 for Table, >25
  for DynamoDB) to prove the wave-chunking is correct.
- **`Clear<T>`** removes only that type's partition; other types untouched.
- **`ClearAll`** empties the table.
- **Unsupported surface throws**: string `Query`/`QueryStream`, `ToQueryString`, `Project(string)`,
  `SupportsSpatial/Vector/FullText == false` and their methods throw, no `ITemporalDocumentStore`.
- **Size guard**: an oversized body throws the clear too-large error, not a raw SDK 4xx.
- **DynamoDB Streams change feed** (phase 2, if built): `SubscribeChanges` observes an insert/update.

---

## The four-artifact checklist (per `CLAUDE.md`)

1. **Code + tests** — as above. These are **backend-specific** providers — the release note must state
   the tier: NoSQL key-partitioned stores; client-side query evaluation (full-partition scan per type
   unless promoted columns are mapped); **Int/Long auto-gen unsupported (use Guid/String)**; no
   spatial/vector/full-text/temporal; DynamoDB adds an optional Streams-backed change feed.
2. **Docs site** (`~/Desktop/dev/documentation/src/content/docs/documentdb/`) — add
   `azure-table.mdx` and `dynamodb.mdx` provider pages (registration, credentials, the
   `(typeName,id)` key model, the query/scan cost caveat + promoted-column escape hatch, the Int/Long
   caveat, size limits, concurrency). Update the **provider compatibility matrix** on `querying.mdx`
   (or wherever it lives) to add both rows across every capability column. Add **release notes** under
   the current `## 10.x TBD` section in `release-notes.mdx`:
   `<RN type="feature">Azure Table Storage provider (Shiny.DocumentDb.AzureTable) — schema-free document store over Azure Table / Cosmos Table API; ETag optimistic concurrency, client-side LINQ query, native batch …</RN>`
   and a matching `<RN type="feature">` for DynamoDB (note the Streams change feed).
3. **Skill** (`skills/shiny-documentdb/SKILL.md`) — add both providers to the provider list and any
   capability matrix, the two `Add…DocumentStore` registration snippets, the Int/Long-auto-gen
   limitation, the client-side-query cost note, and the unsupported-feature list. Add keywords to
   `triggers:` (`AzureTable`, `Azure Table`, `TableStorage`, `DynamoDb`, `DynamoDB`, `AWS`,
   `AddAzureTableDocumentStore`, `AddDynamoDbDocumentStore`).
4. **readme.md** (repo root) — add Azure Table and DynamoDB to the provider list (the readme is packed
   into the NuGet package).

---

## Sequencing

1. **Azure Table first** — lowest effort (natural PartitionKey/RowKey fit, first-class ETag CAS,
   simple string `Data` column, mature `Azure.Data.Tables` SDK). Landing it hardens the shared
   "client-side query + partition-key store" scaffold.
2. **DynamoDB second** — reuse the scaffold; the extra work is attribute-value marshalling, the AWS
   credential/region surface, conditional-write CAS, and (phase 2) the Streams change feed + PartiQL
   string query.
3. **Phase 2 (either, after both ship)** — promoted/indexed columns (`MapIndexedProperty`) for
   server-side pushdown (OData filter / GSI), the string `Query`/`ToQueryString` implementations
   (OData / PartiQL), DynamoDB Streams `IChangeFeedDocumentStore`, and optional
   `IObservableDocumentStore`.

---

## Decisions locked

- **Two separate packages**, one `IDocumentStore` each, extending `DocumentProviderBase` — not a shared
  "NoSQL base" refactor (the existing NoSQL providers don't share one; don't invent it here).
- **Client-side query at launch** (LiteDB `ExpressionInterpreter` model); server-side pushdown is a
  documented phase-2 enhancement gated on promoted columns. No native expression translator on day one.
- **`(typeName, id)` → partition/sort key** on both backends (mirrors Cosmos `/typeName`), one table
  per store by default.
- **ETag / conditional-write CAS** wired to the existing `MapVersionProperty` + `ConcurrencyException`
  contract.
- **Int/Long auto-gen throws `NotSupportedException`** on both (no cheap MAX); Guid/String are the
  supported auto-gen kinds. Explicit Int/Long ids allowed.
- **No spatial/vector/full-text/temporal**; string `Query`/`ToQueryString`/`Project(string)` throw at
  launch (LiteDB precedent).
- **Compensating unit-of-work** (no cross-partition transaction), matching Cosmos; native batch only as
  an in-partition optimization.

## Alternatives considered / rejected

- **A shared `NoSqlDocumentStoreBase` extracted from Cosmos/Mongo/LiteDB.** Rejected for this cut — the
  three don't share one today, the abstraction isn't proven, and extracting it is a separate refactor
  with its own risk. Copy the patterns; refactor later if a fourth/fifth NoSQL provider makes the
  duplication painful.
- **Native server-side query as a launch requirement.** Rejected — Table's OData subset and DynamoDB's
  key-condition/filter model can't express arbitrary predicates over an opaque `Data` JSON blob without
  promoted columns, and `ExpressionInterpreter` already gives correct results. Ship correct-but-scans
  first; make it fast with promoted columns in phase 2.
- **Scan-based Int/Long auto-increment** (or a counter item). Rejected — a hidden full-scan / hot
  counter-item footgun. Throw and steer to Guid/String, matching the "honest `NotSupportedException`"
  discipline used elsewhere.
- **Table-per-type instead of one table + typeName partition.** Rejected — breaks the library's
  cross-type `IDocumentStore` model, multiplies provisioning, and diverges from the Cosmos precedent.
- **Skipping DynamoDB Streams.** Not rejected — deferred to phase 2. It's the one place DynamoDB beats
  Table (`IChangeFeedDocumentStore`), worth doing, but not a launch blocker.

## Open questions (resolve during build, none block design)

- **Azure Table 64 KB-per-column limit** — a single `Data` string column caps the body at 64 KB (entity
  1 MB). Decide launch behavior: (a) throw a clear too-large error (simplest, recommended), or (b)
  split the body across multiple string columns (`Data`, `Data1`, …) to reach ~1 MB. Recommend (a) for
  launch, document, revisit if users hit it.
- **DynamoDB read consistency** — expose an option for strongly-consistent reads on `Get`/`Query`
  (default eventually-consistent). Cheap to add; decide default (eventual, matching DynamoDB's own).
- **Cosmos DB Table API** — `Azure.Data.Tables` also targets Cosmos Table API. Decide whether to
  document/test it as a supported endpoint of the Azure Table provider (likely yes, zero code cost) or
  stay silent.
- **`RowKey`/`sk` for non-string ids** — lock the canonical string encoding (invariant `ToString()`
  for Guid/Int/Long) and ensure ordering expectations for `OrderBy` on the id are documented (lexical,
  not numeric — matters if anyone sorts by an Int id stored as a string key).
- **Batch partial-failure semantics** — DynamoDB `BatchWriteItem` returns `UnprocessedItems`; decide
  retry-with-backoff vs. surface. `TransactWriteItems` is all-or-nothing. Align `BatchInsert` return
  count semantics with the other providers.
