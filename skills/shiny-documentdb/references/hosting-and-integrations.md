# Orleans, sync, OData, Aspire, and REST endpoints

Read only the sections relevant to the current task. These are application-facing API details.

## Contents

- Orleans Grain Storage
- Offline-First Sync (Shiny.DocumentDb.AppDataSync)
- OData Endpoints (Shiny.DocumentDb.OData / Shiny.DocumentDb.AspNetCore.OData)
- Aspire Integration (Shiny.DocumentDb.Aspire.Hosting / .Client / .Orleans)
- REST + Live-Query Endpoints (Shiny.DocumentDb.AspNetCore, 13.0+)

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
- **Clustering / membership (`IMembershipTable`)** — `AddDocumentDbClustering(...)`; default table `orleans_membership`. Per-silo rows + a global table-version row are updated together in a single `IDocumentSession`, each CAS-gated. **Requires multi-document transactions → relational or MongoDB replica set; Cosmos is NOT supported** (single-partition batches only).
- **Grain directory (`IGrainDirectory`)** — `AddDocumentDbGrainDirectory("Default", ...)`; default table `orleans_graindirectory`. Per-row version CAS for register/unregister races; no transaction required.
- Companion `.MongoDb`/`.CosmosDb` packages currently add grain-storage registration only; use `StoreFactory` to point reminders/membership/directory at a Mongo/Cosmos store. All three are covered by PostgreSQL integration tests.

### Orleans persistent streams (`AddDocumentDbStreams`)

A persistent stream provider on the same store — durable, inspectable Orleans streams with no queue service.

```csharp
siloBuilder.AddDocumentDbStreams("Default", o =>
{
    o.DatabaseProvider = new PostgreSqlDatabaseProvider(cs);
    // o.TotalQueueCount = 8;                      // queues AND counter rows → the enqueue-concurrency dial
    // o.MaxPollInterval = TimeSpan.FromSeconds(1);// idle backoff ceiling = worst-case latency with no change feed
    // o.Retention       = TimeSpan.FromHours(1);  // null keeps every delivered event forever
});
```

Produce/consume with the ordinary Orleans stream API (`GetStreamProvider("Default").GetStream<T>(...)`) — nothing is provider-specific. `clientBuilder.AddDocumentDbStreams(...)` lets an external client produce directly, which gives that client its own DB connection (fine on a trusted network, wrong for internet-facing — publish through a grain).

- **Backend gate is narrower than the other system stores and enforced at silo start**: PostgreSQL, SQL Server, MySQL, MariaDB, Oracle, CockroachDB. SQLite/LiteDB/DuckDB and the document/key-partitioned stores throw. The gate is the `IDocumentStore.SupportsPessimisticLocking` capability, not a name list.
- **Do not suggest SQLite or LiteDB for streams.** A local file cannot back a multi-silo cluster's queues; for a single silo Orleans' in-memory provider already applies. This is why the streams gate differs from the outbox's `SupportsTransactions` gate.
- **Sequencing**: a per-queue counter row reserved under `LockMode.Update` inside the enqueue transaction. Never propose a native identity/`BIGSERIAL` column here — values are assigned at insert but rows appear at commit, so a late-committing transaction gets stepped over by the receiver's watermark and its event is silently never delivered.
- **Delivery**: at-least-once, ordered per queue, failover replays from the acknowledgement watermark. **Not** transactional with grain state — for that use the outbox.
- **`IsRewindable` is `true`** — a subscriber may resume from an old `StreamSequenceToken` and the cache replays it from the events table. The rewind window is exactly `Retention` (default 1 hour); past it Orleans' ordinary cache-miss applies, so set `Retention` to the replay window you actually want.
- **Operations**: `IStreamAdmin`, resolved **keyed by provider name** (`services.GetRequiredKeyedService<IStreamAdmin>("Default")`) — `Queues()`, `TotalDepth()`, `StuckStreams()`, `Backlog()`. Read-only on purpose: a stream event is a position in a gap-free sequence every subscriber holds a cursor into, so there is no safe requeue/delete the way there is for an outbox message. Do not propose one. ShinyDocDbMyAdmin has a matching **Streams** screen in both front ends.
- Alert on **oldest-undelivered age**, not queue depth — depth alone cannot separate a busy queue from a dead pulling agent.
- **Throughput**: thousands of events/sec on PostgreSQL. Do not present it as an Event Hub / Kafka replacement.


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
    // Runs on TWO triggers only: first-time setup of this database, or a destructive recreation.
    // The "already seeded" marker (__shiny_documentdb_aspire_seed) lives IN the backing database, so
    // dropping the volume / deleting the SQLite file makes the next start a first-time setup again.
    // Restarting the AppHost against surviving data does NOT re-seed.
    .WithSeeder(async (ctx, ct) =>
    {
        var s = BuildStore(ctx.Provider, ctx.ConnectionString);   // ctx: StoreName, Provider, ConnectionString, Recreate
        if (ctx.Recreate && s is IDocumentMaintenance m)          // true only in Recreate mode on an already-seeded db
            await m.ClearAll(ct);                                 // the CALLBACK wipes; the AppHost issues no DDL
        await DocumentSeedRunner.RunAsync(s, seeders, cancellationToken: ct);
    });
    // ...or rebuild on every start:  .WithSeeder(Seed, DocumentStoreSeedMode.Recreate)   // dev loop only
    // Marker is written only after the callback succeeds -> a failed seed retries next start.
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
// Streams are opt-in — NOT part of DocumentDbOrleansFeatures.All, because they need a backend with
// row-level locking and the silo refuses to start without it (an All that included them would stop a
// working SQLite/DuckDB Aspire app from booting on a package update).
builder.UseOrleans(silo => silo.UseAspireDocumentDb(
    "orleans",
    DocumentDbOrleansFeatures.All | DocumentDbOrleansFeatures.Streams,
    streamProviderName: "Default",
    configureStreams: o => o.TotalQueueCount = 8));   // tuning only — the store always comes from Aspire

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
       // Configure the ASSISTANT from the AppHost. Doing so makes it READ-ONLY in the tool (same
       // treatment a host-provided connection gets) - the host is the single source of truth.
       // Azure/OpenAiCompatible REQUIRE endpoint: and throw here without it.
       .WithAi(AdminAiProvider.Anthropic, "claude-sonnet-4-5-20250929",
               builder.AddParameter("anthropic-key", secret: true))
       .WithAiWrites(insert: true)              // separate call ON PURPOSE; nothing granted without it
       .WithAiFor(otherStore, AdminAiProvider.OpenAI, "gpt-4o")  // per-store, layered over the default
       .WithDataVolume()                        // persist saved connections/queries across runs
       .WithHostPath("/host/dbs", "/databases") // needed to reach a FILE-backed store from the container
       .WithSecretKey(builder.AddParameter("admin-key", secret: true))
       .WithReadOnly()                          // blocks every write path incl. non-SELECT SQL
       .WaitFor(store);

// The TERMINAL front end as a resource instead (Aspire 13.5+, needs the shinydocdb tool installed).
// A process, not a container: it reaches a file-backed store with NO bind mount. Dev loop only —
// ExcludeFromManifest, so anything you PUBLISH still wants AddDocumentDbAdmin above.
// Requires #pragma warning disable ASPIRETERMINAL001 (Aspire's own experimental id; this surface reuses it).
builder.AddDocumentDbAdminTerminal()            // tool: DocumentDbAdminTerminalTool.Local => `dotnet tool run`
       .WithReference(store)                    // same env pair; store arrives as a host-provided connection
       .WithStartupProfile(store)               // --profile <store name>: attach lands ON the database
       .WithAi(AdminAiProvider.OpenAiCompatible, "llama3",         // same WithAi surface as the container
               endpoint: "http://localhost:11434/v1")
       .WithDataDirectory("./.admin")           // else it shares the developer's ~/.shinydocdbmyadmin
       .WithReadOnly()
       .WithoutAi()
       .WaitFor(store);
// Attach from the CLI (the dashboard lists the resource but has no terminal view in 13.5):
//   aspire config set features.terminalCommandsEnabled true
//   aspire terminal ps | aspire terminal attach documentdb-terminal   (Ctrl+B D detach, Ctrl+B T take control)
```

The AppHost injects the connection string + a provider discriminator (`Shiny:DocumentDb:<name>:Provider`);
the client selects the matching provider. Resolve the store keyed: `[FromKeyedServices("orders")] IDocumentStore`.
Use `configureOptions: o => …` for plain option setup and `configureServiceOptions: (sp, o) => …` when an
option needs another DI service; set `DocumentStoreSettings.MultiTenant` for the common shared-table tenancy
case. Non-Aspire callers get the same primitive via `AddDocumentStore(services, name, (sp, o) => …)`. To back
a source-generated typed `DocumentContext` with an Aspire resource, use `AddDocumentContextProvider("name")`
(relational + SQLite only — same provider family as `AddDocumentStore`).

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
