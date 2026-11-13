# Provider notes, telemetry, concurrency, and change monitoring

Read only the sections relevant to the current task. These are application-facing API details.

## Contents

- Telemetry & Diagnostics
- Concurrency and lifetime
- Change Monitoring (IObservableDocumentStore)
- Native Change Feeds (IChangeFeedDocumentStore)

## Telemetry & Diagnostics

The core `Shiny.DocumentDb` package emits OpenTelemetry-native metrics + tracing for every operation — **embedded and always-on** on every provider and construction path (no decorator, no opt-in, no separate package; zero-cost when unobserved). Just subscribe your OTel pipeline to the meter/source `Shiny.DocumentDb`:

```csharp
services.AddDocumentStore(o => o.DatabaseProvider = new SqliteDatabaseProvider("Data Source=app.db"));

services.AddOpenTelemetry()
    .WithMetrics(m => m.AddMeter("Shiny.DocumentDb"))
    .WithTracing(t => t.AddSource("Shiny.DocumentDb"));
```

**REMOVED in 11.0** (do not generate): `AddDocumentStoreInstrumentation()`, `InstrumentedDocumentStore`, and `DocumentStoreOptions.Instrumentation`. Instrumentation is embedded; there is nothing to call. A session emits a correlated `unit_of_work` parent span; `SaveChanges` records the buffered-operation count.

**Structured `ILogger` logging:** when a store is created from the container (any provider's `Add…DocumentStore` or `IServiceProvider` ctor) with an `ILoggerFactory` registered, every SQL / operation statement is logged at `Debug` under the `Shiny.DocumentDb` category (control via `Logging:LogLevel:Shiny.DocumentDb`), composed with the `options.Logging` string callback — on the relational core and all six non-relational providers. Container-free `new …DocumentStore(options)` is callback-only, unchanged.

**Keyed/named stores:** a store registered with `AddDocumentStore("orders", …)` automatically tags its signals with `db.namespace = "orders"` so multiple stores are distinguishable. The non-keyed path omits `db.namespace`.

Built on `System.Diagnostics.Metrics.Meter` (via `IMeterFactory`) and `ActivitySource`. Emits, per the OTel database client semantic conventions: a `db.client.operation.duration` histogram (plus a `db.client.operations` counter and a `db.client.response.returned_rows` histogram), tagged `db.system.name` / `db.operation.name` / `db.collection.name` / `outcome` / `error.type` (plus `db.namespace` on named stores); and a `{system}.{operation}` `ActivityKind.Client` span per call with error status + exception capture. `db.system.name` identifies the provider.

- **Coverage**: CRUD, string `Query`/`QueryStream`, the fluent-query terminals (`ToList`/`ToAsyncEnumerable`/`Count`/`Any`/`ExecuteDelete`/`ExecuteUpdate`/`Max`/`Min`/`Sum`/`Average`/`NearestVectors`), spatial/vector, all `ITemporalDocumentStore` ops, and `IDocumentSession.SaveChanges` (inner ops become child spans of the transaction span).
- **Not traced**: `NotifyOnChange`/`SubscribeChanges` (long-lived subscriptions, passed through); the fluent **builder** operators (no I/O); provider internals (raw SQL, RU, pool) — use the per-provider `Logging` option for raw SQL.
- **Zero-cost** when nothing is listening. **Privacy**: only metadata (op, type name, outcome, counts) — never document bodies, ids, or parameter values. Instrumentation also applies to keyed registrations.

### MongoDB-Specific Notes

The `Shiny.DocumentDb.MongoDb` provider implements `IDocumentStore` natively over `MongoDB.Driver`. Documents are stored as a typed BSON envelope (`_id`, `id`, `typeName`, `data`, `createdAt`, `updatedAt`) inside a collection that defaults to `"documents"`. Map types to dedicated collections with `cfg.ToCollection(...)` inside a `ConfigureDocument<T>` block.

- **Predicates evaluated in C#** — LINQ expressions are translated to a MongoDB filter at the type/sort/skip/take level; complex predicates are evaluated client-side after a typed find.
- **Raw SQL throws** — `Query<T>(string)` and `QueryStream<T>(string)` throw `NotSupportedException`. Use the LINQ-based `Query<T>()` overload.
- **`Upsert` deep-merges in C#** — null properties are stripped recursively (RFC 7396 semantics).
- **`IDocumentSession` uses a compensating model** — single-node MongoDB cannot use ACID multi-document transactions without a replica set. The provider tracks inserts and deletes them on failure (matches the CosmosDB provider).
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

## Concurrency and lifetime

A `DocumentStore` instance is safe to share across threads. Use a separate session per unit of work; a session buffers writes and is not a change tracker. SQLite, SQLCipher, and DuckDB operations on a store are serialized. Do not assume LiteDB or IndexedDB support concurrent multi-process or multi-tab writes.

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

Changes performed in a `IDocumentSession` are buffered and emitted *after* `SaveChanges` commits. A rollback discards the buffered events.

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
