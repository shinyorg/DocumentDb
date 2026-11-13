# CRUD, sessions, transactions, concurrency, backup, and write hooks

Read only the sections relevant to the current task. These are application-facing API details.

## Contents

- Optimistic Concurrency (Row Versioning)
- Core API Reference (IDocumentStore)
- Transactions (IDocumentSession)
- Write Interceptors
- Pessimistic locking (`LockMode`)
- Transactional Outbox (`AddOutbox` + `AddDocumentOutbox`, 13.0+)

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

To group several writes into one transaction, open an `IDocumentSession` with `store.OpenSession()`, queue
`Add`/`AddRange`/`Update`/`Upsert`/`Remove`, then call `SaveChanges`. All commit or all roll back.
Contiguous same-type runs of inserts, upserts, updates, and removes are each coalesced into the
matching batch method. Use `BeginTransaction` or the session `RunInTransaction` extension for an explicit transaction.

```csharp
await using var uow = store.OpenSession();   // IDocumentSession is the unit of work
uow.Add(order)
   .AddRange(orderLines)   // coalesced into one batch insert
   .Update(customer)
   .Remove<Cart>(cartId);
await uow.SaveChanges();    // one transaction; rolls back entirely on failure
```

Use scoped `IDocumentSession` in ASP.NET Core or open a session per unit of work via `IDocumentSessionFactory` in other hosts. A session is a write buffer, not a
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

**Merge writes and sidecars (blobs, spatial, vector).** A merge (`Upsert`, `Update(patch: true)`) strips nulls, so
a property the patch omits keeps its stored value — and that applies to the out-of-body sidecars too. A patch that
doesn't carry the mapped blob, geometry or embedding leaves the stored one and its sidecar row alone, so a partial
patch never has to re-send a payload or a location, and never drops the document out of spatial/vector search.

This includes the **embedding**, which needs care: a mapped vector is a `ReadOnlyMemory<float>`, a non-nullable struct
that serializes as `[]` rather than `null` when unset. The merge drops that empty array alongside nulls, so a partial
patch keeps the stored vector — don't re-send an embedding just to change a neighbouring field.

The corollary: **a merge can't clear one.** Setting the property to `null` (or an embedding to `default`) and calling
`Upsert` reads as "not in this patch" and changes nothing. To actually remove a blob, a location or an embedding, use a
write that carries the whole document:

```csharp
user.Avatar = null;
await store.Update(user);                          // or: store.Upsert(user, patchIfUpdate: false)
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
- **String grammar only.** There is no typed LINQ on a collection — never generate `store.Collection("x").Query().Where(o => …)`. Use `Query<T>()` when you have a CLR type. If you want the *typed* builder but a raw JSON **result**, that is the [JSON terminals](queries.md#json-terminals--results-as-raw-json-instead-of-t) on `Query<T>()`, not a collection.
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

`SetProperty` and `ExecuteUpdate` values are scalars: string, char, numbers, bool, enums, the date/time types, Guid, or null. Each is written exactly as an insert would have serialized it under the store's options. An enum is written as its number, or as its member name with a `JsonStringEnumConverter`. `NaN`/`Infinity` throw unless `AllowNamedFloatingPointLiterals` is on. This path is AOT-safe. To replace a nested object or collection, use `Update` or `Upsert`.

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

**Explicit transactions + consistent reads (11.0, relational):** `await using var tx = await session.BeginTransaction();` (one active at a time) for locking reads (`session.Get(id, LockMode.Update)`) and grouping multiple `ExecuteUpdate`/`ExecuteDelete`; `SaveChanges` joins the active tx without committing it; explicitly call `tx.Commit()` after flushing the queued writes. Pass an isolation level for a **consistent-read session** — `await session.BeginTransaction(IsolationLevel.Snapshot)` — so every read sees one snapshot. **Telemetry:** a session emits a `<system>.unit_of_work` parent span (tag `db.session.id`) so its ops nest into one correlated trace, and `SaveChanges` records the `db.client.unit_of_work.operations` histogram (buffered writes per commit). Zero-cost when unobserved.

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

## Transactions (IDocumentSession)

Grouping writes into one transaction is done through an `IDocumentSession` opened from the store — use `BeginTransaction` or the session `RunInTransaction` extension for explicit transactions. Queue `Add`/`AddRange`/`Update`/`Upsert`/`Remove`, then `SaveChanges`.

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


## Pessimistic locking (`LockMode`)

`session.Get(id, LockMode.Update)` inside an explicit transaction takes a real row lock (`FOR UPDATE`, or `WITH (UPDLOCK, HOLDLOCK)` on SQL Server). Requires an active transaction — it throws otherwise, because a lock with nothing to hold it is meaningless.

```csharp
await using var session = store.OpenSession();
await using var tx = await session.BeginTransaction();
var counter = await session.Get<Counter>("c1", LockMode.Update);   // blocks other lockers until commit
counter!.Value++;
session.Update(counter);
await session.SaveChanges();
await tx.Commit();
```

- Check `store.SupportsPessimisticLocking` before relying on it. **True**: PostgreSQL, SQL Server, MySQL, MariaDB, Oracle, CockroachDB. **False**: SQLite, DuckDB (the transaction boundary locks the whole database instead — correct, but not row-level), and the non-relational stores (which throw for anything but `LockMode.None`).
- `LockMode.Share` throws on Oracle (no shared row lock). MariaDB emits `LOCK IN SHARE MODE`, not MySQL 8's `FOR SHARE`.
- Use it for counters, sequences, leases and job claims — anything reserving a value from a shared row. For ordinary write conflicts prefer optimistic concurrency (`cfg.MapVersionProperty` + `ConcurrencyException`).

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
