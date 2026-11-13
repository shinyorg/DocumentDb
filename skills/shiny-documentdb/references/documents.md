# Document models, mapping, IDs, and contexts

Read only the sections relevant to the current task. These are application-facing API details.

## Contents

- Per-Type Configuration (`ConfigureDocument<T>`)
- Strongly-Typed Context (DocumentContext)
- Computed Properties (MapComputedProperty)
- Unique Indexes (MapUniqueIndex)
- Document Types

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
    cfg.MapComputedProperty<decimal>("Total", o => o.Quantity * o.UnitPrice, setter: (o, v) => o.Total = v);
    cfg.MapBlob(i => i.Signature, o => o.ComputeHash = true);
    cfg.MapTemporal(o => o.Retention = TimeSpan.FromDays(90));
    cfg.OnBeforeWrite((ctx, ct) => Task.CompletedTask);
});
```

### The builder surface

| Member | Description |
|---|---|
| `cfg.Table` | The type's **own** storage unit — table / collection / container / object store. Exclusive: a second type on the same name throws. Unset ⇒ shares the store default (`TableName`) |
| `cfg.TypeName` | The resolved type name (per `TypeNameResolution`). Assign to `cfg.Table` to name it after the type |
| `cfg.MapIdProperty(x => x.Key)` / `("Key")` | Custom Id property; the string form is the AOT-safe one |
| `cfg.MapVersionProperty(x => x.RowVersion)` | Optimistic concurrency (+ AOT accessor overload) |
| `cfg.AddQueryFilter(pred)` / `("name", pred)` | Global query filter, optionally named |
| `cfg.MapProperty(x => x.Ssn, p => p.Encrypt(mode))` | Per-property options — field-level encryption |
| `cfg.MapSpatialProperty(...)` | `GeoPoint?` or `Geometry?` — **one per type**, and it is the *only* geometry that spatial queries can name |
| `cfg.MapVectorProperty(x => x.Embedding, dimensions: n, …)` | ANN embedding — **one per type**. Leave `indexKind` unset for the provider default |
| `cfg.MapFullTextProperty(x => x.Body)` / `([a, b])` | Full-text index — **one per type** (several fields, one index) |
| `cfg.MapComputedProperty<TValue>(...)` | Derived value — **one generic argument**, not two |
| `cfg.MapUniqueIndex(x => x.Email, filter:, name:)` | Unique index over JSON values — `x => new { x.A, x.B }` for composite; also `cfg.MapProperty(x => x.Email, p => p.Unique())` |
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
  Setting `cfg.Table` twice on the **same** type takes the last value.
- **A custom table is exclusive to one document type.** `cfg.Table` means "give this type a place of its
  own", so pointing a second type at the same name throws `ArgumentException`. To put several types in the
  **same** table — which is normal, and how the Orleans system stores work — leave `cfg.Table` unset on all
  of them and set `DocumentStoreOptions.TableName`; every type lands there, discriminated by `TypeName`.
  Never try to co-locate types by giving them a matching `cfg.Table`.
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

// WRONG — a custom table is exclusive to one type; this throws on the second call:
// options.ConfigureDocument<Invoice>(cfg => cfg.Table = "billing");
// options.ConfigureDocument<Payment>(cfg => cfg.Table = "billing");
// To co-locate them, set neither and let TableName ("docs" above) hold both.

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
  work (`context.Add(x)` + `await context.SaveChanges()`, or `context.BeginTransaction()`); while a transaction
  is open the typed sets (reads, writes, `Clear`, `Batch*`) run **inside** it. Reach the raw session via
  `context.Session`; `context.Store` calls do NOT join the transaction (and on SQLite/DuckDB wait on its connection). **No** change tracking, identity map, or navigation/`Include` — for two types at once use an explicit `Query<T>().Join<TRight>(…)` (relational + MongoDB).
- Works over **any** provider (only needs `IDocumentStore`). The generated `ConfigureModel`/`Add<Context>`
  target the relational `DocumentStoreOptions`; for LiteDB/MongoDB/Cosmos build that store yourself and pass
  it: `new AppContext(liteDbStore)`.

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
    cfg.MapComputedProperty<decimal>(o => o.Total, o => o.Quantity * o.UnitPrice);
    cfg.MapComputedProperty<string>(o => o.FullName, o => o.First + " " + o.Last);
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

## Unique Indexes (MapUniqueIndex)

Declare uniqueness on the type; the store enforces it on every provider **except DuckDB**, on every write path (Insert, Update,
Upsert, SetProperty, `ExecuteUpdate`, batches, sessions, backup import). A violating write writes nothing and throws
`UniqueConstraintException`.

```csharp
options.ConfigureDocument<Customer>(cfg =>
{
    cfg.MapUniqueIndex(x => x.Email);                               // one property
    cfg.MapUniqueIndex(x => new { x.Region, x.AccountNumber });     // composite: an anonymous type
    cfg.MapUniqueIndex(x => x.Address.PostalCode);                  // nested property
    cfg.MapProperty(x => x.Sku, p => p.Unique());                   // shorthand for a single property
});

// Unique among live documents only — the usual pairing with soft delete
options.ConfigureDocument<User>(cfg =>
{
    cfg.AddSoftDelete(x => x.IsDeleted);
    cfg.MapUniqueIndex(x => x.Email, filter: x => !x.IsDeleted);
});

try
{
    await store.Insert(customer);
}
catch (UniqueConstraintException ex)
{
    // ex.TypeName, ex.DocumentType, ex.IndexName, ex.PropertyNames, ex.DocumentId (null for set-based writes)
    // The duplicated value is deliberately NOT in the message (keys are often personal data).
}
```

Rules to generate correct code:
- **Catch the exception; don't pre-check with a query.** "Query, then insert" races; the index is the guarantee.
  `UniqueConstraintException` derives from `InvalidOperationException`.
- **Key parts are plain property chains** (`x => x.Email`, `x => x.Address.City`, or `x => new { x.A, x.B }`). A method
  call (`x => x.Email.ToLower()`) or a repeated property throws `ArgumentException`.
- **Values compare exactly (case-sensitive) on every provider.** For case-insensitive uniqueness, normalize the value
  (e.g. lower-case the email) before writing it.
- **Scoped to the document type** — other types in the same table may hold the same value — and, with shared-table
  multi-tenancy (`TenantIdAccessor`), to the tenant.
- A document whose key has a **null or missing** part is not constrained, nor is one the **filter** rejects. The filter
  is re-evaluated on every write: updating a document back into the filter can collide.
- Index name is `uq_{Type}_{Props}` (or `uq_{Type}_{name}` with `name:`), capped at 51 characters (a relational index is created as that name plus a short table hash).
- A **randomized**-encrypted property can't be a key part or be read by the filter (`DocumentConfigurationException` at
  build); `EncryptionMode.Deterministic` works.
- `CreateIndexAsync` is a **non-unique** performance index — never use it for uniqueness.

Provider notes:
- **Relational providers and MongoDB / Amazon DocumentDB** enforce with a native unique index created when the
  table/collection is initialized. Documents already violating it make that fail with `DocumentConfigurationException`
  on first use. The index is created once and never altered: after changing a key or filter, drop the old index (or
  pass a new `name:`).
- **DuckDB** does not support unique indexes (it can't index a JSON expression) — mapping one throws
  `DocumentConfigurationException` when the store is built.
- **MySQL / MariaDB**: upserts and backup `Replace`/`SkipExisting` of a unique-indexed type use read-then-write, because
  `ON DUPLICATE KEY` / `INSERT IGNORE` would treat another document's unique value as the conflict.
- **Cosmos DB, LiteDB, IndexedDB, DynamoDB, Azure Table, Firestore, Redis, RavenDB** keep their own index entries in step
  with each write. Documents stored **before** the mapping was added are not back-filled.
- **MongoDB / Amazon DocumentDB** filters must be expressible as a partial-index filter (`&&`, `||`, `!boolProp`,
  `==`, `<`/`<=`/`>`/`>=`, `== null`, constant-list `Contains`); anything else fails at index creation.
- **SQL Server / Oracle** don't constrain string values longer than 4000 characters; **PostgreSQL** rejects index
  entries over ~2.7 KB.

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

### Document metadata (`DocumentMetadata`) — created/updated timestamps + tenant

To expose when a document was created / last changed, declare ONE settable `DocumentMetadata` property. Do **not**
add your own `CreatedAt`/`UpdatedAt` properties and stamp them in an interceptor — the store already keeps both
timestamps and fills this in:

```csharp
public class Order
{
    public string Id { get; set; } = "";
    public DocumentMetadata? Metadata { get; set; }   // found by type — no mapping call
}

await store.Insert(order);                // order.Metadata.CreatedAt / UpdatedAt stamped, no re-read
var o = await store.Get<Order>("o1");     // o.Metadata never null on a stored document
var stale = await store.Query<Order>().Where(x => x.Metadata!.UpdatedAt < cutoff).OrderBy(x => x.Metadata!.CreatedAt).ToList();
store.Query<Order>().Where($"Metadata.CreatedAt > {since}");   // string grammar resolves the same path
```

The property can have any name (found by type); query paths use that name (`Audit.CreatedAt`). Rules: the property must be assignable (`set`, `init`, or `[JsonInclude]` non-public setter) — get-only fails
validation; at most one per type; members (`CreatedAt`, `UpdatedAt`, `TenantId`, `IsPersisted`) are read-only to app code.
`TenantId` is the owning tenant under shared-table multi-tenancy (`TenantIdAccessor`, relational providers) and `null`
otherwise — don't add your own tenant property to read it back; it can't be queried (queries are already tenant-scoped).
It is never written into the body (the envelope is the only copy), queries on it run against the envelope, and
`IsPersisted` can't be queried. `Update` leaves `CreatedAt` as your instance had it; an `Upsert` sets `CreatedAt` only
where the provider knows the insert branch ran. Temporal snapshots (`History`/`AsOf`) new it up unstamped
(`IsPersisted == false`). Soft delete and the concurrency version are separate features (`AddSoftDelete`,
`MapVersionProperty`) — don't look for them on `DocumentMetadata`. All providers.
