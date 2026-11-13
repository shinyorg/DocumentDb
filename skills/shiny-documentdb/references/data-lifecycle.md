# Seeding, blobs, history, soft delete, encryption, and validation

Read only the sections relevant to the current task. These are application-facing API details.

## Contents

- Seeding initial data
- Blobs (MapBlob / MapBlobCollection)
- Temporal History (System-Time Versioning)
- Soft Delete (`AddSoftDelete<T>`)
- Field-Level Encryption (core, 13.0+)
- JSON Schema Validation (Shiny.DocumentDb.JsonSchema)

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

### Geofencing & reference geo data (moved)

`Shiny.DocumentDb.Geofencing` and `Shiny.DocumentDb.Geo` are no longer part of this library. GPS geofencing over
spatially-mapped documents (and the US state / Canadian province / city dataset) now ships from the Shiny client repo as
`Shiny.Geofencing.Regions` + `Shiny.Geofencing.Regions.DocumentDb` (SQLite) - `AddRegionGeofencing<TDelegate>(cfg =>
cfg.AddDocumentRegionSet<T>(...))`. Use the **shiny-locations** skill for it.

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

Opt-in append-only versioning per type. Enable with `cfg.MapTemporal(...)` in the type's `ConfigureDocument` block; every `Insert`/`Update`/`Upsert`/`Remove`/`SetProperty`/`RemoveProperty`/`BatchInsert` (including writes inside a `IDocumentSession`) records a versioned snapshot to a per-type history sidecar. Only mapped types incur the extra write.

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

Temporal history is implemented on every provider.

The history-query methods live on the **`ITemporalDocumentStore`** capability interface (`ITemporalDocumentStore : IDocumentStore`), **not** the base `IDocumentStore` — the same pattern as `IObservableDocumentStore` / `IChangeFeedDocumentStore`, and the `Backup`/`ClearAllAsync` precedent. Resolve or cast to `ITemporalDocumentStore` (every store, relational and NoSQL, implements it). A history call for a type not passed to `MapTemporal<T>` throws `InvalidOperationException`.

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
