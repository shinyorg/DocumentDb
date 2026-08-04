# Plan: Backing stores — `IDistributedCache` and Data Protection key ring

**Status:** Designed, not started.
**Target version:** `12.6` (two new packages; no core changes).
**Packages:** `Shiny.DocumentDb.Extensions.Caching` (works anywhere — server, MAUI, console) and
`Shiny.DocumentDb.AspNetCore.DataProtection`.

> Self-contained build spec. Read `CLAUDE.md` (repo root) for the four-artifact rule (code+tests, docs site,
> skill, readme) before considering any commit "done". Branch off `v12`.

---

## Goal

Let an app that already has a document store use it for the two things it would otherwise add infrastructure
for:

```csharp
builder.Services.AddDocumentDbDistributedCache(o => o.KeyPrefix = "app:");   // IDistributedCache
builder.Services.AddDataProtection().PersistKeysToDocumentDb();              // key ring survives restarts
```

Both are small, both are well-specified by the framework, and both are the kind of integration that pulls
people into a library — "I needed a distributed cache, I already had DocumentDb, done". Microsoft ships
`Microsoft.Extensions.Caching.SqlServer` and EF Core ships `PersistKeysToDbContext` for exactly these reasons;
the document-store equivalents are missing.

## Positioning — be honest in the docs

This is **not** a Redis replacement for hot-path caching. It is:

- a **shared, durable** cache for apps that already have a database and do not want a second dependency;
- the natural cache for **MAUI/Blazor WASM** (SQLite/IndexedDB) where there is no Redis at all;
- a way to keep the Data Protection key ring off the filesystem in a container, without Azure Blob/Key Vault.

If the app is already on Redis, use `Shiny.DocumentDb.Redis` for documents and Redis' own cache for caching.
Say that in the first paragraph of the docs page.

## Non-goals

- **No `IMemoryCache` implementation.** In-process caching over a database is nonsense.
- **No cache stampede protection / `HybridCache` L1.** `HybridCache` composes over `IDistributedCache` — we
  implement the L2 contract (including `IBufferDistributedCache` so it can avoid copies) and let the framework
  do the rest.
- **No tag-based invalidation.** Not in `IDistributedCache`. A `RemoveByPrefix` extension is offered as a
  non-standard convenience, clearly marked.
- **No key-ring encryption of our own.** Compose `ProtectKeysWith…` as normal (or the field-level encryption
  plan, once it lands).

---

## Package 1 — `Shiny.DocumentDb.Extensions.Caching`

### Surface

```csharp
public sealed class DocumentCacheOptions
{
    public string? StoreName { get; set; }                        // keyed store
    public string KeyPrefix { get; set; } = "";
    /// <summary>Skip the sliding-expiry write when less than this much of the window has elapsed.
    /// Trades exactness for write amplification. Default 20%.</summary>
    public double SlidingRefreshThreshold { get; set; } = 0.2;
    /// <summary>Background sweep of expired entries. Null disables (rely on native TTL / read-time filtering).</summary>
    public TimeSpan? SweepInterval { get; set; } = TimeSpan.FromMinutes(5);
    public int SweepBatchSize { get; set; } = 1000;
}

public static IServiceCollection AddDocumentDbDistributedCache(this IServiceCollection services, Action<DocumentCacheOptions>? configure = null);

/// <summary>Non-standard convenience: removes every entry whose key starts with the prefix.</summary>
public static Task<int> RemoveByPrefix(this IDistributedCache cache, string prefix, CancellationToken ct = default);
```

### Document

```csharp
public sealed class DocumentCacheEntry
{
    public string Id { get; set; } = null!;                 // prefix + key
    public required string Value { get; set; }              // base64 payload
    public DateTimeOffset? AbsoluteExpiration { get; set; }
    public double? SlidingSeconds { get; set; }
    public DateTimeOffset ExpiresAt { get; set; }           // computed effective expiry — the queried field
    public DateTimeOffset LastAccessed { get; set; }
}
```

`MapIndexedProperty<DocumentCacheEntry>(x => x.ExpiresAt)` at registration, so the sweep is an indexed
`ExecuteDelete` rather than a scan.

### Behavior

- **Get** — `store.Get<DocumentCacheEntry>(key)`; treat `ExpiresAt <= now` as a miss (and delete lazily).
  Expiry is checked in code, **not** with a global query filter: a query filter is a fixed expression evaluated
  at registration, so `e => e.ExpiresAt > DateTimeOffset.UtcNow` would freeze "now" at startup. This is a real
  trap; leave a comment at the call site.
- **Set** — compute `ExpiresAt` from absolute/relative/sliding options, then `Upsert`.
- **Refresh** — sliding entries push `ExpiresAt` forward, but only when more than `SlidingRefreshThreshold` of
  the window has elapsed. Without this, every cache *read* is a database *write*.
- **Remove** — `store.Remove<DocumentCacheEntry>(key)`.
- **Async/sync** — `IDistributedCache`'s sync members are implemented over the async path with the standard
  "do not block a request thread" caveat documented (and `GetAwaiter().GetResult()` only where the interface
  forces it).
- **`IBufferDistributedCache`** — implement `TryGet(string, IBufferWriter<byte>)` / `Set(string, ReadOnlySequence<byte>, …)`
  so `HybridCache` avoids array allocations.
- **Native TTL where the provider has it** — Cosmos item TTL, Mongo TTL index, DynamoDB TTL, Firestore, Redis.
  Where present, set it at write time and skip the sweeper; where absent (relational, LiteDB, DuckDB, IndexedDB),
  run the sweeper. One capability check at registration, documented as a tier table.
- **Large values** — base64 in JSON inflates by ~33% and lands in the document body. Cap it
  (`MaxValueBytes`, default 1 MB, throw above) and point at `IBlobDocumentStore` for anything bigger.

---

## Package 2 — `Shiny.DocumentDb.AspNetCore.DataProtection`

### Surface

```csharp
public static IDataProtectionBuilder PersistKeysToDocumentDb(this IDataProtectionBuilder builder, string? storeName = null);
```

### Implementation

`IXmlRepository` over a `DataProtectionKeyDocument { Id, FriendlyName, Xml, CreatedAt }`:

- `GetAllElements()` → `store.Query<DataProtectionKeyDocument>().ToList()` → `XElement.Parse`.
- `StoreElement(element, friendlyName)` → `Insert` (id = Guid v7). Key elements are immutable; never update.

That is the whole implementation — roughly 60 lines. The value is in the details:

- **Startup ordering.** The key ring is read during the first protect/unprotect. The store must be initialized
  by then; if the app uses `SkipTableInitialization`, the repository must initialize its own type or fail with a
  clear message rather than "table not found" at first login.
- **Multi-instance.** Two instances starting cold can both generate a key. That is tolerated by Data Protection
  (both keys are valid and both are published), but note it, and note that the default key lifetime/rotation
  semantics are unchanged.
- **Encryption at rest.** Keys are stored as plaintext XML unless the app composes `ProtectKeysWith…`. Say so
  explicitly on the docs page — a key ring in a table with no `ProtectKeysWith` is a finding in any security
  review.
- **Tenancy.** With a tenant-routed store (see [tenant-store-routing](./tenant-store-routing.md)), each tenant
  gets its own key ring — usually what you want, occasionally a surprise. Document it.

---

## Tests

`tests/Shiny.DocumentDb.Extensions.Caching.Tests`:

- Conformance across the `IDistributedCache` contract: set/get/remove, absolute expiry, relative-to-now,
  sliding refresh, get-after-expiry is a miss, remove-missing is a no-op.
- Sliding refresh threshold: N reads inside the threshold produce **zero** writes (assert with a counting
  interceptor — the shipped interceptor surface makes this trivial).
- Sweeper deletes expired and only expired entries, in batches.
- `IBufferDistributedCache` path round trips without materializing arrays.
- `HybridCache` over it: L2 hit after L1 eviction.
- `RemoveByPrefix` removes exactly the matching set.
- Value size cap throws with a message pointing at blobs.
- Provider matrix: SQLite + PostgreSQL + Mongo + Cosmos (native TTL asserted where supported).
- `FakeTimeProvider` drives every expiry test — no `Task.Delay`.

`tests/Shiny.DocumentDb.AspNetCore.DataProtection.Tests`:

- Round trip: protect in one `WebApplicationFactory` instance, unprotect in a second sharing the store.
- Restart survival: new key ring reads the persisted elements.
- Two cold instances both start successfully.
- Composed `ProtectKeysWith` (certificate) round trips.

## Four-artifact checklist

- **Code + tests** — as above; both projects into `DocumentDb.slnx` and `build.slnf`.
- **Docs** — new `caching.mdx` (with the "not a Redis replacement" opener, the provider TTL tier table, and the
  sliding-write-amplification note) and `data-protection.mdx` (with the `ProtectKeysWith` warning). Release
  notes: `feature` ×2.
- **Skill** — short sections for both; `triggers:` += IDistributedCache, HybridCache, output cache, data
  protection, key ring.
- **readme.md** — feature bullets + badges.

## Risks

- **Cache write amplification** is the one thing that makes a database-backed cache look bad. The sliding
  threshold, the value cap, and the indexed expiry column are all load-bearing — none of them is optional
  polish.
- **Sync-over-async** on `IDistributedCache`'s synchronous members can deadlock in some hosts. Prefer the async
  members everywhere in our own code and document the caveat rather than pretending it away.
- **Scope creep into output caching / session state.** Both are plausible next steps (`IOutputCacheStore`,
  `ITicketStore`) but each has its own semantics. Ship these two, see if anyone asks.
