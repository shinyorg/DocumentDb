# Plan: Tenant store routing, hardened (`AddMultiTenantDocumentStore`)

**Status:** Designed, not started. **This hardens shipped code — it does not introduce the concept.**
**Target version:** `12.8` (mostly additive; one behavior change on store lifetime, see [Breaking](#breaking-surface)).
**Package:** core (`Shiny.DocumentDb`, `DependencyInjection/`).

> Self-contained build spec. Read `CLAUDE.md` (repo root) for the four-artifact rule (code+tests, docs site,
> skill, readme) before considering any commit "done". Branch off `v12`.

---

## What already exists (read this first)

The repo ships **two** tenancy models and both work:

1. **Shared-table / row-level** — `AddDocumentStore(configure, multiTenant: true)` sets
   `DocumentStoreOptions.TenantIdAccessor`, which adds a `TenantId` column and filters every read and write.
   Wired into the SQL layer directly (`DocumentStore.cs`, `IQueryExecutor.TenantFilter`), with an Aspire
   convenience that resolves it from `ITenantResolver`.
2. **Tenant-per-database** — `AddMultiTenantDocumentStore(Func<string, DocumentStoreOptions> optionsFactory)`
   registers `IDocumentStore` as **scoped**, resolving through `MultiTenantDocumentStoreFactory`, which caches
   one `DocumentStore` per tenant id in a `ConcurrentDictionary`.

So physical isolation is not missing. What is missing is everything that makes (2) survive a real deployment.
`MultiTenantDocumentStoreFactory` is 18 lines:

```csharp
public IDocumentStore GetStore(string tenantId)
    => stores.GetOrAdd(tenantId, id => new DocumentStore(optionsFactory(id), services));
```

## Goal

Make tenant-per-database production-grade: bounded, provider-agnostic, initialized, observable, and
lifecycle-managed — without changing the one-line registration that already works.

## The concrete gaps

| # | Gap | Consequence today |
|---|---|---|
| G1 | **Unbounded store cache.** No cap, no idle eviction, no disposal until the root container dies. | A 5,000-tenant SaaS holds 5,000 live stores and 5,000 connection pools. Memory and pool exhaustion, not a leak you can tune. |
| G2 | **`GetOrAdd` runs its factory concurrently.** Two threads racing a cold tenant both build a `DocumentStore`; the loser is discarded **undisposed**. | Orphaned store/connection handles under load — exactly when it hurts. |
| G3 | **Relational-only.** The factory hard-codes `new DocumentStore(...)`, the core relational store. | Cannot route tenants across Mongo / Cosmos / LiteDB / any non-relational provider, even though they all implement `IDocumentStore`. |
| G4 | **No per-tenant initialization.** Seeding and (planned) migrations run from a hosted service against the registered store at startup; a tenant whose store is created on first request at 3pm never runs them. | New tenants start with no reference data. Silent. |
| G5 | **No session/context integration.** `AddDocumentStore` registers a scoped `IDocumentSession` and an `IDocumentSessionFactory`; `AddMultiTenantDocumentStore` registers neither. | `IDocumentSession`, `DocumentContext` and unit-of-work do not work in a tenant-routed app. |
| G6 | **No telemetry identity.** `StoreName` is never set, so every tenant reports `db.namespace` unset — all tenants collapse into one metric stream. | Cannot see which tenant is slow. |
| G7 | **No lifecycle API.** No way to list, warm, evict, or drop a tenant's store. | Offboarding a tenant means restarting the process. |

## Non-goals

- **No tenant *provisioning*.** Creating the database/schema itself stays the caller's job (or their Aspire /
  IaC pipeline). We create tables inside a database that exists, which is what store initialization already does.
- **No cross-tenant queries.** Aggregating across tenants means iterating tenants yourself.
- **No change to the shared-table model.** `TenantIdAccessor` stays exactly as-is; the two models remain
  independent and composable (a tenant-routed store may itself be shared-table for sub-tenants).
- **No tenant discovery.** `ITenantResolver` remains the single source of "who am I".

## Design decisions (locked)

| Decision | Choice | Consequence |
|---|---|---|
| Factory signature | Add a `Func<string, IDocumentStore>` overload; keep the `Func<string, DocumentStoreOptions>` one as the relational convenience | G3 fixed without breaking a single existing call site. |
| Cache policy | Size cap + idle timeout, both configurable; LRU eviction with **deferred disposal** | G1. Evicting a store in use must not break an in-flight request. |
| Eviction safety | Ref-counted handle: a scoped resolution takes a lease; eviction disposes when the last lease returns | Correctness over simplicity — this is the one place a naive cache silently corrupts requests. |
| Race fix | `Lazy<IDocumentStore>` values (or `GetOrAdd` + dispose-the-loser) | G2. |
| Initialization | Per-tenant `DocumentInitializationHostedService` equivalent, run **once per tenant on first resolution**, before the store is handed out | G4, and it is the same hook data migrations will need when that plan lands. |
| Telemetry | `StoreName` defaults to the tenant id (overridable) | G6 — `db.namespace` becomes the tenant. Note the cardinality warning in docs. |

---

## Public API surface

```csharp
// src/Shiny.DocumentDb/DependencyInjection/TenantStoreOptions.cs
public sealed class TenantStoreOptions
{
    /// <summary>Maximum tenant stores held open. Least-recently-used beyond this are evicted. Default: 100.</summary>
    public int MaxCachedStores { get; set; } = 100;

    /// <summary>Evict a tenant store after this long without a resolution. Default: 20 minutes. Null disables.</summary>
    public TimeSpan? IdleTimeout { get; set; } = TimeSpan.FromMinutes(20);

    /// <summary>Runs once per tenant, before the store is first handed out — seeding, migrations, warmup.</summary>
    public Func<string, IDocumentStore, CancellationToken, Task>? OnTenantStoreCreated { get; set; }

    /// <summary>Tags this tenant's spans/metrics with <c>db.namespace</c>. Defaults to the tenant id.</summary>
    public Func<string, string>? StoreNameFactory { get; set; }
}

// ServiceCollectionExtensions
/// <summary>Tenant-per-database routing for ANY provider. IDocumentStore resolves scoped to the current
/// tenant (via ITenantResolver); IDocumentSession and IDocumentSessionFactory are wired to match.</summary>
public static IServiceCollection AddMultiTenantDocumentStore(
    this IServiceCollection services,
    Func<string, IDocumentStore> storeFactory,
    Action<TenantStoreOptions>? configure = null);

/// <summary>Relational convenience — unchanged signature, now honoring TenantStoreOptions.</summary>
public static IServiceCollection AddMultiTenantDocumentStore(
    this IServiceCollection services,
    Func<string, DocumentStoreOptions> optionsFactory,
    Action<TenantStoreOptions>? configure = null);

// src/Shiny.DocumentDb/DependencyInjection/ITenantStoreManager.cs
/// <summary>Operational control over the tenant store cache. Resolve from DI.</summary>
public interface ITenantStoreManager
{
    IReadOnlyCollection<string> ActiveTenants { get; }

    /// <summary>Builds and initializes a tenant's store ahead of its first request.</summary>
    Task WarmAsync(string tenantId, CancellationToken ct = default);

    /// <summary>Closes a tenant's store, waiting for in-flight leases. Idempotent.</summary>
    Task EvictAsync(string tenantId, CancellationToken ct = default);
}
```

### Registration wiring (mirrors `AddDocumentStore`)

```csharp
services.AddSingleton<TenantDocumentStoreCache>(...);          // replaces MultiTenantDocumentStoreFactory
services.AddSingleton<ITenantStoreManager>(sp => sp.GetRequiredService<TenantDocumentStoreCache>());
services.AddScoped<IDocumentStore>(sp =>
    sp.GetRequiredService<TenantDocumentStoreCache>()
      .Lease(sp.GetRequiredService<ITenantResolver>().GetCurrentTenant(), sp));   // lease released with the scope
services.AddScoped<IDocumentSession>(sp =>
    new DocumentSession(sp.GetRequiredService<IDocumentStore>(), sp, ownedScope: null));
services.TryAddSingleton<IDocumentSessionFactory>(sp => new DocumentSessionFactory(sp, sp.GetRequiredService<IServiceScopeFactory>()));
```

The lease is released when the DI scope is disposed — register the handle as the scope-owned disposable so
eviction can never pull a store out from under a running request.

---

## Implementation notes

**`TenantDocumentStoreCache`** (internal, replaces `MultiTenantDocumentStoreFactory` — deleted outright, no
forwarding shim, per `CLAUDE.md`):

- `ConcurrentDictionary<string, Lazy<Task<TenantStoreEntry>>>` — `Lazy` fixes G2, `Task` because initialization
  (G4) is async and must complete before the first hand-out.
- `TenantStoreEntry` holds the store, an `int` lease count, and `LastUsedUtc` (stamped from the ambient
  `TimeProvider`, so tests drive eviction on a controlled clock — the pattern soft delete already uses).
- Eviction: on insert, if `Count > MaxCachedStores`, evict the LRU idle entry; a periodic sweep (a `PeriodicTimer`
  in a hosted service, only registered when `IdleTimeout != null`) evicts by age.
- Disposal: mark evicted → refuse new leases → dispose when the lease count hits zero.
- `OnTenantStoreCreated` runs inside the `Lazy<Task<…>>`, so exactly one caller runs it and everyone else awaits.

**Initialization (G4):** the natural implementation of `OnTenantStoreCreated` is "run the same seeders the
startup hosted service runs, for this store". Expose that as a ready-made helper —
`TenantStoreOptions.SeedFromRegisteredSeeders()` — so the common case is one line, and so the
[data-migrations plan](./data-migrations.md) can slot migrations into the same hook when it lands.

---

## Breaking surface

Tenant stores can now be **disposed while the process lives** (idle eviction). Code that captured
`IDocumentStore` in a singleton and used it after eviction gets `ObjectDisposedException` instead of silently
working. That is the correct behavior for a bounded cache and matches the scoped registration's contract, but it
is a behavior change worth a `type="breaking"` release note. Opt out with `IdleTimeout = null` and
`MaxCachedStores = int.MaxValue`.

## Tests (`tests/Shiny.DocumentDb.Tests/TenantRoutingTests.cs`)

SQLite-backed (one file per tenant) for speed, plus one non-relational fixture to prove G3:

- Two tenants get two stores; documents written under tenant A are invisible to tenant B (physical isolation).
- Cold-start race: 50 concurrent resolutions of an unseen tenant build **one** store (assert via a counting
  factory) and dispose nothing.
- `MaxCachedStores = 2`: resolving a third tenant evicts the LRU and disposes it exactly once.
- Eviction with an in-flight lease waits: the store stays usable until the scope disposes, then is disposed.
- `IdleTimeout` with a `FakeTimeProvider`: advancing past the timeout evicts.
- `OnTenantStoreCreated` runs exactly once per tenant, before the first document operation, and a throwing hook
  fails the resolution (and does not cache a broken entry).
- `IDocumentSession` / `DocumentContext` resolve against the right tenant inside a scope.
- Telemetry: spans for tenant A carry `db.namespace = "A"`.
- Non-relational: a Mongo (or LiteDB) `Func<string, IDocumentStore>` factory routes correctly.
- `ITenantStoreManager.WarmAsync` / `EvictAsync` / `ActiveTenants`.

## Four-artifact checklist

- **Code + tests** — as above; delete `MultiTenantDocumentStoreFactory`.
- **Docs** — the tenancy content is currently a paragraph inside `query-filters.mdx`. Promote it to a
  `multi-tenancy.mdx` page covering both models side by side (shared-table vs tenant-per-database: isolation,
  backup granularity, noisy-neighbor, cost), the cache knobs, per-tenant seeding, and the eviction contract.
  Release notes: `enhancement` + `breaking`.
- **Skill** — the skill teaches `TenantIdAccessor`; add the tenant-per-database model, when to pick which, and
  the `TenantStoreOptions` knobs. `triggers:` += multi-tenant/tenant-per-database.
- **readme.md** — extend the tenancy bullet to name both models.

## Risks

- **Lease accounting is the whole ballgame.** Get it wrong and a request uses a disposed store under load — the
  worst kind of intermittent bug. Keep the lease handle dead simple, cover it with the concurrency tests above,
  and prefer "leak a store until the process ends" over "dispose one that is in use" if a case is ambiguous.
- **`db.namespace = tenantId` is unbounded cardinality** on a large SaaS. Ship it as the default because it is
  what people want at 20 tenants, and document `StoreNameFactory` (e.g. bucket, or constant) for 5,000.
