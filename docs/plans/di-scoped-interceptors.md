# Plan: Scoped DI for interceptors via a first-class DocumentContext

**Status:** Designed, not started.
**Target version:** `10.x` (new feature → minor bump off the `10.0.x` line in `version.json`). **Additive**
— the container-free `new SqliteDocumentStore(conn)` path, existing interceptor registration, and the current
`IDocumentStore` surface all keep working unchanged. Ships across **all providers** (relational + non-relational),
because DI-interceptor parity is part of the deliverable.

> Self-contained build spec. Read `CLAUDE.md` (repo root) for the four-artifact rule (code+tests, docs site,
> skill, readme) before considering any commit "done". Branch off `v10`.

---

## Not an audit interceptor — audit already exists (temporal)

Do **not** ship a `CreatedBy`/`UpdatedBy` audit interceptor. Temporal already owns audit and does it better:
`TemporalOptions.CaptureActor` (`Temporal.cs:42`) stamps *who* onto every history row, indexed
`(TypeName, Actor)`, surfaced via `DocumentVersion.Actor` and `ChangesByActor<T>`
(`DocumentStore.cs:2567-2581`) — a full versioned who-changed-what-when trail, not just last-touched fields.

The real gap is that `CaptureActor` is a `Func<string?>` invoked on the **singleton** store with **no scope**
(`DocumentStore.cs:1072` `mapping.CaptureActor?.Invoke()`), so it cannot cleanly resolve a request-scoped
current-user service — you're forced to close over `IHttpContextAccessor` or a static. This feature's
headline consumer is therefore a **scope-aware `CaptureActor`**, fed by the ambient scope the
`DocumentContext` carries (§7). Temporal is implemented on **every** provider (relational via
`BuildHistory*Sql`; native `*.Temporal.cs` for Cosmos/Mongo/LiteDB/IndexedDb), so this delivers
cross-provider audit-with-actor end to end.

## Goal

Let write-time hooks resolve **scoped** services during a write. The concrete motivating consumers are
**scope-aware `TemporalOptions.CaptureActor`** (pull the acting user from a request-scoped current-user
service — see below), **tenant enforcement**, and **validation needing a scoped repo** — plus general
`IDocumentInterceptor` / `IDocumentBulkInterceptor` scope access. All while:

1. flowing the **caller's** scope (same scoped instances the request already has), not a fresh child scope;
2. keeping the zero-interceptor and options-only-lambda hot paths **allocation-free**;
3. making **`DocumentContext` the documented primary entry point** and the scope carrier, without turning it
   into a stateful EF-style change-tracker;
4. working on **every provider**, closing a latent gap where DI interceptors silently never fire on
   Mongo/Cosmos/LiteDB/IndexedDB/DynamoDB/AzureTable today.

Decisions below are locked (confirmed with the maintainer).

## Why the current design blocks this

- The store is a **singleton**. `AddDocumentStore` → `new DocumentStore(options, sp)`
  (`ServiceCollectionExtensions.cs:8-25`); per-context stores are keyed singletons
  (`DocumentContextServiceCollectionExtensions.cs:61-74`). A singleton cannot capture a request scope.
- The `IServiceProvider` handed to the store constructor is **used once and dropped** — `DocumentStore.cs:105-109`
  calls `options.Interceptors.AttachServiceProvider(sp)` (`Interceptors.cs:172-191`), which enumerates
  `IEnumerable<IDocumentInterceptor>` / `IEnumerable<IDocumentBulkInterceptor>` **once** and caches them as
  effectively-singletons. The SP is never stored in a field.
- The interceptor context objects (`DocumentWriteContext`, `DocumentBulkContext` — `Interceptors.cs:41-132`)
  carry **no** `IServiceProvider`/scope.
- The **only already-scoped thing** in the system is `DocumentContext`
  (`DocumentContextServiceCollectionExtensions.cs:20-32` = `AddScoped`; its own doc comment says "register it
  scoped, like a DbContext"). So the context is the natural place for the ambient scope to enter.
- **Provider gap:** non-relational stores have no `IServiceProvider` constructor at all
  (`MongoDbDocumentStore.cs:42`), so `AttachServiceProvider` is never called for them → DI-registered
  interceptors silently don't run there today.

## Decisions (locked)

- **Flow the ambient scope through `DocumentContext`; fresh child scope only as fallback.** Interceptors see
  the *same* scoped instances the caller has. A fresh child scope is opened only when there is no ambient scope
  (Orleans grain, MAUI, background worker, raw singleton-store use) **and** an interceptor opts into needing one.
- **`DocumentContext` becomes the primary, documented app-facing API and the scope carrier — but stays
  stateless.** No identity map, no `SaveChanges()`. Raw `IDocumentStore` remains the low-level / no-DI path
  (tests, scripts, MAUI without a context). `DocumentSet<T>` stays a thin forwarder.
- **Core takes a dependency on `Microsoft.Extensions.DependencyInjection.Abstractions` only** (tiny, AOT-safe:
  `IServiceScopeFactory`, `IKeyedServiceProvider`; `IServiceProvider` is already in `System`). Registration
  extension methods stay in `Shiny.DocumentDb.Extensions.DependencyInjection`.
- **Scope is threaded via the existing `AsyncLocal` `DocumentOperationScope`** (`Interceptors.cs:310-362`),
  extended to carry an `IServiceProvider`. **No `IDocumentStore` method signatures change.**
- **Opt-in marker so the hot path stays free.** Only interceptors implementing a new
  `IScopedDocumentInterceptor` marker trigger scope machinery; plain interceptors and lambda/options
  interceptors keep the current null-context allocation-free path.
- **Deterministic ordering.** DI + options interceptors get an explicit `Order` (default 0), so audit →
  soft-delete → validation can be sequenced. Ties break by current order (options-first, then DI as today).
- **Cross-provider.** `DocumentProviderBase` gets the same SP-attach + scope plumbing; every provider store
  gains an `IServiceProvider` constructor path.

## Design

### 1. Scope-carrying call context

Extend `DocumentOperationScope` (already `AsyncLocal`, already tracks suppression + `Source`) to also hold an
`IServiceProvider? Services`. Add:

```csharp
// Interceptors.cs — DocumentOperationScope
public static IDisposable EnterServices(IServiceProvider services);   // pushes Services, restores on dispose
internal static IServiceProvider? CurrentServices { get; }
```

Expose the resolved provider on the context objects (nullable — null when no scope and no fallback):

```csharp
public sealed class DocumentWriteContext {  // Interceptors.cs:41
    public IServiceProvider? Services { get; internal set; }
    // ...existing members unchanged
}
public sealed class DocumentBulkContext {   // Interceptors.cs:117
    public IServiceProvider? Services { get; internal set; }
}
```

`Services` resolves in this order at pipeline entry (`InterceptorPipeline.BeforeWrite`, `Interceptors.cs:213`):
1. `DocumentOperationScope.CurrentServices` (ambient — set by a scoped `DocumentContext`);
2. else, if any interceptor about to run implements `IScopedDocumentInterceptor` **and** the store holds an
   `IServiceScopeFactory`, open one child scope, store it on the context, dispose it after `AfterWrite`
   (one scope spanning Before→write→After; for a UnitOfWork, one scope for the whole commit);
3. else `null`.

If an interceptor reads `ctx.Services` and it's `null`, that's a clear "no DI available here" error surfaced by
the interceptor, not a silent no-op.

### 2. `DocumentContext` as scope carrier

`DocumentContext` gains an optional `IServiceProvider` (its own scoped provider), injected by the generated /
hand-written scoped registration. Its forwarding wraps operations:

```csharp
// DocumentContext.cs
protected DocumentContext(IDocumentStore store, IServiceProvider? services = null) { ... }
// DocumentSet<T> operations enter the scope for the duration of the call:
//   using (DocumentOperationScope.EnterServices(this.services)) { await store.Insert(...); }
```

Because the common ASP.NET path already has a live scope, this allocates **nothing** beyond the `AsyncLocal`
push/pop. `DocumentSet<T>` stays a pure forwarder; the scope enter/exit lives in `DocumentContext` (or a tiny
internal helper the sets call) so all set operations are covered uniformly, including `UnitOfWork`.

**Stateless guarantee stays.** No identity map, no tracking, no `SaveChanges`. `CreateUnitOfWork()` remains the
explicit batching primitive.

### 3. Store holds an `IServiceScopeFactory` (fallback only)

`DocumentStore` and `DocumentProviderBase` gain a nullable `IServiceScopeFactory? scopeFactory` field, captured
in the SP-taking constructor (resolve `sp.GetService<IServiceScopeFactory>()`). Used **only** for the
fresh-child-scope fallback in §1 step 2. This is the single new retained reference to anything DI-shaped.

### 4. Provider parity

- Add the SP-taking constructor / attach path to `DocumentProviderBase` (`DocumentProviderBase.cs:10-44`) so
  Mongo/Cosmos/LiteDB/IndexedDB/DynamoDB/AzureTable route through the same `AttachServiceProvider` +
  `scopeFactory` capture.
- Update each provider store ctor (e.g. `MongoDbDocumentStore.cs:42`) and its DI registration to pass `sp`.
- Verify `Interceptors` binding (`MongoDbDocumentStore.cs:192`) still points at `options.Interceptors`.

### 5. Ordering

Add `int Order => 0;` to `IDocumentInterceptor` / `IDocumentBulkInterceptor` (default interface member — keeps
existing implementors source-compatible). `InterceptorPipeline` sorts the merged options+DI lists by `Order`
once at attach time. Document the guarantee: lower runs first; equal `Order` keeps options-before-DI.

### 6. `IScopedDocumentInterceptor` opt-in

```csharp
public interface IScopedDocumentInterceptor : IDocumentInterceptor { }  // marker only
```

`InterceptorPipeline` precomputes, at attach time, whether *any* registered interceptor needs a scope. If none
do, the scope-resolution branch is skipped entirely and the current allocation profile is preserved byte-for-byte.

## Provider compatibility tier

**All providers** — interceptor scope + audit context is provider-agnostic. Release note must state it lifts DI
interceptors to *every* provider (previously relational-only, undocumented).

## Phasing

1. **Core plumbing** — `DocumentOperationScope.Services`, `ctx.Services`, DI abstractions ref on core,
   `scopeFactory` field, `IScopedDocumentInterceptor`, `Order`. Relational store path first.
2. **`DocumentContext` scope carrier** — ctor SP, scope-enter wrapping, generator update
   (`DocumentContextGenerator.cs` emits the SP into the base ctor call; scoped registration passes it in).
3. **Provider parity** — `DocumentProviderBase` + all non-relational stores.
4. **Scope-aware `CaptureActor`** — add a `Func<IServiceProvider, string?>` overload on `TemporalOptions`
   (keep the existing `Func<string?>`), invoked with `ctx.Services`. Ship a documented sample resolving a
   scoped current-user service into the temporal actor. **Ordering** lands here too.
5. **Docs / skill / readme** per CLAUDE.md.

## Tests

- Interceptor resolves a **scoped** service and sees the *same instance* the caller injected (assert identity)
  — ASP.NET-style scope via `DocumentContext`.
- No-ambient-scope fallback: raw singleton store + `IScopedDocumentInterceptor` gets a fresh working scope;
  scope disposed after `AfterWrite` (assert scoped `IDisposable`/`IAsyncDisposable` disposed).
- Hot path unchanged: zero-interceptor and options-lambda-only writes allocate no scope (behavioral +
  ideally an allocation assertion).
- `Order` sequences three interceptors deterministically.
- Parity: the scoped-interceptor identity test runs green against **every** provider (or is explicitly skipped
  with reason for any deferred backend).
- `AfterWrite` scope still valid pre-commit / in-transaction.
- UnitOfWork: one scope spans the whole commit, not per row.
- Scope-aware `CaptureActor`: the `Func<IServiceProvider, string?>` overload resolves a scoped current-user
  service and lands the right actor on the temporal history row (`DocumentVersion.Actor`).

## Deferred / non-goals

- **No `CreatedBy`/`UpdatedBy` audit interceptor.** Temporal already owns audit (`CaptureActor` +
  `ChangesByActor`); this feature makes that actor scope-aware instead of duplicating it.
- **No change tracking / identity map / `SaveChanges` on `DocumentContext`.** Explicitly rejected — fights the
  schema-free explicit-write model and AOT.
- **No full DI container dependency in core** — abstractions only.
- **No per-write scope by default** — only ambient flow, or opt-in fallback.

## Open questions

- Keep `ctx.Services` as the primitive vs. a richer `DocumentCallContext` (user/tenant/correlation)? Lean:
  primitive `ctx.Services` + a scope-aware `CaptureActor` overload cover the known cases; only introduce a
  richer context object if tenancy/validation patterns converge on shared plumbing.
- Orleans: grain activation scope vs. per-write child scope — confirm the Orleans persistence path resolves a
  sensible scope (likely the fallback child scope; document it).
