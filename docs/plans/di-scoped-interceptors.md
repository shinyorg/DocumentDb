# Plan: Scoped DI + transaction-visible store for interceptors (via a first-class DocumentContext)

**Status:** SHIPPED (11.0.0, branch `feature/di-scoped-interceptors`). Phases 1–5 complete: core plumbing
(`ctx.Services`, transaction-bound `ctx.Store` via implicit one-op UoW, `IScopedDocumentInterceptor`, `Order`,
public `IDocumentStore.SuppressInterceptors()`); `DocumentContext` scope carrier (`AttachScope` in the scoped
registration — no user ctor change); provider parity (SP ctor + `DocumentProviderBase.AttachServiceProvider` on
all six non-relational stores, DynamoDB/AzureTable registrations wired) + Orleans SP fix; sample + tests
(`ScopedInterceptorTests`, `ProviderInterceptorParityTests`); four-artifact docs sync. **Residual (documented,
not built):** Mode B fallback child scope is relational-only — on the non-relational providers a scoped
interceptor gets its scope from an ambient `DocumentContext` (Mode A); the two boolean-patch overloads delegate
via `TransactionalDocumentStore.UpdateMerge`/`UpsertReplace`.
**Target version:** `11.0` (shipped as an additive feature). **Additive**
— the container-free `new DocumentStore(options)` path, existing interceptor registration, and the current
`IDocumentStore` surface all keep working unchanged. Ships across **all providers** (relational + non-relational),
because DI-interceptor parity is part of the deliverable.

> Self-contained build spec. Read `CLAUDE.md` (repo root) for the four-artifact rule (code+tests, docs site,
> skill, readme) before considering any commit "done". Branch off `v10`.

---

## Not an audit interceptor — audit already exists (temporal)

Do **not** ship a `CreatedBy`/`UpdatedBy` audit interceptor on the back of this feature. Temporal already
owns audit and does it better: `TemporalOptions.CaptureActor` (`Temporal.cs:42`) stamps *who* onto every
history row, indexed `(TypeName, Actor)`, surfaced via `DocumentVersion.Actor` and `ChangesByActor<T>`
(`DocumentStore.cs:2567-2581`) — a full versioned who-changed-what-when trail, not just last-touched fields.

This feature is **generic scoped-service access inside write hooks** — nothing more. It is deliberately
*not* framed around any one consumer. `CaptureActor` today is a `Func<string?>` invoked on the **singleton**
store with **no scope** (`DocumentStore.cs:1072` `mapping.CaptureActor?.Invoke()`), which is one illustration
of the general limitation this feature removes — but making `CaptureActor` itself scope-aware is explicitly
**out of scope** here (see non-goals). Ship the mechanism; let callers decide what to build on it.

## Goal

Let write-time hooks resolve **scoped** services during a write — general `IDocumentInterceptor` /
`IDocumentBulkInterceptor` access to the caller's DI scope. This is a plumbing feature with **no single
headline consumer**; validation against a scoped repo is one plausible use, but the deliverable is the
mechanism, not any specific interceptor. All while:

1. flowing the **caller's** scope (same scoped instances the request already has), not a fresh child scope;
2. keeping the zero-interceptor and options-only-lambda hot paths **allocation-free**;
3. making **`DocumentContext` the documented primary entry point** and the scope carrier, without turning it
   into a stateful EF-style change-tracker;
4. working on **every provider**, closing a latent gap where DI interceptors silently never fire on
   Mongo/Cosmos/LiteDB/IndexedDB/DynamoDB/AzureTable today;
5. giving interceptors a **transaction-scoped store handle** (`ctx.Store`) — reads and side-effect writes from a
   hook run on the *active connection and transaction* and see this unit's **uncommitted rows**
   (read-your-writes within the txn on relational + LiteDB; committed-state on other NoSQL — see provider tier).
   This is a core requirement, not a nicety: it's impossible today and unsafe through any DI-resolved store
   (shared-connection deadlock).

This feature therefore has **two mechanisms**, both plumbing (no headline consumer): scoped-service access via
`ctx.Services`, and a transaction-visible store via `ctx.Store`. Decisions below are locked (confirmed with the
maintainer).

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
  extended to carry an `IServiceProvider`. **No *existing* `IDocumentStore` method signatures change** — the only
  addition is the additive public `SuppressInterceptors()` handle (default-implemented / additive, so existing
  implementors and callers stay source-compatible).
- **Opt-in marker so the hot path stays free.** Only interceptors implementing a new
  `IScopedDocumentInterceptor` marker trigger scope machinery; plain interceptors and lambda/options
  interceptors keep the current null-context allocation-free path.
- **Deterministic ordering.** DI + options interceptors get an explicit `Order` (default 0), so audit →
  soft-delete → validation can be sequenced. Ties break by current order (options-first, then DI as today).
- **Cross-provider.** `DocumentProviderBase` gets the same SP-attach + scope plumbing; every provider store
  gains an `IServiceProvider` constructor path.
- **Transaction-visible interceptors via a session-bound `ctx.Store` (LOCKED — core requirement, was
  open-question #1, resolved as "option A, lazy").** Every write that has per-doc interceptors registered
  executes as an **implicit one-operation unit of work**: a transaction opens, a `TransactionalDocumentStore`
  bound to that connection+transaction becomes `ctx.Store`, and `BeforeWrite`/`AfterWrite` run inside it. A hook
  can read/write through `ctx.Store` and see the unit's **uncommitted** rows (read-your-writes in the txn), and
  `AfterWrite` side-effects commit **atomically** with the triggering write — no shared-mode deadlock, because
  it's the same session, not a re-entrant top-level call. **Lazy:** with no per-doc interceptors registered,
  single writes keep their current non-transactional fast path (no added transaction; hot path byte-for-byte
  unchanged). Correctness bonus: on this path a single write **and** its temporal-history append become atomic
  (today they autocommit as separate statements in shared mode). Full in-hook uncommitted-row *visibility* holds
  on the **relational providers and LiteDB** (real same-connection ACID transactions); on the other NoSQL
  providers `ctx.Store` is safe to call but atomicity/visibility follow that backend's model (Cosmos
  transactional batch, Mongo replica-set sessions, committed-state elsewhere) — see provider tier.
- **Orleans: fresh child scope per grain write; not grain-activation scope (LOCKED).** Grain storage is a
  singleton `IGrainStorage` over a singleton store with no ambient `DocumentContext`, so an
  `IScopedDocumentInterceptor` gets a **fresh child scope from the silo root `IServiceScopeFactory`, opened per
  `WriteStateAsync`** (Before→write→After, disposed after) — the standard fallback path, nothing Orleans-specific.
  Grain-activation scope is rejected (it would keep "scoped" services alive for the activation's lifetime, and
  Orleans doesn't expose activation as a DI scope). **Prerequisite:** `DocumentDbGrainStorage.BuildRelationalStore`
  (`DocumentDbGrainStorage.cs:70`) currently does `new DocumentStore(dso)` with **no `IServiceProvider`** — thread
  the silo `services` in (`new DocumentStore(dso, services)`), which also fixes the latent bug that DI
  interceptors never fire in Orleans grain storage today. `ctx.Store` needs no Orleans-specific work — a grain
  write is a single write and rides the implicit-one-op-UoW path onto a real transaction.

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
    public IDocumentStore Store { get; internal set; }   // originating, operation-scoped store (below)
    // ...existing members unchanged
}
public sealed class DocumentBulkContext {   // Interceptors.cs:117
    public IServiceProvider? Services { get; internal set; }
    public IDocumentStore Store { get; internal set; }
}
```

**`ctx.Store` — the originating, operation-scoped store.** The store sets `ctx.Store = this` when it builds the
context. Crucially, inside a `UnitOfWork`/transaction the context is built by the `TransactionalDocumentStore`
(`DocumentStore.cs:2954`), so `ctx.Store` is the **transaction-scoped** store — reads and side-effect writes made
through it in `BeforeWrite`/`AfterWrite` run on the *same connection and transaction* as the triggering write (an
`AfterWrite` outbox insert commits atomically with it and sees the just-written uncommitted row). DI cannot supply
this: `ctx.Services.GetService<IDocumentStore>()` returns the singleton top-level store, which opens its own
connection (not atomic; a SQLite second-writer deadlock) and, under keyed per-context registration, may be the
wrong store entirely. `ctx.Store` is never null. **Guardrail:** writing through `ctx.Store` re-enters the
interceptor pipeline; for side-effect writes that must not recurse (outbox), wrap them in a suppression scope —
expose the existing internal `DocumentOperationScope.SuppressInterceptors()` as a public handle on the store as
part of this phase. **`ctx.Store` is always session-bound (LOCKED).** On the `UnitOfWork` path it's the unit's
`TransactionalDocumentStore`; a **single** write with per-doc interceptors is wrapped in an implicit one-op unit
of work so `ctx.Store` is a `TransactionalDocumentStore` there too — never the top-level store. This is why
reads/writes through `ctx.Store` see uncommitted rows and never deadlock in shared-connection mode (same
session, not a re-entrant `ExecuteAsync`/`sharedSemaphore` call). `ctx.Store` is valid **only within the hook**
(its connection/transaction closes when the write — or the enclosing unit — completes); don't capture it past
`AfterWrite`.

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

## Sample (Phase 4 deliverable)

The documented, neutral sample to ship — a scoped interceptor that validates a write against **request-scoped**
services and writes an audit/outbox row atomically through `ctx.Store`.

**Packages:** `Shiny.DocumentDb` (core — `IScopedDocumentInterceptor`, `DocumentWriteContext`),
`Shiny.DocumentDb.Sqlite` (`SqliteDatabaseProvider`, or any provider),
`Shiny.DocumentDb.Extensions.DependencyInjection` (`AddDocumentStore` / `AddDocumentContext`).

```csharp
using Microsoft.Extensions.DependencyInjection;
using Shiny.DocumentDb;

// IScopedDocumentInterceptor (marker) is what makes the pipeline populate ctx.Services.
public sealed class OrderValidationInterceptor : IScopedDocumentInterceptor
{
    public int Order => 0;                       // lower runs first

    public async Task BeforeWrite(DocumentWriteContext ctx, CancellationToken ct)
    {
        if (ctx.DocumentType != typeof(Order))
            return;

        // Same scope the caller (request) already holds — resolve scoped deps here, NOT via the ctor.
        var sp        = ctx.Services ?? throw new InvalidOperationException("No DI scope for this write.");
        var validator = sp.GetRequiredService<IOrderValidator>();   // scoped
        var user      = sp.GetRequiredService<ICurrentUser>();       // scoped

        await validator.EnsureCanPlace((Order)ctx.Document!, user.Id, ct);
    }

    public async Task AfterWrite(DocumentWriteContext ctx, CancellationToken ct)
    {
        if (ctx.DocumentType != typeof(Order))
            return;

        // ctx.Store is the transaction-scoped store → this outbox row commits atomically with the order.
        // Suppress so the outbox insert doesn't re-enter this interceptor.
        using (ctx.Store.SuppressInterceptors())
            await ctx.Store.Insert(new OutboxEntry(ctx.Id!, "OrderPlaced"), ct);
    }
}
```

**Mode A — through `DocumentContext` (recommended, ASP.NET): the request scope flows in.**

```csharp
public sealed class AppDb : DocumentContext
{
    public AppDb(IDocumentStore store) : base(store) { }
    public DocumentSet<Order> Orders => this.Set<Order>();
}

builder.Services.AddDocumentContext<AppDb>(              // AppDb + its store: scoped
    o => o.DatabaseProvider = new SqliteDatabaseProvider("Data Source=app.db"),
    store => new AppDb(store));

builder.Services.AddScoped<IOrderValidator, OrderValidator>();
builder.Services.AddScoped<ICurrentUser, HttpCurrentUser>();
builder.Services.AddSingleton<IDocumentInterceptor, OrderValidationInterceptor>();  // stateless → singleton
```

Because `AppDb` is scoped, it enters `DocumentOperationScope.EnterServices(<request scope>)` around each write,
so `ctx.Services` is the *same* scope — `IOrderValidator`/`ICurrentUser` are the request's own instances.

**Mode B — raw `IDocumentStore` (MAUI / Orleans / background, no ambient scope): fallback child scope.**

```csharp
services.AddDocumentStore(o => o.DatabaseProvider = new SqliteDatabaseProvider("Data Source=app.db"));
services.AddScoped<IOrderValidator, OrderValidator>();
services.AddSingleton<IDocumentInterceptor, OrderValidationInterceptor>();
```

No context carries a scope, so because the interceptor is `IScopedDocumentInterceptor` the pipeline opens one
fresh child scope per write (Before→write→After) and disposes it after. Right for "needs a scoped repo"; it is a
*new* scope, not a request scope, so request-identity must ride a singleton ambient accessor.

**Two gotchas, both consequences of the singleton pipeline:** register the interceptor as a **singleton** (a
scoped `IDocumentInterceptor` registration is not re-resolved per scope), and resolve scoped deps from
**`ctx.Services`** inside the method, never the constructor.

## Provider compatibility tier

**All providers** — `ctx.Services` (scoped DI) + interceptor firing is provider-agnostic. Release note must state
it lifts DI interceptors to *every* provider (previously relational-only, undocumented).

**`ctx.Store` is non-null and safe to call on every provider** (the shared-connection deadlock is relational-only;
NoSQL drivers are pooled/thread-safe). But the *transaction guarantee* is tiered — document this table:

| Tier | Providers | `ctx.Store` guarantee |
|---|---|---|
| Full | SQLite, DuckDB, MySQL, SQL Server, PostgreSQL, Oracle, LiteDB | uncommitted-row visibility in-hook + `AfterWrite` side-effects atomic |
| Partition | Cosmos | atomic only within one type/partition (`CreateTransactionalBatch`); hook reads see committed state |
| Conditional | MongoDB | full **only** on a replica set (client session/transaction); standalone → committed-state, best-effort |
| Best-effort | IndexedDB, DynamoDB, Azure Table | committed-state reads only; no in-hook uncommitted visibility (IndexedDB's browser txn also auto-closes across `await`; DynamoDB / Azure Table have no interactive read-your-writes transaction) |

Implementation follows the tier: keep the implicit-one-op-UoW **relational-first**; NoSQL routes single-write
interceptors through its existing `RunUnitAsync` where a real transaction exists (LiteDB), else runs the hook
against the store with committed-state semantics. Don't force a transaction a backend can't cheaply provide.
(Confirm `MongoDbDocumentStore.RunUnitAsync` opens a client session when one is available.)

## Phasing

1. **Core plumbing** — `DocumentOperationScope.Services`, `ctx.Services`, session-bound `ctx.Store` (single
   writes with per-doc interceptors run as an **implicit one-op unit of work** so `ctx.Store` is always a
   `TransactionalDocumentStore`; lazy — untouched hot path when no interceptors), a public
   `SuppressInterceptors()` handle on the store, DI abstractions ref on core, `scopeFactory` field,
   `IScopedDocumentInterceptor`, `Order`. Relational store path first.
2. **`DocumentContext` scope carrier** — ctor SP, scope-enter wrapping, generator update
   (`DocumentContextGenerator.cs` emits the SP into the base ctor call; scoped registration passes it in).
3. **Provider parity** — `DocumentProviderBase` + all non-relational stores; the `ctx.Store` guarantee follows
   the provider tier below (full txn visibility on relational + LiteDB; committed-state elsewhere). Includes the
   **Orleans SP fix** (`DocumentDbGrainStorage.cs:70` → pass `services` into the store) so DI interceptors fire
   and the per-write fallback scope resolves in a silo.
4. **Sample + ordering** — ship the documented sample (see "Sample" above): a scoped
   `IScopedDocumentInterceptor` resolving scoped services via `ctx.Services` and writing an outbox row
   atomically through `ctx.Store`. **Ordering** lands here too.
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
- `ctx.Store` (UnitOfWork): an `AfterWrite` insert through `ctx.Store` commits **atomically** — assert the outbox
  row and the triggering write both roll back together when the unit is aborted, and the side-effect insert
  (wrapped in `SuppressInterceptors()`) does not re-fire the interceptor. `ctx.Store` non-null on every provider.
- `ctx.Store` (single write): side-effects through `ctx.Store` work **without deadlock in both pooled and
  shared-connection mode** (guaranteed by the session-bound `ctx.Store` decision; a naive top-level-store
  approach would deadlock in shared mode).
- `ctx.Store` read-from-hook: a `BeforeWrite` that **queries** through `ctx.Store` (`Get`/`Query`) runs without
  deadlock against a `RequiresSingleConnection` provider (in-memory SQLite / DuckDB). This test must run against
  a shared-mode provider, not only pooled.
- `ctx.Store` visibility semantics: `BeforeWrite` `ctx.Store.Get(id)` returns the **prior** row (new value is on
  `ctx.Document`); `AfterWrite` `ctx.Store.Get(id)` returns the **new** row with generated id/version.
- `ctx.Store` uncommitted-rows-in-unit: in a multi-write `UnitOfWork`, the `BeforeWrite` of a later document can
  read (via `ctx.Store`) an earlier document this same unit already wrote but has **not committed**; aborting the
  unit rolls back all of them together.

## Deferred / non-goals

- **No `CreatedBy`/`UpdatedBy` audit interceptor.** Temporal already owns audit (`CaptureActor` +
  `ChangesByActor`); don't duplicate it on top of this feature.
- **No scope-aware `CaptureActor` overload in this feature.** A `Func<IServiceProvider, string?>` overload on
  `TemporalOptions` is a trivial future add once `ctx.Services` exists; it is intentionally left out so the
  deliverable stays the generic mechanism and isn't framed around actor capture.
- **No tenancy changes.** Tenant resolution and enforcement are out of scope; enforcement stays in the
  store's query generation (read filter + write stamp), which write-only interceptors can't express anyway.
- **No change tracking / identity map / `SaveChanges` on `DocumentContext`.** Explicitly rejected — fights the
  schema-free explicit-write model and AOT.
- **No full DI container dependency in core** — abstractions only.
- **No per-write scope by default** — only ambient flow, or opt-in fallback.

## Open questions

All prior open questions are now decided:
- **(Resolved)** `ctx.Store` on single writes → **session-bound via an implicit one-op unit of work, lazy** (see
  Decisions). Was the main open question.
- **(Resolved)** Orleans scope → **fresh child scope per grain write** (not grain-activation), gated on the
  `DocumentDbGrainStorage` SP fix (see Decisions + Phase 3).
- **(Resolved)** NoSQL `ctx.Store` transaction visibility → **provider tier table** (Provider compatibility
  tier); documentation, not a blocker.
- **(Resolved)** `ctx.Services` primitive vs. richer `DocumentCallContext` → **primitive** — tenancy and actor
  capture are out of scope, so nothing converges on a richer context object.

Remaining verification (not decisions, confirm during build):
- `MongoDbDocumentStore.RunUnitAsync` — confirm it opens a client session/transaction when the deployment is a
  replica set, so the Conditional tier holds; otherwise it's committed-state everywhere on Mongo.
- IndexedDB — confirm a hook that awaits before touching `ctx.Store` degrades cleanly to committed-state rather
  than throwing "transaction not active".
