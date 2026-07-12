# Plan (RFC): Store-as-connection — a per-operation session context

**Status:** DESIGN / RFC. Not built. Resolved so far: shared-mode keeps one connection (§7); injectable
session model (§4a); `IDocumentSessionFactory` = scope-ownership primitive (§4b/§4a); multi-store keying moves
onto the factory and absorbs `IDocumentStoreProvider` (§4c, no keyed DI sugar); `DocumentContext` wraps a
session (§4d); the **session *is* the unit of work** — `UnitOfWork`/`CreateUnitOfWork` removed (§4e). MAUI/no-scope
resolved (§4a-MAUI): factories only, no injectable session; inject the singleton `IDocumentStore` for everyday
CRUD (one managed SQLite connection) and `IDocumentSessionFactory` for atomic multi-writes; registration mirrors
EF's `AddDbContext`/`AddDbContextFactory`. Explicit session transactions added (§4f): `BeginTransaction` (one
active at a time) for locking reads + set-based `ExecuteUpdate`/`ExecuteDelete`; `SaveChanges` joins the active
tx or auto-creates one. MAUI typed-context via `IDocumentContextFactory<T>` per unit of work (§4d/§4a-MAUI).
Concrete C# signatures drafted (§5.1–5.5: root delta, `IDocumentSession`, `IDocumentTransaction`+`LockMode`,
`IDocumentSessionFactory`, `DocumentContext`/`DocumentSet<T>`/`IDocumentContextFactory<T>`). **Core + SQLite spike
DONE and green (§8)** — validates the boundary additively (old surface untouched). **Next: decide whether to
proceed to the full breaking migration across all providers.**
**Target version:** `12.0` (tentative). **Breaking** — `IDocumentStore` is split by lifetime; providers
implement a new session type. Per the "Removing or replacing code" rule in `CLAUDE.md` this lands as a clean
break with a release note and **no `[Obsolete]` shims** — every provider, test, sample, doc, skill, and readme
move in the same cut.

---

## The request (maintainer)

> I look at the library and we seem to have traps everywhere for thread safety, scoping issues (look at what
> had to be done for diagnostics), etc. I want to look at the document store as a "connection" that opens and
> closes to make that its context.

Direction chosen after review: **write this RFC first**, and adopt the **one-shot convenience layer on the
root** ergonomics (§4) rather than an EF-`DbContext`-style "explicit sessions only" model.

---

## 1. Root cause — one decision, many symptoms

`IDocumentStore` is a **process-lifetime singleton that is also the per-operation API**
(`AddSingleton<IDocumentStore>`, `DependencyInjection/ServiceCollectionExtensions.cs:42`). A singleton has
nowhere to hang per-operation state, so everything that is naturally per-operation was pushed into ambient
`AsyncLocal` slots and bolt-on carriers:

| Symptom | Location | What it really is |
|---|---|---|
| `AsyncLocal<IServiceProvider?> services` | `Interceptors.cs:414` (`DocumentOperationScope`) | The flowing DI scope, because the store can't hold one |
| `AsyncLocal<DocumentOperationSource> current` | `Interceptors.cs:412` | The operation's source (Direct/Temporal) |
| `AsyncLocal<bool> suppressed` | `Interceptors.cs:413` | The interceptor-suppression flag |
| `DocumentContext.AttachScope(sp)` | `DocumentContext.cs:41` | A scope-carrier type whose only job is to smuggle the request scope into interceptors |
| Fallback child scope | `DocumentStore.cs:2286` (`RunUnitImpl`) | Manufactures a scope per unit-of-work when none flowed in |
| `AsyncLocal<bool> active` re-entrancy guard | `Diagnostics/OperationTracker.cs:22` | Needed only because one op re-enters the store as a nested unit-of-work |
| `ThrowIfScopedInterceptors` | `ServiceCollectionExtensions.cs` | Registration-time guard compensating for the captive singleton |

Every row is a symptom of the same missing concept: **there is no per-operation context object.**

**And it already exists internally.** `TransactionalDocumentStore` (`DocumentStore.cs:2302`) pins exactly one
connection + transaction, implements `IDocumentStore`, and is short-lived. The "session/connection" the
maintainer is describing is already the shape of the code — it is simply not the public unit, so the state that
belongs on it is scattered into ambient slots instead.

**The connection story is already split-brained**, which reinforces the direction:
- **Shared mode** (SQLite, DuckDB, LiteDB — `provider.RequiresSingleConnection == true`): one long-lived
  `sharedConnection` and a `SemaphoreSlim(1,1)` serialize *every* operation (`DocumentStore.cs:418`). This is
  the biggest concurrency ceiling in the library and is hit hardest by Orleans grain storage on SQLite.
- **Pooled mode** (Postgres, SqlServer, MySQL, MariaDb, CockroachDb, Oracle): a fresh connection opened and
  closed per op (`DocumentStore.cs:437`) — *already* the model we want.

So half the providers already behave the target way; the other half serialize globally.

## 2. Goal

Split `IDocumentStore` by **lifetime** so per-operation state lives on a per-operation object, and delete the
ambient plumbing that exists only to work around its absence.

## 3. The two levels

### Root / factory — singleton, process-lifetime
Owns everything that legitimately outlives an operation:
- the connection pool (pooled mode) or the single shared connection + its serialization (shared mode);
- `ChangeBroadcaster` and all native change-feed subscriptions (they outlive any session — see §6);
- the `tableInitTasks` `Lazy<Task>` cache and shared-connection init flag;
- the interceptor registry, telemetry `Meter`/`ActivitySource`, `DocumentStoreOptions`, id-accessor cache.

Proposed name: keep **`IDocumentStore`** as the root (least churn for the DI story and the singleton
registration), and add the session type below. Alternative considered: rename root to
`IDocumentStoreFactory` — rejected as gratuitous extra churn given the convenience layer keeps CRUD on the
root anyway.

### Session — `IAsyncDisposable`, short-lived, **is the unit of work**
`store.OpenSession()` (sync to build — no I/O; connection is lazy) returns an **`IDocumentSession`** that owns:
- **the pending-writes buffer** — the session *is* the unit of work; there is no separate `UnitOfWork` type
  (§4e);
- **the DI scope** (a field, not an `AsyncLocal`);
- **the operation context** — source, suppression flag, re-entrancy depth (fields, not `AsyncLocal`).

Writes (`Add/Update/Upsert/Remove`) **buffer**; `SaveChanges()` opens one connection + transaction, flushes
the buffer atomically, commits, and releases the connection. Reads (`Get/Query/Count/…`) are immediate
(borrow a connection per read). So the session holds **no connection while idle or between SaveChanges** —
only for the duration of a `SaveChanges` or a read. This is the EF-`DbContext` contract: **short-lived,
single-flow, not thread-safe.** Disposal (without a final `SaveChanges`) discards un-flushed writes and
disposes the scope if owned (§4a/§4b).

### What each ambient trap becomes
- `AsyncLocal<IServiceProvider?> services` → **deleted.** The session holds the scope; interceptors read
  `session.Services`.
- `DocumentContext` scope-carrier role → **the context wraps a session** (composition — see §4d). `AttachScope`
  deleted; `IDocumentStoreProvider` folded into the factory (§4c).
- `RunUnitImpl` fallback child-scope dance → **deleted.** A session always has a scope.
- `ThrowIfScopedInterceptors` → **deleted.** Scoped interceptors resolve from the session scope naturally.
- `AsyncLocal<DocumentOperationSource> current` / `AsyncLocal<bool> suppressed` → **session fields.**
- `OperationTracker`'s `AsyncLocal<bool> active` → a **session depth counter** (nesting is now explicit —
  the one-shot convenience methods open a session and the inner unit-of-work reuses it rather than re-entering
  the public singleton).

## 4. Ergonomics — one-shot convenience on the root (chosen)

The common call must not regress. The root keeps the full CRUD/query surface; each root method **opens and
disposes a session internally** — which is exactly what happens today via the implicit one-op unit of work, so
this is a refactor of an existing code path, not new behavior:

```csharp
// Immediate convenience (inject IDocumentStore) — commits each call, unchanged from today:
await store.Insert(customer);
var list = await store.Query<Customer>("...").ToListAsync();

// Unit of work (inject IDocumentSession, or open one) — buffer then commit atomically:
await using var session = store.OpenSession();
session.Add(a);
session.Add(b);
await session.SaveChanges();        // one transaction, one connection, one DI scope
```

The root's immediate `Insert(x)` is itself a one-op session under the hood (open → `Add` → `SaveChanges` →
dispose) — the existing implicit one-op-unit-of-work path, now explicit. There is **no `CreateUnitOfWork()`**
and **no `UnitOfWork` type**: `OpenSession()` *is* how you get a unit of work (§4e). `DocumentContext` /
`DocumentSet<T>` forward to a session instead of carrying an
`AsyncLocal` scope; a scoped `DocumentContext` registration binds its session to the request scope.

This is the Dapper-`IDbConnection`-extensions model (one-shot helpers) layered over an EF-`DbContext`-style
explicit session — you get both, and the explicit form is the escape hatch, not the default tax.

## 4a. Injectable session, connection ownership, and the SQLite single-connection rule

**Direction (maintainer):** the session should live on the DI scope so it can be **injected directly** (not
only obtained via `store.OpenSession()`), and the session **is the unit of work** (§4e) — no separate type.

Because the session is a unit of work, it takes the **EF-`DbContext` contract: short-lived, single-flow, not
thread-safe** (it holds a mutable pending-writes buffer). That settles connection ownership cleanly:

- An **idle session holds no connection.** The connection is borrowed only for the duration of a `SaveChanges`
  (write flush) or a read — **or held for the whole of an explicit `BeginTransaction` (§4f)**, which pins one
  connection precisely so it can hold locks and span multiple statements.
- **`SaveChanges`** opens one connection + transaction, flushes the buffer atomically, commits, releases —
  pooled providers rent from the ADO.NET pool (concurrent sessions get their own → real concurrency); shared
  providers (SQLite/DuckDB/LiteDB) take the root's single connection under the existing `SemaphoreSlim(1,1)`.
  Concurrent `SaveChanges` on SQLite serialize — inherent to SQLite's single writer, not a new limitation.
- **Reads** borrow a connection per read the same way.

So SQLite's one-connection rule is honored by construction regardless of how many sessions are alive: every
flush/read funnels through the one connection under the semaphore, *exactly today's behavior*, just re-homed.

> **Consequence — the "one app-lifetime singleton session for MAUI" idea (earlier §4a draft) is retired.** A
> unit of work cannot be a long-lived, multi-thread singleton — its pending buffer is mutable per-flow. MAUI /
> desktop therefore use the **factory per unit of work** (§4b), the idiomatic `IDbContextFactory` pattern. For
> the "just write one thing" case those apps inject the **thread-safe singleton `IDocumentStore`** (immediate
> convenience). *If you'd rather keep a separate thread-safe autocommit session concept, that's the fork to
> flag — this RFC assumes the merge.*

### Lifetime registration is per host model
There is no single correct `ServiceLifetime` — it depends on whether the host has an ambient scope. The
**immediate `IDocumentStore` is always a singleton**; the **`IDocumentSession` (unit of work)** varies:

| Host | `IDocumentSession` (unit of work) | Rationale |
|---|---|---|
| ASP.NET Core | **Scoped** — one UoW per request, disposed with the request; carries the request's DI scope into interceptors | Framework creates a scope per request; single-threaded per request. Un-flushed writes at scope-end are discarded (EF contract) — call `SaveChanges` |
| MAUI / desktop / Blazor-hybrid | **`IDocumentSessionFactory`** — open one per unit of work (`await using`) | No ambient scope; a UoW can't be an app-lifetime singleton. Immediate one-offs go through the singleton `IDocumentStore` |
| Background workers / jobs / anything ad-hoc | **`IDocumentSessionFactory`** (`OpenSession()` per unit of work) | Explicit control where no scope is natural |

### Registration mirrors EF (`AddDbContext` vs `AddDbContextFactory`)
The rule that removes all ambiguity: **a host with no ambient scope gets factories only — never an injectable
session.** Registering a scoped `IDocumentSession` where there is no scope either throws at resolve (MAUI root)
or, if downgraded to singleton, hands out a shared non-thread-safe UoW (a footgun). So:

| Registration | Registers | For |
|---|---|---|
| `AddDocumentStore(o => …)` | singleton `IDocumentStore` + singleton `IDocumentSessionFactory` (+ `IDocumentContextFactory<T>` when a context is declared) | **everywhere** — the universally-safe base. This alone is the full MAUI / desktop / Blazor-Hybrid / console story |
| `AddScopedDocumentSession()` (or the ASP.NET overload / flag) | scoped `IDocumentSession` (+ scoped `DocumentContext`) | request-scoped hosts (ASP.NET Core) — opt-in on top of the base |

This is exactly EF's split — `AddDbContext` (scoped) for ASP.NET vs `AddDbContextFactory` (singleton) for
Blazor/WinUI/WPF/MAUI/console — so the mental model transfers.

### §4a-MAUI. The definitive MAUI / no-ambient-scope story
MAUI (and desktop, Blazor Hybrid, console) has **no per-request scope**, and the session is now a
non-thread-safe UoW, so "inject one app-lifetime session" is off the table. The original intuition —
*one managed SQLite connection, one thing to reach for* — still holds; it just lands on the **root**, not a
session:

- **Everyday CRUD → inject the singleton `IDocumentStore`.** `await store.Insert/Update/Get/Query(...)` —
  thread-safe, immediate, and on SQLite every call funnels through the root's **one** shared connection under
  the existing `SemaphoreSlim(1,1)`. This *is* "one connection managed properly," and it's the object you
  inject everywhere.
- **A multi-write transaction → open a short session from `IDocumentSessionFactory`.**
  `await using var s = factory.OpenSession(); s.Add(a); s.Add(b); await s.SaveChanges();` Its flush borrows the
  same one SQLite connection under the same semaphore, so concurrent store calls just queue.
- **Typed/EF-style preference → `IDocumentContextFactory<T>.Create()`** per unit of work (`await using`).

Two facts make this comfortable rather than ceremonious:
1. **An idle session holds no connection** (§4a) — it only touches the connection during `SaveChanges`/reads.
   So a session may be scoped to *whatever your logical unit is*: a single write, or a whole **editor screen**
   that accumulates edits and commits on "Save." A screen-lifetime UoW held across user think-time hoards
   nothing (as long as it stays on one flow — which a single screen is).
2. **Most MAUI apps already wrap data access in their own singleton repository/service.** That singleton
   injects `IDocumentStore` / `IDocumentSessionFactory` one layer down and exposes domain methods; the
   ViewModels inject the repository and never see a session at all. The factory ceremony lives in exactly one
   place.

**Net:** in MAUI you inject `IDocumentStore` (or your own repo) for the common path and reach for
`IDocumentSessionFactory` only when you need an atomic multi-write unit. There is genuinely **one SQLite
connection** for the whole app (owned by the root), regardless of how many transient sessions come and go —
which is precisely the "1 session / 1 connection" outcome the original direction asked for, now expressed
safely.

A scoped `DocumentContext` (ASP.NET) binds its session to the resolving scope; the factory path owns its own
child scope and session (§4b).

## 4b. `IDocumentSessionFactory` — for scope-less circumstances
Some call sites have **no ambient DI scope** to ride: background workers / jobs, `IHostedService`s, MAUI
timers and event handlers, Orleans grains, startup seeders, and change-feed callbacks. Injecting a scoped
`IDocumentSession` there either throws (no scope) or captures the wrong one. `IDocumentSessionFactory` is the
answer, and its **real job is scope ownership**, not merely "make a session":

- An **injected** session rides a scope *someone else owns* (request / app root); the container disposes it and
  the session must **not** dispose the scope.
- A **factory** session mints a **private child scope** and **owns + disposes** it. The caller must
  `await using` the session; disposing the session tears down the scope. The session carries an `ownsScope`
  flag to distinguish the two.

```csharp
public interface IDocumentSessionFactory
{
    // Fresh session-owned child scope; the session disposes it on dispose.
    // Synchronous — no I/O at open (the connection opens lazily on first op, §4a).
    IDocumentSession OpenSession();
    IDocumentSession OpenSession(string storeName);                  // named/multi-store (see §4c)

    // Bind to a caller-supplied scope (Orleans activation, a manual IServiceScope, a page scope).
    // The session does NOT dispose the supplied scope — the caller owns it.
    IDocumentSession OpenSession(IServiceProvider scope);
    IDocumentSession OpenSession(string storeName, IServiceProvider scope);

    // Root-only surface (change feed, maintenance, backup) by name — folds in IDocumentStoreProvider.
    IDocumentStore GetStore(string storeName = "default");
}
```

Design notes:
- **Synchronous open.** Idle sessions hold no connection (§4a), so `OpenSession()` is pure allocation — cheap
  in a hot background loop, no `await` at the open point.
- **Shared primitive.** The root's one-shot convenience methods and the scoped registration both build on
  `OpenSession`. Preserve today's hot-path optimization: only mint a child scope when a **scoped interceptor is
  registered** (`NeedsScope`); otherwise the session references the root provider directly — no per-call scope
  cost.
- **Interface segregation.** The **root `IDocumentStore` implements `IDocumentSessionFactory`**, and scope-less
  consumers depend on the narrow interface rather than the whole root surface (also trivial to mock).
- **No ambient reuse — deliberate.** The factory always creates a fresh, independent session; it never looks
  for a "current" one. Reaching for the ambient session is exactly the `AsyncLocal` behavior this RFC deletes.
  Want the current session → inject it; no scope → use the factory.
- **Disposal is the footgun.** A factory session not disposed leaks its child scope (and a held connection if a
  transaction is open). Annotate the return for a missing-`await using` analyzer warning, and use `await using`
  in every sample.
- **Orleans / seeders.** Grain storage holds the root + factory and opens a session per grain op
  (`OpenSession(activationScope)` where Orleans exposes one, else `OpenSession()`); `IDocumentSeeder`
  runs against a factory session.

### The one caveat to call out: captive dependencies
A **scoped** session must not be captured by a **singleton** consumer (the classic ASP.NET captive-dependency
error — a singleton would pin the first request's session forever). Guidance: singletons that need the store
inject **`IDocumentStore`** (the root, one-shot convenience) or **`IDocumentSessionFactory`**, never a scoped
`IDocumentSession`. We can add a lightweight startup diagnostic (mirroring the removed
`ThrowIfScopedInterceptors`) that warns when a singleton captures a scoped session.

## 4c. Multi-store / keyed naming moves onto the factory

**Direction (maintainer):** move keyed-store handling onto the document factory. Today keying is done three
overlapping ways — keyed `IDocumentStore` (by context type *and* by name), `IDocumentStoreProvider.GetStore`,
and the default un-keyed store. That sprawl collapses into **one surface: `IDocumentSessionFactory` is the
multi-store entry point.**

- The factory (a singleton) holds the `name → root` registry and builds each root lazily.
- `OpenSession(name)` / `OpenSession(name, scope)` open a session on a named store; `GetStore(name)` returns
  the root for the root-only surface (change feed, maintenance, backup) — **this folds in
  `IDocumentStoreProvider`, which is removed.**
- We stop registering N keyed `IDocumentStore` singletons purely for user resolution. Internally the factory
  still owns one root per name.

**Trade-off:** you lose `[FromKeyedServices("orders")] IDocumentStore` declarative constructor injection.
**Decision — no keyed DI sugar at all.** Keying lives *only* on the factory (imperative) and on
`DocumentContext` (declarative, §4d); there is **no keyed `IDocumentSession`/`IDocumentStore` registration**.
Rationale:

- It would break the **one-scope-one-session** invariant (§4d). Injecting `[FromKeyedServices("orders")]` and
  `[FromKeyedServices("billing")]` into one request yields *two* sessions — two scopes/connections/transactions
  in a single scope — reintroducing the "which session am I on?" ambiguity this RFC removes.
- The factory and `DocumentContext` already cover every multi-store case, and both make multiplicity
  **explicit**: `factory.OpenSession("orders")` is an obviously-distinct owned session; two contexts are two
  clearly-separate façades. Keyed injection was the *implicit* path that blurred that line.
- It also multiplies the captive-dependency / lifetime matrix per key for zero capability gain.

So the plain **un-keyed `IDocumentSession` is "the scope's one session"** (the single-store common case);
anything multi-store goes through the factory or a `DocumentContext`. Keyed DI was only ever compensating for
the absence of a factory.

## 4d. DocumentContext folds in as a typed façade over a session

Today `DocumentContext` is a *stateless facade over the shared singleton store* plus `AttachScope` (the
`AsyncLocal` scope-carrier). In the new model it **wraps a session, not the store** — and inherits the whole
lifetime / ownership / factory story for free.

- **Composition.** `DocumentContext` HAS-A `IDocumentSession`; `DocumentSet<T>` forwards to it. The generator's
  `Set<T>()` changes from `new(this.Store, …)` to `new(this.Session, …)` (`src/Shiny.DocumentDb.Generators`).
  Expose `.Session` and `.Store` (root) as escape hatches.
- **`AttachScope` deleted.** The session carries the scope; nothing ambient to flow.
- **Ownership mirrors the session.** An injected scoped/singleton context wraps the scope's session; a
  factory-created context (`IDocumentContextFactory<T>.Create()`) **owns** its session + scope and is
  `await using`. `IDocumentContextFactory<T>` becomes a typed wrapper over `IDocumentSessionFactory` — the same
  scope-ownership primitive (§4a/§4b).
- **Disposal changes.** Today's contexts "need no disposal" (stateless); owning contexts must be disposed, so
  `DocumentContext : IAsyncDisposable` disposes its session iff `ownsScope`. Injected scoped contexts are
  disposed by the container.
- **Because the session is the UoW, so is `DocumentContext`.** `context.Add(x)` buffers; `context.SaveChanges()`
  commits — the context matches EF's `DbContext`-is-the-unit-of-work shape exactly. No `CreateUnitOfWork()` on
  the context.
- **Coherence rule — one scope, one session.** A scoped `IDocumentSession`, an injected `DocumentContext`, and
  `store.Insert(...)` one-shots in the *same* DI scope all use the **same** underlying session, so their writes
  share one connection / transaction / scope. Implementation: the scoped context resolves the scope's
  `IDocumentSession` rather than minting its own; the factory/owning path creates a fresh one.
- **Ties §4c and §4d together.** Each `DocumentContext` subclass is bound to one store (its `[Document]` sets),
  so `AddDocumentContext<OrdersDb>("orders")` wires the context to `factory.OpenSession("orders", scope)`. The
  **context is the declarative way to name a store; the factory is the imperative way.**

### Typed context on MAUI + concurrent-flow thread-safety
On MAUI (no ambient scope), the typed context is used exactly like the raw session: **inject
`IDocumentContextFactory<T>` (singleton) and `Create()` one per unit of work** (`await using`). A
`DocumentContext` is a UoW, so it is **never made thread-safe and never shared** — safety comes from
**isolation**: each concurrent flow gets its own context. Three layers:

| Thing | Lifetime | Thread-safe | Shared |
|---|---|---|---|
| `IDocumentContextFactory<T>` | singleton | **yes** | injected everywhere |
| `DocumentContext` (a UoW) | short-lived, one flow | no | **never shared** |
| Root (pool / single SQLite connection + broadcaster + caches) | singleton | **yes** (SQLite serialized by the semaphore) | shared |

The canonical hazard — **a background job writing while the UI writes** — is solved by each creating its own
context:

```csharp
// UI thread                          // background thread (concurrent)
await using var db = factory.Create(); await using var db = factory.Create();
db.Orders.Add(order);                  db.SyncLog.Add(entry);
await db.SaveChanges();                await db.SaveChanges();
```

Two independent UoWs → no shared buffer/transaction. **Pooled** providers run them in parallel (own
connections); **SQLite** funnels both flushes through the one connection under the `SemaphoreSlim`, so they
serialize safely (one briefly waits). This is EF's Blazor/MAUI `IDbContextFactory` guidance, unchanged.

Caveats/pattern:
- A **long explicit `BeginTransaction` (§4f) on SQLite** holds the single connection for its duration, so a
  long background transaction blocks the UI's writes until commit. Keep background SQLite transactions short;
  pooled providers have no such contention.
- The ergonomic wrapper: a **singleton app service/repository** injects `IDocumentContextFactory<T>` once and
  does `await using var db = factory.Create()` inside each method — so ViewModels inject *that* service and
  never see a context or factory. One injected thing, still safe.

## 4e. The session *is* the unit of work — no separate type

**Direction (maintainer):** fold the unit of work into the session; delete the separate `UnitOfWork` type.

The session carries the pending-writes buffer directly:

- **Buffered write verbs** (from the retired `UnitOfWork`): `Add` / `AddRange` / `Update` / `Upsert` /
  `Remove` accumulate operations; **`SaveChanges()`** flushes them atomically in one transaction (with the
  existing contiguous-same-kind coalescing) and clears the buffer. `SaveChanges` may be called repeatedly —
  each call is one transaction over the ops buffered since the last (EF semantics).
- **Immediate reads** (`Get` / `Query` / `QueryStream` / `Count` / spatial / vector / full-text) execute now
  and, as today, **do not see un-flushed buffered writes** (no identity map / read-your-writes — consistent
  with the current `UnitOfWork`).
- **Immediate single writes** (`Insert` / `Update` / `Upsert` / `Remove` / `SetProperty` / `RemoveProperty`)
  live on the **root `IDocumentStore`** (thread-safe convenience). Under the hood a root write is a one-op
  session: open → `Add` → `SaveChanges` → dispose. Injecting the session gets you a UoW; injecting the store
  gets you immediate commits.
- `SuppressInterceptors()` moves from `UnitOfWork.SaveChanges(suppressInterceptors: true)` onto the session as
  a scope/flag.

**Removed outright** (`CLAUDE.md` no-cruft rule): the `UnitOfWork` type and `IDocumentStore.CreateUnitOfWork()`
/ `DocumentContext.CreateUnitOfWork()`. `OpenSession()` replaces them.

**Note the contract shift:** an injected `IDocumentSession.Insert`-style call is no longer immediate — the
session's write verbs buffer until `SaveChanges`. That is deliberate (it *is* a unit of work) and is why the
verbs are the EF-style `Add/Update/Upsert/Remove`, not `Insert`, to signal buffering at the call site. Callers
who want immediate writes inject `IDocumentStore`.

## 4f. Explicit transactions on the session (beyond `SaveChanges`)

**Direction (maintainer):** a session needs an explicit transaction that outlives a single `SaveChanges`, so
you can (a) **read-and-lock** data inside it, and (b) run **multiple `ExecuteUpdate`/`ExecuteDelete`** (the
existing set-based terminals on `IDocumentQuery<T>`) in its scope. **Only one transaction is active at a time.**
If `SaveChanges` is called, it uses the session's active transaction if set, or **creates one if not**.

This mirrors EF's `context.Database.BeginTransaction()` (`IDbContextTransaction`) — a **lifecycle/connection
scope**, not a second write-buffer type. The write buffer stays on the session (§4e); the transaction just
controls the pinned connection and the commit boundary.

```csharp
await using var tx = await session.BeginTransaction();        // opens + pins ONE connection; sets session.CurrentTransaction
var acct = await session.Get<Account>(id, LockMode.Update);   // pessimistic locking read, held for tx duration
await session.Query<Ledger>().Where(...).ExecuteUpdate(x => x.Posted, true);   // set-based, runs INSIDE tx
await session.Query<Ledger>().Where(...).ExecuteDelete();                      // another set-based op, same tx
session.Add(newEntry);                                        // buffered
await session.SaveChanges();                                  // flushes the buffer INTO tx — does NOT commit
await tx.Commit();                                            // commits, releases connection, clears CurrentTransaction
```

Semantics:
- **`BeginTransaction()` → `IDocumentTransaction : IAsyncDisposable`.** Opens the connection + a DB
  transaction, pins them, and records it as the session's `CurrentTransaction`. **Throws if one is already
  active** (one-at-a-time; no nesting — savepoints are a possible future add, out of scope here).
- **Everything on the session routes through the active transaction while it is open:** locking reads,
  `ExecuteUpdate`/`ExecuteDelete` (immediate, set-based), and the buffered verbs flushed by `SaveChanges`.
  `session.Query<T>()` terminals bind to the session's active transaction.
- **`SaveChanges` participates, does not own.** Active tx set → flush buffer into it, **no commit** (the tx
  owner commits, and `SaveChanges` may be called repeatedly within one tx). No tx → create an implicit one,
  flush, commit, release (the §4e auto path).
- **`Commit()` / `Rollback()` / dispose-without-commit (= rollback)** end the transaction, release the
  connection, and clear `CurrentTransaction`.
- **Execution-order note.** Immediate ops (locking reads, `ExecuteUpdate`/`ExecuteDelete`) run at call time;
  buffered writes flush at `SaveChanges`. All are atomic within the transaction, but they interleave by
  **execution order, not declaration order** — a buffered `Add` before an `ExecuteDelete` still executes after
  it unless you `SaveChanges` first.

**Connection & concurrency.** An explicit transaction pins one connection for its whole duration (that is where
the locks live). On **pooled** providers the tx rents its own connection — concurrent sessions' transactions run
in parallel. On **shared** providers (SQLite/DuckDB/LiteDB) the tx holds the root's single connection + its
`SemaphoreSlim(1,1)` for its duration, so it **serializes the whole store** until commit/rollback — inherent to
SQLite's single writer. Keep explicit SQLite transactions short.

**Provider tiers (`LockMode`).** Pessimistic locking is relational: SQL Server `UPDLOCK`/`HOLDLOCK`,
PostgreSQL/MySQL/MariaDB/CockroachDB `FOR UPDATE` / `FOR SHARE`; SQLite maps a locking read to a
`BEGIN IMMEDIATE`/`EXCLUSIVE` transaction (whole-DB write lock — no row granularity). Document-native and
key-partitioned stores are optimistic-only: MongoDB has multi-document transactions (via its own client
session) but different read-lock semantics; **Cosmos, DynamoDB, Azure Table** throw `NotSupportedException` for
`BeginTransaction`/`LockMode` (they offer ETag/conditional CAS instead). Note the tier in the release note.

## 5. What moves where (member-by-member sketch)

| Surface | Root (`IDocumentStore`) — immediate, singleton | Session (`IDocumentSession`) — unit of work, short-lived |
|---|---|---|
| Immediate single writes (`Insert/Update/Upsert/Remove/SetProperty/RemoveProperty`) | primary (one-op session under the hood) | — (use buffered verbs) |
| Buffered write verbs (`Add/AddRange/Update/Upsert/Remove`) + `SaveChanges` | — | **primary** (the UoW, §4e) |
| Explicit transaction (`BeginTransaction` → `IDocumentTransaction`, `LockMode` reads) | — | **session only** (one active at a time; `SaveChanges` joins it, §4f) |
| Set-based `ExecuteUpdate` / `ExecuteDelete` (on `IDocumentQuery<T>`) | one-shot (own tx) | runs in the session's active tx if open (§4f) |
| Batch (`BatchInsert/Upsert/Update/Remove`) | one-shot convenience | equivalent via `AddRange` + `SaveChanges` |
| Query (`Query<T>`, `QueryStream`, `Count`, late-bound JSON lane) | one-shot convenience | primary (immediate) |
| `SuppressInterceptors` | — | session flag |
| DI registration | root singleton + `IDocumentSessionFactory` (multi-store, absorbs `IDocumentStoreProvider`, §4c) | injectable `IDocumentSession` — scoped (ASP.NET) / factory elsewhere per §4a |
| `DocumentContext` / `DocumentSet<T>` / `IDocumentContextFactory<T>` | — | wraps a session; *is* the UoW (`Add`/`SaveChanges`); generator re-targets `Set<T>` to `.Session` (§4d) |
| Spatial / vector / full-text query families | one-shot convenience | primary (immediate) |
| `NotifyOnChange` / `SubscribeChanges` (change feed) | **root only** (process-lifetime) | — |
| `IDocumentMaintenance.ClearAll`, `IDocumentBackup`, temporal `ITemporalDocumentStore` | root (may open sessions internally) | temporal reads may take a session |
| Interceptor registry, options, telemetry sources | root | reads them |

### 5.1 `IDocumentStore` (root) — ≈ today's interface, minus `CreateUnitOfWork`, plus the factory

The root keeps **today's full `IDocumentStore` member surface** (immediate CRUD, batch, `Query<T>`,
`QueryStream`, `Count`, `GetDiff`, the late-bound JSON lane, spatial/vector/full-text families, capability
probes) so existing call sites don't churn. The **delta** is small:

```csharp
public interface IDocumentStore : IDocumentSessionFactory,          // OpenSession / GetStore (§4b/§4c)
                                  ITemporalDocumentStore, IObservableDocumentStore,
                                  IChangeFeedDocumentStore, IDocumentMaintenance, IDisposable
{
    // + everything on today's IDocumentStore EXCEPT CreateUnitOfWork (removed — §4e).
    //   Immediate writes (Insert/Update/Upsert/SetProperty/RemoveProperty/Remove/Clear/BatchInsert
    //   + JSON-lane writes) each run as a one-op session internally.
    //   Reads (Get/Query/QueryStream/Count/GetDiff + spatial/vector/FTS) unchanged.
    //   NotifyOnChange/SubscribeChanges, ClearAll, Backup, temporal stay here (root-lifetime).

    // SuppressInterceptors moves to the session; the root no longer exposes CreateUnitOfWork().
}
```

### 5.2 `IDocumentSession` — the unit of work

```csharp
public interface IDocumentSession : IAsyncDisposable
{
    // ── Identity ─────────────────────────────────────────────────────────
    /// The session's DI scope (interceptors read this — replaces the AsyncLocal).
    IServiceProvider Services { get; }
    /// The owning root, for the root-only surface (change feed, maintenance, backup).
    /// NOTE: writes via .Store are immediate and do NOT join this session's transaction.
    IDocumentStore Store { get; }

    // ── Buffered unit-of-work writes (from the retired UnitOfWork; all buffer until SaveChanges) ──
    IDocumentSession Add<T>(T document, JsonTypeInfo<T>? typeInfo = null) where T : class;
    IDocumentSession AddRange<T>(IEnumerable<T> documents, JsonTypeInfo<T>? typeInfo = null) where T : class;
    IDocumentSession Update<T>(T document, JsonTypeInfo<T>? typeInfo = null) where T : class;
    IDocumentSession Upsert<T>(T patch, JsonTypeInfo<T>? typeInfo = null) where T : class;
    IDocumentSession Remove<T>(object id) where T : class;
    int  PendingCount { get; }
    void ClearPending();
    Task SaveChanges(CancellationToken ct = default);
    Task SaveChanges(bool suppressInterceptors, CancellationToken ct = default);

    // ── Explicit transaction (§4f) — one active at a time ────────────────
    IDocumentTransaction? CurrentTransaction { get; }
    Task<IDocumentTransaction> BeginTransaction(CancellationToken ct = default);
    Task<IDocumentTransaction> BeginTransaction(System.Data.IsolationLevel isolation, CancellationToken ct = default);

    // ── Immediate reads (see committed data, not the un-flushed buffer) ──
    Task<T?> Get<T>(object id, JsonTypeInfo<T>? typeInfo = null, CancellationToken ct = default) where T : class;
    Task<T?> Get<T>(object id, LockMode lockMode, JsonTypeInfo<T>? typeInfo = null, CancellationToken ct = default) where T : class;
    Task<JsonPatchDocument<T>?> GetDiff<T>(object id, T modified, JsonTypeInfo<T>? typeInfo = null, CancellationToken ct = default) where T : class;

    /// Fluent query. Terminals (ToList/ExecuteUpdate/ExecuteDelete/…) run on the session's
    /// CurrentTransaction when one is open, else in their own auto-committed transaction.
    IDocumentQuery<T> Query<T>(JsonTypeInfo<T>? typeInfo = null) where T : class;
    IDocumentQuery<T> Query<T>(LockMode lockMode, JsonTypeInfo<T>? typeInfo = null) where T : class;
    Task<IReadOnlyList<T>> Query<T>(string whereClause, JsonTypeInfo<T>? typeInfo = null, object? parameters = null, CancellationToken ct = default) where T : class;
    IAsyncEnumerable<T> QueryStream<T>(string whereClause, JsonTypeInfo<T>? typeInfo = null, object? parameters = null, CancellationToken ct = default) where T : class;
    Task<int> Count<T>(string? whereClause = null, object? parameters = null, CancellationToken ct = default) where T : class;

    //   + late-bound JSON-lane reads: Get(Type,…), Query(Type,…), QueryStream(Type,…)
    //   + spatial / vector / full-text read families (same signatures as the root) — all immediate,
    //     and bound to CurrentTransaction when open.

    // ── Interceptor control (was UnitOfWork/store) ───────────────────────
    IDisposable SuppressInterceptors();
}
```

### 5.3 `IDocumentTransaction` + `LockMode`

```csharp
public interface IDocumentTransaction : IAsyncDisposable   // dispose without Commit == Rollback
{
    bool IsActive { get; }
    Task Commit(CancellationToken ct = default);
    Task Rollback(CancellationToken ct = default);
}

/// Pessimistic locking hint for reads inside a transaction. Relational only; document-native/
/// key-partitioned providers throw NotSupportedException for anything but None (§4f).
public enum LockMode
{
    None = 0,   // no lock hint (default)
    Update,     // exclusive: FOR UPDATE / UPDLOCK,HOLDLOCK / SQLite BEGIN IMMEDIATE|EXCLUSIVE
    Share,      // shared:    FOR SHARE  / (SQLite: same as Update — whole-DB)
}
```

### 5.4 `IDocumentSessionFactory` (multi-store entry point, §4b/§4c)

```csharp
public interface IDocumentSessionFactory
{
    IDocumentSession OpenSession();                                       // default store, fresh owned scope
    IDocumentSession OpenSession(string storeName);                       // named store (§4c)
    IDocumentSession OpenSession(IServiceProvider scope);                 // bind caller-owned scope (not disposed)
    IDocumentSession OpenSession(string storeName, IServiceProvider scope);
    IDocumentStore   GetStore(string storeName = "default");              // root-only surface by name; absorbs IDocumentStoreProvider
}
```

### 5.5 `DocumentContext` + `DocumentSet<T>` + `IDocumentContextFactory<T>`

```csharp
public abstract class DocumentContext : IAsyncDisposable
{
    protected DocumentContext(IDocumentSession session);   // generated ctors pass the resolved/created session

    public IDocumentSession Session { get; }
    public IDocumentStore   Store => this.Session.Store;

    // Unit-of-work surface forwards to the session (the context IS the UoW, §4e):
    public Task SaveChanges(CancellationToken ct = default) => this.Session.SaveChanges(ct);
    public Task<IDocumentTransaction> BeginTransaction(CancellationToken ct = default) => this.Session.BeginTransaction(ct);

    public ValueTask DisposeAsync();   // disposes Session iff this context owns it (factory-created); no-op for injected

    protected DocumentSet<T> Set<T>(JsonTypeInfo<T>? typeInfo = null) where T : class
        => new(this.Session, typeInfo, this);              // generator emits one Set<T> property per [Document]
}

public sealed class DocumentSet<T> where T : class
{
    // Buffered (forward to session, flush on context/session SaveChanges):
    public DocumentSet<T> Add(T document);
    public DocumentSet<T> AddRange(IEnumerable<T> documents);
    public DocumentSet<T> Update(T document);
    public DocumentSet<T> Upsert(T patch);
    public DocumentSet<T> Remove(object id);
    // Immediate reads:
    public Task<T?> Get(object id, CancellationToken ct = default);
    public IDocumentQuery<T> Query();
    public Task<int> Count(CancellationToken ct = default);
}

public interface IDocumentContextFactory<out TContext> where TContext : DocumentContext
{
    TContext Create();                          // owns a fresh session + child scope — await using
    TContext Create(IServiceProvider scope);    // bind a caller-owned scope
}
```

**Removed types/members:** `UnitOfWork` (class), `IDocumentStore.CreateUnitOfWork()`,
`DocumentContext.CreateUnitOfWork()`, `IDocumentStoreProvider` (folded into `IDocumentSessionFactory`),
`DocumentContext.AttachScope`. `DocumentContext.Store` changes from an `IDocumentStore` field to
`Session.Store`.

### 5.6 Registration & usage — MAUI vs ASP.NET

#### MAUI (no ambient scope → factories only)

```csharp
// MauiProgram.cs
var builder = MauiApp.CreateBuilder();

builder.Services.AddDocumentStore(o =>          // singleton IDocumentStore + IDocumentSessionFactory
    o.DatabaseProvider = new SqliteDatabaseProvider(
        $"Data Source={Path.Combine(FileSystem.AppDataDirectory, "app.db")}"));

builder.Services.AddAppDbFactory(o =>           // generated from [Document]-annotated AppDb:
    o.DatabaseProvider = new SqliteDatabaseProvider(                 // singleton IDocumentContextFactory<AppDb>
        $"Data Source={Path.Combine(FileSystem.AppDataDirectory, "app.db")}"));
// NOTE: no AddScopedDocumentSession() — a UoW can't be an app-lifetime singleton (§4a-MAUI).
```

```csharp
// A) Immediate CRUD — inject the singleton store (thread-safe; one managed SQLite connection)
public class CustomerService(IDocumentStore store)
{
    public Task Save(Customer c)      => store.Upsert(c);
    public Task<Customer?> Load(Guid id) => store.Get<Customer>(id);
}

// B) Unit of work — inject the session factory, one session per unit (await using)
public class CheckoutService(IDocumentSessionFactory factory)
{
    public async Task Place(Order order, Payment payment)
    {
        await using var s = factory.OpenSession();
        s.Add(order);
        s.Add(payment);
        await s.SaveChanges();                  // both committed atomically
    }
}

// C) Typed context — inject the context factory, create one per unit of work
public class OrdersViewModel(IDocumentContextFactory<AppDb> dbf)
{
    public async Task Archive(Guid id)
    {
        await using var db = dbf.Create();
        var o = await db.Orders.Get(id);
        o.Archived = true;
        db.Orders.Update(o);                    // buffered
        await db.SaveChanges();
    }
}

// D) Explicit transaction + pessimistic lock (e.g. decrement stock safely)
public class StockService(IDocumentSessionFactory factory)
{
    public async Task Reserve(string sku)
    {
        await using var s  = factory.OpenSession();
        await using var tx = await s.BeginTransaction();
        var item = await s.Get<Inventory>(sku, LockMode.Update);   // row locked for tx duration
        item.Qty -= 1;
        s.Update(item);
        await s.SaveChanges();                  // flush into tx (no commit)
        await tx.Commit();                      // keep SQLite transactions short (§4f)
    }
}

// E) Concurrent flows are safe because each Create()s its own context (§4d)
//    UI thread and a background job never share a context / session / buffer.
```

The "one injected thing" pattern — a singleton repository owns the factory so ViewModels inject the repo:

```csharp
public class OrderRepository(IDocumentContextFactory<AppDb> dbf)          // singleton
{
    public async Task<IReadOnlyList<Order>> Recent()
    {
        await using var db = dbf.Create();
        return await db.Orders.Query().OrderByDescending(x => x.CreatedAt).Paginate(0, 20).ToList();
    }
}
```

#### ASP.NET Core (request scope → injectable session/context)

```csharp
// Program.cs — raw session
builder.Services
    .AddDocumentStore(o => o.DatabaseProvider = new PostgreSqlDatabaseProvider(cs))
    .AddScopedDocumentSession();                 // request-scoped IDocumentSession

// …or the typed context (scoped context + scoped session in one call)
builder.Services.AddAppDb(o => o.DatabaseProvider = new PostgreSqlDatabaseProvider(cs));
```

```csharp
// A) Minimal API — inject the request-scoped session
app.MapPost("/orders", async (Order order, IDocumentSession session) =>
{
    session.Add(order);
    await session.SaveChanges();                 // committed within the request
    return Results.Created($"/orders/{order.Id}", order);
});

app.MapGet("/orders/{id:guid}", async (Guid id, IDocumentSession session) =>
    await session.Get<Order>(id) is { } o ? Results.Ok(o) : Results.NotFound());

// B) Controller with the typed context (scoped)
public class OrdersController(AppDb db) : ControllerBase
{
    [HttpPost]
    public async Task<IActionResult> Create(Order order)
    {
        db.Orders.Add(order);
        await db.SaveChanges();
        return CreatedAtAction(nameof(Get), new { order.Id }, order);
    }
}

// C) Transaction with locking reads across two writes
app.MapPost("/transfer", async (TransferReq req, IDocumentSession session) =>
{
    await using var tx = await session.BeginTransaction();
    var from = await session.Get<Account>(req.From, LockMode.Update);
    var to   = await session.Get<Account>(req.To,   LockMode.Update);
    from.Balance -= req.Amount; to.Balance += req.Amount;
    session.Update(from); session.Update(to);
    await session.SaveChanges();                 // flush into tx
    await tx.Commit();
    return Results.Ok();
});

// D) Background worker — NO request scope → use the factory (not the scoped session)
public class ExpiryWorker(IDocumentSessionFactory factory) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            await using var s = factory.OpenSession();
            await s.Query<Token>().Where(t => t.ExpiresAt < DateTime.UtcNow).ExecuteDelete(ct);
            await Task.Delay(TimeSpan.FromMinutes(5), ct);
        }
    }
}
```

**The rule of thumb across both:** inject `IDocumentStore` for immediate one-offs (both hosts); inject the
request-scoped `IDocumentSession`/`AppDb` in ASP.NET request handlers; use `IDocumentSessionFactory` /
`IDocumentContextFactory<T>` anywhere there is no scope (all of MAUI, plus ASP.NET background/singleton code).

## 6. Tensions to resolve (honest list)

1. **Change feeds / `NotifyOnChange` stay on the root.** They outlive any session, so the split genuinely
   produces *two* interfaces, not one moved wholesale. Native feeds (`ChangeFeedSubscription`, Postgres
   `LISTEN`, SqlServer `SqlDependency`, DynamoDB Streams) each own a dedicated long-lived connection *by
   design* and must remain outside any per-session pooling.
2. **Table-init cache & shared-connection identity** move to the root (shared across sessions) — fine, the
   cache is already a `ConcurrentDictionary<string, Lazy<Task>>`.
3. **Orleans assumes a singleton store** (`DocumentDbGrainStorage.cs:39` builds one store in its ctor and
   reuses it for every grain). Under the split the grain storage holds the **root** and opens a session per
   grain operation — which is strictly better than today (no `RunUnitImpl` fallback-scope per write) and, in
   pooled mode, lets grain writes fan out instead of serializing.
4. **Migration cost is wide.** 25+ provider packages implement `IDocumentStore` directly. Splitting the
   interface is mechanical-but-broad; the core `DocumentStore` / `TransactionalDocumentStore` pair already
   models both halves, so providers mostly need their per-op methods rehomed onto a session type. Do it with
   one subagent per provider — **but** heed the git-stash-in-shared-worktree hazard from the instrumentation
   cut (see `[[feedback_parallel_agent_git]]`); use worktrees or serialize. Also in scope: the
   **source generator** (`src/Shiny.DocumentDb.Generators`) re-targets `Set<T>` to `.Session` and emits the
   new context/factory registrations (§4d); **`IDocumentStoreProvider` is removed** (folded into the factory,
   §4c); and the extension packages that inject the default un-keyed `IDocumentStore` (AppDataSync, AI, OData,
   seeding) must be checked — they keep working via the root, but any that assumed keyed-store resolution move
   to the factory. The **`UnitOfWork` type and `CreateUnitOfWork()` are removed** (folded into the session,
   §4e) — every call site (`store.CreateUnitOfWork()` / `context.CreateUnitOfWork()` in tests, samples, docs,
   skill) migrates to `OpenSession()` + `Add`/`SaveChanges`.
5. **Two lock-free-read data races surfaced by the survey** should be fixed in the same pass while we're in
   this code: DynamoDB `streamsClient` lazy init without a lock (`DynamoDbDocumentStore.ChangeFeed.cs:14`),
   and Cosmos `initializedContainers` plain-`HashSet` read outside its lock (`CosmosDbDocumentStore.cs:175`).
   SqlServer `SqlDependency.Start/Stop` is process-global keyed by connection string — a shared subscription
   torn down by the first disposer; document or reference-count it.

## 7. RESOLVED — shared-mode keeps one connection; the session is a context over it

In shared mode the provider mandates **one** connection (SQLite locks the whole DB on write). **Decision
(maintainer):** keep the single connection — **do not** give each session its own connection. This is option
**(A)** from the original fork, and §4a is how it reconciles with an injectable session:

- The root owns the single shared connection and its `SemaphoreSlim(1,1)`.
- A session is a **context** (DI scope + operation context), not a connection owner. Autocommit ops borrow the
  one connection per-op under the semaphore; a transaction holds it for the transaction's duration. Concurrent
  sessions/threads serialize — **no throughput change, zero behavioral surprise**, and it honors SQLite's
  one-connection rule by construction regardless of how many sessions are alive.
- MAUI reality (one app-lifetime singleton session + one connection) falls straight out of this: the session
  is that singleton, and the connection is the root's long-lived shared one — i.e. *today's* `sharedConnection`
  behavior, now with a clean injectable context instead of `AsyncLocal` plumbing.

**Deferred, not chosen:** the WAL-multi-connection variant (each session its own connection for real read
concurrency) remains a possible future opt-in (`DocumentStoreOptions.SharedConnectionPerSession` / WAL-pool),
kept out of this cut so the breaking change stays about *structure*, not *runtime concurrency semantics*. It
would need per-connection `PRAGMA`/UDF registration and a rethink of the shared-connection init flag.

## 8. Options considered (for the split itself)

1. **Two-level root/session split + one-shot convenience on the root.** *(CHOSEN.)* Removes every ambient
   trap, keeps simple-call ergonomics, matches the internal `TransactionalDocumentStore` model.
2. **Explicit sessions only (pure `DbContext` model).** CRUD lives *only* on the session; every caller opens
   one. Cleanest conceptual model but maximal ceremony and the largest call-site churn across samples/docs.
   Rejected for ergonomics.
3. **Targeted trap removal, no API change.** Collapse the DI-scope `AsyncLocal` into an explicit context
   passed through internal calls, leave the public singleton as-is. Smallest change, but leaves the
   singleton-is-the-operation-unit design (and the shared-mode ceiling) in place — treats symptoms, not cause.
   Rejected as the primary path; some of its internal plumbing is reused by option 1.

## 9. The four artifacts (per `CLAUDE.md`) when this is built

1. **Code + tests** — core split, `TransactionalDocumentStore` promoted to public `IDocumentSession`, all 25+
   providers rehomed, Orleans holds the root + opens sessions. Run
   `tests/Shiny.DocumentDb.Tests` + `tests/Shiny.DocumentDb.Orleans.Tests`.
2. **Docs site** — rewrite the lifetime/threading guidance; new `sessions.mdx` (or expand `crud.mdx`);
   `orleans.mdx` note; a `type="breaking"` release note against the `12.0` (or current `version.json`) section.
3. **Skill** (`skills/shiny-documentdb/SKILL.md`) — update default guidance to show `OpenSession()` for
   grouped/transactional writes and note the one-shot convenience for simple calls; refresh `triggers:`.
4. **readme.md** — update the lifetime/threading section and feature list.

## 10. Related memory

- `[[feedback_parallel_agent_git]]` — the parallel-provider migration must avoid `git stash`/`reset` in a
  shared working tree; use worktrees.
- The instrumentation cut (`docs/plans/embedded-instrumentation.md`) is the precedent for a wide,
  all-providers, no-shim breaking change and its `AsyncLocal` re-entrancy guard is one of the traps this RFC
  proposes to convert into a session field.

## 8. Spike result — core + SQLite (DONE, green)

An **additive** vertical slice proved the boundary against the real code without removing the existing surface
(so the whole suite still builds and the old paths are untouched). Files added to `src/Shiny.DocumentDb/`:
`LockMode.cs`, `IDocumentTransaction.cs`, `IDocumentSession.cs`, `IDocumentSessionFactory.cs`,
`DocumentSession.cs`, `DocumentTransaction.cs`, `DocumentSessionFactory.cs`, and `DocumentStore.Session.cs`
(a `partial` holding the internal `BeginExplicitUnitAsync` seam + `ExplicitUnit`). DI: `AddDocumentStore` now
also registers a singleton `IDocumentSessionFactory`; new `AddScopedDocumentSession()`. Tests:
`tests/…/SessionSpikeTests.cs` — **9/9 pass**; BatchWrite/Suppress/ScopedInterceptor suites still green
(the `UnitOfWork` refactor and DI change are behavior-preserving).

### How it reused the existing machinery (low-risk path for the full cut)
- **Buffer + coalescing:** `UnitOfWork`'s flush loop was extracted to `internal Task FlushInto(IDocumentStore tx, ct)`.
  The session holds a `UnitOfWork` and either `FlushInto(activeTx.Store)` (join, no commit) or `buffer.SaveChanges()`
  (auto path → existing `RunUnitAsync`). Zero duplication.
- **Explicit tx:** `BeginExplicitUnitAsync` reuses the shared-connection-under-semaphore / pooled-connection
  acquisition and the existing `TransactionalDocumentStore` (a full transaction-bound `IDocumentStore`), but hands
  Commit/Rollback to the caller instead of committing inline like `RunUnitImpl`.
- **Scope flow:** the session pushes `Services` via the existing `DocumentOperationScope.EnterServices` for the
  flush, so scoped interceptors resolve from the session's scope — the same mechanism, now fed explicitly.

### Findings that must shape the full migration
1. **DDL must be committed OUTSIDE the explicit transaction.** First cut created the table lazily *inside* the
   transaction; a rollback then dropped a table the process-wide `tableInitTasks` cache still believed existed
   (`no such table` on the next read). Fix in the spike: initialize `options.TableName` on the connection
   *before* `BeginTransactionAsync`. **Full-cut TODO:** a transaction can touch tables other than the default —
   pre-initialize *every* table the unit may write (or run all `Ensure*Table` DDL on a separate autocommit
   connection) so no `CREATE`/index DDL ever rides the user transaction. This generalizes to temporal/spatial/
   vector sidecar tables too.
2. **`IDocumentSession` is `IAsyncDisposable`-only** → scoped registration requires `CreateAsyncScope()` /
   `await using`; a sync `scope.Dispose()` throws. ASP.NET disposes request scopes async, so this is fine there,
   but the doc/skill samples must use `await using`.
3. **Shared-mode reentrancy caveat confirmed:** while an explicit SQLite transaction holds the semaphore, calling
   a root immediate op (`session.Store.Insert`) would deadlock on the same semaphore. Normal transactional flow
   (session buffered writes + `SaveChanges` flushing into the tx; reads via the tx store) does not — it never
   re-acquires. Documented as the "`.Store` ops don't join the tx" caveat (§5.2).
4. **`LockMode` is plumbed but only the tx boundary is enforced** (SQLite whole-DB lock). Emitting
   `FOR UPDATE`/`UPDLOCK,HOLDLOCK` SQL is per-provider work in the full cut; the enum + "locking read requires an
   active transaction" guard are in place.

### Not yet exercised (deferred to the full cut)
Named/multi-store factory overloads (throw), `DocumentContext` re-targeting to a session, the root's convenience
one-shot re-plumbing, and every non-SQLite provider. The spike deliberately left the old `UnitOfWork`/
`CreateUnitOfWork` public surface in place; the full cut removes them per §4e/§6.
