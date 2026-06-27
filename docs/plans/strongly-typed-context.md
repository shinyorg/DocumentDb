# Plan: Strongly-typed `DocumentContext` (EF-Core-style typed facade, source-generated)

**Status:** Designed, not started.
surface ships **in core** (`Shiny.DocumentDb`); only the source generator is a separate opt-in analyzer
package (`Shiny.DocumentDb.Generators`). Can slip to a later minor without touching existing code.

> Self-contained build spec. The implementing agent does not have the design conversation —
> everything needed is here. Read `CLAUDE.md` (repo root) for the four-artifact rule (code+tests,
> docs site, skill, readme) before considering any commit "done".

Branch off `v9` (the current working branch) before starting.

---

## Goal

Offer an **optional, EF-Core-style typed front-end** over the existing `IDocumentStore`, so callers work
against a discoverable model class instead of remembering to type `<T>` at every call site:

```csharp
// today — store-first
var adults = await store.Query<User>().Where(x => x.Age > 18).ToList();
await store.Insert(user);

// addon — model-first
var adults = await ctx.Users.Where(x => x.Age > 18).ToList();
ctx.Users.Add(user);
await ctx.SaveChanges();
```

The library is already fully generic (`store.Query<T>()`, `store.Insert(user)`, …), so this is **not** a
capability feature — it is an **ergonomics + discoverability** layer:

1. **Model-as-a-class** — a context exposes `DocumentSet<T>` properties; the set of aggregates becomes
   discoverable (IntelliSense) and enumerable instead of a `<T>` you re-type per call.
2. **`JsonTypeInfo<T>` becomes invisible.** Today every `IDocumentStore` method takes an optional
   `JsonTypeInfo<T>?` the caller must remember to pass for AOT. A `DocumentSet<T>` holds the right
   `JsonTypeInfo<T>` once and threads it into every call automatically. **This is the headline win the
   raw store can't give.**
3. **Declarative configuration** — table name / id / filters expressed as attributes on the context, lowered
   into the existing `DocumentStoreOptions`.

Everything is **compile-time, source-generated, AOT-clean** — no reflection-based model discovery.

---

## Decisions locked (from design conversation)

- **Scope = Tier 1 facade only (typed sets), NO change tracking.** `SaveChanges` flushes the existing
  `UnitOfWork`; it does **not** do identity-map snapshot diffing. Change tracking (identity map + snapshot
  via `GetDiff`) is a possible **future** Tier 2, explicitly out of scope here. Navigation properties /
  `Include` / joins are **never** in scope — a document store embeds, it doesn't join.
- **The context class *is* a `System.Text.Json` `JsonSerializerContext`.** This is the crux. The user
  declares one partial class that derives from `JsonSerializerContext`, decorated with the canonical
  `[JsonSerializable(typeof(T))]` type list. STJ's generator produces the `JsonTypeInfo<T>` metadata and
  `Default`/instance accessors; **our** generator independently reads the same `[JsonSerializable]`
  attributes and emits a second partial adding the `DocumentSet<T>` properties. Two generators, same
  source, no chaining.
- **`DocumentContext` is an interface (`IDocumentContext`), not a base class.** Because the class must
  derive from `JsonSerializerContext` for STJ's generator to fire, our behavior is grafted on via an
  interface + generated members + a ctor that takes `IDocumentStore`. There is no spare base-class slot.
- **One canonical type list, on `[JsonSerializable]`.** `[Document(...)]` is **override-only** (table, id,
  filters) and never re-enumerates types. Defaults cover the 90% (table = type name via the store's
  existing `TypeNameResolution`, id by the store's existing convention).
- **No reinvention of STJ metadata.** We never hand-emit `JsonTypeInfo`; we only reference what STJ
  already generated off the context instance (`this.User`, etc.). Reimplementing STJ's metadata
  (`[JsonPropertyName]`, ctor matching, `required`/`init`, polymorphism, collections, nullability,
  per-release behavior changes) is a maintenance sinkhole and is explicitly rejected.
- **Runtime lives in core (`Shiny.DocumentDb`), not a separate package.** `IDocumentContext` and
  `DocumentSet<T>` are ~3 tiny types that reference only `IDocumentStore` / `UnitOfWork` /
  `IDocumentQuery<T>`, all already in core. A standalone `Shiny.DocumentDb.Context` package would add a
  package boundary and a version-lockstep dependency for no benefit. Only `Shiny.DocumentDb.Generators`
  (the Roslyn analyzer) is a separate package.
- **Write-method naming convention (locked).** Two families, no overload collision by construction:
  - **Buffered** (`void`, EF-style, enqueue onto the context `UnitOfWork`, flushed on `SaveChanges`):
    `Add(T)`, `AddRange(IEnumerable<T>)`, `Update(T)`, `Remove(T)`.
  - **Immediate** (`Task`, store-style, write now): `Insert(T)`, `Upsert(T)`, `Remove(object id)`.
  - There is **no immediate `Update`** on the set — that's the one verb that would collide with the
    buffered `void Update(T)` (can't overload on return type). Immediate updates use buffered `Update` +
    `SaveChanges`, or drop to `ctx.Store.Update(doc)`. `Remove(T)` (buffered) and `Remove(object id)`
    (immediate) are distinct overloads because `T : class ≠ object`. Net: every verb has exactly one
    signature; the EF mental model (`Add` then `SaveChanges`) and the immediate model (`Insert` now) both
    read cleanly.

### Alternatives considered and rejected

- *Type list on our own `[Document(typeof(T))]` attribute, driving everything from the document context.*
  Reads beautifully (one attribute set, like `JsonSerializerContext` itself) — **but** source generators
  don't chain, so STJ's generator never sees those types and we'd own `JsonTypeInfo<T>` generation end to
  end (see above sinkhole). Rejected. The "inherit STJ, read its `[JsonSerializable]`" design gives the
  same single-list ergonomics without owning metadata.
- *Two separate classes (a `JsonSerializerContext` **and** a sibling `DocumentContext` that points at it
  via `[DocumentContext(typeof(AppJsonContext))]`).* Works and is composable, but it's two classes and an
  indirection. The single-class "context *is* the JSON context" design is strictly nicer; keep the
  two-class form only as a documented fallback for users who already have a shared `JsonSerializerContext`
  they can't reshape.
- *Reflection-discovered `DocumentSet<T>` properties (real EF style).* Breaks the AOT-first posture
  (`IsAotCompatible`, source-gen JSON throughout) — the exact reason EF Core isn't AOT-friendly. Rejected.
- *Change tracking / `SaveChanges` with snapshot diffing in v1.* Real cost (identity map, detached-entity
  rules, the EF gotchas) for a document store where most writes are explicit upserts. Deferred to a
  possible Tier 2; v1 `SaveChanges` is just a `UnitOfWork` flush.
- *Navigation properties / `Include` / lazy load.* Philosophically wrong for a document store. Never.

---

## Architecture facts the implementer needs

- **`IDocumentStore`** (`src/Shiny.DocumentDb/IDocumentStore.cs`) is the entire backing surface. Every
  `DocumentSet<T>` / `DocumentContext` member is a thin forward to it. No provider needs to change — this
  feature sits *above* the store contract and works against all 10 providers identically.
- **`IDocumentQuery<T>`** (`src/Shiny.DocumentDb/IDocumentQuery.cs`) is the fluent query builder
  (`Where`/`OrderBy`/`Paginate`/`Select`/`ToList`/`Count`/`ExecuteUpdate`/…). `DocumentSet<T>` query
  methods delegate straight to `store.Query<T>(jsonTypeInfo)` and return `IDocumentQuery<T>` — do **not**
  wrap or re-expose it; just hand it back so the full existing query surface is available for free.
- **`UnitOfWork`** (`store.CreateUnitOfWork()`) buffers `Add`/`AddRange`/`Update`/`Upsert`/`Remove` and
  applies atomically on `SaveChanges`. This is the entire mechanism behind context `SaveChanges` in Tier 1.
- **`DocumentStoreOptions`** (`src/Shiny.DocumentDb/DocumentStoreOptions.cs`) is where all model config
  already lives: `MapTypeToTable<T>`, `MapIdProperty<T>`, `AddQueryFilter<T>`, `MapVersionProperty<T>`,
  `UseGuidV7Ids`, etc. The generator's job for `[Document(...)]` is to **lower attributes into calls on
  this options object** — not to invent a parallel config system.
- **STJ source-gen behavior to rely on:** a `partial class X : JsonSerializerContext` decorated with
  `[JsonSerializable(typeof(User))]` gets, from STJ's generator, both `X.Default` and an instance property
  `JsonTypeInfo<User> User { get; }`. Our generated `DocumentSet<User>` pulls its `JsonTypeInfo` from
  `this.User`. Confirm the instance-property accessor name = the type's simple name (STJ default); if a
  `[JsonSerializable(TypeInfoPropertyName = "...")]` override is present, honor it.
- **Generators in the repo:** confirm whether a source-generator project already exists (search
  `src/**/*.Generators*.csproj` / `[Generator]`). If yes, add to it; if not, create
  `src/Shiny.DocumentDb.Generators/` as an `IIncrementalGenerator`, packaged as an analyzer
  (`IncludeBuildOutput=false`, `analyzers/dotnet/cs` packaging) per `Directory.Build.props` conventions.

---

## Part 1 — runtime surface

A tiny hand-written runtime (in core `Shiny.DocumentDb`, or a `Shiny.DocumentDb.Context` package — prefer
**core**, it's a few small types with no new dependencies).

### `IDocumentContext`

```csharp
public interface IDocumentContext
{
    IDocumentStore Store { get; }
    Task SaveChanges(CancellationToken ct = default);   // Tier 1: flush the active UnitOfWork
}
```

### `DocumentSet<T>`

A thin, allocation-light struct/class bound to `(IDocumentStore store, JsonTypeInfo<T> typeInfo)`. Every
member forwards to the store **with `typeInfo` pre-applied** so callers never pass it:

```csharp
public sealed class DocumentSet<T> where T : class
{
    readonly IDocumentStore store;
    readonly JsonTypeInfo<T>? typeInfo;
    internal DocumentSet(IDocumentStore store, JsonTypeInfo<T>? typeInfo) { ... }

    // queries — delegate to store.Query<T>(typeInfo); return the existing IDocumentQuery<T> as-is
    public IDocumentQuery<T> Query()                                  => store.Query(typeInfo);
    public IDocumentQuery<T> Where(Expression<Func<T,bool>> p)        => store.Query(typeInfo).Where(p);
    public Task<T?> Get(object id, CancellationToken ct = default)    => store.Get(id, typeInfo, ct);
    public Task<IReadOnlyList<T>> ToList(CancellationToken ct = default) => store.Query(typeInfo).ToList(ct);

    // immediate writes (Task, store-style — write now, bypass the UoW)
    public Task Insert(T doc, CancellationToken ct = default)         => store.Insert(doc, typeInfo, ct);
    public Task Upsert(T doc, CancellationToken ct = default)         => store.Upsert(doc, typeInfo, ct);
    public Task<bool> Remove(object id, CancellationToken ct = default) => store.Remove<T>(id, ct);

    // buffered mutations (void, EF-style — enqueue onto the context UnitOfWork, applied on SaveChanges)
    public void Add(T doc);
    public void AddRange(IEnumerable<T> docs);
    public void Update(T doc);            // no immediate Task Update — see naming convention
    public void Remove(T doc);            // distinct from Remove(object id): T : class ≠ object
}
```

**`Add`/`AddRange`/`Update`/`Remove(T)`** push onto the context's single shared `UnitOfWork`;
`IDocumentContext.SaveChanges` calls `unitOfWork.SaveChanges(ct)`. The immediate `Insert`/`Upsert`/
`Remove(id)` forms bypass the UoW and write now (mirrors having both `store.Insert` and `UnitOfWork.Add`
today). Both modes coexist cleanly under the locked naming convention (see Decisions) — the EF model
(`Add` + `SaveChanges`) and the immediate model (`Insert` now) each read unambiguously.

---

## Part 2 — the source generator

**New (or existing) project:** `src/Shiny.DocumentDb.Generators/` — an `IIncrementalGenerator`.

### Input it matches

A user-written partial class deriving from `JsonSerializerContext` that **also** carries our marker
attribute (e.g. `[GenerateDocumentSets]`) **or** implements `IDocumentContext` — pick the marker form, it's
explicit and cheap to match:

```csharp
[JsonSerializable(typeof(User))]
[JsonSerializable(typeof(Order))]
[Document(typeof(User),  Table = "users", Id = nameof(User.Email))]   // optional, override-only
[GenerateDocumentSets]
public partial class AppContext : JsonSerializerContext, IDocumentContext
{
    public AppContext(IDocumentStore store) : base() => this.Store = store;   // or let the gen emit this
}
```

### Output it emits (a second partial of the same class)

For each `[JsonSerializable(typeof(TX))]` on the class:

```csharp
partial class AppContext
{
    public IDocumentStore Store { get; }                       // if not user-provided
    UnitOfWork? __uow;
    UnitOfWork Uow => __uow ??= this.Store.CreateUnitOfWork();
    public Task SaveChanges(CancellationToken ct = default) => this.Uow.SaveChanges(ct);

    DocumentSet<User>?  __users;
    public DocumentSet<User>  Users  => __users  ??= new(this.Store, this.User,  this.Uow);   // this.User = STJ JsonTypeInfo<User>
    DocumentSet<Order>? __orders;
    public DocumentSet<Order> Orders => __orders ??= new(this.Store, this.Order, this.Uow);
}
```

- **Set property name** = pluralized type name by default (`User` → `Users`); allow
  `[Document(typeof(User), Set = "People")]` to override. Keep pluralization dumb (append `s`, handle the
  obvious `y`→`ies`); don't pull in a pluralization library — overridable attribute covers oddities.
- **`JsonTypeInfo` source** = the STJ-generated instance accessor (`this.User`). If
  `TypeInfoPropertyName` is set on `[JsonSerializable]`, use that name.
- **`[Document(...)]` lowering** → emit a generated `static void ConfigureModel(DocumentStoreOptions o)`
  (or an `IConfigureOptions`-style hook) that calls `o.MapTypeToTable<User>("users")`,
  `o.MapIdProperty<User>(x => x.Email)`, `o.AddQueryFilter<T>(...)` etc., so DI registration can apply the
  whole model in one call. This keeps attributes as sugar over the existing options API — no parallel
  config.

### DI glue

```csharp
// generated or hand-written extension
services.AddDocumentContext<AppContext>(opt => { /* provider + overrides */ });
```

Registers `AppContext` (resolving `IDocumentStore`), applies the generated `ConfigureModel`, and lets the
user add fluent overrides last (precedence: **attributes are defaults, fluent `configure` wins** — so the
90% is declarative with a clean escape hatch). Confirm how the store + `DocumentStoreOptions` are
registered today (search for the existing `Add*DocumentStore` extension per provider) and mirror it.

### Diagnostics the generator should report

- Class marked `[GenerateDocumentSets]` but **not** `partial` → error.
- Class not deriving from `JsonSerializerContext` → error (no `JsonTypeInfo` to bind to).
- `[Document(typeof(X))]` for a type with no matching `[JsonSerializable(typeof(X))]` → warning (it won't
  have a generated `JsonTypeInfo`; it's an orphan override).
- Two sets resolving to the same property name → error (ask for `Set =`).

---

## Build order

1. **Runtime** (`IDocumentContext`, `DocumentSet<T>`) in core — pure forwards to `IDocumentStore` /
   `UnitOfWork`. Unit-testable by hand-writing a context (no generator yet).
2. **Generator** — `IIncrementalGenerator` matching the marked partial, emitting sets + `SaveChanges` +
   `ConfigureModel`. Start with sets only; add `[Document]` lowering second.
3. **DI** — `AddDocumentContext<T>` + attribute→options precedence.
4. **Tests** (below). Run `dotnet test tests/Shiny.DocumentDb.Tests/...` plus the generator suite.

---

## Tests

**Runtime (`tests/Shiny.DocumentDb.Tests`)** — hand-author a context against SQLite:
- `ctx.Users.Where(...).ToList()` == `store.Query<User>().Where(...).ToList()` (same results).
- `ctx.Users.Add(a); ctx.Users.Add(b); await ctx.SaveChanges();` → both persisted atomically; nothing
  written before `SaveChanges` (UoW buffering).
- Direct `ctx.Users.Insert(x)` writes immediately (no `SaveChanges` needed).
- `JsonTypeInfo` is threaded: run with `UseReflectionFallback=false` + a source-gen context and confirm
  queries/writes work (proves the set carries `JsonTypeInfo`, AOT-clean).
- Runs against at least SQLite (relational) and one document provider (LiteDB) to prove it's store-agnostic.

**Generator (`tests/Shiny.DocumentDb.Generators.Tests`, new)** — snapshot/verify generator output:
- Two `[JsonSerializable]` types → two `DocumentSet<T>` props named correctly, each bound to the right
  `this.<Type>` `JsonTypeInfo`.
- `[Document(Set="People")]` override → property renamed.
- `[Document(Table=..., Id=...)]` → `ConfigureModel` emits the matching `MapTypeToTable`/`MapIdProperty`.
- Each diagnostic fires (non-partial, non-`JsonSerializerContext`, orphan `[Document]`, name clash).
- Compile the generated output against the runtime to prove it builds (golden-file + compile test).

---

## Gotchas

1. **No generator chaining.** STJ's generator will not see anything *we* emit. We only ever **read** the
   user's hand-written `[JsonSerializable]` attributes and **reference** the `JsonTypeInfo` STJ produced.
   Never emit `[JsonSerializable]` expecting STJ to act on it.
2. **The context can't use a base class.** It must derive from `JsonSerializerContext`, so `DocumentContext`
   is an interface, and shared behavior is generated into the partial — not inherited.
3. **`JsonTypeInfo` accessor name.** Defaults to the type's simple name; honor `TypeInfoPropertyName` on
   `[JsonSerializable]`. If a user nests types or uses generics, the accessor name may differ — read it
   from the STJ attribute rather than assuming.
4. **Single shared `UnitOfWork` per context instance.** All buffered `Add`/`Update`/`Remove` across all
   sets must enqueue onto the **same** UoW so `SaveChanges` is one atomic flush. Lazily create it; reset
   (null it) after `SaveChanges` so a context instance is reusable for a second batch. Decide and document
   context lifetime (scoped, like `DbContext`) — a context is **not** thread-safe (the UoW isn't).
5. **`Update` overload ambiguity (resolved — don't reintroduce).** There is exactly one `Update`: the
   buffered `void Update(T)`. Do **not** add an immediate `Task Update(T)` — it can't overload on return
   type and will break the build. Immediate update = buffered `Update` + `SaveChanges`, or `ctx.Store.Update`.
   `Remove(T)` (buffered) and `Remove(object id)` (immediate) are fine — distinct parameter types.
6. **Attributes are sugar, not a second config system.** `[Document(...)]` must lower into the existing
   `DocumentStoreOptions.Map*` calls. Don't fork model configuration; fluent `configure` overrides win.
7. **AOT.** The whole point is to stay source-gen/AOT-clean. No reflection-based set discovery, no
   `JsonSerializer.Serialize(object)` — the set already holds `JsonTypeInfo<T>`. Keep
   `Shiny.DocumentDb.Generators` dependency-free beyond Roslyn.
8. **Don't wrap `IDocumentQuery<T>`.** Return it as-is from set query methods so the full existing query
   surface (spatial/vector/full-text terminators, `ExecuteUpdate`, `NotifyOnChange`, …) comes for free and
   stays in sync automatically.

---

## Four-artifact sync (per CLAUDE.md — do not skip)

- **Code + tests:** runtime in core + generator project + both test suites above. Provider tier in the
  release note: **all providers** (sits above `IDocumentStore`; no provider-specific code).
- **Docs site** (`~/Desktop/dev/documentation/src/content/docs/documentdb/`): new `context.mdx` (or a
  section in an existing page) covering the `JsonSerializerContext`-derived context, `DocumentSet<T>`, the
  buffered-vs-immediate write modes, `SaveChanges`, `[Document]` overrides, and `AddDocumentContext<T>`.
  Add a `<RN type="feature">` under a `## 9.2 TBD` heading (create it if absent). Explicitly state: **no
  change tracking, no navigation/`Include`** (so it's not mistaken for full EF).
- **Skill** (`skills/shiny-documentdb/SKILL.md`): add `DocumentContext`, `IDocumentContext`,
  `DocumentSet`, `GenerateDocumentSets`, `[Document]`, `AddDocumentContext` to `triggers:`; document the
  recommended single-class pattern (derive from `JsonSerializerContext`, mark it, get typed sets) and that
  `JsonTypeInfo` is threaded automatically.
- **readme.md** (repo root): add the strongly-typed context to the feature list.
