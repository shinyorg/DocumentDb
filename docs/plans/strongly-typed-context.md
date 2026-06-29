# Plan: Strongly-typed `DocumentContext` (EF-Core-style typed facade, source-generated)

**Status:** ✅ **Shipped (v1)** on branch `v10`. Runtime (`DocumentContext`, `DocumentSet<T>`,
`DocumentAttribute`, `DocumentSerialization`) is in core `Shiny.DocumentDb`; the generator is in
`src/Shiny.DocumentDb.Generators` (analyzer package). Tests: `tests/Shiny.DocumentDb.Tests/TypedContextTests.cs`
(SQLite strict-AOT + LiteDB + DI, 17 tests) and `tests/Shiny.DocumentDb.Generators.Tests` (6 generator
unit/diagnostic tests). Docs/skill/readme synced. **Deferred:** `DocumentSerialization.Generated` (generator
owns metadata) — `DDB004` warns and falls back to `Auto` until built.

**Resolution of the gating decision:** serialization became a **per-type `[Document]` knob**
(`DocumentSerialization`) instead of a build-blocking global choice. `JsonContext` mode (user owns a
`JsonSerializerContext`) and `Reflection`/`Auto` (store resolver / reflection fallback) ship now with **no
metadata generation**; the expensive STJ-metadata reproduction (option A) is deferred behind `Generated`. The
generated sets pass `null` `JsonTypeInfo` so the store resolves from its own options — which keeps naming
aligned and is self-consistent for writes+queries (sidesteps gotcha #3 entirely).

**Target version:** `10.0` (current `version.json`) — additive, no breaking changes. The source generator is
a separate opt-in analyzer package (`Shiny.DocumentDb.Generators`).

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
await ctx.Users.Upsert(user);
```

The library is already fully generic (`store.Query<T>()`, `store.Insert(user)`, …), so this is **not** a
capability feature — it is an **ergonomics + discoverability** layer:

1. **Model-as-a-class** — a context exposes `DocumentSet<T>` properties; the set of aggregates becomes
   discoverable (IntelliSense) and enumerable instead of a `<T>` you re-type per call.
2. **`JsonTypeInfo<T>` becomes invisible.** Today every `IDocumentStore` method takes an optional
   `JsonTypeInfo<T>?` the caller must remember to pass for AOT. A `DocumentSet<T>` holds the right
   `JsonTypeInfo<T>` once and threads it into every call automatically. **This is the headline win the
   raw store can't give.**
3. **Single declarative type list + config** — document types and their table/id/filter config are declared
   **once**, as `[Document(...)]` attributes on the context, and lowered into the existing
   `DocumentStoreOptions`.

Everything is **compile-time, source-generated, AOT-clean** — no reflection-based model discovery.

---

## Decisions locked (from design conversation)

### Surface / scope
- **Tier 1 facade only — NO change tracking.** Typed sets that forward to `IDocumentStore`. No identity
  map, no snapshot diffing. Change tracking is a possible future Tier 2 (built on `GetDiff` + `UnitOfWork`),
  explicitly out of scope here. Navigation properties / `Include` / joins are **never** in scope — a
  document store embeds, it doesn't join.
- **`DocumentSet<T>` mutators are immediate (`Task`), matching `IDocumentStore` 1:1:** `Insert` / `Update` /
  `Upsert` / `Remove(object id)`, plus queries (`Query`/`Where`/`Get`/`ToList`/…). DocumentDb is
  immediate-first everywhere; the set matches. **No buffered `Add`/`SaveChanges` family on the set** —
  it can't coexist with the immediate family (the `void`/`Task` overload collision below), and the
  immediate model is the library's native idiom. Explicit transactions stay available via
  `ctx.CreateUnitOfWork()` (the existing generic `UnitOfWork`). An EF-style typed buffered batch can be a
  later add-on.
- **`DocumentContext` is a real base class** (not an interface). Because we no longer derive from
  `JsonSerializerContext` (see below), the base-class slot is free — the context inherits `Store`,
  `CreateUnitOfWork()`, and the protected `Set<T>(...)` helper.

### Serialization / source-gen ownership (the crux — corrected from an earlier wrong design)
- **Our generator owns serialization end to end. STJ's generator is NOT in the loop.** Source generators
  cannot chain (one generator never sees another's output), so we cannot emit `[JsonSerializable]` for
  STJ to consume, and we will not require the user to *also* maintain a `JsonSerializerContext`. The single
  source of truth is **`[Document(typeof(T))]` on our context**, and our generator produces the
  `JsonTypeInfo<T>` itself.
- **The generated `JsonTypeInfo<T>` MUST be metadata-mode (`JsonMetadataServices.CreateObjectInfo<T>` with
  a full `JsonPropertyInfo[]`), NOT a value-converter wrapper.** This is forced by how the store queries —
  see *Architecture facts*. The opaque converter shortcut is rejected (it breaks all property-based
  `Where`/`OrderBy`/`Select`).
- **The context installs an `IJsonTypeInfoResolver`** covering every generated type, on the
  `JsonSerializerOptions` it hands the store, so the translator's nested-type descent
  (`Options.GetTypeInfo(nestedType)`) resolves. In effect we reproduce the relevant parts of STJ's
  source-gen output (per-type metadata + a resolver) ourselves.
- **Generated JSON property names must match the store's path translator exactly:** member name transformed
  by `JsonSerializerOptions.PropertyNamingPolicy`, honoring `[JsonPropertyName]`. The store's default
  options use **CamelCase** (`DocumentStore.cs:2082`). Each generated `JsonPropertyInfo` must set both
  `Name` (the JSON key) and `AttributeProvider` (the real `PropertyInfo`) — the translator reads both.

### Packaging / naming
- **Runtime lives in core (`Shiny.DocumentDb`).** `DocumentContext` + `DocumentSet<T>` are small and
  reference only `IDocumentStore` / `UnitOfWork` / `IDocumentQuery<T>`, all already in core. Only
  `Shiny.DocumentDb.Generators` (the Roslyn analyzer) is a separate package.
- **Single type list on `[Document(...)]`.** No second list anywhere. `[Document(typeof(T))]` both declares
  the type *and* (optionally) overrides table/id/filters; bare types get store defaults (table from
  `TypeNameResolution`, id by convention).
- **Immediate-write overload safety:** there is exactly one of each verb (`Insert`/`Update`/`Upsert` =
  `Task`; `Remove(object id)` = `Task<bool>`). Do not add `void` buffered twins — `void X(T)` vs
  `Task X(T)` can't overload on return type and won't compile.

### Alternatives considered and rejected

- *Single `[Document]` list that feeds STJ's generator.* Impossible — generators don't chain; STJ never
  sees attributes/types our generator produces.
- *Context derives from `JsonSerializerContext`; user writes `[JsonSerializable]`; two generators read the
  same source (ours references STJ's `this.User`).* **Rejected by user.** It still puts STJ's generator in
  the loop and ties the model to a second attribute system; the goal is one `[Document]` list on our own
  context with no `JsonSerializerContext` dependency. (Kept only as a possible documented fallback for
  users who already maintain a shared `JsonSerializerContext`.)
- *Generate a `JsonConverter<T>` and bridge via `JsonMetadataServices.CreateValueInfo<T>`.* Easy to
  generate, but produces a `JsonTypeInfo` with **empty `.Properties`**. The store's query translator
  iterates `JsonTypeInfo.Properties` to map CLR members → JSON keys, so every property-based query would
  throw "property not found." **Rejected** — metadata-mode is mandatory.
- *Dual buffered + immediate set (`Add`/`SaveChanges` and `Insert`).* Impossible against the real
  `UnitOfWork`: `Update`/`Upsert`/`Remove` would each need a `void` (buffered) and a `Task` (immediate)
  overload with identical params. Rejected; immediate-only set.
- *Reflection-discovered `DocumentSet<T>` properties (real EF style).* Breaks the AOT-first posture.
  Rejected.
- *Change tracking / navigation / `Include`.* Out of scope / never (see above).

---

## Architecture facts the implementer needs

- **`IDocumentStore`** (`src/Shiny.DocumentDb/IDocumentStore.cs`) is the entire backing surface. Every set
  member is a thin forward to it; no provider changes — the feature works against all 10 providers.
- **The query translator drives the metadata requirement.** `DocumentQueryExtensions.ResolveJsonPath<T>`
  (`~341`) and `ResolveJsonProperty` (`~368`) do:
  ```csharp
  foreach (var prop in typeInfo.Properties)                       // <-- requires populated .Properties
      if (prop.Name.Equals(name, OrdinalIgnoreCase) ||
          (prop.AttributeProvider is PropertyInfo pi && pi.Name.Equals(name, OrdinalIgnoreCase)))
          return prop;
  // nested descent:
  currentTypeInfo = jsonTypeInfo.Options.GetTypeInfo(pi.PropertyType);   // <-- requires a resolver
  ```
  So the generated `JsonTypeInfo<T>` must expose `.Properties` (each with `Name` + `AttributeProvider`),
  and `Options` must resolve nested types. This is why metadata-mode + a resolver are non-negotiable.
- **`UnitOfWork`** (`store.CreateUnitOfWork()`): `Add<T>(T, JsonTypeInfo<T>?)`, `AddRange`,
  `Update<T>(T, …)`, `Upsert<T>(T, …)`, **`Remove<T>(object id)`** (by id, not entity), `SaveChanges`.
  Surfaced via `ctx.CreateUnitOfWork()` for explicit transactions.
- **`DocumentStoreOptions`** (`src/Shiny.DocumentDb/DocumentStoreOptions.cs`) holds all model config:
  `MapTypeToTable<T>`, `MapIdProperty<T>`, `AddQueryFilter<T>`, `MapVersionProperty<T>`, `UseGuidV7Ids`,
  and `JsonSerializerOptions`. `[Document(...)]` lowers into calls here — do **not** fork config.
- **Registration shape to mirror:** providers register via `services.AddDocumentStore(opts => { opts.DatabaseProvider = …; … })`
  (see `samples/Sample.ODataApi/Program.cs:24`, `samples/Sample.Maui/MauiProgram.cs:26`). The context's
  DI extension wraps this.
- **Options exposure gap:** `IDocumentStore` does **not** currently expose its `JsonSerializerOptions`. The
  context needs them (to build `CreateObjectInfo` and install the resolver, and to align naming policy with
  the translator). Either add an accessor to `IDocumentStore`/its options, or thread the options through the
  DI extension into the context ctor. **Resolve before building** (see Open decisions).
- **Generator project:** confirm whether one exists (search `src/**/*Generators*.csproj`, `[Generator]`).
  If not, create `src/Shiny.DocumentDb.Generators/` as an `IIncrementalGenerator`, packaged as an analyzer
  (`IncludeBuildOutput=false`, `analyzers/dotnet/cs`) per `Directory.Build.props`.

---

## Open decisions (resolve before/at start of build)

1. **The big one — serialization-metadata approach & cost.** The store forces metadata-mode `JsonTypeInfo`
   + a resolver, which means **reproducing STJ's source-gen output** (per-type `CreateObjectInfo` with
   `JsonPropertyInfo[]`, ctor handling for records/`required`/`init`, collections, nullability, naming
   policy, polymorphism) — a large, version-fragile surface. Pick one:
   - **(A) Own it fully:** generate metadata-mode `JsonTypeInfo<T>` + `IJsonTypeInfoResolver` ourselves.
     Delivers the single-`[Document]`-list goal; highest build/maintenance cost; must track STJ behavior
     across .NET versions.
   - **(B) Reconsider STJ composition** despite the earlier rejection — let STJ generate the metadata
     (user keeps a `JsonSerializerContext`), our generator only adds typed sets + config lowering. Far less
     code; costs the "one list on our own context" ergonomic.
   - **(C) Hybrid:** own a *narrow, documented subset* (records/POCOs of primitives + simple collections,
     CamelCase, `[JsonPropertyName]`) and require an escape hatch (`[Document(typeof(X), TypeInfo = …)]` /
     bring-your-own `JsonTypeInfo<X>`) for anything outside it. Caps the sinkhole; some types fall back.
   - *Current lean: (C)* — but this is the gating decision; everything in Part 2 assumes (A)/(C) mechanics.
2. **Nested complex types.** Auto-walk the reachable type closure (generate metadata + register every
   reachable type in the resolver), **or** require each embedded type to be declared (`[Document]` or a
   lighter `[DocumentType]` marker)? Auto-walk is friendlier but expands the metadata surface; explicit is
   simpler and bounded. (Ties into decision 1's scope.)
3. **`JsonSerializerOptions` source.** Add an accessor on `IDocumentStore` to read the store's options, or
   thread options through `AddDocumentContext` into the context ctor? The generated resolver must end up on
   the *same* options the store serializes/queries with, or naming/nested resolution drift.

---

## Part 1 — runtime surface (core `Shiny.DocumentDb`)

### `DocumentContext` (base class)

```csharp
public abstract class DocumentContext
{
    protected DocumentContext(IDocumentStore store) => this.Store = store;

    public IDocumentStore Store { get; }
    public UnitOfWork CreateUnitOfWork() => this.Store.CreateUnitOfWork();   // explicit-transaction escape hatch

    protected DocumentSet<T> Set<T>(JsonTypeInfo<T> typeInfo) where T : class
        => new(this.Store, typeInfo);
}
```

### `DocumentSet<T>`

Thin, bound to `(IDocumentStore store, JsonTypeInfo<T> typeInfo)`. Every member forwards to the store with
`typeInfo` pre-applied, so callers never pass it:

```csharp
public sealed class DocumentSet<T> where T : class
{
    readonly IDocumentStore store;
    readonly JsonTypeInfo<T> typeInfo;
    internal DocumentSet(IDocumentStore store, JsonTypeInfo<T> typeInfo)
        => (this.store, this.typeInfo) = (store, typeInfo);

    // queries — return the existing IDocumentQuery<T> as-is (full query surface for free)
    public IDocumentQuery<T> Query()                                 => this.store.Query(this.typeInfo);
    public IDocumentQuery<T> Where(Expression<Func<T,bool>> p)       => this.store.Query(this.typeInfo).Where(p);
    public Task<T?> Get(object id, CancellationToken ct = default)   => this.store.Get(id, this.typeInfo, ct);
    public Task<IReadOnlyList<T>> ToList(CancellationToken ct = default) => this.store.Query(this.typeInfo).ToList(ct);

    // writes — immediate; JsonTypeInfo threaded automatically, never typed by the caller
    public Task Insert(T doc, CancellationToken ct = default)        => this.store.Insert(doc, this.typeInfo, ct);
    public Task Update(T doc, CancellationToken ct = default)        => this.store.Update(doc, this.typeInfo, ct);
    public Task Upsert(T doc, CancellationToken ct = default)        => this.store.Upsert(doc, this.typeInfo, ct);
    public Task<bool> Remove(object id, CancellationToken ct = default) => this.store.Remove<T>(id, ct);
}
```

---

## Part 2 — the source generator

**Project:** `src/Shiny.DocumentDb.Generators/` — an `IIncrementalGenerator`. (Mechanics below assume
Open-decision #1 lands on **(A) own it** or **(C) hybrid**; if it lands on **(B)**, the metadata-generation
parts drop and this collapses to "read `[JsonSerializable]`, emit sets + config.")

### Input it matches

A user-written partial class deriving from `DocumentContext`, decorated with `[Document(...)]` per type:

```csharp
[Document(typeof(User),  Table = "users", Id = nameof(User.Email))]   // override-only fields optional
[Document(typeof(Order))]                                             // bare → store defaults
public partial class AppContext : DocumentContext
{
    public AppContext(IDocumentStore store) : base(store) { }         // or let the generator emit it
}
```

### Output it emits (a second partial of the same class)

For each `[Document(typeof(TX))]`:

1. **Metadata-mode `JsonTypeInfo<TX>`** (per Open-decision #1) — `JsonMetadataServices.CreateObjectInfo<TX>`
   with a `JsonPropertyInfo[]` where each entry sets `Name` (member name via `PropertyNamingPolicy`,
   honoring `[JsonPropertyName]`) and `AttributeProvider` (the real `PropertyInfo`). Cache per type.
2. **A typed set property** bound to that `JsonTypeInfo`:
   ```csharp
   partial class AppContext
   {
       DocumentSet<User>? __users;
       public DocumentSet<User> Users => this.__users ??= this.Set(this.UserTypeInfo);
       // ...UserTypeInfo is the generated metadata-mode JsonTypeInfo<User>
   }
   ```
3. **An `IJsonTypeInfoResolver`** combining all generated type infos, installed on the options the context
   uses (so nested `Options.GetTypeInfo(...)` resolves).
4. **`static void ConfigureModel(DocumentStoreOptions o)`** lowering `[Document(...)]` →
   `o.MapTypeToTable<User>("users")`, `o.MapIdProperty<User>(x => x.Email)`, `o.AddQueryFilter<…>`, etc.
5. **A DI extension** (generated per context, so **no reflection** — AOT-clean):
   ```csharp
   public static class AppContextRegistration
   {
       public static IServiceCollection AddAppContext(this IServiceCollection services, Action<DocumentStoreOptions> configure)
       {
           services.AddDocumentStore(o =>
           {
               AppContext.ConfigureModel(o);                 // attribute defaults first
               // install the generated resolver on o.JsonSerializerOptions
               configure(o);                                 // user fluent overrides win
           });
           services.AddScoped<AppContext>(sp => new AppContext(sp.GetRequiredService<IDocumentStore>()));
           return services;
       }
   }
   ```

- **Set property name** = pluralized type name (`User` → `Users`); `[Document(typeof(User), Set="People")]`
  overrides. Dumb pluralization (append `s`, `y`→`ies`); the override covers oddities.
- **Naming policy alignment** is mandatory: generate `Name` with the same policy the store/query translator
  use (default CamelCase) — mismatch silently breaks queries.

### Diagnostics

- Context marked with `[Document]` but **not** `partial` → error.
- Context not deriving from `DocumentContext` → error.
- A `[Document]` type outside the generator's supported metadata subset (decision 1C) with no
  `TypeInfo=`/converter escape hatch → error with a clear "supply a JsonTypeInfo" message.
- Two sets resolving to the same property name → error (ask for `Set =`).
- Nested complex type not declared/resolvable (decision 2, explicit mode) → error.

---

## Build order

1. **Runtime** (`DocumentContext`, `DocumentSet<T>`) in core — pure forwards. Unit-testable by
   hand-authoring a context + hand-written metadata-mode `JsonTypeInfo` (no generator yet).
2. **Resolve Open-decision #1** — spike the metadata generation for one record type; confirm `.Properties`
   + `AttributeProvider` satisfy `ResolveJsonPath` and a real `Where`/`OrderBy` round-trips on SQLite.
   This spike de-risks the whole feature; do it before committing to the full generator.
3. **Generator** — emit metadata `JsonTypeInfo` + resolver + sets + `ConfigureModel` + DI extension.
   Start with the supported subset; widen per decision 1.
4. **Tests** (below). Run `dotnet test tests/Shiny.DocumentDb.Tests/...` plus the generator suite.

---

## Tests

**Runtime (`tests/Shiny.DocumentDb.Tests`)** — hand-author a context (with hand-written metadata-mode
`JsonTypeInfo`) against SQLite + LiteDB:
- `ctx.Users.Where(u => u.Age >= 18).ToList()` == `store.Query<User>().Where(...).ToList()` — **proves
  property-path translation works through the generated/hand-written metadata** (the core risk).
- `OrderBy`, `Select`, nested-property `Where` all resolve (exercise `Options.GetTypeInfo` descent).
- `Insert`/`Update`/`Upsert`/`Get`/`Remove(id)` round-trip.
- `ctx.CreateUnitOfWork()` batches atomically.
- Runs with `UseReflectionFallback=false` to prove AOT-cleanliness (metadata + resolver carry everything).

**Generator (`tests/Shiny.DocumentDb.Generators.Tests`, new)** — snapshot/verify + compile + run:
- Two `[Document]` types → two correctly-named sets, each bound to a metadata-mode `JsonTypeInfo` whose
  `.Properties` carry `Name` (CamelCase) + `AttributeProvider`.
- Generated resolver resolves all declared (and, per decision 2, nested) types.
- `[Document(Set="People")]`, `[Document(Table=…, Id=…)]` → renamed set / correct `ConfigureModel`.
- `[JsonPropertyName]` honored in generated `Name`.
- Each diagnostic fires.
- **End-to-end**: compile the generated output, register against SQLite, run a property query — not just a
  golden-file snapshot. The metadata must actually drive the translator.

---

## Gotchas

1. **Metadata-mode is mandatory, converters are not enough.** The store reads `JsonTypeInfo.Properties`;
   a value-converter `JsonTypeInfo` has none → all property queries break. Generate `CreateObjectInfo` with
   full `JsonPropertyInfo[]`.
2. **Set both `Name` and `AttributeProvider` on every `JsonPropertyInfo`.** The translator matches on
   either; `AttributeProvider` must be the real `PropertyInfo` (STJ does this via `typeof(T).GetProperty`
   with an IL2075 suppression — mirror that pattern; it's AOT-safe for user-rooted types).
3. **Naming policy must match the store.** Default is CamelCase (`DocumentStore.cs:2082`). Generate JSON
   names with the same policy + `[JsonPropertyName]`, or queries silently return nothing.
4. **The resolver must land on the store's options.** Nested descent uses `jsonTypeInfo.Options.GetTypeInfo`.
   Install the generated `IJsonTypeInfoResolver` on the exact `JsonSerializerOptions` the store uses
   (Open-decision #3), not a private copy.
5. **No buffered set methods.** Immediate-only (`Insert`/`Update`/`Upsert`/`Remove(id)`). Don't add `void`
   twins — won't compile (return-type overload). Transactions via `ctx.CreateUnitOfWork()`.
6. **Attributes are sugar, not a second config system.** `[Document(...)]` lowers into existing
   `DocumentStoreOptions.Map*`; fluent `configure` overrides win.
7. **Context lifetime / threading.** Register scoped (like `DbContext`); a context is not thread-safe.
8. **Don't wrap `IDocumentQuery<T>`.** Return it as-is so spatial/vector/full-text terminators,
   `ExecuteUpdate`, `NotifyOnChange`, etc. come for free and stay in sync.
9. **No generator chaining.** We never rely on STJ's generator; we never emit `[JsonSerializable]`
   expecting STJ to act on it.

---

## Four-artifact sync (per CLAUDE.md — do not skip)

- **Code + tests:** runtime in core + generator project + both test suites. Provider tier in the release
  note: **all providers** (sits above `IDocumentStore`).
- **Docs site** (`~/Desktop/dev/documentation/src/content/docs/documentdb/`): new `context.mdx` covering
  `DocumentContext`/`DocumentSet<T>`, `[Document]`, immediate writes, transactions via `CreateUnitOfWork`,
  `AddDocumentContext`. `<RN type="feature">` under a `## 9.2 TBD` heading (create if absent). State
  explicitly: **no change tracking, no navigation/`Include`**, and the supported-type subset (decision 1).
- **Skill** (`skills/shiny-documentdb/SKILL.md`): add `DocumentContext`, `DocumentSet`, `[Document]`,
  `AddDocumentContext` to `triggers:`; document the single-`[Document]`-list pattern and that `JsonTypeInfo`
  is threaded automatically.
- **readme.md** (repo root): add the strongly-typed context to the feature list.
