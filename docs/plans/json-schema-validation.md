# Plan: JSON-Schema validation before write

**Status:** Designed, not started.
**Target version:** `8.2.0` (current `version.json`) — additive feature, no breaking changes. New opt-in
package, so it can also slip to a later minor without affecting core.

> Self-contained build spec. The implementing agent does not have the design conversation —
> everything needed is here. Read `CLAUDE.md` (repo root) for the four-artifact rule (code+tests,
> docs site, skill, readme) before considering any commit "done".

Branch off `v8` (the current working branch) before starting.

---

## Goal

Let callers attach a **JSON Schema** to a document type and have the store **validate the serialized
JSON against that schema immediately before it is persisted**. A document that fails validation aborts
the write (and rolls back the surrounding unit of work), exactly like a throwing `BeforeWrite`
interceptor does today.

Two halves:

1. **Core primitive (`Shiny.DocumentDb`):** expose the *actual JSON that is about to be stored* to
   `BeforeWrite` interceptors via a lazy accessor on `DocumentWriteContext`. This is the piece the user
   asked for — "something that provides the `JsonDocument` just before we go to store." It is generally
   useful (auditing, redaction checks, outbox payload capture), not just for schema validation.

2. **Validation package (`Shiny.DocumentDb.JsonSchema`, new):** an opt-in package that depends on
   `JsonSchema.Net`, registers a `JsonSchemaInterceptor : IDocumentInterceptor`, and adds a
   `MapJsonSchema<T>(...)` registration API.

---

## Decisions locked (from design conversation)

- **Approach: Hybrid — lazy `JsonDocument` on the context.** Add a lazy `GetJsonDocument()` /
  `GetJson()` accessor to `DocumentWriteContext`, backed by a serialize delegate the provider supplies
  when it builds the context (the provider has `jsonOptions`/`typeInfo`; the `InterceptorPipeline` does
  not). Interceptors validate the *exact* JSON that will be written, and — as an optimization — the
  provider can reuse the cached JSON for the real write so the document is **not serialized twice**.
- **Packaging:** the `JsonSchema.Net` dependency lives in a **new `Shiny.DocumentDb.JsonSchema`
  package**, not core. Core stays dependency-free; `GetJsonDocument()` is a plain System.Text.Json
  primitive with no schema knowledge.
- **Library: `JsonSchema.Net`** (json-everything). It validates `JsonNode`/`JsonElement`/`JsonDocument`
  directly against a `JsonSchema`, is built on System.Text.Json, and has real AOT/trim support.

### Alternatives considered and rejected

- *Interceptor serializes `ctx.Document` itself (no provider changes).* Simplest — pure addition, ships
  fast — but serializes every document **twice** (once to validate, once to store) and the validated
  bytes may differ from the stored bytes if options/typeInfo diverge. Rejected in favour of the faithful
  lazy-context primitive, which is also reusable beyond schema validation.
- *NJsonSchema / Newtonsoft-based validators.* Not AOT/trim-safe. The library is AOT-first
  (`IsAotCompatible`, `UseReflectionFallback`, source-gen `JsonSerializerContext` throughout). Rejected.
- *`[JsonSchema("…")]` attribute on the document type.* Reflection-y and at odds with the AOT-first
  posture. A `MapJsonSchema<T>` + optional resolver delegate covers the same need without reflection.
  Skip the attribute (can revisit if asked).
- *A separate `IDocumentSerializationInterceptor` interface with a JSON-shaped signature.* Unnecessary
  surface — the existing `BeforeWrite` already fires at the right moment (pre-write, post-mutation).
  Surfacing JSON on the existing context is smaller and composes with everything already there.

---

## Architecture facts the implementer needs

There are **two** independent write paths, each with its own serialization call. Both must wire the new
serialize delegate into the context they build.

### Path 1 — relational (core `DocumentStore.cs`)

`src/Shiny.DocumentDb/DocumentStore.cs` **is** the shared relational store. SQLite, MySQL, PostgreSQL,
SQL Server, Oracle, and DuckDB all run through it, so wiring it once covers **all six** relational
providers. Key sites (line numbers approximate — re-grep before editing):

- Private context wrappers: `NewWriteContext` (~1117), `RunBeforeWriteAsync` (~1120),
  `RunAfterWriteAsync` (~1123).
- Per-op `NewWriteContext(...)` + `RunBeforeWriteAsync(...)` calls in Insert (~1165), Update (~1329),
  Upsert (~1372), Delete (~1733).
- Central serializer: `static string SerializeDocument<T>(T value, JsonTypeInfo<T>? typeInfo,
  JsonSerializerOptions options)` (~2008). Real writes call it at ~1204 (insert), ~1357 (update), ~1403
  (upsert), and the batch insert path (~1299 region).
- A nested executor type near line ~2611–2649 also holds `jsonOptions` and its own
  `NewWriteContext`/`RunBeforeWriteAsync` — wire it too (this is the UoW/transactional executor path).

### Path 2 — document providers (`DocumentProviderBase`)

`src/Shiny.DocumentDb/DocumentProviderBase.cs` centralizes interceptor plumbing for the four
non-relational stores. Each calls `NewWriteContext` / `RunBeforeWriteAsync` and then serializes with its
own static `Serialize(document, typeInfo, jsonOptions)`:

- `src/Shiny.DocumentDb.LiteDb/LiteDbDocumentStore.cs`
- `src/Shiny.DocumentDb.MongoDb/MongoDbDocumentStore.cs`
- `src/Shiny.DocumentDb.CosmosDb/CosmosDbDocumentStore.cs`
- `src/Shiny.DocumentDb.IndexedDb/IndexedDbDocumentStore.cs`

### The context factory

`DocumentWriteContext` is built in `InterceptorPipeline.NewWrite<T>(...)` (Interceptors.cs ~165) and
`BeforeWriteBatch<T>(...)` (~198). The pipeline has **no serializer**, so the serialize delegate must be
threaded in from the call site (`DocumentProviderBase.NewWriteContext` and core
`DocumentStore.NewWriteContext`), each of which already holds `jsonOptions` and resolves `typeInfo`.

---

## Part 1 — core primitive: JSON on the write context

**File:** `src/Shiny.DocumentDb/Interceptors.cs`

Add to `DocumentWriteContext`:

```csharp
// set internally when the context is built; null for delete-by-id (no document)
Func<object, string>? jsonFactory;
string? cachedJson;

/// <summary>
/// The exact JSON that will be persisted for <see cref="Document"/>, serialized with the store's
/// configured options/typeInfo. Computed on first access and cached. Returns null for deletes
/// (no document). If an earlier interceptor mutated <see cref="Document"/>, the cache is invalidated
/// so this always reflects the current document.
/// </summary>
public string? GetJson()
{
    if (this.Document == null || this.jsonFactory == null)
        return null;
    return this.cachedJson ??= this.jsonFactory(this.Document);
}

/// <summary>Parsed form of <see cref="GetJson"/>. Caller owns the returned document — dispose it.</summary>
public JsonDocument? GetJsonDocument()
{
    var json = this.GetJson();
    return json == null ? null : JsonDocument.Parse(json);
}
```

- **Cache invalidation:** the `Document` setter must clear `cachedJson` (turn the auto-property into a
  backing field + setter that nulls the cache). This matters because interceptors run in order and an
  earlier one may replace `Document`.
- **`jsonFactory` plumbing:** add an internal setter or constructor param. `InterceptorPipeline.NewWrite`
  and `BeforeWriteBatch` gain an optional `Func<object,string>? jsonFactory` parameter; the
  `DocumentProviderBase.NewWriteContext` and core `DocumentStore.NewWriteContext` wrappers pass a closure
  `doc => SerializeDocument((T)doc, typeInfo, jsonOptions)` (relational) / `Serialize(...)` (providers).
  Because `typeInfo`/`jsonOptions` are captured per-call, the delegate is AOT-safe — no extra reflection.

**Optional, do second (perf):** in each real-write site, if a context exists and `GetJson()` was already
computed, reuse `ctx.GetJson()` instead of calling `SerializeDocument` again. Guard on the document being
unchanged since (it's the same captured doc). Skip this if it complicates the diff — correctness does not
depend on it, only the no-double-serialize promise. Document whichever you choose in the release note.

**No-interceptor hot path stays free:** `NewWrite` still returns `null` when nothing is registered, so
the factory closure is never allocated unless an interceptor exists.

### Part 1 tests (`tests/Shiny.DocumentDb.Tests`)

- `GetJson()` returns the serialized doc inside `BeforeWrite`, equals what's persisted (round-trip read).
- Mutating `ctx.Document` in an earlier interceptor → a later interceptor's `GetJson()` reflects the
  change (cache invalidation).
- `GetJson()`/`GetJsonDocument()` return null for delete-by-id.
- Batch insert: each context's `GetJson()` matches its row.
- Run against **both** paths — at minimum SQLite (relational) and LiteDB (provider).

---

## Part 2 — `Shiny.DocumentDb.JsonSchema` package

**New project:** `src/Shiny.DocumentDb.JsonSchema/Shiny.DocumentDb.JsonSchema.csproj`

- Reference `Shiny.DocumentDb` + `PackageReference` to `JsonSchema.Net`.
- Match `Directory.Build.props` conventions (it supplies versioning, `IsAotCompatible`, packaging
  metadata). Confirm `JsonSchema.Net` is trim/AOT-clean for the targeted TFMs; if it emits warnings,
  note the tier in the release note rather than suppressing blindly.

### Schema registry + options API

```csharp
public sealed class JsonSchemaOptions
{
    // type -> compiled JsonSchema
    public JsonSchemaOptions MapJsonSchema<T>(JsonSchema schema);
    public JsonSchemaOptions MapJsonSchema<T>(string schemaJson);   // parse once at registration
    public JsonSchemaOptions MapJsonSchema<T>(Stream schemaJson);
    // dynamic fallback when no static map entry exists
    public Func<Type, JsonSchema?>? Resolver { get; set; }
    // Throw (default) | Collect-and-throw-aggregate | callback
    public SchemaValidationFailureMode FailureMode { get; set; }
}
```

Registration — both styles, mirroring how interceptors register today:

```csharp
// DI
services.AddDocumentJsonSchema(o => o.MapJsonSchema<Customer>(customerSchemaJson));

// or options-based, alongside AddInterceptor
options.AddJsonSchemaValidation(o => o.MapJsonSchema<Customer>(customerSchemaJson));
```

`AddDocumentJsonSchema` registers `JsonSchemaInterceptor` as `IDocumentInterceptor` (singleton) — the
existing `InterceptorPipeline.AttachServiceProvider` already resolves `IEnumerable<IDocumentInterceptor>`
from DI, so no core change is needed to pick it up.

### The interceptor

```csharp
sealed class JsonSchemaInterceptor : IDocumentInterceptor
{
    public Task BeforeWrite(DocumentWriteContext ctx, CancellationToken ct)
    {
        if (ctx.Operation == DocumentOperation.Delete)
            return Task.CompletedTask;

        var schema = this.ResolveSchema(ctx.DocumentType);
        if (schema == null)
            return Task.CompletedTask;            // unmapped types pass through

        using var doc = ctx.GetJsonDocument();
        if (doc == null)
            return Task.CompletedTask;

        var results = schema.Evaluate(doc.RootElement /* or JsonNode */, EvaluationOptions);
        if (!results.IsValid)
            throw new DocumentSchemaValidationException(ctx.DocumentType, ctx.TypeName, results);

        return Task.CompletedTask;
    }

    public Task AfterWrite(DocumentWriteContext ctx, CancellationToken ct) => Task.CompletedTask;
}
```

- `DocumentSchemaValidationException : Exception` carries `DocumentType`, `TypeName`, and a flattened
  list of `{ instanceLocation, error }` so callers can surface field-level messages. Throwing here aborts
  the write and rolls back the unit — this is already guaranteed by the pipeline (see `interceptors.md`).
- **Scope:** Insert / Update / Upsert (Delete has null `Document`; skip). Batch writes go through
  `BeforeWriteBatch` → same `BeforeWrite`, so they're covered for free. Set-based `ExecuteUpdate` /
  `ExecuteDelete` / `Clear` never materialize documents — **out of scope by nature** (call this out in
  docs so nobody expects schema enforcement on bulk updates).
- **Source filtering:** decide whether `DocumentOperationSource.Temporal` (Restore) writes are validated.
  Default: **validate them too** (restoring an invalid historical doc should still fail) — but make it a
  `JsonSchemaOptions` toggle since a strict-schema-tightening scenario might want to allow old data back.

### Part 2 tests (`tests/Shiny.DocumentDb.JsonSchema.Tests`, new)

- Valid document → write succeeds, row present.
- Invalid document (missing required prop, wrong type, failed constraint) → `DocumentSchemaValidationException`,
  **nothing persisted** (verify rollback).
- Unmapped type → passes through untouched.
- Resolver path (no static map entry).
- Upsert + batch insert both enforce.
- Delete is not blocked.
- AOT smoke: validation works with a source-gen `JsonSerializerContext` and `UseReflectionFallback=false`.

---

## Build order (one integrated effort, logical grouping)

1. **Part 1 core primitive** — `GetJson()`/`GetJsonDocument()` + cache invalidation + factory plumbing
   through `InterceptorPipeline` and **both** context-building call sites (relational core +
   `DocumentProviderBase`). Wire all providers' serialize delegates. Tests on SQLite + LiteDB.
2. **(Optional) reuse cached JSON** in the real-write sites to avoid double serialization.
3. **Part 2 package** — new project, `JsonSchema.Net` ref, options/registry, interceptor, exception.
4. **Tests** for both parts; run `dotnet test tests/Shiny.DocumentDb.Tests/...` and the new suite.

---

## Gotchas

1. **Two serialization paths, not one.** Relational writes serialize in core `DocumentStore.cs`; the four
   document providers serialize in their own stores. Both must inject the serialize delegate or
   `GetJson()` will be null on the path you forgot. There is no single shared serialize call to hook.
2. **Cache invalidation on `Document` mutation.** Interceptors run in registration order; an earlier one
   may replace `Document`. `GetJson()` must reflect the latest. Clear `cachedJson` in the setter.
3. **`JsonDocument` ownership.** `GetJsonDocument()` returns a fresh `JsonDocument` the caller must
   dispose (`using`). Don't cache the `JsonDocument` itself (disposed-reuse bug); cache the string and
   re-parse, or hand back a `JsonNode`. Pick one and be consistent.
4. **Batch path.** `BeforeWriteBatch` builds N contexts in a loop — each needs its own factory closure
   bound to that document, not a shared one.
5. **AOT.** The factory closure captures the already-resolved `typeInfo`/`jsonOptions`; do **not**
   introduce a reflection-based `JsonSerializer.Serialize(object)` call. Confirm `JsonSchema.Net` is
   trim-clean for the package's TFMs; record the compatibility tier in the release note.
6. **Out-of-scope writes.** Bulk/set-based ops and delete-by-id can't be schema-validated (no
   materialized document). State this explicitly in docs and the skill so it isn't mistaken for a gap.

---

## Four-artifact sync (per CLAUDE.md — do not skip)

- **Code + tests:** core + new package + both test suites above. Note provider tier in the release note
  (works on all providers because it hooks the shared interceptor path).
- **Docs site** (`~/Desktop/dev/documentation/src/content/docs/documentdb/`): extend `interceptors.md`
  (or a new `validation.mdx`) with `GetJson()`/`GetJsonDocument()` + the `Shiny.DocumentDb.JsonSchema`
  package and `MapJsonSchema<T>`. Add a `<RN type="feature">` release note under the `8.2` section
  (create a `## 8.2 TBD` heading if absent). Document the out-of-scope set-based/delete limitation.
- **Skill** (`skills/shiny-documentdb/SKILL.md`): add `JsonSchema`, `MapJsonSchema`,
  `DocumentSchemaValidationException`, `GetJsonDocument`, `Shiny.DocumentDb.JsonSchema` to `triggers:`
  and document the recommended registration pattern.
- **readme.md** (repo root): add JSON-schema validation to the feature list.
