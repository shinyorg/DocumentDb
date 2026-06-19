# DocumentDb Interceptors — Implementation Spec

> Self-contained build spec. The implementing agent does not have the design conversation —
> everything needed is here. Read `CLAUDE.md` (repo root) for the four-artifact rule before
> considering any commit "done".

Branch off `v7` (the working branch) before starting.

---

> ## ⚠️ DEPENDENCY: implement `unit-of-work-savechanges.md` FIRST
>
> The UnitOfWork/SaveChanges plan removes public `RunInTransaction` and consolidates **every**
> write (single convenience methods + units) onto ONE path: the internal engine →
> `TransactionalDocumentStore` per-op executor → single commit. This plan was written against the
> *old* shape (each public method owns its own transaction). After the UoW plan lands, adjust this
> plan as follows — the changes make the work **smaller**, not larger:
>
> - **Wire per-doc interceptors at the single engine/executor dispatch point**, NOT in each public
>   `Insert`/`Update`/… method (those become thin units-of-one and no longer hold the write logic).
> - **"Gotcha #1" below is RESOLVED and can be ignored** — after the UoW plan there is no separate
>   nested-store path to forget; the executor *is* the only write path.
> - **"AfterWrite inside the transaction, before commit"** now means inside the *unit's* single
>   transaction. In a multi-op unit: each op's before/after fires around its core write; the one
>   commit happens after all ops. A throwing `BeforeWrite` rolls back the whole unit.
> - **`BatchInsert` per-doc firing** must hook into the coalesced insert-run dispatch
>   (`BatchInsertCoreAsync`): before-hooks loop (pre-serialization, mutable `Document`) → serialize →
>   multi-row INSERT → after-hooks loop (Id/Version populated). The UoW plan commits to keeping that
>   structure per-doc-hookable.
> - **`OnBeforeInsert` shim**: the UoW plan still calls the existing `RunBeforeInsertHooksAsync`;
>   this plan generalizes/replaces that call site as already described.
> - **No phases / single integrated build** (per the decision): the "Build order (commit by commit)"
>   section below is a logical grouping, not separate deliverables — fold it into the one effort.

---

## Goal

Add **before/after interceptors** for write operations to `IDocumentStore`, across all providers.
Two granularities:

1. **Per-document interceptor** — fires for single-document writes (`Insert`, `BatchInsert` per item,
   `Update`, `Upsert`, `Remove`).
2. **Bulk/set interceptor** — fires once for set-based writes (`ExecuteUpdate`, `ExecuteDelete`,
   `Clear`).

No read-side interceptor in v1 (see "Explicitly out of scope").

---

## Why these decisions (so you don't re-litigate them)

- **In-core, not a decorator.** Interceptors are invoked directly inside each store's write path so the
  after-hook can run *inside the transaction* (enables a transactional outbox) and see the generated
  id + version. A decorator (`InstrumentedDocumentStore` style) was rejected because it fires outside
  the transaction and can't see generated ids cleanly.
- **Per-doc interceptors do NOT fire on set-based ops.** A set-based `UPDATE … WHERE` never loads the
  documents — there is nothing to hand a per-row interceptor. This mirrors EF Core, where
  `ExecuteUpdate`/`ExecuteDelete` deliberately bypass the change tracker and SaveChanges interceptors.
  Bulk ops get their own `IDocumentBulkInterceptor` (predicate + affected count). Document this gap
  loudly. **No materialize-then-loop opt-in in v1.**
- **Temporal writes fire interceptors, flagged — never skipped.** A `Restore` is a real document change;
  silently skipping it would make audit/cache-invalidation interceptors incorrect. Fire normally but set
  `ctx.Source = DocumentOperationSource.Temporal` so each interceptor decides whether to act. Note:
  the temporal *history-table writes* (`AppendHistoryAsync`) are internal sidecar writes, NOT
  `IDocumentStore<T>` calls, so interceptors never see them regardless — no work needed there.
- **No read interceptor.** Predicate injection already exists (`AddQueryFilter<T>` + tenant filter);
  observability already exists (`InstrumentedDocumentStore` / OpenTelemetry); decrypt/transform-on-read
  belongs in serialization converters, not a query pipeline. A result-mutating read hook also breaks
  streaming (`ToAsyncEnumerable`) and projection typing (`Select<TResult>`, `Project` → `JsonObject`).

---

## Current state of the codebase (verified)

### IDocumentStore — `src/Shiny.DocumentDb/IDocumentStore.cs`
Write methods to wire: `Insert`, `BatchInsert`, `Update`, `Upsert`, `SetProperty`, `RemoveProperty`,
`Remove`, `Clear`. (Plus `RunInTransaction`, `Get`, `Query`, `QueryStream`, `Count`, spatial/vector —
not write-intercepted.)

### IDocumentQuery<T> — `src/Shiny.DocumentDb/IDocumentQuery.cs`
Already has the set-based mutation methods:
- `Task<int> ExecuteDelete(CancellationToken ct = default)`
- `Task<int> ExecuteUpdate(Expression<Func<T, object>> property, object? value, CancellationToken ct = default)`

### Existing partial hook (generalize this — keep source-compatible)
`DocumentStoreOptions.OnBeforeInsert<T>` — `src/Shiny.DocumentDb/DocumentStoreOptions.cs` (~lines 505-516).
Stored in `beforeInsertHooks`, resolved via `ResolveBeforeInsertHooks()`, invoked by
`DocumentStore.RunBeforeInsertHooksAsync<T>` (~lines 1013-1019) inside `Insert` and `Upsert`.
**Generalize this into the new interceptor pipeline; keep `OnBeforeInsert<T>` working as a shim.**

### Existing query-filter system (reference — do NOT duplicate as a read interceptor)
`DocumentStoreOptions.AddQueryFilter<T>(predicate)` / `AddQueryFilter<T>(name, predicate)` —
`DocumentStoreOptions.cs` (~lines 187-211). Applied via `DocumentQuery<T>.GetEffectivePredicates()`
and `DocumentStore.AppendGlobalFilters<T>()` to reads AND to `Get`/`Remove`/`Update`/`SetProperty`/
`Clear`/`ExecuteUpdate`/`ExecuteDelete`. `IgnoreQueryFilters()` opts out.

### Existing post-commit notification (separate concern — leave as-is)
`IObservableDocumentStore.NotifyOnChange<T>` + `ChangeBroadcaster.PublishChange(...)`. This is a
fire-and-forget post-commit stream, NOT an interceptor. Keep `PublishChange` calls where they are.

### Provider implementations
| Provider | Class | File |
|---|---|---|
| SQLite / Postgres / MySQL / SQL Server / Oracle / DuckDB (all relational) | `DocumentStore` (shared base) | `src/Shiny.DocumentDb/DocumentStore.cs` |
| MongoDB | `MongoDbDocumentStore` | `src/Shiny.DocumentDb.MongoDb/MongoDbDocumentStore.cs` |
| LiteDB | `LiteDbDocumentStore` | `src/Shiny.DocumentDb.LiteDb/LiteDbDocumentStore.cs` |
| CosmosDB | `CosmosDbDocumentStore` | `src/Shiny.DocumentDb.CosmosDb/CosmosDbDocumentStore.cs` |
| IndexedDB | `IndexedDbDocumentStore` | `src/Shiny.DocumentDb.IndexedDb/IndexedDbDocumentStore.cs` |

All 7 relational providers share the one `DocumentStore.cs` — wire it once there. The 4 document
providers each have their own options class and write methods — wire each separately.

### Set-based op implementations (where bulk interceptors hook)
- Relational: `src/Shiny.DocumentDb/Internal/DocumentQuery.cs` — `ExecuteDelete` (~295-317, single
  `DELETE … WHERE TypeName [+ tenant] [+ where]`), `ExecuteUpdate` (~319-348, single `UPDATE … SET
  Data = json_set(...)`). `Clear<T>` is in `DocumentStore.cs` (~1478-1506), single `DELETE`.
- MongoDB: `ExecuteDeleteAsync`/`ExecuteUpdatePropertyAsync` in `MongoDbDocumentStore.cs` (~739-768),
  native `DeleteMany`/`UpdateMany`.
- Cosmos / LiteDB / IndexedDB: these loop internally (load-then-mutate) — but the bulk interceptor
  still fires ONCE per call, not per looped row.

### Relational write-path anchors (where to place per-doc hooks)
In `src/Shiny.DocumentDb/DocumentStore.cs`:
- `Insert<T>` (~1047-1092): currently calls `RunBeforeInsertHooksAsync` at top; writes inside
  `ExecuteAsync(...)` via `InsertCoreAsync`; `PublishChange` after. **Before** at top, **after** inside
  the `ExecuteAsync` session block after `InsertCoreAsync` succeeds.
- `Update<T>` (~1174-1207): same shape via `UpdateCoreAsync`.
- `Upsert`, `SetProperty`, `RemoveProperty`, `Remove<T>` (~1446-1476), `Clear<T>` (~1478-1506).
- `AppendGlobalFilters<T>` (~126-149), `ExecuteAsync`, `PublishChange` are the existing helpers.

---

## New public API

Namespace: `Shiny.DocumentDb` (same as `IDocumentStore`). New files under `src/Shiny.DocumentDb/`.

```csharp
public enum DocumentOperation { Insert, Update, Upsert, Delete, Clear }

public enum DocumentOperationSource { Direct, Temporal }

public interface IDocumentInterceptor
{
    Task BeforeWrite(DocumentWriteContext ctx, CancellationToken ct);
    Task AfterWrite(DocumentWriteContext ctx, CancellationToken ct);
}

public sealed class DocumentWriteContext
{
    public DocumentOperation Operation { get; init; }       // Insert | Update | Upsert | Delete
    public DocumentOperationSource Source { get; init; }    // Direct | Temporal
    public Type DocumentType { get; init; }
    public string TypeName { get; init; }

    public object Id { get; internal set; }                 // may be default before insert id-gen; set after
    public object? Document { get; set; }                   // MUTABLE in BeforeWrite; null for Delete-by-id
    public int? Version { get; internal set; }

    // After only:
    public bool Succeeded { get; internal set; }
    public Exception? Error { get; internal set; }
}

public interface IDocumentBulkInterceptor
{
    Task BeforeBulkWrite(DocumentBulkContext ctx, CancellationToken ct);
    Task AfterBulkWrite(DocumentBulkContext ctx, CancellationToken ct);
}

public sealed class DocumentBulkContext
{
    public DocumentOperation Operation { get; init; }       // Update | Delete | Clear
    public DocumentOperationSource Source { get; init; }
    public Type DocumentType { get; init; }
    public string TypeName { get; init; }

    public string? WhereClause { get; init; }               // translated predicate (incl. injected query filters); null for Clear-all
    public (string Property, object? Value)? Assignment { get; init; }  // ExecuteUpdate only

    public int AffectedCount { get; internal set; }         // after only
}
```

### Registration surface
On `DocumentStoreOptions` (`src/Shiny.DocumentDb/DocumentStoreOptions.cs`):
- `OnBeforeWrite<T>(Func<DocumentWriteContext, CancellationToken, Task>)` — lambda sugar, type-filtered on `T`.
- `OnAfterWrite<T>(Func<DocumentWriteContext, CancellationToken, Task>)`.
- Keep `OnBeforeInsert<T>` — reimplement it as a shim that registers an interceptor whose `BeforeWrite`
  runs only when `ctx.Operation == Insert`. Existing callers, docs examples, and `SKILL.md` snippets
  must keep compiling and behaving identically.
- Also resolve `IEnumerable<IDocumentInterceptor>` and `IEnumerable<IDocumentBulkInterceptor>` from DI
  and run them alongside the options-registered ones. Registration order = execution order. DI-resolved
  run after options-registered (document the order; keep it deterministic).

For the document providers that use their own options class (`MongoDbDocumentStoreOptions`, etc.),
add the same registration surface to each, OR resolve a shared interceptor list passed in at
construction. Pick whichever is least invasive per provider but keep the public API shape identical
across providers.

---

## Semantics (enforce in code + tests)

**Per-document:**
- `BeforeWrite` fires before serialization. Mutations to `ctx.Document` MUST be persisted (it must be
  re-read after the hook, not captured before). Throwing from `BeforeWrite` aborts the write and the
  exception propagates to the caller (throwing is the cancel mechanism — no separate cancel flag in v1).
- `AfterWrite` fires inside the transaction after the core write succeeds, before commit, with `Id`,
  `Version`, `Succeeded = true`. If the core write throws, `AfterWrite` does NOT fire (the before/after
  pair only completes on success). Do not swallow exceptions.
- `BatchInsert`: fire per-document (before each, after each). If this is too chatty for huge batches,
  that's a future opt-out — not v1.
- `Remove` (delete-by-id): `ctx.Document` is null, `ctx.Id` is set.
- `Insert`: `ctx.Id` may be default in `BeforeWrite` (server/sequence-generated id not yet assigned);
  it MUST be populated by `AfterWrite`.

**Bulk:**
- `BeforeBulkWrite` fires once with `WhereClause` + `Assignment` (for `ExecuteUpdate`) + `Source`;
  can throw to abort.
- `AfterBulkWrite` fires once after the op with `AffectedCount`.
- Per-document interceptors MUST NOT fire for `ExecuteUpdate`/`ExecuteDelete`/`Clear`.
- `Clear<T>` → `Operation = Clear`, `WhereClause = null` (or the global-filter clause if filters are
  registered — match whatever `AppendGlobalFilters` produced).

**Temporal:**
- `Restore` (and any internal temporal-driven write) sets `Source = Temporal`. Use an internal
  `AsyncLocal<DocumentOperationSource>` ambient that `Restore` sets around its inner `Insert`/`Update`
  call, so you don't fork every write method into a source-carrying overload. Default ambient = `Direct`.
  Find `Restore` via the temporal feature (`ITemporalDocumentStore`, `MapTemporal`); it lives in the
  relational `DocumentStore` and `MongoDbDocumentStore` (temporal is SQLite + Mongo so far).

---

## Explicitly out of scope for v1

- No read/query interceptor of any kind (no `OnQuery`, no result transform).
- No materialize-then-loop opt-in for bulk ops (per-doc interceptors simply don't fire on bulk).
- No cancel flag (throw to abort).
- No per-batch (whole-`BatchInsert`) interceptor — per-item only.

---

## Build order (commit by commit)

### Commit 1 — Contracts + relational + tests
1. Add the new types (`IDocumentInterceptor`, `IDocumentBulkInterceptor`, `DocumentWriteContext`,
   `DocumentBulkContext`, `DocumentOperation`, `DocumentOperationSource`) under `src/Shiny.DocumentDb/`.
2. Add registration surface to `DocumentStoreOptions`; reimplement `OnBeforeInsert` as a shim.
3. Wire the relational `DocumentStore.cs`: per-doc before/after in `Insert`/`BatchInsert`/`Update`/
   `Upsert`/`SetProperty`/`RemoveProperty`/`Remove`; bulk before/after in `Internal/DocumentQuery.cs`
   `ExecuteDelete`/`ExecuteUpdate` and in `Clear<T>`.
4. Temporal `AsyncLocal` flag wired through `Restore`.
5. Tests in `tests/Shiny.DocumentDb.Tests/` — see checklist below.
6. Run `dotnet test tests/Shiny.DocumentDb.Tests/Shiny.DocumentDb.Tests.csproj` — must be green.

### Commit 2 — Document providers
Replicate per-doc + bulk wiring in `MongoDbDocumentStore`, `LiteDbDocumentStore`,
`CosmosDbDocumentStore`, `IndexedDbDocumentStore`. Temporal flag in Mongo (it has `Restore`).
Add/extend provider-specific tests. Run the relevant suites.

### Commit 3 — Four-artifact follow-through (CLAUDE.md)
- **Docs site** (`~/Desktop/dev/documentation/src/content/docs/documentdb/`): new `interceptors.mdx`
  feature page; `<RN type="feature">` release note in `release-notes.mdx`. Version = raw version from
  `version.json` (strip prerelease suffix). If that version isn't released, use/create a `## <version>
  TBD` heading per the CLAUDE.md rules.
- **Skill** (`skills/shiny-documentdb/SKILL.md`): interceptor usage pattern; add `triggers:` keywords
  `IDocumentInterceptor`, `IDocumentBulkInterceptor`, `OnBeforeWrite`, `OnAfterWrite`.
- **readme.md** (repo root): feature-list entry. (Packed into NuGet via `PackageReadmeFile`.)

Interceptors are a core, all-provider feature — the release note carries no backend-specific
compatibility caveat (unlike temporal, which is SQLite+Mongo only).

---

## Test checklist

Per-document (run against SQLite at minimum; ideally parametrized across providers like existing tests):
- [ ] `BeforeWrite` fires for Insert / Update / Upsert / Remove with correct `Operation`.
- [ ] Mutation of `ctx.Document` in `BeforeWrite` is persisted (read back and assert).
- [ ] Throw in `BeforeWrite` aborts the write (document not present / unchanged) and propagates.
- [ ] `AfterWrite` sees populated `Id` (esp. generated id on Insert) and `Version`.
- [ ] `AfterWrite` does NOT fire when the core write throws (e.g. duplicate-key Insert, concurrency on Update).
- [ ] `BatchInsert` fires per-item.
- [ ] Multiple interceptors run in registration order; options-registered before DI-registered.
- [ ] `OnBeforeInsert` shim still works (back-compat).

Bulk:
- [ ] `ExecuteDelete` / `ExecuteUpdate` / `Clear` each fire bulk before+after exactly once.
- [ ] Per-document interceptors do NOT fire for any bulk op.
- [ ] `AfterBulkWrite.AffectedCount` matches the returned count.
- [ ] `ExecuteUpdate` populates `Assignment`; `WhereClause` reflects the predicate.
- [ ] Bulk `BeforeBulkWrite` throw aborts.

Temporal:
- [ ] `Restore` fires interceptors with `Source == Temporal`.
- [ ] Direct writes report `Source == Direct`.
- [ ] History-table writes do not surface as interceptor events.

---

## Gotchas

- ~~`RunInTransaction` swaps in a nested/transactional store (`TransactionalDocumentStore`)…~~
  **RESOLVED by the UnitOfWork plan (prerequisite).** Public `RunInTransaction` is gone and the
  `TransactionalDocumentStore` executor becomes the *only* write path, so there is no second path to
  forget. Wire interceptors once at that executor's per-op dispatch and all writes (single + unit)
  are covered. (If for some reason the UoW plan is NOT done first, this gotcha is live again and you
  must wire both the public methods and the nested store — but that's the wrong order; don't.)
- Document providers have INDEPENDENT write implementations — don't assume wiring the relational base
  covers them. Each of the 4 needs its own edits.
- Keep `PublishChange` (post-commit notification) separate from `AfterWrite` (in-transaction interceptor).
  They are different features; don't merge them.
- `AsyncLocal` for temporal source: set-and-restore in a `try/finally` so a throwing `Restore` doesn't
  leak `Temporal` into later operations on the same execution context.
