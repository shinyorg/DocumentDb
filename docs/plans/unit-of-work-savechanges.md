# Plan: UnitOfWork + SaveChanges as the primary write model

**Status:** Designed, not started.
**Target version:** breaking change → bump from 7.2.1 to **7.3** (or 8.0 if you want to signal it loudly). Decide at cut time.

**Implementation order: THIS PLAN LANDS FIRST**, before `interceptors.md`. The two plans rewrite the same write methods. This plan consolidates *every* write (single convenience methods + `UnitOfWork`) onto one path — the internal engine → `TransactionalDocumentStore` per-op executor → single commit. Interceptors (Plan B) then hook into that **one** dispatch point instead of ~7 public methods + the nested-store duplicate path. Doing interceptors first would mean tearing their wiring out when this plan collapses those methods. See "Hand-off to the interceptors plan" at the bottom.
**Decision locked:** `RunInTransaction` becomes the **internal-only** transaction engine. `UnitOfWork` + `SaveChanges` is the only public write-grouping API. Read-modify-write-in-transaction is no longer publicly available — callers use CAS/ETag (`IfMatch`) + retry for those cases.

**Alternatives considered and rejected:**
- *Buffer pending ops on `IDocumentStore` itself (store-as-unit-of-work).* Rejected: the store is long-lived shared infrastructure (connections, broadcaster/subscribers, caches; effectively singleton on mobile). A mutable buffer on it causes concurrency corruption (two callers share one buffer) and forgotten-flush footguns. A unit of work is per-operation, short-lived state and belongs in its own object.
- *Make `IDocumentStore` scoped + add `IDocumentStoreFactory` (mirror EF's `DbContext` / `IDbContextFactory`).* Coherent, but requires splitting the fused store into singleton infra (connection/client, broadcaster, caches) + a lightweight per-operation context — a much larger 8.0-scale refactor. Also breaks on mobile (no ambient DI scope) and creates captive-dependency problems for singleton consumers (Orleans grain storage). Deferred; not needed to hit the "only inject one type" goal — `CreateUnitOfWork()` already keeps `IDocumentStore` the sole injected dependency.

**Design rationale is documented for users** in the docs site FAQ: `~/Desktop/dev/documentation/src/content/docs/documentdb/faq.mdx` (write-API decision tree + "why isn't the store itself the unit of work" + read-your-writes note). That page documents the *target* API and must ship in the same PR as the code (see four-artifact sync) — hold the docs branch until the implementation lands.

---

## Goal

Replace the public `RunInTransaction` + ad-hoc convenience-write model with a single, clean model:

- One public way to group writes: `UnitOfWork` with `SaveChanges`.
- Exactly **one** place a transaction opens (inside `SaveChanges`), so the store never has to reason about nested transactions.
- **Preserve batch-insert speed** — `SaveChanges` must dispatch runs of same-type inserts through the existing chunked multi-row path (`BatchInsertCoreAsync`), not one INSERT per document.
- Convenience methods (`Insert`, `BatchInsert`, `Update`, `Upsert`, `Remove`, `Clear`) remain on `IDocumentStore` but become auto-committed "units of one" over the same engine.

### Explicitly NOT doing
- No EF-style snapshot/proxy change tracking (dirty detection). Writes stay explicit (`Add`/`Update`/`Remove`). Keeps the lightweight, schema-free ethos.
- No public interactive-transaction API. (That capability is intentionally dropped; CAS/ETag covers the conditional-write cases.)

---

## Current state (for reference)

- `src/Shiny.DocumentDb/IDocumentStore.cs` — `RunInTransaction(Func<IDocumentStore, Task>, CancellationToken)` is a **public** interface method.
- `src/Shiny.DocumentDb/UnitOfWork.cs` — concrete `UnitOfWork` class that buffers `List<Func<IDocumentStore, CancellationToken, Task>>` closures and replays them inside `RunInTransaction` on `Commit()`. Created via the **extension method** `DocumentStoreExtensions.CreateUnitOfWork(this IDocumentStore)`. Only supports `Add` (insert), `Update`, `Remove`.
  - **This extension class is being removed entirely** — `CreateUnitOfWork` is promoted to a first-class method on `IDocumentStore`.
  - Current `Commit()` replays one closure per op → loses batch speed. This is the main thing the rewrite fixes.
- `src/Shiny.DocumentDb/DocumentStore.cs`
  - `RunInTransaction` ~L1510: opens a transaction, builds `TransactionalDocumentStore`, buffers change notifications in `pendingChanges`, emits them after commit.
  - `BatchInsert` ~L1094: before-insert hooks → open txn → `BatchInsertCoreAsync` → vector sidecars → temporal history → commit → publish changes.
  - `BatchInsertCoreAsync` ~L381: ID resolution + serialize, then chunked multi-row INSERT. `const int BatchChunkSize = 500;` (~L379). Uses `provider.BuildBatchInsertSql(tableName, chunkSize)`.
  - `TransactionalDocumentStore` ~L2280: the per-op executor bound to the pinned connection/transaction. Its `RunInTransaction` throws "Nested transactions are not supported." Its `BatchInsert` ~L2747 reuses the pinned txn via `BatchInsertCoreAsync`.
- `src/Shiny.DocumentDb.MongoDb/MongoDbDocumentStore.cs` — `BatchInsert` ~L221 uses `InsertManyAsync`; `RunInTransaction` ~L583 uses a compensating store (`MongoDbCompensatingStore`).
- `src/Shiny.DocumentDb.CosmosDb/CosmosDbDocumentStore.cs` — `BatchInsert` ~L295 uses transactional batch chunked at 100; `RunInTransaction` uses `CosmosDbTransactionalStore` (compensating).

---

## Target public API

```csharp
public interface IDocumentStore
{
    // unchanged reads: Query, Get, Count, QueryStream, GetDiff, SupportsSpatial/Vector ...

    // convenience writes — each is an auto-committed unit of one
    Task Insert<T>(T document, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken ct = default) where T : class;
    Task<int> BatchInsert<T>(IEnumerable<T> documents, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken ct = default) where T : class;
    Task Update<T>(T document, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken ct = default) where T : class;
    Task Upsert<T>(T patch, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken ct = default) where T : class;
    Task<bool> SetProperty<T>(...);     // unchanged
    Task<bool> RemoveProperty<T>(...);  // unchanged
    Task<bool> Remove<T>(object id, CancellationToken ct = default) where T : class;
    Task<int> Clear<T>(CancellationToken ct = default) where T : class;

    UnitOfWork CreateUnitOfWork();      // promoted from extension method to interface method

    // RunInTransaction REMOVED from the interface (now a private engine on the implementation)
}
```

```csharp
public sealed class UnitOfWork
{
    UnitOfWork Add<T>(T doc, JsonTypeInfo<T>? jsonTypeInfo = null) where T : class;
    UnitOfWork AddRange<T>(IEnumerable<T> docs, JsonTypeInfo<T>? jsonTypeInfo = null) where T : class; // batch-speed path
    UnitOfWork Update<T>(T doc, JsonTypeInfo<T>? jsonTypeInfo = null) where T : class;
    UnitOfWork Upsert<T>(T patch, JsonTypeInfo<T>? jsonTypeInfo = null) where T : class;
    UnitOfWork Remove<T>(object id) where T : class;
    void Clear();
    int PendingCount { get; }
    Task SaveChanges(CancellationToken ct = default);   // replaces Commit (see naming note)
}
```

**Naming:** rename `Commit` → `SaveChanges` (EF-familiar). Keep `Commit` as a `[Obsolete]` forwarding alias for one minor version if we want a softer landing — optional, decide at implementation.

---

## Internal design

### 1. Typed operation buffer (enables coalescing)

Replace `List<Func<IDocumentStore, CancellationToken, Task>>` with typed records so `SaveChanges` can inspect and group ops:

```csharp
enum UowOpKind { Insert, Update, Upsert, Remove }

// One entry per queued op. Generic payload captured via a small closure or a typed holder
// so we don't lose the compile-time T (needed for accessor/typeInfo resolution).
abstract class UowOp { public abstract Type DocType { get; } public abstract UowOpKind Kind { get; } }
sealed class UowOp<T> : UowOp where T : class { /* kind, document(s) or id, jsonTypeInfo */ }
```

The op must retain `T` and the optional `JsonTypeInfo<T>` so the executor can resolve the id accessor, table/type name, version mapping, etc. — same inputs `BatchInsertCoreAsync` needs today.

### 2. Coalescing rule in `SaveChanges`

Walk the pending list **in order**. Dispatch per run:

- **Contiguous `Insert`/`AddRange` of the same `T`** → coalesce into one grouped insert via `BatchInsertCoreAsync` (chunked multi-row `INSERT … VALUES`, 500/chunk). Same code path and same speed as standalone `BatchInsert`.
- **`Update`/`Upsert`/`Remove`** → **no multi-row equivalent exists** (update is a full-document `UPDATE … SET Data=@data WHERE Id=@id` by Id; remove is `DELETE … WHERE Id=@id`). Execute one statement per op, but reuse a single prepared `DbCommand` per contiguous same-`(kind,T)` run, rebinding parameters per row. The win here is the shared transaction + connection + reused command, not statement count — which is where most of the per-op overhead actually lives (esp. SQLite: N separate auto-commit writes = N × begin/commit + connection-semaphore acquire).
- A different `T`, or a different op kind, ends the current run.

Rules / guarantees:
- Order is preserved across boundaries, so sequencing semantics (insert-then-remove-same-id, type-A-before-type-B, update-after-insert) are never reordered.
- Only **adjacent** same-type inserts coalesce — don't over-promise global reordering. If a caller wants one big insert batch, they group their `Add`s (or use `AddRange`), which guarantees a contiguous run.
- Recovers full batch-insert throughput for the common case and is strictly faster than today's per-closure UoW.

### 3. One transaction boundary

`SaveChanges` calls the now-private engine (the current `RunInTransaction` body) **once**, passing a callback that runs the dispatch table against the pinned `TransactionalDocumentStore`. Because `SaveChanges` is the sole caller of the engine:
- No public path opens a transaction, so the "nested transaction" guard in `TransactionalDocumentStore.RunInTransaction` becomes unreachable (keep the throw as a defensive invariant).
- Change notifications keep using the existing `pendingChanges` buffer-then-emit-after-commit machinery.

### 4. Convenience methods become units of one

Reimplement `Insert`/`BatchInsert`/`Update`/`Upsert`/`Remove`/`Clear` on `DocumentStore` as:
```
var uow = CreateUnitOfWork();
uow.Add(doc);            // or AddRange / Update / Remove ...
await uow.SaveChanges(ct);
```
`BatchInsert` returns the inserted count (preserve current signature/semantics). Generated IDs continue to flow back into the caller's objects via `accessor.SetId` in-place — keep this; add/keep a test asserting it.

**`BatchInsert` stays a first-class method on `IDocumentStore`** (decided). Routing a pure bulk insert through `CreateUnitOfWork().AddRange(...).SaveChanges()` would be the *same* fast path internally, so a unit buys zero extra speed for inserts-only — and `BatchInsert` keeps its `int` count return and one-liner ergonomics. The only reason to put a bulk insert inside a unit is to make it atomic **with other (non-insert) operations** via `AddRange`; if there are no other ops there's nothing to be atomic with, so `BatchInsert` is the cleaner spelling. So: `BatchInsert` = direct method (auto-committed insert-run-of-one); `AddRange` = the mix-with-other-ops case. Both share `BatchInsertCoreAsync`.

Watch the hot path: a unit-of-one shouldn't regress single-insert latency. If the UoW allocation/iteration shows up, give `DocumentStore` a private fast path that calls the engine directly for the 1-op case. Measure before optimizing.

### 5. Provider parity

- **Relational base (`DocumentStore`)** — primary implementation above.
- **MongoDB (`MongoDbDocumentStore`)** — best case: map the **entire** mixed op list to one `BulkWriteAsync` (ordered: `InsertOneModel`/`ReplaceOneModel`/`DeleteOneModel`), which batches mixed ops in a single round trip — better than the relational per-statement update path. Insert-only runs can still use `InsertManyAsync`. Fall back to the compensating-store pattern (`MongoDbCompensatingStore`) only where bulk write can't express the semantics. Remove public `RunInTransaction`; keep its body as the internal engine.
- **CosmosDB (`CosmosDbDocumentStore`)** — `TransactionalBatch` supports mixed ops (`CreateItem`/`ReplaceItem`/`DeleteItem`) but only **within one partition key** (partitioned by `typeName`) and max 100 ops/batch. So a unit must split into **one batch per distinct `T`** (each chunked at 100); a single-type unit is one batch. Use `CosmosDbTransactionalStore` (compensating) only for the cross-type case where a single atomic batch isn't possible. Same internal-engine treatment.

  > Note: cross-type units are **not** atomic on Cosmos (separate per-partition batches) — document this provider limitation, consistent with the existing partitioning model.
- Any other providers under `src/` that implement `IDocumentStore` (SQLite, LiteDB, DuckDB, IndexedDB, MySQL, SQL Server, PostgreSQL, Oracle): each must drop the public `RunInTransaction` and route through the shared engine. **Audit all providers** — grep for `RunInTransaction` and `IDocumentStore` implementers before declaring done.
- Orleans persistence sits on the same `IDocumentStore` contract — verify it doesn't call `RunInTransaction` directly; if it does, migrate to a UoW.

---

## File-by-file work

1. `src/Shiny.DocumentDb/IDocumentStore.cs` — remove `RunInTransaction`; add `UnitOfWork CreateUnitOfWork()`.
2. `src/Shiny.DocumentDb/UnitOfWork.cs` — rewrite buffer to typed ops; add `AddRange`/`Upsert`; implement coalescing `SaveChanges`; **delete `DocumentStoreExtensions`** (the `CreateUnitOfWork` extension) — it moves onto the interface/implementation.
3. `src/Shiny.DocumentDb/DocumentStore.cs` — make `RunInTransaction` private (rename to e.g. `RunInTransactionCore`); implement `CreateUnitOfWork`; reimplement convenience writes as units of one; keep `TransactionalDocumentStore` as the per-op executor; consider the 1-op fast path.
4. `src/Shiny.DocumentDb.MongoDb/MongoDbDocumentStore.cs` — internal-only engine; `SaveChanges` insert-run → `InsertManyAsync`.
5. `src/Shiny.DocumentDb.CosmosDb/CosmosDbDocumentStore.cs` — internal-only engine; insert-run → transactional batch (100).
6. **All other providers** — drop public `RunInTransaction`, route through engine. (Grep first.)
7. Orleans — verify/migrate any `RunInTransaction` usage.

---

## Tests

- `tests/Shiny.DocumentDb.Tests` — at minimum:
  - `SaveChanges` commits all ops atomically; failure rolls back the whole unit (including generated-id rollback).
  - Coalescing: N same-type `Add`s issue chunked multi-row INSERTs (assert behaviorally — count/perf or via logging hook), not N single inserts.
  - Update/remove run: a contiguous run of same-type updates (or removes) commits inside the one transaction with a reused command — all rows applied, atomic rollback on a mid-run failure.
  - Mixed unit (insert run + update + insert run + remove) executes in buffer order and is fully atomic.
  - Ordering preserved across mixed ops: insert→remove same id, update-after-insert, interleaved types.
  - Generated IDs flow back onto caller objects after `SaveChanges`.
  - Chunk boundary correctness (> `BatchChunkSize` = 500 same-type adds).
  - Convenience methods still behave identically (Insert/BatchInsert/Update/Upsert/Remove/Clear).
  - Vector sidecar + temporal history still written inside the same txn for grouped inserts.
- `tests/Shiny.DocumentDb.Orleans.Tests` — run if Orleans touched.
- Run: `dotnet test tests/Shiny.DocumentDb.Tests/Shiny.DocumentDb.Tests.csproj` (+ Orleans suite for Orleans changes).

---

## Four-artifact sync (per CLAUDE.md — do in the same change)

1. **Code + tests** — above. Note provider compatibility tier: this is core `IDocumentStore` behavior, applies to all providers.
2. **Docs site** (`~/Desktop/dev/documentation/src/content/docs/documentdb/`):
   - Update `crud.mdx` (and any transactions/querying page) to teach `CreateUnitOfWork` + `SaveChanges` as the write-grouping model; remove `RunInTransaction` docs.
   - Add a **`type="breaking"`** release note in `release-notes.mdx` for the version (raw version from `version.json`, no prerelease suffix). New newest-version section at top; `## <version> TBD` if not yet released.
3. **Skill** (`skills/shiny-documentdb/SKILL.md`) — update default write guidance to UoW/`SaveChanges`; remove `RunInTransaction` guidance; update `triggers:` keyword list (`UnitOfWork`, `SaveChanges`, `CreateUnitOfWork`; drop `RunInTransaction`).
4. **`readme.md`** (repo root) — update feature list / inline guidance to the new model.

---

## Open / confirm-at-implementation

- **Version number**: 7.3 vs 8.0 for the breaking removal. (Note: interceptors land in the same release — a feature, not breaking — so the breaking removal here sets the version.)
- **`Commit` alias**: ship `[Obsolete]` `Commit` forwarder for one version, or hard-rename to `SaveChanges`.
- **1-op fast path**: only add if benchmarks show UoW overhead on single writes.
- **Final audit**: grep `RunInTransaction` across `src/` + Orleans + tests + both doc/skill artifacts so no public reference survives.

---

## Hand-off to the interceptors plan (`interceptors.md`)

When this plan is in place, leave the codebase shaped so interceptors wire in cleanly:

- **One dispatch point.** Per-op execution (insert/update/upsert/remove + the coalesced insert-run) all happens in the engine/`TransactionalDocumentStore` executor. That is the single place interceptors hook — do not scatter hook calls back into the public convenience methods. This is what eliminates the interceptor plan's "Gotcha #1" (writes through the nested store silently skipping interceptors): after this plan there is no second write path.
- **Before/after relative to the unit's single transaction.** A unit's per-op `BeforeWrite` fires before that op's core write; `AfterWrite` fires after it succeeds; the single `commit` happens after all ops. A throwing `BeforeWrite` aborts and rolls back the whole unit (consistent with how a unit already rolls back on any failure).
- **Coalesced batch must stay per-doc-hookable.** The insert-run dispatch (`BatchInsertCoreAsync` path) must remain structured as: loop before-hooks (mutable `Document`, pre-serialization) → serialize → multi-row INSERT → loop after-hooks (with generated `Id`/`Version` populated). Don't optimize the loop away in a manner that prevents per-document before/after firing.
- **`OnBeforeInsert` / `RunBeforeInsertHooksAsync`.** This plan keeps calling the existing `RunBeforeInsertHooksAsync`. The interceptor plan generalizes it into the interceptor pipeline and replaces that call site — leave it as a single, clearly-marked call so it's a clean swap.
- **Temporal `Source` ambient.** `Restore`'s inner `Insert`/`Update` become units-of-one; the interceptor plan's `AsyncLocal<DocumentOperationSource>` wraps them unchanged.
