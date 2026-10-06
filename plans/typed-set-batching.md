# Typed sets: buffered writes + transaction-aware reads (issue #10)

Status: **plan — partially done.** 14.1 shipped the minimal fix: `DocumentSet<T>` routes through the session's
`Target` (now `internal`), so set reads/writes/`Clear`/`Batch*` join an open transaction — covered by
`DocumentContextTransactionConformanceTests` on all 8 relational providers. The buffered-set redesign, guard (A) and
the rest below are still open. Source: https://github.com/shinyorg/DocumentDb/issues/10
("Batch operations freeze inside transaction", SQLite, 14.0.0).

## The bug

```csharp
using var context = factory.Create();
var tx = await context.BeginTransaction();
await context.MyModels.Clear();            // ← hangs forever on SQLite / DuckDB
await context.MyModels.BatchInsert(models);
await tx.Commit();
```

Root cause (verified in code, not yet reproduced by a test):

1. SQLite and DuckDB set `RequiresSingleConnection => true`, which puts `DocumentStore` in **shared mode**. Every
   operation waits on `sharedSemaphore` (`DocumentStore.cs:99`).
2. `BeginExplicitUnitAsync` takes that semaphore and **holds it until commit/rollback/dispose**
   (`DocumentStore.Session.cs:33`).
3. `DocumentSet<T>` sends everything to the **root** store: `IDocumentStore store => this.session.Store`
   (`DocumentSet.cs:31`). It never uses the session's transaction-aware `Target` (`DocumentSession.cs:76`).
4. `Clear()` → root store → `sharedSemaphore.WaitAsync(CancellationToken.None)` → waits on a lock this same async
   flow holds → deadlock. `Commit()` is never reached.

On the pooled providers (PG, SQL Server, MySQL, MariaDB, Cockroach, Oracle) it doesn't hang, but set writes run on
a separate connection and **commit outside the caller's transaction**. They can also block on row locks the
transaction holds.

Contributing cause: we call `DocumentContext` "EF-Core-style" (code comment, `context.mdx`, readme, skill). That
sets the reporter's expectation, but it isn't the design intent.

## Target design

`DocumentContext` is **not** an EF clone (no change tracking, no identity map). The typed set becomes a typed
**write buffer** plus an **immediate reader**, and everything routes through the session.

| Kind | `DocumentSet<T>` members | Behavior |
|---|---|---|
| Buffered | `Add`, `AddRange`, `Update`, `Upsert`, `Remove(id)`, `RemoveRange(ids)` | Queued on the context's session. Nothing touches the database until `db.SaveChanges()`, which flushes the queue grouped into the batch fast paths (`BatchInsert` / `BatchUpdate` / `BatchUpsert` / `BatchRemove`). With an explicit transaction open, the flush goes into it; otherwise `SaveChanges` opens and commits its own. |
| Immediate read | `Get`, `Query`, `Where`, `ToList`, `Count` | Run now, against the session's `Target` (the transaction-bound store while a transaction is open, else the root store). |
| Immediate set-based | `Clear` | Runs now, against `Target`, so it joins an open transaction. |

The buffered members return the set (or `void`), **not** a `Task`, and are synchronous. That mirrors the existing
`db.Add` / `IDocumentSession.Add` shape.

Buffered writes are not visible to reads until `SaveChanges`. This is already the documented session semantic
("a write buffer, not a change tracker") and stays the same.

The issue's code becomes:

```csharp
await using var tx = await db.BeginTransaction();
await db.MyModels.Clear();
db.MyModels.AddRange(models);
await db.SaveChanges();                  // one BatchInsert, inside tx
var all = await db.MyModels.ToList();    // reads inside tx, sees the inserts
await tx.Commit();
```

## Decisions

Recommendations are given; items marked **OPEN** need a sign-off before building.

1. **Immediate single-document writes on the set (`Insert`, immediate `Update`/`Upsert`/`Remove`)**: **OPEN.**
   Recommendation: delete outright (no-cruft rule). `Insert` → `Add`. `Update`/`Upsert`/`Remove` keep their names
   but become buffered (the return type changes from `Task`/`Task<bool>`, so every call site fails to compile —
   no silent behavior change). Immediate writes remain available via `db.Store.Insert(...)` / `db.Session.Store`.
2. **`BatchInsert` / `BatchUpsert` / `BatchUpdate` / `BatchRemove` on the set**: **OPEN.**
   Recommendation: delete from the set. `AddRange`/`RemoveRange` + `SaveChanges` reaches the same fast path. They
   stay on `IDocumentStore`.
3. **Overloads with no buffered equivalent**: `Update(doc, patch: true)` (RFC 7396 merge update) and
   `Upsert(patch, patchIfUpdate: false)` (replace-on-update). The buffer has only four op kinds. **OPEN.**
   - (a) Add `MergeUpdate` and `ReplaceUpsert` op kinds to `UnitOfWork` (plus `IDocumentSession` / `DocumentContext`
     overloads), coalescing into the per-item loop when no batch fast path exists. Or
   - (b) Drop these overloads from the set; callers use `db.Store`.
   Recommendation: (a) for parity. It's a moderate change in `UnitOfWork.cs` (two op records). Check the relational
   batch paths for merge support first. If they lack it, flush these ops one at a time inside the unit.
4. **Version**: **DECIDED — v15.** Bump `version.json` to `15.0.0`. Release notes go under a new `## 15.0 TBD`
   section at the top of `release-notes.mdx`.
5. **`Clear` inside a transaction on non-transactional providers**: `Target` is the root store there anyway (no
   explicit transaction engine). No change.

## Also in scope

### A. Deadlock guard for direct root-store calls (SQLite/DuckDB)

Routing the sets through `Target` fixes the reported path. But `db.Store.X(...)` or an injected `IDocumentStore`
used in the same flow while an explicit transaction holds the shared connection still deadlocks silently.

- Track the owning flow in shared mode: an `AsyncLocal<ExplicitUnit?>` (or an owner token) set in
  `BeginExplicitUnitAsync`, cleared on release.
- At the shared-semaphore acquire sites, if the current flow already owns the explicit unit, throw
  `InvalidOperationException`: "A transaction is open on this store's single connection; use the session
  (`db.Session` / typed sets) inside the transaction, not the root store."
- Other flows (a different request or thread) still wait normally. That's correct serialization, not a deadlock.
- Find every `sharedSemaphore.WaitAsync` site and route them through one helper so the check can't be missed.

### B. `IDocumentSession` surface

The set's immediate `Clear` needs `Target`. `Target` is private on `DocumentSession`. Options:

- Make it `internal` and let `DocumentSet` use it (the set already casts to `DocumentSession` for `EnterScope`).
  For a non-`DocumentSession` implementation, fall back to `session.Store`. Recommended; no public surface change.
- Also add `RemoveRange` to `IDocumentSession` / `DocumentContext` so the three surfaces match
  (`AddRange` already exists).

### C. Wording: drop "EF-Core-style"

- `src/Shiny.DocumentDb/DocumentContext.cs:6` and `:64`
- `documentation/.../documentdb/context.mdx:7`, and the `DbContext`/`IDbContextFactory` comparisons at 96, 106 and
  143 (keep lifetime guidance, drop the "it's EF" framing)
- `readme.md:49` and the "`DbContext`-style API" heading at `:158`
- `skills/shiny-documentdb/SKILL.md` (including the `db.Users.Insert(u)` example at ~1339)

### D. Incidental cleanup found while reading (same change)

- `UnitOfWork.Commit()` is `[Obsolete]` — delete it (no-cruft rule).
- `UnitOfWork`/`IUnitOfWorkEngine` XML docs still reference the removed `IDocumentStore.CreateUnitOfWork` — fix the
  crefs to point at `IDocumentSession`.
- The `DocumentSet.cs:28-29` comment ("do not join the context's explicit transaction") becomes false — rewrite it.

## Implementation steps

1. **Failing tests first** (`tests/Shiny.DocumentDb.Tests`)
   - SQLite: the issue repro (`BeginTransaction` → set `Clear` → `AddRange` → `SaveChanges` → `ToList` → `Commit`),
     wrapped in a timeout so a regression fails instead of hanging the suite.
   - Rollback: same flow, then `tx.Rollback()` / dispose → the table is unchanged (proves set writes joined the
     transaction).
   - Buffering: `db.X.Add(...)` without `SaveChanges` → nothing persisted; after `SaveChanges` → persisted.
   - Coalescing: `AddRange` of N → one `BatchInsert` (assert via the existing batch span/metric, or a counting
     interceptor).
   - Guard (A): `db.Store.Insert(...)` inside an open SQLite transaction on the same flow throws (no hang); a
     different flow waits and then succeeds after commit.
   - Pooled providers: add the rollback test to the relational conformance suite so PG, SQL Server, MySQL, etc.
     prove set writes/reads join the transaction.
   - DuckDB: same as SQLite (shared mode).
2. **`DocumentSession`**: expose `Target` internally; add `RemoveRange`.
3. **`UnitOfWork`**: `MergeUpdate` / `ReplaceUpsert` op kinds (if decision 3a); delete `Commit()`; fix crefs.
4. **`DocumentSet<T>`**: new buffered members → `session.Add/AddRange/Update/Upsert/Remove/RemoveRange`.
   Reads and `Clear` → `Target`. Delete the members per decisions 1 and 2. Keep `EnterScope` only where still needed
   (reads; `SaveChanges` already flows scope).
5. **`DocumentContext`**: add `RemoveRange` (and the merge/replace overloads if 3a); update the XML docs.
6. **Shared-mode guard (A)** in `DocumentStore` / `DocumentStore.Session.cs`.
7. **Update call sites**: `TypedContextTests` (13), `GeneratedContextTests` (8), `ScopedInterceptorTests` (3),
   `DocumentContextHookTests` (2), `TenantRoutingTests` (1), Aspire `ClientContextTests` (2). Check that the
   scoped-interceptor tests still see the caller's scope through `SaveChanges` (the session already flows it).
   The generator (`DocumentContextGenerator.cs`) only emits `Set<T>()` properties — no change expected. Confirm
   with the generator tests.
8. **Full suite** with Docker: `tests/Shiny.DocumentDb.Tests` (+ Orleans tests if touched, + Aspire tests for the
   context fixtures).
9. **Docs** (`~/Desktop/dev/documentation/src/content/docs/documentdb/`)
   - `context.mdx`: rewrite the typed-set section (buffered vs immediate table, transaction example from above),
     and fix the 9 `db.X.Insert/...` samples.
   - `interceptors.mdx` and `multi-tenancy.mdx`: one sample each.
   - Leave the historical v12 blog post and old release notes alone.
   - Release notes under `## 15.0 TBD`: one `type="breaking"` note (set writes are buffered; `Insert`/
     `Batch*` removed from sets; migration: `db.X.Insert(d)` → `db.X.Add(d); await db.SaveChanges();`), and one
     `type="fix"` note (typed sets and `Clear`/reads join an open transaction; SQLite/DuckDB deadlock fixed; root-store
     re-entry now throws instead of hanging).
10. **Skill + readme**: buffered-set guidance, `triggers:` (add `RemoveRange`), drop the EF framing.
11. **Reply on issue #10**: explain the cause, give the workaround for 14.0 (`context.Session.Query<T>().ExecuteDelete()`
    + `context.Add(...)` + `context.SaveChanges()` + `context.Session.Query<T>().ToList()`, all inside the
    transaction), and say the fix ships in v15.

## Risks

- **Silent semantic change**: avoided — the buffered `Update`/`Upsert`/`Remove` change return type, so old
  `await db.X.Update(d)` fails to compile (you can't `await` a non-Task) instead of silently not writing. Verify
  this holds for every overload, including a fluent return of `DocumentSet<T>`.
- **Forgotten `SaveChanges`**: a buffered write with no `SaveChanges` is dropped on dispose. That's the existing
  session behavior; call it out in the docs. Optional: a debug-only warning log when a session is disposed with
  `PendingCount > 0`.
- **Guard false positives**: an interceptor using `ctx.Store` during a flush is already handed the transaction-bound
  store, so it isn't affected. Test that an outbox interceptor inside a SQLite explicit transaction still works.
