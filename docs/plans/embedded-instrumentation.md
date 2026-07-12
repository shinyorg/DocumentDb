# Plan: Embedded OpenTelemetry instrumentation (replace the decorator)

**Status:** In progress (branch `feature/di-scoped-interceptors`).
**Target version:** `11.0`. **Breaking** — the `AddDocumentStoreInstrumentation` extension methods and the
`DocumentStoreOptions.Instrumentation` flag are **removed outright** (no `[Obsolete]` shims — see the
"Removing or replacing code" rule in `CLAUDE.md`).

---

## The request (maintainer)

> Why not use an `ActivitySource` directly in the library — if the `ActivitySource` has no listener
> (OTel/diagnostics not set up), `StartActivity` returns null and we just continue on? **Full replace** of the
> decorator with embedded, always-on, zero-cost-when-unobserved instrumentation. Don't deprecate the old
> methods — just remove them; this is open source, no leftover cruft.

Rationale: the embedded `ActivitySource`/`Meter` pattern is the idiomatic .NET approach (EF Core, Npgsql,
HttpClient, StackExchange.Redis). It removes the opt-in decorator, covers the **container-free**
`new …DocumentStore(options)` path the decorator never could, and can emit internal detail the decorator
(sitting at the public boundary) can't see.

## The concern that was raised

`using var activity = Source.StartActivity(...)` ends the span on dispose **and** sets `Activity.Current`
(AsyncLocal), so children nest under it. The decorator saw exactly one operation per public call because it
was blind to the store's internals. **Embedded**, the store re-enters itself at layers the decorator never
saw:

- `DocumentStore.Insert` → the **implicit one-op unit of work** (added for scoped interceptors) →
  `RunUnitAsync` → `TransactionalDocumentStore.Insert`;
- a temporal `Restore` calls `Update`;
- sidecar writes (spatial/vector/history) run within a write.

A naive per-method wrap would emit **nested/duplicate spans** and **double-count** the operations counter for
one logical call. Streaming adds a second subtlety: a span held open across `yield return` leaves
`Activity.Current` set in the consumer's code between `MoveNext`s.

## Options considered

1. **Embedded + `AsyncLocal` re-entrancy guard** — only the outermost tracked operation on a flow emits;
   nested ones run span-free. *(CHOSEN.)*
2. **Hybrid** — keep the decorator as an always-on public-boundary wrapper, add a static `ActivitySource`
   only for the container-free path. Clean span boundaries, but two mechanisms.
3. **Keep the decorator**, just make it always-on and drop the opt-in flag.

## Outcome (chosen: option 1)

Embedded instrumentation with a re-entrancy guard.

- **Static engine** (`Diagnostics/DocumentStoreMetrics.cs`): a process-wide `ActivitySource` + `Meter` named
  `Shiny.DocumentDb`, static `StartActivity` / `Record`. No `IMeterFactory`/DI dependency — identical on every
  construction path (DI and container-free). Zero-cost when unobserved.
- **`OperationTracker`** carries the `AsyncLocal<bool>` guard. `Track` / `Track<T>` skip emitting when nested.
  `TrackStream` skips when nested but does **not** set the guard, so operations the consumer performs *during*
  enumeration still record their own spans.
- Each store holds an `OperationTracker` (system = provider name; `db.namespace` = the keyed store name when
  registered via `AddDocumentStore(name, …)`). Public `IDocumentStore` methods + `RunUnitAsync` are wrapped;
  `TransactionalDocumentStore` and internal `ExecuteAsync`/sidecars are **not** (the guard covers them).
- **Removed outright:** `InstrumentedDocumentStore`, `InstrumentedDocumentQuery`,
  `DiagnosticsServiceCollectionExtensions` (`AddDocumentStoreInstrumentation`), and
  `DocumentStoreOptions.Instrumentation` + its auto-wire in `AddDocumentStore`.

### Accepted trade-offs (documented, not accidental)

- **Per-operation child spans inside a user `UnitOfWork` go away.** The decorator recorded each buffered op
  inside `SaveChanges` as a child span of the transaction; with the guard, a `SaveChanges` emits **one**
  `transaction` span and the inner ops run span-free. The transaction span (name, duration, outcome) remains.
- **A store op invoked *during* consumption of a `QueryStream`** is still instrumented (guard not set by
  streaming), but a `QueryStream` *nested inside* another tracked op is enumerated span-free.

### Coverage / phasing

Operation names and `db.*` tags match the former decorator so existing OTel dashboards keep working.
`InstrumentationTests` are rewritten to assert always-on embedded behavior via an `ActivityListener` /
`MeterListener`, isolated per-test by a unique keyed `db.namespace` (the static source emits suite-wide).

**Done (green, full suite passing):**
- Static engine + re-entrancy guard; decorator / `AddDocumentStoreInstrumentation` / `options.Instrumentation`
  removed outright; `Aspire.Client` manual decoration removed (store is always-on).
- Core relational `DocumentStore`: `Insert`, `Update`, `Upsert`, `Remove`, `BatchInsert`, `Get`, `Query`
  (string), `QueryStream`, `Count`, `Clear`, and `RunUnitAsync` (`transaction`).
- Shared fluent `DocumentQuery<T>`: `ToList` (`query.to_list`).

**Remaining (mechanical, same pattern):**
- Core: the boolean-patch `Update`/`Upsert` overloads, `BatchUpsert`/`BatchUpdate`/`BatchRemove`,
  `SetProperty`/`RemoveProperty`, `GetDiff`, `ClearAll`, spatial (`WithinRadius`/`WithinBoundingBox`/
  `NearestNeighbors` + the `Geo*` set), `NearestVectors`, `FullTextSearch`, temporal (`History`/`AsOf`/
  `AsOfAll`/`ChangesByActor`/`ChangesBetween`/`Restore`/`GetDiffBetween`), and the late-bound JSON lane.
- The remaining `DocumentQuery<T>` terminals (`Count`/`Any`/`First*`/`Single*`/`Sum`/`Min`/`Max`/`Avg`/
  `Paginate`/`ExecuteUpdate`/`ExecuteDelete`/`ToArray`/GroupBy) and the six non-relational provider stores
  (+ their own query builders), which lost decorator-based telemetry until embedded.
