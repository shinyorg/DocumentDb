# Plan: OpenTelemetry instrumentation for keyed / named document stores

**Status:** Designed, not started.
**Target version:** next patch/minor off `v10` (raw version from `version.json`). Additive — no breaking
changes. Existing non-keyed instrumentation behavior is unchanged; new overload + keyed flag path are new.

> Self-contained build spec. Read `CLAUDE.md` (repo root) for the four-artifact rule (code+tests, docs
> site, skill, readme) before considering any commit "done". Branch off `v10`.

---

## Goal

Let OpenTelemetry instrumentation decorate **keyed/named** `IDocumentStore` registrations, not just the
single non-keyed one. Concretely:

1. A new `AddDocumentStoreInstrumentation(string name)` overload that decorates the keyed store registered
   under `name`.
2. Make `DocumentStoreOptions.Instrumentation = true` work on the **eager keyed** overload
   `AddDocumentStore(name, Action<DocumentStoreOptions>)` (today it throws `NotSupportedException`).
3. Tag each instrumented store with its name so metrics/spans from multiple stores are distinguishable.

Out of scope: the lazy keyed overload `AddDocumentStore(name, Action<IServiceProvider, DocumentStoreOptions>)`
— its options are configured at resolve time, so the flag can't be read at registration. Consumers of that
overload call `AddDocumentStoreInstrumentation(name)` explicitly. Document this; don't try to auto-wire it.

## Why it's limited today

`AddDocumentStoreInstrumentation()` in `src/Shiny.DocumentDb.Diagnostics/ServiceCollectionExtensions.cs`
decorates by **find-remove-replace on a single descriptor**, hard-filtered to `!d.IsKeyedService`:

```csharp
var descriptor = services.LastOrDefault(d => d.ServiceType == typeof(IDocumentStore) && !d.IsKeyedService)
    ?? throw ...;
services.Remove(descriptor);
// rebuild inner factory, re-add non-keyed InstrumentedDocumentStore, re-point ITemporalDocumentStore
```

Keyed stores are registered per name via `AddKeyedSingleton<IDocumentStore>(name, ...)` (see
`ServiceCollectionExtensions.AddDocumentStore(name, ...)`), resolved through
`IDocumentStoreProvider.GetStore(name)` → `GetRequiredKeyedService<IDocumentStore>(name)`. The decorator
never touches those descriptors.

**Good news that makes this cheaper than expected:** `InstrumentedDocumentStore` already implements
`IDocumentStore` + `ITemporalDocumentStore` + `IObservableDocumentStore` + `IChangeFeedDocumentStore`
faithfully (casts keep working; inner is on `.Inner`). Keyed stores are consumed by resolving the keyed
`IDocumentStore` and casting to the capability interface — so decorating the keyed `IDocumentStore`
descriptor is **sufficient**; there is no separate keyed `ITemporalDocumentStore` descriptor to re-point
(unlike the non-keyed path). One descriptor rewrite per key, done.

## Implementation

### 1. Keyed decorate overload — `Shiny.DocumentDb.Diagnostics/ServiceCollectionExtensions.cs`

Add:

```csharp
public static IServiceCollection AddDocumentStoreInstrumentation(this IServiceCollection services, string name)
```

- `ArgumentException.ThrowIfNullOrWhiteSpace(name)`.
- `services.AddMetrics(); services.TryAddSingleton<DocumentStoreMetrics>();` (same as non-keyed).
- Find: `services.LastOrDefault(d => d.ServiceType == typeof(IDocumentStore) && d.IsKeyedService && Equals(d.ServiceKey, name))`
  — throw `InvalidOperationException` naming `name` if absent.
- Remove it, rebuild the **inner keyed factory** from the descriptor, handling the three keyed shapes:
  - `descriptor.KeyedImplementationFactory` → `Func<IServiceProvider, object?, object>` (the common case,
    since the DI overloads register with `(sp, _) => new DocumentStore(...)`).
  - `descriptor.KeyedImplementationInstance`.
  - `descriptor.KeyedImplementationType` → re-add the concrete type as a keyed service and resolve it.
- Re-add: `services.Add(new ServiceDescriptor(typeof(IDocumentStore), name, (sp, key) =>
  new InstrumentedDocumentStore(inner(sp, key), sp.GetRequiredService<DocumentStoreMetrics>(), storeName: name),
  descriptor.Lifetime));`
- **Do not** re-point any capability interface (keyed capabilities aren't separately registered — the cast on
  the decorated instance already works).

Refactor the shared body (metrics registration + factory-shape switch + wrap) into a private helper used by
both the keyed and non-keyed overloads to avoid duplication.

### 2. Store-name tag — `DocumentStoreMetrics` + `InstrumentedDocumentStore`

Thread an optional store name into the decorator so measurements/spans can be told apart:

- `InstrumentedDocumentStore(IDocumentStore inner, DocumentStoreMetrics metrics, string? storeName = null)`.
  Keep the existing 2-arg constructor working (default `storeName: null`) so the direct-construction path and
  existing tests are unaffected.
- Add the tag on every metric measurement and span **only when `storeName` is non-null**. Use the OTel
  convention **`db.namespace`** for the logical store name (fits "logical database name"); if the team
  prefers a vendor tag, use `shiny.documentdb.store` — pick one, be consistent, document it in
  `diagnostics.mdx`'s tag table.
- The non-keyed `AddDocumentStoreInstrumentation()` passes no name → tag omitted (byte-for-byte current
  behavior; existing `InstrumentationTests` assertions on tags stay green).

### 3. DI flag wiring — `Shiny.DocumentDb.Extensions.DependencyInjection/ServiceCollectionExtensions.cs`

- **Eager keyed overload** `AddDocumentStore(name, Action<DocumentStoreOptions>)`: replace the current
  `if (options.Instrumentation) throw new NotSupportedException(...)` guard with, **after** the
  `AddKeyedSingleton` + `TryAddSingleton<IDocumentStoreProvider>` lines:
  `if (options.Instrumentation) services.AddDocumentStoreInstrumentation(name);`
- **Keyed multi-tenant overload** delegates to the eager one → inherits the behavior for free.
- **Lazy keyed overload** `AddDocumentStore(name, Action<IServiceProvider, DocumentStoreOptions>)`: leave as
  is (options not known at registration). Its XML doc should note: to instrument, call
  `AddDocumentStoreInstrumentation(name)` explicitly.
- Non-keyed overloads: unchanged.

### 4. Update `DocumentStoreOptions.Instrumentation` doc (core)

The XML summary currently says "Only honored by the non-keyed `AddDocumentStore` overloads." Change to: honored
by the non-keyed overloads **and** the eager keyed overload `AddDocumentStore(name, Action<DocumentStoreOptions>)`;
the lazy `(IServiceProvider, DocumentStoreOptions)` overload still requires an explicit
`AddDocumentStoreInstrumentation(name)` call.

## Tests — `tests/Shiny.DocumentDb.Tests/InstrumentationTests.cs`

Add:
1. **Keyed flag decorates** — `AddDocumentStore("orders", o => { o.DatabaseProvider = …; o.Instrumentation = true; })`,
   then assert both `sp.GetRequiredKeyedService<IDocumentStore>("orders")` and
   `sp.GetRequiredService<IDocumentStoreProvider>().GetStore("orders")` are `InstrumentedDocumentStore`.
2. **Explicit keyed overload** — register a keyed store without the flag, call
   `AddDocumentStoreInstrumentation("orders")`, assert decorated.
3. **Isolation** — two keyed stores, only one instrumented → the other resolves undecorated.
4. **Name tag** — two instrumented keyed stores (`"a"`, `"b"`); run an op on each; assert the
   `db.namespace` (or chosen tag) on measurements partitions correctly. Reuse the existing
   `TelemetryCollector`.
5. **Missing key throws** — `AddDocumentStoreInstrumentation("nope")` with no such keyed store →
   `InvalidOperationException` naming the key.
6. **Non-keyed unchanged** — existing 7 tests must still pass (no `db.namespace` tag on the non-keyed path).

Verify Sqlite suite: `dotnet test tests/Shiny.DocumentDb.Tests/Shiny.DocumentDb.Tests.csproj --filter "FullyQualifiedName~InstrumentationTests"`.

## Four artifacts

- **Docs** `documentation/.../diagnostics.mdx`: remove the "keyed store throws `NotSupportedException`" caveat
  from the DI-flag section; document `AddDocumentStoreInstrumentation(string name)`; add the store-name tag
  (`db.namespace`) to the tag table with a note it's present only for named stores.
- **Skill** `skills/shiny-documentdb/SKILL.md`: update the Telemetry section's keyed caveat; mention the
  keyed overload + name tag.
- **readme.md**: update the diagnostics paragraph (drop "keyed throws"; add named-store support).
- **Release note** `release-notes.mdx`: one `<RN type="enhancement">` under the current/next version —
  "Instrumentation now supports keyed/named stores (`AddDocumentStoreInstrumentation(name)` and
  `Instrumentation = true` on named registrations), tagged with `db.namespace`."

## Edge cases / decisions to make during build

- **Double-wrap idempotency.** Calling `AddDocumentStoreInstrumentation(name)` twice (or flag + explicit call)
  would double-decorate. The factory shape makes a pre-check hard. Cheapest guard: in the wrap factory, if
  `inner is InstrumentedDocumentStore` return it as-is. Add a test for the flag+explicit double path.
- **Tag choice** `db.namespace` vs `shiny.documentdb.store` — decide up front; the docs tag table must match.
- **`IChangeFeedDocumentStore` / `IObservableDocumentStore` on keyed stores** already work through the
  decorator's faithful implementation; no extra descriptors. Confirm with a keyed `NotifyOnChange` smoke assert
  if cheap, else rely on the existing capability-faithfulness coverage.
