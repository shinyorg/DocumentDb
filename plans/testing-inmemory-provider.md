# Plan: Testing package + in-memory provider (`Shiny.DocumentDb.Testing`)

**Status:** Designed, not started.
**Target version:** `12.6` (new package; no core changes expected — if one is needed, it means the shared
provider surface has a gap worth fixing anyway).
**Package:** `Shiny.DocumentDb.Testing` — an in-memory `IDocumentStore` plus test helpers. No test-framework
dependency (works with xUnit/NUnit/MSTest/TUnit alike).

> Self-contained build spec. Read `CLAUDE.md` (repo root) for the four-artifact rule (code+tests, docs site,
> skill, readme) before considering any commit "done". Branch off `v12`.

---

## Goal

Give consumers a store they can `new` up in a unit test — no file, no Docker, no cleanup, deterministic clock,
and the ability to make writes fail on demand:

```csharp
var store = new InMemoryDocumentStore(o =>
{
    o.TimeProvider = fakeClock;
    o.ConfigureDocument<Order>(cfg => cfg.AddSoftDelete(x => x.DeletedAt));
});

await store.Insert(new Order { Total = 100 });
Assert.Single(await store.Query<Order>().Where(x => x.Total > 50).ToList());

// and the thing SQLite cannot do:
store.Faults.OnNextWrite<Order>(new ConcurrencyException(...));
await Assert.ThrowsAsync<ConcurrencyException>(() => svc.PlaceOrder());
```

Today the fastest option for a consumer is SQLite (file or `:memory:`), which is genuinely good — but it needs a
real file lifecycle or connection pinning, it cannot simulate transient failures, it is not available on Blazor
WASM, and it makes "unit" tests do I/O. Every other provider needs Docker.

## Why now (and why it is cheap)

The `v12` shared-provider-surface work is the enabler. A provider is now: derive `DocumentQueryBase<T>`, return
`QueryExecution<T>.Candidates(...)`, and plug into the shared write pipeline + `DocumentMappingRegistry` +
`IDocumentStoreOptions`. Roughly 4,300 lines of per-provider duplication were deleted to make exactly this kind
of thing small. An in-memory backend is the cheapest possible consumer of that surface — and building it is a
**free conformance audit** of the shared surface: anything it cannot implement without touching core is a real
gap in the abstraction.

## Non-goals

- **Not a replacement for provider integration tests.** It cannot tell you your PostgreSQL `jsonb` path works.
  The docs must lead with this, and the package must not be positioned as "test your data layer here".
- **No persistence.** Not to disk, not across process restarts. A snapshot/restore helper for fixtures is in
  scope; a file format is not.
- **No SQL, no `ToQueryString`, no `EXPLAIN`.** There is no engine to explain.
- **No JSON-collection lane in v1.** That lane is relational-only today; adding it here would create the only
  non-relational implementation and set an expectation the other providers do not meet.

## Design decisions (locked)

| Decision | Choice | Consequence |
|---|---|---|
| Storage | `ConcurrentDictionary<string typeName, ConcurrentDictionary<string id, StoredDoc>>` holding **serialized JSON**, not CLR objects | Serialization bugs (converters, naming policy, AOT resolvers, field-level encryption) surface here exactly as they do on a real provider. Storing live objects would hide them and hand out shared mutable references. |
| Query execution | `QueryExecution<T>.Candidates` — the base does filtering, ordering, paging | Zero query code. Behavior is by definition identical to every other client-side provider. |
| Transactions | Snapshot/rollback of the affected type maps; `SupportsTransactions => true` | Makes it usable for unit-of-work and outbox tests, which is a large fraction of what people want a fake for. |
| Clock | `TimeProvider` throughout | `UpdatedAt`, temporal history, soft-delete timestamps, cache expiry all become deterministic. |
| Fault injection | First-class (`store.Faults`) | The one capability no real provider can offer conveniently, and the reason to prefer this over SQLite. |
| Conformance | Runs the existing shared conformance suites in CI | The only defense against a fake that quietly disagrees with reality. |

---

## Capability tiers (decide up front, document exactly)

| Capability | In-memory | Notes |
|---|---|---|
| CRUD, batch, patch/merge, `SetProperty`/`RemoveProperty`, `GetDiff` | ✅ | Shared write pipeline. |
| Typed + string-grammar queries, ordering, paging, cursor paging | ✅ | Reuse the client-side evaluation path LiteDB/IndexedDB already use — **do not write a third evaluator**. |
| Aggregates, `GroupBy` | ✅ | Base class defaults (client-side) already do this. |
| Query filters, soft delete, interceptors, sessions, transactions | ✅ | All core, all provider-agnostic. |
| Optimistic concurrency | ✅ | Version counter per stored doc. |
| Temporal history | ✅ | Append a history list per document — cheap, and makes temporal unit-testable for the first time. |
| Blobs | ✅ | Sidecar dictionary. |
| Change observation (`IObservableDocumentStore`), `NotifyOnChange` | ✅ | In-process broadcast; makes SSE/live-query tests possible without a database. |
| Vector search | ✅ brute force | Linear cosine/L2 scan. Exact by construction — no ANN approximation. |
| Full-text search | ⚠️ naive | Tokenize + match, no stemming/ranking parity. Report `SupportsFullText = true` but **document that scores and stemming differ from every engine**; consider `false` if that surprise is worse than the convenience. Decide before coding. |
| Spatial | ✅ | Reuse the existing managed geometry algorithms. |
| Computed properties | ✅ | Alias form only; "materialized/indexed" is meaningless here. |
| JSON collections, `ToQueryString`, `EXPLAIN`, bulk copy, change feed | ❌ | Throw `NotSupportedException` with the standard message shape. |
| Backup/restore, seeding, `ClearAll` | ✅ | Provider-agnostic; also gives fixtures a fast reset. |

---

## Surface

```csharp
public sealed class InMemoryDocumentStoreOptions : IDocumentStoreOptions   // interceptors, query filters, mappings
{
    public TimeProvider TimeProvider { get; set; } = TimeProvider.System;
    public JsonSerializerOptions? SerializerOptions { get; set; }
    /// <summary>Adds latency to every operation, to shake out missing awaits and race conditions.</summary>
    public TimeSpan OperationDelay { get; set; }
}

public sealed class InMemoryDocumentStore : IDocumentStore, IObservableDocumentStore, ITemporalDocumentStore,
                                            IBlobDocumentStore, IDocumentBackup, IDocumentMaintenance, IDisposable
{
    public InMemoryDocumentStore(Action<InMemoryDocumentStoreOptions>? configure = null);

    /// <summary>Fault injection — see <see cref="DocumentFaultInjector"/>.</summary>
    public DocumentFaultInjector Faults { get; }

    /// <summary>Everything currently stored for a type, deserialized. For assertions.</summary>
    public IReadOnlyList<T> Snapshot<T>() where T : class;

    /// <summary>Every write the store has seen, in order — the assertion surface for "did my code write?".</summary>
    public IReadOnlyList<RecordedOperation> Operations { get; }

    public void Reset();
}

public sealed class DocumentFaultInjector
{
    public void OnNextWrite<T>(Exception error) where T : class;
    public void OnNextRead<T>(Exception error) where T : class;
    public void Always<T>(Func<DocumentOperation, Exception?> policy) where T : class;
    /// <summary>Fail the Nth call, then succeed — for retry-policy tests.</summary>
    public void FailTimes<T>(int count, Exception error) where T : class;
    public void Clear();
}

// DI
public static IServiceCollection AddInMemoryDocumentStore(this IServiceCollection services, Action<InMemoryDocumentStoreOptions>? configure = null);
public static IServiceCollection AddInMemoryDocumentStore(this IServiceCollection services, string name, Action<InMemoryDocumentStoreOptions>? configure = null);
```

`RecordedOperation` carries operation, type name, id, and the serialized JSON — enough for "assert we upserted
the customer with `Status = active`" without a query.

---

## Tests

**The important test is not a new test file — it is running the existing suites against the new provider.**

- Add an `InMemoryStoreFixture : IDocumentStoreFixture` to `tests/Shiny.DocumentDb.Tests/Fixtures`, and wire it
  into `DocumentQueryConformanceTests` and every other conformance/provider-parametrized suite whose capabilities
  it claims (CRUD, batch, temporal, blob, backup, soft delete, interceptors, sessions, change monitoring,
  aggregates/GroupBy, cursor paging, spatial, vector).
- Any conformance test it fails is either a provider bug **or** a documented capability gap — no third option,
  and no quiet `[Fact(Skip)]`.

Then a small suite of its own (`tests/Shiny.DocumentDb.Testing.Tests`):

- Fault injection: next-write fails then succeeds; `FailTimes(2)` drives a Polly-style retry to success.
- `TimeProvider`: `UpdatedAt`, soft-delete stamps, and temporal `AsOf` all move only when the fake clock does.
- Isolation: two `InMemoryDocumentStore` instances share nothing; `Reset()` clears everything including blobs,
  history, and recorded operations.
- Serialization fidelity: a type with a custom converter / naming policy / source-gen context round trips
  identically to SQLite (run the same assertions against both fixtures).
- Transactions: rollback restores the pre-transaction snapshot, including temporal history.
- Thread safety: 100 concurrent writers to the same type produce 100 documents and no torn state.

## Four-artifact checklist

- **Code + tests** — as above; project into `DocumentDb.slnx` and `build.slnf`.
- **Docs** — new `testing.mdx`: the capability table above (verbatim — it is the contract), the "this does not
  replace provider integration tests" warning up top, fault-injection recipes, and a comparison with SQLite
  `:memory:` (when each is the right call). Add a row to `providers.mdx` marking it **test-only, never for
  production**. Release note `type="feature"`.
- **Skill** — a testing section showing the fixture pattern and the fault injector; `triggers:` += testing,
  unit test, in-memory, fake, mock.
- **readme.md** — feature bullet + badge, with the test-only qualifier in the sentence itself.

## Risks

- **False confidence** is the entire risk of this package. Mitigations, in order of importance: (1) run the
  shared conformance suites against it in CI; (2) lead the docs with the limitation; (3) name it
  `Shiny.DocumentDb.Testing`, not `.InMemory`, so the intended use is in the package id; (4) never let it
  quietly accept something a real provider rejects.
- **Full-text parity is unwinnable.** Decide the `SupportsFullText` answer deliberately (see the tier table) —
  a naive matcher that reports `true` will produce tests that pass in memory and fail on PostgreSQL.
- **Scope drift into "production-lite".** People will ask for persistence. The answer is SQLite, and it is a
  good answer.
