# Plan: Aspire support for the typed `DocumentContext` / multi-store patterns

**Status:** Designed, not started.
**Target version:** `11.x` (additive — new client helper, no breaking changes). Bump the raw
`version.json` minor when cut.

> Self-contained build spec. Read `CLAUDE.md` (repo root) for the four-artifact rule (code+tests, docs
> site, skill, readme) before considering any commit "done". Branch off `v11`. The docs site is the
> **separate** repo at `~/Desktop/dev/documentation`.

---

## Goal

Let an Aspire-hosted service back a source-generated `DocumentContext` (EF-style `DocumentContext` +
`DocumentSet<T>`, shipped v10) with an Aspire-provisioned database resource — resolving the connection
string + provider discriminator from Aspire config exactly the way `AddDocumentStore` already does for
the keyed-store pattern, and wiring the health check + OpenTelemetry the same way.

Today the Aspire client (`Shiny.DocumentDb.Aspire.Client`) exposes **only** `AddDocumentStore(builder,
name, …)`, which registers a keyed-by-name `IDocumentStore`. There is no path to point a `DocumentContext`
at an Aspire resource — a consumer would have to hand-read `ConnectionStrings:<name>` +
`Shiny:DocumentDb:<name>:Provider` and `new XDatabaseProvider(...)` themselves, defeating the integration.

**In scope:** the relational + SQLite provider family already covered by `DocumentProviderKind`
(`Sqlite, Postgres, CockroachDb, SqlServer, MySql, MariaDb`). NoSQL providers (Redis/AzureTable/DynamoDB,
Mongo/Cosmos) are a **separate, larger effort** — see [Not in scope](#not-in-scope).

## Why this can't be a generic `AddDocumentContext<TContext>` in the client

The source generator (`DocumentContextGenerator.cs`) emits, per context, a **public** entry point:

```csharp
public static IServiceCollection AddOrdersContext(
    this IServiceCollection services,
    Action<DocumentStoreOptions> configure)
    => DocumentContextServiceCollectionExtensions.AddDocumentContext<global::OrdersContext>(
        services,
        options => { OrdersContext.ConfigureModel(options); configure(options); },
        static session => new global::OrdersContext(session));
```

- `OrdersContext.ConfigureModel(...)` is **`internal`** (`DocumentContextGenerator.cs:216`) — the Aspire
  client assembly cannot call it.
- The activator (`session => new OrdersContext(session)`) is likewise only known to the generated code.

So the client **cannot** reimplement context registration generically. It must **compose with** the
generated `Add{Context}` / `Add{Context}Factory` method: the caller invokes the generated method, and the
Aspire client supplies the `Action<DocumentStoreOptions>` that points the context's store at the Aspire
resource.

## Design decision (locked)

| Decision | Choice | Consequence |
|---|---|---|
| API shape | A helper returning `Action<DocumentStoreOptions>` that the caller passes to the generated `Add{Context}` | Composes with both the scoped `Add{Context}` and the factory `Add{Context}Factory` variants with one helper; no generator changes. |
| Provider scope | Relational + SQLite only (existing `DocumentProviderKind`) | Reuses `DatabaseProviderFactory` unchanged. |
| Health/OTel | Wired by the helper, honoring `DocumentStoreSettings` | Same behavior as `AddDocumentStore`; keyed off `name`. |

Rejected: a first-class generic `AddDocumentContext<TContext>(builder, name, …)` — impossible without the
internal `ConfigureModel` + activator (see above). Rejected: emitting an Aspire-aware overload from the
generator — couples the generator to the Aspire package.

## Deliverable — client helper

New public method on `Shiny.DocumentDb.Aspire.Client.DocumentStoreClientExtensions`:

```csharp
/// Resolves the Aspire-injected connection string (ConnectionStrings:<name>) + provider discriminator
/// (Shiny:DocumentDb:<name>:Provider) for <paramref name="name"/>, wires the store health check + OTel
/// (honoring DocumentStoreSettings), and returns the Action you hand to a source-generated
/// Add{Context}/Add{Context}Factory method so the context's store targets the Aspire resource.
public static Action<DocumentStoreOptions> AddDocumentContextProvider(
    this IHostApplicationBuilder builder,
    string name,
    Action<DocumentStoreSettings>? configureSettings = null,
    Action<DocumentStoreOptions>? configureOptions = null);
```

Consumer usage:

```csharp
// scoped (ASP.NET Core):
builder.AddOrdersContext(builder.AddDocumentContextProvider("orders"));

// factory (MAUI/Blazor/desktop/background):
builder.AddOrdersContextFactory(builder.AddDocumentContextProvider("orders"));
```

### Implementation notes

- **Refactor first.** Extract the shared "resolve conn string → resolve `DocumentProviderKind` → wire
  health check → wire OTel" block currently inlined in `AddDocumentStore`
  (`DocumentStoreClientExtensions.cs:35-86`) into a private helper. Both `AddDocumentStore` and
  `AddDocumentContextProvider` call it — no duplication (CLAUDE.md "no leftover cruft").
- The returned `Action<DocumentStoreOptions>` sets
  `o.DatabaseProvider = DatabaseProviderFactory.Create(kind, connectionString)` and then invokes the
  caller's `configureOptions`. It runs **inside** the generated method's configure delegate, i.e. after
  `ConfigureModel`, so context model mappings are already applied.
- Health check name stays `DocumentDb.{name}`; `DocumentStoreHealthCheck(kind, connString)` self-builds a
  provider and is independent of whether the store is keyed by name or by context type. No change needed.
- Honor `DocumentStoreSettings.DisableHealthChecks / DisableTracing / DisableMetrics / MultiTenant /
  ConnectionString / Provider` identically to `AddDocumentStore`.
- **Multi-store note:** because each `DocumentContext` registers its store keyed by `typeof(TContext)`
  (`DocumentContextServiceCollectionExtensions.cs:73`), multiple contexts already coexist. Calling
  `AddDocumentContextProvider` once per context (each with its own Aspire `name`) is the multi-store story
  — no extra work.

## Tests

`tests/Shiny.DocumentDb.Aspire.Tests` currently does **not** reference the source generator. To test a
real generated context:

1. Add to `Shiny.DocumentDb.Aspire.Tests.csproj`:
   ```xml
   <ProjectReference Include="..\..\src\Shiny.DocumentDb.Generators\Shiny.DocumentDb.Generators.csproj"
                     OutputItemType="Analyzer" ReferenceOutputAssembly="false"/>
   ```
   (mirrors `Shiny.DocumentDb.Tests.csproj:54-55`).
2. Add a fixture: a `[Document]` type + a `DocumentContext` subclass with a `DocumentSet<T>` (crib
   `tests/Shiny.DocumentDb.Tests/Fixtures/GeneratedDocumentContext.cs`).
3. New `ClientContextTests` mirroring `ClientTests`:
   - context resolves and its `DocumentSet<T>` round-trips against SQLite `:memory:`;
   - provider discriminator selects the right `IDatabaseProvider` (capture via `configureOptions`);
   - health check `DocumentDb.<name>` registered by default, absent when disabled;
   - `DocumentStoreSettings.Provider` / `.ConnectionString` overrides win;
   - factory variant (`Add{Context}Factory`) resolves an `IDocumentContextFactory<TContext>`.
   - multi-context: two contexts on two names coexist without shadowing.

**Run the FULL suite** (`tests/Shiny.DocumentDb.Tests` + `tests/Shiny.DocumentDb.Aspire.Tests`) — needs
**Docker** (Testcontainers). Do not claim green off a filtered subset. (See `feedback_full_tests_docker`.)

## Docs / skill / readme (four-artifact rule)

- **Docs site** (`~/Desktop/dev/documentation/src/content/docs/documentdb/`): add an "Aspire +
  DocumentContext" section to the Aspire page (and cross-link from the typed-context page). Release note
  under `## 11.x TBD` in `release-notes.mdx`, `type="feature"`.
- **Skill** (`skills/shiny-documentdb/SKILL.md`): add `AddDocumentContextProvider` to the Aspire example;
  add it to the `triggers:` list.
- **readme.md** (repo root): one bullet under the Aspire feature line.

## Not in scope

NoSQL providers (Redis, AzureTable, DynamoDB) do **not** fit the `IDatabaseProvider` /
`DatabaseProviderFactory` model the Aspire client is built on — they have their own options types, register
**un-keyed** singletons, need provider-specific health probes, and (DynamoDB) have no connection-string
concept or first-class Aspire resource. That is a separate, larger plan; capture it under a NoSQL-Aspire
plan when picked up. This plan is deliberately limited to the relational + SQLite family so it stays small
and reuses the existing factory untouched.

## Effort estimate

- Client code: ~1 small method + a refactor of the shared wiring (~30 lines net).
- Tests: generator wiring into the Aspire test project + fixture context + ~6 facts (moderate — the
  generator-in-test-project wiring is the main risk).
- Docs/skill/readme: standard four-artifact tax across two repos.

Small-to-moderate. The code is trivial; the test-project generator wiring and the two-repo doc updates are
what push it past a "just do it" one-liner.
