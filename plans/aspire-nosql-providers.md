# Plan: Aspire support for the NoSQL providers (Redis, Azure Table, DynamoDB)

**Status:** Designed, not started.
**Target version:** `11.x` (additive per provider). Phase by provider: **Redis** → **Azure Table** →
**DynamoDB (deferred)**; see [Phasing](#phasing).

> Self-contained build spec. Read `CLAUDE.md` (repo root) for the four-artifact rule (code+tests, docs
> site, skill, readme) before considering any commit "done". Branch off `v11`. The docs site is the
> **separate** repo at `~/Desktop/dev/documentation`. The full suite (incl. these providers) needs
> **Docker** (Testcontainers) — see `feedback_full_tests_docker`.

> Companion plan: `plans/aspire-documentcontext.md` (relational + SQLite `DocumentContext` support). That
> one reuses the existing factory untouched; **this** one does not — see below.

---

## Goal

Extend the Aspire integration (`Shiny.DocumentDb.Aspire.Hosting` + `.Client`) so an Aspire-provisioned
Redis / Azure Storage (Table) / DynamoDB resource can back a DocumentDb store the same way the relational
providers already do: host stamps a provider discriminator + connection string, client resolves them and
registers a keyed `IDocumentStore` with a health check + OpenTelemetry.

## The core problem — these don't fit the existing model

The entire Aspire client is built around three assumptions that hold for the relational + SQLite family and
**fail** for the NoSQL trio:

| Assumption (relational) | Reality (Redis / AzureTable / DynamoDB) |
|---|---|
| Provider implements `IDatabaseProvider`, built by `DatabaseProviderFactory.Create(kind, connString)` and assigned to `DocumentStoreOptions.DatabaseProvider`. | **None implement `IDatabaseProvider`.** Each has its own options type (`RedisDocumentStoreOptions`, `AzureTableDocumentStoreOptions`, `DynamoDbDocumentStoreOptions`) and its own `AddXxxDocumentStore(Action<TOptions>)` extension. The whole `DatabaseProviderFactory` path is unusable. |
| Store registered **keyed by name** (`AddDocumentStore(name, …)` → `AddKeyedSingleton<IDocumentStore>(name, …)`), so `GetKeyedService<IDocumentStore>(name)` and multi-store work. | Each `AddXxxDocumentStore` registers an **un-keyed** `AddSingleton<IDocumentStore>`. No `name` overload, no `StoreName`. A second store overwrites `IDocumentMaintenance` and there is no keyed resolve. |
| Health check opens an `IDatabaseProvider` connection and runs `SELECT 1` (`DocumentStoreHealthCheck`). | No SQL, no `IDatabaseProvider`. Needs a provider-specific liveness probe (Redis `PING`, a Table service call, a DynamoDB `DescribeEndpoints`/`ListTables`). |

So this is **not** "add three enum values." Each provider needs (a) a keyed registration overload in its own
package, (b) a divergent client branch, and (c) a bespoke health probe. Do not shoehorn these into
`DatabaseProviderFactory`.

## Connection models diverge too

- **Redis** — `RedisDocumentStoreOptions.ConnectionString` is a StackExchange.Redis string
  (`localhost:6379`). Aspire's official `Aspire.Hosting.Redis` resource injects exactly this. Clean fit.
- **Azure Table** — `AzureTableDocumentStoreOptions.ConnectionString` is an Azure Storage connection
  string. Aspire's Azure Storage / Azurite resource injects a table connection string. Fit, pending
  verification of the exact conn-string shape Azurite emits for the Tables endpoint.
- **DynamoDB** — **no connection string.** `DynamoDbDocumentStoreOptions` takes `IAmazonDynamoDB` /
  `AWSCredentials` / `RegionEndpoint` / `ServiceUrl`. There is **no first-class Aspire DynamoDB resource**
  (only community LocalStack integrations). A single `ConnectionStrings:<name>` + discriminator can't carry
  region + credentials + service URL. This is the one that does not fit the Aspire contract at all.

## Design decisions (locked)

| Decision | Choice | Consequence |
|---|---|---|
| Keyed parity | **Add keyed `AddXxxDocumentStore(name, …)` + `StoreName` to each provider package** | Preserves the Aspire keyed-by-name contract + multi-store; keeps telemetry `db.namespace` tagging. |
| Client dispatch | **Per-kind branch**, not `DatabaseProviderFactory` | NoSQL providers register through their own extension, not `o.DatabaseProvider`. |
| Health checks | **Per-provider probe**, dispatched by `DocumentProviderKind` | `DocumentStoreHealthCheck` becomes a dispatcher; relational keeps `SELECT 1`. |
| DynamoDB | **Defer** | No conn string, no first-class Aspire resource; document as a known gap. |
| Enum sync | Extend **both** `DocumentProviderKind` copies (host + client) identically | They are spelling-identical by contract (`DocumentProviderKind.cs` header comment). |

## Deliverables

### Shared client changes (`Shiny.DocumentDb.Aspire.Client`)

1. Extend `DocumentProviderKind` (client + host copies) with `Redis`, `AzureTable` (and `DynamoDb` only if/
   when 2c lands). Update the `ResolveProvider` error message enumerating valid kinds
   (`DocumentStoreClientExtensions.cs:98-100`).
2. In `AddDocumentStore`, branch on `kind`: relational → today's `o.DatabaseProvider = factory.Create(...)`;
   NoSQL → call the provider-specific keyed registration (below) instead. Keep the `configureOptions` /
   `configureServiceOptions` / `MultiTenant` hooks working where the provider supports them (note:
   multi-tenant shared-table semantics may not map to every NoSQL backend — gate + document).
3. Turn `DocumentStoreHealthCheck` into a kind dispatcher: relational keeps `SELECT 1`; Redis does `PING`;
   AzureTable does a cheap service call. (Reuse the same connection string the store uses.)

### Per-provider package changes

Each of `Shiny.DocumentDb.Redis` / `.AzureTable` (and `.DynamoDb` if un-deferred):

- Add a keyed overload `AddXxxDocumentStore(this IServiceCollection, string name, Action<TOptions>)` that
  registers `AddKeyedSingleton<IDocumentStore>(name, …)` + keyed `IDocumentMaintenance`, mirroring
  `ServiceCollectionExtensions.AddDocumentStore(name, …)` in core.
- Add a `StoreName` property to the options type and thread it into the store's embedded telemetry
  (`db.namespace = name`), matching `DocumentStoreOptions.StoreName`.

### Hosting changes (`Shiny.DocumentDb.Aspire.Hosting`)

- `DocumentProviderKindDetector.Detect` (`Internal/DocumentProviderKindDetector.cs`): add a branch for the
  Redis resource type (`RedisResource` from `Aspire.Hosting.Redis`) → `DocumentProviderKind.Redis`, and for
  the Azure Storage/Tables resource → `AzureTable`. DynamoDB has no resource to detect → stays
  explicit-kind-only (another reason to defer it).
- Add the relevant Aspire hosting package reference(s) needed to reference those resource types.

## Phasing

### 2a. Redis — do first (most tractable)
Clean `ConnectionString`, official `Aspire.Hosting.Redis` resource, `PING` health probe. Full slice: enum +
client branch + keyed `AddRedisDocumentStore(name,…)` + `StoreName` + health probe + hosting detector +
tests + docs/skill/readme.

### 2b. Azure Table — second (moderate)
Same slice as Redis. Extra risk: confirm the Azurite Tables connection-string shape and pick a cheap
liveness call. Azure Storage hosting package reference.

### 2c. DynamoDB — deferred (recommended)
Does not fit the connection-string + discriminator contract and has no first-class Aspire resource. If
picked up later: model configuration as an explicit `DocumentStoreSettings.DynamoDb { ServiceUrl, Region,
Credentials }` block on the **client** side (skip host auto-detection), and register through a keyed
`AddDynamoDbDocumentStore(name,…)`. Health probe: `ListTables` against the configured `ServiceUrl`. Until
then, note it as a known gap in the release note.

## Tests

Extend `tests/Shiny.DocumentDb.Aspire.Tests/ClientTests.cs` (and hosting detector tests) per provider:

- discriminator selects the right store (Redis/AzureTable), keyed store resolves and round-trips;
- keyed multi-store: two named stores of the same kind coexist;
- health check registered by default / absent when disabled / returns Healthy against a live container;
- `DocumentStoreSettings.Provider` / `.ConnectionString` overrides win;
- hosting `DocumentProviderKindDetector` maps the Redis / Azure Storage resource types.

These use **Testcontainers** (Redis; Azurite for Tables) → **Docker required**. Do not report green off a
filtered subset. Run the full `tests/Shiny.DocumentDb.Tests` + `tests/Shiny.DocumentDb.Aspire.Tests`.

## Docs / skill / readme (four-artifact rule)

- **Docs site** (`~/Desktop/dev/documentation/src/content/docs/documentdb/`): extend the Aspire page with a
  provider-support matrix (relational + SQLite ✓, Redis ✓, Azure Table ✓, DynamoDB — explicit config only /
  deferred, Mongo/Cosmos deferred). Release note per provider under `## 11.x TBD`, `type="feature"`.
- **Skill** (`skills/shiny-documentdb/SKILL.md`): add the new provider kinds + keyed NoSQL Aspire example;
  update `triggers:`.
- **readme.md** (repo root): update the Aspire provider list.

## Effort estimate

Not small. Per provider it's: 1 enum value (×2 copies) + a client dispatch branch + a keyed registration
overload and `StoreName` in the provider package + a bespoke health probe + a hosting detector branch +
container-backed tests + the four-artifact tax across two repos. Redis ≈ the cleanest; Azure Table similar
with conn-string verification; DynamoDB is a different shape and recommended deferred.
