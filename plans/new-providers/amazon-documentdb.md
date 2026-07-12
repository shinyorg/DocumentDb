# Plan — Amazon DocumentDB provider (`Shiny.DocumentDb.DocumentDb`)

## Why / fit

Amazon DocumentDB is **MongoDB-wire-compatible** (emulates the 3.6 / 4.0 / 5.0 API on a different
engine). That makes it the **cheapest** of the five: it reuses the existing
`Shiny.DocumentDb.MongoDb` provider almost wholesale. The work is not a new store — it's a **thin
subclass** (the v11 MariaDB/CockroachDB pattern, but applied to the Mongo document-native store)
plus TLS/connection handling and **capability down-flagging** for the ops DocumentDB doesn't support.

**Archetype:** thin subclass of `MongoDbDocumentStore`. `DocumentDbDocumentStore : MongoDbDocumentStore`.

## Prerequisite refactor (mirror v11)

`MongoDbDocumentStore` currently exposes capability via private members + `Supports*` props. To
subclass and down-flag, make the relevant members **`virtual`/`protected virtual`** (exactly what the
memory note *"MariaDB & CockroachDB — made base members virtual"* records for the relational bases):

- `SupportsFullText` (DocumentDB has **no `$text`** → must return false)
- `SupportsVector` (DocumentDB vector search differs from Atlas `$vectorSearch` → false in v1)
- the full-text / vector terminators, so the subclass can override to throw a DocumentDB-specific message
- the connection/`IMongoClient` construction hook (so the subclass can inject the TLS `MongoClientSettings`)
- `MongoBatchEligible` / bulk-write path (DocumentDB supports a narrower bulk surface — verify)

Keep the change minimal and behavior-preserving for the existing Mongo provider (same virtual = same
result).

## Dependencies & shape

- NuGet: same `MongoDB.Driver` (no new driver). `ProjectReference` → `..\Shiny.DocumentDb.MongoDb` **and** core.
- Files (small): `DocumentDbDocumentStore.cs` (subclass + capability overrides), `DocumentDbDocumentStoreOptions.cs` (extends Mongo options with TLS/CA-cert config), `ServiceCollectionExtensions.cs` (`AddDocumentDbDocumentStore`).

## Connection / options

- Extend `MongoDbDocumentStoreOptions` (or wrap it) with DocumentDB specifics:
  - **TLS on by default** with the Amazon RDS CA bundle (`global-bundle.pem`) — accept a cert path or embedded bundle, build `MongoClientSettings` with `SslSettings`.
  - `retryWrites=false` by default (DocumentDB doesn't support retryable writes; the driver default `true` breaks writes) — a common footgun; set it and document it.
  - `readPreference=secondaryPreferred` guidance for read scaling (optional).
- Standard `mongodb://user:pass@cluster-endpoint:27017/?tls=true&replicaSet=rs0&readPreference=…`.

## Capability deltas vs. MongoDB provider

| Feature | MongoDB provider | Amazon DocumentDB | Action |
|---|---|---|---|
| CRUD, LINQ query pushdown, OrderBy, paging, Count | ✅ | ✅ (core find/aggregate supported) | inherit |
| `$expr` scalar pushdown (string funcs, date parts, math) | ✅ | ⚠️ **partial** — some aggregation operators unsupported per-engine-version | test the emitted `$expr`; where an operator is unsupported, fall back client-side or throw clearly |
| GroupBy (client-side) | ✅ | ✅ (materialize client-side — unaffected) | inherit |
| `IObservableDocumentStore` (in-proc) | ✅ | ✅ | inherit |
| `IChangeFeedDocumentStore` | ❌ (Mongo provider doesn't implement it) | *DocumentDB has change streams (4.0+)* | out of scope now; if Mongo later adds change streams, DocumentDB can inherit |
| `SupportsFullText` / `FullTextSearch` (`$text`) | ✅ | ❌ **no `$text`** | **override → false / throw** with a message pointing at OpenSearch |
| `SupportsVector` / `NearestVectors` (`$vectorSearch`) | ✅ Atlas | ❌ (different syntax) | **override → false / throw** in v1 |
| `SupportsSpatial` / `Geo*` (`2dsphere`) | ✅ | ✅ (geospatial supported) | inherit (verify operator coverage in tests) |
| `ITemporalDocumentStore` (sidecar history) | ✅ | ✅ (pure app-level, no server dep) | inherit |
| `IDocumentBackup`, `IDocumentMaintenance` | ✅ | ✅ | inherit |
| `IUnitOfWorkEngine` (compensating) | ✅ | ✅ (multi-doc txns exist but compensating path is fine) | inherit |
| CAS (mapped version field, atomic filter) | ✅ | ✅ | inherit |

Net: subclass overrides **full-text** and **vector** to unsupported, injects TLS + `retryWrites=false`,
and adds a compatibility test matrix. Everything else is inherited.

## Implementation phases

1. **Refactor**: make the needed `MongoDbDocumentStore` members virtual (no behavior change; run the full Mongo suite to confirm parity).
2. **Subclass + options + DI** (`AddDocumentDbDocumentStore`): TLS/CA bundle, `retryWrites=false`, capability overrides for full-text/vector.
3. **Compat test pass**: run the shared conformance suite against DocumentDB; catalogue any `$expr`/aggregation gaps and guard/fallback them.

## Testing

- No official DocumentDB Testcontainer. Options, in order of preference:
  1. **`localstack`** or the community **`amazon/documentdb`-compatible** images if available; otherwise
  2. gate a DocumentDB integration test behind an env-var connection string (opt-in CI, real cluster), and
  3. run the **existing Mongo conformance suite** against a Mongo container as a proxy for the inherited behavior, plus targeted unit tests asserting the capability overrides (full-text/vector throw, TLS/`retryWrites` applied).
- Explicitly test: `FullTextSearch` throws a DocumentDB-specific message; `$expr`-heavy LINQ queries either work or fall back cleanly.

## Four-artifact sync

1. **Code + tests** above. Release note tier: **Mongo-compatible subclass**; note the down-flagged features (no `$text` full-text, no vector) and required TLS + `retryWrites=false`.
2. **Docs**: `amazon-documentdb.mdx` — lead with "it's the Mongo provider + TLS + capability caveats", connection-string template, unsupported-features table; `<RN type="feature">`.
3. **Skill**: add Amazon DocumentDB + `AddDocumentDbDocumentStore` to `triggers:`/provider list; note it's Mongo-compatible with no full-text/vector.
4. **readme.md**: provider list + one-line caveat.

## Risks / open questions

- **Testing without a container** is the main gap — decide between LocalStack/community image vs. opt-in real-cluster CI. Recommend: unit-test the overrides deterministically + opt-in integration gate.
- The exact set of unsupported `$expr`/aggregation operators varies by DocumentDB engine version — the compat test pass must enumerate them; prefer graceful client-side fallback over hard throws where feasible.
- Confirm the virtual-member refactor doesn't regress the Mongo provider (full suite must stay green — this is the one change that touches shipping code).
