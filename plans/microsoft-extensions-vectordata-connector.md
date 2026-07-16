# Plan: `Microsoft.Extensions.VectorData` connector (`Shiny.DocumentDb.Extensions.VectorData`)

**Status:** Designed + spike-validated, not started.
**Target version:** `11.2` (new feature → minor bump off `11.1.0` in `version.json`). Additive — a **new
opt-in package**, no changes to any existing shipped contract. Branch off `v11`.

> Self-contained build spec. Read `CLAUDE.md` (repo root) for the four-artifact rule (code+tests, docs site,
> skill, readme) before considering any commit "done". This feature is backend-scoped — note the provider
> compatibility tier in the release note.

---

## Goal

Ship a **`Microsoft.Extensions.VectorData` (MEVD) connector** so `Shiny.DocumentDb` can be used anywhere the
.NET AI ecosystem (MEAI, Microsoft Agent Framework, Semantic Kernel) expects an abstract `VectorStore` /
`VectorStoreCollection<TKey, TRecord>`. The MEVD *abstractions* went **GA** (`Microsoft.Extensions.VectorData.Abstractions`
10.x); the contract is stable to build against.

The differentiator no other connector has: **one connector, many backends.** Every existing MEVD connector is
single-store (Qdrant, Azure AI Search, pgvector, Redis, SQL Server, Pinecone). Ours lets the *same* MEVD record
model and code run over any DocumentDb vector-capable provider — SQLite for dev/mobile, PostgreSQL/SQL Server
for prod — swapping backends by configuration. "Use the operational document DB you already have as your MEVD
vector store" is the story.

## Non-goals

- **Not a memory feature.** This is ecosystem interop, not an agent long-term-memory tool pack (that's a
  separate effort on native APIs). Keep them decoupled.
- **Not a change to `IDocumentStore`.** The core contract does not grow a MEVD-shaped bulge. Everything lives
  in the new package and depends only on public core surface + the MEVD abstractions.
- **No new vector engine work.** We adapt the existing `NearestVectors` / `MapVectorProperty` / auto-embed
  muscle. If a capability isn't already in the engine, it isn't in scope here.
- **Not universal across all 10+ providers.** Vector-capable subset only (see tier). Non-vector providers
  throw a clear `NotSupportedException`, matching the GroupBy-on-Azure/Dynamo precedent.

## Rationale — why do this, and the costs

The honest case both ways, so a future reader can re-litigate the decision rather than infer it.

### Why do it

1. **Unique positioning no other connector has: one connector, many backends.** Every existing MEVD
   connector is single-store (Qdrant, Azure AI Search, pgvector, Redis, SQL Server, Pinecone). Ours runs the
   *same* MEVD record model over any DocumentDb vector-capable backend — SQLite for dev/mobile, PostgreSQL/
   SQL Server for prod — swapped by config. "Use the operational document DB you already have as your MEVD
   vector store, change providers without touching code" is a story nobody else tells.
2. **It's mostly an adapter over muscle we already have — proven, not hoped.** We don't build vector search:
   `NearestVectors`, `MapVectorProperty`, auto-embed-on-insert, typed documents all exist. Critically, MEVD's
   filter model is `Expression<Func<T,bool>>`, which the engine already lowers to SQL — the spike proved the
   filter is a **pure passthrough**, so the chunkiest part of most connectors is a no-op for us.
3. **Ecosystem reach + embedding synergy.** MEVD is the GA standard vector-store contract consumed by MEAI
   and the Microsoft Agent Framework. A connector puts DocumentDb in the mental model of every .NET RAG/agent
   sample, and it pairs naturally with `IEmbeddingGenerator` (the same thing driving our auto-embed).
4. **The GA-timing risk is gone.** The abstractions went GA; the "moving target / breaking renames" worry
   that argued for *waiting* no longer applies. The target is stable to build against.

### Costs and risks (the reasons to hesitate)

1. **It's a real surface, not a weekend.** ~11 requirement groups (see [Surface](#surface-from-the-official-mevd-connector-requirements)).
   Filter is free, but attribute reading, the generic/dynamic-model path, collection lifecycle, value-mapping,
   validation, and standard-exception wrapping are all genuine work.
2. **Vector-capable providers only.** Works on the `SupportsVector` subset; non-vector providers throw. So
   "DocumentDb is a VectorStore" is really "its vector-capable tier is" — must be documented plainly, not
   glossed. See [Provider compatibility tier](#provider-compatibility-tier).
3. **It's a *parallel* typed surface.** MEVD wants its own record attributes (`[VectorStoreKey/Data/Vector]`)
   and `VectorStoreCollectionDefinition` — not our `[Document]`/`DocumentContext` model. We take on a second
   typed API + its attribute set alongside the native one. Mitigation: `MapVectorRecord<T>()` reads the MEVD
   attributes and emits the DocumentDb wiring, so users decorate once; a `[Document]`-bridge is deferred.
4. **Ongoing maintenance tracks an external contract.** Lower churn risk now that it's GA, but we inherit
   MEVD's versioning cadence and any post-GA additions to the abstraction.

### Verdict

Worth doing, as an **opt-in package** scoped to vector-capable providers, because the differentiation
(multi-backend) plus the reuse (filter passthrough + `NearestVectors` + auto-embed) make the value/cost ratio
clearly positive now that the target is GA. It is **not** worth folding into core, and **not** a prerequisite
for the separate memory feature.

## Spike results (validation — already run)

A throwaway spike (in scratchpad, not committed) built `DocumentDbVectorStore : VectorStore` and
`DocumentDbCollection<TKey,TRecord> : VectorStoreCollection<TKey,TRecord>` against **MEVD 10.7.0**, over the
**SQLite + sqlite-vec** provider, and ran green (3/3 xUnit):

1. **Compiles against the GA base classes.** The abstract surface is exactly what §"Surface" lists below.
2. **CRUD round-trips** — `UpsertAsync` → `store.Upsert(record, patchIfUpdate:false)`, `GetAsync` →
   `store.Get`, `DeleteAsync` → `store.Remove`.
3. **`SearchAsync` maps cleanly** onto `store.NearestVectors<T>(query, top, filter)` → `VectorResult<T>` →
   `VectorSearchResult<T>(record, score)`; nearest-first ordering preserved.
4. **The load-bearing de-risk — filter passthrough — works end to end.** MEVD's
   `VectorSearchOptions<TRecord>.Filter` is `Expression<Func<TRecord,bool>>`, **byte-identical** to the
   `filter` parameter on `IDocumentStore.NearestVectors<T>`. The spike passed a MEVD-native
   `n => n.Tag == "red"` filter straight through and confirmed it excluded the vector-*nearest* record whose
   tag didn't match — i.e. the filter genuinely reaches the ANN query, no translation layer required.

**Consequence:** the single hardest part of most MEVD connectors (translating the abstraction's filter into
the store's query language) is a **no-op** for us, because DocumentDb already accepts the same
`Expression<Func<T,bool>>` shape and lowers it to SQL internally (AOT-safe, never `Compile()`d). This is the
core reason the value/cost ratio is favourable.

---

## Design decisions (locked)

| Decision | Choice | Consequence |
|---|---|---|
| Packaging | **New opt-in package** `Shiny.DocumentDb.Extensions.VectorData` | Mirrors `Shiny.DocumentDb.Extensions.AI`. Core untouched. |
| Store lifecycle | **Thin connector over a pre-built `IDocumentStore`** | Honest about DocumentDb's "map vectors at startup" constraint (see below). |
| Record→store mapping | **Attribute reader helper** translates `[VectorStoreVector]`/`[VectorStoreKey]` → `MapVectorProperty` / id | Users decorate the record once (MEVD-native); helper emits the DocumentDb wiring. |
| Filter | **Passthrough** — hand `VectorSearchOptions.Filter` straight to `NearestVectors` | Proven by spike. No IR/grammar work. |
| Provider tier | **Vector-capable subset only** | Non-vector providers throw `NotSupportedException` at construction. |
| Dynamic collections | **Throw `NotSupportedException` in v1** | Revisit later atop the JSON lane (`Type`+`JsonNode`). |
| Score | **Surface `VectorResult.Score` as-is**; rely on ordering | Matches DocumentDb's documented "ordering portable, score direction provider-specific" stance. Documented, not normalized. |
| String search values | **Embed via `IEmbeddingGenerator`** (from the collection definition or DI) | Reuses `Shiny.DocumentDb.Extensions.AI` auto-embed plumbing. |

### The one genuine architectural wrinkle: when vector mappings are fixed

`DocumentStore` reads its `MapVectorProperty<T>` registrations **at construction / table-init**; a mapping
added after the store is built is not picked up. MEVD, by contrast, hands you record types lazily via
`GetCollection<TKey,TRecord>(name)`. Two ways to reconcile:

- **(A) Pre-configured store (chosen for the core).** The connector wraps an already-built `IDocumentStore`
  whose `DocumentStoreOptions` already called `MapVectorProperty<T>` for every record type. `GetCollection`
  just constructs a collection facade. Simple, cheap, one connection pool, honest.
- **(B) Store-per-record-type (rejected as the default).** `GetCollection` reads the attributes and builds a
  dedicated `DocumentStore` per `TRecord`. Ergonomic ("just decorate and go") but multiplies connection pools
  and table-init passes; surprising resource cost behind a factory method.

**Chosen:** (A) as the contract, with a DI-friendly builder that makes (A) feel like (B): the user lists their
record types once, the builder reads MEVD attributes and calls `MapVectorProperty` for each, then builds the
single store the connector wraps. See the registration sketch.

---

## Where it sits — reuse, don't reinvent

| MEVD needs | DocumentDb already has | Gap |
|---|---|---|
| Vector search + filter | `IDocumentStore.NearestVectors<T>(query, k, Expression<Func<T,bool>> filter)` (`IDocumentStore.cs:401`) | none — passthrough |
| Vector property declaration | `DocumentStoreOptions.MapVectorProperty<T>(…)` (`DocumentStoreOptions.cs:473/515`) | map `[VectorStoreVector]` → this |
| Upsert / Get / Delete | `Upsert(patchIfUpdate:false)` / `Get` / `Remove` on `IDocumentStore` | none |
| Filtered non-vector get | `store.Query<T>().Where(filter).ToList()` | none |
| Distance metrics | `VectorDistance { Cosine, Euclidean, DotProduct, Hamming }` (`VectorDistance.cs`) | map from MEVD `DistanceFunction` string consts |
| Index kinds | `VectorIndexKind { None, Flat, Hnsw, Ivf, DiskAnn, QuantizedFlat }` (`VectorIndexKind.cs`) | map from MEVD `IndexKind` string consts |
| Embedding generation | `Shiny.DocumentDb.Extensions.AI` auto-embed over `IEmbeddingGenerator<string,Embedding<float>>` | reuse for string search values |
| Capability gate | `IDocumentStore.SupportsVector` (`IDocumentStore.cs:387`) | throw at ctor when false |

Net new code is an **adapter + an attribute reader + a value-mapping table** — no engine work.

---

## Provider compatibility tier

Vector search is flagged per provider via `SupportsVector`. In-tier (connector works):
**SQLite** (via `Shiny.DocumentDb.Sqlite.VectorSupport`), **PostgreSQL/pgvector**, **CockroachDB**,
**SQL Server**, **Oracle**, **DuckDB**, **CosmosDB**, **MongoDB (Atlas)**, **Amazon DocumentDB**, **Redis**.

Out of tier (constructor throws `NotSupportedException` — no vectors): **LiteDB**, **IndexedDB**, plus
Azure Table / DynamoDB / Firestore / RavenDB unless/until they gain `SupportsVector`.

Filter push-down inside the ANN search follows the engine's existing behaviour: Cosmos / pgvector / SQL Server
/ Atlas / DuckDB pre-filter; SQLite post-filters candidates (engine's candidate-multiplier heuristic). The
connector inherits this transparently — it just forwards the filter.

---

## Surface (from the official MEVD connector requirements)

Types to implement (all in `Microsoft.Extensions.VectorData`, GA):

**`DocumentDbVectorStore : VectorStore`** — sealed. Wraps the pre-built `IDocumentStore`.
- `GetCollection<TKey,TRecord>(string name, VectorStoreCollectionDefinition? def)` → construct facade.
- `GetDynamicCollection(string, VectorStoreCollectionDefinition)` → **throw** (v1).
- `ListCollectionNamesAsync(ct)` → enumerate mapped types' table/set names.
- `CollectionExistsAsync(name, ct)` → true when a table exists (DocumentDb auto-creates on init/write).
- `EnsureCollectionDeletedAsync(name, ct)` → `store.Clear<T>()` for the mapped type.
- `GetService(Type, object?)` → return the wrapped `IDocumentStore` / `this`; else null.

**`DocumentDbCollection<TKey,TRecord> : VectorStoreCollection<TKey,TRecord>`** (`: IVectorSearchable<TRecord>`)
— sealed. `where TKey : notnull, TRecord : class`.
- `Name { get; }`.
- `CollectionExistsAsync` / `EnsureCollectionExistsAsync` (no-op; DocumentDb provisions tables + vector index
  at init) / `EnsureCollectionDeletedAsync` → `store.Clear<TRecord>()`.
- `GetAsync(TKey, RecordRetrievalOptions?, ct)` → `store.Get<TRecord>(key)`; return null when missing (§1.5).
- `GetAsync(IEnumerable<TKey>, …)` → override with `BatchRemove`-style batch get; subset-return semantics (§1.6).
- `GetAsync(Expression<Func<TRecord,bool>>, int top, FilteredRecordRetrievalOptions<TRecord>?, ct)` →
  `store.Query<TRecord>().Where(filter)` + `Skip`/`Take(top)`.
- `DeleteAsync(TKey, ct)` / `DeleteAsync(IEnumerable<TKey>, ct)` → `store.Remove` / `store.BatchRemove`;
  succeed when absent (§1.3/1.4).
- `UpsertAsync(TRecord, ct)` / `UpsertAsync(IEnumerable<TRecord>, ct)` → `store.Upsert(patchIfUpdate:false)` /
  `store.BatchUpsert`.
- `SearchAsync<TInput>(TInput, int top, VectorSearchOptions<TRecord>?, ct)` → the core mapping:
  1. Resolve query vector: `ReadOnlyMemory<float>` / `float[]` directly; `string` / other → embed via the
     collection's `IEmbeddingGenerator` (from `VectorStoreCollectionDefinition.EmbeddingGenerator` or DI).
  2. `store.NearestVectors<TRecord>(vector, top + options.Skip, options.Filter, ct)`.
  3. Apply `Skip`, `ScoreThreshold`; project to `VectorSearchResult<TRecord>(doc, score)`.
- `GetService(Type, object?)`.

Requirement conformance checklist (from MS "build your own connector"):
1. Core base classes ✔ (two). 2. Attribute support — read `[VectorStoreKey/Data/Vector]` via reflection when
   no `VectorStoreCollectionDefinition` supplied. 3. Record definition — honour a supplied
   `VectorStoreCollectionDefinition` over attributes. 4. Collection/index creation — map `IndexKind`/
   `DistanceFunction` to DocumentDb enums (table below); creation is DocumentDb's job at init. 5. Data-model
   validation — validate key/vector types at collection construction; throw early. 6. Storage naming — honour
   `StorageName` / `JsonPropertyName` (DocumentDb is JSON-based, so lean on STJ naming). 7. Mappers — the
   record IS the DocumentDb document; identity mapper. 8. Generic data model — **deferred** (dynamic
   collections v-next). 9. Divergent schema — support storage-name divergence only (§ allows just this).
   10. Standard exceptions — wrap failures in `VectorStoreException` / throw `NotSupportedException` for
   unsupported metric/index; `ArgumentException` for bad args. 11. Batching — override the batch Get/Upsert/
   Delete with `Batch*` primitives.

### Value-mapping tables

**`DistanceFunction` (MEVD string const) → `VectorDistance`:** `CosineDistance`/`CosineSimilarity` → `Cosine`;
`EuclideanDistance` → `Euclidean`; `DotProductSimilarity`/`NegativeDotProductSimilarity` → `DotProduct`;
`HammingDistance` → `Hamming`; others (`EuclideanSquaredDistance`, `ManhattanDistance`) → `NotSupportedException`.

**`IndexKind` (MEVD string const) → `VectorIndexKind`:** `Hnsw`→`Hnsw`, `Flat`→`Flat`, `IvfFlat`→`Ivf`,
`DiskAnn`→`DiskAnn`, `QuantizedFlat`→`QuantizedFlat`, `Dynamic`/unset→`Hnsw` (DocumentDb default).

**Key (`TKey`) ↔ id:** MEVD `[VectorStoreKey]` property ↔ DocumentDb document id (`object`). Support `string`,
`Guid`, `int`, `long` keys (DocumentDb id types). Validate at construction.

**Score:** `VectorResult<T>.Score` (float) → `VectorSearchResult<T>.Score` (double?). Ordering is portable
(nearest-first); the raw number's direction/scale is provider-specific — documented, not normalized.

---

## Registration sketch (target ergonomics)

```csharp
// DI — user decorates records with MEVD attributes once; builder emits MapVectorProperty per type,
// builds ONE DocumentStore, wraps it as a VectorStore.
services.AddDocumentDbVectorStore(o =>
{
    o.DatabaseProvider = new SqliteDatabaseProvider("Data Source=app.db") { EnableVectorExtension = true };
    o.MapVectorRecord<Note>();      // reads [VectorStoreKey]/[VectorStoreVector] → MapVectorProperty<Note>
    o.MapVectorRecord<Article>();
});

// resolve MEVD's abstraction anywhere in the AI stack:
VectorStore vs = sp.GetRequiredService<VectorStore>();
var col = vs.GetCollection<string, Note>("notes");
await col.UpsertAsync(note);
await foreach (var hit in col.SearchAsync(queryEmbedding, top: 5,
    new() { Filter = n => n.Tag == "release" })) { … }
```

`MapVectorRecord<T>()` is the attribute reader: pull the `[VectorStoreVector]` property + `Dimensions` +
`DistanceFunction`/`IndexKind`, and the `[VectorStoreKey]` property, then call the existing
`DocumentStoreOptions.MapVectorProperty<T>(getter, setter, dimensions, metric, indexKind)` (AOT-safe overload)
under the hood. Container-free path: a `DocumentDbVectorStore(IDocumentStore store)` ctor for
`new DocumentStore(options)` users who called `MapVectorProperty` themselves.

---

## Package / csproj

`src/Shiny.DocumentDb.Extensions.VectorData/Shiny.DocumentDb.Extensions.VectorData.csproj` — copy the shape of
`Shiny.DocumentDb.Extensions.AI.csproj`:
- `<TargetFramework>$(BaseTargetFramework)</TargetFramework>` (net10.0), inherits Directory.Build.props (AOT/
  trim analyzers on).
- `<ProjectReference Include="..\Shiny.DocumentDb\Shiny.DocumentDb.csproj" />`.
- `<ProjectReference Include="..\Shiny.DocumentDb.Extensions.AI\..." />` (reuse auto-embed) — or keep AI
  optional and only reference `Microsoft.Extensions.AI.Abstractions` for `IEmbeddingGenerator`.
- `<PackageReference Include="Microsoft.Extensions.VectorData.Abstractions" />` + DI abstractions.
- Add to `Directory.Packages.props`: `<PackageVersion Include="Microsoft.Extensions.VectorData.Abstractions"
  Version="$(MicrosoftExtensionsAIVersion)" />` (10.7.0 aligns with the MEAI band; keep in lockstep).

---

## Test matrix

New project `tests/Shiny.DocumentDb.Extensions.VectorData.Tests`:
- **Compile/wiring + SQLite (no Docker)** — port the spike: CRUD round-trip, `SearchAsync` → nearest-first,
  **filter passthrough excludes vector-nearest non-match**, `Skip`/`top`, `ScoreThreshold`, string-value
  embedding via a fake `IEmbeddingGenerator`, attribute-reader (`MapVectorRecord`) produces a working mapping,
  supplied `VectorStoreCollectionDefinition` overrides attributes, unsupported metric/index → `NotSupported`.
- **One relational via Testcontainers** — PostgreSQL/pgvector: same suite, exercising ANN pre-filter push-down.
- **One document-native via Testcontainers** — MongoDB (Atlas local) or Cosmos emulator: score-direction
  difference surfaces (similarity vs distance) but ordering stays nearest-first.
- **Negative** — non-vector provider (LiteDB) → constructor `NotSupportedException`.

Per `CLAUDE.md`: the **full** suite needs Docker for the non-SQLite legs; do not claim green from the SQLite
subset alone. Run `dotnet test` on the new project + confirm no regression in the main suite.

---

## Phasing

- **11.2** — core connector (thin-over-store + `MapVectorRecord` attribute reader), value-mapping tables,
  string-embedding, SQLite + one relational + one document test legs. Ships the differentiator.
- **11.x (later)** — dynamic/generic collections over the JSON lane (`GetDynamicCollection`,
  `Dictionary<string,object>` records); `[Document]`-type bridge so existing typed models can flow through
  without MEVD attributes; keyword-hybrid search if/when the engine gains full-text+vector fusion.

---

## Four-artifact follow-ups (per `CLAUDE.md`)

1. **Code + tests** — as above; full suite green with Docker.
2. **Docs site** (`~/Desktop/dev/documentation/src/content/docs/documentdb/`) — new `vectordata.mdx` (or a
   section on the AI page) covering registration, attribute mapping, provider tier, score caveat; plus a
   **release note** under `## 11.2 TBD` (`type="feature"`, note the backend-specific tier).
3. **Skill** (`skills/shiny-documentdb/SKILL.md`) — add `VectorData`, `VectorStore`, `VectorStoreCollection`,
   `MapVectorRecord`, `AddDocumentDbVectorStore` to the `triggers:` list; document the connector pattern.
4. **readme.md** (repo root) — add the MEVD connector to the feature list.

## Open questions

- **AI package coupling** — reference `Shiny.DocumentDb.Extensions.AI` (get auto-embed for free) vs. only
  `Microsoft.Extensions.AI.Abstractions` (lighter, re-implement the tiny embed call). Lean: abstractions-only
  to keep the dependency graph flat; the embed call is one `GenerateAsync`.
- **VectorData version pinning** — track `$(MicrosoftExtensionsAIVersion)` (10.7.0) vs. an independent
  `$(MicrosoftExtensionsVectorDataVersion)` property. Lean: independent property; the two packages version
  separately upstream.
- **Multiple vectors per record** — MEVD allows N vector properties + `VectorSearchOptions.VectorProperty`
  selection; DocumentDb's `MapVectorProperty` is one-per-type today. v1: support single-vector records, throw
  a clear error on multi-vector until the engine supports it.
