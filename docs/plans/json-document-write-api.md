# Plan: Late-bound JSON lane (`Type` + `JsonNode` reads *and* writes)

**Status:** ✅ **BUILT** (branch `feature/json-lane`, 2026-07-02). All 23 `JsonLaneTests` pass; full suite green
except 2 pre-existing Mongo/Cosmos backup tests (infra flakiness, pass in isolation). Four artifacts done.

**Deltas from the plan as designed (discovered during build):**
1. **Provider scope is relational-tier, not "all providers".** There is *not* a single `IDocumentStore`
   implementation — MongoDB, Cosmos, LiteDB, Azure Table, DynamoDB each have their own store, plus a
   diagnostics decorator and two UnitOfWork stores. The 6 lane methods are **default interface methods that
   throw `NotSupportedException`** (mirroring spatial/vector/full-text); the core relational `DocumentStore`
   overrides them (covers SQLite/SQLCipher/MySQL/SQL Server/PostgreSQL/Oracle/DuckDB). NoSQL/key-partitioned
   providers inherit the throw until a later cut. The diagnostics decorator delegates to inner.
2. **Not available inside a `UnitOfWork`** — `TransactionalDocumentStore` and `CompensatingStore` throw
   (writes can't be compensation-tracked; consistent with UoW-JSON-out-of-scope).
3. **Reads match their typed siblings exactly:** `Get(type,id)` applies global query filters (like `Get<T>`);
   `Query`/`QueryStream(type,…)` apply tenancy only (like the string `Query<T>`), **not** global filters.
4. **Writes mutate the caller's node in place** (inject Id/version), mirroring typed `Insert<T>` — not a clone.
5. **Custom (converter-based) Id types throw** on the lane (Guid/int/long/string only); use typed `Insert<T>`.
6. Added an internal `DocumentWriteContext.RawJson` so interceptors see the body with `Document == null`.

Files: `Internal/JsonLaneAccessors.cs` (node Id accessor + node helpers), `DocumentStore.JsonLane.cs`
(partial: the 6 methods + write dispatch + non-generic resolvers/filters), `Internal/ChangeBroadcaster.cs`
(non-generic `Publish(Type,…)`), `Interceptors.cs` (`RawJson`), `IDocumentStore.cs` (6 default methods),
throwing stubs in the UoW/compensating/decorator stores. Tests: `tests/…/JsonLaneTests.cs`.

---

**Original status:** Designed, not started.
**Target version:** `10.0.0` (raw version from `version.json`, currently `10.0.0-beta.{height}`) — additive,
no breaking changes. New methods on `IDocumentStore`; every existing typed call is untouched.

This plan covers **both halves** of the late-bound lane — the caller brings a registered `Type` and works in
raw JSON instead of a CLR `T` on both sides:
- **Write** — `Insert`/`Update`/`Upsert(Type, JsonNode)` (the bulk of this doc).
- **Read** — `Get`/`Query`/`QueryStream(Type, …)` returning `JsonNode` (see **"Read lane"** section). The read
  half is nearly free: every read already funnels through a `ReadListAsync(cmd, projector)` where the row *is*
  the stored JSON string and the projector deserializes to `T`; the raw variant swaps the projector for
  `JsonNode.Parse(json)`. Filtering reuses the **existing WHERE/OData string** surface — there is deliberately
  **no "filter by JsonNode"** (a JSON query-object DSL would overlap OData and repeat the dropped JsonPath
  feature; query-by-example is a possible thin future convenience, not in this cut).

> Self-contained build spec. The implementing agent does not have the design conversation —
> everything needed is here. Read `CLAUDE.md` (repo root) for the four-artifact rule (code+tests,
> docs site, skill, readme) before considering any commit "done".

Branch off `v10` (the current working branch) before starting.

---

## Goal

Let callers write documents by handing the store a **`Type` token and the JSON body directly**, instead
of a typed CLR instance:

```csharp
Task<int> Insert(Type type, JsonNode document, CancellationToken ct = default);
Task<int> Update(Type type, JsonNode document, CancellationToken ct = default);
Task<int> Upsert(Type type, JsonNode document, CancellationToken ct = default);
```

This is the **late-bound / dynamic ingestion** lane — generic HTTP intake, message-bus payloads, ETL,
gateways — where the caller has a registered document `Type` (so all mappings resolve) but does **not**
have `T` at the call site and already holds the payload as JSON. It skips CLR serialization; the supplied
JSON is stored **as-is**.

A single `JsonNode` that is a **`JsonArray`** is treated as "many documents of `type`" and routed through
the batch path atomically. A single `JsonObject` writes one document. Both return the number of documents
written.

### What this is NOT

This is **not** `IDocumentBackup` / `BulkImportAsync`. That lane deliberately bypasses versioning/CAS,
temporal history, interceptors, tenant scoping, and query filters for restore throughput. **This lane
rides the normal write pipeline and keeps all of those.** The one-line justification:

> *Same guarantees as the typed write path, but you bring the JSON and a `Type` instead of a `T`.*

If it silently dropped those hooks it would just be `BulkImport` with a worse signature.

---

## Decisions locked (from design conversation)

- **Accept `JsonNode`, not `JsonDocument`/`JsonElement`.** The write path must **inject a generated Id**
  (and set/bump the version) into the payload before persisting. `JsonDocument`/`JsonElement` are
  read-only; `JsonObject` (a `JsonNode`) is mutable, so we edit in place with no re-parse. Callers holding
  a `JsonDocument` convert with `doc.Deserialize<JsonNode>(jsonOptions)` or `JsonNode.Parse(json)`. We do
  **not** add `JsonDocument`/`string` overloads in this cut (can revisit if asked) — one accepted node
  type keeps the surface small.
- **Key by `Type`, not `string typeName`.** Every mapping resolver (`ResolveTableName`, spatial, vector,
  version, query-filter) is already keyed by `Type`, and `[Document]`/`MapTypeToTable` registrations are
  `Type`-based. A `Type` token resolves all of them with no new lookup path. (A `string typeName` overload
  could be added later; out of scope here.)
- **Single method handles object *and* array.** `document is JsonArray` → batch; otherwise a single
  `JsonObject`. A `JsonValue`/primitive throws `ArgumentException`. Return `Task<int>` uniformly (a single
  object returns `1`) — the count is genuinely useful for the array case and there is no typed
  `Task`-returning method to stay signature-compatible with.
- **Payload is stored AS-IS — caller owns the property shape.** The node's members are written verbatim
  (`node.ToJsonString(jsonOptions)` does **not** re-apply the naming policy to existing members). The
  supplied JSON must already be in the **persisted shape** — i.e. property names matching what `T` would
  serialize to under the configured `JsonSerializerOptions` (camelCase by default). This mirrors
  `BulkImport`'s "bound AS-IS" contract. The Id and version members we read/inject use the **same
  policy-resolved member names** the typed path uses, so they line up.
- **Missing mapped properties throw — no silent sidecar skips.** Because the payload is stored AS-IS, a
  caller who omits a property the engine actually reads would otherwise get a silently-broken sidecar. So
  the JSON lane **validates that every registered mapping's JSON path is present in the node**, and throws a
  clear `InvalidOperationException` naming the missing path(s) instead of skipping. Scope and rules:
  - **Which mappings are checked:** the ones the core lane reads out of the node — **spatial**
    (`SpatialMapping.JsonPath`) and **vector** (`VectorMapping.JsonPath`). *Not* checked: **version** (engine
    injects it), **Id** (has its own auto-gen/throw rules), **computed** (`ComputedMapping` is derived in SQL,
    not a caller-supplied property), and **indexed properties** (live in the Azure Table / DynamoDB provider
    packages, outside this core lane — note as a scope caveat).
  - **Absent vs. null:** only an **absent key** throws. A path that is present but JSON `null` (`"location": null`)
    is treated as an intentional "this document has no value" — proceed and skip that sidecar exactly as today.
    This distinguishes "you forgot the property" (structural error) from "this doc genuinely has none."
  - **Per-operation:** **Insert** and **Update** (full-document writes) always validate. **Upsert** validates
    **only when the payload carries no Id** (no Id ⇒ guaranteed brand-new insert ⇒ full doc expected); an
    Upsert that carries an Id is treated as a possible RFC 7396 partial merge and **skips** the presence check
    (a status-only patch must not be forced to re-send the location). Reuse the node `IdAccessor.IsDefaultId`
    signal already resolved for Id handling — no extra read.
- **Full pipeline parity is in scope for this single release — there is no phased/v2 split.** Tenancy,
  temporal, version/CAS, spatial, vector, JSON interceptors, and change publishing all work on this lane
  in `10.0.0`. See the parity table below for exactly how each is sourced.
- **Zero new `IDatabaseProvider` surface.** All core write helpers (`InsertCoreAsync`, `UpdateCoreAsync`,
  `UpsertMergeCoreAsync`, `GenerateIdAsync`, `SpatialUpsertAsync`, `VectorUpsertAsync`,
  `AppendHistoryAsync`) already live in `DocumentStore` and take `(id, typeName, json)` + call existing
  provider `Build*Sql` hooks. The whole feature is implemented in `DocumentStore` + `IdAccessor`/mapping
  node-readers. No provider package changes.

### Alternatives considered and rejected

- **Route through `BulkImportAsync`.** Rejected — it intentionally drops versioning, temporal,
  interceptors, tenant scoping, and filters (see `IDocumentBackup.cs` docstring). The entire point of this
  API is to keep them.
- **Deserialize the node to `T` and call the existing typed `Insert<T>`.** Defeats the purpose (requires a
  resolvable `JsonTypeInfo<T>`, reflects/allocates the object, and re-serializes) and does nothing the
  typed API doesn't already. The value here is *not* materializing `T`.
- **Accept `JsonDocument` (as literally requested).** Read-only, so Id/version injection needs a re-parse
  to a mutable tree on every write. `JsonNode` avoids it; the `JsonDocument` ergonomic is a one-liner at
  the call site.
- **Separate `BatchInsert(Type, JsonArray)` methods instead of array-detection.** More surface for no gain
  — `JsonArray` is a `JsonNode`, so one method dispatches cleanly and the intent ("write this JSON of that
  type") reads naturally.

---

## API surface (new — `IDocumentStore`)

```csharp
/// <summary>
/// Writes one or many documents of <paramref name="type"/> from a caller-supplied JSON body, without a
/// CLR instance. The body is stored AS-IS (property names must match the type's serialized shape).
/// If <paramref name="document"/> is a <see cref="JsonArray"/>, every element is written as a document of
/// <paramref name="type"/> atomically (one transaction); a <see cref="JsonObject"/> writes a single
/// document. Rides the normal write pipeline — tenancy, temporal history, versioning/CAS, spatial/vector
/// sidecars, JSON interceptors, and change notifications all apply. Returns the number of documents written.
/// </summary>
/// <param name="type">A registered document type. Resolves table/typeName and all mappings.</param>
/// <param name="document">A <see cref="JsonObject"/> (one doc) or <see cref="JsonArray"/> (many). A
/// primitive <see cref="JsonValue"/> throws <see cref="ArgumentException"/>.</param>
Task<int> Insert(Type type, JsonNode document, CancellationToken cancellationToken = default);

/// <summary>Full-document replace. Every target document must already exist (missing → throws and rolls
/// back the whole call). Same object/array + pipeline semantics as <see cref="Insert(Type, JsonNode, CancellationToken)"/>.</summary>
Task<int> Update(Type type, JsonNode document, CancellationToken cancellationToken = default);

/// <summary>RFC 7396 JSON Merge Patch upsert (deep-merge if the Id exists, insert-as-is otherwise). Same
/// object/array + pipeline semantics as <see cref="Insert(Type, JsonNode, CancellationToken)"/>.</summary>
Task<int> Upsert(Type type, JsonNode document, CancellationToken cancellationToken = default);
```

Implemented **only** in `DocumentStore` (the single `IDocumentStore` implementation); no default-interface
loop needed because there is no per-provider override — the shared core already fans out to provider hooks.

---

## Read lane (new — `IDocumentStore`)

The symmetric read half: same late-bound callers pull documents back as raw JSON without materializing `T`.

```csharp
/// <summary>Reads a single document of <paramref name="type"/> by Id as raw JSON, or null if not found.
/// Honors tenancy + global query filters exactly like the typed <c>Get&lt;T&gt;</c>.</summary>
Task<JsonNode?> Get(Type type, object id, CancellationToken cancellationToken = default);

/// <summary>Runs the same string WHERE/OData filter surface as <c>Query&lt;T&gt;(string, …)</c> but returns
/// each matching document as a <see cref="JsonNode"/> (no deserialize to <c>T</c>). Tenancy + global filters
/// apply.</summary>
Task<IReadOnlyList<JsonNode>> Query(Type type, string whereClause, object? parameters = null, CancellationToken cancellationToken = default);

/// <summary>Streaming form of <see cref="Query(Type, string, object?, CancellationToken)"/>.</summary>
IAsyncEnumerable<JsonNode> QueryStream(Type type, string whereClause, object? parameters = null, CancellationToken cancellationToken = default);
```

**Why this is nearly free (and an AOT win).** Documents are stored as JSON; every typed read already ends in
`ReadListAsync(cmd, json => DeserializeDocument(json, typeInfo, jsonOptions))` — the row **is** the JSON
string. The raw variant runs the **same SQL builder** with the projector swapped to
`json => JsonNode.Parse(json)!`. No `JsonTypeInfo<T>` is needed at all, so this path works under
reflection-disabled AOT even when no context is registered for `type` — and it skips the deserialize
allocation, so it is *cheaper* than the typed read.

**Design decisions (locked):**
- **Filter is the existing WHERE/OData string** — `Query(Type, string whereClause, …)` reuses the same engine
  and parameter binding as `Query<T>(string whereClause, …)`. **No `JsonNode`/JSON-object filter** is added:
  a JSON query-object DSL would overlap OData and repeat the dropped JsonPath feature. Query-by-example
  (`Query(Type, JsonObject example)` lowering present members to equality ANDs) is noted as a *possible thin
  future convenience*, explicitly out of scope for this cut.
- **No `IDocumentQuery<T>`-style late-bound builder.** The typed `Query<T>()` LINQ builder stays typed; the
  late-bound lane is string-filter + JSON-out only. Callers wanting rich composition already have the typed
  builder. (Keeps the surface to three methods.)
- **Return `JsonNode`, consistent with the write lane.** `Get` returns `JsonObject` (as `JsonNode?`); `Query`
  returns `JsonObject`s. Callers holding these can forward/re-serialize or convert to `JsonDocument` trivially.

### Read-lane implementation sketch
All in `DocumentStore.cs`, reusing the existing private read helpers:
- Add non-generic overloads of the read executors (or a shared core taking `Type` + a `Func<string, JsonNode>`
  projector) that build the **same** SQL via the existing `ResolveTableName(Type)`/filter/tenancy path used by
  `Get<T>`/`Query<T>` — the new `ResolveTableName(Type)`/`ResolveTypeName(Type)` overloads from the write lane
  are reused here.
- `Get(Type, id)` → the `Get<T>` command path, projector `json => JsonNode.Parse(json)`, null when no row.
- `Query`/`QueryStream(Type, whereClause, parameters)` → the `Query<T>(string, …)`/`QueryStream<T>` command
  path, same projector.
- **Global query filters:** the string-`Query<T>` path resolves `JsonTypeInfo` today; on the raw lane resolve
  filters by `Type` (`ResolveQueryFilters(type)`) and append exactly as the typed path — throw the same way if
  filters exist but the info can't be resolved. Tenancy binds `@tenantId` identically (it never needed `T`).

---

## Feature parity — how each pipeline concern is sourced on the JSON lane

| Concern | Typed path source | JSON-lane source | Work |
|---|---|---|---|
| **table / typeName / Cosmos partition** | `typeof(T)` | the `Type` arg via `ResolveTypeName(Type)`/`ResolveTableName(Type)` | add non-generic resolver overloads (trivial) |
| **Tenancy** | ambient `Func<string>` (`tenantIdAccessor`), not the doc | identical — `InsertCoreAsync`/`UpdateCoreAsync` already bind `@tenantId` | **free** |
| **Temporal history** | `AppendHistoryAsync(..., json, ...)` — takes JSON string + id + typeName | pass `node.ToJsonString(jsonOptions)` (Upsert passes `null`, reads post-merge back like today) | **free** |
| **Id extract / auto-gen** | `IdAccessor<T>` over the CLR object | node-based read/default/inject using the **same resolved JSON member name + `IdKind`** (see below) | new node accessor |
| **Version / optimistic CAS** | `VersionMapping.GetVersion/SetVersion` (`Func/Action<object,int>`) | read/bump on the node at the version JSON member; DB-side CAS already uses `VersionMapping.JsonPath` | node get/set of version |
| **Spatial sidecar** | `SpatialMapping.GetGeoPoint(document)` delegate | navigate `SpatialMapping.JsonPath` in the node → read `Latitude`/`Longitude` sub-members | node geo-reader |
| **Vector sidecar** | `VectorMapping.GetVector(document)` delegate | navigate `VectorMapping.JsonPath` in the node → read the float array (length-validate as today) | node vector-reader |
| **Interceptors** | `BeforeWrite`/`AfterWrite` with `ctx.Document as T` | fire with `ctx.Document == null`; `ctx.GetJsonDocument()` returns the node's JSON (object-mutating interceptors no-op — documented) | non-generic write context |
| **Global query filters (Update)** | `AppendGlobalFilters<T>(cmd, typeInfo)` | resolve `JsonTypeInfo` via `jsonOptions.GetTypeInfo(type)`; throw same as today if filters exist but no typeInfo | reuse existing helper |
| **Change feed publish** | `PublishChange(type, id, document)` with the CLR object | publish id + typeName + change-type + JSON; materialize `T` for typed subscribers on demand via `jsonOptions.GetTypeInfo(type)` **only when there are subscribers** | node/lazy publish |
| **Auto-embed (`BeforeInsert` hook)** | mutates the object to fill the vector from text | **does not run** — caller must supply the vector in the JSON | documented limitation |

Two behaviors that are **documented limitations**, not bugs:
1. **Object-mutating interceptors do not fire** (there is no `T` to mutate). JSON-shaped interceptors via
   `GetJsonDocument()` work fully. State this in `SKILL.md` and the docs page.
2. **Auto-embedding does not run** on this lane — the vector must be present in the supplied JSON, or the
   vector sidecar row is simply skipped (same "empty embedding → skip" rule as today in `VectorUpsertAsync`).

---

## Implementation sketch

All in `src/Shiny.DocumentDb/DocumentStore.cs` (+ `Internal/IdAccessor.cs`, mapping readers). No provider
changes.

### 1. Non-generic resolvers
Add `string ResolveTypeName(Type)` / `string ResolveTableName(Type)` next to the generic ones (they just
forward `typeof(T)` today). All mapping resolvers (`ResolveSpatialMapping`, `ResolveVectorMapping`,
`ResolveVersionMapping`, `ResolveQueryFilters`) already accept `Type`.

### 2. Node-based Id handling (`Internal/IdAccessor.cs`)
Add a `Type`-keyed lookup that resolves the **JSON member name** for the Id (the same resolution
`IdAccessor.Create` does via `JsonTypeInfo.Properties`, honoring naming policy / `[JsonPropertyName]` /
`[Document(Id=...)]`) plus the `IdKind`, and exposes node ops — the typed CLR delegates are **not** needed:

```csharp
string?  ReadId(JsonObject doc);        // null/absent → not set
bool     IsDefaultId(JsonObject doc);   // absent, or default(Guid)/0/"" per IdKind
void     WriteId(JsonObject doc, string id);
```

Insert Id rules mirror the typed path exactly:
- Guid/int/long default/absent → `GenerateIdAsync(session, kind, table, typeName, ct)` then `WriteId`.
  (int/long is MAX+1 inside the txn, as today.)
- String default/absent → throw `InvalidOperationException` ("Insert requires a non-empty string Id…").
- Update/Upsert with default/absent Id → throw (same messages as the typed path).

### 3. Node-based version get/set
Reuse the version JSON member name (resolved in `ResolveVersionJsonPaths`). Insert → set `1`. Update →
`expected = read(node)`; write `expected+1`; pass `(expected, VersionMapping.JsonPath)` to
`UpdateCoreAsync`. Upsert → `expected>0 ? bump : set 1`, pass through to `UpsertMergeCoreAsync`. The
DB-side CAS is already JSON-path based, so `ConcurrencyException` behavior is identical.

### 4. Node-based spatial/vector readers
`SpatialUpsertAsync`/`VectorUpsertAsync` gain a path that, given the node, navigates the mapping's
`JsonPath` and reads the geo point (`Latitude`/`Longitude`) / float array instead of invoking the CLR
delegate — then feeds the **same** `provider.BuildSpatialUpsertSql`/`BuildVectorUpsertSql`. Length
validation stays in `VectorUpsertAsync`.

### 4b. Mapped-property presence validation
Before the spatial/vector readers run, validate presence for each registered mapping whose path the lane
reads (`ResolveSpatialMapping(type)`, `ResolveVectorMapping(type)`):
- Navigate the mapping's `JsonPath` in the node. **Key absent → collect the path.** Present (including
  explicit `null`) → OK.
- Run for **Insert**, **Update**, and **Upsert only when `IsDefaultId(node)` is true** (no Id ⇒ insert).
- If any paths were collected, throw
  `new InvalidOperationException($"Document of type '{typeName}' is missing mapped propert{y/ies}: {paths}. The JSON write lane stores the body AS-IS, so every mapped property must be present (use JSON null to indicate no value).")`
  **before** any row is written, so the whole call (and, for arrays, the whole batch) rolls back. In the
  array loop, validate each element as it is processed — the first missing-path element throws and rolls the
  batch back, same as a dup Id.

### 5. Single vs array dispatch + transaction
- `JsonObject` → one `ExecuteAsync(table, session => …)` running: interceptor `BeforeWrite` → Id
  resolve/inject → version → `InsertCore/UpdateCore/UpsertMergeCore` → spatial → vector → history →
  `AfterWrite`; then `PublishChange`. Return `1`.
- `JsonArray` → open **one** `ExecuteAsync` session and run every element through that same per-element
  body inside the single transaction (atomic / all-or-nothing; the first failure — dup Id on Insert,
  missing row on Update, version conflict — throws and rolls the whole call back). Return the count.
  (A multi-row-statement optimization is possible later but is not required; correctness-first single-txn
  loop ships in this release.)
- `JsonValue`/other → `throw new ArgumentException("Insert/Update/Upsert(Type, JsonNode) requires a JsonObject or JsonArray.")`.

### 6. Non-generic write context
Add a `NewWriteContext(DocumentOperation, string typeName, object? id, JsonNode node)` overload whose
`GetJsonDocument()` returns the node directly (no serialize delegate needed — we already hold the JSON) and
whose `Document` is `null`. `BeforeWrite`/`AfterWrite` run normally.

---

## Testing (`tests/Shiny.DocumentDb.Tests`, run the suite before "done")

Add a `JsonDocumentWriteTests` fixture, parameterized across the in-repo providers (at minimum SQLite;
include the others the existing write tests cover):

- **Round-trip:** `Insert(type, jsonObject)` then typed `Get<T>` returns the same document.
- **Id auto-gen:** Guid/int/long absent → Id generated and readable; string absent → throws; supplied Id
  honored.
- **Array:** `Insert(type, jsonArray)` returns N; all N present; a mid-array dup Id rolls back **all** N.
- **Update:** replaces; missing Id throws and rolls back the batch.
- **Upsert:** merges (RFC 7396) when present, inserts when absent; null-property strip matches typed Upsert.
- **Version/CAS:** stale version in the node → `ConcurrencyException`; success bumps the stored version.
- **Tenancy:** ambient tenant is written; cross-tenant Update no-ops/throws same as typed.
- **Temporal:** history row appended for Insert and Update; `AsOf`/`History` see it.
- **Spatial + vector:** sidecar rows written from the JSON; `WithinRadius`/vector search find the doc;
  wrong vector length throws.
- **Interceptor:** a `BeforeWrite` interceptor sees the JSON via `GetJsonDocument()`; object-mutating
  interceptor is a no-op (asserted, so the documented limitation is pinned).
- **Shape contract:** a payload with wrong-cased members stores wrong-cased (documents the AS-IS rule).
- **Bad input:** `JsonValue` throws `ArgumentException`.
- **Missing mapped property:** with a spatial (or vector) mapping registered, `Insert`/`Update` of a node
  that omits that path throws `InvalidOperationException` naming the path and writes nothing; a present-but-`null`
  path does **not** throw (writes the doc, skips the sidecar). An Upsert **with an Id** that omits the path
  does **not** throw (partial merge); an Upsert **without an Id** does throw (insert). Array: one element
  missing the path rolls back the whole batch.

### Read lane
- **Get round-trip:** typed `Insert<T>` then `Get(type, id)` returns a `JsonNode` whose members match the
  stored JSON; missing id → `null`.
- **Query filter parity:** `Query(type, whereClause, parameters)` returns the same document set as
  `Query<T>(whereClause, parameters)`, as `JsonNode`s; `QueryStream` yields the same sequence.
- **Tenancy + global filters:** cross-tenant / filtered-out documents are excluded from `Get`/`Query` exactly
  as on the typed path.
- **AOT / no typeInfo:** `Get`/`Query` succeed for a `type` with no registered `JsonTypeInfo` (the raw lane
  never deserializes) — asserts the AOT advantage.
- **Write→read symmetry:** `Insert(type, jsonObject)` then `Get(type, id)` returns an equal node (pins the
  full late-bound round-trip).

---

## The four-artifact checklist (per `CLAUDE.md`)

1. **Code + tests** — as above. Feature lives on `IDocumentStore`, works across every provider (no
   provider-specific code); it's a **core** feature, not backend-scoped — note that in the release note.
2. **Docs site** (`~/Desktop/dev/documentation/src/content/docs/documentdb/`) — update `crud.mdx` with a
   "Late-bound / JSON lane" section covering **both** writes and reads: write object + array examples, the
   AS-IS shape rule, the mapped-property presence rule (absent mapped path throws, present-`null` is a
   deliberate "no value", Upsert-with-Id is exempt), the two documented limitations; plus the read methods
   (`Get`/`Query`/`QueryStream` → `JsonNode`), that filtering uses the existing WHERE/OData string (no
   filter-by-JsonNode), and the AOT/no-typeInfo advantage. Add a **release note** under `## 10.0 TBD` in
   `release-notes.mdx`:
   `<RN type="feature">Late-bound JSON lane — Insert/Update/Upsert and Get/Query/QueryStream by Type + JsonNode …</RN>`.
3. **Skill** (`skills/shiny-documentdb/SKILL.md`) — add the three write `(Type, JsonNode)` signatures and the
   three read `(Type, …) → JsonNode` signatures, the object-vs-array rule, the "JSON stored AS-IS / caller
   owns property casing" contract, the mapped-property presence rule, the "filter via the existing string,
   not a JsonNode" note, and the interceptor/auto-embed limitations. Add keywords (`JsonNode`, `late-bound`,
   `Insert(Type`, `Query(Type`) to `triggers:`.
4. **readme.md** (repo root) — add the late-bound JSON lane (read + write) to the feature list.

---

## Resolved decisions (was: open questions)

- **Change-feed payload type for typed subscribers — RESOLVED.** `ChangeBroadcaster` is already internally
  keyed by `Type` (`ConcurrentDictionary<Type, object>` of `Subject<T>`) but only exposes a generic surface
  (`Observe<T>`/`HasSubscribers<T>`/`Publish<T>`). The JSON lane holds a `Type`, not `T`, so:
  - Add a non-generic entry point `void Publish(Type type, DocumentChangeType changeType, string id, string? json, JsonSerializerOptions jsonOptions)`
    to `ChangeBroadcaster`, backed by a non-generic `ISubject` interface that `Subject<T>` implements with
    `bool HasSubscribers` + `void EmitJson(DocumentChangeType, string id, string? json, JsonSerializerOptions)`.
  - `Publish(Type, …)` looks up the subject for `type`; **no subject / no subscribers → no-op** (cheap
    dictionary miss, same as the typed hot path). Only when `HasSubscribers` does `EmitJson` run.
  - Inside `EmitJson`, the subject knows `T`, so it lazily deserializes the JSON to `T` via
    `jsonOptions.GetTypeInfo(typeof(T))` and emits `DocumentChange<T>`. If the typeInfo is unresolvable
    (pure reflection-disabled AOT with no context for `T`), emit `Document = null` — a **JSON-only change**
    (`DocumentChange<T>.Document` is already nullable; `Removed` publishes null today). Document this AOT
    edge case. In practice `T` is compile-time known to any `NotifyOnChange<T>` subscriber, so its typeInfo
    is virtually always resolvable.
  - `DocumentStore.PublishChange` on the JSON lane calls this non-generic overload with the post-write JSON
    (Upsert reads back the merged doc as today). No behavioral change to the typed `PublishChange<T>`.
- **`UnitOfWork` integration — RESOLVED as future work.** Out of scope for this cut — `UnitOfWork` buffers
  typed `Add/Update/Upsert`. A JSON-node buffered op could follow later; noted as future work, not built now.

**Plan is build-ready. JsonPath query API (`docs/plans/json-path-query-api.md`) was dropped — the existing
LINQ / OData / computed-property / typed-query mechanisms already cover query needs.**
