# Plan: Late-bound JSON write API (`Insert`/`Update`/`Upsert` by `Type` + `JsonNode`)

**Status:** Designed, not started.
**Target version:** `10.0.0` (raw version from `version.json`, currently `10.0.0-beta.{height}`) — additive,
no breaking changes. New methods on `IDocumentStore`; every existing typed call is untouched.

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

---

## The four-artifact checklist (per `CLAUDE.md`)

1. **Code + tests** — as above. Feature lives on `IDocumentStore`, works across every provider (no
   provider-specific code); it's a **core** feature, not backend-scoped — note that in the release note.
2. **Docs site** (`~/Desktop/dev/documentation/src/content/docs/documentdb/`) — update `crud.mdx` with a
   "Late-bound / JSON writes" section (object + array examples, the AS-IS shape rule, the two documented
   limitations). Add a **release note** under `## 10.0 TBD` in `release-notes.mdx`:
   `<RN type="feature">Insert/Update/Upsert by Type + JsonNode …</RN>`.
3. **Skill** (`skills/shiny-documentdb/SKILL.md`) — add the three `(Type, JsonNode)` signatures, the
   object-vs-array rule, the "JSON stored AS-IS / caller owns property casing" contract, and the
   interceptor/auto-embed limitations. Add keywords (`JsonNode`, `late-bound`, `Insert(Type`) to
   `triggers:`.
4. **readme.md** (repo root) — add the JSON write lane to the feature list.

---

## Open questions (resolve during build, none block design)

- **Change-feed payload type for typed subscribers.** Preferred: publish JSON + lazily deserialize to `T`
  via `jsonOptions.GetTypeInfo(type)` **only if** there are subscribers and a typeInfo is resolvable; if
  no typeInfo (pure reflection-disabled AOT with no context for `type`), publish a JSON-only change and
  document it. Confirm against the `IChangeFeedDocumentStore`/`IObservableDocumentStore` payload shape.
- **`UnitOfWork` integration.** Out of scope for this cut — `UnitOfWork` buffers typed `Add/Update/Upsert`.
  A JSON-node buffered op could follow later; note it as future work, don't build it now.
