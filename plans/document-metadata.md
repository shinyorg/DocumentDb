# Plan: `DocumentMetadata` — store-owned timestamps on the typed document

**Status:** built on `14.0` (all providers). See *As built* below for where the implementation differs from the design.
**Target version:** `14.0` (additive; rides the v14 beta).
**Packages touched:** `Shiny.DocumentDb` (core) + every provider (hydration + query lowering) + generator.

> Self-contained build spec. Read `CLAUDE.md` (repo root) for the four-artifact rule (code+tests, docs site,
> skill, readme) before considering any commit "done".

---

## As built (2026-09-21)

- **Body exclusion is a strip, not a `JsonTypeInfo` modifier.** Serialize, then `MetadataSupport.StripFromBody` removes
  the property (a streaming raw-byte copy, only for types that declare metadata). A modifier would have had to mutate the
  caller's shared `JsonSerializerOptions` (affecting their own API serialization) and would miss explicitly-passed
  `JsonTypeInfo`s from another context. `DocumentMetadata` carries a type-level `[JsonConverter]` so every resolver
  (reflection, `JsonSerializerContext`, `DocumentSerialization.Generated`) serializes it without registration.
- **Relational reads widen the select list only for metadata types** (`MetadataSupport.SelectColumns`), so existing SQL
  (and `ToQueryString` output) is unchanged for everyone else. Provider-owned search SQL (spatial / vector / full-text)
  is untouched — those paths stamp with one follow-up `IN` lookup (`StampFromEnvelopeAsync`).
- **Write cores return their timestamp** (`InsertCoreAsync` etc. → `Task<DateTimeOffset>`), microsecond-truncated, so the
  stamped value equals the stored one. Document providers use their own storage precision (Mongo: milliseconds; the
  string-envelope providers keep full ticks).
- **Upsert `CreatedAt`:** stamped wherever the provider knows the insert branch ran (every document provider); the
  relational native upsert can't tell, so it leaves `CreatedAt` as the caller had it.
- **Projections touching metadata project client-side** on the relational providers (filters/order/paging stay in SQL) —
  the timestamps aren't in the body a SQL `json_object` reads. Document providers already project client-side.
- **Temporal:** snapshots are newed up unstamped (`IsPersisted == false`); `Restore` carries the live `CreatedAt`.
- **Query push-down:** relational, MongoDB/Amazon DocumentDB, Cosmos, Firestore, Azure Table, DynamoDB push down to the
  envelope; Redis (timestamps not in the RediSearch index), RavenDB, LiteDB, IndexedDB evaluate in memory over stamped
  candidates. IndexedDB is unverified at runtime (no harness).

## Goal

Every provider already stamps `CreatedAt` / `UpdatedAt` on the stored envelope (relational columns, Mongo
`createdAt`/`updatedAt`, Cosmos item root, LiteDB/Redis/Firestore/Raven/AzureTable/Dynamo/IndexedDB envelope
fields). None of it reaches the typed `T` — the only way to see it today is backup/export or `DocumentRecord`.
Expose it, read-only, on the document itself, and make it queryable against the envelope (indexed column), not
the JSON body:

```csharp
public class Order
{
    public string Id { get; set; } = "";
    public decimal Total { get; set; }
    public DocumentMetadata? Metadata { get; set; }   // discovered by type; the store news it up on load
}

var order = await store.Get<Order>(id);
order.Metadata.CreatedAt;   // stamped from the envelope on read
order.Metadata.UpdatedAt;

var stale = await store.Query<Order>()
    .Where(x => x.Metadata.UpdatedAt < cutoff)          // → envelope column / root field
    .OrderBy(x => x.Metadata.CreatedAt)
    .ToList();

store.Query<Order>().Where("Metadata.UpdatedAt < @0", cutoff);   // string grammar parity
```

## The type

```csharp
namespace Shiny.DocumentDb;

public sealed class DocumentMetadata
{
    public DateTimeOffset CreatedAt { get; internal set; }
    public DateTimeOffset UpdatedAt { get; internal set; }

    /// True once the store has stamped this instance (read or successful write). False on a fresh `new()`.
    public bool IsPersisted { get; internal set; }
}
```

- Public parameterless ctor (so users *may* initialise it, though they never need to); setters **internal** — user code cannot set them,
  which is the whole "read-only" guarantee. Mirrors `DocumentBlob`'s internal-setter model.
- Deliberately small. See *Non-goals* for what is **not** on it and why.

## Design decisions

1. **Discovered by type, zero configuration.** A property of type `DocumentMetadata` on `T` is the opt-in —
   no `cfg.MapMetadata(...)`. At most one per type; a second one fails validate-on-build
   (`DocumentConfigurationValidator`, new code). Found through `JsonTypeInfo.Properties` (ignored properties
   still appear there — AOT-safe, works for Reflection / JsonContext / Generated resolvers alike).

2. **Never stored in the body.** A `JsonTypeInfo` modifier (same seam as field encryption) sets
   `ShouldSerialize = false` and drops the deserialize setter for the metadata property when the store
   serializes. **Capture the original `JsonPropertyInfo.Set` delegate before nulling it** — decision 3 needs
   it to assign the new instance. The envelope is the single source of truth — no JSON copy to drift, no JSON path to query by
   accident. Outside the store (e.g. an API returning the POCO) the object serializes normally, which is the
   desirable behaviour.

3. **Always populated — the library news it up.** On every load (and every write stamp-back), if the
   property is null the store creates a `DocumentMetadata` and assigns it through the captured setter, then
   stamps it; if non-null it stamps that instance in place. A document returned by the store therefore
   **never** has a null `Metadata` — users don't write `= new()` and don't null-check.
   - Consequence: the property **must be assignable** — public `set`, `init`, or a non-public setter marked
     `[JsonInclude]` (all of which give `JsonPropertyInfo` a `Set` delegate, AOT-safe). A get-only property
     is rejected by validate-on-build (`DocumentConfigurationValidator`) with a message pointing at
     `{ get; set; }`. No reflection on backing fields.
   - Declare it nullable (`DocumentMetadata?`) or not — both work; nullable is the honest shape for an
     object the user constructed but hasn't saved (`Metadata` is null until the first write stamps it).
   One shared helper in core (`Internal/MetadataSupport.cs`) —
   `Stamp(object doc, DateTimeOffset created, DateTimeOffset updated)` driven by a cached per-type
   get/set accessor pair.

4. **Writes stamp back.** After a successful write the passed instance is updated:
   - Same null rule as decision 3: a null `Metadata` on the passed instance is newed up, then stamped.
   - `Insert` / `BatchInsert`: `CreatedAt = UpdatedAt = now`, `IsPersisted = true`.
   - `Update` / `SetProperty` / `ExecuteUpdate`-by-id paths that hold the instance: `UpdatedAt = now`.
   - `Upsert`: `UpdatedAt = now`; `CreatedAt = now` only if the provider knows the insert branch ran,
     otherwise left as-is (don't lie — a fresh object upserted over an existing row keeps `CreatedAt` default
     until re-read). Documented.
   - Cancelled writes (`ctx.Cancel()`) stamp nothing.
   - **Precision gotcha:** stamp the value *as the provider will round-trip it* (Mongo = millisecond UTC,
     SQLite = ISO text, etc.) so `written.Metadata.UpdatedAt == reread.Metadata.UpdatedAt`. Add a
     per-provider `TruncateTimestamp` hook defaulting to identity; `BackupTimestampFidelityTests` already
     covers the round-trip precision per provider — reuse its knowledge.

5. **Queries lower to the envelope, not the body.** `ExpressionLowerer` recognises a member chain that passes
   through a `DocumentMetadata`-typed member and emits a new `EnvelopeFieldNode(EnvelopeField.CreatedAt |
   UpdatedAt)` instead of a `RootFieldNode`. Emitters: relational → the column (like `ComputedColumnNode`);
   Mongo → root field; Cosmos → `c.createdAt`; others per their envelope. Providers that evaluate
   client-side (LiteDB/IndexedDB tiers, non-indexed Azure Table / Dynamo / Redis) just need metadata stamped
   **before** the in-memory filter runs — check each provider's order of operations. Where a provider can
   neither push down nor filter client-side, throw with a clear message (don't silently match nothing).

6. **String grammar parity** (CLAUDE.md rule): `FilterExpressionParser` resolves `Metadata.CreatedAt` /
   `Metadata.UpdatedAt` (by the property's CLR name) to the same `EnvelopeFieldNode` for `Where`, `OrderBy`
   and `Project`. Tests on both surfaces.

## Hydration surface audit (the real cost of this feature)

Relational reads are `SELECT Data …` in ~20 places (`DocumentStore.cs`, `Internal/DocumentQuery.cs`,
`IDatabaseProvider.cs`, `DocumentStore.JsonLane.cs`). Every typed-document read must also select
`CreatedAt, UpdatedAt` — **only when `T` has a metadata property** (cached flag), so types without it pay
nothing. Before editing, consolidate: route typed reads through one row-reader so this is one change, not 20.

Surfaces that must stamp (build the checklist into the conformance suite):

| Surface | Notes |
|---|---|
| `Get`, `Query` terminals (`ToList`, `First*`, `Single*`, stream, cursor paging) | core path |
| `DocumentSession` / `DocumentSet` / `DocumentContext` | via the store |
| Joins (`IJoinResult`) | both sides; relational JOIN selects per-side columns |
| `NotifyOnChange` / change feed payloads | where the feed carries the envelope |
| Temporal `History` / `AsOf` / `AsOfAll` | `UpdatedAt = ValidFrom` of that version; `CreatedAt` = first version's `ValidFrom` if cheaply known, else default + `IsPersisted = false`. Decide at build; document either way |
| Outbox, seeding | writes → stamp-back only |

**Out of scope (no `T` to stamp):** JSON collections (`IJsonDocumentCollection`), raw-JSON terminals
(`ToJsonList`, `WriteJsonArrayTo` — the body only; say so in docs), backup/export (already carries timestamps),
`Select` projections to anonymous/DTO types *unless* they reference `x.Metadata.*`, which lowers via
decision 5 (include projection support; it's the same node).

## Generator / AOT

- `DocumentSerialization.Generated`: the emitted resolver must keep the metadata property in `Properties`
  (so it's discoverable) and the store's modifier handles exclusion. Add a generator test.
- No reflection at stamp time — use the `JsonPropertyInfo.Get`/`Set` delegates.

## Tests

- New shared conformance suite (`DocumentMetadataConformanceTests`, same pattern as
  `DocumentQueryConformanceTests`) inherited by every provider fixture: stamp on Get/Query/stream/cursor,
  stamp-back on Insert/Update/Upsert, round-trip equality (precision), `Where`/`OrderBy` LINQ + string,
  body does **not** contain `Metadata`, type without metadata unaffected, cancelled write stamps nothing,
  null property is newed up on every read surface and on write stamp-back, pre-initialised instance is
  stamped in place (same reference), `init` and `[JsonInclude] private set` shapes work, get-only property
  fails validation, second metadata property fails validation.
- Session + DocumentContext + join + temporal cases on SQLite (+ one NoSQL).
- Full suite with Docker per CLAUDE.md.

## Phases

0. **Consolidate relational typed reads** onto one row-reader (no behaviour change; full suite green).
1. Core: `DocumentMetadata`, modifier, `MetadataSupport` stamp helper, validator rule, relational read +
   write stamp-back, SQLite tests.
2. Query: `EnvelopeFieldNode`, lowerer + relational emitters, string grammar, projection.
3. NoSQL providers: hydration + emit per provider, conformance suite everywhere.
4. Sessions/joins/temporal/change feed; generator.
5. Docs (`crud.mdx` + `querying.mdx`), release note under `## 14.0 TBD` (`type="feature"`), SKILL.md
   (+ `DocumentMetadata` trigger), readme feature list.

---

## Non-goals — and the broader "special objects" decisions (2026-09-21)

This plan is the one surviving piece of a proposed family of "special objects" (Metadata, Geometry,
GeometryCollection, Blob, BlobCollection, Vector) with a shared lazy-load / dirty-check model. That idea was
evaluated and **rejected as a unifying abstraction**, because the members don't share a storage model:

| Object | Source of truth | Sidecar role | Verdict |
|---|---|---|---|
| Blob / BlobCollection | sidecar only | storage | **Already shipped** (`DocumentBlob`, `DocumentBlobCollection`, `LoadAsync`/`LoadAllAsync`, pending-write). Nothing to add. |
| Geometry | document JSON | derived index | **No lazy geometry type.** Moving geometry out of the body breaks SQLite (filters on the JSON path) and Cosmos/Mongo (native index needs it inline). Named per-property geometry = `plans/multiple-spatial-properties.md`; many shapes in one slot = existing `GeoGeometryCollection`. |
| Vector | document JSON | derived index | **No special vector type.** Cosmos/Mongo vector search needs the vector in the item. |
| Metadata | envelope columns | none | **This plan.** |

Do **not**, going forward:

- **Put soft delete on `DocumentMetadata`.** Soft delete stays the decoupled `AddSoftDelete<T>` extension
  (query filter + interceptor). Folding it in would couple an optional feature into a core type. If ever
  wanted, a read-only `IsDeleted` surfaced *only when soft delete is enabled* — not a flag owned by metadata.
- **Put `Version` on `DocumentMetadata`.** The concurrency version is a user-owned JSON property
  (`MapVersionProperty`) used in the CAS `WHERE`; duplicating it on metadata creates two sources of truth.
- **Add TypeName / TenantId / ETag to it speculatively.** Add a field only with a concrete use case.
- **Build general dirty tracking / a change tracker.** Blobs track pending writes on the object itself; a
  general "automatic dirty check" means an EF-style change tracker, which the store deliberately does not have.
- **Add a per-object `LoadIfNeededAsync()` across all special members.** It is N+1 on every page. If bulk
  loading is needed, it is a query-level include (`.IncludeBlobs(x => x.Photos)`, backed by
  `BatchLoadBlobs`), not a per-document call.
- **Invent a shared `ILazyDocumentPart` interface** until a second genuinely lazy (sidecar-sourced) type
  exists. Today there is only one.

If read payload size (large polygons, embeddings) becomes the actual pain, the right shape is a general
per-property option — e.g. `cfg.MapProperty(x => x.Embedding, p => p.ExcludeFromDefaultLoad())` + a query
`.Include(x => x.Embedding)` — that keeps the data in the body (indexes intact) but skips it on read. Not
planned; recorded so it isn't re-derived.
