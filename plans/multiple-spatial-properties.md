# Plan: Multiple indexed geometry properties per document type

**Status:** Phase 0 **shipped** on `13.0`; phases 1-5 designed, not started.
**Target version:** `13.1` (additive feature).
**Packages touched:** `Shiny.DocumentDb` (core) + the 6 relational spatial providers + Cosmos / MongoDB / Redis.

> Self-contained build spec. Read `CLAUDE.md` (repo root) for the four-artifact rule (code+tests, docs site,
> skill, readme) before considering any commit "done". Branch off `v13`.

---

## Goal

Let a document type declare **more than one independently-indexed geometry property**, and let every spatial
query name which one it means:

```csharp
options.ConfigureDocument<Delivery>(cfg => cfg
    .MapSpatialProperty(x => x.Origin)        // GeoPoint
    .MapSpatialProperty(x => x.Destination)   // GeoPoint
    .MapSpatialProperty(x => x.Route));       // GeoLineString

// each indexed separately, each addressable
var inbound = await store.GeoIntersects<Delivery>(x => x.Destination, neighbourhood);
var crossing = await store.GeoCrosses<Delivery>(x => x.Route, riverLine);

// and in a typed Where, scoped to the right index
var q = store.Query<Delivery>().Where(x => DocumentFunctions.Within(x.Destination, zone));
```

Today `DocumentMappingRegistry.AddSingle` (`src/Shiny.DocumentDb/DocumentMappingRegistry.cs:342`) throws on the
second `MapSpatialProperty` call for a type: *"'Delivery' already has a spatial mapping on 'Origin'; it cannot
also be mapped on 'Destination'."*

## Why — the index-selectivity argument

The workaround people are pointed at is `GeoGeometryCollection`: put both shapes in one property. That is the
right answer when the shapes are **one semantic thing** (a service area made of three disjoint polygons — the
union envelope is exactly what you want to index). It is the wrong answer when the shapes are **semantically
distinct**, because `SpatialMapping.ResolveEnvelope` (`src/Shiny.DocumentDb/Internal/SpatialMapping.cs:40`)
unions them into a single bounding box:

- A Vancouver → Toronto delivery collapses to a box covering half of Canada.
- "Deliveries destined for this neighbourhood" then prunes nothing — every row survives the R\*Tree/GiST filter
  and lands in the managed refine loop (`DocumentStore.Geometry.cs:92-100`).
- The index is not merely unhelpful; it is actively misleading, because the query *looks* indexed.

There is no modelling trick that fixes this. Separate slots need separate envelopes.

## Non-goals

- **No "any mapped shape matches" fan-out query.** A single query names exactly one property. An implicit
  OR-across-properties query returns the same document once per matching shape and forces a dedup pass; the
  caller can union two explicit queries if they want that.
- **No predicates between two mapped properties of the same document** (`Intersects(x.Route, x.ServiceArea)`).
  Every provider pushdown assumes one field and one constant geometry (`ExpressionLowerer.cs:200-206`).
- **No new provider tiers.** The set of spatial-capable providers is unchanged (SQLite, PostgreSQL, SQL Server,
  Oracle, MySQL, DuckDB, Cosmos, MongoDB, Redis). LiteDB / IndexedDB / Azure Table / DynamoDB / RavenDB /
  Firestore stay out.
- **No change to the JSON-collection lane.** `DynamicFieldBinder.cs:71` still refuses geo functions over a
  schema-free collection — a mapping is a typed-document concept.
- **No per-property capability differences.** If a provider supports spatial, it supports N properties.

---

## Current state — what actually works today

Establish this before writing code; two of the three plausible readings of "multiple geometries" are already
handled, and one of them is **silently wrong**.

| Reading | Today |
|---|---|
| Many shapes, one semantic slot | ✅ Works. `GeoGeometryCollection` / `GeoMultiPolygon` / `GeoMultiPoint` are mapped and indexed by their union envelope. No change needed. |
| Second geometry property queried via typed LINQ | ⚠️ **Broken on 5 of 6 relational providers** — see below. |
| Second geometry property *indexed* | ❌ Blocked by `AddSingle`. This plan. |

### The Phase 0 bug

`ExpressionLowerer.LowerPredicateMethod` (`Internal/Query/ExpressionLowerer.cs:192-206`) resolves the geometry
side of a `DocumentFunctions` predicate from **the expression's own JSON path**, not from the mapping registry.
It happily lowers `Where(x => DocumentFunctions.Intersects(x.Destination, poly))` on an unmapped property into a
`SpatialPredicateNode` carrying `JsonPath = "destination"`. What providers do with that path diverges:

| Provider | Uses `jsonPath`? | Result on an unmapped property |
|---|---|---|
| SQLite | ✅ `docdb_st_*(json_extract(Data, '$.{jsonPath}'), …)` | Correct (unindexed scan) |
| Cosmos | ✅ `ST_INTERSECTS(c.data.{path}, …)` | Correct (unindexed) |
| PostgreSQL | ❌ ignores it — `ST_Intersects(geom, …)` over the sidecar | **Silently answers about the mapped property** |
| SQL Server | ❌ ignores it — `geom.STIntersects(…)` | **Silently wrong** |
| MySQL / MariaDB | ❌ ignores it | **Silently wrong** |
| Oracle | ❌ ignores it — `SDO_RELATE(geom, …)` | **Silently wrong** |
| DuckDB | ❌ ignores it | **Silently wrong** |
| MongoDB / Redis / LiteDB / IndexedDB | n/a — client-side expression evaluation | Correct |

The sidecar `geom` column holds only the mapped property's geometry, so the `Id IN (SELECT docId FROM
{table}_spatial WHERE typeName = @typeName AND ST_…(geom, …))` subquery answers a question the caller did not
ask. This is a live defect on `v13`, independent of this feature, and it must be closed before N mappings make
it N times more reachable.

---

## Design decisions (locked)

| Decision | Choice | Consequence |
|---|---|---|
| Registry shape | `Dictionary<Type, List<SpatialMapping>>`, keyed within the list by `PropertyName` — mirroring the existing blob pattern (`DocumentMappingRegistry.AddBlob`, lines 241-254) | Re-mapping the *same* property replaces; a *different* property appends. Duplicate property name is still a config error. |
| Terse API preserved | `MapSpatialProperty(x => x.Location)` unchanged; `store.GeoIntersects<T>(geom)` unchanged when exactly one property is mapped | Zero migration for the overwhelmingly common single-geometry type. Geo reference data (`Shiny.DocumentDb.Geo`) needs no edit. |
| Ambiguity | The no-selector overload throws when a type has >1 mapping, naming the mapped properties and the selector overload | Never guess. Never silently pick the first. |
| Sidecar keying | Add a `propertyName` column; key becomes `(docId, typeName, propertyName)` | One sidecar table per document table, N rows per document. Query joins gain `AND r.propertyName = @spatialProperty`. |
| Sidecar migration | Detect the old shape at init → **drop and rebuild** the sidecar by streaming the type's documents | The sidecar is pure derived data, so a rebuild is always safe. SQLite cannot widen a table-level `UNIQUE` in place, so an `ALTER` path would need a rebuild there anyway — one uniform code path beats six. |
| Query scoping | `BuildSpatialFilterSql` / `BuildSpatialDistanceSql` gain a property-name parameter; emitters must resolve the node's `JsonPath` to a mapping and fail loudly if it does not | Closes the Phase 0 bug and makes the N-property case correct by construction. |
| Native index per path | Cosmos `SpatialIndexes` and Mongo `2dsphere` are created per mapped path (both already iterate by `JsonPath`) | Nearly free — both loops just move from `.Values` over a dictionary to a flattened list. |

### Rejected: one sidecar table per property (`{table}_spatial_{property}`)

Tempting because it needs no migration at all — the existing table keeps its exact meaning and extra properties
get their own. Rejected because: SQLite creates 3 R\*Tree shadow tables per virtual table (so 4 tables per
mapped property per document table); Oracle (30 chars pre-12.2 semantics) and MySQL identifier budgets make
`{table}_spatial_{property}` collision-prone; and the admin tool's system-table suppression already
pattern-matches fixed `_spatial*` suffixes (`ShinyDocDbMyAdmin.Core/Services/DocumentAdminService.cs:87`).

---

## Phase 0 — close the wrong-property bug — ✅ SHIPPED (`13.0`)

Built at the **lowering** layer rather than in the emitters as originally sketched. `ExpressionLowerer` is the
only place `SpatialPredicateNode` / `SpatialDistanceNode` are constructed, and it already holds both the field
expression and the document's `JsonTypeInfo` — so one funnel (`GeometryFieldJsonPath`) covers the LINQ surface,
the string grammar (which lowers through the same path) and `Distance`-in-`OrderBy`, across every relational
provider *and* Cosmos, instead of the same check being re-implemented per emitter.

What landed:

- `DocumentMappingRegistry.SpatialJsonPathsFor(Type)` — the mapped JSON paths for a type, cached; an empty set
  when the type has no mapping. Set-shaped already, so Phase 1 only changes what fills it.
- `Internal/Query/SpatialPathGuard.cs` — one throw and one message, shared by the lowerer and MongoDB.
- `ExpressionLowerer.Lower` / `.LowerValue` take an optional `IReadOnlySet<string>? spatialPaths`
  (`null` = the caller cannot determine them, e.g. the schema-free collection lane → check skipped).
- Threaded from every typed translation site: `JsonExpressionVisitor.Translate`, `DocumentQuery`
  (`BuildWhereClause` + the method-call `OrderBy` branch, via a `ResolveSpatialPaths()` helper mirroring the
  existing `ResolveFullText()`), `ProjectedDocumentQuery`, `GroupedDocumentQuery`,
  `JsonProjectionDocumentQuery`, the `DocumentStore` filter and global-query-filter sites,
  `CosmosExpressionVisitor`, and `MongoExpressionVisitor`.
- MongoDB was included even though it answered *correctly* (unindexed): one backend accepting a query the
  others reject is the portability bug this is meant to prevent.

Tests: `Unmapped_Geometry_Property_In_Where_Throws` on the shared `DocumentFunctionsSpatialProviderTestsBase`
(green on SQLite, DuckDB, PostgreSQL, CockroachDB, MySQL, SQL Server, MongoDB; Oracle-native skipped for want
of `ORACLE_SPATIAL_IMAGE`), plus SQLite-local cases for `OrderBy(Distance(…))`, the string grammar, and a type
with no spatial mapping at all. Full suite green. Release-noted as `type="fix"`; docs, skill and readme updated.

**Gap left open deliberately:** LiteDB / IndexedDB evaluate predicates client-side and are not spatial-capable,
so a `DocumentFunctions` geo call there still evaluates in memory rather than throwing. They have no sidecar to
disagree with, so nothing is silently wrong — but it is a divergence to close if they ever gain a spatial tier.

## Phase 1 — registry accepts N mappings

- `DocumentMappingRegistry`: `spatialMappings` → `Dictionary<Type, List<SpatialMapping>>`; replace the
  `AddSingle` call in `AddSpatial` (line 276) with an `AddBlob`-shaped `AddSpatial` that replaces by
  `PropertyName` and appends otherwise. **Leave `AddSingle` in place** — vector and full-text still use it.
- Keep `ResolveSpatialMapping(Type)` returning `SpatialMapping?` for the single case (throwing on ambiguity),
  and add `ResolveSpatialMappings(Type) → IReadOnlyList<SpatialMapping>` and
  `ResolveSpatialMapping(Type, string propertyName)`.
- `SpatialMappings` property → `IReadOnlyDictionary<Type, List<SpatialMapping>>`; update the three
  `SupportsSpatial => …SpatialMappings.Count > 0` probes (Cosmos `:116`, Mongo Geometry `:16`, Redis `:99`) and
  `DocumentStore.cs:287`.
- Mirror the resolve members on the four options shims: `DocumentStoreOptions.cs:180`,
  `CosmosDbDocumentStoreOptions.cs:107`, `MongoDbDocumentStoreOptions.cs:106`, `RedisDocumentStoreOptions.cs:140`.
- `SpatialMappingFactory.ResolveJsonPaths` already takes `IEnumerable<SpatialMapping>` — feed it the flattened
  list.
- At this phase the store still uses only the first mapping; nothing observable changes yet.

## Phase 2 — sidecar carries the property

Signature change on `IDatabaseProvider` (`src/Shiny.DocumentDb/IDatabaseProvider.cs:489-520`): the four spatial
DML builders and the two query builders take the property discriminator (as a bound `@spatialProperty`
parameter, never interpolated).

Per provider — all six are the same mechanical edit, so do one properly and mirror it:

| Provider | Sidecar today | Change |
|---|---|---|
| SQLite (`SqliteDatabaseProvider.cs:291-329,379`) | `_spatial_map(docId,typeName) UNIQUE` + rtree keyed by map rowid | Add `propertyName` to the map table and its `UNIQUE`; upsert/delete/bbox-query gain the predicate |
| PostgreSQL (`:101-142`) + CockroachDB (`:60`) | `PRIMARY KEY (docId,typeName)` + optional PostGIS `geom`/GiST | Widen PK; `ON CONFLICT` target follows |
| SQL Server (`:99-160`) | identity `rowid` PK + `UNIQUE (docId,typeName)` (895-byte clustered-key limit) | Widen the unique constraint — **re-check the 900-byte index-key budget** with a third `NVARCHAR(450)` column; likely needs narrower `propertyName` (128 is plenty) |
| MySQL (`:95-133`) + MariaDB | `PRIMARY KEY (docId,typeName)` + `SPATIAL INDEX` (needs `NOT NULL` SRID column) | Widen PK; watch the 3072-byte InnoDB key limit — size `propertyName` at `VARCHAR(128)` |
| Oracle (`:136-216`) | sidecar + `USER_SDO_GEOM_METADATA` + MDSYS index | Widen PK; metadata registration is per-table, not per-property — unchanged |
| DuckDB (`:98-138`) | `PRIMARY KEY (docId,typeName)` + R-Tree on `geom` | Widen PK |

Write path — `DocumentStore.SpatialUpsertAsync` / delete / clear (`DocumentStore.cs:1108-1165`) loop over the
type's mappings instead of resolving one, binding `@spatialProperty` per row. The null-geometry branch (purge a
stale row when the property went null) applies per property.

`DocumentStore.cs:2248` (bulk-delete falls back to the per-document path when a spatial mapping exists) and
`IsBatchFastEligible` (`:628`) switch from `!= null` to `.Count > 0`.

### Migration

At table init, after `BuildCreateSpatialTablesSql`, probe the sidecar for the `propertyName` column (each
provider supplies a `BuildSpatialSchemaProbeSql`). If absent: drop the sidecar (and, on SQLite, the R\*Tree
virtual table and its shadow tables), recreate, then re-index by streaming every document of every
spatially-mapped type through `SpatialUpsertAsync`. Log it at information level — one line stating the table
and the row count rebuilt. It is one-time and idempotent; a crash mid-rebuild leaves the new (empty or partial)
sidecar and the probe still passes, so **write a completion marker or make the rebuild re-runnable** — decide
this in build (see open questions).

## Phase 3 — property-selected query API

Add an `Expression<Func<T, Geometry?>>` (and `Func<T, GeoPoint?>` for the point methods) leading overload to:

- the 11 `Geo*<T>` methods (`IDocumentStore.cs:335-375`, implemented in `DocumentStore.Geometry.cs`),
- `WithinRadius<T>` / `NearestNeighbors<T>` (`IDocumentStore.cs:298,322`),
- the Cosmos implementations (`CosmosDbDocumentStore.Geometry.cs:56`, `CosmosDbDocumentStore.cs:1221,1276,1318`),
- Mongo (`MongoDbDocumentStore.Geometry.cs:164` `RequireSpatial<T>`) and Redis (`RedisDocumentStore.cs:1021,1045`).

Resolution helper, one place, shared:

```csharp
SpatialMapping RequireSpatial<T>(string? propertyName)   // null = "the" mapping
```

throwing `InvalidOperationException` with the mapped property list when `propertyName` is null and the type has
more than one. `GeometryQuery` (`DocumentStore.Geometry.cs:46`) threads the resolved mapping's property name
into `BuildSpatialBoundingBoxQuerySql`.

Also thread it through the string-expression grammar per the query-surface-parity rule in `CLAUDE.md`: the
existing `FilterExpressionParser` geo functions already take a field path, so the work is confirming the
Phase 0 guard resolves that path against the (now plural) mappings — no new grammar.

## Phase 4 — native per-path indexes

- Cosmos (`CosmosDbDocumentStore.cs:210`): iterate the flattened mapping list — one `SpatialPath` per property.
  Verify Cosmos accepts multiple spatial paths on one container (it does; confirm against the emulator test).
- MongoDB (`MongoDbDocumentStore.Geometry.cs:198` `EnsureGeoIndexAsync`): the index name is already derived from
  `mapping.JsonPath`, so N mappings give N distinctly-named `2dsphere` indexes. Confirm the index-ensured cache
  is keyed by (collection, path) and not just collection — `MongoDbDocumentStore.cs:1099-1101` needs the same.
- Redis: one geo key per mapped property; confirm the key naming already includes the property.

## Phase 5 — everything else

- `DocumentConfigurationValidator` (`:43,80`): both loops iterate `SpatialMappings` — update for the list shape
  so the "randomized-encrypted property cannot be spatial" check runs per mapping.
- `DocumentStore.JsonLane.cs:36,355`: the JSON write lane resolves one spatial mapping — same loop treatment.
- Admin (`ShinyDocDbMyAdmin.Core/Services/DocumentAdminService.Geometry.cs`, `Tui/Widgets/GeoCanvas.cs`) and MCP
  (`Shiny.DocumentDb.Mcp/Internal/McpResourceFactory.cs`): audit for a single-geometry assumption; the map view
  needs a property picker when a type has several.
- Docs (`~/Desktop/dev/documentation/src/content/docs/documentdb/`): update the spatial page with the multi-slot
  example **and the "when to use a GeometryCollection instead" guidance** — the union-envelope explanation
  belongs in the docs, not just this plan. Release notes: one `fix` (Phase 0) + one `feature`.
- `skills/shiny-documentdb/SKILL.md`: the default guidance stays "one mapped geometry"; add the multi-property
  form and the rule for choosing between it and a collection type.
- `readme.md`: feature list mention.

---

## Testing

Full suite, Docker up (`CLAUDE.md` rule — no claiming green from a filtered subset).

- **Conformance suite addition** (runs for every spatial-capable provider): a two-geometry type
  (`Origin` point + `Route` linestring) asserting (a) each property indexed independently, (b) a query on one
  property does not match documents that only satisfy the other, (c) nulling one property purges only its
  sidecar row, (d) delete purges all rows, (e) `ClearAll` purges all rows.
- **Selectivity regression:** insert a long route whose endpoints are far apart; assert a query near one
  endpoint bbox-prunes the other. This is the whole point of the feature — prove the index is doing work, not
  just that the answer is right.
- **Ambiguity:** the no-selector overload throws on a two-mapping type and still works on a one-mapping type.
- **Phase 0 guard:** per relational provider, an unmapped-property predicate throws rather than answering.
- **Migration:** create a store on the old sidecar shape (raw DDL in the test), open it with the new code,
  assert rebuild + correct query results. Per relational provider.
- **Unchanged single-mapping behaviour:** the existing spatial tests must pass untouched. If any needs editing,
  the terse API was not actually preserved.

## Open questions for build

1. **Rebuild crash-safety.** Marker table, or make the probe check row counts against document counts? Simplest
   defensible answer: rebuild inside the same transaction as the DDL where the engine allows transactional DDL
   (PostgreSQL, SQL Server, SQLite) and accept re-runnable rebuilds elsewhere.
2. **`propertyName` column width.** Fixed 128 chars everywhere keeps SQL Server's clustered-key budget and
   MySQL's InnoDB limit safe — confirm no real property name exceeds it (they are CLR identifiers, so no).
3. **Redis geo key naming** — verify it is already property-qualified; if not, that is a data migration too.
4. **Does anything key a cache on "the" spatial mapping per type?** Grep the index-ensured caches in Mongo and
   the table-init memo in `DocumentStore.cs:287` before Phase 4.

## Effort

Phase 0 is small and independently shippable. Phases 1-5 are roughly a 6-provider mechanical edit plus ~28
call sites (`ResolveSpatialMapping` / `SpatialMappings` across core, Cosmos, Mongo, Redis) plus the migration
path — the migration and the SQL Server / MySQL index-key budgets are the only parts with real unknowns.
