# Plan: Expand spatial from point-only to full geometry

**Status:** Approved — ready to build (winding normalization + `Geo`-prefix naming + DE-9IM relate engine
confirmed). Not started.
**Target version:** `10.x` (new feature → minor bump off the `10.0.x` line in `version.json`). Additive —
no breaking changes to the public contract. Phased; ships in one release covering **SQLite + Cosmos +
MongoDB**, with the remaining native providers deferred (see [Phasing](#phasing) and
[Deferred](#deferred-not-in-this-release)).

> Self-contained build spec. Read `CLAUDE.md` (repo root) for the four-artifact rule (code+tests, docs
> site, skill, readme) before considering any commit "done". Branch off `v10`.

**This is the entire spatial scope — one release, no v2.** Everything that passes a single test is in:
*does it give a gain the caller can't cheaply get themselves — index-accelerated or index-enabling?* That
covers the full geometry model, the full topological-predicate family, distance (sort + k-nearest +
distance-band), and measurement/validity. Only constructive geometry algebra (which a store cannot
accelerate) is out — see [Out of scope](#out-of-scope).

---

## Goal

Upgrade the spatial feature from **point-only** (`GeoPoint` + `WithinRadius` / `WithinBoundingBox` /
`NearestNeighbors`) to **full OGC geometry** — `LineString`, `Polygon` (with holes), `MultiPoint`,
`MultiLineString`, `MultiPolygon`, `GeometryCollection` — plus the complete query surface that operates on it:

- **Topological predicate queries** (all `Geo`-prefixed): `GeoIntersects`, `GeoContainedBy`, `GeoContains`,
  `GeoDisjoint`, `GeoTouches`, `GeoCrosses`, `GeoOverlaps`, `GeoEquals`, `GeoCovers`, `GeoCoveredBy`.
- **Distance-band query**: `GeoWithinDistance(geometry, meters)` — generalizes `WithinRadius` off point-only.
- **Distance sort + geometry k-nearest** on every predicate (`orderByDistanceFrom`) and on `NearestNeighbors`.
- **Measurement + validity** on the `Geometry` model, feeding the existing indexed computed-property path.

The design is lifted from the sibling **Shiny.Spatial** library (`~/Desktop/dev/geospatialdb`), which
already runs the same two-pass query pipeline. This plan **absorbs its geometry + predicate primitives** into
DocumentDb's existing spatial feature and builds a DE-9IM relate engine on top of them. It does **not** port
Shiny.Spatial's `SpatialTable` / `SpatialFeature` API, its WKB serialization, its geofencing package, or its
seeded databases — those stay in Shiny.Spatial as its own (dependency-free, mobile-GPS) product.

## Why it's cheap (the architecture already exists)

DocumentDb's `WithinRadius` / `WithinBoundingBox` / `NearestNeighbors` (`src/Shiny.DocumentDb/DocumentStore.cs`
~lines 2209–2380) already implement the **exact two-pass pipeline** every predicate here is built on:

1. **Pass 1 — coarse bbox filter (SQL, O(log n)):** the SQLite R\*Tree sidecar
   (`{table}_spatial USING rtree(id, minLat, maxLat, minLng, maxLng)`) via
   `IDatabaseProvider.BuildSpatialBoundingBoxQuerySql(tableName, additionalWhere)`.
2. **Pass 2 — refinement (C#):** `Internal/GeoMath.HaversineDistance` on the survivors.

So the scaffolding a full-geometry engine needs — R\*Tree sidecar, the `BuildSpatial*Sql` hook family, the
`MapSpatialProperty` registration model, table-init + write-sync plumbing — **is already in the codebase**.

The R\*Tree columns are already an **envelope** (`minLat/maxLat/minLng/maxLng`); today they are fed a
degenerate point box (`SqliteDatabaseProvider.BuildSpatialUpsertSql`, ~line 266:
`VALUES (…, @spatialLat, @spatialLat, @spatialLng, @spatialLng)`). Feed that a real geometry envelope and the
existing bbox hook becomes the pass-1 filter for **every** geometry type and **every** predicate — meaning
**SQLite gets the whole predicate family with zero new provider SQL**, just envelope population + a C#
predicate refine.

## Decisions (locked)

- **Algorithm code is copied, not shared.** Port Shiny.Spatial's `PointInPolygon`, `SegmentIntersection`,
  and `DistanceCalculator` into `src/Shiny.DocumentDb/Internal/` next to `GeoMath`. Small, stable surface;
  both repos are ours. No shared micro-package, no cross-repo release coupling. Accept minor drift.
- **One DE-9IM relate engine, not per-predicate primitives.** Shiny.Spatial only ships `Intersects` /
  `Contains` primitives. Build a single `SpatialRelate(a, b)` returning the 9-intersection matrix (on top of
  the ported `PointInPolygon` / `SegmentIntersection`), then express **every** predicate as a matrix pattern.
  `Touches` / `Crosses` / `Overlaps` are subtle to hand-roll individually and fall out correctly from the
  matrix; this matches how PostGIS/JTS model it and keeps one tested relate instead of ten fragile predicates.
- **Whole predicate family is `Geo`-prefixed.** `GeoIntersects`, `GeoContainedBy`, `GeoContains`,
  `GeoDisjoint`, `GeoTouches`, `GeoCrosses`, `GeoOverlaps`, `GeoEquals`, `GeoCovers`, `GeoCoveredBy`,
  `GeoWithinDistance` — consistent, discoverable, and `GeoEquals` avoids the `object.Equals` clash. The three
  existing shipped methods (`WithinRadius` / `WithinBoundingBox` / `NearestNeighbors`) keep their names.
- **`GeoContains` (stored ⊇ query) is a first-class method.** The common point-in-region lookup is still
  served by `GeoIntersects(point)`, but the general "which stored polygon contains this line/polygon" needs
  the real method, so it's included.
- **Wire format is GeoJSON, not WKB.** New geometry types serialize as GeoJSON in the document body,
  mirroring `Internal/GeoPointJsonConverter.cs`. Aligns with the JSON store and with Cosmos/Mongo native geo
  (both GeoJSON-native). Shiny.Spatial's WKB layer is not brought over.
- **`GeoPoint` stays.** It remains the lightweight point struct and simplest mapping; the new `Geometry`
  hierarchy is additive. A `GeoPoint` maps to a `Point` geometry (zero-area envelope) for indexing, so the
  existing radius/bbox/nearest methods keep working unchanged.
- **v1 providers: SQLite + Cosmos + MongoDB.** All other native-geo providers and the fallback providers are
  deferred (see below).
- **Distance sort is in scope, full depth.** Every predicate takes an optional `Geometry? orderByDistanceFrom`
  and returns `SpatialResult<T>`; geometry k-nearest extends the existing `NearestNeighbors` to
  geometry-mapped types. See [Distance](#distance) for the provider matrix.
- **Measurement + validity are in scope.** Model-level scalar accessors feed the existing `MapComputedProperty`
  (materialized+indexed) so measurement queries push down; a lightweight validity guard protects the index.
  See [Geometry model completeness](#geometry-model-completeness--measurement--validity).
- **Polygon ring winding is normalized on serialize.** The GeoJSON converter rewrites exterior rings to
  counter-clockwise and holes to clockwise (right-hand rule) so the native `2dsphere` (Mongo) and Cosmos
  spatial indexes accept every polygon the SQLite C# path accepts. The C# relate engine is winding-agnostic,
  so this is purely to keep the three v1 providers behaviourally identical.

## Public API surface (`Shiny.DocumentDb`)

```csharp
// New geometry model — src/Shiny.DocumentDb/Geometry/ , GeoJSON-serialized, immutable/sealed
public abstract class Geometry
{
    public abstract GeoBoundingBox GetEnvelope();
    // in-memory scalar accessors (measurement + validity)
    public double Area { get; }
    public double Length { get; }          // a.k.a. Perimeter for closed rings
    public GeoPoint Centroid { get; }
    public int NumPoints { get; }
    public int NumGeometries { get; }
    public bool IsValid { get; }
    public bool IsSimple { get; }
    public Geometry MakeValid();           // lightweight: ring closure, winding, degenerate removal
}
public sealed class GeoLineString        : Geometry { /* ≥2 coords */ }
public sealed class GeoPolygon           : Geometry { /* exterior ring + optional holes; ring ≥4 */ }
public sealed class GeoMultiPoint        : Geometry { }
public sealed class GeoMultiLineString   : Geometry { }
public sealed class GeoMultiPolygon      : Geometry { }
public sealed class GeoGeometryCollection: Geometry { }
// GeoPoint (existing struct) gains GeoBoundingBox GetEnvelope() and implicit → Point geometry

// New IDocumentStore methods — default-throw NotSupportedException, provider opts in
// (identical pattern to the existing WithinRadius / vector / full-text methods).
// Every predicate has the same shape: query geometry, optional distance-order reference, optional filter.
Task<IReadOnlyList<SpatialResult<T>>> GeoIntersects<T> (Geometry geometry,  Geometry? orderByDistanceFrom = null, Expression<Func<T,bool>>? filter = null, CancellationToken ct = default) where T : class;
Task<IReadOnlyList<SpatialResult<T>>> GeoContainedBy<T>(Geometry container, Geometry? orderByDistanceFrom = null, Expression<Func<T,bool>>? filter = null, CancellationToken ct = default) where T : class;
Task<IReadOnlyList<SpatialResult<T>>> GeoContains<T>   (Geometry geometry,  Geometry? orderByDistanceFrom = null, Expression<Func<T,bool>>? filter = null, CancellationToken ct = default) where T : class;
Task<IReadOnlyList<SpatialResult<T>>> GeoDisjoint<T>   (Geometry geometry,  Geometry? orderByDistanceFrom = null, Expression<Func<T,bool>>? filter = null, CancellationToken ct = default) where T : class;
Task<IReadOnlyList<SpatialResult<T>>> GeoTouches<T>    (Geometry geometry,  Geometry? orderByDistanceFrom = null, Expression<Func<T,bool>>? filter = null, CancellationToken ct = default) where T : class;
Task<IReadOnlyList<SpatialResult<T>>> GeoCrosses<T>    (Geometry geometry,  Geometry? orderByDistanceFrom = null, Expression<Func<T,bool>>? filter = null, CancellationToken ct = default) where T : class;
Task<IReadOnlyList<SpatialResult<T>>> GeoOverlaps<T>   (Geometry geometry,  Geometry? orderByDistanceFrom = null, Expression<Func<T,bool>>? filter = null, CancellationToken ct = default) where T : class;
Task<IReadOnlyList<SpatialResult<T>>> GeoEquals<T>     (Geometry geometry,  Geometry? orderByDistanceFrom = null, Expression<Func<T,bool>>? filter = null, CancellationToken ct = default) where T : class;
Task<IReadOnlyList<SpatialResult<T>>> GeoCovers<T>     (Geometry geometry,  Geometry? orderByDistanceFrom = null, Expression<Func<T,bool>>? filter = null, CancellationToken ct = default) where T : class;
Task<IReadOnlyList<SpatialResult<T>>> GeoCoveredBy<T>  (Geometry geometry,  Geometry? orderByDistanceFrom = null, Expression<Func<T,bool>>? filter = null, CancellationToken ct = default) where T : class;
// distance-band: "within N meters of this geometry"
Task<IReadOnlyList<SpatialResult<T>>> GeoWithinDistance<T>(Geometry geometry, double meters, Geometry? orderByDistanceFrom = null, Expression<Func<T,bool>>? filter = null, CancellationToken ct = default) where T : class;
// NearestNeighbors (existing signature, unchanged) additionally supports Geometry-mapped types — see Distance.

// New registration overloads — alongside the existing GeoPoint ones on DocumentStoreOptions
DocumentStoreOptions MapSpatialProperty<T>(Expression<Func<T, Geometry?>> property) where T : class;                       // reflection
DocumentStoreOptions MapSpatialProperty<T>(string propertyName, Func<T, Geometry?> accessor) where T : class;             // AOT-safe
```

Every predicate returns `SpatialResult<T>`; `DistanceMeters` is populated (and results ordered ascending)
when `orderByDistanceFrom` is supplied, and `null`/unordered otherwise.

`SpatialMapping` (`Internal/SpatialMapping.cs`) gains a `Func<object, Geometry?> GetGeometry` alongside the
existing `GetGeoPoint`; a `GeoPoint` mapping populates both (point → `Point` geometry) so nothing regresses.

## Distance

Distance ships at full depth. Three shapes, one primitive (`DistanceCalculator.Haversine` +
`DistanceToSegment`, ported from Shiny.Spatial; point-to-geometry = `0` if the point is inside a polygon, else
min great-circle distance to its edges):

1. **Order predicate results by a reference geometry** — the `orderByDistanceFrom` param on every predicate.
   The survivor set is already materialized in the pass-2 refine, so SQLite sorts it in C#; Cosmos/Mongo push
   an `ORDER BY ST_DISTANCE` / `$geoNear` when the reference is a **point**.
2. **Geometry-to-geometry distance** — when `orderByDistanceFrom` is a non-point geometry. SQLite (C#) and
   Cosmos (`ST_DISTANCE` accepts any GeoJSON) are native; **Mongo has no geometry-to-geometry distance
   operator** (`$near`/`$geoNear` require a point query), so the Mongo store falls back to computing distance
   in C# over the candidate set for non-point references. Documented tier difference, not a blocker.
3. **Geometry k-nearest** — `NearestNeighbors(GeoPoint center, int count)` keeps its signature and now works
   for `Geometry`-mapped types: point-to-geometry distance + the **expanding-envelope loop** the point
   `NearestNeighbors` already uses (the true nearest geometry can sit outside a fixed bbox, so the candidate
   envelope grows until `count` survivors stabilize). Cosmos/Mongo order natively against the point center.

`GeoWithinDistance(geometry, meters)` reuses the same primitive: expand the query envelope by `meters`
(`EnvelopeExpander`) → bbox candidates → keep survivors whose distance ≤ `meters`. Native as
`ST_DISTANCE(...) <= meters` on Cosmos and `$geoNear`/`$near` (point query) on Mongo.

**Provider matrix:** point reference → native on all three (SQLite C#, Cosmos `ST_DISTANCE`, Mongo
`$geoNear`); geometry reference → SQLite + Cosmos native, Mongo C# fallback.

**Fidelity caveat (document, don't solve):** SQLite/Mongo-fallback distances are a Haversine/planar
approximation; Cosmos/Mongo-native use geodesic `ST_DISTANCE`. Ordering can differ on near-ties across
providers — same tiered-fidelity story as the predicates themselves.

## Geometry model completeness — measurement + validity

Both pass the scope test (*index-accelerated or index-enabling*), so they're part of this release.

- **Scalar accessors on `Geometry`** — `Area`, `Length`/`Perimeter`, `Centroid`, `NumPoints`, `NumGeometries`,
  computed in-memory on the model (planar/geodesic to match the distance primitive). Trivial code.
- **The index-enabling payoff:** these compose with the existing **`MapComputedProperty`** (materialized +
  indexed, v9.2). Mapping `Area` (or `Length`) as a materialized computed property makes `area > X` /
  `ORDER BY area` an **indexed** query — server-side measurement filtering with no bespoke API. This is why
  measurement belongs here: it's the geometry model feeding machinery we already ship, not a new surface.
- **Validity** — `IsValid` / `IsSimple` on the model (ring closure, min-vertex count, coordinate finiteness,
  self-intersection). Guards the write path so a garbage polygon can't corrupt the R\*Tree envelope or get
  rejected by Mongo `2dsphere` / Cosmos at insert. Cheap and high-robustness.
- **Lightweight `MakeValid`** — ring-closure, winding correction, degenerate/duplicate-vertex removal, riding
  on the winding-normalization already done at serialize. **Not** full OGC noding/polygonization repair (see
  [Out of scope](#out-of-scope)).

## Phasing

Each behavior-changing phase carries the **four-artifact sync** (see [below](#four-artifact-sync-every-behavior-phase)).

### Phase 1 — Geometry model + GeoJSON + accessors (core only)
- Add the `Geometry` hierarchy under `src/Shiny.DocumentDb/Geometry/` with GeoJSON `JsonConverter`s modeled on
  `Internal/GeoPointJsonConverter.cs`.
- `GetEnvelope()` on every type and on `GeoPoint`.
- **Scalar accessors**: `Area`, `Length`/`Perimeter`, `Centroid`, `NumPoints`, `NumGeometries` (in-memory),
  plus `IsValid` / `IsSimple` and lightweight `MakeValid` (ring closure, winding correction,
  degenerate/duplicate removal — shares the serialize-time winding normalization).
- **Tests** (`tests/Shiny.DocumentDb.Tests`): GeoJSON roundtrip for all types incl. polygon-with-holes;
  envelope correctness; ring validation (LineString ≥2, polygon ring ≥4); accessor correctness (area/length/
  centroid on known shapes) and `IsValid`/`MakeValid` on malformed input.

### Phase 2 — DE-9IM relate engine (core, pure functions)
- Copy `PointInPolygon` (ray-casting, hole-aware), `SegmentIntersection` (cross-product), and
  `DistanceCalculator` into `src/Shiny.DocumentDb/Internal/`.
- Build `SpatialRelate(a, b)` → 9-intersection matrix (type-pair dispatch incl. Multi\*/collection recursion),
  and express **every** predicate — `Intersects`, `ContainedBy`/`Within`, `Contains`, `Disjoint`, `Touches`,
  `Crosses`, `Overlaps`, `Equals`, `Covers`, `CoveredBy` — as a matrix pattern.
- Extend `GeoMath` with geometry-envelope and envelope-expand-by-distance helpers (for `GeoWithinDistance`).
- **Tests**: the DE-9IM truth table per geometry-type pair; every predicate against hand-built pairs
  (point-in-polygon incl. hole, segment crossing/parallel, touching boundaries, overlapping polygons).

### Phase 3 — Envelope-driven indexing + write path (core + SQLite)
- `MapSpatialProperty(Geometry)` overloads; `SpatialMapping.GetGeometry`.
- Change the write-sync path (`DocumentStore.cs` `SpatialUpsertAsync` ~line 965, and every Insert/Update/
  Upsert/Remove/Clear caller) and `SqliteDatabaseProvider.BuildSpatialUpsertSql` to bind a real envelope —
  params `@spatialMinLat/@spatialMaxLat/@spatialMinLng/@spatialMaxLng` instead of `@spatialLat/@spatialLng`.
  **This param rename is the only mildly breaking change and is fully internal** (provider hook + core write
  path). No public-contract break.
- `WithinRadius`/`WithinBoundingBox`/`NearestNeighbors` keep working (a point is a zero-area envelope).
- **Validity guard on write:** run lightweight `IsValid`/`MakeValid` before populating the envelope so a
  malformed geometry can't corrupt the R\*Tree row (or later be rejected by Mongo/Cosmos native indexes).
- **Measurement → indexed queries:** verify a `Geometry` scalar accessor (e.g. `Area`) can back a
  `MapComputedProperty` materialized+indexed column, so `area > X` / `ORDER BY area` push down. No new query
  API — the existing computed-property path consuming the new accessors.
- **Tests**: existing spatial suite still green; geometry documents round-trip and index with correct
  envelopes; a materialized `Area` computed property filters/sorts via the normal query path.

### Phase 4 — Predicate family + distance (SQLite-complete)
- Implement all predicate methods + `GeoWithinDistance` in `DocumentStore.cs`: compute query-geometry
  envelope → run existing `BuildSpatialBoundingBoxQuerySql` for candidates → `SpatialRelate` refine → return.
  Thread the optional `filter` through `JsonExpressionVisitor.Translate(...)` as `additionalWhere`, exactly as
  `WithinRadius` does today (`DocumentStore.cs` ~line 2230).
- **`GeoDisjoint` special-case:** the bbox pre-filter is anti-selective (most of the answer is rows the bbox
  filter would reject), so it can't use bbox narrowing — implement as full candidate scan + `!Intersects`
  refine. **Document that `GeoDisjoint` is O(n) on the two-pass providers.**
- **`GeoWithinDistance`:** expand the query envelope by `meters` before the bbox filter, then keep survivors
  with distance ≤ `meters`.
- **Distance sort (shapes 1 & 2):** when `orderByDistanceFrom` is supplied, compute point-to-geometry distance
  on each survivor (`DistanceCalculator`) and order ascending, populating `SpatialResult.DistanceMeters`.
- **Geometry k-nearest (shape 3):** extend `NearestNeighbors` to `Geometry`-mapped types using the existing
  expanding-envelope loop with point-to-geometry distance.
- `SupportsSpatial` is already `true` on SQLite → the whole family lands there immediately.
- Add passthroughs in `src/Shiny.DocumentDb.Diagnostics/InstrumentedDocumentStore.cs` with an OTel span per
  predicate (mirror the existing `within_radius` span); tag whether a distance sort ran.
- **Tests**: port/extend `DatabaseTests`/`QueryTests` — every predicate against a seeded geometry set,
  bare-point intersecting a polygon, `GeoWithinDistance`, `filter` push-down combined with a geometry
  predicate, distance-ordered results (point ref + geometry ref), geometry `NearestNeighbors` ordering.

### Phase 5 — Cosmos parity (native + documented refine)
- Override the predicates in `src/Shiny.DocumentDb.CosmosDb/CosmosDbDocumentStore.cs` over the existing GeoJSON
  `SpatialPath` index (registered at `IndexingPolicy.SpatialIndexes`, ~line 190). Point geometry already works.
- **Native:** `GeoIntersects` → `ST_INTERSECTS`; `GeoContainedBy` → `ST_WITHIN`; `GeoWithinDistance` →
  `ST_DISTANCE(...) <= meters`; distance sort → `ORDER BY ST_DISTANCE`.
- **Refine fallback:** Cosmos has **no** `ST_TOUCHES`/`ST_CROSSES`/`ST_OVERLAPS`/`ST_EQUALS`/`ST_CONTAINS`, so
  `GeoTouches`/`GeoCrosses`/`GeoOverlaps`/`GeoEquals`/`GeoContains`/`GeoCovers`/`GeoCoveredBy` fetch the
  `ST_INTERSECTS` candidate set and refine with `SpatialRelate` in C#. Document the native-vs-refine split.
- **Tests**: Cosmos spatial tests (emulator-gated as existing Cosmos tests are), incl. the refine predicates.

### Phase 6 — MongoDB parity (native + documented refine)
- Add a spatial override using a `2dsphere` index + `$geoIntersects` / `$geoWithin` (GeoJSON-native). Mongo
  already overrides vector/full-text; spatial follows the same shape but is currently `SupportsSpatial =>
  false` — wire the index creation at table-init and implement the three point methods + the full predicate
  family. Feed the `2dsphere` index winding-normalized GeoJSON (see Decisions).
- **Native:** `GeoIntersects` → `$geoIntersects`; `GeoContainedBy` → `$geoWithin`; point-reference distance /
  `GeoWithinDistance` (point) → `$geoNear`/`$near`.
- **Refine fallback:** the finer predicates and non-point-reference distance refine with `SpatialRelate` /
  `DistanceCalculator` in C# over the `$geoIntersects`/`$geoWithin` candidate set.
- **Tests**: Mongo spatial tests (container-gated as existing Mongo tests are), incl. the refine predicates
  and the geometry-reference C# distance fallback.

## Deferred (not in this release)

Document these explicitly; do not silently imply full coverage.

- **Native geo, other providers** — PostGIS, SQL Server `geography`, MySQL `ST_*`, Oracle Spatial, DuckDB
  `spatial`. Each just overrides the store-level spatial methods later (full DE-9IM via `ST_Relate`/`ST_Touches`
  etc.); ship independently, note the provider tier in each release note. The tiered-performance story matches
  how vector/full-text already degrade per provider.
- **Fallback providers** — LiteDB, IndexedDB (WASM), AzureTable, DynamoDB have no bbox index. Leave them
  `SupportsSpatial => false`. A later opt-in full-scan + in-memory refine path is possible (correct but slow);
  not built here.

### Out of scope

- **Constructive geometry algebra** — Buffer / Union / Intersection / Difference / ConvexHull / Simplify.
  Out because the store can't accelerate them (no index, no round-trip win over an in-process call), not
  because they're a "different part." A dependency-free robust implementation is effectively reimplementing
  NetTopologySuite. Callers do these in their own process.
- **Full OGC `MakeValid` (repair)** — only the lightweight repair (ring/winding/degenerate) is in scope;
  full noding/polygonization repair is a heavy algorithm with low leverage.

## Four-artifact sync (every behavior phase)

Per `CLAUDE.md`, a change isn't "done" until all four are in sync:

1. **Code + tests** — `src/`, `tests/`. Run `dotnet test tests/Shiny.DocumentDb.Tests/Shiny.DocumentDb.Tests.csproj`
   before considering the phase complete. Note the provider compatibility tier in each release note
   (SQLite two-pass vs Cosmos/Mongo native-or-refine vs deferred).
2. **Docs site** (`~/Desktop/dev/documentation/src/content/docs/documentdb/`) — extend the spatial page
   (`spatial.mdx`, or the geo section of `querying.mdx`); add a `<RN type="feature">` note against the raw
   `version.json` version (strip any prerelease suffix) under a `## <version> TBD` heading (create it at the
   top of `release-notes.mdx` if absent).
3. **Skill** (`skills/shiny-documentdb/SKILL.md`) — new geometry types, the `Geo*` predicate family,
   `GeoWithinDistance`, measurement/validity accessors, `MapSpatialProperty(Geometry)` guidance; add the new
   public types/methods to the `triggers:` keyword list.
4. **readme.md** (repo root) — bump the spatial feature bullet from point-only to full-geometry.

## Risks / open questions

- **Envelope param rename** (Phase 3) touches the SQLite provider hook and the core write path together —
  land them in one commit so the write path never binds params the SQL doesn't expect.
- **`filter` + geometry predicate interaction** — confirm `additionalWhere` from `JsonExpressionVisitor`
  composes cleanly with the geometry pass-2 refine (it already does for `WithinRadius`; same code path).
- **DE-9IM boundary semantics ⇄ native operators** — the C# relate engine's boundary rules must match what
  Cosmos `ST_WITHIN` / Mongo `$geoWithin` do, or the same query returns different rows per provider. Add
  cross-provider conformance tests on a shared geometry-pair fixture.
- **`GeoDisjoint` cost** — O(n) on the two-pass providers (bbox pre-filter is anti-selective); documented, and
  worth considering an explicit opt-in guard so it can't be called accidentally on a large corpus.
- **Antimeridian / pole edge cases** — Haversine + planar relate inherit Shiny.Spatial's limitations;
  document them rather than solving them here.
- **Winding order** — decided (normalize on serialize, see Decisions); the only residual risk is a polygon the
  caller hands us already-inverted such that normalization flips its intended interior. Document that
  exterior-ring orientation is normalized, not inferred from area.
- **Geometry k-nearest termination** — the expanding-envelope loop must have the same growth cap / max-radius
  guard the point `NearestNeighbors` already uses, so a `count` larger than the corpus can't loop unbounded.

## Reference: source material in Shiny.Spatial

`~/Desktop/dev/geospatialdb/src/Shiny.Spatial/`:
- `Geometry/` — Coordinate, Envelope, Point, LineString, Polygon, MultiPoint, MultiLineString, MultiPolygon,
  GeometryCollection (model to mirror; GeoJSON instead of their WKB).
- `Algorithms/` — `DistanceCalculator`, `PointInPolygon`, `SegmentIntersection`, `SpatialPredicates`
  (Intersects/Contains — use as primitives under the new DE-9IM relate engine), `EnvelopeExpander`
  (**copy these**).
- Tests: `GeometryTests`, `AlgorithmTests`, `DatabaseTests`, `QueryTests` (port relevant cases).
