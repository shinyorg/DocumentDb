# Plan: Expand spatial from point-only to full geometry

**Status:** Designed, not started.
**Target version:** `10.x` (new feature → minor bump off the `10.0.x` line in `version.json`). Additive —
no breaking changes to the public contract. Phased; ships in one release covering **SQLite + Cosmos +
MongoDB**, with the remaining native providers deferred (see [Phasing](#phasing) and
[Deferred](#deferred-not-in-this-release)).

> Self-contained build spec. Read `CLAUDE.md` (repo root) for the four-artifact rule (code+tests, docs
> site, skill, readme) before considering any commit "done". Branch off `v10`.

---

## Goal

Upgrade the spatial feature from **point-only** (`GeoPoint` + `WithinRadius` / `WithinBoundingBox` /
`NearestNeighbors`) to **full OGC geometry** — `LineString`, `Polygon` (with holes), `MultiPoint`,
`MultiLineString`, `MultiPolygon`, `GeometryCollection` — with two new predicate queries:

```csharp
Task<IReadOnlyList<T>> Intersects<T>(Geometry geometry, Expression<Func<T,bool>>? filter = null, CancellationToken ct = default);
Task<IReadOnlyList<T>> ContainedBy<T>(Geometry container, Expression<Func<T,bool>>? filter = null, CancellationToken ct = default);
```

The design is lifted from the sibling **Shiny.Spatial** library (`~/Desktop/dev/geospatialdb`), which
already runs the same two-pass query pipeline. This plan **absorbs its geometry + predicate engine** into
DocumentDb's existing spatial feature. It does **not** port Shiny.Spatial's `SpatialTable` / `SpatialFeature`
API, its WKB serialization, its geofencing package, or its seeded databases — those stay in Shiny.Spatial as
its own (dependency-free, mobile-GPS) product.

## Why it's cheap (the architecture already exists)

DocumentDb's `WithinRadius` / `WithinBoundingBox` / `NearestNeighbors` (`src/Shiny.DocumentDb/DocumentStore.cs`
~lines 2209–2380) already implement the **exact two-pass pipeline** Shiny.Spatial is built on:

1. **Pass 1 — coarse bbox filter (SQL, O(log n)):** the SQLite R\*Tree sidecar
   (`{table}_spatial USING rtree(id, minLat, maxLat, minLng, maxLng)`) via
   `IDatabaseProvider.BuildSpatialBoundingBoxQuerySql(tableName, additionalWhere)`.
2. **Pass 2 — refinement (C#):** `Internal/GeoMath.HaversineDistance` on the survivors.

So the scaffolding a full-geometry engine needs — R\*Tree sidecar, the `BuildSpatial*Sql` hook family, the
`MapSpatialProperty` registration model, table-init + write-sync plumbing — **is already in the codebase**.

The R\*Tree columns are already an **envelope** (`minLat/maxLat/minLng/maxLng`); today they are fed a
degenerate point box (`SqliteDatabaseProvider.BuildSpatialUpsertSql`, ~line 266:
`VALUES (…, @spatialLat, @spatialLat, @spatialLng, @spatialLng)`). Feed that a real geometry envelope and the
existing bbox hook becomes the pass-1 filter for **every** geometry type — meaning **SQLite gets
intersect/contains with zero new provider SQL**, just envelope population + a C# predicate refine.

## Decisions (locked)

- **Algorithm code is copied, not shared.** Port Shiny.Spatial's `PointInPolygon`, `SegmentIntersection`,
  and `SpatialPredicates` into `src/Shiny.DocumentDb/Internal/` next to `GeoMath`. Small, stable surface;
  both repos are ours. No shared micro-package, no cross-repo release coupling. Accept minor drift.
- **Wire format is GeoJSON, not WKB.** New geometry types serialize as GeoJSON in the document body,
  mirroring `Internal/GeoPointJsonConverter.cs`. Aligns with the JSON store and with Cosmos/Mongo native geo
  (both GeoJSON-native). Shiny.Spatial's WKB layer is not brought over.
- **`GeoPoint` stays.** It remains the lightweight point struct and simplest mapping; the new `Geometry`
  hierarchy is additive. A `GeoPoint` maps to a `Point` geometry (zero-area envelope) for indexing, so the
  existing radius/bbox/nearest methods keep working unchanged.
- **v1 providers: SQLite + Cosmos + MongoDB.** All other native-geo providers and the fallback providers are
  deferred (see below).

## Public API surface (`Shiny.DocumentDb`)

```csharp
// New geometry model — src/Shiny.DocumentDb/Geometry/ , GeoJSON-serialized, immutable/sealed
public abstract class Geometry { public abstract GeoBoundingBox GetEnvelope(); }
public sealed class GeoLineString        : Geometry { /* ≥2 coords */ }
public sealed class GeoPolygon           : Geometry { /* exterior ring + optional holes; ring ≥4 */ }
public sealed class GeoMultiPoint        : Geometry { }
public sealed class GeoMultiLineString   : Geometry { }
public sealed class GeoMultiPolygon      : Geometry { }
public sealed class GeoGeometryCollection: Geometry { }
// GeoPoint (existing struct) gains GeoBoundingBox GetEnvelope() and implicit → Point geometry

// New IDocumentStore methods — default-throw NotSupportedException, provider opts in
// (identical pattern to the existing WithinRadius / vector / full-text methods)
Task<IReadOnlyList<T>> Intersects<T>(Geometry geometry, Expression<Func<T,bool>>? filter = null, CancellationToken ct = default) where T : class;
Task<IReadOnlyList<T>> ContainedBy<T>(Geometry container, Expression<Func<T,bool>>? filter = null, CancellationToken ct = default) where T : class;

// New registration overloads — alongside the existing GeoPoint ones on DocumentStoreOptions
DocumentStoreOptions MapSpatialProperty<T>(Expression<Func<T, Geometry?>> property) where T : class;                       // reflection
DocumentStoreOptions MapSpatialProperty<T>(string propertyName, Func<T, Geometry?> accessor) where T : class;             // AOT-safe
```

`SpatialMapping` (`Internal/SpatialMapping.cs`) gains a `Func<object, Geometry?> GetGeometry` alongside the
existing `GetGeoPoint`; a `GeoPoint` mapping populates both (point → `Point` geometry) so nothing regresses.

## Phasing

Each behavior-changing phase carries the **four-artifact sync** (see [below](#four-artifact-sync-every-behavior-phase)).

### Phase 1 — Geometry model + GeoJSON (core only)
- Add the `Geometry` hierarchy under `src/Shiny.DocumentDb/Geometry/` with GeoJSON `JsonConverter`s modeled on
  `Internal/GeoPointJsonConverter.cs`.
- `GetEnvelope()` on every type and on `GeoPoint`.
- **Tests** (`tests/Shiny.DocumentDb.Tests`): GeoJSON roundtrip for all types incl. polygon-with-holes;
  envelope correctness; ring validation (LineString ≥2, polygon ring ≥4).

### Phase 2 — Predicate algorithms (core, pure functions)
- Copy `PointInPolygon` (ray-casting, hole-aware), `SegmentIntersection` (cross-product), and
  `SpatialPredicates.Intersects/Contains` (type-pair dispatch incl. Multi\*/collection recursion) into
  `src/Shiny.DocumentDb/Internal/`.
- Extend `GeoMath` with geometry-envelope and envelope-expand-by-distance helpers.
- **Tests**: port Shiny.Spatial's `AlgorithmTests` (point-in-polygon incl. hole, segment crossing/parallel,
  predicate type-pair dispatch).

### Phase 3 — Envelope-driven indexing + write path (core + SQLite)
- `MapSpatialProperty(Geometry)` overloads; `SpatialMapping.GetGeometry`.
- Change the write-sync path (`DocumentStore.cs` `SpatialUpsertAsync` ~line 965, and every Insert/Update/
  Upsert/Remove/Clear caller) and `SqliteDatabaseProvider.BuildSpatialUpsertSql` to bind a real envelope —
  params `@spatialMinLat/@spatialMaxLat/@spatialMinLng/@spatialMaxLng` instead of `@spatialLat/@spatialLng`.
  **This param rename is the only mildly breaking change and is fully internal** (provider hook + core write
  path). No public-contract break.
- `WithinRadius`/`WithinBoundingBox`/`NearestNeighbors` keep working (a point is a zero-area envelope).
- **Tests**: existing spatial suite still green; geometry documents round-trip and index with correct
  envelopes.

### Phase 4 — `Intersects` / `ContainedBy` (SQLite-complete)
- Implement in `DocumentStore.cs`: compute query-geometry envelope → run existing
  `BuildSpatialBoundingBoxQuerySql` for candidates → C# `SpatialPredicates` refine → return. Thread the
  optional `filter` through `JsonExpressionVisitor.Translate(...)` as `additionalWhere`, exactly as
  `WithinRadius` does today (`DocumentStore.cs` ~line 2230).
- `SupportsSpatial` is already `true` on SQLite → full geometry lands there immediately.
- Add passthroughs in `src/Shiny.DocumentDb.Diagnostics/InstrumentedDocumentStore.cs` with OTel spans
  `intersects` / `contained_by` (mirror the existing `within_radius` span).
- **Tests**: port `DatabaseTests`/`QueryTests` — FindIntersecting(polygon), FindContainedBy, bare-point
  intersecting a polygon, `filter` push-down combined with a geometry predicate.

### Phase 5 — Cosmos parity (native)
- Override `Intersects`/`ContainedBy` in `src/Shiny.DocumentDb.CosmosDb/CosmosDbDocumentStore.cs` using
  `ST_INTERSECTS` / `ST_WITHIN` over the existing GeoJSON `SpatialPath` index (registered at
  `IndexingPolicy.SpatialIndexes`, ~line 190). Point geometry already works there.
- **Tests**: Cosmos spatial tests (emulator-gated as existing Cosmos tests are).

### Phase 6a — MongoDB parity (native)
- Add a spatial override to the MongoDB store using a `2dsphere` index + `$geoIntersects` / `$geoWithin`
  (GeoJSON-native). Mongo already overrides vector/full-text; spatial follows the same shape but is currently
  `SupportsSpatial => false` — wire the index creation at table-init and implement the three point methods +
  the two new predicate methods.
- **Tests**: Mongo spatial tests (container-gated as existing Mongo tests are).

## Deferred (not in this release)

Document these explicitly; do not silently imply full coverage.

- **Native geo, other providers** — PostGIS, SQL Server `geography`, MySQL `ST_*`, Oracle Spatial, DuckDB
  `spatial`. Each just overrides the store-level spatial methods later; ship independently, note the provider
  tier in each release note. The tiered-performance story matches how vector/full-text already degrade per
  provider.
- **Fallback providers** — LiteDB, IndexedDB (WASM), AzureTable, DynamoDB have no bbox index. Leave them
  `SupportsSpatial => false`. A later opt-in full-scan + in-memory refine path is possible (correct but slow);
  not built here.
- **Non-point `NearestNeighbors`** — Shiny.Spatial orders non-point geometries by *envelope centroid*, an
  approximation. Recommend keeping `NearestNeighbors` point-only unless a documented centroid approximation
  is explicitly wanted.
- **Buffer / union / difference / convex-hull / area / length** — not in Shiny.Spatial either; out of scope.

## Four-artifact sync (every behavior phase)

Per `CLAUDE.md`, a change isn't "done" until all four are in sync:

1. **Code + tests** — `src/`, `tests/`. Run `dotnet test tests/Shiny.DocumentDb.Tests/Shiny.DocumentDb.Tests.csproj`
   before considering the phase complete. Note the provider compatibility tier in each release note
   (SQLite two-pass vs Cosmos/Mongo native vs deferred).
2. **Docs site** (`~/Desktop/dev/documentation/src/content/docs/documentdb/`) — extend the spatial page
   (`spatial.mdx`, or the geo section of `querying.mdx`); add a `<RN type="feature">` note against the raw
   `version.json` version (strip any prerelease suffix) under a `## <version> TBD` heading (create it at the
   top of `release-notes.mdx` if absent).
3. **Skill** (`skills/shiny-documentdb/SKILL.md`) — new geometry types, `Intersects`/`ContainedBy`,
   `MapSpatialProperty(Geometry)` guidance; add the new public types/methods to the `triggers:` keyword list.
4. **readme.md** (repo root) — bump the spatial feature bullet from point-only to full-geometry.

## Risks / open questions

- **Envelope param rename** (Phase 3) touches the SQLite provider hook and the core write path together —
  land them in one commit so the write path never binds params the SQL doesn't expect.
- **`filter` + geometry predicate interaction** — confirm `additionalWhere` from `JsonExpressionVisitor`
  composes cleanly with the geometry pass-2 refine (it already does for `WithinRadius`; same code path).
- **Antimeridian / pole edge cases** — Haversine + planar ray-casting inherit Shiny.Spatial's limitations;
  document them rather than solving them here.

## Reference: source material in Shiny.Spatial

`~/Desktop/dev/geospatialdb/src/Shiny.Spatial/`:
- `Geometry/` — Coordinate, Envelope, Point, LineString, Polygon, MultiPoint, MultiLineString, MultiPolygon,
  GeometryCollection (model to mirror; GeoJSON instead of their WKB).
- `Algorithms/` — `DistanceCalculator`, `PointInPolygon`, `SegmentIntersection`, `SpatialPredicates`,
  `EnvelopeExpander` (**copy these**).
- Tests: `GeometryTests`, `AlgorithmTests`, `DatabaseTests`, `QueryTests` (port relevant cases).
