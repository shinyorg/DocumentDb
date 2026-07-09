# Plan: Native spatial pushdown tier (opt-in per provider)

**Status:** Designed, not started.
**Target version:** `11.x` (additive minor off the `11.0.0` line). No breaking changes — purely opt-in.
**Depends on:** the shipped spatial feature (`Geometry` model, GeoJSON, C# relate engine, the `Geo*` predicate
family, the envelope-sidecar relational tier). Branch off current.

> Self-contained build spec. Read `CLAUDE.md` (repo root) for the four-artifact rule before considering any
> commit "done".

---

## Goal

Add a second, **opt-in** performance tier for the relational providers that pushes each geometry predicate all
the way into the database using the engine's **native spatial types, indexes, and functions** — so the exact
test runs in SQL (no C# refine, no candidate over-fetch), distance is geodesic, and nearest-neighbour is
index-assisted. The default stays the **dependency-free envelope-sidecar tier** already shipped; power users
flip one flag per provider to get native behaviour.

**The public API does not change.** `GeoIntersects`, `GeoContainedBy`, … `GeoWithinDistance`, distance
ordering, and the `Geometry` model are identical across tiers. Only *where the work runs* differs, so user
code is portable between tiers (modulo documented boundary-case drift).

## Opt-in flags (default off)

Per-provider `init`-only bool, mirroring the existing `SqliteDatabaseProvider.EnableVectorExtension`:

```csharp
new PostgreSqlDatabaseProvider(cs) { UsePostGis = true }
new SqlServerDatabaseProvider(cs)  { UseNativeSpatial = true }
new OracleDatabaseProvider(cs)     { UseNativeSpatial = true }
new MySqlDatabaseProvider(cs)      { UseNativeSpatial = true }
new DuckDbDatabaseProvider(cs)     { UseSpatialExtension = true }
```

Off → the envelope-sidecar + C# refine tier (today, unchanged). On → native tier below.

## Architecture — reuse the vector precedent, add one capability

The store layer is already hook-driven and provider-agnostic. The **vector feature is the exact template**:
it stores an embedding in a native-typed sidecar (`{table}_vec_{type}` with a `vector` column + HNSW index),
created via `CREATE EXTENSION IF NOT EXISTS vector`, and the store calls provider hooks
(`BuildVectorUpsertSql` / `BuildVectorDeleteSql` / `BuildVectorClearSql` /
`BuildVectorSearchSql → (Sql, Parameters)`). The native spatial tier mirrors this shape one-for-one.

### One new capability on `IDatabaseProvider`

```csharp
// Default false → store keeps using BuildSpatialBoundingBoxQuerySql + C# refine (today's path).
bool SupportsNativeSpatialPredicates => false;

// Returns index-accelerated SQL for ONE predicate, or null if the engine has no native operator for it
// (store then falls back to the native-candidate + C# refine path — see below).
(string Sql, IReadOnlyDictionary<string, object?> Parameters)? BuildSpatialPredicateSql(
    string tableName,
    SpatialPredicate predicate,     // enum: Intersects/Disjoint/Contains/Within/Covers/CoveredBy/
                                    //       Touches/Crosses/Overlaps/Equals/WithinDistance
    string queryGeoJson,            // the query geometry, GeoJSON
    double? meters,                 // for WithinDistance
    string? orderByGeoJson,         // reference geometry for distance ordering (nullable)
    string? additionalWhere);       // pushed-down filter, as today
```

`SupportsNativeSpatialPredicates` is wired to the flag (`=> this.UsePostGis`, etc.). The DTO returns the
computed `DistanceMeters` as a projected column when `orderByGeoJson` is supplied (native `ST_Distance`), so
the store no longer computes distance in C# on the native path.

### One branch in `DocumentStore.Geometry.cs`

`GeometryQuery<T>` gains a single fork:

```
if (provider.SupportsNativeSpatialPredicates
    && provider.BuildSpatialPredicateSql(...) is { } native)
{
    // run native.Sql with native.Parameters; deserialize d.Data; DistanceMeters from the projected column.
    // NO C# refine.
}
else
{
    // today: bbox candidates (BuildSpatialBoundingBoxQuerySql) + SpatialPredicates refine.
}
```

Everything else — the `Geo*` methods, the `Geometry` model, the write sync entry points — is untouched.

### Only ONE sidecar per store (the key simplification)

When native is on, the sidecar becomes a **native-geometry** table, not the bbox table:

```
{table}_spatial ( docId, typeName, geom <native spatial type> )
+ a native spatial index on geom
```

The native spatial index serves **both** roles, so there's never a double sidecar and never a table scan:
- Predicates with a native operator → full pushdown via `BuildSpatialPredicateSql`.
- Predicates the engine lacks (see matrix) → `BuildSpatialPredicateSql` returns SQL that selects the native
  `ST_Intersects` **candidate set** (index-accelerated) and the store refines those in C# with
  `SpatialPredicates` — the identical pattern already used for Cosmos/Mongo. Coverage stays complete.

The write path serializes the mapped `Geometry` to the engine's ingest format (GeoJSON/WKT) on
Insert/Update/Upsert and removes on Remove/Clear — same call sites as today (`SpatialUpsertAsync` etc.),
just a different `BuildSpatialUpsertSql` body selected by the flag.

## Per-provider native mapping

| Engine | Native type + index | Ingest | Predicate form | Distance | Missing → candidate-refine |
|---|---|---|---|---|---|
| **PostGIS** | `geography(Geometry,4326)` + **GiST** | `ST_GeomFromGeoJSON` | `ST_Intersects/Contains/Within/Covers/CoveredBy/Touches/Crosses/Overlaps/Equals/Disjoint`, `ST_DWithin(g,m)` | `ST_Distance` geodesic; `ORDER BY geom <-> p` KNN | none (full DE-9IM) |
| **SQL Server** | `geography` + spatial index | `geography::STGeomFromText(WKT,4326)` | method syntax: `g.STIntersects(@q)=1`, `.STContains/.STWithin/.STTouches/.STCrosses/.STOverlaps/.STEquals/.STDisjoint`; `.STDistance(@q)<=@m` | `.STDistance` geodesic | `STCovers`/`STCoveredBy` → refine; **left-hand ring rule** on ingest |
| **MySQL** | `GEOMETRY` SRID 4326 + `SPATIAL` index | `ST_GeomFromGeoJSON` | `ST_Intersects/Contains/Within/Touches/Crosses/Overlaps/Equals/Disjoint` | `ST_Distance_Sphere` (meters) | `Covers`/`CoveredBy`/`WithinDistance` → refine; **lat-long axis order** on SRID 4326 |
| **Oracle Spatial** | `SDO_GEOMETRY` + `MDSYS.SPATIAL_INDEX` | `SDO_UTIL.FROM_GEOJSON` | `SDO_RELATE(g,q,'mask=…')` (CONTAINS/INSIDE/COVERS/COVEREDBY/TOUCH/OVERLAPBDYINTERSECT/EQUAL/ANYINTERACT/DISJOINT); `SDO_WITHIN_DISTANCE(g,q,'distance=m unit=meter')` | `SDO_GEOM.SDO_DISTANCE` | none (SDO masks cover all); metadata registration required |
| **DuckDB** | `GEOMETRY` + R-Tree (spatial ext) | `ST_GeomFromGeoJSON` | `ST_Intersects/Contains/Within/Touches/Crosses/Overlaps/Equals`, `ST_DWithin` | `ST_Distance_Sphere` | `Covers`/`CoveredBy`/`Disjoint` → refine; **planar** unless sphere distance used |

## Safe activation & lifecycle

- **Extension bootstrap at init** (mirrors the pgvector path `CREATE EXTENSION IF NOT EXISTS vector`):
  PostGIS → `CREATE EXTENSION IF NOT EXISTS postgis`; DuckDB → `INSTALL spatial; LOAD spatial;`. SQL Server /
  MySQL native spatial is built-in (no extension). Oracle Spatial is part of the DB but the index needs
  `USER_SDO_GEOM_METADATA` registered before `CREATE INDEX … INDEXTYPE IS MDSYS.SPATIAL_INDEX`.
- **Fail loud, not silent.** If the flag is on but the extension/edition is unavailable (or PostGIS create
  fails), throw a clear `NotSupportedException` at store init — never degrade mid-query.
- **The flag is a startup decision.** It selects the sidecar shape, so flipping it on an existing store
  requires rebuilding the spatial sidecar. Because sidecars are reconstructable from the document body, expose
  a `ReindexSpatial<T>()` maintenance step (drop + repopulate from the stored documents). Document that
  toggling the flag needs a reindex; do not attempt silent migration.

## Semantic-drift guard (definition of done)

Native DE-9IM boundary rules can differ subtly from the C# relate engine (touching boundaries, collinear
overlaps, degenerate rings). The tier is not "done" until a **cross-tier conformance suite** asserts that, over
a shared geometry-pair fixture, `native-on` and `native-off` return the **same document sets** for every
predicate — with any deliberate, documented exception called out. Where they legitimately differ (e.g. a
provider's boundary convention), prefer the native result on the native tier and document it.

## Phasing

Each phase carries the **four-artifact sync** (code+tests, docs release note + spatial page tier notes,
`SKILL.md`, `readme.md`). Per the "do it all" directive these ship together, but the natural build order is:

1. **Contract + store fork + conformance harness.** Add `SupportsNativeSpatialPredicates` +
   `BuildSpatialPredicateSql` + the `SpatialPredicate` enum; the single `GeometryQuery` branch; the
   native-geometry sidecar write path selected by flag; `ReindexSpatial<T>()`. Build the cross-tier conformance
   test base (runs the whole predicate matrix twice — flag off vs on — asserting identical results).
2. **PostGIS** (cleanest — full DE-9IM, GiST, `ST_DWithin`, KNN operator). Validates the whole design end to end.
3. **DuckDB** (`INSTALL/LOAD spatial`, R-Tree, `ST_DWithin`).
4. **MySQL** (`SPATIAL` index; axis-order handling; `Covers`/`DWithin` via candidate-refine).
5. **SQL Server** (method syntax; left-hand ring orientation on ingest; `Covers`/`CoveredBy` via refine).
6. **Oracle Spatial** (`SDO_GEOMETRY`, metadata registration, `SDO_RELATE` masks, `SDO_WITHIN_DISTANCE`).

Container-gated tests per provider run **both** tiers (flag off = existing envelope suite; flag on = native).

## Not in scope

- **Changing the default.** The envelope-sidecar tier stays the default everywhere; native is always opt-in.
- **SQLite native spatial.** SQLite's only bundled spatial index is R\*Tree (already the default tier); the
  native option would be SpatiaLite — explicitly excluded (native dependency, breaks the mobile/zero-dep story).
- **Cosmos/Mongo.** They are already native by default; no tier flag applies.
- **Non-SQL fallback stores** (LiteDB, IndexedDB, Azure Table, DynamoDB) — still `SupportsSpatial => false`.

## Risks / open questions

- **Extension availability** — PostGIS / DuckDB `spatial` must be installable by the connection's role; Oracle
  Spatial is edition-dependent. Init-time probe + clear failure.
- **Ingest validity & winding** — SQL Server `geography` enforces the **left-hand rule** (opposite of the
  GeoJSON right-hand rule we normalize to) and rejects invalid rings; the ingest path must re-orient and may
  need `MakeValid`/`ReorientObject`. MySQL/Oracle SRID **axis order** (lat-long on 4326 / 8307) is a classic
  footgun — pin it in the ingest SQL and cover it with a north-of-equator + east/west fixture.
- **Distance-unit/semantics parity** — geodesic (PostGIS/SQL Server/Oracle) vs sphere (MySQL/DuckDB) vs planar
  (DuckDB default). Document per provider; the conformance suite asserts ordering, not exact meters.
- **Reindex ergonomics** — make `ReindexSpatial<T>()` resumable/idempotent for large tables; document the
  flag-toggle requirement prominently.
