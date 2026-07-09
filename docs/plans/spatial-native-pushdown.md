# Plan: Native spatial + `DocumentFunctions` LINQ composition (native-by-default)

**Status:** ✅ Complete (Cosmos code-only per test-env constraints). Native pushdown now runs over a
**true 2-D spatial index** per provider — PostgreSQL GiST, MySQL SPATIAL, DuckDB R-Tree, SQL Server spatial
index (native geometry column in the sidecar, populated on write), SQLite R\*Tree, **Oracle `SDO_GEOMETRY` +
MDSYS spatial index** (`SDO_RELATE` operators, metadata-registered), CosmosDB native spatial index, MongoDB
2dsphere (ensured on the LINQ path). Init-time fail-loud provisioning is gated on a mapped geometry.
Oracle native is validated against the full `gvenzl/oracle-free` image (Spatial included) via
`ORACLE_SPATIAL_IMAGE`; the default slim CI image lacks Oracle Spatial, so those tests skip there.
`Distance`-in-`OrderBy` is native on SQLite/PostgreSQL/MySQL/DuckDB/SQL Server (SQL Server via a correlated
subquery to the sidecar geometry column).

**Original status:** Substantially complete. The `DocumentFunctions` LINQ surface (11 predicates in `Where` +
`Distance` in `OrderBy`) is implemented on every provider. **Validated against live containers:** SQLite
(R\*Tree + `docdb_st_*` UDF), MySQL, DuckDB, PostgreSQL/PostGIS (combined PostGIS+pgvector test image), SQL
Server (native planar `geometry` column, WKT ingest), MongoDB (intersect/within/point-distance subset).
**Implemented, not CI-validated** (test environment lacks the engine option): Oracle (`SDO_GEOM` — image lacks
Oracle Spatial) and CosmosDB (`ST_*` — vnext-preview emulator's PG backend lacks the spatial functions); both
work on the real engines. `PortableSpatial` forces the envelope tier. Where a predicate has no native operator
on a provider, the `Where` call throws a clear message and the dedicated `store.Geo*` methods (all predicates,
every provider) cover it. Remaining niceties: the strict fail-loud native-required init resolution (currently
native is on by default and fails at query time where an engine option is absent).
**Target version:** `11.0.0` (still `11.0.0-beta` in `version.json` — folds into the same unreleased release as
the geometry feature, so redefining the default spatial behaviour here is **not** a released-behaviour break).
**Depends on:** the geometry feature already in this beta (`Geometry` model, GeoJSON, C# relate/distance
engine, the `Geo*` predicate methods, the envelope-sidecar relational implementation). Branch off current.

> Self-contained build spec. Read `CLAUDE.md` (repo root) for the four-artifact rule before considering any
> commit "done".

---

## Goal & philosophy

Make **`DocumentFunctions` spatial in LINQ the headline surface** — users write pure, composable LINQ:

```csharp
var results = await store.Query<Zone>()
    .Where(z => DocumentFunctions.Intersects(z.Area, searchArea) && z.Active)
    .OrderBy(z => DocumentFunctions.Distance(z.Area, origin))
    .Skip(20).Take(20)
    .ToList();
```

For this to be reliable, a spatial predicate must be a **real function the query engine can evaluate** — so
spatial goes **native by default**. When a spatial property is mapped, the store **auto-enables** the engine's
native spatial support at init and **fails loud** if it can't. No silent fallback (a silent fallback is exactly
what would make `DocumentFunctions.Intersects` in a `Where` fail to translate — the thing we're avoiding).

The dedicated `store.GeoIntersects(...)` methods (with `orderByDistanceFrom` / k-nearest ergonomics) **remain**
as a second surface; on native providers both surfaces share the same native SQL.

## Resolution model — every spatial-capable provider *can be* the function

`DocumentFunctions` methods carry a **real C# implementation** (the relate/distance engine), and the
`ExpressionLowerer` maps them to the best mechanism per provider (the `Soundex` precedent, generalized):

| Provider | LINQ / predicate mechanism | Indexed? | Enable step |
|---|---|---|---|
| **MySQL / SQL Server / Oracle** | native `ST_*` / `SDO_*` — **built into the engine** | ✅ | none (always present) |
| **PostgreSQL** | native **PostGIS** `ST_*` | ✅ | `CREATE EXTENSION IF NOT EXISTS postgis` at init; **fail loud** if it can't |
| **DuckDB** | native `spatial` `ST_*` | ✅ | `INSTALL spatial; LOAD spatial;` at init; **fail loud** if it can't |
| **SQLite** | R\*Tree bbox prefilter **+ registered C# UDF refine** (`SqliteConnection.CreateFunction`) | ✅¹ | none (UDF registered at connect) |
| **CosmosDB / MongoDB** | native geo operators in their query translators | ✅ | none |
| **LiteDB / IndexedDB / Azure Table / DynamoDB** | — | — | **error at init** (no spatial engine) |

¹ **SQLite stays fully first-class, indexed for both surfaces.** The dedicated `store.Geo*` methods use the
R\*Tree directly (as shipped). For the LINQ surface, the SQLite query builder **injects the R\*Tree join + a
bbox prefilter** around the UDF refine whenever a `DocumentFunctions` spatial call appears as a top-level
`AND` term — so `Where(DocumentFunctions.Intersects(z.Area, poly) && z.Active)` lowers to an R\*Tree-pruned
query with the UDF as the exact refine, not a table scan. (If a spatial term can't be safely pruned — e.g.
nested under an `OR` — it degrades to a UDF scan for that term only; correct, just unindexed.) See
[SQLite — full first-class spatial](#sqlite--full-first-class-spatial).

**Only two providers ever fail at init for a *native* reason** — PostgreSQL and DuckDB, the only ones needing
an extension. The three built-in engines never fail; SQLite/Cosmos/Mongo never need enabling.

## Init behaviour (the fail-loud contract)

At store initialization, for each type with a spatial mapping:

1. Resolve the provider's spatial mechanism (above).
2. **Auto-enable** where required (PostGIS / DuckDB `spatial`). If enabling fails (extension absent, role lacks
   privilege, offline), **throw a clear `NotSupportedException` at init** naming the provider and the fix
   (e.g. *"PostgreSQL spatial requires the PostGIS extension; install it or set `PortableSpatial = true`"*).
3. **No spatial engine** (LiteDB / IndexedDB / Azure Table / DynamoDB) → **throw at init** (replaces today's
   silent `SupportsSpatial => false` then throw-at-first-query). Fail fast at config time.
4. Register the SQLite UDFs on the connection (SQLite only).
5. Create the native spatial sidecar/column + index (native providers) or R\*Tree (SQLite).

### `PortableSpatial` escape hatch (relational only)

`new PostgreSqlDatabaseProvider(cs) { PortableSpatial = true }` (and the other relational providers) forces the
**dependency-free envelope-sidecar tier** already built — for the rare deployment that must run on a stock
engine without the extension and accepts the trade-off. In this mode:

- The dedicated `store.Geo*` methods work (indexed bbox + C# refine, as shipped).
- `DocumentFunctions.<spatial>` inside a relational `Where` throws a clear "not translatable under
  `PortableSpatial`; use `store.GeoIntersects(...)`" — mirroring the `Soundex` "not translatable on Cosmos/
  Mongo" convention.

Default is **off** → native, fail-loud, LINQ works.

## `DocumentFunctions` surface

Add to the public `DocumentFunctions` static class, each with a real relate/distance-engine body:

```csharp
public static bool Intersects (Geometry a, Geometry b);
public static bool Disjoint   (Geometry a, Geometry b);
public static bool Contains   (Geometry a, Geometry b);
public static bool Within     (Geometry a, Geometry b);
public static bool Covers     (Geometry a, Geometry b);
public static bool CoveredBy  (Geometry a, Geometry b);
public static bool Touches    (Geometry a, Geometry b);
public static bool Crosses    (Geometry a, Geometry b);
public static bool Overlaps   (Geometry a, Geometry b);
public static bool GeoEquals  (Geometry a, Geometry b);
public static bool WithinDistance(Geometry a, Geometry b, double meters);
public static double Distance (Geometry a, Geometry b);   // for OrderBy
```

Lowering (`ExpressionLowerer`, extending the `DocumentFunctions` switch): map each to a `SpatialPredicate`/
`SpatialFn` IR node, which `SqlPredicateEmitter` renders per provider — native `ST_*`/`SDO`, the SQLite UDF
name, or the Cosmos/Mongo operator. The in-memory providers would run the C# body directly — but they error at
init here, so that path is dormant unless we later enable them.

## Provider contract additions

Reuse the vector-feature shape (native-typed sidecar + `(Sql, Parameters)` hooks). Add to `IDatabaseProvider`:

```csharp
// Native predicate SQL for the dedicated store.Geo* methods AND the lowered DocumentFunctions calls.
bool SupportsNativeSpatialPredicates => false;
(string Sql, IReadOnlyDictionary<string, object?> Parameters)? BuildSpatialPredicateSql(
    string tableName, SpatialPredicate predicate, string queryGeoJson, double? meters,
    string? orderByGeoJson, string? additionalWhere);

// SQLite only: register the scalar UDFs on a freshly opened connection.
void RegisterSpatialFunctions(DbConnection connection) { }   // default no-op
```

`SqlPredicateEmitter` gains a spatial-function case that, for the active provider, emits the native operator
or the registered UDF call. `DocumentStore.Geometry.cs`'s dedicated methods call `BuildSpatialPredicateSql`
on native providers (no C# refine); under `PortableSpatial` they use today's bbox + refine.

## Per-provider native mapping

| Engine | Native type + index | Ingest | Predicate form | Distance | Notes |
|---|---|---|---|---|---|
| **PostGIS** | `geography(Geometry,4326)` + **GiST** | `ST_GeomFromGeoJSON` | `ST_Intersects/Contains/Within/Covers/CoveredBy/Touches/Crosses/Overlaps/Equals/Disjoint`, `ST_DWithin(g,m)` | `ST_Distance` geodesic; `<->` KNN | full DE-9IM |
| **SQL Server** | `geography` + spatial index | `geography::STGeomFromText(WKT,4326)` | method syntax `g.STIntersects(@q)=1`, `.STContains/.STWithin/.STTouches/.STCrosses/.STOverlaps/.STEquals/.STDisjoint`, `.STDistance(@q)<=@m` | `.STDistance` geodesic | no `STCovers/STCoveredBy` → emit `STContains`+relate or candidate-refine; **left-hand ring rule** on ingest |
| **MySQL** | `GEOMETRY` SRID 4326 + `SPATIAL` | `ST_GeomFromGeoJSON` | `ST_Intersects/Contains/Within/Touches/Crosses/Overlaps/Equals/Disjoint` | `ST_Distance_Sphere` | no `Covers`/`DWithin` → `ST_Distance_Sphere<=m`; **lat-long axis order** on 4326 |
| **Oracle Spatial** | `SDO_GEOMETRY` + `MDSYS.SPATIAL_INDEX` | `SDO_UTIL.FROM_GEOJSON` | `SDO_RELATE(g,q,'mask=…')`, `SDO_WITHIN_DISTANCE(g,q,'distance=m unit=meter')` | `SDO_GEOM.SDO_DISTANCE` | register `USER_SDO_GEOM_METADATA` before index; SRID 8307 |
| **DuckDB** | `GEOMETRY` + R-Tree (spatial ext) | `ST_GeomFromGeoJSON` | `ST_Intersects/Contains/Within/Touches/Crosses/Overlaps/Equals`, `ST_DWithin` | `ST_Distance_Sphere` | planar unless sphere distance |
| **SQLite (UDF)** | R\*Tree (dedicated methods) | GeoJSON text args | UDFs `docdb_st_intersects(a,b)` … `docdb_st_dwithin(a,b,m)`, `docdb_st_distance(a,b)` over the relate engine | UDF `docdb_st_distance` | LINQ path = scan; dedicated = R\*Tree |

Predicates a native engine lacks (SQL Server `Covers`; MySQL `Covers`/`DWithin`) emit the native
`ST_Intersects` candidate set + C# relate refine — same one-sidecar candidate-refine pattern as Cosmos/Mongo,
so coverage stays complete.

## SQLite — full first-class spatial

SQLite is a primary target and loses **nothing** — it gets the complete predicate family through both
surfaces, indexed:

- **Dedicated `store.Geo*` methods** — R\*Tree bbox + C# relate refine (exactly as shipped).
- **LINQ `DocumentFunctions`** — the SQLite emitter recognizes a spatial function node and, for each top-level
  `AND` spatial term whose query geometry is a constant/parameter, rewrites the query to:
  ```sql
  SELECT d.Data FROM {t} d
    JOIN {t}_spatial_map m ON m.docId = d.Id AND m.typeName = d.TypeName
    JOIN {t}_spatial     r ON r.id = m.rowid
   WHERE r.maxLat >= @qMinLat AND r.minLat <= @qMaxLat        -- R*Tree prune from the query envelope
     AND r.maxLng >= @qMinLng AND r.minLng <= @qMaxLng
     AND docdb_st_intersects(json_extract(d.Data,'$.area'), @poly) = 1   -- exact UDF refine
     AND (json_extract(d.Data,'$.active') = 1)                -- the rest of the Where, composed
  ```
  Non-spatial predicates compose normally; ordering/paging/`Count` operate on the exact (post-refine) set. Only
  a spatial term that can't be safely pruned (nested under `OR`) falls back to a UDF scan **for that term**.
- **UDFs registered per connection** — `docdb_st_intersects/contains/within/covers/coveredby/touches/crosses/
  overlaps/equals/disjoint(a,b)`, `docdb_st_dwithin(a,b,m)`, `docdb_st_distance(a,b)` — thin wrappers over the
  relate/distance engine, hooked into the connection-open path (mirror the vector-extension load).

So SQLite matches the native providers on capability and is index-accelerated on both surfaces; the only
thing it lacks is a native geodesic `ST_Distance` (it uses the same Haversine as the relate engine — already
the documented cross-provider fidelity note).

## String `whereClause` & OData

- **Raw SQL passthrough** — on native providers callers can already hand-write
  `Query<Zone>("ST_Intersects(geom, ST_GeomFromGeoJSON(:poly))", new { poly })`. No library work.
- **OData `$filter`** — follow-on: translate the spec's `geo.intersects` / `geo.distance` to the native SQL in
  the OData provider. Native-only; optional.

## Semantic-drift guard (definition of done)

Native DE-9IM boundary rules can differ subtly from the C# relate engine (and the SQLite UDF uses the C#
engine, so SQLite vs PostGIS can differ on boundary cases). A **cross-mechanism conformance suite** asserts
that, over a shared geometry-pair fixture, every provider returns the same result set per predicate; document
any legitimate exception (e.g. a native boundary convention), preferring the native result there.

## Phasing

Four-artifact sync per phase (docs headline shifts to the LINQ surface; `SKILL.md`; `readme.md`; release note).

1. **`DocumentFunctions` surface + IR + emitter + init-resolution + fail-loud + `PortableSpatial` + no-geo
   init errors + conformance harness.** (Core; provider-agnostic scaffolding.)
2. **SQLite** — registered relate/distance UDFs **+ the R\*Tree bbox-join injection** for the LINQ surface, so
   `Where(Intersects(...))` is index-accelerated; validates the whole `DocumentFunctions` pipeline locally with
   no containers (fast end-to-end signal for the core scaffolding).
3. **PostgreSQL / PostGIS** — auto-`CREATE EXTENSION`, GiST, full DE-9IM; both surfaces native.
4. **DuckDB** — `INSTALL/LOAD spatial`, R-Tree.
5. **MySQL** — built-in `ST_*`, axis-order handling, `Covers`/`DWithin` via candidate-refine.
6. **SQL Server** — method syntax, left-hand ring ingest, `Covers`/`CoveredBy` via refine.
7. **Oracle Spatial** — `SDO_GEOMETRY`, metadata registration, `SDO_RELATE` masks.
8. **Cosmos / Mongo** — lower `DocumentFunctions` spatial into their existing geo translators (already native).

Container-gated tests run both surfaces; the conformance suite runs across every mechanism.

## Not in scope

- **SQLite native** (SpatiaLite) — excluded; SQLite uses R\*Tree + UDFs, no native dependency.
- **In-memory spatial on LiteDB/IndexedDB** — chosen to error at init instead (easy to enable later via the
  in-memory `DocumentFunctions` execution path if desired).
- **Azure Table / DynamoDB** — no spatial; error at init.

## Risks / open questions

- **PortableSpatial vs LINQ** — under the escape hatch, `DocumentFunctions` spatial in a relational `Where`
  throws; make the message point at `store.Geo*`. This is the one intentional inconsistency; document it.
- **SQLite UDF availability** — `SqliteConnection.CreateFunction` must be registered on every pooled
  connection the store opens; ensure registration hooks the connection-open path (mirror the vector extension
  load).
- **Ingest validity & axis/winding** — SQL Server left-hand rule (inverse of our GeoJSON normalization);
  MySQL/Oracle SRID axis order (4326 lat-long / 8307). Handle in ingest SQL; cover with a hemisphere fixture.
- **Distance-unit parity** — geodesic vs sphere vs planar vs the SQLite/relate Haversine. Conformance suite
  asserts ordering, not exact meters; document per provider.
- **Fail-loud ergonomics** — the init exception must be actionable (name the extension + the `PortableSpatial`
  opt-out); a misconfigured prod DB should read the message and know the fix.
