# Plan: Generic envelope-sidecar spatial for the relational providers

**Status:** ✅ Complete — all five providers (PostgreSQL, MySQL, DuckDB, SQL Server, Oracle) implemented and
green (40/40 provider geometry tests; full suite 3076 passed, 0 failed). Native `ST_*`/`SDO` pushdown and the
non-SQL fallback stores remain out of scope as noted below.
**Target version:** `11.0.0` (raw version from `version.json`, currently `11.0.0-beta.{height}`). Additive —
no breaking changes.
**Depends on:** the full-geometry release ([`spatial-full-geometry.md`](./spatial-full-geometry.md)) — the
`Geometry` model, GeoJSON, the C# relate engine, distance, and the store-level query methods must be in place
(they are). Branch off `v10`/current.

> Self-contained build spec. Read `CLAUDE.md` (repo root) for the four-artifact rule before considering any
> commit "done".

---

## Goal

Light up the **full spatial surface** — point queries (`WithinRadius`/`WithinBoundingBox`/`NearestNeighbors`)
**and** every geometry predicate (`GeoIntersects`/`GeoContainedBy`/`GeoContains`/`GeoDisjoint`/`GeoTouches`/
`GeoCrosses`/`GeoOverlaps`/`GeoEquals`/`GeoCovers`/`GeoCoveredBy`/`GeoWithinDistance`), distance sort,
geometry k-nearest, and measurement/validity — on the five relational providers that currently report
`SupportsSpatial => false`: **PostgreSQL, SQL Server, MySQL, Oracle, DuckDB**.

The approach is **dependency-free and native-spatial-free**: a plain **envelope sidecar table** (four indexed
`double` columns) + the existing C# two-pass refine. No PostGIS/`geography`/`SDO_GEOMETRY`/DuckDB-`spatial`
extension, no `ST_*` dialect translation. This is deliberately the *breadth-first, cheap* tier; native
`ST_*`/`SDO` pushdown stays a later, per-provider optimization (see [Not in scope](#not-in-scope)).

## Why it's cheap (the store layer is already provider-agnostic)

The entire store-level spatial implementation — point methods (`DocumentStore.cs`), all geometry predicate
methods (`DocumentStore.Geometry.cs`), the write-sync path, and the C# relate/distance refine — is **100%
provider-agnostic**. It touches the provider only through six members of `IDatabaseProvider`
(`IDatabaseProvider.cs:389–395`):

```csharp
bool    SupportsSpatial;
string? BuildCreateSpatialTablesSql(string tableName);
string? BuildSpatialUpsertSql(string tableName);
string? BuildSpatialDeleteSql(string tableName);
string? BuildSpatialClearSql(string tableName);
string? BuildSpatialBoundingBoxQuerySql(string tableName, string? additionalWhere);
```

So a provider gets **the whole feature** — all 11 predicates, distance ordering, geometry k-nearest,
measurement-via-`MapComputedProperty`, validity — purely by implementing those five SQL builders + flipping
`SupportsSpatial`. **No core changes.** The write path already binds the envelope params
(`@spatialMinLat/@spatialMaxLat/@spatialMinLng/@spatialMaxLng`) and the bbox query binds
`@typeName/@minLat/@maxLat/@minLng/@maxLng` — the SQL just has to match those names.

## The envelope sidecar (simpler than SQLite's R\*Tree)

SQLite needed an R\*Tree virtual table + a `docId → rowid` map table (R\*Tree keys are integers). The
relational providers don't have R\*Tree, but they don't need the map table either — key the sidecar directly
on `(docId, typeName)`:

```
{table}_spatial (
    docId    <text>,
    typeName <text>,
    minLat   <double>, maxLat <double>,
    minLng   <double>, maxLng <double>,
    PRIMARY KEY (docId, typeName)
)
-- plus a composite index for the pass-1 prune:
CREATE INDEX idx_{table}_spatial ON {table}_spatial (typeName, minLat, maxLat, minLng, maxLng);
```

Main documents already live in `"{table}"` with columns `Id`, `TypeName`, `Data` (consistent across the
relational providers). The bbox pass-1 is a plain range join:

```sql
SELECT d.Data FROM "{table}" d
  INNER JOIN "{table}_spatial" r ON r.docId = d.Id AND r.typeName = d.TypeName
  WHERE d.TypeName = @typeName
    AND r.maxLat >= @minLat AND r.minLat <= @maxLat
    AND r.maxLng >= @minLng AND r.minLng <= @maxLng
    {AND (additionalWhere) when present}
```

Pass-2 (the C# relate/refine) is unchanged and already in core.

## The five hooks per provider

Only the **DDL/DML dialect** differs. Parameter names must match core exactly.

| Hook | Body | Provider delta |
|---|---|---|
| `BuildCreateSpatialTablesSql` | `CREATE TABLE {…}_spatial (…)` + index | `double` type name; `CREATE TABLE IF NOT EXISTS` vs Oracle's existence guard; identifier quoting |
| `BuildSpatialUpsertSql` | upsert on `(docId,typeName)` binding the 6 `@spatial*` params | **idiom:** PG/DuckDB `INSERT … ON CONFLICT … DO UPDATE`; MySQL `INSERT … ON DUPLICATE KEY UPDATE`; SQL Server / Oracle `MERGE` |
| `BuildSpatialDeleteSql` | `DELETE … WHERE docId=@spatialDocId AND typeName=@spatialTypeName` | quoting only |
| `BuildSpatialClearSql` | `DELETE … WHERE typeName=@typeName` | quoting only |
| `BuildSpatialBoundingBoxQuerySql` | the range join above | quoting; `d.Data` column type is JSONB/NVARCHAR/JSON/CLOB but selected verbatim |
| `SupportsSpatial` | `=> true` | — |

Per-provider specifics to nail:
- **PostgreSQL** — `double precision`; `ON CONFLICT (docId, typeName) DO UPDATE`; table quoted `"{table}_spatial"`, columns unquoted. Simplest — **do this first**.
- **DuckDB** — `DOUBLE`; `ON CONFLICT … DO UPDATE` (supported). No `spatial` extension load.
- **MySQL** — `DOUBLE`; `INSERT … ON DUPLICATE KEY UPDATE`; backtick quoting; ensure `utf8`/case handling matches the main table.
- **SQL Server** — `float`; `MERGE` upsert (or a `DELETE`+`INSERT` to sidestep the well-known MERGE concurrency caveats — pick one and note it); `[bracket]` quoting.
- **Oracle** — `BINARY_DOUBLE`; `MERGE … USING (SELECT … FROM DUAL)` (mirror the existing vector-sidecar MERGE pattern in `OracleDatabaseProvider.cs`); uppercased identifiers; **watch the 128-char identifier limit** on `{table}_spatial` (30 on pre-12.2). Oracle SQL already uses `@name` params elsewhere in this provider, so follow that convention verbatim.

## What comes for free (zero additional work)

Because these all live in core and only consume the hooks + C# refine:
`GeoIntersects`, `GeoContainedBy`, `GeoContains`, `GeoDisjoint`, `GeoTouches`, `GeoCrosses`, `GeoOverlaps`,
`GeoEquals`, `GeoCovers`, `GeoCoveredBy`, `GeoWithinDistance`, distance sort (`orderByDistanceFrom`), geometry
`NearestNeighbors`, and measurement filtering via a materialized `MapComputedProperty`.

## Performance & fidelity (document honestly)

This is the **index-assisted two-pass** tier — between SQLite's R\*Tree and true native `ST_*`:
- Pass-1 uses a **B-tree** composite index (`typeName` equality prefix + `minLat`/`maxLat` range). It prunes
  well by type and latitude band, but a plain B-tree is weaker than R\*Tree/GiST for 2-D — a query spanning a
  wide longitude range at a common latitude can pass more candidates to the C# refine. Still index-assisted,
  **not** a full table scan.
- **`GeoDisjoint`** remains anti-selective → scans the type (O(n)), same as SQLite.
- Distances are the Haversine/planar approximation (same C# engine); no geodesic `ST_DISTANCE`. Ordering can
  differ on near-ties vs the native Cosmos/Mongo tier — already the documented cross-tier story.

## Phasing

Each phase carries the **four-artifact sync** (code+tests, docs release note + spatial page provider table,
`SKILL.md` provider notes, `readme.md`).

- **Phase 0 — shared test base.** Add a provider-parameterized geometry test base (mirror
  `BulkBackupTestsBase`): one `GeometryQuerySpecBase(IDocumentStoreFixture)` running the full predicate +
  distance matrix, subclassed per provider fixture. Confirms every provider behaves identically. (SQLite/Cosmos/
  Mongo can adopt it too.)
- **Phase 1 — PostgreSQL** (simplest idiom). Implement the five hooks + `SupportsSpatial`; run the spec base
  container-gated. This validates the whole generic approach end-to-end.
- **Phase 2 — DuckDB + MySQL** (`ON CONFLICT` / `ON DUPLICATE KEY`).
- **Phase 3 — SQL Server** (`MERGE`/`DELETE`+`INSERT`).
- **Phase 4 — Oracle** (`MERGE` from DUAL, `BINARY_DOUBLE`, identifier length).

A tiny shared helper (`RelationalSpatialSql`) can emit the common table/query shape with provider callbacks
for quoting/type/upsert to cut duplication — optional; per-provider inline is fine too.

## Cross-cutting wiring to verify

- **Table-init** already calls `BuildCreateSpatialTablesSql` (`DocumentStore.cs:243`) — returning non-null
  auto-creates the sidecar. Confirm it's gated on a spatial mapping existing for the type (as SQLite is), so
  non-spatial tables don't get an empty sidecar.
- **`IDocumentMaintenance.ClearAll`** enumerates user tables via `BuildListTablesSql` — ensure the new
  `{table}_spatial` sidecars are included (dropped/cleared) and not mistaken for a document table.
- **`IDocumentBackup`** export/restore: sidecars are rebuilt by the write path on restore (as today) — no
  export needed; confirm restore repopulates envelopes.
- **JSON lane** write path (`DocumentStore.JsonLane.cs`) already computes the envelope from the GeoJSON node —
  provider-agnostic, works once the hooks exist.

## Not in scope

- **Native `ST_*`/`SDO` pushdown** (PostGIS `ST_Intersects`/GiST, SQL Server `geography` methods, MySQL
  `ST_*`, Oracle `SDO_RELATE`, DuckDB `spatial`) — the performance-optimal tier; a later, independent per-
  provider optimization. This plan intentionally delivers correctness + reasonable performance everywhere
  first, dependency-free.
- **Fallback providers** (LiteDB, IndexedDB, AzureTable, DynamoDB) — not SQL, so the SQL-sidecar approach
  doesn't apply. A separate in-memory full-scan-refine tier (`SupportsSpatial` behind an explicit opt-in)
  could follow; not here.

## Risks / open questions

- **B-tree bbox selectivity** — acceptable for the correctness-first tier; document, and let native pushdown
  address hotspots later.
- **SQL Server `MERGE` concurrency** — prefer a guarded upsert or `DELETE`+`INSERT` in one statement/txn.
- **Oracle identifier length** on `{table}_spatial` (and any index name) — verify against the target Oracle
  version; shorten the suffix/index name if needed.
- **Antimeridian / pole envelopes** — a bbox crossing ±180° over-selects or misses; inherits the existing
  SQLite limitation. Document, don't solve.
- **Double precision / NULL handling** — a document with a null spatial property must skip the sidecar (the
  core write path already does this via `ResolveEnvelope` returning null); confirm per provider.
