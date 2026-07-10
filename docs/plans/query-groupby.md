# Plan: Query `GroupBy` — grouped aggregation over JSON

**Status:** Designed, not started.
**Target version:** `11.0` (`version.json` = `11.0.0-beta`, so notes go under `## 11.0 TBD`). **Additive** —
promotes a today-dormant `IDocumentQuery<T>.GroupBy` to a real surface on top of the already-shipping aggregate
engine; one optional string-grammar extension. No breaking change. Branch off `v11`.

> Self-contained build spec. Read `CLAUDE.md` (repo root) for the four-artifact rule (code+tests, docs site,
> skill, readme) and the **query-surface parity** rule (LINQ + string grammar) before considering any commit
> "done".

---

## The surprise: half of this already ships

Grouped aggregation over documents **already works today** on every relational provider — it is just implicit,
undocumented, and inconsistently wired. The real engine is `Internal/AggregateTranslator.cs`, reached through
`Select` + the `Sql.*` markers (`src/Shiny.DocumentDb/Sql.cs`):

```csharp
// WORKS TODAY (SQLite/PG/MySQL/SQL Server/Oracle/DuckDB) — revenue + order count per status
var rollup = await store.Query<Order>()
    .Where(o => o.CreatedAt >= since)
    .Select(o => new StatusRollup {
        Status  = o.Status,        // bare member access → becomes a GROUP BY column
        Count   = Sql.Count(),
        Revenue = Sql.Sum(o.Total)
    })
    .ToList();
```

`AggregateTranslator.TranslateAggregateExpression` treats each **bare member access** in the projection as a
`GROUP BY json_extract(Data,'$.path')` column and each `Sql.Count/Sum/Min/Max/Avg` as an aggregate term
(`AggregateTranslator.cs:64-71`). `ProjectedDocumentQuery` emits the real `... GROUP BY <cols>` clause
(`ProjectedDocumentQuery.cs:121`, `:172`, `:236`) whenever the projection contains a `Sql.*` aggregate **or** a
non-null `groupBy` field is present (`useAggregate` gate at `:107`, `:154`, `:206`).

So the group key is literally `provider.JsonExtract("Data", jsonPath)` — **grouping on a JSON property is the
only thing it does.** Top-level, nested (`o.Address.Country`), whatever path resolves.

### What's actually broken / missing

1. **`IDocumentQuery<T>.GroupBy(selector)` is dormant and inconsistent.** It is captured
   (`DocumentQuery.groupBy`, `:20`/`:95`) and only ever consulted as `this.groupBy != null` to *toggle*
   aggregate mode after a `Select`. On its own (no `Select`) the core ignores it, and providers diverge:
   Cosmos throws `"GroupBy is only supported with Select projections containing aggregate functions."`
   (`CosmosDbDocumentQuery.cs:100`), LiteDB stashes it but never groups (`LiteDbDocumentQuery.cs:100`), the
   Diagnostics wrapper forwards it (`InstrumentedDocumentQuery.cs:28`), Azure Table has its own stub. It is a
   placeholder that reads as supported.
2. **The grouping is implicit and a footgun.** Add a bare member to an aggregate `Select` and you silently
   change the `GROUP BY`. There is no discoverable "this is a grouped query" signal, no `g.Key`.
3. **No multi-key story is documented** (it works — two bare members = two group columns — but nobody knows).
4. **No `HAVING`.** No way to filter groups by an aggregate (`grep -rn HAVING` → zero hits).
5. **No string-grammar parity.** `FilterExpressionParser` has no aggregate functions in `IsValueFunction`
   (`FilterExpressionParser.cs:942`), so `Project("status, count() as n")` can't group — violates the
   CLAUDE.md parity rule the moment we make the LINQ surface public.
6. **Non-relational providers don't push down.** Mongo (`$group`), Cosmos (constrained), in-memory
   (LiteDB/IndexedDB) all need their own path; the key-partitioned NoSQL providers can't do it at all.

**Conclusion:** the value/effort ratio is unusually good — the relational engine is ~80% built. This plan is
mostly *exposing, hardening, and giving parity* to what exists, plus Mongo + in-memory push-down, plus an honest
capability tier for the providers that can't.

---

## Goal

A **discoverable, explicit** grouped-aggregation surface that lowers to the existing aggregate engine, works on
a JSON property (or a **derived** value), supports multi-key and `HAVING`, and has LINQ + string-grammar parity.
Provider support is **capability-tiered** exactly like spatial / full-text.

```csharp
// Explicit, EF-style — the discoverable public surface
var rollup = await store.Query<Order>()
    .Where(o => o.CreatedAt >= since)
    .GroupBy(o => o.Status)                         // group key = a JSON property
    .Having(g => Sql.Sum(g => g.Total) > 10_000)    // optional — filter groups by an aggregate
    .Select(g => new StatusRollup {
        Status  = g.Key,                            // the group key
        Count   = Sql.Count(),
        Revenue = Sql.Sum(g => g.Total),
        AvgLine = Sql.Avg(g => g.Total)
    })
    .OrderByDescending(r => r.Revenue)              // order the grouped rows (post-Select, on the result shape)
    .ToList();

// Multi-key
.GroupBy(o => new { o.Status, o.Region })          // GROUP BY two JSON columns; g.Key.Status / g.Key.Region

// Derived key — group by a value you never declared a column for
.GroupBy(o => Sql.DatePart(o.CreatedAt, DatePart.Month))   // "revenue by month"
```

---

## Non-goals

- **Not replacing the whole-set scalar aggregates.** `query.Count/Sum/Min/Max/Average(selector)`
  (`DocumentQuery.cs:305-461`) stay — they are ungrouped terminals and remain the simplest path for "total
  over the whole filtered set".
- **No `IGrouping<TKey,T>` materialization** (i.e. no "give me each group *with its member documents*"). This
  is aggregate-only: a group produces one output row. Returning the raw members per group is a full-scan
  client-side operation on most backends; if ever wanted it is a separate, clearly-costed follow-up
  (`GroupBy(...).ToLookup()`), not v1.
- **No grouping on the key-partitioned NoSQL providers** (Azure Table / DynamoDB). They **throw**
  `NotSupportedException` rather than silently scan-and-group client-side (same discipline as their cursor /
  spatial stance).
- **No `DISTINCT` aggregate** (`COUNT(DISTINCT x)`) in v1 — note as a follow-up; the `Sql.*` markers don't carry
  a distinct flag today.

---

## Design decisions (locked)

| Decision | Choice | Consequence |
|---|---|---|
| Public shape | **Explicit `GroupBy(key).Select(g => …)`** with `g.Key`, on top of the existing engine | Discoverable; kills the "bare member silently groups" footgun. The implicit member-access path stays working (back-compat) but docs steer everyone to the explicit form. |
| Group key | Any value expression → `json_extract` (member), **or** a `Sql.*`/derived scalar, **or** an anonymous type for multi-key | "Group by a JSON property" is the headline; derived keys (date-part, computed prop) are the doc-store differentiator. |
| Aggregates | Reuse `Sql.Count/Sum/Min/Max/Avg` markers | Already built + translated. Extend `Sql.Sum(g => g.X)` lambda form so it reads naturally off the group. |
| `HAVING` | New `Having(g => <bool over Sql.* aggregates>)` builder | Emitted as `HAVING <expr>` after `GROUP BY`. The only new predicate lowering; reuses aggregate translation. |
| Ordering grouped rows | `OrderBy` **after** `Select`, over the result shape | Sorting a grouped result sorts by an output column/aggregate; the post-`Select` `OrderBy` (today throws — see below) must be allowed for grouped queries. |
| Result typing | `Select(g => new TResult { … })` (typed) **and** `Project("…")` (JsonObject) | Parity with the rest of the query surface. |
| Providers that can't push down | **Throw** `NotSupportedException` with an actionable message | Mirrors spatial / full-text / cursor. No silent client-side grouping. |
| `Paginate` on a grouped query | Allowed (paginate the group rows) | Groups are rows; `LIMIT/OFFSET` after `GROUP BY … [HAVING] … ORDER BY` is valid SQL. |

**Post-`Select` `OrderBy` for grouped queries.** `ProjectedDocumentQuery.OrderBy` currently throws
`"Cannot modify query after Select."` (`ProjectedDocumentQuery.cs:80`). For grouped/aggregate projections we
**must** allow ordering by an output column (you can't order a grouped result any other way). Scope the
relaxation to the aggregate path: when `useAggregate`, accept `OrderBy(r => r.SomeResultMember)` and emit it
against the result-shape column (the alias produced by the projection), not the source JSON. Non-aggregate
`Select` keeps throwing (unchanged).

---

## Public API surface

### Builder verbs — `Shiny.DocumentDb/IDocumentQuery.cs`

`GroupBy` already exists on the interface (keep the signature). Add `Having`, and a grouped result marker so the
compiler can offer `g.Key` / `Sql.Sum(g => …)`:

```csharp
/// <summary>
/// Groups the filtered documents by <paramref name="keySelector"/> and switches the query into
/// aggregate mode: the following <see cref="Select"/> / <see cref="Project"/> projects one row per group,
/// using <c>g.Key</c> for the group value and <see cref="Sql"/> aggregates (Count/Sum/Min/Max/Avg) over
/// the group's members. The key may be a JSON property (<c>o =&gt; o.Status</c>), a derived scalar
/// (<c>o =&gt; Sql.DatePart(o.CreatedAt, DatePart.Month)</c>), or an anonymous type for a multi-column key
/// (<c>o =&gt; new { o.Status, o.Region }</c>). Not supported on key-partitioned providers
/// (Azure Table / DynamoDB) — those throw <see cref="NotSupportedException"/>.
/// </summary>
IGroupedDocumentQuery<T, TKey> GroupBy<TKey>(Expression<Func<T, TKey>> keySelector);
```

Rather than overload the flat `IDocumentQuery<T>`, `GroupBy` returns a **`IGroupedDocumentQuery<T, TKey>`**
whose `Select`/`Project` take an `IDocumentGroup<TKey, T>` so `g.Key` is strongly typed:

```csharp
namespace Shiny.DocumentDb;

/// <summary>A grouped query awaiting an aggregate projection. Produced by <see cref="IDocumentQuery{T}.GroupBy"/>.</summary>
public interface IGroupedDocumentQuery<T, TKey>
{
    /// <summary>Filters groups by an aggregate predicate (SQL <c>HAVING</c>). May be called more than once (AND-ed).</summary>
    IGroupedDocumentQuery<T, TKey> Having(Expression<Func<IDocumentGroup<TKey, T>, bool>> predicate);

    /// <summary>Projects one row per group. Use <c>g.Key</c> and <see cref="Sql"/> aggregates in the selector.</summary>
    IDocumentQuery<TResult> Select<TResult>(
        Expression<Func<IDocumentGroup<TKey, T>, TResult>> selector,
        JsonTypeInfo<TResult>? resultTypeInfo = null) where TResult : class;

    /// <summary>String-grammar projection of the group (see the aggregate grammar below) → <c>JsonObject</c> rows.</summary>
    IDocumentQuery<JsonObject> Project(string fields, JsonTypeInfo<T>? jsonTypeInfo = null);
}

/// <summary>The group handed to an aggregate projection: its key plus the <see cref="Sql"/>-aggregable members.</summary>
public interface IDocumentGroup<out TKey, T>
{
    /// <summary>The group key (the value the query grouped by).</summary>
    TKey Key { get; }
    // No member enumeration — aggregate-only. Sql.Sum(g => g.X) reads members via the aggregate translator.
}
```

The returned `IDocumentQuery<TResult>` from `Select` is an ordinary query, so `OrderBy` (grouped-row ordering,
per the relaxation above), `Paginate`, `ToList`, `ToAsyncEnumerable`, and `ToQueryString` all flow through
unchanged.

> **Fallback shape** if the generic `IGroupedDocumentQuery` proves heavy for the source generator / AOT
> resolver: keep the existing flat `GroupBy(Expression<Func<T, object>>)` returning `IDocumentQuery<T>` and rely
> on the *already-working* implicit "bare member in aggregate `Select` = group column" mechanism, adding only
> `Having` + docs. Less type-safe (no `g.Key`), but zero new interfaces. Decide before build; the emitter below
> is identical either way.

### `Sql` aggregate markers — `Shiny.DocumentDb/Sql.cs`

Add the **lambda-over-group** overloads so aggregates read naturally off `g`, alongside the existing
value-form ones (keep both — the value form backs the implicit path):

```csharp
public static TValue Sum<TSource, TValue>(this IDocumentGroup<object, TSource> g, Func<TSource, TValue> selector) => throw …;
// …Min/Max/Avg the same. Count() is parameterless (COUNT(*)).
```

(Exact generic ergonomics settled at build; these are never executed — pure expression-tree markers, same as
today's `Sql.*`.)

### String grammar — aggregate functions in `FilterExpressionParser`

Per the parity rule, the string surface must group too. Extend the projection grammar so
`GroupBy(...).Project("…")` accepts aggregates and a bare key column:

```csharp
store.Query<Order>()
     .GroupBy("status")
     .Project("status, count() as orders, sum(total) as revenue")
     .Having("sum(total) > 10000")     // string HAVING
```

- Add `count`, `sum`, `avg`, `min`, `max` to a new `IsAggregateFunction` set consulted **only** in the
  projection/HAVING context (`FilterExpressionParser.cs` near `IsValueFunction` at `:942`).
- Bare identifiers in a grouped `Project` that aren't aggregates resolve to group-key columns (must match the
  `GroupBy` key path — validate and throw a clear error otherwise).
- Both surfaces lower to the **same** `AggregateTranslator` output (identical `GROUP BY` / `HAVING` SQL) —
  cover LINQ ↔ string parity in tests exactly like the existing function-parity suites.

---

## How it executes

The engine already emits `SELECT <json_object of aggregates> FROM {t} WHERE TypeName=@t [AND filter]
GROUP BY <cols>`. This plan adds three emit pieces and the provider tiers.

1. **`GroupBy` key → group columns.** Lower the key selector with the existing selector→JSON-path machinery
   (`AggregateTranslator.BuildMemberChainFromRoot` + `JsonPropertyNameResolver.BuildJsonPath`). A member →
   `json_extract(Data,'$.path')`; an anonymous type → one column per member; a `Sql.*`/derived scalar → lower
   through the shared value IR (`ExpressionLowerer.LowerValue`) so date-part / computed-property keys work.
   `g.Key` in the projection re-emits the **same** column expression(s).
2. **`Having` → `HAVING`.** Lower the predicate's aggregate sub-expressions with `TranslateSqlMarker`
   (reused verbatim) and its comparisons with the ordinary predicate lowering, emit `HAVING <expr>` immediately
   after `GROUP BY` (`ProjectedDocumentQuery` SQL assembly at `:121`/`:172`/`:236`).
3. **Grouped-row `OrderBy`.** In the aggregate branch, resolve `OrderBy(r => r.Member)` to the projected output
   alias and append `ORDER BY <alias>` after `HAVING` (relaxing the current post-`Select` throw for the
   aggregate path only).

**Relational providers (SQLite, PostgreSQL, MySQL, SQL Server, Oracle, DuckDB)** — all of the above is portable
ANSI `GROUP BY` / `HAVING` over `json_extract` columns; **already emitting** for the group part. Only `HAVING`
+ grouped-`OrderBy` + multi-key + derived-key are genuinely new SQL, and all are dialect-neutral. Recommend an
index on the group-key path (`MapIndexedProperty` / `MapComputedProperty(indexed:true)`) for hot rollups; note
in docs + skill.

**MongoDB** — grouping is Mongo's strength. `MongoDbDocumentQuery` builds a `$group` stage: `_id` = the key
expression(s), accumulators `$sum`/`$avg`/`$min`/`$max`/`$sum:1`(count); `Having` → a `$match` after `$group`;
grouped `OrderBy` → `$sort` after. Natural, first-class.

**LiteDB / IndexedDB (in-memory)** — evaluate the group client-side: materialize the filtered set, LINQ
`GroupBy` on the compiled key selector, compute the aggregates, project. Correct and simple, but it **loads all
matched docs** — document the cost and recommend a `Where` pre-filter. (The `Sql.*` markers throw if actually
invoked, so the in-memory path computes aggregates directly, not via the markers.)

**Cosmos** — Cosmos SQL supports `GROUP BY` with real constraints (limited aggregate set, cross-partition
behavior, no `ORDER BY` on a grouped aggregate in some SDK versions). Implement the supported subset in
`CosmosDbDocumentQuery` (replace the current blanket throw at `:100`), and throw a clear
`NotSupportedException` for the parts Cosmos can't express (e.g. grouped `OrderBy` if unavailable) — same honest
tiering as the full-text plan.

**Azure Table / DynamoDB** — no server-side grouping and key-partitioned. **Throw**
`NotSupportedException("GroupBy is not supported on <provider> — read with a filter and aggregate client-side, or use a relational/Mongo store.")`. No silent scan.

**Engine touch-points:**
- `Internal/AggregateTranslator.cs` — accept an explicit key spec (from `GroupBy`) in addition to inferring from
  bare members; add `HAVING` lowering; support anonymous-type and derived-scalar keys via `ExpressionLowerer`.
- `Internal/ProjectedDocumentQuery.cs` — thread the key spec + `Having` clause + grouped-`OrderBy` through the
  three SQL-assembly sites; relax the post-`Select` `OrderBy` throw for the aggregate path.
- `Internal/DocumentQuery.cs` — `GroupBy<TKey>` returns the new `IGroupedDocumentQuery<T,TKey>` (or, fallback
  shape, keep flat).
- New `Internal/GroupedDocumentQuery.cs` — carries key + havings, produces the `ProjectedDocumentQuery` on
  `Select`/`Project`.
- `Internal/FilterExpressionParser.cs` — aggregate-function recognition in the projection/HAVING context.
- `IDatabaseProvider` — **no new seam needed for relational** (reuses `JsonExtract`/`JsonExtractNumeric`/
  `JsonObject`). Mongo/Cosmos implement in their own query classes; in-memory in their evaluators.

---

## Provider capability matrix

| Provider | Grouping | Notes |
|---|---|---|
| SQLite / SQLCipher | ✅ Push-down | `GROUP BY json_extract` — group part **already emits**. + HAVING/ORDER BY/multi-key. |
| PostgreSQL / MySQL | ✅ Push-down | Same portable ANSI. |
| SQL Server / Oracle | ✅ Push-down | Same; grouped `OrderBy` + `OFFSET/FETCH` paging valid. |
| DuckDB | ✅ Push-down | Analytics engine — grouping is its wheelhouse. |
| MongoDB | ✅ Push-down | `$group` + `$match`(HAVING) + `$sort`. First-class. |
| LiteDB / IndexedDB | ⚠️ Client-side | In-memory LINQ `GroupBy`; loads matched set — document the cost. |
| Cosmos | ⚠️ Partial | Supported subset of Cosmos `GROUP BY`; throws on the parts it can't express. |
| **Azure Table / DynamoDB** | ❌ Throws | Key-partitioned, no server grouping — `NotSupportedException`, no silent scan. |

Providers that don't opt in inherit the `NotSupportedException` default — no silent wrong behavior.

---

## Tests — `tests/Shiny.DocumentDb.Tests/GroupByQueryTests.cs`

1. **Single-key rollup** SQLite: seed orders across N statuses; `GroupBy(o => o.Status).Select(g => new { g.Key,
   Count = Sql.Count(), Revenue = Sql.Sum(g => g.Total) })`; assert per-status count + sum vs a LINQ-to-objects
   baseline over the same seed.
2. **Group by a nested JSON property** (`o => o.Address.Country`) — path resolves correctly.
3. **Multi-key** (`new { o.Status, o.Region }`) — one row per (status, region) combo; `g.Key.Status` /
   `g.Key.Region` project correctly.
4. **Derived key** (`Sql.DatePart(o.CreatedAt, Month)` or a `MapComputedProperty`) — "by month" rollup groups on
   the computed value, not a stored column.
5. **`Having`** filters groups by an aggregate (`Sql.Sum(g => g.Total) > threshold`) — excluded groups absent.
6. **Grouped `OrderBy`** (`.OrderByDescending(r => r.Revenue)`) + **`Paginate`** over group rows — top-N groups.
7. **Empty/COALESCE**: a group with all-null aggregate source yields `0` not null (existing `CoalesceZero`).
8. **String grammar** (`GroupBy("status").Project("status, count() as n, sum(total) as revenue")` +
   `Having("sum(total) > …")`) yields identical rows to the LINQ form — **LINQ ↔ string parity**.
9. **Back-compat**: the *implicit* bare-member aggregate `Select` (no explicit `GroupBy`) still groups exactly
   as before.
10. **In-memory** (LiteDB + IndexedDB): same rollup via client-side grouping matches the SQLite baseline.
11. **Cross-provider parity**: core single-key + multi-key + HAVING against PostgreSQL + MySQL + SQL Server +
    Mongo (container-gated, `Assert.Skip` when unavailable) — same aggregates.
12. **Unsupported providers**: Azure Table + DynamoDB `GroupBy` ⇒ `NotSupportedException` with the guidance
    message.
13. **`DocumentSet<T>`** (typed context) exposes `GroupBy` and rolls up correctly.
14. **`ToQueryString`** on a grouped query emits `GROUP BY` (+ `HAVING`) — snapshot the SQL for the relational
    dialects.

Run: `dotnet test tests/Shiny.DocumentDb.Tests/Shiny.DocumentDb.Tests.csproj --filter "FullyQualifiedName~GroupByQueryTests"`.

---

## Build order (SQLite-first precedent)

explicit `GroupBy`/`Having` surface + `IGroupedDocumentQuery` + engine wiring on the existing `AggregateTranslator`
(fully testable on SQLite) → in-memory (LiteDB/IndexedDB) client-side path + parity tests → string-grammar
aggregates + parity → relational batch (PG, MySQL, SQL Server, Oracle, DuckDB) → Mongo `$group` → Cosmos subset
→ Azure Table / DynamoDB explicit-throw → 4-artifact docs sync.

## Four-artifact sync (CLAUDE.md)

1. **code + tests** — above.
2. **docs site** (`~/Desktop/dev/documentation/.../documentdb/`) — `querying.mdx`: a "Grouping & aggregation"
   section (the explicit `GroupBy(key).Having(...).Select(g => …)` recipe, `g.Key`, multi-key, derived keys,
   grouped `OrderBy`/paging, the indexing tip, the string-grammar form), and note that the whole-set
   `Count/Sum/Average` terminals remain for ungrouped totals. Add the **capability matrix** above. Release note
   `<RN type="feature">` under `## 11.0 TBD` (create the section per the release-note rules).
3. **skill** (`skills/shiny-documentdb/SKILL.md`) — add `GroupBy` / `Having` / `IGroupedDocumentQuery` /
   `Sql.Count/Sum/Avg` to `triggers:`; a "group + aggregate" recipe; a one-liner steering generated code to
   `GroupBy(...).Select(g => …)` for rollups and to the scalar `Average/Sum` terminals for whole-set totals;
   note the provider tier (throws on Azure Table / DynamoDB).
4. **readme.md** (repo root) — add grouped aggregation to the query feature bullet.

## Edge cases / decisions to make during build

- **`IGroupedDocumentQuery` vs flat back-compat shape** — pick before build (see fallback note). The implicit
  bare-member path must keep working regardless (test #9).
- **`Sql.*` generic ergonomics** — the lambda-over-group form (`Sql.Sum(g => g.Total)`) vs the value form
  (`Sql.Sum(o.Total)`); support both, they lower identically.
- **Derived-key type round-trip** — a `DatePart`/computed key must bind + compare with the right type in `g.Key`
  and in `HAVING` (the same TEXT-vs-typed class of bug as the SQLite decimal `$filter` issue in
  `project_odata_sample`). Cover with a numeric + datetime key test.
- **Grouped `OrderBy` alias resolution** — order by the projected output column, not the source JSON; ensure the
  alias exists in the `SELECT` (error clearly if ordering by a non-projected member).
- **Cosmos subset boundaries** — enumerate exactly which aggregates + ordering Cosmos SQL allows for the target
  SDK and throw precisely for the rest (mirror the full-text Cosmos handling).
- **`COUNT(DISTINCT)` / `ToLookup` (members-per-group)** — explicitly deferred; note as follow-ups.
