# Plan — cross-type JOIN queries

**Status:** Built for **14.0** (release note under `## 14.0 - TBD`). This file records the shipped design and the
follow-ups; the review of the original sketch that led here is summarized at the end.

## What shipped

```csharp
var rows = await store.Query<Order>()
    .Where(o => o.Status == "open")                                // left side
    .Join<Customer>((o, c) => o.CustomerId == c.Id, JoinKind.Left)
    .Where((o, c) => c.Region == "eu" && o.Total > c.CreditLimit)  // either side, or both
    .OrderBy((o, c) => c.Name)
    .Paginate(0, 50)
    .Select((o, c) => new { o.Id, Customer = c == null ? null : c.Name })
    .ToList();

var json = await store.Query<Order>()
    .Join<Customer>("o", "c", "o.customerId = c.id")
    .Where("c.region = 'eu' and o.total > c.creditLimit")
    .OrderBy("c.name")
    .Project("o.id as orderId, c.name as customer, o.total")
    .ToList();
```

- **Surface:** `IDocumentQuery<T>.Join<TRight>(on, kind, rightTypeInfo)` and `Join<TRight>(leftAlias, rightAlias, on, kind,
  rightTypeInfo)` (default interface methods that throw) → `IJoinQuery<TLeft, TRight>` (`Where` LINQ/string/interpolated,
  `OrderBy[Descending]` LINQ/string, `Paginate`, `IgnoreQueryFilters`, `Select`, `Project`) → `IJoinResult<TResult>`
  (`ToList`, `ToAsyncEnumerable`, `Count`, `Any`, `First`, `FirstOrDefault`, `ToQueryString`). `JoinKind { Inner, Left }`.
  `DocumentStoreCapabilities.Joins`.
- **Providers:** SQLite, SQLCipher, DuckDB, PostgreSQL, CockroachDB, MySQL, MariaDB, SQL Server, Oracle (one statement),
  MongoDB (one aggregation). Amazon DocumentDB (`SupportsJoins => false` — no correlated `$lookup` sub-pipeline), Cosmos,
  LiteDB, IndexedDB, Azure Table, DynamoDB, Firestore, Redis, RavenDB throw `NotSupportedException`.
- **Grammar:** every join field is alias-qualified (a LINQ join's parameter names are its aliases); the comparison grammar
  accepts a field on the right-hand side everywhere, which also closed a single-type parity gap (`Where("total > discount")`).

## As built

**IR (single-document queries unchanged).** `RootFieldNode`, `ArrayLengthNode`, `CountSubqueryNode`, `AnyNode` and
`NullCheckRootNode` gained an optional `Source` (the table alias); the emitter qualifies `Data` only when it is set, so a
single-type query emits exactly what it did. `SideMissingNode` is `c == null` over a side (`j1.Id IS NULL`).
`ExpressionLowerer.LowerJoin`/`LowerJoinValue` take a `ParameterExpression → JoinSide(Source, TypeInfo, Computed)` map;
computed properties, spatial/full-text functions and schema-free fields throw inside a join.

**Core (`Internal/`).**
- `JoinDefinition<TLeft,TRight>` — the two parameters, kind, condition, `JoinSideSource<T>` per side (type info, query
  filters, the source query's `Where`s and ignore-filter state, computed lookup), and `JoinFieldBinder` for the grammar.
- `JoinQueryBase<TLeft,TRight>` — immutable builder, `Prepare()` (per-side scope with `DocumentPredicateRewriters` applied
  once to single-side conditions; a condition across both sides that touches an encrypted property throws), and shaping:
  `Select`/`Project` are interpreted over `JoinPair<TLeft,TRight>` after materialization. `Project` returns `null` for a
  field of a left join's missing side; an unguarded `Select` over it throws an explanatory `InvalidOperationException`.
- `JoinDocumentQuery` (relational) — `SELECT j0.Data, j1.Data FROM t0 j0 {INNER|LEFT} JOIN t1 j1 ON j1.TypeName = @jt1
  [AND j1.TenantId] [AND right filters] AND (condition) WHERE j0.TypeName = @jt0 [AND j0.TenantId] [AND left scope + where]
  ORDER BY … pagination`. The right table is touched first when it differs (tables are created lazily per operation).
  Streaming uses the new `IQueryExecutor.ReadRowsAsync` (the old string reader now delegates to it).
- `DocumentQueryBase` gained virtual `Join` overloads (throwing) and `JoinLeftSource()` for document-native providers.

**MongoDB (`MongoJoinQuery`).** `$match` (left type + left scope + left-only conditions) → `$lookup { from, let, pipeline:
[$match right type + right filters + right-only conditions, $match $expr(cross comparisons over $$let vars)] }` →
`$unwind { preserveNullAndEmptyArrays: left }` → `$match` (right-side conditions under `__join.data`, `c == null` as
`$exists`, cross comparisons as `$expr`) → `$sort`/`$skip`/`$limit` or `$count`. Cross-document conditions must compare two
properties; ordering is by properties; a left join's condition cannot test the left document alone.

## Where the build departed from the revised plan

| Revised plan | Shipped | Why |
|---|---|---|
| Phase 1: fold the ~10 inline relational SELECTs onto one builder, guarded by SQL snapshot tests | Not done | The join composes its own statement; single-document emission only changed behind a `Source` that is `null` for them, so there was nothing to snapshot. |
| Server-side `json_object` projection needing `JsonTypeInfo<TResult>` | Both documents are selected and the selector runs after materialization | Correct typing on every dialect, anonymous types, encrypted properties as plaintext, computed properties populated. Cost: both bodies travel. |
| Encrypted property in the projection throws | Allowed | Consequence of the above — the projection sees decrypted documents. |
| `MongoDB` capability flag true, others explicit false | `Joins = true` on the relational and MongoDB options; defaults elsewhere | Amazon DocumentDB inherits MongoDB's options, so its capability record reports `true` while the store refuses (`SupportsJoins`) — same pre-existing inconsistency as its `FullText`/`Vector`. |

## Follow-ups (not built)

- Three or more sides; `GroupBy`/`Having` over a join; cursor paging on a join.
- Server-side projection for wide documents (select only the needed paths) if the double-body read shows up in profiles.
- MongoDB: left-only conditions inside a left join's condition; non-property cross comparisons (`$expr` over functions).
- Amazon DocumentDB: an equality-only `$lookup` (localField/foreignField) path without right-side filters, if asked for.
- Capability records for Amazon DocumentDB (`Joins`, `FullText`, `Vector` all report MongoDB's values).
- Document-native encryption rewrite gap (from Phase 0): `DocumentQueryBase` still rewrites before in-memory evaluation.

## Original review (2026-09-13), in brief

The first sketch assumed the pipeline could take a second document by extension. It could not: the lowerer treated every
lambda parameter as the one document, `Data`/`TypeName`/`TenantId` were unqualified, most joins are self-joins on the shared
`documents` table (so the right side's scope must sit in `ON`), the string grammar had no field-to-field comparison and
silently bound `l.x` as a nested path on schema-free collections, and MongoDB had no `$lookup`. It also reversed published
"no JOINs" positioning, now rewritten in `limitations.mdx`, `querying.mdx`, `context.mdx`, the readme and `SKILL.md`.
