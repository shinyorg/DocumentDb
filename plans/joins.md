# Plan — cross-type JOIN queries

**Status:** Built for **14.0** (release note under `## 14.0 - TBD`), **hardening open** — see "Hardening" below; that
list must be finished before 14.0 ships. The review of the original sketch that led here is summarized at the end.

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

## Hardening — OPEN, do next (no stopping until every box is checked)

Corners found in review after the 14.0 build. Each item lists what is wrong, the fix, and the proof. Work top to bottom;
the work is not finished until every item is checked, the **full** suite is green with Docker running (`CLAUDE.md`), and
the docs/skill/readme/release note match what the code actually does.

### 1. Join on the real `Id` column, not `json_extract(Data, '$.id')`
- **Wrong:** `ExpressionLowerer` has no `Id` special case, so `(o, c) => o.CustomerId == c.Id` compares against the JSON body.
  The right table's primary key `(Id, TypeName)` is never used — a scan per join on a large right-hand type.
- **Fix:** when a join-side member chain is exactly the side's mapped id property (default `Id`, or `cfg.MapIdProperty`),
  lower it to a new `IdColumnNode(Source, ClrType)` emitted as `{source}.Id`. The `Id` column is stored as text on every
  relational provider, so the *other* operand must compare as text: cast it (`provider` hook, e.g. `CAST(… AS TEXT)` /
  `NVARCHAR` / `VARCHAR2`) or, for a constant, bind the id's stored string form (same normalization `Get` uses — Guid
  lowercase "D", int/long invariant). Check how each provider's `BuildGetSql` binds the id before choosing the cast.
  Apply on both sides (`o.Id == c.OrderId` too). MongoDB: map the id member to the top-level `id` field (indexed with
  `typeName`) instead of `data.id`, in `$match`, `let` and `$expr`.
- **Proof:** `JoinQueryTestsBase` — join on `c.Id` with Guid, int and string id types (models `JoinGuidCustomer`,
  `JoinIntCustomer`) returning correct pairs on every provider; `ToQueryString` asserts the rendered SQL references
  `j1.Id` and not `'$.id'` (relational) / `"$id"` not `"$data.id"` (MongoDB). A custom `MapIdProperty` side joins too.

### 2. Sessions and `DocumentContext` — docs claim it, nothing tests it
- **Wrong:** `querying.mdx` says a join from `session.Query<T>()` runs inside the session's transaction. Untested; the
  relational join also calls `executor.ExecuteAsync(rightTable, …)` to touch the right table, which must not escape the
  transaction or deadlock SQLite's single connection.
- **Fix:** whatever the tests expose. If the transaction executor cannot serve it, fix the executor path rather than
  softening the docs.
- **Proof:** relational-only test (fixtures implementing `IDatabaseFixture`, or a `SupportsTransactions` guard): open a
  session, `BeginTransaction`, insert an order + customer through the session, join through `session.Query<JoinOrder>()`
  and see the uncommitted pair; roll back and the join is empty. A `DocumentSet<T>.Query().Join(…)` test on SQLite.
  Cross-table variant (right type in its own `cfg.Table`) inside the transaction.

### 3. Field-to-field comparison on JSON collections
- **Wrong:** the grammar change (`FilterExpressionParser.ParseComparison` field on the right-hand side) was tested on typed
  queries only. On a name-keyed `store.Collection("x")` both operands are unresolved (`DynamicFieldBinder`), so
  `a > b` may throw building `Expression.GreaterThan(object, object)` or silently compare as strings.
- **Fix:** in the schema-free binder, two unresolved operands must require a type hint for relational operators
  (`total:number > limit:number`), with a clear error when neither side is hinted; equality may fall back to string.
  One hinted side types the other (`AdaptTo`). Type-keyed collections (`Collection(typeof(T))`) resolve through metadata.
- **Proof:** `JsonGrammarParityTests` (SQLite): the same `"total > discount"` through `Query<T>()`, `Collection(typeof(T))`
  and `Collection("name")` with hints returns the same ids; unhinted `a > b` on a name-keyed collection throws the message.
  One MongoDB/document-provider string `Where("total > discount")` test — confirm `MongoExpressionVisitor` handles two
  member operands or throws a clear error (it currently evaluates the "value" side as a closed expression).

### 4. Joins under native AOT
- **Wrong:** `Sample.Aot` publishes clean but never runs a join; `Select`/`Project` go through `ExpressionInterpreter`
  (reflection `PropertyInfo.GetValue` over `JoinPair<,>`, anonymous-type construction) and `ToJsonNode`
  (`options.GetTypeInfo(value.GetType())`).
- **Fix:** add a join to `samples/Sample.Aot` (named-DTO `Select`, string `Project`, left join) executed at startup so the
  CI publish step runs it; fix any trim/AOT warning or runtime failure found (annotations or source-generated type infos
  in the sample's context). Run the published binary locally, not just the publish.
- **Proof:** `dotnet publish samples/Sample.Aot -r osx-arm64 -c Release` warning-free **and** the binary's join output
  printed/asserted.

### 5. Extra connection per cross-table join (relational)
- **Wrong:** `JoinDocumentQuery.EnsureRightTableAsync` runs `executor.ExecuteAsync(rightTable, _ => true)` before every
  cross-table join — a second connection/session round-trip on pooled providers each time.
- **Fix:** add `IQueryExecutor.ExecuteAsync(IReadOnlyList<string> tableNames, …)` (or an `EnsureTableAsync(session,
  table)` seam) so both tables initialize inside the one session the join uses; implement in `DocumentStore` and the
  transaction executor. Delete `EnsureRightTableAsync`.
- **Proof:** existing `SidesInDifferentTables` stays green on all providers; add a test that a cross-table join against a
  never-touched right type works as the first operation on a fresh store (the lazy-init case).

### 6. MongoDB `Any` counts everything
- **Wrong:** `MongoJoinQuery.AnyAsync` runs `$count` over the whole match.
- **Fix:** build the pipeline with `$limit: 1` (no sort) and test for a row.
- **Proof:** `CountAnyFirst` stays green; `ToQueryString`-style assertion not needed — unit-check the pipeline builder emits
  `$limit` for Any (internal test via IVT).

### 7. Test coverage the plan promised
- String-syntax equivalents: right-side query filter + `IgnoreQueryFilters` through `Join<T>("o","c",…)`; tenancy via the
  string join; `Paginate` + `OrderByDescending("…")` string; interpolated `Where` on a string join.
- SQLCipher: declare `JoinQueryTests` for a SQLCipher fixture if one exists (else add the class to the SQLCipher test area).
- MongoDB negative cases: left join with a left-only condition throws the documented message; a cross-document
  non-property comparison (`o.Total + 1 > c.CreditLimit`) throws.
- Relational: `Join` after `Select`/`Project`/`GroupBy` is not reachable (terminal types) — assert the default interface
  throw on a projected query.

### 8. Docs and records match the code
- `querying.mdx` Joins section: replace "Index the right side's key path" with the Id-column behaviour from item 1;
  change "One SQL statement" only if item 5 lands; state the session behaviour exactly as item 2 proves it.
- Release note (14.0 TBD) and readme bullet: mention joining on the id column, sessions, and the JSON-collection hint rule
  from item 3. `SKILL.md` join rules: same.
- Amazon DocumentDB capability record: `DocumentDbDocumentStoreOptions` re-implements `IDocumentStoreOptions.Capabilities`
  with `Joins = false` (and, deliberately, `FullText`/`Vector` = false, with a release note because validate-on-build will
  now reject those mappings there). Update "Where the build departed" table.
- This section: check each box as it lands; delete the section when all are done and fold anything notable into
  "As built".

**Exit:** every item above done; full `Shiny.DocumentDb.Tests` + Orleans + admin + Tui + MCP suites green in Release
with Docker; AOT sample published and run; `plans/joins.md`, memory (`project_joins`) updated.

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
