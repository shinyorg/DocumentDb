# Plan — JOIN support in Shiny.DocumentDb

## Goal

Add first-class support for querying across document types via a JOIN, with a
provider-capability flag (`SupportsJoins`) that mirrors the existing pattern used
by `SupportsSpatial`, `SupportsVector`, `SupportsFullText`, `SupportsTemporal`,
`SupportsChangeFeed`, etc. Providers without the ability throw a clear
`NotSupportedException` at call time; the capability record surfaces the same
truth so `DocumentConfigurationValidator` can fail startup when a JOIN-dependent
feature is mapped against a backend that cannot serve it.

DocumentDb is a document store, not an ORM, so this is deliberately **narrow**:
key-based inner / left joins between two (later N) document types, projected
into an anonymous / user-defined shape. No implicit navigation properties, no
change-tracked graphs, no lazy loading.

---

## Provider matrix (target for v1)

| Provider              | v1 support | Reason                                                      |
|-----------------------|-----------|--------------------------------------------------------------|
| SQLite                | ✅         | JSON1 + `json_extract`, straightforward SQL emit             |
| DuckDB                | ✅         | Native SQL joins                                             |
| SQL Server            | ✅         | `OPENJSON` / computed columns                                |
| PostgreSQL / MariaDb / MySQL / CockroachDb / Oracle | ✅ | Same relational path |
| MongoDB               | ✅         | `$lookup` aggregation                                        |
| LiteDb                | ⚠️ opt-in  | In-memory join, gated behind capability                      |
| Cosmos DB             | ❌         | Only intra-document joins; cross-container not supported     |
| IndexedDb             | ❌         | No cross-store joins                                         |
| AzureTable, DynamoDb, Firestore, Redis, RavenDb | ❌ | Key-partitioned / no join primitive |

`SupportsJoins => false` is the default on `IDocumentStore` and
`IDatabaseProvider`; only backends that implement it flip it on.

---

## API design

### 1. Public surface on `IDocumentStore`

Add to `src/Shiny.DocumentDb/IDocumentStore.cs`, next to
`SupportsSpatial`/`SupportsVector`/`SupportsFullText`:

```csharp
/// <summary>Returns true when this store can serve cross-type JOIN queries.</summary>
bool SupportsJoins => false;

/// <summary>
/// Starts a JOIN query rooted on <typeparamref name="TLeft"/>.
/// The returned <see cref="IJoinQuery{TLeft}"/> lets the caller add
/// <c>Join</c> / <c>LeftJoin</c> calls and terminate with a
/// <c>Select</c> projection into a user-defined shape.
/// </summary>
IJoinQuery<TLeft> Join<TLeft>(JsonTypeInfo<TLeft>? typeInfo = null) where TLeft : class
    => throw new NotSupportedException("Joins are not supported by this provider.");
```

### 2. New builder — `IJoinQuery<...>`

New files under `src/Shiny.DocumentDb/`:

- `IJoinQuery.cs` — one-, two-, three-argument fluent builder.
- `JoinKind.cs` — `Inner`, `Left`.

```csharp
public interface IJoinQuery<TLeft> where TLeft : class
{
    IJoinQuery<TLeft> Where(Expression<Func<TLeft, bool>> predicate);
    IJoinQuery<TLeft, TRight> Join<TRight>(
        Expression<Func<TLeft, TRight, bool>> on,
        JoinKind kind = JoinKind.Inner) where TRight : class;
}

public interface IJoinQuery<TLeft, TRight> where TLeft : class where TRight : class
{
    IJoinQuery<TLeft, TRight> Where(Expression<Func<TLeft, TRight, bool>> predicate);
    IJoinQuery<TLeft, TRight> OrderBy<TKey>(Expression<Func<TLeft, TRight, TKey>> key);
    IJoinQuery<TLeft, TRight> OrderByDescending<TKey>(Expression<Func<TLeft, TRight, TKey>> key);
    IJoinQuery<TLeft, TRight> Paginate(int offset, int take);

    // Add a third side.
    IJoinQuery<TLeft, TRight, TThird> Join<TThird>(
        Expression<Func<TLeft, TRight, TThird, bool>> on,
        JoinKind kind = JoinKind.Inner) where TThird : class;

    // Terminate — the projector runs server-side where supported, otherwise
    // client-side after materialization (same rule as Select on IDocumentQuery).
    IDocumentQuery<TResult> Select<TResult>(
        Expression<Func<TLeft, TRight, TResult>> selector,
        JsonTypeInfo<TResult>? resultTypeInfo = null) where TResult : class;

    Task<IReadOnlyList<TResult>> ToList<TResult>(
        Expression<Func<TLeft, TRight, TResult>> selector,
        CancellationToken ct = default) where TResult : class;
}
```

Rationale for reusing `IDocumentQuery<TResult>` on `Select`: the caller gets
`Paginate`, `OrderBy`, `ToList`, `Count`, `Any`, `ToCursorPage`, `ToJsonList`,
`RawJsonRows` for free — all downstream operations work exactly like a
single-type query.

### 3. Capability record

Extend `Configuration/DocumentStoreCapabilities.cs`:

```csharp
/// <summary>Cross-type JOIN queries.</summary>
public bool Joins { get; init; }
```

And wire it through `DocumentStoreOptions.Capabilities` from the provider's own
`SupportsJoins`, exactly as `Temporal`/`Vector`/`FullText` are wired today.

### 4. Provider hook — `IDatabaseProvider`

Add to `IDatabaseProvider.cs` (near `SupportsTemporal`, `SupportsSpatial`,
`SupportsFullText`):

```csharp
bool SupportsJoins => false;

/// <summary>
/// Emits the SQL for a join plan. Called with a normalized
/// <see cref="JoinPlan"/> produced by the core lowerer.
/// </summary>
string BuildJoinSql(JoinPlan plan, IList<QueryParameter> parameters)
    => throw new NotSupportedException();
```

MongoDB (and any other non-SQL provider) implements the plan directly through
its own hook set (`$lookup`) rather than `BuildJoinSql`.

---

## Core IR / lowering

New nodes in `src/Shiny.DocumentDb/Internal/Query/QueryNodes.cs`:

- `JoinNode(JoinKind Kind, Type LeftType, Type RightType, PredicateNode On, ValueNode? Filter, IReadOnlyList<OrderNode> Order, PageNode? Page, ProjectionNode Projection)`
- `AliasedFieldNode(int SideIndex, string Path, Type ClrType)` — the existing
  `DocumentFieldExpression` gets an alias/side prefix so the SQL emitter can
  render `left.data ->> 'x'` vs `right.data ->> 'y'`.

`ExpressionInterpreter` grows a two-parameter overload (and later three-param)
that walks a `(TLeft, TRight) => bool` / `(TLeft, TRight) => new {...}` lambda,
labels each parameter with its side index, and produces `JoinNode`.

`SqlPredicateEmitter` is extended so a `AliasedFieldNode` renders with the
correct table alias. This is a mechanical change — the emitter already routes
paths through a single "emit a field ref" callback.

---

## String-expression grammar parity

Per repo convention (query-surface parity): the string API must accept the same
join shape.

New helper (kept small on purpose):

```csharp
store.Join<Order>()
    .Join<Customer>("l.customerId = r.id")
    .Where("l.total > 100 and r.region = 'EMEA'")
    .Select("l.id as orderId, r.name as customer, l.total");
```

Implementation: extend `FilterExpressionParser` with a side qualifier (`l.` /
`r.` / a caller-supplied alias) and route the resulting nodes through the same
`AliasedFieldNode` used by the LINQ path. The `Select("…")` variant returns
`IDocumentQuery<JsonObject>` the same way `Project` does today.

---

## Global query filters

Every joined side must still have its registered global query filters applied
(same rule as `IDocumentQuery.Where`). The join builder honors
`IgnoreQueryFilters()` per-side; ignoring on the root disables the root's
filters only, matching the existing single-type semantics.

---

## Diagnostics

`ToQueryString()` on the join-produced `IDocumentQuery<TResult>` returns the
composed SQL (relational, DuckDB) or the aggregation pipeline (MongoDB).
Providers that fall back to a client-side merge throw, matching today's
behavior for in-memory/document providers on `ToQueryString`.

Existing `ActivitySource` / `Meter` counters get one new tag
(`documentdb.join.sides`) so the join count is observable — no new source or
meter is introduced.

---

## Validation (`DocumentConfigurationValidator`)

Nothing at v1: joins are opt-in per call. If, later, a feature is added that
requires joins at startup (e.g. a mapped denormalized read model), that
feature's validator reads `capabilities.Joins` and fails fast the same way
`Vector`/`FullText` do today.

---

## Work breakdown (in order)

1. **Core contracts** — `SupportsJoins` on `IDocumentStore` +
   `IDatabaseProvider`, `Joins` on `DocumentStoreCapabilities`, wiring in
   `DocumentStoreOptions`.
2. **Builder + IR** — `IJoinQuery<...>`, `JoinKind`, `JoinNode`,
   `AliasedFieldNode`. Two-parameter overload of `ExpressionInterpreter`.
3. **SQL emit** — extend `SqlPredicateEmitter` for aliased fields; add
   `BuildJoinSql` default that composes `SELECT … FROM {left} l JOIN {right} r
   ON …`. Providers override only when their JSON-extraction dialect differs
   (SQL Server `OPENJSON`, Oracle `JSON_VALUE`, etc.).
4. **Provider enablement** — flip `SupportsJoins => true` on: SQLite, DuckDB,
   Postgres, MySQL, MariaDb, CockroachDb, SqlServer, Oracle.
5. **MongoDB** — implement via `$lookup` aggregation. Emits its own plan
   translator (not `BuildJoinSql`).
6. **LiteDb (opt-in)** — in-memory join keyed off `SupportsJoins` and a small
   `JoinOptions.AllowInMemoryJoin` flag, off by default. Documented as O(n·m)
   and never for large collections.
7. **String grammar parity** — extend `FilterExpressionParser` with side
   qualifiers; add `Join(string on)` / `Select(string projection)` string
   overloads. Cover both LINQ + string surfaces in tests.
8. **Query-string diagnostics** — `ToQueryString` for relational + MongoDB
   join queries.
9. **Tests**
   - Unit: interpreter, node lowering, SQL emitter alias, string parser side qualifiers.
   - Integration (per provider that reports `SupportsJoins == true`):
     - inner join with equality key
     - left join preserving unmatched left rows
     - three-way join
     - `Where` combining left + right predicates
     - `OrderBy` across sides + `Paginate`
     - `ToCursorPage`
     - string-grammar equivalents
   - Negative: `SupportsJoins == false` providers throw at
     `store.Join<T>()` call.
   - Full-suite run required (per repo convention) — Docker must be up.

10. **Docs + skill + readme**
    - `~/Desktop/dev/documentation/src/content/docs/documentdb/querying.mdx`
      gains a "Joins" section with LINQ + string examples and the provider
      matrix.
    - New `<RN type="feature">` line under the current
      `## <version> TBD` heading of `release-notes.mdx`.
    - `skills/shiny-documentdb/SKILL.md` — add `Join<T>` and the join grammar
      to the trigger list and default guidance; add a "when NOT to join"
      note (Cosmos, IndexedDb, key-partitioned stores).
    - `readme.md` — add JOIN to the feature list.

---

## Explicit non-goals for v1

- No implicit navigation properties or `[ForeignKey]`-style attributes.
- No change tracking or graph writes across joined types.
- No cross-tenant / cross-partition joins on Cosmos.
- No `GroupJoin` / hierarchical projection — a group-by after join can be
  achieved with the existing `GroupBy` on the projected `IDocumentQuery`.
- No auto-index creation on join keys — surfaced via a docs guidance note
  (map an index on the FK path the way you would today).
