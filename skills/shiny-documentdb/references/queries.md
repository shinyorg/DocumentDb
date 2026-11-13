# Queries, projections, aggregates, pagination, and filters

Read only the sections relevant to the current task. These are application-facing API details.

## Contents

- Scalar functions in `Where` predicates
- Fluent Query Builder (IDocumentQuery<T>)
- Pagination
- Expression Query Patterns
- Projections
- Ordering
- Scalar Aggregates
- Grouped Aggregation (GROUP BY)
- Streaming
- Index Management
- Global Query Filters

## Scalar functions in `Where` predicates

The relational providers (SQLite, SQLCipher, DuckDB, MySQL, SQL Server, PostgreSQL, Oracle) translate these to native SQL. LiteDB/IndexedDB evaluate them in-memory, so they always work there too.

- **String**: `s.ToLower()`/`ToUpper()`, `s.Length`, `s.Trim()`/`TrimStart()`/`TrimEnd()`, `s.Substring(start[, len])`, `s.Replace(a, b)`, `s.IndexOf(x)`, `string.IsNullOrEmpty(s)`, `a + b`, plus the existing `Contains`/`StartsWith`/`EndsWith`.
- **Math**: `Math.Abs/Round/Ceiling/Floor/Sqrt/Pow/Sign`. (`Ceiling`/`Floor`/`Sqrt`/`Pow` need the SQLite math extension; `Abs`/`Round` are always available.)
- **Flag enums** (stored numerically — the default): `x.Permissions.HasFlag(Permissions.Write)` or `(x.Permissions & Permissions.Write) == Permissions.Write`. Both lower to the same bitwise test (`BITAND` on Oracle) on the relational providers and Cosmos; MongoDB translates `HasFlag` to `$bitsAllSet`. Do **not** enable `JsonStringEnumConverter` if you need to query flags — bitwise tests require the numeric representation.
- **String-stored (non-flag) enums**: plain enum `==`/`!=`/`in` comparisons work whether the enum is stored numerically (default) or as a string via `JsonStringEnumConverter` — the query layer binds the exact member name the converter persisted, on both the LINQ and string surfaces (`Where(x => x.Level == Priority.High)` and `Where("Level == 'High'")`). Only flag enums require numeric storage.
- **Phonetic**: `DocumentFunctions.Soundex(x.Name)` → native `SOUNDEX()` (SQL Server/MySQL/Oracle) or a registered connection UDF (SQLite). Not translatable on Cosmos/Mongo — compute a stored Soundex field there instead.

```csharp
await store.Query<Account>().Where(a => a.Name.ToLower() == "alice").ToList();
await store.Query<Account>().Where(a => a.Permissions.HasFlag(Permissions.Write)).ToList();
await store.Query<Account>().Where(a => DocumentFunctions.Soundex(a.Name) == DocumentFunctions.Soundex("Smith")).ToList();
```

## Fluent Query Builder (IDocumentQuery<T>)

The fluent query builder is the primary way to query documents. Start with `store.Query<T>()` and chain builder methods, then terminate with a materialization method.

**Builders are immutable (12.0+, every provider).** Each builder call returns a **copy** — assign the result. `q.Where(...)` as a bare statement is discarded (the relational stores mutated in place before 12.0; the document stores never did):

```csharp
var q = store.Query<User>();
q = q.Where(x => x.Age >= 18);        // correct
// q.Where(x => x.Age >= 18);         // WRONG — discarded
```

**Composing after `Select`/`Project` throws `NotSupportedException`** on every provider (12.0 unified this; the relational stores threw `InvalidOperationException` before).

### Builder Methods (non-executing, return IDocumentQuery<T>)

| Method | Description |
|--------|-------------|
| `.Where(predicate)` | Filter by LINQ expression. Multiple calls combine with AND. |
| `.Where(filter[, jsonTypeInfo])` | Filter by a runtime filter string (e.g. `"Age >= 30 and Status == 'open'"`) — AOT-safe. `and`/`or`/`not`, comparisons, `is [not] null`, `in (…)`, `contains/startsWith/endsWith`. |
| `.WhereIn(selector, values[, nulls])` / `.WhereNotIn(selector, values[, nulls])` | Set-membership filter (`IN` / `NOT IN`) from an in-memory collection. The collection is lowered to the store's native construct (`IN` / `$in`), not expanded into the filter text. `nulls` is a `NullHandling` (`Ignore` default / `Raw` / `Match`). Empty set ⇒ `WhereIn` matches nothing, `WhereNotIn` matches everything. Also takes a string property name overload. |
| `.OrderBy(selector)` / `.OrderByDescending(selector)` | Sort by property (expression). |
| `.OrderBy(name[, jsonTypeInfo])` / `.OrderByDescending(name[, jsonTypeInfo])` | Sort by property name (string) — AOT-safe via `JsonTypeInfo<T>`. Supports dotted paths. |
| `.OrderBy(name, direction[, jsonTypeInfo])` | Sort by property name with a runtime direction string (`asc`/`ascending`/`desc`/`descending`, case-insensitive; empty → ascending). |
| `.GroupBy(keySelector)` | Group into one row per key for an aggregate projection (`.Select(g => …)` with `g.Key` + `g.Count()`/`g.Sum(x => x.P)`). |
| `.Join<TRight>(on[, kind, rightTypeInfo])` / `.Join<TRight>(leftAlias, rightAlias, on[, kind])` | Start a two-type join → `IJoinQuery<T, TRight>` (see [Joins](#joins-querytjointright-140-relational--mongodb)). Relational providers + MongoDB. |
| `.Paginate(offset, take)` | Limit results with SQL LIMIT/OFFSET. |
| `.Select(selector, resultTypeInfo?)` | Project into a different shape via `json_object`. |
| `.Project(fields[, jsonTypeInfo])` | Project a runtime-chosen field list (e.g. `"name,email"`) into `IDocumentQuery<JsonObject>` — AOT-safe. For REST sparse fieldsets; no DTO required. Supports scalar functions with an alias (`"lower(email) as email"`) on every provider. |

### Terminal Methods (execute SQL)

| Method | Returns | Description |
|--------|---------|-------------|
| `.ToList()` | `Task<IReadOnlyList<T>>` | Materialize all results into a list. |
| `.ToAsyncEnumerable()` | `IAsyncEnumerable<T>` | Stream results one-at-a-time. |
| `.Count()` | `Task<long>` | Count matching documents. |
| `.Any()` | `Task<bool>` | Check if any documents match. |
| `.First()` / `.FirstOrDefault()` | `Task<T>` / `Task<T?>` | First match. **Use these instead of `(await ToList())[0]`** — the row limit is pushed to the provider (`LIMIT 1`). `First` throws when nothing matched; `FirstOrDefault` returns null. Predicate and string-filter overloads exist: `.First(u => u.Age == 40)`, `.First("status == 'open'")`. |
| `.Single()` / `.SingleOrDefault()` | `Task<T>` / `Task<T?>` | The only match; throws when a second one matches (two rows are fetched to detect it, no extra round trip). |
| `.ExecuteDelete()` | `Task<int>` | Delete matching documents. Returns count. |
| `.ExecuteUpdate(property, value)` | `Task<int>` | Update a property on all matching documents via `json_set()`. Returns count. |
| `.ExecuteUpdate(build)` | `Task<int>` | Update **several** properties in ONE statement: `.ExecuteUpdate(b => b.Set(o => o.Status, "x").Set(o => o.ClosedAt, now))`. Prefer it over consecutive single-property calls — one statement, one predicate evaluation, atomic. Same property twice throws. |
| `.Max(selector)` | `Task<TValue>` | Maximum value of a property. |
| `.Min(selector)` | `Task<TValue>` | Minimum value of a property. |
| `.Sum(selector)` | `Task<TValue>` | Sum of a property. |
| `.Average(selector)` | `Task<double>` | Average of a property. |
| `.PageResult(page, pageSize, zeroBased?)` | `Task<PagedResults<T>>` | Run the query and return records + total count in one envelope. 1-based by default. |
| `.ToCursorPage(cursor, take)` | `Task<CursorPage<T>>` | One forward seek/keyset page. `null` cursor = first page; `NextCursor` null = last page. See Pagination. |
| `.ToCursorStream(pageSize?)` | `IAsyncEnumerable<T>` | Walk every cursor page automatically — resumable full scan, no deep-offset cost. |
| `.ToQueryString()` | `DocumentQueryString` | Build the query the configuration **would** run **without executing it** — for debugging/logging. See below. |

### JSON terminals — results as raw JSON instead of `T`

Same typed builder, different materialization. Generate these whenever the document is read only to be written back out (an ASP.NET endpoint returning stored documents), so nothing is deserialized just to be re-serialized.

| Method | Returns | Typed twin |
|--------|---------|-------------|
| `.ToJsonList()` | `Task<IReadOnlyList<JsonObject>>` | `.ToList()` |
| `.ToJsonAsyncEnumerable()` | `IAsyncEnumerable<JsonObject>` | `.ToAsyncEnumerable()` |
| `.FirstJson()` / `.FirstOrDefaultJson()` | `Task<JsonObject>` / `Task<JsonObject?>` | `.First()` / `.FirstOrDefault()` |
| `.SingleJson()` / `.SingleOrDefaultJson()` | `Task<JsonObject>` / `Task<JsonObject?>` | `.Single()` / `.SingleOrDefault()` |
| `.FirstOrDefaultRawJson()` | `Task<string?>` | — (no parse at all) |
| `.ToRawJsonAsyncEnumerable()` | `IAsyncEnumerable<string>` | — |
| `.WriteJsonArrayTo(stream)` | `Task<int>` (rows written) | — |
| `.ToJsonCursorPage(cursor, take)` | `Task<CursorPage<JsonObject>>` | `.ToCursorPage()` |
| `.RawJsonRows(maxRows)` | `IAsyncEnumerable<string>` | the primitive the rest are built on |
| `.SupportsRawJson` | `bool` | whether this query can use the lane at all |

```csharp
// one document — hand the stored body straight to the client, no parse
var raw = await store.Query<Order>().Where(o => o.Id == id).FirstOrDefaultRawJson();
return raw is null ? Results.NotFound() : Results.Content(raw, "application/json");

// a whole list — one JSON array written straight to the response, never buffered
ctx.Response.ContentType = "application/json";
await store.Query<Order>()
    .Where(o => o.Status == "open")
    .OrderByDescending(o => o.CreatedAt)
    .WriteJsonArrayTo(ctx.Response.Body, ct);
```

Rules that matter when generating code:

- **Terminals only — build first, then take JSON.** `Where` / `OrderBy` / `Paginate` / `IgnoreQueryFilters` all still apply and are all typed; there is no string-grammar building after a JSON terminal (that is `store.Collection(...)`).
- **Works on every provider.** Relational + Cosmos hand back the persisted body untouched; everywhere else the provider materializes `T` and re-serializes through the type's `JsonTypeInfo` — same JSON, same API, but no saving. Don't promise a perf win on MongoDB/LiteDB/IndexedDB/Redis/RavenDB/Firestore/AzureTable/DynamoDB.
- **Encrypted types throw `NotSupportedException`** — the stored body holds ciphertext and only the typed terminals decrypt. Never generate a raw terminal for a type with `cfg.MapProperty(x => x.P, p => p.Encrypt())`.
- **When JSON is an optimization and the typed path is also correct, probe — don't catch.** `query.SupportsRawJson` is `false` for an encrypted type and after `Select`/`Project`/`GroupBy`; generate `query.SupportsRawJson ? await query.ToJsonList(ct) : <typed path>`. That is what the built-in OData engine and the AI `query` tool do. Generate a bare raw terminal (no probe) only when the raw JSON *is* the requirement.
- **Shape differs from the object** — materialized computed properties live outside the body and blob payloads are metadata envelopes, so neither appears. Use the typed terminals when you need them.
- **`WriteJsonArrayTo` writes `[]` when nothing matches** and returns the row count; set `Response.ContentType` yourself.
- **Not on projections.** After `Select` / `Project` / `GroupBy` the raw terminals throw — the result is no longer a stored document. `Project(...)` already returns `IDocumentQuery<JsonObject>`; use its `ToList()`.

### Inspecting the generated query — `.ToQueryString()`

`.ToQueryString()` returns a `DocumentQueryString { string Sql; IReadOnlyDictionary<string, object?> Parameters; }` describing the `ToList()` form of the query without executing it. Its `ToString()` renders the parameter values as a `-- @name=value` comment header above the SQL. Works for both LINQ and string-expression `Where` (same pipeline) and includes `Where`/`OrderBy`/`Paginate`/`Select`/`Project`.

```csharp
var qs = store.Query<User>().Where(u => u.Age > 28).ToQueryString();
qs.Sql;          // "SELECT Data FROM ... WHERE TypeName = @typeName AND (json_extract(Data, '$.age') > @p0);"
qs.Parameters;   // { ["@typeName"] = "User", ["@p0"] = 28 }
Console.WriteLine(qs); // comment header + SQL
```

Provider support: relational providers (SQLite, SQL Server, PostgreSQL, MySQL, Oracle, DuckDB) and Cosmos return SQL + parameters; MongoDB returns its rendered BSON filter (or full find command) as JSON with empty `Parameters`. LiteDB and IndexedDB — and client-side projections after `Select`/`Project` on the document providers — throw `NotSupportedException`.

### Common Patterns

```csharp
// Get all documents of a type
var users = await store.Query<User>().ToList();

// Filter
var results = await store.Query<User>()
    .Where(u => u.Age > 25)
    .ToList();

// Set membership (IN / NOT IN) from an in-memory collection
var statuses = new[] { "Open", "Pending", "Review" };
var open = await store.Query<Order>()
    .WhereIn(o => o.Status, statuses)
    .ToList();

// NOT IN, treating a null in the set as "also exclude null fields"
var assigned = await store.Query<Order>()
    .WhereNotIn(o => o.AssignedTo, new string?[] { "alice", null }, NullHandling.Match)
    .ToList();

// Filter + sort
var results = await store.Query<User>()
    .Where(u => u.Age > 25)
    .OrderBy(u => u.Name)
    .ToList();

// Filter + sort + paginate
var page = await store.Query<User>()
    .Where(u => u.Age > 25)
    .OrderBy(u => u.Name)
    .Paginate(0, 20)
    .ToList();

// Stream results
await foreach (var user in store.Query<User>()
    .Where(u => u.Age > 25)
    .OrderByDescending(u => u.Age)
    .ToAsyncEnumerable())
{
    Console.WriteLine(user.Name);
}

// Count
var count = await store.Query<User>()
    .Where(u => u.Age > 25)
    .Count();

// Check existence
var any = await store.Query<User>()
    .Where(u => u.Name == "Alice")
    .Any();

// Delete matching documents
int deleted = await store.Query<User>()
    .Where(u => u.Age < 18)
    .ExecuteDelete();

// Update a property on matching documents
int updated = await store.Query<User>()
    .Where(u => u.Age < 18)
    .ExecuteUpdate(u => u.Age, 18);

// Update a nested property
int updated = await store.Query<Order>()
    .Where(o => o.ShippingAddress.City == "Portland")
    .ExecuteUpdate(o => o.ShippingAddress.City, "Eugene");

// Several properties in one statement (preferred over consecutive ExecuteUpdate calls)
int expired = await store.Query<Order>()
    .Where(o => o.Status == "open" && o.CreatedAt < cutoff)
    .ExecuteUpdate(b => b
        .Set(o => o.Status, "expired")
        .Set(o => o.ClosedAt, DateTimeOffset.UtcNow));

// One document — never materialize a list to take its first element
var youngest = await store.Query<User>().OrderBy(u => u.Age).FirstOrDefault();
var byEmail  = await store.Query<User>().Single(u => u.Email == email);

// Scalar aggregates
var maxAge = await store.Query<User>().Max(u => u.Age);
var minAge = await store.Query<User>().Where(u => u.Name != "Admin").Min(u => u.Age);
var totalAge = await store.Query<User>().Sum(u => u.Age);
var avgAge = await store.Query<User>().Average(u => u.Age);
```

### Joins (`Query<T>().Join<TRight>(…)`, 14.0+, relational + MongoDB)

Two document types in one engine query. Generate it when the user needs data from two types together (orders with
their customer's name, a report across types); keep embedding for data that always travels with the document.

```csharp
var rows = await store.Query<Order>()
    .Where(o => o.Status == "open")                               // left side only
    .Join<Customer>((o, c) => o.CustomerId == c.Id)                 // JoinKind.Inner by default
    .Where((o, c) => c.Region == "eu" && o.Total > c.CreditLimit)   // either side, or across both
    .OrderBy((o, c) => c.Name)
    .Paginate(0, 50)
    .Select((o, c) => new { o.Id, Customer = c.Name, o.Total })     // any shape; anonymous is fine here
    .ToList();

// Left join: the right document is null when nothing matched — guard it.
var withCustomer = await store.Query<Order>()
    .Join<Customer>((o, c) => o.CustomerId == c.Id, JoinKind.Left)
    .Select((o, c) => new { o.Id, Customer = c == null ? null : c.Name })
    .ToList();

// String syntax — every field alias-qualified. A LINQ join's parameter names are its aliases too.
var json = await store.Query<Order>()
    .Join<Customer>("o", "c", "o.customerId = c.id")
    .Where("c.region = 'eu' and o.total > c.creditLimit")
    .OrderBy("c.name")
    .Project("o.id as orderId, c.name as customer, o.total")     // IJoinResult<JsonObject>
    .ToList();
```

Rules when generating join code:
- `Join` returns `IJoinQuery<TLeft, TRight>`; finish with `Select((o, c) => …)` or `Project("…")`, then `ToList`,
  `ToAsyncEnumerable`, `Count`, `Any`, `First`, `FirstOrDefault` or `ToQueryString`. A join has no cursor paging,
  `ExecuteUpdate`/`ExecuteDelete`, `NotifyOnChange` or raw JSON terminals.
- Call `OrderBy`/`Paginate` **after** `Join` — before it they throw. `Where` on the source query is fine and filters the left side.
- `Select` runs over the materialized documents, so anonymous types, scalars and encrypted properties all work (unlike the
  relational single-type `Select`, which needs a named DTO). On a left join guard the right parameter
  (`c == null ? … : …`) — unguarded access throws `InvalidOperationException`; string `Project` yields `null` for the missing side.
  Find unmatched rows with `.Where((o, c) => c == null)`.
- Query filters (soft delete) and shared-table tenancy apply per side; the right side's are part of the join condition, so a
  left join keeps its rows. `IgnoreQueryFilters()` on the join lifts both sides.
- Never join on an encrypted property (throws `NotSupportedException`); filtering one against a constant on its own side is fine.
- No spatial/full-text `DocumentFunctions`, computed properties, JSON collections or `GroupBy` inside a join; exactly two types.
- **Providers:** SQLite, SQLCipher, DuckDB, PostgreSQL, CockroachDB, MySQL, MariaDB, SQL Server, Oracle (one SQL `JOIN`);
  MongoDB (`$lookup` — a condition across the two documents must compare two properties, ordering is by properties only,
  and a left join's condition cannot test the left document alone). Amazon DocumentDB, Cosmos DB, LiteDB, IndexedDB,
  Azure Table, DynamoDB, Firestore, Redis and RavenDB throw `NotSupportedException` — embed or run two queries there.
- The comparison grammar accepts a field on the right-hand side on single-type queries too: `Where("total > discount")`.

## Pagination

`Paginate(offset, take)` appends `LIMIT {take} OFFSET {offset}` to the generated SQL. It does not execute the query — it's a builder method that stores state until a terminal method is called.

```csharp
// First page (items 0-19)
var page1 = await store.Query<User>()
    .OrderBy(u => u.Name)
    .Paginate(0, 20)
    .ToList();

// Second page (items 20-39)
var page2 = await store.Query<User>()
    .OrderBy(u => u.Name)
    .Paginate(20, 20)
    .ToList();

// With filtering
var page = await store.Query<User>()
    .Where(u => u.Age >= 18)
    .OrderBy(u => u.Age)
    .Paginate(0, 10)
    .ToList();

// With projection
var page = await store.Query<User>()
    .OrderBy(u => u.Name)
    .Paginate(0, 10)
    .Select(u => new UserSummary { Name = u.Name, Email = u.Email })
    .ToList();

// Streaming with pagination
await foreach (var user in store.Query<User>()
    .OrderBy(u => u.Name)
    .Paginate(0, 50)
    .ToAsyncEnumerable())
{
    Console.WriteLine(user.Name);
}
```

### `PageResult` — records + total count

For UI/REST responses use `.PageResult(page, pageSize)` to materialize the page slice *and* the total matching count in a single envelope. 1-based by default; pass `zeroBased: true` for 0-based indexing.

```csharp
public record PagedResults<T>(
    IEnumerable<T> Records,
    int TotalCount,
    int Page,
    int PageSize
);

// 1-based (default)
var result = await store.Query<User>()
    .Where(u => u.Active)
    .OrderBy(u => u.Name)
    .PageResult(page: 1, pageSize: 20);

// 0-based opt-in
var result = await store.Query<User>()
    .OrderBy(u => u.Name)
    .PageResult(page: 0, pageSize: 20, zeroBased: true);
```

- `TotalCount` reflects the current `Where` predicates (and any global query filters) — pagination state is ignored when counting.
- Overrides any prior `.Paginate(...)` call on the query.
- `pageSize` must be > 0; `page` must be `>= 1` (or `>= 0` when `zeroBased: true`). Otherwise throws `ArgumentOutOfRangeException`.

### `ToCursorPage` — cursor / keyset (seek) pagination

For **infinite scroll, deep paging, or large exports**, prefer cursor paging over offset paging: it stays O(log n) per page (with an index on the sort key) and doesn't skip/duplicate rows when documents change between fetches. Pass `null` for the first page; hand the previous page's `NextCursor` back for each subsequent one; a `null` `NextCursor` marks the end. The keyset is derived from the query's `OrderBy` (an `Id` tiebreaker is appended automatically).

```csharp
string? cursor = null;
do
{
    var page = await store.Query<Order>()
        .Where(o => o.Status == "open")
        .OrderByDescending(o => o.CreatedAt)   // keyset derived from THIS OrderBy
        .ToCursorPage(cursor, take: 50);       // CursorPage<T> { Items, NextCursor, HasMore }

    Render(page.Items);
    cursor = page.NextCursor;                  // null ⇒ last page
}
while (cursor != null);

// Or walk every page automatically (resumable full scan, no deep-offset cost):
await foreach (var o in store.Query<Order>().OrderByDescending(x => x.CreatedAt).ToCursorStream(pageSize: 200))
    Export(o);
```

- **Choose offset (`PageResult`) when you need a page number or a total count**; choose cursor when you only move forward, page deep, or forever-scroll.
- A cursor is valid only for the **exact same `OrderBy` + filters** that produced it — reusing it under a different sort throws `InvalidOperationException` (a shape hash catches it). Not valid after `Select`/`Project`/`GroupBy` (throws `NotSupportedException`). `take` must be `> 0` and `≤ 10,000`.
- Index the sort key (`cfg.MapIndexedProperty`) for hot cursor paths, and order by a **non-nullable** column (a `NULL` sort value at a page boundary can skip rows).
- **Provider tier:** relational providers seek server-side; LiteDB/IndexedDB/MongoDB page the keyset client-side; Cosmos/DynamoDB/Azure Table throw `NotSupportedException` (not yet supported).

### Dynamic sort columns (string-based OrderBy)

When the sort column is determined at runtime (e.g. a column-header click, a `?sort=` query string), use the string-based overloads. They are AOT-safe: resolution walks `JsonTypeInfo.Properties` and synthesizes an `Expression.Property(parameter, PropertyInfo)` tree — no `Type.GetProperty(string)` reflection on `T`, no `Expression.Compile()`.

> **`jsonTypeInfo` is optional** on every string overload (`Where`, `OrderBy`, `OrderByDescending`, `Project`). When omitted, the query reuses the `JsonTypeInfo<T>` it resolved at creation (from `Query(ctx.User)` or the registered context), so `store.Query(ctx.User).OrderBy("Name")` works without re-passing it.

```csharp
// Sort by CLR name
var results = await store.Query<User>().OrderBy("Name", ctx.User).ToList();

// Or by JSON name (after the naming policy)
var results = await store.Query<User>().OrderBy("name", ctx.User).ToList();

// Descending
var results = await store.Query<User>().OrderByDescending("Age", ctx.User).ToList();

// Dotted path for nested properties
var orders = await store.Query<Order>().OrderBy("ShippingAddress.City", ctx.Order).ToList();

// Driven by an external value
string sort = request.Query["sort"]; // e.g. "Name", "Age", "ShippingAddress.City"
var results = await store.Query<User>()
    .Where(u => u.Active)
    .OrderBy(sort, ctx.User)
    .ToList();

// Direction as a runtime string too (e.g. "?sort=name&dir=desc").
// Accepts "asc"/"ascending"/"desc"/"descending" (case-insensitive);
// an empty/null/whitespace direction defaults to ascending.
string dir = request.Query["dir"];
var results = await store.Query<User>()
    .OrderBy(sort, dir, ctx.User)
    .ToList();
```

Matching rules:
- Case-insensitive match against either the CLR property name (`PropertyInfo.Name`) or the JSON property name (`JsonPropertyInfo.Name` after the naming policy).
- Dotted segments traverse nested types; each nested type must also be registered in your `JsonSerializerContext`.
- Unknown segments throw `ArgumentException`. Null / empty / whitespace paths throw `ArgumentNullException` / `ArgumentException`.
- The `OrderBy(name, direction, jsonTypeInfo)` overload parses the direction string and delegates to the `OrderBy` / `OrderByDescending` string overloads — same AOT-safe resolution. An unrecognized direction throws `ArgumentException`.

### Dynamic filter strings (string-based Where)

When the filter is supplied at runtime (a REST `?filter=`, a saved view, an admin search), use `Where(string, JsonTypeInfo<T>)`. It parses a small expression language into the same expression tree a compiled predicate produces, so it runs through the normal translator and stays AOT/trim-safe (no `Compile()`; fields resolved through `JsonTypeInfo`).

```csharp
var open = await store.Query<User>()
    .Where("Age >= 30 and Status == 'open'", ctx.User)
    .ToList();

// Combines with compiled predicates
var results = await store.Query<User>()
    .Where(u => u.Active)
    .Where(request.Filter, ctx.User)
    .ToList();
```

Grammar:
- `and` / `or` / `not` and parentheses.
- Comparisons `==` (or `=`), `!=` (or `<>`), `>`, `>=`, `<`, `<=`. Relational ops are rejected for `string`/`bool`/`Guid`.
- `field is null` / `field is not null` (and `field == null`).
- `field in (a, b, c)`.
- String functions `contains(field, 'x')`, `startsWith(field, 'x')`, `endsWith(field, 'x')`.
- Field names follow the string-`OrderBy` rules (case-insensitive CLR/JSON name, dotted paths). String literals use single/double quotes; double the quote to escape. Literals are coerced to the field's CLR type. Syntax errors / unknown fields throw `ArgumentException`.

When the filter *shape* is fixed but its *values* come from code, prefer the interpolated overload `Where(FilterInterpolatedStringHandler, JsonTypeInfo<T>)` — write `Where($"…")`. Each `{value}` hole is captured as a typed argument and bound as a parameter (never formatted into the text), so **don't quote interpolated string values and don't build the filter with string concatenation** — that reintroduces the injection/quoting problems this overload removes.

```csharp
var status = request.Query["status"];
var minAge = 30;

var open = await store.Query<User>()
    .Where($"Age >= {minAge} and Status == {status}", ctx.User)  // {status} needs no quotes; injection-safe
    .ToList();
```

- An interpolated `$"..."` literal binds to this overload; a plain `string` variable binds to the raw `Where(string)` overload. So pass the raw `?filter=` text as a `string` to parse it, and use `$"..."` only to inject values.
- Holes are valid only where a literal would appear — comparison RHS, `in (...)` list, or string-function argument (`contains(Email, {fragment})`) — never as a field name. Values coerce to the field's CLR type; a `null` value becomes an `is null` check.

### Runtime field projection (string-based Project)

`Project(fields, JsonTypeInfo<T>)` selects a runtime-chosen field list and returns `IDocumentQuery<JsonObject>` — no DTO needed. Ideal for REST sparse fieldsets (`?fields=name,email`).

```csharp
IReadOnlyList<JsonObject> rows = await store.Query<User>()
    .Where("Age >= 30", ctx.User)
    .OrderBy("Name", ctx.User)
    .Project("Name, Email", ctx.User)
    .ToList();

var name = rows[0]["name"]!.GetValue<string>();

// Pagination / Count / Any / streaming work on the projected query.
var page = await store.Query<User>().Project("name,email", ctx.User).PageResult(1, 20);

// Scalar functions are allowed and require an alias.
var shaped = await store.Query<User>()
    .Project("name, lower(email) as email, length(name) as len, year(created) as yr", ctx.User)
    .ToList();
```

- Relational providers emit `json_object('name', json_extract(Data,'$.name'), …)`; CosmosDB/MongoDB/LiteDB/IndexedDB project client-side via the compile-free interpreter. Supported on **every** provider.
- Output keys are the **leaf JSON name** (`ShippingAddress.City` → `city`) unless overridden with `as alias`; functions require an alias. Duplicate keys throw `ArgumentException`.
- Functions are the same set as the string `Where` grammar (`lower`/`upper`/`length`/`trim`/`substring`/`replace`/`indexof`, `abs`/`round`/`ceiling`/`floor`/`sqrt`/`sign`, `year`/`month`/`day`/…, `soundex`).
- After `Project` the query is terminal-shaped: `ToList`/`ToAsyncEnumerable`/`Count`/`Any`/`Paginate` work; `Where`/`OrderBy`/`Select`/aggregates throw.

## Expression Query Patterns

The expression visitor translates LINQ expressions to `json_extract` SQL. Property names are resolved from `JsonTypeInfo` metadata, so `[JsonPropertyName]` and naming policies are respected.

### Equality and Comparisons

```csharp
u => u.Name == "Alice"       // json_extract(Data, '$.name') = @p0
u => u.Age > 25              // json_extract(Data, '$.age') > @p0
u => u.Age <= 25             // json_extract(Data, '$.age') <= @p0
```

### Logical Operators

```csharp
u => u.Age == 25 && u.Name == "Alice"          // (... AND ...)
u => u.Name == "Alice" || u.Name == "Bob"      // (... OR ...)
u => !(u.Name == "Alice")                       // NOT (...)
```

### Null Checks

```csharp
u => u.Email == null          // ... IS NULL
u => u.Email != null          // ... IS NOT NULL
```

### String Methods

```csharp
u => u.Name.Contains("li")       // ... LIKE '%' || @p0 || '%'
u => u.Name.StartsWith("Al")     // ... LIKE @p0 || '%'
u => u.Name.EndsWith("ob")       // ... LIKE '%' || @p0
```

### Nested Object Properties

```csharp
o => o.ShippingAddress.City == "Portland"
// json_extract(Data, '$.shippingAddress.city') = @p0
```

### Collection Queries with Any()

```csharp
// Object collection — filter by child property
o => o.Lines.Any(l => l.ProductName == "Widget")
// EXISTS (SELECT 1 FROM json_each(...) WHERE ...)

// Primitive collection — filter by value
o => o.Tags.Any(t => t == "priority")
// EXISTS (SELECT 1 FROM json_each(...) WHERE value = @p0)

// Check if collection has any elements
o => o.Tags.Any()
// json_array_length(Data, '$.tags') > 0
```

### Collection Queries with Count()

```csharp
// Count elements (no predicate)
o => o.Lines.Count() > 1
// json_array_length(Data, '$.lines') > 1

// Count matching elements (with predicate)
o => o.Lines.Count(l => l.Quantity >= 3) >= 1
// (SELECT COUNT(*) FROM json_each(...) WHERE ...) >= 1

// Property form — collection .Count / array .Length map to the same
// array-length function as .Count() (works on every provider)
o => o.Lines.Count == 0
o => o.Tags.Count > 1
// json_array_length(Data, '$.lines') = 0   /   ... > 1
```

`string.Length` and dictionary `.Count` are **not** array lengths and throw `NotSupportedException` (instead of silently generating a dead query) — use `.Count()` / `.Any()` for collection length.

### DateTime and DateTimeOffset

Values are formatted as ISO 8601 to match `System.Text.Json` output:

```csharp
var cutoff = new DateTime(2025, 1, 1, 0, 0, 0, DateTimeKind.Utc);
e => e.StartDate > cutoff

var start = new DateTimeOffset(2025, 1, 1, 0, 0, 0, TimeSpan.Zero);
e => e.CreatedAt >= start && e.CreatedAt < end
```

### Captured Variables

```csharp
var targetName = "Alice";
u => u.Name == targetName    // Extracted from closure at translate time
```

## Projections

Project into DTOs at the SQL level via `json_object` — no full document deserialization needed. Use `.Select()` on the query builder.

### Flat Projection

```csharp
var results = await store.Query<User>()
    .Where(u => u.Age == 25)
    .Select(u => new UserSummary { Name = u.Name, Email = u.Email })
    .ToList();
```

### Nested Source Properties

```csharp
var results = await store.Query<Order>()
    .Where(o => o.Status == "Shipped")
    .Select(o => new OrderSummary { Customer = o.CustomerName, City = o.ShippingAddress.City })
    .ToList();
```

### All Documents with Projection

```csharp
var results = await store.Query<Order>()
    .Select(o => new OrderDetail { Customer = o.CustomerName, LineCount = o.Lines.Count() })
    .ToList();
```

### Collection Methods in Projections

```csharp
// Count()
o => new OrderDetail { LineCount = o.Lines.Count() }
// SQL: json_array_length(Data, '$.lines')

// Count(predicate)
o => new OrderDetail { GadgetCount = o.Lines.Count(l => l.ProductName == "Gadget") }
// SQL: (SELECT COUNT(*) FROM json_each(...) WHERE ...)

// Any()
o => new OrderDetail { HasLines = o.Lines.Any() }
// SQL: CASE WHEN json_array_length(...) > 0 THEN json('true') ELSE json('false') END

// Any(predicate)
o => new OrderDetail { HasPriority = o.Tags.Any(t => t == "priority") }
// SQL: CASE WHEN EXISTS (...) THEN json('true') ELSE json('false') END

// Collection aggregates — Sum, Max, Min, Average
o => new R { TotalQty = o.Lines.Sum(l => l.Quantity) }
// SQL: (SELECT SUM(json_extract(value, '$.quantity')) FROM json_each(Data, '$.lines'))

o => new R { MaxPrice = o.Lines.Max(l => l.UnitPrice) }
// SQL: (SELECT MAX(json_extract(value, '$.unitPrice')) FROM json_each(Data, '$.lines'))
```

## Ordering

Sort results at the SQL level using the fluent `.OrderBy()` and `.OrderByDescending()` methods.

```csharp
// Ascending
var users = await store.Query<User>()
    .OrderBy(u => u.Age)
    .ToList();

// Descending
var users = await store.Query<User>()
    .OrderByDescending(u => u.Age)
    .ToList();

// With filter
var results = await store.Query<User>()
    .Where(u => u.Age > 25)
    .OrderBy(u => u.Name)
    .ToList();

// With projection
var results = await store.Query<User>()
    .OrderBy(u => u.Name)
    .Select(u => new UserSummary { Name = u.Name, Email = u.Email })
    .ToList();

// With streaming
await foreach (var user in store.Query<User>()
    .OrderByDescending(u => u.Age)
    .ToAsyncEnumerable())
{
    Console.WriteLine(user.Name);
}
```

Generated SQL: `ORDER BY json_extract(Data, '$.age') ASC`

## Scalar Aggregates

Compute Max, Min, Sum, Average across documents using terminal methods on the query builder.

```csharp
var maxAge = await store.Query<User>().Max(u => u.Age);
var minAge = await store.Query<User>().Min(u => u.Age);
var totalAge = await store.Query<User>().Sum(u => u.Age);
var avgAge = await store.Query<User>().Average(u => u.Age);

// With predicate filter
var maxAge = await store.Query<User>()
    .Where(u => u.Age < 35)
    .Max(u => u.Age);
```

## Grouped Aggregation (GROUP BY)

Use the explicit `GroupBy(keySelector).Select(g => …)` surface for a roll-up of one row per key.
Use `g.Key` for the group value and the `Sql` group aggregates — `g.Count()`, `g.Sum(x => x.Prop)`,
`g.Avg`, `g.Min`, `g.Max` — over the group's members. (For a single total over the **whole** filtered
set, prefer the scalar terminals `.Count()` / `.Sum()` / `.Average()` instead.) Aggregates are typed by the
selected member: `g.Sum`/`g.Avg` of a `decimal` keep full scale (exact on providers with a native decimal
type; SQLite aggregates as `REAL`), and `g.Min`/`g.Max` work over **dates and strings**, not just numbers.

```csharp
var rollup = await store.Query<Order>()
    .Where(o => o.CreatedAt >= since)
    .GroupBy(o => o.Status)                              // group key = a JSON property
    .Having(g => g.Sum(o => o.Total) > 10_000)          // optional: filter groups by an aggregate
    .Select(g => new StatusRollup
    {
        Status  = g.Key,                                // the group key
        Count   = g.Count(),
        Revenue = g.Sum(o => o.Total),
        AvgLine = g.Avg(o => o.Total)
    })
    .OrderByDescending(r => r.Revenue)                  // order the grouped rows (by an output column)
    .Paginate(0, 10)
    .ToList();

// Nested key:   .GroupBy(o => o.ShippingAddress.Country)
// Derived key:  .GroupBy(o => o.CreatedAt.Month)          // "revenue by month" — no stored column
// Multi-key:    .GroupBy(o => new { o.Status, o.Region }) // g.Key.Status / g.Key.Region

// String grammar (relational only): count()/sum(x)/avg(x)/min(x)/max(x) each need an alias.
var rows = await store.Query<Order>()
    .GroupBy("status")
    .Having("sum(total) > 10000")
    .Project("status, count() as orders, sum(total) as revenue")   // → JsonObject rows
    .ToList();
```

**Provider tier:** push-down on the relational providers (SQLite, SQLCipher, PostgreSQL, MySQL, SQL
Server, Oracle, DuckDB — `GROUP BY` + `HAVING` + grouped `ORDER BY` + multi/derived keys). MongoDB,
Cosmos, LiteDB and IndexedDB group **client-side** (typed surface only — no string grammar). Azure Table
and DynamoDB **throw** `NotSupportedException` (key-partitioned).

## Streaming

Use `.ToAsyncEnumerable()` instead of `.ToList()` to stream results one-at-a-time without buffering.

```csharp
// Stream all
await foreach (var user in store.Query<User>().ToAsyncEnumerable())
{
    Console.WriteLine(user.Name);
}

// Stream with filter and sort
await foreach (var user in store.Query<User>()
    .Where(u => u.Age > 30)
    .OrderBy(u => u.Name)
    .ToAsyncEnumerable())
{
    Console.WriteLine(user.Name);
}

// Stream with projection
await foreach (var summary in store.Query<Order>()
    .Where(o => o.Status == "Shipped")
    .Select(o => new OrderSummary { Customer = o.CustomerName, City = o.ShippingAddress.City })
    .ToAsyncEnumerable())
{
    Console.WriteLine($"{summary.Customer} in {summary.City}");
}

// Stream with pagination
await foreach (var user in store.Query<User>()
    .OrderBy(u => u.Name)
    .Paginate(0, 50)
    .ToAsyncEnumerable())
{
    Console.WriteLine(user.Name);
}
```

**Note:** Streaming on shared-connection providers (SQLite, SQLCipher, DuckDB) holds the per-store semaphore for the lifetime of enumeration — calling other store methods inside the same `await foreach` will block until it completes. On pooled providers (PostgreSQL, MySQL, SQL Server, Oracle) the streaming reader uses one connection from the driver pool and does not block concurrent ops on the same store, but interleaving writes can still surprise consumers expecting a stable snapshot.

## Index Management

Methods on `DocumentStore` directly (not on `IDocumentStore`) since indexes are DDL, not document CRUD. Each provider generates the appropriate index DDL for its database engine.

### Create an Index

```csharp
await store.CreateIndexAsync<User>(u => u.Name, ctx.User);
// CREATE INDEX IF NOT EXISTS idx_json_User_name
// ON documents (json_extract(Data, '$.name'))
// WHERE TypeName = 'User';
```

### Nested Property Index

```csharp
await store.CreateIndexAsync<Order>(o => o.ShippingAddress.City, ctx.Order);
```

### Drop a Specific Index

```csharp
await store.DropIndexAsync<User>(u => u.Name, ctx.User);
```

### Drop All Indexes for a Type

```csharp
await store.DropAllIndexesAsync<User>();
```

Index names are deterministic (`idx_json_{typeName}_{jsonPath}`). `CreateIndexAsync` uses `IF NOT EXISTS`, so calling it multiple times is safe.


## Global Query Filters

EF Core-style `HasQueryFilter` equivalent. Register a predicate on `DocumentStoreOptions` (or the provider-specific options class) and it's AND-applied to every query of `T` — including single-document operations, bulk operations, and per-query change monitoring. `Insert`/`BatchInsert`/`Upsert` are intentionally unfiltered (matches EF Core). Raw SQL (`Query<T>(string)` / `QueryStream<T>(string)`) is unfiltered.

### Registration

```csharp
var store = new DocumentStore(new DocumentStoreOptions
{
    DatabaseProvider = new SqliteDatabaseProvider("Data Source=mydata.db")
}
);
options.ConfigureDocument<User>(cfg => cfg.AddQueryFilter(u => !u.IsDeleted));   // unnamed
options.ConfigureDocument<Order>(cfg =>
{
    cfg.AddQueryFilter("tenant", o => o.TenantId == tenantCtx.Current);         // named
    cfg.AddQueryFilter("status", o => o.Status != "Archived");
});
```

`AddQueryFilter<T>` is available on `DocumentStoreOptions`, `LiteDbDocumentStoreOptions`, `CosmosDbDocumentStoreOptions`, `MongoDbDocumentStoreOptions`, and `IndexedDbDocumentStoreOptions`.

### Opting out per query

```csharp
// Disable every filter
var all = await store.Query<User>().IgnoreQueryFilters().ToList();

// Disable a specific named filter (others still apply)
var anyTenant = await store.Query<Order>().IgnoreQueryFilters("tenant").ToList();
```

`IgnoreQueryFilters` must be called **before** `Select(...)` — calling it on a projected query throws.

### Captured variables re-read per query

```csharp
options.ConfigureDocument<Order>(cfg => cfg.AddQueryFilter("tenant", o => o.TenantId == tenantCtx.Current));

tenantCtx.Current = "acme";
await store.Query<Order>().ToList();   // filters by acme

tenantCtx.Current = "globex";
await store.Query<Order>().ToList();   // re-translated, filters by globex
```

### Filtered vs unfiltered paths

| Path | Filtered? |
|---|---|
| `Query<T>()` + all terminals, `query.NotifyOnChange()` | Yes |
| `Get<T>` / `GetDiff<T>` | Yes — returns null if filter rejects |
| `Update<T>` | Yes — throws "not found" if filter rejects |
| `SetProperty<T>` / `RemoveProperty<T>` / `Remove<T>` | Yes — returns false if filter rejects |
| `Clear<T>` / `Count<T>` | Yes |
| `Insert<T>` / `BatchInsert<T>` / `Upsert<T>` | **No** — matches EF Core |
| `Query<T>(rawSql)` / `QueryStream<T>(rawSql)` | **No** — matches EF Core's FromSqlRaw |

### Caveats

- **`JsonTypeInfo<T>` is required** for SQL providers (the filter is translated by the expression visitor). Configure a `JsonSerializerContext` on `JsonSerializerOptions` or the registered filter throws `InvalidOperationException` at first use.
- **Spatial sidecars** are not aware of per-row filter rejections on bulk Remove/Clear. Mix soft-delete with spatial cautiously.
