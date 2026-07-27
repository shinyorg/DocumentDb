# Plan: JSON collections — one late-bound lane, keyed by name **or** type

**Status:** Designed, not started. Supersedes `plans/dynamic-documents.md`.
**Target version:** `12.2` (**breaking** — the eight `IDocumentStore` JSON-lane members are removed).
Phase 0 spike → Phases 1–3 (core seam + unified lane) → Phase 4 (relational sweep) → Phase 5
(LiteDB/IndexedDB, droppable) → Phase 6 (pin the NotSupported tier); see [Phasing](#phasing).

> Self-contained build spec. Read `CLAUDE.md` (repo root) for the four-artifact rule (code+tests, docs
> site, skill, readme) before considering any commit "done". Branch off `v12`. The docs site is the
> **separate** repo at `~/Desktop/dev/documentation`. The full suite needs **Docker** (Testcontainers) —
> do not claim green from a filtered subset.

---

## Goal

One way to read and write documents as raw JSON, whether or not a CLR type exists.

```csharp
// schema-free — no CLR type anywhere
var orders = store.Collection("orders");
var id  = await orders.Insert(jsonObject);
var doc = await orders.Get(id);
var rows = await orders.Query()
    .Where("customer.name == 'bob' and total:number > 100")
    .OrderBy("total:number desc")
    .Paginate(0, 50)
    .ToList();                       // IReadOnlyList<JsonObject>

// late-bound over a registered type — full write pipeline, typed path resolution
var users = store.Collection(typeof(User));
await users.Insert(jsonObject);
var admins = await users.Query().Where("roles hasflag 'Admin'").ToList();
```

### Why this shape

`DocumentStore.JsonLane.cs` already writes and reads JSON bodies; it is keyed by `Type`. A schema-free
collection needs the *same* operations keyed by a `string`. The row is identical either way —
`(Id TEXT, TypeName TEXT, Data TEXT, CreatedAt, UpdatedAt)` with PK `(Id, TypeName)`, and `TypeName` is a
plain string at the SQL layer (`Shiny.DocumentDb.Sqlite/SqliteDatabaseProvider.cs:79`). The write
primitives at `DocumentStore.JsonLane.cs:279-296` (`InsertCoreAsync`, `UpdateCoreAsync`,
`UpsertMergeCoreAsync`, `MergeOrReplaceCoreAsync`) are already `Type`-free.

So **keying is the only variable**, and it becomes a parameter — `JsonLaneTarget` — rather than a second
public API. A CLR type contributes exactly two things on top of a name: the mappings/pipeline
(interceptors, temporal, global filters, change feed, spatial/vector/blob sidecars) and a `JsonTypeInfo`
for resolving a *path string* to a *JSON path*. Both are nullable.

**Storage needs zero schema change.**

## Design decisions (locked)

| Decision | Choice | Consequence |
|---|---|---|
| Surface | **One** `IJsonDocumentCollection`, obtained by `Collection(string)` or `Collection(Type)` | No second lane; the pipeline/mapping differences become a documented property of the target, not of the API |
| Existing JSON-lane members | **Deleted** from `IDocumentStore` — no forwarding shims | CLAUDE.md no-cruft rule; `<RN type="breaking">` + a docs migration table |
| Keying | Collection name → the row's `TypeName`; a `Type` resolves to its `TypeName` as today | Name-keyed and type-keyed collections coexist in one table, invisible to each other |
| Query surface | **String grammar only** on `IJsonDocumentQuery`, both keyings | Typed LINQ stays on `Query<T>()`. One front end. Type-keyed gets the *full* function set (`hasflag`, spatial, full-text) because it has a `JsonTypeInfo`; name-keyed gets the schema-free subset |
| Raw SQL | Kept as `Collection(...).Query(whereClause, parameters)` | Preserves today's `Query(Type, whereClause)` capability; mirrors `Query<T>(string)` (`IDocumentStore.cs:183`) |
| Provider tier v1 | Relational (SQLite, DuckDB, MySQL, MariaDB, PostgreSQL, CockroachDB, SQL Server, Oracle) + in-memory (LiteDB, IndexedDB) | Document-native providers throw `NotSupportedException` — same tier the JSON lane has today. Mongo/Firestore have their own visitors that bypass the shared IR, so each is separate work |
| Type hint syntax | Suffix — `total:number` | Lexer-only change; name-keyed only (a type-keyed collection infers from `JsonTypeInfo`). Cannot leak into the typed surface (`:` is a parse error there today) |
| Unhinted `OrderBy` (name-keyed) | **Warn** via `options.Logging` on text-extract providers | Discoverable in dev without making the same query string non-portable between SQLite and PostgreSQL |
| Field-vs-field relational compare (name-keyed) | **Throw**, require a `:type` hint on one side | Explicit, and reversible later — loosening a throw is always safe |

---

## The two seams

### 1. `DocumentFieldExpression` — a path that isn't a property

`Internal/FilterExpressionParser.cs:81` already builds its parser with a **field-resolver delegate**:

```csharp
new Parser(tokens, args, path => DocumentQueryExtensions.BuildMemberAccess(parameter, path, jsonTypeInfo, computed))
```

The parser never touches `JsonTypeInfo` except through that delegate (same at `:102` `ParseProjection`,
`:116` `ParseValueSelector`). So schema-free parsing is a second resolver. But it cannot return
`Expression.Property(...)` — there is no CLR type — so it returns a custom node:

```csharp
// src/Shiny.DocumentDb/Internal/Query/DocumentFieldExpression.cs — NEW
sealed class DocumentFieldExpression(string jsonPath, Type clrType) : Expression
{
    public string JsonPath { get; } = jsonPath;
    public Type ClrType { get; } = clrType;

    public override ExpressionType NodeType => ExpressionType.Extension;
    public override Type Type => this.ClrType;
    public override bool CanReduce => false;

    // MANDATORY. Without it, ExpressionVisitor.VisitExtension -> Expression.VisitChildren throws
    // "must be reducible". Fails only on the SECOND Where (via ParameterReplacer), so it is easy to miss.
    protected override Expression VisitChildren(ExpressionVisitor visitor) => this;

    public DocumentFieldExpression Retype(Type t) => t == this.ClrType ? this : new(this.JsonPath, t);

    // Stable + type-inclusive: DocumentQueryBase.CursorPageCore:367 hashes selector.ToString().
    public override string ToString() => $"${this.JsonPath}:{this.ClrType.Name}";
}
```

`ExpressionLowerer` then needs **one new case** —
`DocumentFieldExpression f => new RootFieldNode(f.JsonPath, f.ClrType)`. Everything downstream
(`SqlPredicateEmitter`, all eight dialects, `AggregateTranslator`, `ProjectionTranslator`) is untouched,
because it only ever sees `RootFieldNode(JsonPath, ClrType)` (`Internal/Query/SqlPredicateEmitter.cs:125`).

Three live visitors walk user predicates and will hit the extension node, hence the override:
`Internal/DocumentQuery.cs:974` `ParameterReplacer` (fires on **any** multi-`Where` query, via
`CombinePredicates:742`), `DocumentQueryPlan.cs:77` `ParameterRebinder`, and
`DocumentStore.JsonLane.cs:472` `JsonLaneParameterReplacer`.

Type-keyed collections never produce this node — they bind through `JsonTypeInfo` exactly as
`Query<T>().Where(string)` does today.

### 2. Type inference lives in the **parser**, not the lowerer

Name-keyed only. `RootFieldNode.ClrType` drives `provider.JsonExtractTyped(...)`, which decides the SQL
cast. Two hard reasons inference cannot be deferred to lowering — both fire at expression-*construction*
time:

1. `Expression.GreaterThan(objectTyped, objectTyped)` **throws** — there is no `>` on `object`.
   `FilterExpressionParser.BuildBinary:920` calls it directly.
2. `CoerceLiteral(raw, typeof(object), …)` (`:987`) matches no branch and falls through to `return raw`,
   so `total > 100` would bind the **string** `"100"`.

Not cosmetic. `IDatabaseProvider.JsonExtractTyped` defaults to plain `JsonExtract`
(`IDatabaseProvider.cs:258`), and PostgreSQL's untyped `#>>` returns `text`
(`PostgreSqlDatabaseProvider.cs:234-254`) — so an untyped `total > 100` is `text > integer` →
`42883 operator does not exist`.

**Four rules:**

1. **Infer from the other operand** — `total > 100` → the number token is `int` → retype the field before
   building the binary. Covers all comparisons, `in (…)`, and interpolated `Where($"total > {n}")` (use the
   placeholder's runtime CLR type directly, no `CoerceLiteral` round-trip).
2. **Infer from the function's required argument type** — `lower(name)` → `string`, `year(created)` →
   `DateTime`, `abs(total)` → `double`, `length(name)` → `string`. Exactly the sites that already assert a type.
3. **Explicit `path:type` hint** where nothing is inferable —
   `string | number | int | long | double | decimal | bool | date | guid` (`number` ≡ `double`).
4. **Floor is `typeof(string)`, not `typeof(object)`** — every dialect's `JsonExtractTyped(_,_,string)` falls
   through to the plain extract, so *emitted SQL is byte-identical*, but the CLR expression is well-typed so
   `Expression.Equal`, `IN`, `IS NULL`, `LIKE` and the in-memory interpreter all construct cleanly instead of
   throwing or reference-comparing.

**Behaviour matrix (name-keyed) — reproduce verbatim in the docs page and release note:**

| Case | SQLite | PG / Cockroach / MySQL / MariaDB / SQL Server / Oracle / DuckDB |
|---|---|---|
| `Where("total > 100")` | correct | **correct** (inferred `int` → `::BIGINT` / `CAST(… AS SIGNED)` / …) |
| `Where("name == 'bob'")`, `Where("active == true")` | correct | correct |
| `OrderBy("total")` *(unhinted)* | **numerically correct** | **lexicographic** — `"100"` before `"9"` → **warns via `options.Logging`** |
| `OrderBy("total:number")` | correct | **correct** |
| `OrderBy("name")`, `OrderBy("created")` *(ISO-8601)* | correct | correct |
| `sum(total)` / `avg(total)` / `count()` | correct | **correct** — aggregates hard-code `typeof(double)` |
| `min(x)` / `max(x)` *(unhinted)* | correct | text compare — right for strings/ISO dates, wrong for numbers; `min(total:number)` fixes it |
| `Project("total")` | correct | returns a JSON string; `total:number` returns a number |

`Where("total > shipping")` (two dynamic fields, nothing to infer) → **throw**: *"relational comparison
between two dynamic fields requires a `:type` hint on at least one side"*.

A **type-keyed** collection has none of this — `JsonTypeInfo` supplies the CLR type, so every row above is
correct on every provider and a `:type` suffix is a parse error there.

---

## Public API

### Removed from `IDocumentStore` (breaking)

All eight late-bound members at `IDocumentStore.cs:9-68` are **deleted outright** — no `[Obsolete]`, no
forwarders:

| Removed | Replacement |
|---|---|
| `Insert(Type, JsonNode)` | `Collection(type).Insert(obj)` / `.Insert(array)` |
| `Update(Type, JsonNode)` | `Collection(type).Update(node)` |
| `Update(Type, JsonNode, bool patch)` | `Collection(type).Update(node, patch)` |
| `Upsert(Type, JsonNode)` | `Collection(type).Upsert(node)` |
| `Upsert(Type, JsonNode, bool patchIfUpdate)` | `Collection(type).Upsert(node, patchIfUpdate)` |
| `Get(Type, object)` | `Collection(type).Get(id)` |
| `Query(Type, string, object?)` | `Collection(type).Query(whereClause, parameters)` |
| `QueryStream(Type, string, object?)` | `Collection(type).QueryStream(whereClause, parameters)` |

~51 call sites across `JsonLaneTests`, `PatchUpdateTests`, `BlobTests`, `GeneratedContextTests`,
`samples/Sample.Aot`, `readme.md`, `skills/shiny-documentdb/SKILL.md` and four docs pages. Mechanical.

### Added

```csharp
namespace Shiny.DocumentDb;

public interface IDocumentStore
{
    /// <summary>
    /// Opens a schema-free, name-keyed JSON collection. Documents are plain <see cref="JsonObject"/>s with
    /// no CLR type and no registered mappings; they are queried with the string grammar only.
    /// </summary>
    /// <param name="name">Collection name, <c>^[A-Za-z_][A-Za-z0-9_]{0,127}$</c> — becomes the row's <c>TypeName</c>.</param>
    /// <param name="idProperty">JSON property holding the id. Read case-insensitively; written verbatim.</param>
    IJsonDocumentCollection Collection(string name, string idProperty = "id")
        => throw new NotSupportedException("JSON collections are not supported by this provider.");

    /// <summary>
    /// Opens a late-bound JSON view over a registered document type. Bodies are stored AS-IS (property names
    /// must match the type's serialized shape) but ride the full write pipeline — tenancy, temporal history,
    /// versioning/CAS, spatial/vector/blob sidecars, interceptors and change notifications all apply, and
    /// queries resolve paths through the type's <see cref="JsonTypeInfo"/>.
    /// </summary>
    IJsonDocumentCollection Collection(Type type)
        => throw new NotSupportedException("JSON collections are not supported by this provider.");
}

/// <summary>
/// A JSON view over one collection of documents, addressed by name (schema-free) or by CLR type
/// (late-bound). Obtain one from <see cref="IDocumentStore.Collection(string, string)"/> or
/// <see cref="IDocumentStore.Collection(Type)"/>.
/// </summary>
public interface IJsonDocumentCollection
{
    /// <summary>The row's <c>TypeName</c> — the supplied name, or the type's resolved type name.</summary>
    string Name { get; }
    /// <summary>The backing CLR type, or <c>null</c> when this collection is schema-free. Mappings, the write
    /// pipeline, global filters, temporal history and change notifications apply only when non-null.</summary>
    Type? DocumentType { get; }
    string IdProperty { get; }

    /// <summary>Inserts one document and returns the stored id. When the id property is absent, JSON-null or
    /// empty an id is generated per the collection's id rules (see remarks on <see cref="IdProperty"/>).</summary>
    Task<string> Insert(JsonObject document, CancellationToken ct = default);
    /// <summary>Inserts every element atomically (one transaction). Returns the number written.</summary>
    Task<int> Insert(JsonArray documents, CancellationToken ct = default);

    /// <summary>Full-document replace when <paramref name="patch"/> is false; RFC 7396 deep-merge when true.
    /// Accepts a <see cref="JsonObject"/> (one) or <see cref="JsonArray"/> (many, atomic). Every target must
    /// already exist.</summary>
    Task<int> Update(JsonNode document, bool patch = false, CancellationToken ct = default);

    /// <summary>Merge-or-insert. When <paramref name="patchIfUpdate"/> is true an existing document is RFC 7396
    /// deep-merged; when false its body is replaced wholesale. Insert-when-absent is identical either way.</summary>
    Task<int> Upsert(JsonNode document, bool patchIfUpdate = true, CancellationToken ct = default);
    Task<int> BatchUpsert(IEnumerable<JsonObject> documents, CancellationToken ct = default);

    Task<JsonObject?> Get(object id, CancellationToken ct = default);
    Task<bool> Remove(object id, CancellationToken ct = default);
    Task<int> BatchRemove(IEnumerable<object> ids, CancellationToken ct = default);
    Task<int> Clear(CancellationToken ct = default);

    /// <summary>Fluent string-grammar query returning raw JSON.</summary>
    IJsonDocumentQuery Query();

    /// <summary>Raw provider-specific SQL WHERE escape hatch — the non-generic twin of
    /// <see cref="IDocumentStore.Query{T}(string, JsonTypeInfo{T}?, object?, CancellationToken)"/>.</summary>
    Task<IReadOnlyList<JsonNode>> Query(string whereClause, object? parameters = null, CancellationToken ct = default);
    IAsyncEnumerable<JsonNode> QueryStream(string whereClause, object? parameters = null, CancellationToken ct = default);

    /// <summary>Creates a JSON expression index. Paths are validated before reaching the DDL.</summary>
    Task CreateIndex(CancellationToken ct = default, params string[] jsonPaths);
}

/// <summary>
/// A string-grammar query returning raw JSON. Immutable — every builder call returns a copy, matching
/// <see cref="IDocumentQuery{T}"/>. On a schema-free collection, field paths are dot-separated with an
/// optional <c>:type</c> suffix (<c>string|number|int|long|double|decimal|bool|date|guid</c>) where the type
/// cannot be inferred; on a type-keyed collection paths resolve through the type's metadata and a
/// <c>:type</c> suffix is an error.
/// </summary>
public interface IJsonDocumentQuery
{
    /// <summary>e.g. <c>"customer.name == 'bob' and total:number > 100"</c>. Multiple calls AND together.</summary>
    IJsonDocumentQuery Where(string filter);
    /// <summary>Interpolated form — each <c>{value}</c> binds as a parameter, never string-concatenated.</summary>
    IJsonDocumentQuery Where(FilterInterpolatedStringHandler filter);
    /// <summary>Comma-separated keys, each optionally suffixed asc/desc, e.g. <c>"total:number desc, name"</c>.</summary>
    IJsonDocumentQuery OrderBy(string ordering);
    IJsonDocumentQuery Paginate(int offset, int take);

    IGroupedDocumentQuery<JsonObject, object> GroupBy(string keyField);
    /// <summary>e.g. <c>"name, lower(email) as email, total:number"</c>. Function projections require an alias.</summary>
    IDocumentQuery<JsonObject> Project(string fields);

    DocumentQueryString ToQueryString();
    Task<IReadOnlyList<JsonObject>> ToList(CancellationToken ct = default);
    IAsyncEnumerable<JsonObject> ToAsyncEnumerable(CancellationToken ct = default);
    Task<long> Count(CancellationToken ct = default);
    Task<bool> Any(CancellationToken ct = default);
    Task<CursorPage<JsonObject>> ToCursorPage(string? cursor, int take, CancellationToken ct = default);
    Task<int> ExecuteDelete(CancellationToken ct = default);
    Task<int> ExecuteUpdate(string jsonPath, object? value, CancellationToken ct = default);
}
```

**Why not `IDocumentQuery<JsonObject>`:** it drags in five typed-LINQ members that are meaningless here and
would all have to throw (`Where(Expression<…>)`, `OrderBy(Expression<…>)`, `Select<TResult>`,
`GroupBy<TKey>`, `Max<TValue>(…)`) — against CLAUDE.md's no-leftover-cruft spirit. Worse, it **collides with
`Internal/JsonProjectionDocumentQuery.cs`**, which is already an `IDocumentQuery<JsonObject>` throwing from
`Where`/`OrderBy`/`GroupBy` for the *opposite* reason (`:60-76`); two shapes with inverted semantics behind
one interface makes `is IDocumentQuery<JsonObject>` meaningless. Every `DocumentQueryExtensions` string helper
is also `IDocumentQuery<T> where T : class` funnelling through `ResolveTypeInfo:86`, which would throw.

Internally it still wraps `DocumentQuery<JsonObject>` — 988 lines of SQL building, cursor pagination,
tenancy, `ExecuteUpdate`/`ExecuteDelete` and telemetry we should not duplicate.

### Deliberate omissions

- **No `Insert(JsonNode)`.** Only the `JsonObject` (→ id) and `JsonArray` (→ count) overloads. A third
  `JsonNode` overload would make the return type depend on a variable's *declared* type — a silent footgun.
  `Update`/`Upsert` take `JsonNode` because they have a single count-returning shape.
- **`Get` returns `JsonObject?`, not `JsonNode?`.** Stored bodies are always objects; this is stricter and
  saves every caller a cast. Minor break vs. `Get(Type, id)` — note it in the release note.

## Ids

The id contract is a property of the **target**, and this is the one place the two keyings legitimately
differ. Both return the stored id from `Insert`.

**Type-keyed** — unchanged from today. Id member and `IdKind` come from the type
(`options.ResolveIdPropertyName`, `JsonLaneIdAccessor.Create`, `Internal/JsonLaneAccessors.cs:85-97`);
`IdKind.Guid` generates **v4** (`DocumentStore.cs:1004-1005`; v7 only via `GuidV7IdConverter` + `MapIdType`,
`Internal/IdAccessor.cs:98`), and `IdKind.String` **refuses** to auto-generate
(`DocumentStore.JsonLane.cs:221-223`).

**Name-keyed:**

- Default property `"id"`, overridable **per collection** (two collections can legitimately disagree — this
  is a `Collection(...)` parameter, not a store option).
- **Read case-insensitively** (`id`/`Id`/`ID`); **write the configured name verbatim**.
- Absent / JSON-null / empty on `Insert` → generate `Guid.CreateVersion7().ToString("N")` and **stamp it into
  the document**, so body and `Id` column agree byte-for-byte and no `IdKind` round-trip is needed. A
  schema-free collection has no declared id type, so a sortable string id is the only sane default.
  **Call the v7-string vs v4-Guid difference out in the release note.**
- Present on `Insert`: string → verbatim; number → invariant `ToString()`; bool/object/array → `ArgumentException`.
- `Get(object id)`: `string` → as-is, `Guid` → `"N"`, `int`/`long` → invariant, else `ArgumentException`.
  Documented rule: **schema-free ids compare as literal strings** — pass back what you stored. (Store a dashed
  Guid string, call `Get(guid)`, and you miss. Say so in the docs.)
- `Update`/`Upsert` with no id → `InvalidOperationException` (mirrors `JsonLane.cs:219`).

New `Internal/DynamicIdBinding.cs` for the name-keyed case, cached per `(collection, idProperty)`.
`JsonLaneIdAccessor` cannot be reused — `Create` requires a resolvable `JsonTypeInfo`.

## Capability matrix

Everything that used to be a "dynamic docs don't support X" list collapses to one rule: **pipeline features
apply when `DocumentType != null`.** The null-check is the same one the write path already needs.

| Capability | Type-keyed | Name-keyed | Reason |
|---|---|---|---|
| Multi-tenancy (`TenantFilter`) | Yes | **Yes** | Tenancy is a column, not a type concern — reuse `GetTenantFilter()` + `AddTenantParam` as `GetJsonImpl` does (`JsonLane.cs:51-55`) |
| Transactions, offset paging, `ToQueryString()` | Yes | **Yes** | Connection/SQL-scoped, not type-scoped |
| Cursor pagination | Yes | **Yes** | Needs only an id getter; the id binding supplies it |
| Backup / bulk export | Yes | **Yes, free** | Walks tables + `TypeName`, never CLR types (`DocumentStoreOptions.AllDocumentTableNames`) |
| RFC 7396 merge (`patch: true`) | Yes | **Yes** | Routes through the existing `MergeOrReplaceCoreAsync(merge: true)` → `json_patch` / `JSON_MERGE_PATCH` / `Internal/JsonMergePatch.cs` fallback |
| Grammar functions needing a registration (`hasflag`, spatial, full-text) | Yes | No | Each needs a CLR enum or a `Map*Property<T>`. Throws a specific message naming the typed alternative |
| Interceptors, temporal history, global filters, change feed, spatial/vector/blob sidecars, versioning/CAS | Yes | No | See below |
| Unit of work / `IDocumentSession` | No | No | `IDocumentSession.Add<T>` buffers by CLR type (`IDocumentSession.cs:24`); matches the documented rule that this lane stays on the root store (`:13`). Reach it via `session.Store.Collection(...)` |
| RFC 6902 `JsonPatchDocument<T>` | No | No | Typed by construction |

The name-keyed "No"s, with what each would take (all **v1.1**, none blocking):

- **Interceptors** — `DocumentWriteContext.DocumentType` is non-nullable; making it nullable is a public
  breaking change deserving its own `<RN type="breaking">`.
- **Change feed** — `ChangeBroadcaster` subjects are `ConcurrentDictionary<Type, object>`
  (`Internal/ChangeBroadcaster.cs:30`); a name-keyed subject dictionary is small but separate.
- **Temporal** — `AppendHistoryAsync` resolves its mapping by `Type`; needs a `MapTemporalCollection("orders")` opt-in.
- **Global filters** — `options.ResolveQueryFilters(Type)` is Type-keyed and a filter is
  `Expression<Func<T,bool>>`; there is no `T`. Arguably N/A rather than deferred.
- **Spatial / vector / full-text / computed / soft-delete** — each needs a `Map*Property<T>` keyed by `Type`.

## Injection surface

`SqliteDatabaseProvider.cs:146,153,610` interpolate `WHERE TypeName = '{typeName}'` and
`json_extract(Data, '$.{jsonPath}')` **as literals**; every relational provider does the same for the path,
and `IndexExpressionHelper.BuildIndexName(typeName, jsonPath)` turns both into a SQL identifier. Name-keyed
collections mean user-supplied strings reach that DDL.

New `src/Shiny.DocumentDb/Internal/DynamicNames.cs`:

```csharp
static class DynamicNames
{
    // ^[A-Za-z_][A-Za-z0-9_]{0,127}$ — no dots, spaces, quotes, hyphens
    public static string ValidateCollection(string name);

    // 1..16 dot-separated segments, each ^[A-Za-z_][A-Za-z0-9_]*$
    public static string ValidatePath(string path);
}
```

**Enforced at exactly two choke points**, plus the two raw-string APIs that bypass the parser:

1. `DocumentStore.Collection(name, idProperty)` — the only place a collection name is minted (also in the
   `IDocumentStore` default impl's guard and the LiteDb/IndexedDb overrides).
2. `DynamicFieldBinder.Resolve(path)` — the only place a path becomes an expression.
3. `IJsonDocumentQuery.ExecuteUpdate(jsonPath, …)` and `IJsonDocumentCollection.CreateIndex(paths)`.

The lexer already constrains grammar identifiers to `[A-Za-z0-9_.]` (`FilterExpressionParser.cs:262`), so a
quote cannot be smuggled through `Where("…")` today — but the raw-string APIs still need the validator.
`Collection(string, …).Query(whereClause)` is the *documented* raw-SQL escape hatch and is the caller's
responsibility, exactly as `Query<T>(whereClause)` is today.

**Documented consequence:** JSON keys containing dashes, spaces, or dots are *storable but not addressable*
in v1. A bracket/quoted-segment syntax (`items["order-id"]`) is a later cut. State it in `limitations.mdx`.

## Query-surface parity (CLAUDE.md)

Both keyings are string-grammar-only, so the parity rule reads: **every function already in
`IsPredicateFunction:972` / `IsValueFunction:980` must work on a type-keyed collection, and every function
that does not require a `Type`-keyed registration must also work schema-free**; any function added later
must be added to both binders. Enforced by `JsonGrammarParityTests` below, which makes a one-sided addition
a test failure.

Three documented schema-free exceptions, each because the *typed* surface also requires a `Type`-keyed
registration and throws for an unmapped type too — parity in kind:

- `hasflag` — needs a CLR enum to parse the flag name.
- `intersects`/`within`/`distance`/… — need `MapSpatialProperty<T>`.
- `lucenematch`/`lucenescore` — need `MapFullTextProperty<T>`.

Each throws a specific message naming the typed alternative.

---

## Phasing

### Phase 0 — de-risk spike (throwaway, ~1h)

Hand-build `Expression.Lambda<Func<JsonObject,bool>>(GreaterThan(field(int), Constant(100)), p)`, run it
through `ParameterReplacer` **twice** (proves the `VisitChildren` override), then `ExpressionLowerer` +
`SqlPredicateEmitter` against `SqliteDatabaseProvider`, and assert the SQL. If this does not hold the whole
seam is wrong and nothing else was spent.

### Phase 1 — the seam (core internals, zero public API)

| | File | Change |
|---|---|---|
| NEW | `Internal/Query/DocumentFieldExpression.cs` | as above |
| NEW | `Internal/DynamicNames.cs` | `ValidateCollection`, `ValidatePath` |
| NEW | `Internal/DynamicFieldReader.cs` | `static object? Read(JsonNode? root, string jsonPath, Type clrType)` — walks dot segments on a `JsonObject`, `null` on miss, unwraps `JsonValue` to the requested CLR type |
| MOD | `Internal/Query/ExpressionLowerer.cs` | `rootTypeInfo` becomes `JsonTypeInfo?` on `Lower:17`, `LowerValue:26`, `Lowerer` ctor `:40`. New case in `LowerValue`'s switch `:344` → `RootFieldNode`; in `LowerPredicate`'s switch `:76` alongside `MemberExpression or ConstantExpression`. Guard `BuildJsonPath` at `:411,422,438,577` and `GetTypeInfo` at `:566` with a clear "a typed member chain requires a JsonTypeInfo — schema-free collections only support string field paths" throw when `rootTypeInfo is null`. `FieldJsonPath:244` and `LowerNullCheck:167` already funnel through `LowerValue` — no change. |
| MOD | `Internal/Query/ExpressionInterpreter.cs` | one case at the top of `Evaluate:32` → `DynamicFieldReader.Read(arg as JsonNode, f.JsonPath, f.ClrType)` |

Nothing downstream changes — verified by reading all of `SqlPredicateEmitter.cs`.

### Phase 2 — schema-free parsing

**Land the refactor first with no dynamic binder, and require the existing grammar suites to pass
unchanged** (`WhereStringTests`, `ScalarFunctionTests`, `StringProjectionDocTests`, `GroupByQueryTests`,
`SoundexTests`, `SpatialPredicateTests`, `FlagEnumTests`). This is the highest-value sequencing decision in
the plan — `FilterExpressionParser` is 1047 lines and the typed path must not shift by a byte.

| | File | Change |
|---|---|---|
| NEW | `Internal/IFieldBinder.cs` | `(Expression, Type) Resolve(string path)`; `Expression Coerce(Expression operand, Type required, string context, int position)`; `Type LiteralTypeFor(Type leafType, bool isNumberToken, bool isStringToken)`; `bool IsUnresolved(Type leafType)` |
| NEW | `Internal/JsonTypeInfoFieldBinder.cs` | today's `BuildMemberAccess`/`CoerceLiteral` behaviour, verbatim. **Also serves type-keyed JSON collections** — this is what makes one query builder cover both keyings |
| NEW | `Internal/DynamicFieldBinder.cs` | `Resolve` → `ValidatePath` + optional `:type` + `new DocumentFieldExpression(...)`; `Coerce` → `f.Retype(required)`; field-vs-field relational compare → throw. Carries a sentinel `DynamicUnresolved` type used only as a `LeafType` return; `DocumentFieldExpression.Type` reports `string` while unresolved so any accidental escape still builds valid SQL |
| MOD | `Internal/FilterExpressionParser.cs` | `Parser` takes `IFieldBinder` instead of `Func<…>`. Add `ParseDynamic(filter, args)`, `ParseDynamicProjection`, `ParseDynamicValueSelector` mirroring `:67,97,111`. Route through `binder.Coerce` at `RequireString:693`, math converts `:552,560,564,572`, date-parts `:591`, `BuildBinary:920`, `BuildBinaryExpr:950`, `ParseInList:699`, `AsGeometryOperand:806`, `hasflag:662`. Lexer `ReadIdentifier:258` accepts `:` |
| NEW | `Internal/DynamicOrderByParser.cs` | `IReadOnlyList<(string Expr, bool Descending)> Parse(string)` — splits on top-level commas (respecting parens/quotes), strips a trailing `asc\|ascending\|desc\|descending`, reusing `DocumentQueryExtensions`' direction words. Genuinely new parsing: `BuildSelector:277` only routes into the grammar when the string contains `(`, so `"total desc"` today becomes `BuildMemberAccess("total desc")` → *Property not found* |
| NEW | `Internal/DynamicPathResolver.cs` | schema-free twins of `ResolveJsonPath` / `ResolveJsonPathWithType` — needed because `GroupStringTranslator:26,33,43` and `DocumentQuery.Project:385` resolve paths **without** going through `BuildMemberAccess` |

### Phase 3 — the unified lane

The biggest reuse win in the plan, and where the alignment actually lands.

| | File | Change |
|---|---|---|
| NEW | `Internal/JsonLaneTarget.cs` | `record JsonLaneTarget(string TypeName, string TableName, IJsonLaneIds Ids, Type? DocumentType, JsonTypeInfo? TypeInfo, VersionMapping?, SpatialMapping?, VectorMapping?)` plus two factories — `FromType(Type)` (every mapping resolved, as `WriteJsonAsync:115-120` does today) and `FromName(string, string idProperty)` (every mapping null) |
| MOD | `DocumentStore.JsonLane.cs` | **Generalize, don't duplicate.** `WriteJsonAsync`/`WriteOneJsonAsync`/`GetJsonImpl`/`QueryJsonImpl`/`QueryStreamJsonImpl` take a `JsonLaneTarget` instead of `Type type`. The already-`Type`-free primitives at `:279-296` are reused as-is. `AppendGlobalFilters` / `PublishJsonChange` / `NewJsonWriteContext` / `AppendHistoryAsync` / `SpatialUpsertFromNodeAsync` / `VectorUpsertFromNodeAsync` skip when `DocumentType == null`. Delete the eight public `Insert/Update/Upsert/Get/Query/QueryStream(Type, …)` methods at `:22-87` |
| MOD | `DocumentStore.cs` | Extract `RemoveCoreAsync(JsonLaneTarget, resolvedId, ct)` from `RemoveImpl<T>:2180` — it is already `Type`-free at the SQL layer and touches `typeof(T)` only to feed `BlobDeleteAllAsync` / `SpatialDeleteAsync` / `VectorDeleteAsync` / `AppendHistoryAsync` (`:2211-2214`), all of which take the nullable `DocumentType`. `Remove<T>` delegates to it. Same for `Clear` / `BatchRemove`. This is what gives the type-keyed collection a **late-bound delete**, which the JSON lane lacks today |
| MOD | `IDocumentStore.cs` | delete the eight members; add the two `Collection` overloads, default-throwing (mirrors the spatial/vector/full-text pattern at `:296-430`) |
| NEW | `IJsonDocumentCollection.cs`, `IJsonDocumentQuery.cs` | public API above |
| NEW | `DocumentStore.Collection.cs` | both overloads, cached per `(name, idProperty)` and per `Type` |
| NEW | `Internal/JsonDocumentCollection.cs`, `Internal/JsonDocumentQuery.cs` | one implementation of each, parameterized by `JsonLaneTarget`; picks `JsonTypeInfoFieldBinder` when `TypeInfo != null`, else `DynamicFieldBinder` |
| NEW | `Internal/DynamicIdBinding.cs` | the name-keyed id binding (above) |
| MOD | `Internal/IQueryExecutor.cs` | non-generic twins beside `:38-39`: `string ResolveTypeName(string collection)`, `string ResolveTableName(string collection)` — following the `ResolveTypeName(Type)` precedent at `JsonLane.cs:397-399`. Implement next to `DocumentStore.cs:141-143`. `EnsureTableInitializedAsync:212` already keys on `tableName` **only** — verified, no change |
| MOD | `Internal/DocumentQuery.cs` | optional `(typeName, tableName, JsonTypeInfo?)` ctor binding; when set, `ResolveTypeName<T>()`/`ResolveTableName<T>()` become `this.TypeName`/`this.TableName` (~20 mechanical substitutions), `RequireTypeInfo()` returns the bound info or is never called (lowering passes `null`), `ResolveFullText`/`ResolveComputedLookup` short-circuit when `DocumentType == null`. **`JsonTypeInfo<JsonObject>` is not guaranteed obtainable** — `Internal/DocumentDbJsonContext.cs` registers only `Geometry`/`DocumentBlob`, so with `UseReflectionFallback = false` it throws. Hence nullable, not manufactured |
| MOD | `Internal/JsonProjectionDocumentQuery.cs` | accept explicit `typeName`/`tableName` instead of `ResolveTypeName<TSource>()`, so `Project(...)` is reused by both keyings |
| MOD | `Internal/GroupStringTranslator.cs` | schema-free branch via `DynamicPathResolver`; `SUM`/`AVG`/`COUNT` hard-code `typeof(double)`; `MIN`/`MAX` honour the hint, default `string` |

**Migration sweep in the same commit** — `JsonLaneTests`, `PatchUpdateTests`, `BlobTests`,
`GeneratedContextTests`, `samples/Sample.Aot`, `readme.md`, `skills/shiny-documentdb/SKILL.md`, and the four
docs pages (`crud.mdx`, `index.mdx`, `aot.mdx`, `release-notes.mdx` prose).

### Phase 4 — relational sweep

No per-provider code expected — all eight derive from `DocumentStore`. Add one `Collection` conformance
subclass per provider and fix whatever falls out. Wire `CreateIndex` onto the existing
`BuildCreateJsonIndexSql(indexName, tableName, paths, typeName)` with validated name/paths. Add the
unhinted-sort warning: detect a text-extract provider by comparing
`JsonExtractTyped(_, _, typeof(object))` to `JsonExtract(...)`.

### Phase 5 — in-memory providers (sequence last so it can slip)

| File | Change |
|---|---|
| `Shiny.DocumentDb.LiteDb/LiteDbDocumentStore.cs` | `LoadRawDocuments(string typeName) → IEnumerable<JsonObject>` (mirrors `LoadDocuments<T>:660`); both `Collection` overrides; raw write/delete by `typeName` |
| NEW `Shiny.DocumentDb.LiteDb/LiteDbJsonQuery.cs` | `DocumentQueryBase<JsonObject>` — `ExecuteAsync` returns `QueryExecution<JsonObject>.Candidates(rawDocs)`; the base filters/orders/pages via `ExpressionInterpreter`, which understands `DocumentFieldExpression` from Phase 1 |
| `Shiny.DocumentDb.IndexedDb/*` | same shape (`IndexedDbDocumentStore.cs:729` is the analogous seam) |

`DocumentQueryBase.Context.GetId` = the collection's id reader (enables cursor pagination); `Context.TypeInfo`
is null for name-keyed, so `Project(string)` needs a schema-free override — `DocumentQueryBase.cs:297-304`
hard-requires a `JsonTypeInfo<T>` for `StringProjection.BuildGetters`.

**If this slips, it drops cleanly** — LiteDB/IndexedDB join the NotSupported tier for `Collection` and one
release-note line changes. They do not support the JSON lane today either, so nothing regresses.

### Phase 6 — pin the NotSupported tier

Mongo, Cosmos, Raven, Firestore, Redis, AzureTable, DynamoDb inherit the default throw. No code, but **one
assertion test per provider** so the tier is pinned rather than accidental.

---

## Tests

Run the **full** suite with Docker up: `dotnet test tests/Shiny.DocumentDb.Tests/Shiny.DocumentDb.Tests.csproj`.

**NEW `tests/Shiny.DocumentDb.Tests/JsonCollectionConformanceTests.cs`** —
`abstract class JsonCollectionConformanceTestsBase(IDocumentStoreFixture fixture)` plus a one-line subclass
per provider following `SqliteProviderTests.cs:86`:

```csharp
[Collection("SQLite")]
public class JsonCollectionConformanceTests(SqliteDatabaseFixture db) : JsonCollectionConformanceTestsBase(db);
```

Every applicable case runs **twice — once name-keyed, once type-keyed** (a `[Theory]` over the two factories,
skipping the cases the matrix marks type-only). Seeds an `orders` collection (`customer.name`, `total`,
`status`, `created`, `tags[]`) and covers: writes (id stamping, duplicate-id throw, atomic array insert,
`patch: true` merge leaving untouched keys, upsert, batch, remove, clear); reads (`Get` by string/int/Guid,
miss → null); the full filter grammar (comparisons, `in`, `is null`, and/or/not, parens, nested paths,
interpolation); ordering (single, multi, desc, function key); offset + cursor paging; `Project`; `GroupBy`
with `Having`; terminals; tenancy isolation (`ITenantDocumentStoreFixture`); and **two name-keyed collections
+ a typed `Query<User>()` sharing one table without seeing each other**.

Four tests carry most of the design risk:

- **Type inference** — seed `total` values `9`, `10`, `100` so lexicographic and numeric comparison give
  *different* answers. Assert `Where("total > 100")` is numerically correct on **every** relational provider,
  `OrderBy("total:number")` is numeric everywhere, and unhinted `OrderBy("total")` matches the documented
  per-provider divergence. This is the test that catches a wrong inference design.
- **The `VisitChildren` regression** — a query with **two** `Where` calls. Fails without the override.
- **Pipeline parity for the type-keyed collection** — assert interceptors fire, temporal history appends,
  global filters apply, spatial/vector sidecars are written, and change notifications publish, for
  `Collection(typeof(T))` writes. This is the net that replaces the "`JsonLaneTests` unmodified" property
  lost to the migration (see Risks).
- **NEW `JsonGrammarParityTests.cs`** (SQLite-only, fast, `[Theory]`) — one `[InlineData]` per entry in
  `IsPredicateFunction:972` / `IsValueFunction:980`, running the **same filter string** against
  `Query<User>()`, `Collection(typeof(User)).Query()` and `Collection("users").Query()` over identical seed
  data, asserting equal id sets. The three documented exceptions assert a specific `NotSupportedException`
  message on the name-keyed leg only.

**No-provider unit tests** (no Docker):

- NEW `DocumentFieldExpressionTests.cs` — survives `ExpressionVisitor.Visit` through both repo visitors;
  `Retype`; `ToString` stability (cursor shape hash).
- NEW `DynamicNamesTests.cs` — rejection corpus: `orders'; DROP TABLE x--`, `a.b`, `a b`, `1abc`, `""`,
  129 chars, `$.a`, `a[0]`, unicode; plus the valid set.
- NEW `DynamicTypeInferenceTests.cs` — asserts **emitted SQL** per dialect via `ToQueryString()`: `::BIGINT`
  on PG, `CAST(… AS SIGNED)` on MySQL, `CAST(… AS BIGINT)` on SQL Server/DuckDB, `RETURNING NUMBER` on
  Oracle, bare `json_extract` on SQLite. Cheapest possible guard on the whole inference design.
- NEW `JsonCollectionNotSupportedTests.cs` — one assertion per document-native provider, both overloads.
- MOD `InterpreterTests.cs` — `DocumentFieldExpression` over a `JsonObject` (nested path, missing → null,
  coercion).
- MOD `JsonLaneTests.cs` — **rewritten to `Collection(typeof(T))`, assertions unchanged.** Mechanical
  find/replace; any assertion that has to change is a behaviour regression and must be justified in review.

---

## Shipping artifacts (CLAUDE.md four-artifact rule)

1. **Code + tests** — above.
2. **Docs site** `~/Desktop/dev/documentation/src/content/docs/documentdb/`
   - NEW `json-collections.mdx` — concept (one lane, two keyings), `store.Collection("orders")` and
     `store.Collection(typeof(User))`, the full write/read/query surface, the **id rules per keying**, the
     **type-inference rules + `:type` hint** with the behaviour matrix rendered as a table, the **name/path
     charset rule** and the dashed-keys limitation, the **capability matrix**, and the
     **provider-compatibility tier**.
   - NEW migration table (old member → new call) on that page, linked from the release note.
   - MOD `crud.mdx` (the JSON-lane section becomes a pointer to the new page; every sample updated),
     `querying.mdx` (schema-free section cross-linking), `providers.mdx` (capability-matrix column),
     `aot.mdx`, `limitations.mdx`, `index.mdx`.
3. **Release note** `release-notes.mdx` — `version.json` is `12.2.0-beta`, so open `## 12.2 TBD` at the top
   (create if absent).
   - `<RN type="breaking">` — the eight `IDocumentStore` JSON-lane members are removed in favour of
     `store.Collection(type)`, with the migration table. Also note `Get` now returns `JsonObject?` and
     `Insert(JsonObject)` now returns the id rather than a count.
   - `<RN type="feature">` — schema-free `store.Collection("name")`: what it is, the code sample, the
     **provider tier stated explicitly**, the **type-inference rules + unhinted-`OrderBy` caveat**, the
     **v7-string vs v4-Guid id difference**, the charset rule, and the explicit deferred list (sessions,
     change feed, interceptors, temporal, global filters, spatial/vector/full-text).
   - `<RN type="enhancement">` — type-keyed JSON collections gain a **fluent string-grammar query builder**
     and **late-bound `Remove`/`BatchRemove`/`Clear`**, neither of which the JSON lane had.
4. **Skill** `skills/shiny-documentdb/SKILL.md` — `triggers:` additions (`json collection`,
   `schema-free collection`, `store.Collection`, `IJsonDocumentCollection`, `IJsonDocumentQuery`,
   `JsonObject collection`, `untyped document`, `no CLR type`, `name-keyed collection`, `type hint`); update
   every existing `Insert(typeof(X), …)` sample; a section on when to reach for `Collection(name)` vs
   `Collection(type)` vs `Query<T>()`, the string-grammar-only rule, when the `:type` hint is **required**
   (any `OrderBy`/`min`/`max`/`Project` on a numeric field on a non-SQLite name-keyed collection), the id
   contract per keying, the provider tier, and the not-supported list — so the agent never generates
   `store.Collection("x").Query().Where(o => …)` or the removed `store.Insert(typeof(X), json)`.
5. **`readme.md`** (repo root, packed into the NuGet package) — update the JSON-lane bullet and sample to the
   `Collection` surface; add a schema-free bullet.

## Risks / open questions

1. **`FilterExpressionParser` refactor** — 1047 lines, ~10 touched sites, typed path must not shift.
   Mitigated by the Phase-2 sequencing (pure refactor green before `DynamicFieldBinder` exists). Highest-value
   sequencing decision in the plan.
2. **The migration weakens the Phase-3 safety net.** `JsonLaneTests` can no longer be the "must stay green
   *unmodified*" regression net for the `JsonLaneTarget` generalization, because the same commit rewrites its
   call sites. Mitigation: do the rewrite as a **pure mechanical find/replace in its own commit before the
   generalization**, so the generalization commit still has an unmodified suite to prove itself against. Plus
   the explicit pipeline-parity test above.
3. **Phase 5 may exceed estimate.** The LiteDB store was read; the **IndexedDB JS interop was not**
   (`IndexedDbJsInterop.cs`, `wwwroot/*.js`) — a raw-JSON read path may not exist there and could need a
   JS-side change, a different order of work. Sequenced last so it drops cleanly.
4. **`StringProjection.BuildGetters` may not factor cleanly** for a schema-free in-memory path
   (`DocumentQueryBase.cs:300`). Not read.
5. **`Collection` name collision.** Confirmed free across `src/` today, but the seven document-native stores
   were not each opened. Surfaces at compile time in Phase 6, cheaply.
6. **`GroupBy` returns `IGroupedDocumentQuery<JsonObject, object>`** — reuse over a bespoke interface, which
   slightly undercuts the "no `IDocumentQuery<JsonObject>` confusion" argument above. Deliberate; overrule if
   the inconsistency grates.
7. **Pre-existing inconsistency noticed while reading:** `AppendGlobalFilters(cmd, Type)`
   (`JsonLane.cs:432-469`) is called from `GetJsonImpl` but **not** `QueryJsonImpl`. Out of scope here, but
   the `JsonLaneTarget` refactor makes it visible — file separately rather than folding it in.
