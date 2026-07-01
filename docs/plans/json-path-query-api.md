# Plan: Native JSON-path query API (`WhereJsonPath` / `OrderByJsonPath` / `SelectJsonPath`)

**Status:** Designed, not started.
**Target version:** `10.0.0` (raw version from `version.json`, currently `10.0.0-beta.{height}`) — additive.
New extension methods on `IDocumentQuery<T>` + one new tiered capability **enum** (`JsonPathSupport`) and a
small dialect surface on `IDatabaseProvider`. No breaking changes; every existing query call is untouched.

**Supported providers (tiered):** `Basic` (single-level wildcard + scalar comparison) on **SQLite,
SQLCipher, MySQL, SQL Server, DuckDB, PostgreSQL, Oracle**; `Full` (recursive descent / multi-wildcard /
in-path filter expressions) additionally on **PostgreSQL, Oracle**. `None` (throws) on **Cosmos, MongoDB,
LiteDB, IndexedDB**.

> Self-contained build spec. The implementing agent does not have the design conversation —
> everything needed is here. Read `CLAUDE.md` (repo root) for the four-artifact rule (code+tests,
> docs site, skill, readme) before considering any commit "done".

Branch off `v10` (the current working branch) before starting.

---

## Goal

Let callers filter, order, and project by a **raw JSON-path string** — including array-index and wildcard
traversal — against the physical stored JSON, on the providers that can shred JSON arrays server-side
(the `Basic` tier — every relational provider; the full path grammar is the `Full` tier — PostgreSQL/Oracle):

```csharp
// Filter — T and TValue both infer (receiver + value), no explicit type args
store.Query<Order>()
     .WhereJsonPath("$.lines[0].price", JsonPathOp.GreaterThan, 10m)
     .ToList();

// Single-wildcard traversal (the thing typed Where / Where(string) cannot express) — Basic tier
store.Query<Order>()
     .WhereJsonPath("$.lines[*].productName", JsonPathOp.Equal, "Widget")
     .ToList();

// Order (scalar path only)
store.Query<Order>().OrderByJsonPath("$.shippingAddress.city").ToList();

// Project a single path to a scalar column (TValue explicit — see open question on the type-arg shape)
IReadOnlyList<decimal> prices =
    await store.Query<Order>().SelectJsonPath<Order, decimal>("$.lines[0].price").ToList();
```

This is the **raw-path / schema-free** lane. It differs from the existing typed and string overloads
(`Where(x => x.A.B)`, `Where("a.b == 1", ctx)`, `OrderBy("a.b", ctx)`, `Project("a,b", ctx)`) in three
deliberate ways:

1. **Operates on the literal stored JSON keys** — no resolution through `JsonTypeInfo`, no naming-policy
   application. The caller writes the path exactly as the JSON is persisted (`$.shippingAddress.city`,
   camelCase by default).
2. **No `JsonTypeInfo<T>` / context required** — nothing is resolved through the model, so these methods
   are AOT-trivial with zero setup (unlike the `(string, JsonTypeInfo<T>)` overloads).
3. **Reaches paths that are not typed model members** — array elements, wildcards, and properties that
   exist in the JSON but on no `[Document]` type. This is the whole point; it is the schema-free story.

### What this is NOT

- **Not** a replacement for the existing string overloads. `Where(string, ctx)` / `OrderBy(string, ctx)`
  / `Project(string, ctx)` stay — they are model-resolved, naming-policy-aware, and work on **every**
  provider (including the in-memory ones). Use those for known model members; use `*JsonPath` for raw
  paths and wildcards.
- **Not** universal. It is gated by the tiered `JsonPathSupport` enum and **throws `NotSupportedException`
  when the provider's tier is below what the path requires** — the same throw-on-unsupported discipline as
  `WithinRadius` under `SupportsSpatial` and `NearestVectors` under `SupportsVector`.

---

## Decisions locked (from design conversation)

- **New tiered capability enum `IDatabaseProvider.JsonPathSupport` (default `None`), not a set of bool
  flags (locked).** One property, three levels, so the two genuinely-different capabilities are expressed
  in a single ordered value:
  ```csharp
  enum JsonPathSupport { None = 0, Basic = 1, Full = 2 }
  JsonPathSupport JsonPathSupport => JsonPathSupport.None;
  ```
  - **`None`** — no server-side JSON array shredding wired for the raw-path lane. The three methods throw
    `NotSupportedException`. (Cosmos, MongoDB, LiteDB, IndexedDB.)
  - **`Basic`** — scalar paths (`$.a.b`, `$.lines[0].price`) and **one** wildcard level with a scalar
    comparison (`$.lines[*].price > 10`, existential). Implemented on the existing `JsonEachFrom` +
    element-extract seam that already backs `.Any(predicate)` — so **no new provider code** for this tier.
    (SQLite, SQLCipher, MySQL, SQL Server, DuckDB, PostgreSQL, Oracle.)
  - **`Full`** — everything in `Basic` plus recursive descent (`$..price`), multiple wildcard levels, and
    in-path filter expressions (`$.lines[*] ? (@.price > 10 && @.qty < 5)`). Requires the native SQL/JSON
    path engine. (PostgreSQL `jsonb_path`, Oracle `JSON_EXISTS`.)
- **The bar shifted (locked).** The earlier "filter-predicate bar → 3 providers" framing rested on a false
  premise: that `OPENJSON`-style shredding was SQL-Server-special. It is not — **every relational provider
  already implements the same array-shredding primitive** as `IDatabaseProvider.JsonEachFrom` (SQLite
  `json_each`, MySQL/Oracle `JSON_TABLE`, Postgres `jsonb_array_elements`, SQL Server `OPENJSON`, DuckDB
  `unnest`), already consumed for collection `.Any()`/`.Count()` predicates at `SqlPredicateEmitter.cs:70,109`.
  So the single-wildcard case is reachable on all seven relational providers with the existing seam; only
  the *full path grammar* is Postgres/Oracle-exclusive. Hence the two-tier enum instead of a hard 3-provider
  line.
- **A raw path is classified at build time to pick the required tier.** No wildcard / one `[*]` and no
  `?()`/`..` → `Basic`. Recursive descent, ≥2 wildcard levels, or an in-path filter → `Full`. A path that
  needs `Full` on a `Basic`-only provider throws `NotSupportedException` with a message naming the offending
  path feature. See the classifier in the implementation sketch.
- **Single-path scalar projection is named `SelectJsonPath<TValue>` (locked).** It returns a scalar
  sequence (`IDocumentQuery<TValue>`), not a `JsonObject`; multi-path projection stays on the existing
  `Project(string, ctx)`. Not reusing the `Project` name, which is `JsonObject`-shaped elsewhere.
- **The methods are a distinct raw-path family**, not new overloads of `Where`/`OrderBy`/`Project`. Names:
  `WhereJsonPath`, `OrderByJsonPath`, `SelectJsonPath`. Naming them apart keeps the raw-JSON-key /
  no-`JsonTypeInfo` semantics obvious at the call site and avoids overload ambiguity with the existing
  `(string, JsonTypeInfo<T>)` methods.
- **Raw JSON keys, no naming policy.** The path is passed to the provider's native path function verbatim.
  The caller owns casing and must match the persisted shape. Documented loudly (same discipline as the
  "stored AS-IS" contract in the JSON-write-API plan).
- **`WhereJsonPath` carries a comparison operand and a CLR type.** A path alone is not a predicate, and
  relational extraction returns text — the CLR type drives the provider's typed CAST (the same
  `JsonExtractTyped` typing story that backs `Where` today; see `IDatabaseProvider.cs:208`). Signature is
  generic: `WhereJsonPath<TValue>(string jsonPath, JsonPathOp op, TValue value)`.
- **Zero impact on non-JSON-path queries.** The flag and the new dialect method are purely additive; the
  existing `SqlPredicateEmitter` / `JsonExpressionVisitor` pipeline is unchanged for all current queries.

### Deferred / future work

- **Promoting `Basic` providers to `Full`.** SQLite/MySQL/SQL Server/DuckDB could reach `Full` later by
  emitting nested-`EXISTS` shredding for multi-wildcard paths and translating in-path filters — more
  emitter work, no public-surface change (the enum value flips). Not built here.
- **Cosmos / MongoDB to `Basic`.** Both can express single-wildcard existence (Cosmos `JOIN`+`EXISTS` over
  the array, Mongo `$elemMatch`), but neither uses the relational `JsonEachFrom` seam, so each needs its own
  emitter. Left `None` this cut; promotable without changing the public methods.

### Alternatives considered and rejected

- **Two boolean flags (`SupportsJsonPath` + `SupportsJsonPathAdvanced`).** Rejected in favor of the ordered
  enum: the tiers are strictly nested (`Full` ⊃ `Basic` ⊃ `None`), so one comparable value (`>= Basic`) is
  clearer than two flags whose invalid combination (`Advanced && !Basic`) has to be documented away.
- **A single always-on `Basic` tier for every provider (no `None`).** Rejected: Cosmos/Mongo/LiteDB/
  IndexedDB don't share the `JsonEachFrom` seam, so pretending they do would mean a silent wrong-answer or a
  bespoke emitter we're not building yet. `None` + throw is honest.
- **Hard 3-provider line (Postgres/Oracle native + SQL Server OPENJSON only).** Rejected once it was clear
  every relational provider already exposes the same shredding primitive — that line would special-case SQL
  Server for a capability the generic `JsonEachFrom` seam already provides to SQLite/MySQL/DuckDB too.
- **Client-side fallback on the non-native providers (materialize + `JsonNode` walk, like the Tier-3
  `ExpressionInterpreter` path).** Rejected per the locked decision: no fallback. `false` means throw,
  matching spatial/vector. (LiteDB/IndexedDB already interpret CLR expressions client-side via the normal
  `Where`; the raw-path lane is explicitly a native-server feature.)
- **New overloads of `Where`/`OrderBy`/`Project` instead of a `*JsonPath` family.** Rejected: overload
  ambiguity with the existing `(string, JsonTypeInfo<T>)` methods, and it hides the very different
  semantics (raw keys, no model resolution, throws on unsupported providers).

---

## `JsonPathSupport` per-provider matrix

| Provider | `JsonPathSupport` | `Basic` seam (shred = `JsonEachFrom`) | `Full` engine |
|---|---|---|---|
| **PostgreSQL** | `Full` | `jsonb_array_elements` + element extract | native `jsonb_path_exists` / `jsonb_path_query_first` |
| **Oracle 23ai** | `Full` | `JSON_TABLE(... '[*]' ...)` + element extract | native `JSON_EXISTS` / `JSON_VALUE` with path predicate |
| **SQL Server** | `Basic` | `OPENJSON` + element extract | — (`JSON_PATH_EXISTS` lacks the full filter grammar) |
| **MySQL** | `Basic` | `JSON_TABLE(... '[*]' ...)` + element extract | — |
| **DuckDB** | `Basic` | `unnest(CAST(... AS JSON[]))` + element extract | — |
| **SQLite / SQLCipher** | `Basic` | `json_each` + element extract | — |
| **Cosmos DB** | `None` | (own `JOIN`/`EXISTS`, not `JsonEachFrom` — deferred) | — |
| **MongoDB** | `None` | (own `$elemMatch`, not `JsonEachFrom` — deferred) | — |
| **LiteDB** | `None` | embedded / client-side | — |
| **IndexedDB** | `None` | embedded / client-side | — |

The enum defaults to `None` on `IDatabaseProvider`. The `Basic` tier needs **no new per-provider code** — it
reuses the existing `JsonEachFrom` + `JsonExtractElementTyped` seam. Only the two `Full` providers override a
native full-path method.

---

## API surface (new)

### `IDatabaseProvider` — the tiered enum + one `Full`-only method

The `Basic` tier reuses the existing seam (`JsonEachFrom`, `JsonExtractTyped`, `JsonExtractElementTyped`) —
so the only new members are the enum and a single `Full`-only native-path method. Add next to the existing
JSON dialect fragments (`src/Shiny.DocumentDb/IDatabaseProvider.cs:207-224`):

```csharp
/// <summary>How much of the raw JSON-path query surface this provider supports. None → the three methods
/// throw NotSupportedException. Basic → scalar + single-wildcard existential (built on JsonEachFrom).
/// Full → also recursive descent / multi-wildcard / in-path filters via a native path engine.
/// Default None; providers opt in.</summary>
JsonPathSupport JsonPathSupport => JsonPathSupport.None;

/// <summary>Full-tier only: a complete boolean SQL fragment true when the native path engine evaluates
/// <paramref name="jsonPath"/> (which may use recursive descent, multiple wildcards, or an in-path filter)
/// against <paramref name="column"/>, comparing via <paramref name="op"/> to <paramref name="valueParam"/>.
/// PostgreSQL → jsonb_path_exists; Oracle → JSON_EXISTS with a PASSING variable. Only called when
/// JsonPathSupport == Full and the path was classified Full.</summary>
string JsonPathNativePredicate(string column, string jsonPath, JsonPathOp op, string valueParam, Type clrType)
    => throw new NotSupportedException("This provider does not support full JSON-path grammar.");
```

`JsonPathSupport` is overridden to `Basic` by SQLite/SQLCipher/MySQL/SQL Server/DuckDB and to `Full` by
PostgreSQL/Oracle; `JsonPathNativePredicate` is overridden only by the two `Full` providers. No new scalar-
extract method is needed for `Basic`: raw scalar paths (`$.a.b`, `$.lines[0].price`) reuse `JsonExtractTyped`
(the existing method already takes a JSON path and types the result), and single-wildcard predicates lower to
the existing collection-`Any` IR (see implementation).

### `IDocumentQuery<T>` extensions (new — `DocumentQueryExtensions.cs`)

```csharp
public enum JsonPathOp { Equal, NotEqual, GreaterThan, GreaterThanOrEqual, LessThan, LessThanOrEqual,
                         Contains, StartsWith, EndsWith }

/// <summary>Filter by a raw JSON-path string against the physical stored JSON (no JsonTypeInfo / naming
/// policy). Scalar and single-wildcard paths need JsonPathSupport >= Basic; recursive-descent /
/// multi-wildcard / in-path-filter paths need Full. Throws NotSupportedException when the provider's tier
/// is below what the path requires.</summary>
public static IDocumentQuery<T> WhereJsonPath<T, TValue>(
    this IDocumentQuery<T> query, string jsonPath, JsonPathOp op, TValue value);

/// <summary>Order by a raw (scalar, non-wildcard) JSON-path string. Needs JsonPathSupport >= Basic;
/// a wildcard/recursive path throws ArgumentException.</summary>
public static IDocumentQuery<T> OrderByJsonPath<T>(
    this IDocumentQuery<T> query, string jsonPath, bool descending = false);

/// <summary>Project a single (scalar, non-wildcard) JSON path to a scalar sequence. Needs JsonPathSupport
/// >= Basic; a wildcard/recursive path throws ArgumentException.</summary>
public static IDocumentQuery<TValue> SelectJsonPath<T, TValue>(
    this IDocumentQuery<T> query, string jsonPath);
```

(Method placement mirrors the existing string `Where`/`OrderBy`/`Project` extensions in
`src/Shiny.DocumentDb/DocumentQueryExtensions.cs:24-250`.)

---

## Implementation sketch

Nearly all wiring lives in `src/Shiny.DocumentDb` (path classifier + query builder + emitter). The five
`Basic` providers add **only** `JsonPathSupport => JsonPathSupport.Basic` (one line each). The two `Full`
providers add `=> JsonPathSupport.Full` plus a `JsonPathNativePredicate` override.

### 1. Path classifier (`Internal/Query/JsonPathClassifier`)
A small parser classifies the raw path string and normalizes it:
- **Tier required:** no `[*]`/`..`/`?()` → `Basic` (scalar); exactly one `[*]` and no `..`/`?()` →
  `Basic` (single-wildcard); `..`, ≥2 wildcard levels, or an in-path `?()` filter → `Full`.
- **Shape:** for a single-wildcard `Basic` path, split into `(collectionPath, elementPath)` at the `[*]`
  — e.g. `$.lines[*].price` → `("lines", "price")`. Scalar paths keep the whole path.
- **`$.` normalization:** strip the leading `$.`/`$` before handing segments to the existing seam methods,
  which re-add `$.` (`JsonExtract`/`JsonEachFrom` prepend `'$.{jsonPath}'`, e.g. `SqliteDatabaseProvider.cs:161,177`).
  So the public API takes canonical `$....` paths but the internal seam gets the un-prefixed form it expects.
- **Gate:** compare the required tier against `provider.JsonPathSupport`; throw `NotSupportedException`
  (tier too low) or `ArgumentException` (wildcard/recursive path given to `OrderByJsonPath`/`SelectJsonPath`,
  which are scalar-only). `None` providers throw before any emission.

### 2. Lowering — `Basic` reuses the existing collection-`Any` IR
This is the crux of why `Basic` is nearly free. A single-wildcard `WhereJsonPath("$.lines[*].price", >, v)`
is the same shape as the typed `o.Lines.Any(l => l.price > v)`, which already lowers to `AnyNode` /
`CountSubqueryNode` (`SqlPredicateEmitter.cs:56,70,104`) built from `JsonEachFrom` + `ElementFieldNode`
(`JsonExtractElementTyped`). So:
- **Scalar `Basic` path** → a `RootFieldNode`-equivalent over the raw path, emitted via the existing
  `JsonExtractTyped("Data", path, clrType)` (`SqlPredicateEmitter.cs:64`), compared to `@pN`.
- **Single-wildcard `Basic` path** → synthesize an `AnyNode` whose `CollectionJsonPath = collectionPath`
  and whose inner predicate is `ElementFieldNode(elementPath, clrType) {op} @pN`. **No new emitter branch,
  no new provider method** — it rides `JsonEachFrom` + `JsonExtractElementTyped` verbatim.
- **`Full` path** → one new `JsonPathNativePredicateNode { Path; Op; ValueParam; ClrType }` with an emitter
  branch → `provider.JsonPathNativePredicate("Data", path, op, valueParam, clrType)`. Only reached when the
  classifier said `Full` and the provider is `Full`.

**The raw path string is never run through `JsonPropertyNameResolver`** — that is the whole difference from
the typed lane.

### 3. Parameter binding
`value` binds as a normal `@pN` parameter through the existing parameterization (respecting
`provider.NormalizeParameterValue`, `IDatabaseProvider.cs:205`). For `Full` native predicates the value is
passed into the path engine via its variable-binding form (Postgres `jsonb_path_exists(..., vars)`, Oracle
`PASSING @p AS "v"`), **never** string-concatenated into the path. The `jsonPath` string itself is trusted
input (like a raw-SQL fragment), not a place for end-user free text — document that.

### 4. `OrderByJsonPath` / `SelectJsonPath` (scalar-only)
Both require a scalar (non-wildcard, non-recursive) path — the classifier throws `ArgumentException`
otherwise. `OrderByJsonPath` appends `ORDER BY {JsonExtractTyped("Data", path, clrType)} ASC|DESC`.
`SelectJsonPath<TValue>` emits a single-column select of the same extract and materializes each row's scalar
to `TValue` via the existing scalar-read path used by `Max`/`Min`/`Sum`. Both work on any `>= Basic` provider
(scalar extract needs no shredding).

### 5. Provider overrides
- **`Basic` (one line each):** SQLite, SQLCipher, MySQL, SQL Server, DuckDB add
  `public JsonPathSupport JsonPathSupport => JsonPathSupport.Basic;`. Nothing else — the `Basic` emission is
  entirely core, over their existing `JsonEachFrom`/`JsonExtractElementTyped`.
- **`Full` (enum + one method):** PostgreSQL and Oracle set `=> JsonPathSupport.Full` and override
  `JsonPathNativePredicate`:
  - PostgreSQL — `jsonb_path_exists(Data, '<path>', jsonb_build_object('v', @p0))` with the comparison folded
    into the path predicate (`... ? (@ > $v)`) — `src/Shiny.DocumentDb.PostgreSql/PostgreSqlDatabaseProvider.cs`
  - Oracle — `JSON_EXISTS(Data, '<path>?(@ > $v)' PASSING @p0 AS "v")` — `src/Shiny.DocumentDb.Oracle/OracleDatabaseProvider.cs`
- **`None` (nothing):** Cosmos, MongoDB, LiteDB, IndexedDB inherit `None` and throw.

---

## Testing (`tests/Shiny.DocumentDb.Tests`, run the suite before "done")

Add a `JsonPathQueryTests` fixture parameterized across the in-repo providers.

**On `Basic`+ providers (SQLite/SQLCipher/MySQL/SQL Server/DuckDB/PostgreSQL/Oracle — all seven relational,
those with integration coverage in CI):**
- **Scalar path filter:** `WhereJsonPath("$.a.b", Equal, x)` returns the right docs; typed comparison correct
  for int/decimal/bool/string/DateTime.
- **Array-index path:** `"$.lines[0].price"` filters on the first element.
- **Single-wildcard existential:** `"$.lines[*].productName" == "Widget"` matches any element; a doc with no
  matching element is excluded; empty array excluded; `> / >= / < / <=` range comparisons work (this is the
  capability the earlier "wildcard-only" bar lacked — verify it on every `Basic` provider, i.e. that the
  `JsonEachFrom` lowering is correct).
- **OrderByJsonPath:** ascending/descending on `"$.shippingAddress.city"`; wildcard/recursive path throws
  `ArgumentException`.
- **SelectJsonPath:** single scalar path materializes to `IReadOnlyList<decimal>`; wildcard/recursive throws.
- **Raw-key contract:** a path using the CLR (PascalCase) name when JSON is camelCase returns nothing —
  pins the "physical keys, no naming policy" rule.
- **No JsonTypeInfo needed:** the whole fixture runs without passing any `JsonTypeInfo`/context.
- **Tier assertion:** `provider.JsonPathSupport` is `Basic` for SQLite/SQLCipher/MySQL/SQL Server/DuckDB.

**`Full`-only paths (recursive descent / multi-wildcard / in-path filter):**
- On **PostgreSQL / Oracle** (`Full`): a recursive-descent path (`"$..price" > x`) and a multi-wildcard path
  return correct results.
- On the five `Basic` providers: the **same** `Full` path throws `NotSupportedException` naming the
  offending feature — pins the tier boundary.
- `provider.JsonPathSupport` is `Full` for PostgreSQL/Oracle.

**On `None` providers (Cosmos, MongoDB, LiteDB, IndexedDB):**
- `WhereJsonPath` / `OrderByJsonPath` / `SelectJsonPath` each throw `NotSupportedException` (even for a
  trivial scalar path).
- `provider.JsonPathSupport` is `None`.

---

## The four-artifact checklist (per `CLAUDE.md`)

1. **Code + tests** — as above. This is a **backend-specific, tiered** feature — call out the tiers in the
   release note (`Basic` on all seven relational providers; `Full` additionally on PostgreSQL/Oracle; throws
   on Cosmos/Mongo/LiteDB/IndexedDB).
2. **Docs site** (`~/Desktop/dev/documentation/src/content/docs/documentdb/`) — add a "Raw JSON-path
   queries" section to `querying.mdx`: the three methods, the `JsonPathSupport` tier matrix table, the
   `Basic` vs `Full` path-grammar boundary, the raw-key/no-naming-policy caveat, the scalar-only rule for
   `OrderByJsonPath`/`SelectJsonPath`, and the "throws below required tier" contract. Add a **release note**
   under `## 10.0 TBD` in `release-notes.mdx`:
   `<RN type="feature">WhereJsonPath / OrderByJsonPath / SelectJsonPath — raw JSON-path queries; Basic tier (scalar + single-wildcard) on all relational providers, Full tier (recursive / multi-wildcard / in-path filters) on PostgreSQL and Oracle …</RN>`.
   **While there, add the JSON-path-query compatibility table to `querying.mdx`** (the earlier gap: the
   provider tiering currently lives only in code via `IDatabaseProvider` flags).
3. **Skill** (`skills/shiny-documentdb/SKILL.md`) — add the three signatures, the `JsonPathSupport` tier
   matrix, the `Basic`/`Full` boundary, the raw-key contract, and the "throws below required tier" rule. Add
   keywords (`WhereJsonPath`, `OrderByJsonPath`, `SelectJsonPath`, `JsonPath`, `JsonPathSupport`) to `triggers:`.
4. **readme.md** (repo root) — add raw JSON-path querying (with the tiered provider list) to the feature list.

---

## Open questions (resolve during build, none block design)

- **Existential semantics for `NotEqual` on a single-wildcard `Basic` path.** `"$.lines[*].x != y"` is
  ambiguous (no element equals `y`, vs. some element `!= y`). Because `Basic` lowers to the collection-`Any`
  IR, lock it to **"NOT Any(element == value)"** = "no element at the path equals `value`", and document;
  revisit if a caller needs the other reading.
- **`Contains`/`StartsWith`/`EndsWith` on `Basic`.** These already have `LIKE` lowering in the element
  predicate path (same as typed `.Any(l => l.Name.Contains(...))`), so they should fall out of the
  collection-`Any` reuse for free — verify per provider and add cases.
- **`ToQueryString()` support.** `Basic` builds normal SQL (json_each + EXISTS) and `Full` builds a native
  path predicate, so `ToQueryString()` should work on all seven relational providers — verify it renders the
  fragment + parameters for both tiers and add cases.
- **Classifier edge cases.** Bracketed quoted keys (`$.["a.b"]`), negative/`last` array indices, and
  whitespace in `?()` filters — decide accept-vs-reject at build; default to a clear `ArgumentException` for
  anything the classifier can't confidently map, rather than emitting wrong SQL.
- **`SelectJsonPath` type-arg ergonomics.** As `SelectJsonPath<T, TValue>` you can't specify only `TValue`
  (C# infers extension type args all-or-nothing), so callers must write `SelectJsonPath<Order, decimal>`.
  Options: (a) accept the two explicit args; (b) add a non-generic `SelectJsonPath(string)` returning
  `IDocumentQuery<JsonNode>` and let the caller read the scalar; (c) expose it on the concrete query type
  where `T` is already bound so only `<TValue>` is needed. Decide at build; (a) is the simplest and matches
  the plan's examples.
