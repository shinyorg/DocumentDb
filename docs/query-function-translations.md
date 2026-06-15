# Query Function Translations — Capability Matrix & Scoped Plan

This document is the design contract and roadmap for expanding the set of BCL methods/operators that
`Shiny.DocumentDb` can translate into native provider queries — the equivalent of Npgsql's
[supported translations](https://www.npgsql.org/efcore/mapping/translations.html) page, scoped to a
schema-free JSON document store across many backends.

It is a cross-session working doc. Update the matrix as functions land; promote sections into the
public docs site (`querying.mdx`) and `skills/shiny-documentdb/SKILL.md` as tiers ship.

## Implementation status (2026-06-14)

**Landed & verified on real containers** — all 6 relational providers (SQLite, DuckDB, MySQL, PostgreSQL,
SQL Server, Oracle) pass the full scalar suite 16/16; MongoDB `HasFlag` (→ `$bitsAllSet`) passes 2/2;
plus LiteDB in-memory and the custom-translation test:
- Shared query IR + `ExpressionLowerer` (recognition) + `SqlPredicateEmitter` (emission) under
  `src/Shiny.DocumentDb/Internal/Query/`. `JsonExpressionVisitor` is now a thin `Lower → Emit` façade —
  the relational `Where` path runs entirely through the IR (parity preserved).
- Dialect seam on `IDatabaseProvider`: `TranslateScalar` (+ `ScalarSqlDefaults`), `BitAnd`, `CastInteger`,
  `SupportsSoundex`, `SupportsUserFunctions`. Per-provider overrides for all 6 relational providers.
- String functions (`ToLower/ToUpper`, `Length`, `Trim*`, `Substring`, `Replace`, `IndexOf`,
  `IsNullOrEmpty`, `+`), `Math.*` (`Abs/Round/Ceiling/Floor/Sqrt/Pow/Sign`), flag enums (`HasFlag` +
  `(x & f) == f`), and `DocumentFunctions.Soundex` (canonical `Phonetics` impl + SQLite connection UDF).
- Tests across SQLite + DuckDB; container-provider dialects wired into their suites (validated by CI).
- Four artifacts synced: this doc, `SKILL.md`, `readme.md`, documentation release note (7.2).

**Not yet done** (tracked for follow-up):
- **Date/time functions** — `ScalarFn` entries + ANSI default exist, but no `DateTime` lowering and no
  SQLite `strftime` override; deferred (needs per-engine date-string parsing).
- **`MapFunctionTranslation` (Level A extensibility)** — registry/API not built; needs the registry
  threaded through the `Translate` call sites.
- **Cosmos / Mongo scalar functions** — these still use their standalone visitors. MongoDB `HasFlag`
  (→ `$bitsAllSet`) is implemented and verified; full string/math/date scalar translation (needs Mongo
  `$expr`) and the Cosmos scalar/flag support are **deferred**. Cosmos `HasFlag` was attempted but the
  emulator returned empty (the Cosmos SDK's enum serialization doesn't line up with a numeric bitwise
  test), so it throws `NotSupportedException` rather than silently mis-match — revisit with the full IR
  refit + a `CosmosSqlDialect`.
- **In-memory compile-free evaluator & selector/projection pushdown** — `Expression.Compile()` still used
  for selectors and the in-memory providers; full end-to-end AOT not yet realized.

## The translation backends

There are **three real translating backends** plus **two in-memory** ones. Work only touches the
three translating paths; the in-memory paths get every BCL method for free.

| Path | Code | Providers | Strategy |
|---|---|---|---|
| Relational SQL | `JsonExpressionVisitor` + `IDatabaseProvider` dialect fragments | SQLite (+SqlCipher), DuckDb, MySql, Oracle, PostgreSql, SqlServer | `json_extract`-based SQL via provider fragments |
| Cosmos | `CosmosExpressionVisitor` | CosmosDb | Own Cosmos-SQL dialect |
| Mongo | `MongoExpressionVisitor` | MongoDb | Builds BSON `FilterDefinition` |
| In-memory (no work) | `LiteDbDocumentQuery`, `IndexedDbDocumentQuery` | LiteDb, IndexedDb | `predicate.Compile()` + LINQ-to-Objects |

The string-filter parser (`FilterExpressionParser`) is a *front end* that builds an
`Expression<Func<T,bool>>`, which then flows through whichever backend above applies. New translations
added to the visitors are automatically reachable from the string-filter surface, but the parser's own
grammar (`contains()/startswith()/endswith()`, `in`, `is null`) must be extended separately if we want
the new functions usable from filter strings.

## Two structural complications (why this costs more than Npgsql)

1. **Everything is JSON text.** `json_extract` returns text/affinity-typed values, not typed columns.
   Math and date functions need an explicit `CAST` on each operand — `Math.Round(x.Price)` becomes
   `ROUND(CAST(json_extract(Data,'$.price') AS REAL))`, and date-part access means parsing the ISO-8601
   string `System.Text.Json` wrote. The existing value-side coercion (`NormalizeValue`,
   `FormatDateTime`, Guid-as-string, enum-as-int in `JsonExpressionVisitor`) now has to be mirrored on
   the *column* side, per function, per engine.

2. **Mongo's `find` filter can't express scalar transforms.** `ToLower`, `Substring`, math, and
   date-parts are not legal in a normal Mongo query filter. They require `$expr` + the aggregation
   language (`$toLower`, `$strLenCP`, `$substrCP`, `$year`, …), or `$regex` for the prefix/substring
   cases already special-cased. The Mongo path therefore needs a second sub-mode (predicate → `$expr`),
   which is the single biggest lift in the whole effort.

## Current capability matrix

Legend: ✅ translated · ⚠️ partial / one path only · ❌ not translated (throws `NotSupportedException`
on the three query paths; works in-memory on LiteDb/IndexedDb).

| Feature | Relational | Cosmos | Mongo | Notes |
|---|---|---|---|---|
| `== != < <= > >=` | ✅ | ✅ | ✅ | |
| `&& \|\| !` | ✅ | ✅ | ✅ | |
| null / `is null` checks | ✅ | ✅ | ✅ | Cosmos distinguishes `IS_DEFINED`/`IS_NULL` |
| `string.Contains/StartsWith/EndsWith` | ✅ | ✅ | ✅ | Relational via `LIKE`; Cosmos via `CONTAINS`/`STARTSWITH`/`ENDSWITH` |
| `string.ToUpper/ToLower` | ❌ | ✅ | ❌ | Cosmos only today |
| `Enumerable.Contains` → `IN` | ✅ | ✅ | ✅ | |
| `Any` / `Any(pred)` | ✅ | ✅ | ✅ | json_each / EXISTS / `$elemMatch` |
| `Count` / `Count(pred)` | ✅ | ✅ | ✅ | |
| collection `.Count` / `.Length` | ✅ | ✅ | ✅ | `json_array_length` / `ARRAY_LENGTH` |
| enum equality (`x.Status == E.A`) | ✅ | ✅ | ✅ | numeric storage; `NormalizeValue` boxes enum→int |
| flag/bitwise enum (`HasFlag`, `&`) | ❌ | ❌ | ❌ | see Tier 1.5 |

Everything below this line is currently ❌ on all three translating paths.

## Target tiers

### Tier 1 — string operations (portable, high demand)

| BCL member | SQLite | DuckDb | MySql | Oracle | SqlServer | Cosmos | Mongo (`$expr`) |
|---|---|---|---|---|---|---|---|
| `ToLower/ToUpper` | `lower/upper` | `lower/upper` | `LOWER/UPPER` | `LOWER/UPPER` | `LOWER/UPPER` | `LOWER/UPPER` ✅ | `$toLower/$toUpper` |
| `Length` | `length` | `length` | `CHAR_LENGTH` | `LENGTH` | `LEN` | `LENGTH` | `$strLenCP` |
| `Trim/TrimStart/TrimEnd` | `trim/ltrim/rtrim` | same | same | same | `TRIM/LTRIM/RTRIM` | `TRIM/LTRIM/RTRIM` | `$trim/$ltrim/$rtrim` |
| `Substring` | `substr` (1-based) | `substr` | `SUBSTRING` | `SUBSTR` | `SUBSTRING` | `SUBSTRING` (0-based) | `$substrCP` | 
| `Replace` | `replace` | `replace` | `REPLACE` | `REPLACE` | `REPLACE` | `REPLACE` | `$replaceAll` |
| `IsNullOrEmpty` | composed | composed | composed | composed | composed | composed | composed |
| `IndexOf` | `instr` | `instr` | `LOCATE` | `INSTR` | `CHARINDEX` | `INDEX_OF` | `$indexOfCP` |
| `Contains/StartsWith/EndsWith` w/ `StringComparison` | upgrade existing | | | | | | | 

**Watch-outs:** `Substring` index base differs (1-based SQL vs 0-based Cosmos/.NET) — normalize in the
visitor, not per provider. `LIKE` is case-insensitive by default on some engines and not others;
`StringComparison.OrdinalIgnoreCase` should map to `LOWER(x) LIKE LOWER(y)` consistently rather than
relying on collation.

### Tier 2 — math (portable, needs CAST)

`Math.Abs/Ceiling/Floor/Round/Sqrt/Pow/Sign/Log/Log10/Exp`, trig (`Sin/Cos/Tan`), `Math.Max/Min`,
modulo. All map to standard SQL math functions but **every operand must be cast to a numeric type
first** because it arrives as `json_extract` text. Mongo needs `$expr` with `$abs/$ceil/$floor/$round/
$sqrt/$pow/$ln/$log10/$exp/$mod/$max/$min`. `Math.Round(x, digits)` and `MidpointRounding` overloads:
decide whether to support the 2-arg form or throw.

### Tier 3 — date/time (highest friction)

Component access (`Year/Month/Day/Hour/Minute/Second/DayOfWeek`), `DateTime.UtcNow/Now/Today`,
`AddDays/AddHours/AddMonths/AddYears`, `DateOnly`/`TimeOnly`, and date differences. Friction comes from
the ISO-8601 **string** stored in JSON: SQLite uses `strftime`, others have `EXTRACT`/`DATEPART`/`YEAR`,
and each parses the stored string differently (some need `CAST(... AS TIMESTAMP)` first, some need
substring slicing). `UtcNow`/`Now` are parameterized at translation time (a captured constant) rather
than a SQL `now()` — decide whether server-evaluated `now()` is ever wanted. Mongo needs `$expr` +
`$year/$month/$dayOfMonth/$hour/$dateAdd/$dateDiff`.

### Tier 1.5 — flag / bitwise enum querying (self-contained, high value)

Enums (including `[Flags]`) already serialize as their **numeric combined value** — `System.Text.Json`'s
default, and no `JsonStringEnumConverter` is registered anywhere in the codebase. `NormalizeValue` in
`JsonExpressionVisitor` already boxes enum *constants* to their underlying integer, so the value side of
a comparison is numeric. That numeric storage is the prerequisite for bitwise querying and it is already
satisfied. **Querying** by flags, however, is not translated — both CLR spellings throw today:

1. `x.Permissions.HasFlag(Permission.Write)` — a `MethodCallExpression` unhandled by `VisitMethodCall`.
2. `(x.Permissions & Permission.Write) == Permission.Write` (and the `... != 0` idiom) — `VisitBinary`'s
   operator switch has no `ExpressionType.And/Or/ExclusiveOr` cases.

Both spellings should normalize to the **same** translation (a "has-all-bits" test), so `HasFlag(f)` and
`(x & f) == f` emit identical SQL/BSON.

| Backend | `HasFlag(f)` → | Notes |
|---|---|---|
| SQLite, DuckDb, MySql, PostgreSql, SqlServer | `(extract & @f) = @f` | native `&`; needs an **integer** cast — existing `JsonExtractNumeric` casts to REAL, which won't bitwise |
| Oracle | `BITAND(extract, @f) = @f` | **no `&` operator** — the reason a `BitAnd` seam is needed, not a hardcoded `&` |
| Cosmos | `(extract & @f) = @f` | Cosmos SQL has bitwise operators |
| Mongo | `{ field: { $bitsAllSet: @f } }` | native query operator — cleanest, no `$expr` |

**Implementation:** add a `BitAnd(string left, string right)` member to the scalar seam (default `"{l} & {r}"`,
Oracle overrides to `BITAND(...)`); add an integer-cast extraction (or a `clrType.IsEnum` branch in
`JsonExtractTyped`) so the operand has integer affinity; recognize `Enum.HasFlag` in `VisitMethodCall`
and the `(x & f)` shape in `VisitBinary`; Mongo emits `$bitsAllSet`. ~2–3 days across all three paths
incl. tests.

**Caveat to document:** querying flags requires the **numeric** JSON representation (the default). If a
consumer enables string enum conversion (`"Read, Write"`), bitwise querying is unsupported — the visitor
should throw a clear message rather than silently mis-match. Detecting the converter at translation time
is awkward (it's a serializer option, not visible on the member), so the safe contract is: document the
requirement, and treat any non-numeric stored value as a translation error at runtime.

### Out of scope (document explicitly)

PostgreSQL-only exotica from the Npgsql page — network (`inet`/`cidr`), trigram (`pg_trgm`), `ltree`,
`cube`, row-value/tuple comparisons, and aggregate regression/statistical functions. None are portable
across SQLite/Mongo/Cosmos/Oracle. They remain available in-memory on LiteDb/IndexedDb only. Full-text
search, if wanted, deserves its own design pass (mirroring `docs/vector-support.md`), not the
scalar-translation mechanism.

Fuzzy-string functions (Soundex/Levenshtein/Metaphone) are **not** in this bucket — they are delivered
via the provider ladder below.

## Architecture: split recognition from emission

There are **three translating backends with three different output shapes** — relational and Cosmos both
build `(string sql, params)`, Mongo builds a `FilterDefinition<BsonDocument>` tree — plus two in-memory
backends (LiteDb, IndexedDb) that compile the lambda and need **no** translation. A single seam on
`IDatabaseProvider` would only reach the 6 relational providers (Cosmos/Mongo don't implement it), so the
design splits the two jobs every visitor currently tangles together:

- **Recognition (shared, backend-agnostic).** Walk the LINQ tree and lower it to a small internal node
  set: member access → JSON path, method/operator → a *logical* operation (`ScalarFn` + args, plus
  `BitAnd` etc.), constant → value (via the compile-free closure walk — see AOT below). This is identical
  across every backend and is the single place custom translations register.
- **Emission (per backend).** Render a logical node into the backend's form. Relational and Cosmos share a
  string builder driven by a per-provider **dialect table** (logical op → template); Mongo renders to a
  `FilterDefinition`. Argument normalization (Substring index-base, CAST insertion, null handling) lives
  in the recognition layer so emitters only supply spelling.

Do **not** add one `IDatabaseProvider` method per function (~30 members already; a dozen
`StringLower/MathAbs/DatePart…` stubs across six providers is the wrong shape). The relational/Cosmos
emitter consults a dialect table:

```csharp
// Per provider: one table, not N interface methods.
// Default table = ANSI-portable rendering; a provider overrides only entries whose dialect differs
// (SqlServer LEN/CHARINDEX, MySql CHAR_LENGTH/LOCATE, Oracle BITAND, …).
string TranslateScalar(ScalarFn fn, IReadOnlyList<string> argSql, Type resultType);
```

Retrofit the existing `ConcatStrings` and string-`LIKE` logic onto this seam so there is one path.

## Extensibility: registering custom translations

Two levels, very different cost/risk:

**Level A — function-registration hook (the primary public surface).** Backend-agnostic registration of a
method/property → logical op, rooted by an expression exemplar so it stays trim/AOT-safe (no
`Type.GetMethod("name")`):

```csharp
options.MapFunctionTranslation(
    () => default(MyType)!.Foo(default!),   // MethodInfo pulled from the tree; statically rooted
    ScalarFn.Custom("FOO", argCount: 1));    // dialect tables render it across all SQL backends + Cosmos
```

One registration covers all 6 relational providers + Cosmos via the dialect table. Mongo either gets a
parallel registration or throws a clear message. In-memory backends run the method directly.

**Level B — bring-your-own translator (advanced, opt-in).** A public `IMethodCallTranslator` /
`IMemberTranslator` registry consulted by the recognition layer before it throws. More power, but it makes
the pipeline a **versioned public contract**. A custom translator targets an output *family* (SQL-string
vs BSON), not "all layers" — be explicit about that. Scope the first cut to the SQL-string family
(relational + Cosmos, one builder abstraction); Mongo is a separate translator interface.

## Non-portable functions: the provider ladder (worked example: Soundex)

Some functions have no single portable spelling — phonetic/fuzzy matching is the canonical case. Rather
than dump them as "out of scope," handle them with a **capability ladder** that rides the same
extensibility + per-connection seams already in the design. Soundex is the reference implementation;
Levenshtein/Metaphone follow the identical pattern.

Native support is uneven: SQL Server / MySQL / Oracle have a built-in `SOUNDEX()`; PostgreSQL exposes
`soundex()` only via the `fuzzystrmatch` extension; **SQLite ships it off by default** (needs the
`SQLITE_SOUNDEX` compile flag, which the bundled `e_sqlite3` does not set); **DuckDB has no core
soundex** (only the `splink_udfs` community extension); Cosmos and Mongo have nothing.

**One AOT-safe C# implementation backs every mechanism:**

| Backend | Emission |
|---|---|
| SqlServer, MySql, Oracle | native `SOUNDEX(col)` |
| PostgreSql | `soundex(col)`, gated on a `SupportsSoundex` capability flag (`fuzzystrmatch` registered) |
| SQLite, DuckDb | register the C# delegate as a connection UDF named `soundex` in `InitializeConnectionAsync` (same per-connection seam as `LoadVectorExtensionAsync`; `Microsoft.Data.Sqlite.CreateFunction` takes a delegate — no codegen, AOT-safe), then emit `soundex(col)` |
| Cosmos, Mongo | no native / no scalar UDF → rewrite against a **computed stored field** (below), else throw with a message pointing at it |
| LiteDb, IndexedDb | in-memory — the C# method runs directly, free |

The marker method (`DocumentFunctions.Soundex(string)`, EF-`EF.Functions`-style) is matched by
`MethodInfo` in the recognition layer; the same delegate is what gets registered as the UDF and what
computes the stored field. No reflection-by-name, no `Compile()` — consistent with the AOT rules above.

**Recommended portable pattern — computed stored key.** For a document store the best-performing
phonetic search is to materialize `NameSoundex` on write (the auto-compute-on-write precedent from vector
auto-embedding) and query it by plain equality — pushes down **and indexes** on all 10 backends. Native /
UDF `SOUNDEX()` is the convenience layer for ad-hoc queries; the stored key is the scale answer, and it is
the *only* mechanism for Cosmos/Mongo. The same C# soundex impl computes the field.

## Untranslatable contract

Decide **per (function × provider)** the behavior when a function cannot be pushed down:

- **Throw `NotSupportedException`** (today's behavior) — safe, predictable, no silent perf cliff.
- **Fall back to in-memory** — fetch-then-filter; convenient but a hidden fetch-all on large sets.

Recommendation: default to **throw** for the translating paths (pushdown-or-fail), and make in-memory
fallback an *opt-in* per-query flag, never the silent default. Document the chosen behavior in the
capability matrix so generated code (via the skill) knows what's safe.

## AOT / trimming compliance

The translation layer **is** AOT/trim-safe, and the build enforces it: `Directory.Build.props` sets
`EnableAotAnalyzer` and `EnableTrimAnalyzer` solution-wide (~98 existing annotation sites), so any new hole
surfaces as a CI warning. The relational predicate path already proves the pattern — `JsonExpressionVisitor`
and `FilterExpressionParser` are compile-free ("never calls `Compile()`"), paths resolve through
source-generated `JsonTypeInfo.Properties`, captured values come from an annotated reflection *walk*
(`TryExtractCapturedValue`), and `InExpressionBuilder` avoids `MakeGenericMethod`. Recognition (MethodInfo
matching) and emission (string/BSON building) add no dynamic codegen.

**Rules the implementation must hold to stay compliant:**

1. Match by `MethodInfo`, never `Type.GetMethod("name")`. The `MapFunctionTranslation` API takes an
   expression exemplar so the user's method is statically rooted and the `MethodInfo` is read from the tree.
2. No `MakeGenericMethod` / `MakeGenericType` in emission (keep the `InExpressionBuilder` discipline).
3. Explicit per-type coercion switches (as `CoerceLiteral` does), not reflection-driven `Convert`.
4. Annotate the one unavoidable closure-value walk with the existing `[UnconditionalSuppressMessage]`
   pattern.

**This work improves AOT posture.** Two existing IL3050 holes — `CosmosExpressionVisitor.cs:289` and
`MongoExpressionVisitor.cs:255`, both extracting captured constants via `Expression.Lambda(...).Compile()
.DynamicInvoke()` — are replaced by the shared compile-free closure walk during the recognition unification.

**End-to-end AOT is in scope (decided).** This work goes fully NativeAOT-clean, not just the
`Where`-predicate path. Today the surrounding query stack still calls `Expression.Compile()` for
**selectors** (OrderBy / projection / GroupBy, in every provider via core `DocumentQuery`) and for
**client-side predicates** (in-memory LiteDb/IndexedDb, change-feed notification filters, global query
filters), all of which warn IL3050 under NativeAOT. The target is to eliminate `Expression.Compile()`
across the query surface:

- **Pushdown backends** (6 relational + Cosmos + Mongo): translate `OrderBy`/projection/`GroupBy` into the
  native query language (`ORDER BY` / `$sort` / projection lists) instead of compiling a selector. *Note:*
  SQL `ORDER BY` shifts null-ordering and string-collation semantics vs the current in-memory sort — call
  it out in the release note.
- **In-memory backends** (LiteDb, IndexedDb) and unavoidably-client-side paths (change feed, global
  filters): these cannot push down. Replace `Expression.Compile()` with a **compile-free tree-walking
  evaluator** for predicates and selectors. This is the long pole of "full AOT across all 10" — it is
  effectively a small LINQ-to-objects interpreter. (Fallback if descoped: keep `Compile()` here and accept
  the runtime interpreter + a localized `[RequiresDynamicCode]`/suppression — but the decision is to build
  the evaluator.)

## Delivery (full scope, not phased)

Build the complete layer in one workstream. The recognition/emission split + dialect-table seam is
foundational and is written first as architecture (retrofitting `ConcatStrings`/`LIKE`), then all target
tiers land on it across the three translating paths with tests across every provider
(`tests/Shiny.DocumentDb.Tests`, plus the Mongo `$expr` sub-mode):

- **Strings** (Tier 1) — `ToLower/ToUpper`, `Length`, `Trim*`, `Substring`, `Replace`, `IsNullOrEmpty`,
  `IndexOf`, `Concat`, `StringComparison` overloads.
- **Flag/bitwise enums** (Tier 1.5) — `HasFlag` + `(x & f) == f`, `BitAnd` seam, Mongo `$bitsAllSet`.
- **Math** (Tier 2) — `Abs/Ceiling/Floor/Round/Sqrt/Pow/Sign/Log/Exp`, trig, `Max/Min`, modulo; CAST helper.
- **Date/time** (Tier 3) — component access, `UtcNow/Now/Today`, `Add*`, `DateOnly`/`TimeOnly`, diffs.
- **Fuzzy/phonetic (provider ladder)** — `Soundex` first (native / extension / connection-UDF /
  computed-field / in-memory), then `Levenshtein`/`Metaphone` on the same ladder; `DocumentFunctions`
  marker methods + a compute-on-write mapping for the stored-key pattern.
- **Extensibility** — `MapFunctionTranslation` (Level A); `IMethodCallTranslator`/`IMemberTranslator`
  registry for the SQL-string family (Level B).
- **Selector/projection pushdown** — translate `OrderBy`/projection/`GroupBy` into native query syntax on
  the pushdown backends (removes the selector `Compile()` across all providers via core `DocumentQuery`).
- **Compile-free in-memory evaluator** — tree-walking predicate/selector evaluator for LiteDb, IndexedDb,
  and the client-side change-feed / global-filter paths, retiring the last `Expression.Compile()` calls.

Each follows the repo's "four artifacts in sync" rule (code+tests, documentation site release note,
`SKILL.md` triggers + supported-translations table, `readme.md` feature list). The tier labels above are a
capability taxonomy for the matrix, not a delivery sequence.

## Rough effort (sizing, not sequencing)

| Item | Estimate |
|---|---|
| Recognition/emission seam + dialect tables (retrofit `ConcatStrings`/`LIKE`) | 3–5 days |
| String fn (each) | ~0.5 day (3 paths + ~5 dialect overrides + tests) |
| Flag/bitwise enum (whole feature) | 2–3 days |
| Math fn (each) | ~0.75 day (adds CAST handling) |
| Date fn (each) | ~1–1.5 days (string parsing + Mongo `$expr`) |
| `MapFunctionTranslation` (Level A) | 2–3 days |
| `IMethodCallTranslator` registry (Level B, SQL-string family) | 3–5 days |
| Soundex ladder (C# impl + native/UDF/extension emission + tests) | 3–4 days |
| Compute-on-write stored key (mapping API + Cosmos/Mongo path) | 2–3 days |
| Additional fuzzy fns on the ladder (Levenshtein/Metaphone, each) | ~1 day |
| Selector/projection pushdown (OrderBy/projection/GroupBy → native) | 4–6 days |
| Compile-free in-memory evaluator (LiteDb/IndexedDb + client-side paths) | 5–8 days |

## Implementation & file layout

The design centers on a small **query IR** in core that every backend lowers to (shared recognition) and
emits from (per-backend). New code clusters under `src/Shiny.DocumentDb/Internal/Query/`; the three
visitors become thin lower→emit wrappers.

### New — core (`src/Shiny.DocumentDb/`)

```
Internal/Query/
  QueryNode.cs              # PredicateNode/ValueNode abstract bases (records below)
  CompareOp.cs, LikeKind.cs # enums
  ScalarFn.cs               # enum: Lower/Upper/Length/Trim*/Substring/Replace/IndexOf/Concat,
                            #       Abs/Ceiling/Floor/Round/Sqrt/Pow/Sign/Log/Exp/Trig/Mod/Max/Min,
                            #       Year/Month/Day/Hour/.../DateAdd/DateDiff/UtcNow, Soundex/Levenshtein/Metaphone, Custom
  ScalarFnDescriptor.cs     # custom fn: name, arity, result type
  ExpressionLowerer.cs      # Lower<T>(Expression<Func<T,bool>>, JsonTypeInfo<T>, FunctionTranslationRegistry) -> PredicateNode
  ClosureValueExtractor.cs  # compile-free TryExtractCapturedValue (lifted out of JsonExpressionVisitor)
  FunctionTranslationRegistry.cs  # MethodInfo/MemberInfo -> ScalarFn | custom; built-in BCL map + user regs
  SqlPredicateEmitter.cs    # Emit(PredicateNode, ISqlDialect, JsonTypeInfo) -> (sql, params)  [relational + Cosmos]
  ExpressionInterpreter.cs  # compile-free general tree-walker for selectors + in-memory predicates
ISqlDialect.cs              # string-emission dialect contract (scalar templates, json extract, bitand, like, concat, param style)
DocumentFunctions.cs        # PUBLIC marker methods (Soundex/Levenshtein/Metaphone) — real bodies so in-memory just runs them
Internal/Phonetics.cs       # AOT-safe Soundex/Levenshtein/Metaphone implementations
IMethodCallTranslator.cs, IMemberTranslator.cs  # PUBLIC Level-B extensibility (SQL-string family)
Internal/ScalarFunctionInstaller.cs  # collects connection UDFs; providers that SupportsUserFunctions install them
```

IR shape (records):

```csharp
abstract record ValueNode;
sealed record FieldNode(IReadOnlyList<string> Path, string JsonPath, Type ClrType) : ValueNode;
sealed record ConstantNode(object? Value, Type ClrType) : ValueNode;
sealed record ScalarFnNode(ScalarFn Fn, IReadOnlyList<ValueNode> Args, Type ResultType) : ValueNode;
sealed record BitAndNode(ValueNode Left, ValueNode Right) : ValueNode;
sealed record ArrayLengthNode(FieldNode Collection) : ValueNode;

abstract record PredicateNode;
sealed record AndNode(PredicateNode L, PredicateNode R) : PredicateNode;     // + OrNode, NotNode
sealed record CompareNode(CompareOp Op, ValueNode Left, ValueNode Right) : PredicateNode;
sealed record NullCheckNode(ValueNode Target, bool IsNull) : PredicateNode;
sealed record InNode(ValueNode Target, IReadOnlyList<object?> Values, NullHandling Null) : PredicateNode;
sealed record LikeNode(ValueNode Target, string Pattern, LikeKind Kind) : PredicateNode;
sealed record HasFlagNode(ValueNode Field, long Mask) : PredicateNode;       // (x & f)==f and HasFlag both lower here
sealed record AnyNode(FieldNode Collection, PredicateNode? Predicate) : PredicateNode;  // + CountCompareNode
```

Dialect seam (string family):

```csharp
public interface ISqlDialect
{
    string JsonExtractTyped(string column, string jsonPath, Type clrType);
    string TranslateScalar(ScalarFn fn, IReadOnlyList<string> argSql, Type resultType); // default = ANSI; provider overrides differences
    string BitAnd(string left, string right);            // default "({l} & {r})"; Oracle -> "BITAND({l},{r})"
    string Like(string target, string pattern, LikeKind kind);
    string Concat(params string[] parts);
    string NullCheck(string sql, bool isNull);
    string Parameter(int index);                         // @pN  / Cosmos @pN
    bool SupportsSoundex => false;                        // Postgres flips true when fuzzystrmatch present
}
```

### Modified — core

- `IDatabaseProvider.cs` — `: ISqlDialect`; existing JSON/concat members become the dialect surface; add the
  new members with ANSI defaults; add `bool SupportsUserFunctions => false`.
- `Internal/JsonExpressionVisitor.cs` — gutted to `ExpressionLowerer.Lower(...)` → `SqlPredicateEmitter.Emit(...)`;
  keep `NormalizeValue`/date formatting (move to a shared `ValueNormalizer`).
- `DocumentStoreOptions.cs` — add `MapFunctionTranslation(...)` (Level A), `MapComputedProperty<T>(target, compute)`
  (thin wrapper over existing `OnBeforeInsert`), and the `FunctionTranslationRegistry` instance.
- `Internal/DocumentQuery.cs` — replace the change-feed `CombinePredicates(...).Compile()` with `ExpressionInterpreter`.
- `Sql.cs` / `DocumentQueryExtensions.cs` — route string-filter parser output through the same registry.

### Modified — relational providers (6)

Each `*DatabaseProvider.cs`: implement the `ISqlDialect` deltas only (a `static` template table). Oracle overrides
`BitAnd`→`BITAND` and dialect spellings (`LEN`/`SUBSTR`/date `EXTRACT`); SqlServer `LEN`/`CHARINDEX`; MySql
`CHAR_LENGTH`/`LOCATE`; PostgreSql sets `SupportsSoundex` when `fuzzystrmatch` is provisioned.
**SQLite + DuckDb** additionally override `SupportsUserFunctions => true` and register the `Phonetics.Soundex`
delegate via `connection.CreateFunction("soundex", …)` inside their existing `InitializeConnectionAsync`.

### Modified — Cosmos / Mongo

- `CosmosSqlDialect.cs` (new) `: ISqlDialect` with `c.data`-prefixed paths and Cosmos function names; `CosmosExpressionVisitor`
  → lower + `SqlPredicateEmitter`. Selector/projection pushdown into Cosmos `ORDER BY`/projection.
- `MongoFilterEmitter.cs` (new) — `PredicateNode` → `FilterDefinition<BsonDocument>`; `$expr` sub-mode for scalar fns,
  `$bitsAllSet` for `HasFlagNode`. `MongoExpressionVisitor` → lower + emit. `BuildSort()` already pushes simple sorts.

### Modified — in-memory (LiteDb, IndexedDb)

Replace every predicate/selector `.Compile()` in `*DocumentQuery.cs` and `*DocumentStore.cs` with
`ExpressionInterpreter` calls (removes the last `Expression.Compile()` / IL3050 sites).

### Tests (`tests/Shiny.DocumentDb.Tests/`)

`Query/ScalarFunctionTests.cs`, `FlagEnumQueryTests.cs`, `SoundexTests.cs`, `OrderByPushdownTests.cs`,
`InterpreterTests.cs`, `CustomTranslationTests.cs`; extend `Fixtures/TestModels.cs` (a `[Flags]` enum + a
phonetic field) and `Fixtures/TestJsonContext.cs`. Run against every provider via the existing harness.

### String front-end & projections converge on the IR

- **String `Where` filters** (`FilterExpressionParser`) build an `Expression<Func<T,bool>>` that flows through
  `ExpressionLowerer` like any other predicate — so they ride the IR for **free** once `JsonExpressionVisitor`
  delegates (done). To surface new functions in the *grammar*, either emit `DocumentFunctions.X(...)` calls
  (Expression bridge, no new infra) or have the parser emit `ScalarFnNode` directly (viable because the IR is
  also in-memory-interpretable).
- **Projections** (`Project(string fields)` and expression selectors) currently go through `ProjectionTranslator`,
  which *duplicates* the where-path value resolution. A projection is just a list of `ValueNode`s → a SELECT
  list, so extract `SqlPredicateEmitter.EmitValue` into a shared `SqlValueEmitter` and lower both projection
  forms to `ValueNode`s. Net **code deletion** (removes the duplication) and projections inherit every scalar
  function automatically. (Resolves open question 2's "expose functions in strings" via the bridge.)

### Build order (dependency, not phases)

1. IR + `ExpressionLowerer` + `ClosureValueExtractor` + `FunctionTranslationRegistry`.
2. `ISqlDialect` + `SqlPredicateEmitter`; refit `IDatabaseProvider`/relational providers; gut `JsonExpressionVisitor`. (Parity checkpoint: existing tests green.)
3. `CosmosSqlDialect` + `MongoFilterEmitter`; refit both visitors.
4. Scalar fns (strings → flag enums → math → date) populate `ScalarFn` + dialect tables + emitters.
5. `DocumentFunctions`/`Phonetics` + UDF installer + `SupportsSoundex` + `MapComputedProperty`.
6. `ExpressionInterpreter`; cut LiteDb/IndexedDb/change-feed off `Compile()`.
7. Selector/projection pushdown (relational + Cosmos).
8. `MapFunctionTranslation` then `IMethodCallTranslator` registry.

## Open questions

1. Server-evaluated `now()` vs captured-constant `UtcNow` — do any callers need the DB clock?
2. Do we extend `FilterExpressionParser`'s grammar to expose the new functions in filter strings, or
   keep filter-string support to the current operator/`contains`/`in` set?
3. `Substring`/`IndexOf` with `StringComparison`/culture overloads — support a subset or throw?
4. In-memory fallback: per-query opt-in flag name and whether it belongs on `IDocumentQuery<T>`.
5. Flag-enum querying when string enum conversion is enabled — throw at runtime on non-numeric stored
   value (proposed), or attempt a string-membership translation? The former is far simpler and safer.
6. Soundex C# implementation — which variant (American Soundex vs SQL Server's specific algorithm)? They
   differ, so native `SOUNDEX()` and the UDF/in-memory/stored-key paths can disagree on the same input.
   Pick one canonical algorithm and use the UDF even where a native exists (consistency over native-ness),
   or accept per-backend variance? Recommend canonical-UDF-everywhere for cross-provider determinism.
7. Should `fuzzystrmatch` (PostgreSQL) be auto-provisioned (`CREATE EXTENSION IF NOT EXISTS`) on init, or
   left to the operator and gated behind the `SupportsSoundex` flag? Auto-provision needs DDL privileges.

**Resolved:** ships in v7 (additive features + the AOT reshape's `OrderBy` semantics shift land in a v7
minor). End-to-end NativeAOT is **in scope** — selectors/projections push down on pushdown backends, and a
compile-free evaluator replaces `Expression.Compile()` on the in-memory/client-side paths.
