# Query Function Translations — Capability Matrix & Scoped Plan

This document is the design contract and roadmap for expanding the set of BCL methods/operators that
`Shiny.DocumentDb` can translate into native provider queries — the equivalent of Npgsql's
[supported translations](https://www.npgsql.org/efcore/mapping/translations.html) page, scoped to a
schema-free JSON document store across many backends.

It is a cross-session working doc. Update the matrix as functions land; promote sections into the
public docs site (`querying.mdx`) and `skills/shiny-documentdb/SKILL.md` as tiers ship.

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

### Out of scope (document explicitly)

PostgreSQL-only exotica from the Npgsql page — network (`inet`/`cidr`), trigram (`pg_trgm`), `ltree`,
`cube`, fuzzy-string (Soundex/Levenshtein/Metaphone), full-text search, row-value/tuple comparisons,
and aggregate regression/statistical functions. None are portable across SQLite/Mongo/Cosmos/Oracle.
They remain available in-memory on LiteDb/IndexedDb only. Full-text search, if wanted, deserves its own
design pass (mirroring `docs/vector-support.md`), not the scalar-translation mechanism.

## Proposed architecture: a scalar-translation seam

**Do not** add one `IDatabaseProvider` method per function — the interface already has ~30 members and a
dozen `StringLower/MathAbs/DatePart…` methods would force stub implementations across six providers.
Instead introduce a single seam:

```csharp
// On IDatabaseProvider
string TranslateScalar(ScalarFn fn, IReadOnlyList<string> argSql, Type resultType);
```

- `ScalarFn` is an enum (or small record) covering the Tier-1/2/3 functions.
- The default interface implementation provides the **ANSI-portable** rendering; a provider overrides
  only the entries whose dialect differs (e.g. SqlServer `LEN`/`CHARINDEX`, MySql `CHAR_LENGTH`/`LOCATE`).
- Implement each provider's overrides as a `static` dialect table (function → template string), so the
  per-provider surface is one dictionary, not N methods.
- Retrofit the existing `ConcatStrings` and the string-`LIKE` logic into this seam so there is one path.
- The visitor (`JsonExpressionVisitor`) owns argument normalization (index-base, CAST insertion,
  null handling) so providers only supply dialect spelling.

Cosmos and Mongo keep their own visitors but should expose an equivalent internal dialect table so the
*recognition* logic (which `MethodCallExpression`/`MemberExpression` maps to which `ScalarFn`) can be
shared, even if the emission differs (SQL string vs BSON `$expr`).

## Untranslatable contract

Decide **per (function × provider)** the behavior when a function cannot be pushed down:

- **Throw `NotSupportedException`** (today's behavior) — safe, predictable, no silent perf cliff.
- **Fall back to in-memory** — fetch-then-filter; convenient but a hidden fetch-all on large sets.

Recommendation: default to **throw** for the translating paths (pushdown-or-fail), and make in-memory
fallback an *opt-in* per-query flag, never the silent default. Document the chosen behavior in the
capability matrix so generated code (via the skill) knows what's safe.

## Scoped delivery plan

**Phase 0 — seam (prerequisite).** Build `TranslateScalar` + the per-provider dialect-table pattern,
retrofit `ConcatStrings`/`LIKE` onto it, no new user-visible functions. Land the capability-matrix
scaffolding in `SKILL.md` + `querying.mdx`. *This is the gate — do it before fanning out.*

**Phase 1 — Tier 1 strings** across all three translating paths + tests across every provider
(`tests/Shiny.DocumentDb.Tests`, plus the Mongo `$expr` sub-mode). ~1 week.

**Phase 2 — Tier 2 math** with the CAST-normalization helper. ~1 week. Re-evaluate demand after Tier 1
ships before committing.

**Phase 3 — Tier 3 date/time** — only if Tiers 1–2 prove the seam and demand justifies the per-engine
date-parsing + Mongo `$expr` cost. ~1.5 weeks.

Each phase follows the repo's "four artifacts in sync" rule (code+tests, documentation site release
note, `SKILL.md` triggers + supported-translations table, `readme.md` feature list).

## Rough effort

| Item | Estimate |
|---|---|
| Phase 0 seam | 2–3 days |
| Tier 1 string fn (each) | ~0.5 day (3 visitors + ~5 dialect overrides + tests) |
| Tier 2 math fn (each) | ~0.75 day (adds CAST handling) |
| Tier 3 date fn (each) | ~1–1.5 days (string parsing + Mongo `$expr`) |
| Tier 1 total (~10 fns) | ~1 week |
| Tiers 1–2 | ~2 weeks |
| Tiers 1–3 | ~3+ weeks |

## Open questions

1. Server-evaluated `now()` vs captured-constant `UtcNow` — do any callers need the DB clock?
2. Do we extend `FilterExpressionParser`'s grammar to expose the new functions in filter strings, or
   keep filter-string support to the current operator/`contains`/`in` set?
3. `Substring`/`IndexOf` with `StringComparison`/culture overloads — support a subset or throw?
4. In-memory fallback: per-query opt-in flag name and whether it belongs on `IDocumentQuery<T>`.
