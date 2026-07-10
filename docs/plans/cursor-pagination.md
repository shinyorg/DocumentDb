# Plan: Cursor / keyset pagination (`ToCursorPage`)

**Status:** Designed, not started.
**Target version:** `11.0` (additive — new terminal on `IDocumentQuery<T>`, no breaking change). `version.json`
is `11.0.0-beta`, so notes go under `## 11.0 TBD`. Phased: keyset core (11.0) → native-token providers →
OData / AI-tool wiring; see [Phasing](#phasing).

> Self-contained build spec. Read `CLAUDE.md` (repo root) for the four-artifact rule (code+tests, docs site,
> skill, readme) before considering any commit "done". Branch off `v11`.

---

## Goal

A **forward-only, seek-based** pagination surface alongside the existing offset paging (`Paginate(offset,
take)` + `PageResult`). The caller pages by handing an opaque cursor back on each call instead of an ever-
growing offset:

```csharp
string? cursor = null;
do
{
    var page = await store.Query<Order>()
        .Where(x => x.Status == "open")
        .OrderByDescending(x => x.CreatedAt)   // cursor keyset is DERIVED from this OrderBy
        .ToCursorPage(cursor, take: 50);

    Render(page.Items);
    cursor = page.NextCursor;                  // null ⇒ last page
}
while (cursor != null);
```

**Why this is worth building** (established earlier in the investigation):

- **Constant per-page cost.** `OFFSET N` scans and discards `N` rows every relational provider; page 500 is
  measurably slower than page 1. Keyset (`WHERE (sortkey, id) > (…)  ORDER BY … LIMIT n`) is O(log n) per page
  with the right index, regardless of depth.
- **Stable under concurrent writes.** Offset paging skips/duplicates rows when documents are inserted or
  removed between page fetches. A keyset anchor doesn't drift.
- **Stops wasting the backend's native mechanism.** Cosmos already drains its `FeedIterator` and **discards the
  SDK continuation token** (`CosmosDbDocumentQuery.cs`, `ReadNextAsync` loop) then re-pages with SQL
  `OFFSET…LIMIT`, paying RU per skipped doc. Azure Table pages **client-side** (`AzureTableDocumentQuery.cs:247`
  `.Skip().Take()`) — it drags the whole prefix over the wire. DynamoDB has `LastEvaluatedKey`. A cursor surface
  lets each of these ride its native token.
- **Drops a round-trip.** `PageResult` (`DocumentQueryExtensions.cs:416`) does `Count()` **plus**
  `Paginate().ToList()` per page. A cursor page has no total, so no count round-trip.

## Non-goals

- **Not a replacement for offset paging.** `Paginate` / `PageResult` / `PagedResults<T>` stay. Cursor paging
  cannot do "jump to page 7", cannot give a total count, and (v1) cannot page backward. The page-number-UI +
  total-count use case remains offset's job. The two coexist; docs steer readers to the right one.
- **No arbitrary random access.** Only `next`. `previous`/`last` are a keyset follow-up (native-token providers
  can't do them at all).
- **No cursor encryption / signing in v1.** The token base64-encodes the trailing sort-key *values* (e.g. a
  timestamp) — it is opaque, not secret. `ICursorProtector` is noted as a later hook.

## Design decisions (locked)

| Decision | Choice | Consequence |
|---|---|---|
| Shape | **Terminal**, not a builder verb | A builder method can't return the *next* cursor; the cursor is a terminal concern. `ToCursorPage(cursor, take)` reads the query's own `OrderBy`. |
| Keyset source | **Derived from existing `OrderBy`** + a mandatory `Id` tiebreaker appended by the engine | No new "order + tiebreak" API to learn; total order guaranteed even when the sort key is non-unique. |
| Direction | **Forward-only** | Matches the lowest common denominator (native tokens are forward-only). Backward keyset is a follow-up. |
| Cursor opacity | **base64url(JSON)**, forward-compatible `v` field + query-shape hash | Cross-query / stale cursors fail loudly instead of returning garbage. |
| Two strategies, one abstraction | **Keyset** (relational/Mongo/in-memory) vs **native token** (Cosmos/Dynamo/AzureTable) behind one `CursorPage<T>` | Caller never sees which; the query (filter+sort) must be identical across page calls either way. |
| After `Select`/`Project`/`GroupBy` | **Throw** `NotSupportedException` | Keyset needs `Id` + raw sort columns, which projection/grouping drop. Mirrors today's "`Paginate` after `Select` throws". |

---

## Public API surface

### Terminal + result — `Shiny.DocumentDb/IDocumentQuery.cs` (+ `Shiny.DocumentDb/CursorPage.cs`)

Add one terminal to `IDocumentQuery<T>`, defaulted so providers opt in exactly like `Project` /
`NearestVectors` / `FullTextMatch` already do:

```csharp
/// <summary>
/// Reads one forward page using seek/keyset pagination derived from the current <see cref="OrderBy"/>
/// (an <c>Id</c> tiebreaker is appended automatically to guarantee a total order). Pass <c>null</c> for
/// the first page; pass the previous page's <see cref="CursorPage{T}.NextCursor"/> for each subsequent
/// page. A null <c>NextCursor</c> on the result marks the last page.
/// <para>
/// The query's filters and ordering MUST be identical to the call that produced <paramref name="cursor"/>;
/// a cursor is only valid for the query shape that created it (enforced by a shape hash for keyset
/// providers). Not valid after <see cref="Select"/> / <see cref="Project"/> / <see cref="GroupBy"/>.
/// </para>
/// </summary>
/// <param name="cursor">Opaque continuation token from a prior page, or null for the first page.</param>
/// <param name="take">Maximum items to return in this page. Must be &gt; 0.</param>
Task<CursorPage<T>> ToCursorPage(string? cursor, int take, CancellationToken ct = default)
    => throw new NotSupportedException("Cursor pagination is not supported by this provider.");
```

```csharp
namespace Shiny.DocumentDb;

/// <summary>One forward page of a cursor/keyset paginated query.</summary>
public sealed record CursorPage<T>(
    IReadOnlyList<T> Items,
    string? NextCursor)
{
    /// <summary>True when another page follows (i.e. <see cref="NextCursor"/> is non-null).</summary>
    public bool HasMore => this.NextCursor != null;
}
```

`NextCursor` is the single source of truth (`null` = end); `HasMore` is convenience only.

### Convenience: auto-following stream — `Shiny.DocumentDb/DocumentQueryExtensions.cs`

A resumable full scan that never pays deep-offset cost — the reason to prefer this over `ToAsyncEnumerable`
for large stable iterations:

```csharp
/// <summary>
/// Enumerates every matching document by walking cursor pages of <paramref name="pageSize"/> until
/// exhausted. Unlike a deep <see cref="IDocumentQuery{T}.Paginate"/> loop this stays O(log n) per page and
/// is stable under concurrent writes. Requires the provider supports <c>ToCursorPage</c>.
/// </summary>
public static async IAsyncEnumerable<T> ToCursorStream<T>(
    this IDocumentQuery<T> query,
    int pageSize = 100,
    [EnumeratorCancellation] CancellationToken ct = default) where T : class
{
    string? cursor = null;
    do
    {
        var page = await query.ToCursorPage(cursor, pageSize, ct).ConfigureAwait(false);
        foreach (var item in page.Items)
            yield return item;
        cursor = page.NextCursor;
    }
    while (cursor != null && !ct.IsCancellationRequested);
}
```

### Naming note

The feature's working name in discussion was `PaginateAfter`. A **terminal** (`ToCursorPage`) is used instead
of a builder verb because a builder returning `IDocumentQuery<T>` has nowhere to hand back the next cursor, and
a separate `ToPage()` terminal after a `PaginateAfter(cursor, take)` builder would split state across two calls
for no gain. If a builder shape is preferred for symmetry with `Paginate`, the fallback is
`PaginateAfter(string? cursor, int take)` (stashes state) + a parameterless `ToCursorPage()` terminal — decide
before build; the engine below is identical either way.

---

## How it executes — two strategy families

The engine picks per provider. The caller sees only `CursorPage<T>`; the constraint in both families is that
**filter + ordering must be identical across page calls** (the cursor is scoped to its query shape).

### Family A — Keyset (relational, MongoDB, LiteDB, IndexedDB, DuckDB)

Fully engine-built and seekable. This is the bulk of the surface and where most providers live.

1. **Resolve the sort spec.** Take the `OrderBy`/`OrderByDescending` selectors already on the query. Append
   `Id` as a final tiebreaker (ascending) so the total order is deterministic even when the primary key is
   non-unique. No `OrderBy` at all ⇒ order by `Id` ascending (cursor still works).
2. **First page (cursor null).** Emit `ORDER BY <keys> LIMIT take+1`. Fetch one extra row to detect
   `HasMore` without a count.
3. **Encode `NextCursor`.** From the last *kept* row (the take-th), capture the value of each sort key +
   `Id`; base64url-encode the [cursor payload](#cursor-format). If ≤ `take` rows came back, `NextCursor = null`.
4. **Next page (cursor supplied).** Decode → validate the shape hash → emit the lexicographic keyset predicate
   as a WHERE fragment ANDed onto the existing filters, then the same `ORDER BY … LIMIT take+1`.

The keyset predicate for keys `k0..kn` with per-key "after" operator `OPi` (`>` for an ascending key, `<` for
descending) and cursor values `v0..vn` is the OR-chain:

```
(k0 OP0 v0)
 OR (k0 = v0 AND k1 OP1 v1)
 OR (k0 = v0 AND k1 = v1 AND k2 OP2 v2)
 ... AND (all prior = ) AND (kn OPn vn)
```

**Emit the OR-chain, not native tuple comparison.** `(a,b) < (x,y)` exists in SQLite/PG/MySQL but not SQL
Server/Oracle, and it is wrong the moment sort directions are mixed (primary DESC, `Id` ASC). The OR-chain is
per-key-direction-correct and portable, so it is the single lowering for every relational provider and Mongo
(`$or` of `$gt`/`$lt` + `$eq` chains). Sort keys reuse the existing JSON-extraction column expressions the
`OrderBy` path already builds; `Id` is the real key column.

**Engine touch-points:**
- Shared `DocumentQuery` (`src/Shiny.DocumentDb/Internal/DocumentQuery.cs`) grows a `BuildKeysetWhere(...)`
  that reuses the same selector→column machinery `BuildOrderBy` uses, plus `ToCursorPage`.
- `IDatabaseProvider` gains `string BuildLimitClause(int take)` (offset-less LIMIT/TOP/FETCH) — a trivial
  sibling of the existing `BuildPaginationClause(offset, take)` so we don't pass a dummy offset 0.
- MongoDB implements `ToCursorPage` in `MongoDbDocumentQuery` (build the `$or` keyset filter + sort + limit).
- LiteDB / IndexedDB / DuckDB apply the keyset predicate in their existing in-memory/native evaluators.

**Indexing note (docs + skill):** keyset is only O(log n) if an index covers `(sortkey, Id)`. Recommend
`MapIndexedProperty` (or a `MapComputedProperty(indexed: true)`) on the sort key for hot cursor paths. Without
an index it is still correct but scans — same as offset.

### Family B — Native continuation token (Cosmos, DynamoDB, Azure Table)

These backends either can't do arbitrary-property keyset (Dynamo/AzureTable are key-partitioned) or fight the
SDK if you try (Cosmos cross-partition `ORDER BY`). They resume the **same** query via the provider's own token,
which we wrap verbatim inside our cursor string.

- **Cosmos** — stop discarding the `FeedIterator` continuation token. Set `QueryRequestOptions.MaxItemCount =
  take`, call `ReadNextAsync` **once**, wrap `FeedResponse.ContinuationToken` in `NextCursor`. `null` SDK token
  ⇒ `null` `NextCursor`. This is the low-cost, high-value item — the token is already in hand
  (`CosmosDbDocumentQuery.cs`), we just stop throwing it away, and RU drops because we no longer `OFFSET`-skip.
- **DynamoDB** — carry `LastEvaluatedKey` (serialize the attribute-value map) into `NextCursor`; feed it back
  as `ExclusiveStartKey`. Also **fixes the deep-page pathology** for the cursor path — no client-side skip.
- **Azure Table** — carry the table continuation token (`x-ms-continuation`, i.e. the `Pageable<T>`
  `AsPages().ContinuationToken`) instead of the current `.Skip().Take()` client-side drain
  (`AzureTableDocumentQuery.cs:247`).

For Family B the cursor payload's `t` is `native`, `n` is the provider token, and the shape hash `h` is a hash
of the rendered query for a sanity check; there is no keyset/`OrderBy` derivation. Ordering, where the provider
supports it, must match between calls (inherent — the token belongs to that query).

---

## Cursor format

`base64url(UTF-8 JSON)`, no padding. Small, forward-compatible, self-describing:

```jsonc
// keyset
{ "v": 1, "t": "k", "h": "9f2a…", "d": "da",          // d = per-key directions: a|d, in key order
  "k": [ {"t":"s","v":"2026-07-01T00:00:00Z"}, {"t":"g","v":"0f3c…"} ] }  // typed sort-key + Id values

// native
{ "v": 1, "t": "n", "h": "9f2a…", "n": "<provider continuation token>" }
```

- **`v`** — format version; bump to evolve without breaking old tokens catastrophically (old `v` ⇒ clear error).
- **`h`** — shape hash. Keyset: hash of `(resolved TypeName + normalized OrderBy spec + "v1")`. Native: hash of
  the rendered query text. On decode, recompute and compare; mismatch ⇒ `InvalidOperationException("cursor does
  not match this query")`. Filters are **not** in the hash (they can't safely change either, but hashing a
  normalized filter is brittle) — instead **document** that filter + sort must be stable across pages, and rely
  on `h` to catch the common "wrong sort" mistake.
- **`k` value typing** — each key carries a discriminator so decode rebinds the right SQL parameter type:
  `s`=string/ISO-datetime, `n`=number, `b`=bool, `g`=Id (string), `0`=null. Prevents a date being compared as
  text on providers where that changes collation (the same class of bug as the SQLite decimal `$filter` issue
  noted in `project_odata_sample`).

Opacity, not security: the token exposes the trailing row's sort-key values. Fine for a `CreatedAt`; call it
out for sensitive sort keys, and note `ICursorProtector` (encrypt/sign the payload) as a future opt-in.

---

## Caveats to bake into docs and tests

- **NULLs in a sort key.** `NULL <op> value` is unknown in SQL, so a keyset boundary that lands on a NULL sort
  value can skip rows. Mitigation (document it): order by non-nullable columns for cursor paths; the `Id`
  tiebreaker is
  always non-null, so single-key-on-`Id` is always safe. Consider emitting `IS NULL` guards in a later cut;
  v1 documents the limitation and tests the non-null happy path.
- **Same-value runs.** Many rows sharing one sort-key value are handled by the `Id` tiebreaker — the OR-chain's
  `(k0 = v0 AND Id > lastId)` term walks them deterministically. This is the offset-paging "boundary dupe/skip"
  bug that keyset *fixes*; assert it directly (a page boundary in the middle of a same-timestamp run).
- **Sort key mutated between pages.** If a document's sort-key value changes after you've paged past it, keyset
  may re-surface or hide it — inherent to seek pagination, same class as replication's watermark caveat.
  Document; don't try to solve.
- **No total count / no page count.** By design. Direct callers who need a total to render "page 3 of 90" use
  offset `PageResult`. `HasMore` answers "is there a next page" cheaply (the take+1 probe).
- **`take` bound.** Validate `> 0`; cap defensively (e.g. reject > 10_000) to avoid an accidental full-table
  page. `ArgumentOutOfRangeException` on violation.

---

## Provider capability matrix

| Provider | Strategy | Notes |
|---|---|---|
| SQLite / SQLCipher | Keyset | OR-chain WHERE + `LIMIT take+1`. Recommend index on sort key. |
| PostgreSQL / MySQL | Keyset | Same. |
| SQL Server / Oracle | Keyset | OR-chain (no native tuple compare) + `OFFSET 0 ROWS FETCH NEXT`. |
| DuckDB | Keyset | Dev/analytics; in-engine. |
| MongoDB | Keyset | `$or` keyset filter + `sort` + `limit`. |
| LiteDB / IndexedDB | Keyset | In-memory/native evaluator applies the predicate. |
| **CosmosDB** | Native token | Wrap discarded `FeedIterator` continuation token; RU win. |
| **DynamoDB** | Native token | `LastEvaluatedKey` ↔ `ExclusiveStartKey`. |
| **Azure Table** | Native token | Table continuation token; removes client-side `.Skip()`. |

Providers that don't opt in inherit the `NotSupportedException` default — no silent wrong behavior.

## No new DI / no new package

`ToCursorPage` is a query-surface addition; it needs **no service registration and no new NuGet package**. It
lives in core (`IDocumentQuery.cs`, `CursorPage.cs`, `DocumentQueryExtensions.cs`) plus per-provider query
classes. `DocumentSet<T>` (typed `DocumentContext`) exposes queries through `IDocumentQuery<T>`, so
`ToCursorPage` / `ToCursorStream` flow through automatically — confirm `DocumentSet` delegates its query
building to the same `Query<T>` path (it does today) and add a set-level test.

## Phasing

- **11.0 — Keyset core.** `ToCursorPage` + `CursorPage<T>` + `ToCursorStream` on all relational providers +
  MongoDB + LiteDB + IndexedDB + DuckDB. `IDatabaseProvider.BuildLimitClause`. Cursor format v1 with shape-hash
  validation. Full four-artifact pass. This is the universal win and the largest surface.
- **11.x — Native-token providers.** Cosmos (stop discarding the token), DynamoDB (`LastEvaluatedKey`), Azure
  Table (continuation token, kills the client-side skip on the cursor path). Same `CursorPage<T>` contract;
  `t:"n"` cursor payload. Low code cost — the tokens already exist in the SDK responses.
- **11.x — Adapter wiring.** OData: map `NextCursor` ↔ `$skiptoken` and emit `@odata.nextLink`
  (`ODataDocumentQuery.cs` currently maps `$skip`/`$top` → `Paginate`). Extensions.AI query tool: optional
  `cursor` arg + return the next cursor (`QueryFunction.cs`). Each independently shippable + release-noted.

---

## Tests — `tests/Shiny.DocumentDb.Tests/CursorPaginationTests.cs`

Keyset (11.0):
1. **Forward walk** SQLite: seed N docs, page by `take`, assert full coverage, no dupes, no gaps vs a
   `ToList().OrderBy(...)` baseline; final `NextCursor == null`.
2. **Single-key `OrderBy` ascending and descending** both walk correctly (operator direction).
3. **Multi-key `OrderBy`** (e.g. `Status` asc, `CreatedAt` desc) — OR-chain lowering correct.
4. **Same-value run**: many rows share one sort-key value and the page boundary falls inside the run — `Id`
   tiebreaker yields no dupe/skip (the bug offset paging has).
5. **No `OrderBy`** ⇒ orders by `Id`, still fully covers.
6. **Stability under concurrent insert**: insert a row *before* the current cursor between pages — the walk is
   unaffected (unlike offset, which would shift). Assert no dupe.
7. **`ToCursorStream`** yields the same full set as the manual loop.
8. **Cursor validation**: a cursor produced under one `OrderBy` used with a different `OrderBy` throws
   `InvalidOperationException`; a `v`-mismatched / corrupt token throws a clear error.
9. **After `Select`/`Project`/`GroupBy`** ⇒ `NotSupportedException`.
10. **`take <= 0`** ⇒ `ArgumentOutOfRangeException`.
11. **Value typing**: DateTime and numeric sort keys round-trip through the cursor and compare correctly
    (guards the TEXT-binding class of bug).
12. **Cross-provider parity**: run the core walk against Postgres + MySQL + SQL Server + Mongo (containerized
    suites where available) — same coverage guarantee.
13. **`DocumentSet<T>`** (typed context) exposes `ToCursorPage` and walks correctly.

Native-token (11.x):
14. **Cosmos** forward walk via wrapped continuation token; assert RU is lower than the equivalent
    `Paginate(offset, take)` deep page (or at least that no `OFFSET` appears in the emitted query).
15. **Azure Table / DynamoDB** forward walk via native token; assert the whole result set is not dragged
    client-side (no `.Skip()` on the cursor path).

Run: `dotnet test tests/Shiny.DocumentDb.Tests/Shiny.DocumentDb.Tests.csproj --filter "FullyQualifiedName~CursorPaginationTests"`.

## Four artifacts (per phase)

- **Docs site** `~/Desktop/dev/documentation/.../documentdb/querying.mdx`: a "Cursor pagination" section under
  the existing paging content — the `ToCursorPage`/`NextCursor` loop, `ToCursorStream`, the **offset-vs-cursor
  decision table** (page-number UI + total ⇒ offset; deep/stable/infinite-scroll ⇒ cursor), the indexing tip,
  and the caveats (no total, forward-only, filter+sort must be stable). Release note `<RN type="feature">` under
  `## 11.0 TBD` (create it) per the release-note rules; a second `<RN>` for the native-token phase.
- **Skill** `skills/shiny-documentdb/SKILL.md`: add `ToCursorPage` / `CursorPage` / `ToCursorStream` to the
  `triggers:` list; a short "page with a cursor" recipe; a one-liner steering generated code to cursor paging
  for infinite scroll / large exports and to `PageResult` when a total/page-number is needed.
- **readme.md** (repo root): add cursor/keyset pagination to the feature list.
- **Release notes** `release-notes.mdx`: one `<RN type="feature">` per phase.

## Edge cases / decisions to make during build

- **Builder vs terminal name.** Ship `ToCursorPage(cursor, take)` (recommended) or the
  `PaginateAfter(cursor, take)` builder + `ToCursorPage()` terminal pair — pick before build; engine identical.
- **`Id` type.** The tiebreaker assumes a comparable `Id`. String/Guid Ids compare fine; if any provider stores
  Id in a form that doesn't sort consistently with its WHERE comparison, normalize in the cursor's `g` type.
- **Interaction with global query filters.** Keyset ANDs onto the *effective* filter (post global-filter). A
  cursor taken with filters applied and reused after `IgnoreQueryFilters()` changes the shape — the shape hash
  covers `OrderBy` only, so document that toggling filters between pages is unsupported (same rule as changing
  the WHERE).
- **Mixed-direction native providers.** Cosmos honors `ORDER BY` with its token; Dynamo/AzureTable ordering is
  key-defined — document that `OrderBy` on those is limited/ignored for the cursor path (already true for their
  offset path).
- **`take+1` on native tokens.** Not needed — the provider's returned token already signals "more"; `HasMore`
  = token present. Only the keyset family uses the +1 probe.
