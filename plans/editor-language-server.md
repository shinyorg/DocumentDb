# Plan: DocumentDb language server (`Shiny.DocumentDb.LanguageServer`)

**Status:** Designed, not started. **This is the shared spine — build this first.**
**Target version:** `13.6` (one new public surface in `Shiny.DocumentDb`, one new service in
`ShinyDocDbMyAdmin.Core`, one new tool package).
**Packages:** `Shiny.DocumentDb.LanguageServer` (new — dotnet tool `shinydocdb-lsp`),
`Shiny.DocumentDb` (new `Shiny.DocumentDb.Language` namespace), `ShinyDocDbMyAdmin.Core` (completion model).
**Clients:** `plans/editor-vscode.md`, `plans/editor-ssms.md`. Neither client contains grammar, schema or
execution logic — they are transport plus UI.

> Self-contained build spec. Read `CLAUDE.md` (repo root) for the four-artifact rule (code+tests, docs site,
> skill, readme) before considering any commit "done". Branch off `v13`.

---

## Goal

Give the string grammar an editor. Today the only place you can type `total:number > 100` and get help is the
admin tool's filter console — a plain `<input>` with no completion, no squiggles, and one error at a time.
Everywhere else (a `Where("…")` in C#, an MCP argument, a REST query string) it is a bare string that fails at
runtime.

The target experience, identical in VS Code and in SSMS/Visual Studio:

```
from Order
where customer.name startswith 'A' and status == 'op▮
                                                   └── open · shipped · cancelled   (from 200 sampled docs)
order by total:number desc
select id, customer.name as name, total:number
limit 50
```

- Completion over **the document composition that is actually in the store** — nested paths, their kinds,
  their distinct values — not a schema someone declared.
- Diagnostics with real ranges, several at once, as you type.
- Hover: what fraction of documents have this field, an example value, whether it is indexed, whether it is
  encrypted.
- Run it, see the rows **and the SQL the library generated** — the admin console's argument, in an editor.

## Why one server and two thin clients

The repo already made this call once: `ShinyDocDbMyAdmin.Core` exists so the web and terminal front ends
"cannot drift apart". Two editor front ends is the same shape, one host boundary further out. LSP is the
boundary that both hosts already speak, and it buys three things beyond reuse:

1. **Out-of-process.** The server is .NET 10 talking to `IDocumentStore`; the clients are TypeScript and a VSIX.
   No provider assembly, no `Microsoft.Data.SqlClient`, no DuckDB native ever loads inside the IDE.
2. **Architecture independence.** A crash or an ARM64/x64 mismatch in the server cannot take the host with it —
   which matters most exactly where the host is least forgiving (see `plans/editor-ssms.md`).
3. **One grammar.** Completion, diagnostics and execution all run the library's own parser and the library's
   own query pipeline. What the editor says the query means is what the application gets.

## Non-goals

- **Not a SQL editor.** SSMS and the `mssql` extension already own T-SQL. This is the DocumentDb string
  grammar, and the generated SQL is shown read-only as output.
- **No CLR-type awareness.** The server has a connection string, not the consumer's assemblies. It works
  through the schema-free JSON lane (`store.Collection(name)`), so paths and types come from sampled documents
  and from the store's own indexes, never from `[Document]` types. A type-keyed lane is out of scope until
  there is a way to hand the server a metadata manifest.
- **Relational providers only in v1** — the JSON-collection lane is relational-only, which is also exactly the
  set `ShinyDocDbMyAdmin.Core` already references (SQLite, SQLCipher, DuckDB, MySQL, MariaDB, PostgreSQL,
  CockroachDB, SQL Server, Oracle). Mongo/Cosmos/Dynamo/etc. are a later phase and need the JSON lane first.
- **No typed-LINQ features.** `GroupBy`/`Having` are in (they have a string grammar); `Include`, vectors and
  temporal are not, in v1.
- **No AI.** The admin's assistant already covers "write me a filter". This is deterministic tooling.

---

## Part 1 — `Shiny.DocumentDb.Language` (core, new public surface)

The one thing the server must **not** do is re-implement the grammar. Today it cannot avoid it, because
`FilterExpressionParser` is internal, throws on the first error, and returns an `Expression` — none of which an
editor can use. So the first piece of work is a parse-only, non-throwing view of the same parser.

Evidence this is the right shape rather than a convenience: `ShinyDocDbMyAdmin.Core/Services/FilterPaths.cs`
recovers field references from a filter with a **regex**, and its own doc comment says why and what would
replace it — *"If the grammar itself gains a 'what does this reference' surface, this should use it."* This is
that surface. Deleting `FilterPaths` outright is part of this work, and it is the acceptance test for the API.

### API

```csharp
namespace Shiny.DocumentDb.Language;

public enum FilterClause { Filter, Ordering, Projection, GroupProjection, Having }

public enum FilterTokenKind
{
    FieldPath, TypeHint, Function, Keyword, Direction, Operator,
    StringLiteral, NumberLiteral, Placeholder, Alias, Punctuation, Unknown
}

public readonly record struct TextSpan(int Start, int Length);

public sealed record FilterToken(FilterTokenKind Kind, TextSpan Span, string Text);
public sealed record FilterDiagnostic(TextSpan Span, string Code, string Message, FilterSeverity Severity);
public sealed record FieldReference(string Path, string? TypeHint, TextSpan Span, FieldUsage Usage);

public enum CompletionExpectation { Field, Operator, Value, Logical, Function, Direction, Alias, TypeHint }
public sealed record CompletionContext(
    CompletionExpectation Expectation,
    TextSpan Replace,
    string? PartialText,
    string? FieldInScope,     // the left-hand path, when completing a value
    string? FunctionInScope,  // the enclosing call, for signature help
    int ArgumentIndex);

public sealed record FilterAnalysis(
    IReadOnlyList<FilterToken> Tokens,
    IReadOnlyList<FilterDiagnostic> Diagnostics,
    IReadOnlyList<FieldReference> Fields)
{
    public CompletionContext? ContextAt(int position);
}

public static class DocumentFilterSyntax
{
    public static FilterAnalysis Analyze(string text, FilterClause clause = FilterClause.Filter, bool allowTypeHints = false);
}

public sealed record DocumentFunctionInfo(
    string Name, FilterClause[] Clauses, bool IsPredicate,
    IReadOnlyList<DocumentFunctionParameter> Parameters, string Summary);

public static class DocumentFunctions   // extend the existing static class, or a sibling catalog
{
    public static IReadOnlyList<DocumentFunctionInfo> Catalog { get; }
}
```

### Implementation notes

| Piece | Change | Why it is small |
|---|---|---|
| `Lexer.Tokenize` | Add an overload returning `(List<Token>, List<FilterDiagnostic>)` instead of throwing; `Token` gains `Length`. The throwing form becomes a wrapper that rethrows the first diagnostic. | Three `throw Error(...)` sites (bad char, bad operator, unterminated string, malformed placeholder). Positions are already tracked. |
| `Parser` | Add a `recover` flag. On error in recovery mode: record the diagnostic, skip to the next `and`/`or`/`,`/`)`/End, continue. Execution keeps `recover: false` and behaves exactly as today. | Recursive-descent with a single `Error()` funnel — the skip-set is one method. |
| Field collection | A `CollectingFieldBinder : IFieldBinder` that records `(path, hint, span, usage)` and returns a placeholder expression, so analysis needs no `JsonTypeInfo` and never builds a tree. | `IFieldBinder` is already the seam — there are three binders today. |
| `ContextAt` | Pure token-stream classification left of the caret. No parse required, so it works on text that does not parse — which is the normal state while typing. | The lexer is ~120 lines and already positional. |
| Function catalog | Replace the two `IsPredicateFunction`/`IsValueFunction` `switch` expressions with one table; the two predicates read from it. | 40 names in two lists today, duplicated again in `FilterPaths.Reserved` and again in the docs page. |

**The function catalog earns its keep beyond completion.** `CLAUDE.md`'s query-surface-parity rule ("whenever
you add a value/predicate function usable in typed LINQ, wire it into `FilterExpressionParser` too") currently
has to be remembered. With the catalog, a new function is one row plus the emitter, and a test can assert that
every `DocumentFunctions` method appears in the catalog — parity becomes enforced instead of instructed.

### Behaviour that must not change

The whole risk of this part is regressing the executing parser. Guard it:

- Every existing filter-grammar test stays untouched and green (they assert thrown messages, which the wrapper
  reproduces byte-for-byte — `FilterParseError.Create` stays the single formatter).
- A property test: for a corpus of valid and invalid filters, `Analyze(...).Diagnostics.Count == 0` iff the
  executing parse does not throw, and when it throws, the first diagnostic's position equals the thrown
  position.
- `FilterPaths` is deleted and `SuggestIndexPaths` re-pointed at `FilterAnalysis.Fields`; its existing tests
  become the regression net for the new path extraction. Per `CLAUDE.md`, deleted — no forwarding shim.

---

## Part 2 — the statement syntax (DQL)

The admin console has four boxes (where / order by / project / take). An editor wants one document. So the
server accepts a **statement** that composes the existing clauses:

```
from <collection>
[where <filter>]
[group by <field> [having <predicate>]]
[order by <ordering>]
[select <projection>]
[limit <n>] [offset <n>]
```

**The critical property: this is clause splitting, not a second grammar.** The statement parser recognises the
six clause keywords and hands each clause's text *verbatim* to the existing parser, carrying the offset so
diagnostics map back to the document. There is no expression syntax here, nothing to keep in parity, and
nothing that can drift. The statement maps 1:1 onto the existing builder:

```csharp
store.Collection(from).Query()
     .Where(where).GroupBy(groupBy).Having(having)
     .OrderBy(orderBy).Project(select).Paginate(offset, limit);
```

Decisions:

| Decision | Choice | Consequence |
|---|---|---|
| Where it lives | In the language-server package, **not** the library | It is a surface for editors. If the admin console or MCP later wants a one-box mode, promote it then, with a release note. |
| Keyword style | Lowercase keywords, `from`/`where`/`order by`/`select`/`limit` | Reads like what a DBA opening SSMS expects, without pretending to be SQL (no `*`, no joins, no subqueries — see `plans/joins.md`). |
| Clause order | Fixed, and enforced with a diagnostic that offers a reorder code action | Order-free parsing makes recovery worse and buys nothing. |
| Multiple statements | Separated by a blank line or `;`, each runnable independently | Matches how people use a `.sql` scratch file; the client gets a `documentSymbol` per statement and a run-at-cursor. |
| Comments | `--` to end of line | The only thing borrowed from SQL that has no equivalent in the grammar. |
| File extension | `.ddbq`, language id `documentdb` | Both clients register it. |

---

## Part 3 — the composition model (what completion actually knows)

`ShinyDocDbMyAdmin.Core` already computes everything needed, for the admin's Structure and Generate tabs:

- `DocumentShape` / `NodeShape` — the **nested** tree over sampled bodies, with per-node kind counts,
  observation counts, numeric ranges, date ranges, and up to `MaxTrackedValues` (40) distinct scalar values
  before a field is treated as free-form.
- `DocumentAdminService.InferSchema` / `SampleBodies` — the 200-document, newest-first sample.
- `DocumentAdminService.Indexes` — which JSON paths are indexed, and index stats.
- `EncryptedFields` — which paths are envelope-encrypted.
- `DocumentAdminService.Geometry` — which paths hold geometry.

New in Core, one service, no new inference:

```csharp
public sealed record CompletionField(
    string Path,               // "customer.address.city"
    JsonValueKind Kind,
    double Occurrence,         // 0..1 of the sample
    IReadOnlyList<JsonNode> Values,   // <= 40 distinct, empty when saturated
    string? Example,
    bool IsArray,
    bool Indexed,
    bool Encrypted,
    bool Geometry,
    string? SuggestedTypeHint); // ":number" / ":date" / ":guid" where the provider needs one

public sealed record CompletionModel(
    string TypeName, int Sampled, IReadOnlyList<CompletionField> Fields, DateTimeOffset BuiltAt);

Task<CompletionModel> BuildCompletionModel(string profileId, string table, string typeName, CancellationToken ct);
```

Flattening `NodeShape` to dotted paths is the whole implementation. Arrays contribute the element shape's
paths (`items.sku`), which is what the grammar addresses anyway.

Caching: keyed on `(profileId, table, typeName)`, 10-minute TTL, invalidated by an explicit
`documentdb/refreshSchema` request and by any write the server itself performs. Sampling is one query; the
first completion in a collection pays it.

**Value completion is the feature people will remember.** `status == '` offering the four values that exist is
only possible because `NodeShape` already collects them, and only honest because it also knows when it
saturated — past 40 distinct values the server offers nothing rather than a misleading four.

---

## Part 4 — the server

Transport: stdio JSON-RPC. `StreamJsonRpc` + `Microsoft.VisualStudio.LanguageServer.Protocol` for the types —
Microsoft-owned, no MediatR, and the same pair the Visual Studio LSP samples use, which keeps the SSMS client
boring. (`OmniSharp.Extensions.LanguageServer` is the alternative; it brings a DI/MediatR pipeline this server
does not need.)

### Standard LSP

| Request | Behaviour |
|---|---|
| `completion` | Driven by `CompletionContext.Expectation`: **Field** → the composition model, sorted by occurrence, indexed paths first, nested paths offered lazily one segment at a time; **Value** → the distinct set for the field in scope, correctly quoted for its kind; **Function** → the catalog filtered by clause; **Operator/Logical/Direction/TypeHint** → fixed sets; after `from` → collection names (`TypeName` distinct, per table). |
| `completionItem/resolve` | Documentation: occurrence %, kind distribution, example value, index and encryption badges. Deferred so the list stays fast. |
| `hover` | Same content for a field; signature and summary for a function. |
| `signatureHelp` | From the catalog, triggered on `(` and `,`. |
| `publishDiagnostics` | Debounced 250 ms. See the rule table below. |
| `semanticTokens/full` | From `FilterAnalysis.Tokens` — the reason colouring is right even for a `:number` hint, which no TextMate grammar will get consistently. |
| `documentSymbol` | One symbol per statement, children per clause. |
| `codeAction` | See below. |
| `formatting` | Clause keywords to column 0, one clause per line, operators spaced. |

### Diagnostics

| Code | Severity | Rule |
|---|---|---|
| `DDB1001` | Error | Parse error from `FilterAnalysis.Diagnostics`, with a real range. |
| `DDB1002` | Warning | Path not present in **any** sampled document. Warning, never error — the store is schema-free and the sample is 200 documents; the message says so. |
| `DDB1003` | Warning | `order by` / `min` / `max` / `select` over a path the sample says is numeric or a date, with no `:type` hint, on a provider whose plain JSON extract returns text. This is the single most common real bug in the string lane, and the sample makes it detectable. |
| `DDB1004` | Warning | Comparison against an encrypted field — it cannot match, because the stored value is ciphertext. |
| `DDB1005` | Hint | Filter or order over a path with no supporting index. The admin already suggests exactly this; here it is inline. |
| `DDB1006` | Warning | Kind mismatch: comparing a path the sample says is a string against a number literal, or vice versa. |
| `DDB1007` | Error | `:type` hint on a type-keyed collection, or an unknown hint name. |

Every warning is suppressible per-file (`-- ddb:disable DDB1005`) and per-connection.

### Code actions

- Add the `:number` / `:date` / `:guid` hint (fixes `DDB1003`).
- Create an index for this path — calls `IJsonDocumentCollection.CreateIndex`, gated by the write setting.
- Quote a bare identifier that was probably meant as a string.
- Wrap both sides in `lower(...)` for a case-insensitive comparison.
- Reorder clauses into canonical order.

### Custom requests

| Method | Purpose |
|---|---|
| `documentdb/connect` | `{ profileId }` or `{ provider, connectionString, table }`. Returns capabilities and collection list. |
| `documentdb/collections` | Distinct `TypeName` per table. |
| `documentdb/schema` | The `CompletionModel`, for the client's explorer tree. |
| `documentdb/refreshSchema` | Drops the cache entry and re-samples. |
| `documentdb/previewSql` | `IJsonDocumentQuery.ToQueryString()` — SQL + parameters, no execution. Updates live as you type. |
| `documentdb/execute` | Runs the statement at a position. Returns rows, columns, generated SQL, parameters, elapsed, total count, truncation flag. Capped at the console's 500 rows. |
| `documentdb/explain` | The provider's `EXPLAIN` for the generated SQL. Note it is a statement **list** per provider, as the admin's EXPLAIN tab found. |

### Connections and safety

- Profiles come from `ShinyDocDbMyAdmin.Core`'s `ProfileStore`, so a connection created in the web or terminal
  admin is already there in the editor — one profile store, three front ends. A client may also pass an
  ad-hoc connection that is never persisted.
- **Read-only by default.** `ExecuteDelete` / `ExecuteUpdate` / `CreateIndex` are refused unless the profile
  opts in, mirroring the per-connection AI write opt-ins added in `13.5`. The client shows the mode in the
  status bar and confirms every write. A destructive statement in a read-only profile is a diagnostic, not a
  runtime failure.
- Secrets never cross the wire in a log: the server logs SQL and parameter *names*, never values, at anything
  below `Trace`.

---

## Packaging

Two flavours from one project, because the two hosts have opposite constraints:

| Flavour | Contents | Consumer |
|---|---|---|
| `Shiny.DocumentDb.LanguageServer` (dotnet tool, `shinydocdb-lsp`) | All relational providers | VS Code, which can require .NET 10 and install into extension storage — VSIX stays under 1 MB and the server updates independently. |
| `shinydocdb-lsp` self-contained single-file win-x64 / win-arm64 | **SQL Server provider only** | Shipped inside the SSMS/VS VSIX, where there is no SDK to install a tool with. |

The SQL-Server-only flavour is what keeps the VSIX sane: `plans/tool-package-size.md` measured DuckDB natives
at 70% and SQLitePCLRaw at 20% of the 152 MB admin tool, and neither is wanted in an SSMS extension. Expect
~35 MB compressed; **measure it in the spike** — untrimmed, because `Microsoft.Data.SqlClient` and the ADO.NET
clients are reflection-heavy and Core already disables the trim/AOT analyzers for that reason.

---

## Phasing

| Phase | Scope | Ships |
|---|---|---|
| **0. Spike (1 day, gates everything)** | Stand a stdio server up, answer `initialize` + a hard-coded completion list, drive it from VS Code and from an SSMS 22 VSIX. | Nothing. Answers the one question that can kill the SSMS half — see `plans/editor-ssms.md`. |
| **1. Analysis surface** | `Shiny.DocumentDb.Language` + function catalog + `FilterPaths` deleted. | A release note; the admin console's index suggestions get more accurate for free. |
| **2. Server v1** | Statement syntax, composition model, completion/hover/diagnostics/semantic tokens, `previewSql`, `execute`. | The tool package. |
| **3. VS Code client** | `plans/editor-vscode.md`. | Marketplace. |
| **4. SSMS / VS client** | `plans/editor-ssms.md`. | Marketplace + VSIX. |
| **5. Later** | Query notebooks, non-relational providers (needs the JSON lane there first), a metadata manifest so typed collections light up. | — |

## Testing

- `Shiny.DocumentDb.Tests`: the analysis API — token spans, multi-error recovery, `ContextAt` at every caret
  position in a corpus of partial filters, catalog/`DocumentFunctions` parity, and the property test that
  analysis and execution agree on validity.
- New `Shiny.DocumentDb.LanguageServer.Tests`: statement splitting and offset mapping; the diagnostic rules
  against a seeded SQLite store; an in-process JSON-RPC harness driving `initialize` → `didOpen` →
  `completion` → `execute`.
- Full suite, Docker up — the server tests use the same Testcontainers fixtures as the provider tests for the
  non-SQLite diagnostics (`DDB1003` is provider-dependent by construction).

## Documentation

New section `documentdb/editors/`: `index.mdx` (what it is, which hosts), `language.mdx` (the statement
syntax, one page, cross-linked from `querying.mdx` and `json-collections.mdx`), plus the two client pages.
`querying.mdx` gains a pointer to the analysis API. Release notes: `feature` for the analysis surface and the
server, `breaking` if anything about `FilterPaths`' removal is observable (it is internal to the admin, so it
should not be).
