# Admin: "is this database actually a DocumentDb store?"

**Status:** plan only — nothing built.
**Target version:** 13.x (see `version.json` → `13.0.0`; release note goes under a `## 13.x TBD` heading).
**Scope:** `ShinyDocDbMyAdmin.Core` + both front ends (Blazor, TUI). One small addition to
`IDatabaseProvider`. An optional engine-side phase is called out separately and is *not* recommended
for the first cut.

---

## 1. What the tool does today

`DocumentAdminService.Classify` (`src/ShinyDocDbMyAdmin.Core/Services/DocumentAdminService.cs:80`):

1. Name-substring rules run **first**:
   `EndsWith("_history")` → History, `EndsWith("_blobs")` → Blobs,
   `Contains("_spatial" | "_vec" | "_fts")` → Sidecar.
2. Only if none matched does it probe the database:
   `SELECT Id, TypeName, Data, CreatedAt, UpdatedAt FROM t WHERE 1 = 0` — success ⇒ `Documents`,
   `DbException` ⇒ `Foreign`. A second probe adds `HasTenantColumn`.

`TableInfo.IsBrowsable == Role == Documents` (`Models/AdminModels.cs:7`) drives every consumer:
`ExplorerTree.razor:210`, `FilterQueryPanel.razor:276`, `DatabaseOverview.razor:56`,
`ExplorerPane.cs:128`, `DatabaseOverviewScreen.cs:56/130`, `AiToolSurface.cs:109`,
`DocumentAdminService.Outbox.cs:47`.

`TestConnection` (`DocumentAdminService.cs:42`) is the only thing resembling a verdict, and it is a
sentence built from a count — not a value anything can render or reason about.

## 2. Why that isn't good enough

1. **Foreign tables are actively mislabelled as ours.** The substring rules are unanchored and run
   before any evidence is gathered, so a plain business table called `audit_history`,
   `customer_blobs`, `geo_spatial_index`, `invoice_vec_lines` or `search_fts_cache` is reported as a
   DocumentDb sidecar. The tool then tells the user this database participates when it does not.
2. **Real document tables can be hidden.** A documents table named `orders_history` never reaches the
   envelope probe — it is classified `History` and disappears from the explorer, the filter console
   and the AI tool surface.
3. **Nothing links a sidecar to an owner.** `foo_history` is called a history sidecar even when no
   `foo` exists. There is no `Owner` on `TableInfo`, so the UI can't say what a sidecar belongs to.
4. **The envelope probe is a weak identity test.** Any table with those five *column names* passes,
   regardless of types, primary key, or whether `Data` contains JSON. Conversely it says nothing
   about how confident we are — an empty table with the right columns and a legacy table full of
   documents are indistinguishable in the output.
5. **It costs one failing statement per foreign table.** Point the tool at a shared schema with 300
   tables and it issues ~300 statements that raise `DbException`. It also constrains us: these probes
   must stay outside a transaction, because on PostgreSQL a failed statement aborts the enclosing one.
6. **The role vocabulary is incomplete and lumpy.** `TableRole.Sidecar` covers three unrelated
   features; SQLite's R*Tree shadows (`_spatial_node/_rowid/_parent`), FTS5 shadows
   (`_data/_idx/_content/_docsize/_config`), `{t}_spatial_map` and `{t}_vec_map_{type}` are only
   caught by accident of substring; the outbox is (correctly) just documents but nothing says so.
7. **There is no database-level answer.** The question "is this database participating in DocumentDb,
   and what of it is ours?" is never computed as a value — only inferred from a filtered list.
8. **Soft delete is invisible, and the tool actively violates it.** `AddSoftDelete`
   (`src/Shiny.DocumentDb/SoftDelete.cs:40`) is app-side config only — an interceptor plus a named
   query filter over an ordinary JSON property. It creates no column, table or index, so *nothing*
   Phase 1 reads from the catalog can see it. Consequences today: flagged documents appear in the
   browser mixed in with live ones and nothing says which is which; counts include them; and
   `DeleteDocuments` (`DocumentAdminService.Crud.cs:206`) issues a real `DELETE`, so the admin
   **hard-deletes a document the application would only have flagged**, breaking an invariant the
   app relies on with no warning.

## 3. Plan

### Phase 1 — evidence-based classification (admin only, one provider addition)

**1a. Read the catalog once instead of probing per table.**
Add to `IDatabaseProvider` (next to `BuildListTablesSql`, `src/Shiny.DocumentDb/IDatabaseProvider.cs:250`):

```
string BuildListColumnsSql();   // (table_name, column_name, data_type) for the current schema
```

Default = ANSI `information_schema.columns` with the same system-schema exclusion the existing
default uses; override for SQLite (`sqlite_master` ⨝ `pragma_table_info`), Oracle
(`user_tab_columns`), MySQL (scoped to `DATABASE()`). Three overrides, same shape as the member
already there — this is not an N-provider mechanical edit.

Classification then becomes pure in-memory work over one dictionary: no failing statements, no
transaction constraint, and we gain column *types*, not just names.

Optionally (cheap on every relational engine we support, and already half-built in
`DocumentAdminService.Indexes.cs` via `BuildListAllIndexesSql`) read the index list too, so
`idx_{table}_typename` and `idx_json_*` can be used as corroborating evidence.

**1b. Score the envelope instead of accepting it.**
New `DocumentTableEvidence` in the Core service:

| signal | weight |
| --- | --- |
| all five envelope columns present | required |
| `Data` is a text/JSON column type | strong |
| PK (or unique index) is `(Id, TypeName)` | strong |
| `idx_{table}_typename` exists | strong |
| `idx_json_*` indexes exist on it | strong |
| one sampled row where `Data` parses as JSON and `TypeName` is non-empty | strong, opt-in |
| `TenantId` column | informational (already surfaced) |

Verdict per table: **Confirmed** (envelope + ≥1 strong), **Probable** (envelope only — e.g. an empty
table created by hand), **No**. Both Confirmed and Probable stay browsable; Probable carries a badge
so the UI can say *why* it is unsure rather than pretending.

**1c. Derive owned object names — stop guessing from substrings.**
Once the document tables are known, compute the names DocumentDb *would* have created and match
against the catalog:

- `provider.HistoryTableName(t)` (`IDatabaseProvider.cs:377`)
- `provider.BlobTableName(t)` (`IDatabaseProvider.cs:712`)
- `provider.VectorTableName(t, type)` (`IDatabaseProvider.cs:531`) for every `TypeName` in the table,
  plus the per-provider map table (SQLite `{t}_vec_map_{type}`)
- `{t}_spatial`, `{t}_spatial_map`, and SQLite's R*Tree shadows `{t}_spatial_{node,rowid,parent}`
- the full-text index and its shadows

**Gap to close:** full-text table names are not on the provider contract — each provider interpolates
them inside `BuildCreateFullTextSql` (e.g. SQLite `{t}_fts_{type}` + FTS5 shadows). Add the missing
counterpart to `VectorTableName`, preferably as one member:

```
IEnumerable<string> OwnedTableNames(string tableName, string typeName);
```

with a default implementation that yields history/blobs/vector/spatial names and per-provider
overrides that add their FTS and shadow tables. This is the single new naming contract Phase 1 needs,
and it is the honest place for the knowledge — the provider already owns the DDL.

Anything the catalog holds that is neither a document table nor a computed owned name is `Foreign`,
**whatever it is called**. Name matching becomes confirmation of a name we computed, not a guess.

**1c-bis. Soft delete — say what can and cannot be known.**
Every other feature in this plan is provable from the catalog because the engine writes DDL for it.
Soft delete writes none: `SoftDelete.Configure` (`src/Shiny.DocumentDb/SoftDelete.cs:40`) registers an
interceptor and a named query filter, and the flag is a property inside `Data` like any other. So the
answer is *not* "add another catalog signal" — there is nothing to read. Two honest mechanisms
instead, in this order:

**Declared (authoritative).** `ConnectionProfile` gains

```
List<SoftDeleteFlag> SoftDeleteFlags   // TypeName, PropertyPath, FlagKind (Boolean | Timestamp)
```

— per-connection opt-in config, the same precedent as `EncryptionKeys` (`ConnectionProfile.cs:43`).
The operator states what their app configured; the tool then treats it as fact. This is the part that
actually pays, because it is what unlocks the Phase 2 behaviour (a delete that flags instead of
deleting, a live/deleted/all filter, a badge on flagged rows).

**Inferred (a candidate, never a verdict).** Over the schema sample the tool already reads
(`InferSchema`, 200 bodies, no extra queries), mark a field a *soft-delete candidate* when **both**:

- its observed value shape is one of the two `SoftDeleteMapping.Build` accepts
  (`SoftDelete.cs:212`) — kinds are exactly `{True,False}`, or exactly `{Null, String}` where the
  strings parse as timestamps; **and**
- its name is in the deleted family (case-insensitive `deleted`: `IsDeleted`, `Deleted`, `DeletedAt`,
  `DeletedOn`, `DeletedUtc`, `SoftDeletedAt`).

This is name matching, which §2.1 rejects — the difference is what it is allowed to do. There the name
*decided a classification on its own*; here it is one of two required signals and the output is a
suggestion the operator confirms into `SoftDeleteFlags`. A candidate never changes a role, never
changes browsability, and is always rendered as "looks like a soft-delete flag — confirm it?", never
as "this type uses soft delete". A domain property called `IsDeleted` that means something else is
therefore harmless: the worst case is a prompt the operator declines.

Corollary: `Restore` / `PurgeDeleted` / `HardDelete` (`SoftDelete.cs:134-169`) are library calls the
admin cannot make — it writes raw JSON over ADO. Their admin equivalents are ordinary SQL over the
declared flag path (clear it, or delete the flagged rows for real), which is why the declaration has
to carry `FlagKind` — restoring a boolean writes `false`, restoring a timestamp writes `null`.

**1d. Model changes** (`Models/AdminModels.cs`):

- `TableInfo` gains `Owner` (string?), `Confidence` (Confirmed/Probable), `Feature` (string?).
- `InferredField` (`AdminModels.cs:84`) gains `SoftDeleteCandidate` (bool) — set by 1c-bis, so the
  Structure tab is where the suggestion surfaces and nothing else has to re-walk the sample.
- `TableRole` splits `Sidecar` into `Spatial`, `Vector`, `FullText`; keeps `History`, `Blobs`,
  `Documents`, `Foreign`. Update both `Describe` switches (`DatabaseOverview.razor:267`,
  `DatabaseOverviewScreen.cs:156`).
- `IsBrowsable` stays `Role == Documents` — every existing consumer keeps working unchanged.

**1e. A database-level verdict.** New record, computed from the same single catalog read:

```
DatabaseIdentity(
    bool Participates,
    IdentityConfidence Confidence,
    int DocumentTables, int TypeCount, int OwnedSidecars, int ForeignTables,
    IReadOnlyList<string> Features,   // temporal, blobs, spatial, vector, full-text, outbox, tenant, encryption
    IReadOnlyList<string> Reasons)    // "3 tables carry the envelope", "documents_history present", ...
```

`TestConnection` keeps its `string` return for existing callers but formats it from this; a new
`GetIdentity(profileId, ct)` returns the record. Feature detection reuses what the tool already
knows how to find: history/blobs/vector/FT sidecars from 1c, outbox from `FindOutboxes`, tenancy from
the `TenantId` column, encryption from `EncryptedFields` over the schema sample.

`Features` lists `soft delete` **only when declared** (1c-bis). A candidate does not put it in the
list — the verdict is a statement about what the database provably is, and an unconfirmed candidate
is not evidence of anything. It belongs in the Structure tab, not the banner.

### Phase 2 — surfacing it

- **Overview (Blazor)** — verdict banner above the table list:
  *"DocumentDb store · 3 document tables · 12 types · temporal, vectors, outbox"* or
  *"Not a DocumentDb database — 42 tables, none carry the envelope."*
  Split today's single "Other tables" panel into **DocumentDb internals** (owned sidecars, with an
  Owner column, collapsed) and **Other tables in this database** (foreign, collapsed, count only).
- **Explorer tree / filter panel** — already browsable-only; optionally nest owned sidecars under
  their owner instead of hiding them outright.
- **TUI** — the verdict in the heading/status line, `Owner` in the role column
  (`history → documents`), and a key to toggle foreign tables.
- **Connections list / edit** — the test button reports the verdict sentence plus the top reason.
- **Profile setting** `HideForeignTables` (default on) on `ConnectionProfile`, so "ignore tables that
  aren't documents" is a stated behaviour rather than an accident of filtering.
- **AI tool surface** (`AiToolSurface.cs:109`) — report role, owner and confidence so the assistant
  stops treating a mislabelled table as browsable. For a declared soft-delete type, scope its
  queries to live documents by default and say so, so the assistant does not report flagged
  documents as current.
- **Soft delete, once declared** (this is the payoff for 1c-bis, and it fixes §2.8):
  - Browse gains a live / deleted / all tri-state, lowered to a predicate over the declared flag
    path through the existing `FilterPaths` machinery — no new SQL surface.
  - Flagged rows render dimmed with a `deleted` badge in both front ends, so "all" is readable.
  - The delete action defaults to **flag** (an `UPDATE` writing `true` / now) with **delete
    permanently** as the explicit second choice, and a restore action that writes `false` / `null`.
    Today's silent hard delete becomes a deliberate choice.
  - Undeclared types keep exactly today's behaviour — a plain delete. No inferred candidate ever
    changes what a button does.
- **Structure tab** — a candidate field carries "looks like a soft-delete flag" with a one-click
  declare, which writes the `SoftDeleteFlags` entry on the profile.

### Phase 3 — a definitive marker (optional; decide after Phase 1 ships)

If heuristics ever prove insufficient, have the engine record what it owns: a `__shinydocdb`
registry table written at table-init (shared DDL, provider supplies quoting only), one row per owned
object — `ObjectName`, `Role`, `OwnerTable`, `TypeName`, `Feature`, `LibraryVersion`, `CreatedAt`.

**Soft delete is the strongest argument for this phase**, because it is the one feature Phase 1
cannot prove at all. The registry is where a behaviour with no DDL can still become a fact: a row
with `Role = Feature`, `Feature = "soft-delete"`, `TypeName`, and the flag property name in a detail
column. `SoftDelete.Configure` already holds everything needed at configuration time (the type and
`SoftDeleteMapping.PropertyName`), and the named query filter is on `options.Mappings`
(`SoftDelete.cs:49`) where table-init can see it — so this is a write, not a new inference. If it
lands, the profile declaration from 1c-bis becomes the fallback for databases written before it,
not the primary path.

- **For:** identity becomes a fact rather than an inference, and the same registry fixes a real
  hazard elsewhere — `DocumentStore.ClearAll` (`src/Shiny.DocumentDb/DocumentStore.cs:2292`) today
  issues `DELETE FROM` against **every** table `BuildListTablesSql` returns, including tables the
  library never created.
- **Against:** a new table in every store; extra writes at init; must be skippable under
  `SkipTableInitialization` and on read-only connections; needs a backfill path for existing
  databases (the admin can offer "register existing objects"); every provider has to agree.
- Phase 1 remains the fallback regardless, because databases created before it exists will have no
  registry. Ship it as its own feature with its own release note if it is wanted.

## 4. Tests

`tests/Shiny.DocumentDb.Tests` (admin classification is Core-level, so it can run against the
existing per-provider fixtures — Docker required for everything but SQLite/DuckDB, per CLAUDE.md):

- Per relational provider: create a store with temporal + blobs + spatial + vector + full text +
  outbox, then add decoys — `audit_history`, `customer_blobs`, `geo_spatial_index`,
  `search_fts_cache`, and an `orders` table with a *partial* envelope. Assert the exact
  role/owner/confidence for every table and `Participates == true`.
- A database of foreign tables only → `Participates == false`, zero browsable, non-empty `Reasons`.
- A documents table named `orders_history` with no `orders` → `Documents` (today's regression).
- An empty hand-created envelope table → `Probable`, still browsable.
- No per-table probe statements are issued (assert via the provider's SQL log hook).
- Soft delete (1c-bis):
  - A store with `AddSoftDelete(x => x.IsDeleted)`, some documents removed → the catalog signals are
    unchanged (nothing new exists), `Features` does **not** list soft delete, and `IsDeleted` comes
    back as a candidate from `InferSchema`. Repeat for a `DateTimeOffset?` flag.
  - A `bool IsActive` and a non-null `DeletedBy` string → **not** candidates (shape fails, name fails).
  - A domain `bool IsDeleted` on a store with no soft delete → candidate, and asserting that it
    changes no role, no `IsBrowsable`, no `Features` entry and no delete behaviour is the point of
    the test.
  - Declared flag: browse live/deleted/all returns the right partition; the delete action writes the
    flag rather than deleting (assert the row survives with the flag set); restore clears it to
    `false` / `null` per `FlagKind`; permanent delete still removes the row.
  - Undeclared type: delete is still a hard delete (no behaviour change without a declaration).

## 5. Docs / skill / readme (per CLAUDE.md)

- Docs site `~/Desktop/dev/documentation/src/content/docs/documentdb/admin/*` — describe the verdict
  banner and the internals/foreign split; refresh the overview screenshot
  (scripted puppeteer capture, see `plans/`-adjacent notes on admin screenshots). Document the
  soft-delete declaration explicitly, including the reason it exists: the tool has no way to
  discover soft delete from the database, and until a type is declared the admin's delete is a
  permanent one. Cross-link the soft-delete feature page.
- Release note under `## 13.x TBD` in `release-notes.mdx` — `type="enhancement"` for Phase 1/2;
  `type="feature"` (and `breaking` if `TableRole` members shift for embedders) as applicable.
- Skill `skills/shiny-documentdb/SKILL.md` — only if `OwnedTableNames` / `BuildListColumnsSql` land
  on `IDatabaseProvider`; that is a provider-author surface, so a line in the provider section, not
  in the codegen guidance.
- `readme.md` — only if Phase 3 (engine registry) is built.
