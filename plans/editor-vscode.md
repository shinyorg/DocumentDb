# Plan: VS Code extension (`shiny-documentdb`)

**Status:** Designed, not started. **Build this client first** — see "Why VS Code before SSMS".
**Target version:** `13.6`, alongside the server.
**Depends on:** `plans/editor-language-server.md` phases 1–2. This document is transport plus UI only; the
grammar, the schema inference and the execution all live in the server.
**New tree:** `editors/vscode/` (TypeScript, its own `package.json`, not in `DocumentDb.slnx`).

> Self-contained build spec. Read `CLAUDE.md` (repo root) for the four-artifact rule before considering any
> commit "done". Branch off `v13`.

---

## Why VS Code before SSMS

Microsoft retired Azure Data Studio on **28 February 2026** and named VS Code with the `mssql` extension as
the successor, so the data-tooling audience is actively moving into VS Code and expects to find tools there.
On the other side, the SSMS FAQ says plainly that *"third-party extensions are not supported"* and that
feedback items about them are *"closed without investigation"*. VS Code is where the extension is supported,
discoverable, cross-platform, and where every provider we support — not just SQL Server — is in reach.

Ship here first, prove the server against a real client, then reuse it in the VSIX.

## Goal

`.ddbq` files that behave like `.sql` files do with the `mssql` extension: pick a connection, get completion
over the real document composition, see squiggles as you type, hit a key, get rows.

```
   DOCUMENTDB                        ┌─ orders.ddbq ─────────────────────────┐
   ▾ local-sqlite                    │ from Order                            │
     ▾ documents                     │ where status == 'open'                │
       ▸ Order       4,312           │   and total:number > 100              │
       ▸ Customer      871           │ order by createdAt desc               │
   ▾ prod-sqlserver  (read-only)     │ limit 50                              │
     ▸ documents                     └───────────────────────────────────────┘
                                     Results · Generated SQL · Explain · Messages
```

## Non-goals

- Not a replacement for the `mssql` extension. It does not connect to SQL Server as SQL Server; it connects to
  a DocumentDb store that may happen to live in one.
- No document editing grid in v1. The admin tool owns CRUD; the editor is query-first. (Row → "Open in
  admin" is the bridge.)
- No notebook in v1 (phase 3 below).
- No bundled .NET runtime.

---

## Package layout

```
editors/vscode/
  package.json            contributes: language, grammar, commands, views, settings, keybindings
  src/extension.ts        activation, server acquisition, client wiring
  src/serverAcquire.ts    finds/installs/updates shinydocdb-lsp
  src/connections.ts      profile list, secret storage, status bar
  src/explorer.ts         TreeDataProvider over documentdb/collections + documentdb/schema
  src/results.ts          results webview (grid, SQL, explain, messages)
  syntaxes/ddbq.tmLanguage.json
  snippets/ddbq.json
  language-configuration.json
```

Built with `esbuild`, published with `vsce`. Versioned in lockstep with the server package so the pinned
server version is a constant in `package.json`.

## Server acquisition

The VSIX ships **no runtime and no server**. On activation:

1. `documentdb.server.path` set? Use it (developer loop, and the escape hatch).
2. Else look for `<globalStorageUri>/tools/shinydocdb-lsp` at the pinned version.
3. Else check for .NET 10 (`dotnet --list-runtimes`). Missing → a notification with the download link and no
   further nagging; the extension stays dormant rather than half-working.
4. Else `dotnet tool install Shiny.DocumentDb.LanguageServer --version <pinned> --tool-path <globalStorage>/tools`,
   with progress. On extension update with a version bump, `dotnet tool update` on first activation.

This keeps the VSIX under a megabyte, lets the server ship fixes without a Marketplace round trip, and means
a user with several DocumentDb workspaces has one server binary.

Activation events: `onLanguage:documentdb`, `onView:documentdbExplorer`, and the commands. Nothing runs until
a `.ddbq` file or the explorer is opened.

## Client wiring

`vscode-languageclient/node`, `TransportKind.stdio`, document selector `{ language: 'documentdb' }`.

Everything in the "Standard LSP" table of the server plan is free — completion, hover, signature help,
diagnostics, semantic tokens, document symbols, code actions, formatting — because they are protocol. The
extension's own code is only the four things LSP does not cover:

### 1. Connections

- A `documentdb.connections` setting (array of `{ id, name, provider, table, readOnly }`) plus the connection
  string in `SecretStorage`, never in `settings.json`.
- **Also read the admin tool's profile store**, so a connection made in `shinydocdb` or the web admin is
  offered here with no re-entry. Read-only import — the extension proposes, the user accepts, and the copy
  lives in VS Code.
- Status bar item shows the active connection and its mode; clicking it opens the picker. A `.ddbq` file
  remembers its connection in workspace state, so reopening a file does not re-ask.
- `-- @connection prod-sqlserver` as the first line of a file pins it, which is what makes a checked-in query
  reproducible for the next person.

### 2. Explorer view

`TreeDataProvider`: connection → table → collection (with document count) → field (with kind, occurrence %,
index and encryption badges) — fed by `documentdb/collections` and `documentdb/schema`, so the tree and the
completion list can never disagree. Context menus: New Query, Refresh Schema, Copy Path, Create Index,
Open in Admin (launches `shinydocdb` or the web admin at that collection, when installed).

### 3. Results

A webview panel per query editor (reused, not stacked), with four tabs:

| Tab | Content |
|---|---|
| **Results** | Virtualised grid over the returned `JsonObject`s. Nested objects render as expandable cells; a row expands to raw JSON. Copy as JSON / CSV; save to file. |
| **Generated SQL** | `documentdb/previewSql`, updated as you type even before running — this is the tab that teaches people what the grammar costs, and it is the admin console's best idea. |
| **Explain** | `documentdb/explain`, rendered per statement (it is a list, not a string). |
| **Messages** | Elapsed, rows returned vs. matched, truncation notice at 500, warnings. |

Theme-aware (VS Code CSS variables), no external CDN, CSP-locked.

### 4. Commands and keys

| Command | Default key |
|---|---|
| `documentdb.run` — run the statement at the cursor | `Ctrl/Cmd+Enter` |
| `documentdb.runAll` | `Ctrl/Cmd+Shift+Enter` |
| `documentdb.cancel` | `Ctrl/Cmd+Alt+Break` |
| `documentdb.newQuery` (from the explorer, pre-filled `from <collection>`) | — |
| `documentdb.selectConnection` | — |
| `documentdb.refreshSchema` | — |
| `documentdb.previewSql` (toggle live SQL) | — |

## Settings

| Setting | Default | Notes |
|---|---|---|
| `documentdb.connections` | `[]` | Secrets excluded. |
| `documentdb.importAdminProfiles` | `true` | Offer the admin tool's profiles. |
| `documentdb.rowLimit` | `500` | Server-capped at the same number. |
| `documentdb.sampleSize` | `200` | Documents sampled for the composition model. |
| `documentdb.diagnostics.<code>` | `on` | Per-rule severity for `DDB1002`–`DDB1006`. |
| `documentdb.allowWrites` | `false` | Per-connection override; gates `ExecuteDelete`/`ExecuteUpdate`/`CreateIndex`. |
| `documentdb.server.path` / `documentdb.server.trace` | — | Developer loop. |

## Syntax and snippets

A small TextMate grammar (keywords, strings, numbers, comments, `:type` hints) so a file coloured before the
server starts is not white. Semantic tokens refine it once the server is up — field vs. function vs. hint is
decided by the real lexer, which no TextMate rule will get right.

Snippets: `from…where…order by…limit`, `group by…having`, `select … as …`, and one per non-obvious function
(`withindistance`, `lucenematch`, `soundex`).

## Testing

- `@vscode/test-electron` integration tests: activation with no .NET present (must degrade to a notification,
  not throw), server handshake, completion at a caret in a fixture file against a seeded SQLite store shipped
  in the fixture folder, run-and-render.
- Unit tests for `serverAcquire` version resolution and for the `-- @connection` directive.
- The server's own tests carry the grammar and schema coverage; this suite must not re-test them.
- Lint/format via the repo's existing conventions for the admin `wwwroot` assets.

## Publishing

Marketplace publisher `shinylib` (same identity as the NuGet owner), extension id `shiny-documentdb`, icon
from the existing brand assets, categories *Programming Languages* + *Data Science*. Also publish to
Open VSX so VSCodium/Cursor users are covered. The Marketplace readme is a trimmed copy of the docs page,
with the same screenshots — capture them with the existing scripted puppeteer flow used for the admin docs
(`plans`/tooling note in `project_admin_docs_screenshots`), pointed at the extension host instead.

## Risks

| Risk | Mitigation |
|---|---|
| No .NET 10 on the machine | Detect and degrade; never a broken half-experience. Consider a self-contained fallback download if telemetry says this bites. |
| Schema sample is wrong (heterogeneous collection) | Every schema-derived diagnostic is a warning with the sample size in its message, and every one is suppressible. Never an error. |
| Large result sets | Server caps at 500 and says so; the grid virtualises; export streams from a re-run rather than holding rows in the webview. |
| Confusion with the `mssql` extension | Docs and the Marketplace listing lead with what this is *not*; the language id and file extension are distinct. |

## Documentation

`documentdb/editors/vscode.mdx` — install, connect, the four tabs, keybindings, settings table, screenshots.
Cross-link from `admin/query-console.mdx` ("the same console, in your editor") and from `querying.mdx`.
Release note: `feature`. `readme.md` gains the extension in the tooling list, and
`skills/shiny-documentdb/SKILL.md` gains `.ddbq` and the statement syntax to its `triggers:` list, so an agent
asked to "write a DocumentDb query file" produces the right thing.
