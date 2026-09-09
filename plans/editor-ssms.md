# Plan: SSMS 22 / Visual Studio 2026 extension (`Shiny.DocumentDb.VisualStudio`)

**Status:** Designed, not started. **Gated on a spike — read "The support problem" first.**
**Target version:** `13.7` (after the VS Code client has proved the server).
**Depends on:** `plans/editor-language-server.md` phases 1–2, and `plans/editor-vscode.md` having shipped.
**New tree:** `editors/visualstudio/` (VSIX, not in `DocumentDb.slnx` — it needs a Windows/VS build).

> Self-contained build spec. Read `CLAUDE.md` (repo root) for the four-artifact rule before considering any
> commit "done". Branch off `v13`.

---

## The support problem (state this before writing any code)

SSMS 22 is generally available, 64-bit, x64 and Arm64, and **is built on the Visual Studio 2026 shell**. Its
FAQ answers "Are extensions supported in SSMS?" like this:

> Third-party extensions are not supported. SSMS does not actively block them from loading, but any items
> related to third-party extensions on the feedback site are closed without investigation.

So an SSMS extension is *possible and unsupported*. That is not a reason to skip it — DBAs live in SSMS and
that is the whole point of the request — but it changes the design in three concrete ways:

1. **Target Visual Studio 2026 as the primary host; SSMS is one extra `InstallationTarget` on the same VSIX.**
   The same artifact then serves developers in VS, where extensions *are* supported, so the investment is not
   hostage to an unsupported host. VS 2026's API-version compatibility model means one VSIX covers both.
2. **Put as little as possible inside the host process.** All the value — grammar, schema, execution — is in
   the out-of-process language server. The VSIX is an activation shim, a tool window, and an editor
   registration. If SSMS breaks it, the breakage is small and the server is untouched.
3. **Do not touch Object Explorer.** Extending SSMS's own trees means the `Microsoft.SqlServer.Management.*`
   assemblies, which are exactly the surface the FAQ is disclaiming. We bring our own tool window instead.

The out-of-process design also removes the ARM64 hazard that catches SSMS extensions: because the server is a
separate process, its architecture is independent of the host's, so an Arm64 SSMS running the x64 shell under
emulation is not a constraint on us. The managed shim stays **AnyCPU** regardless, and the VSIX carries both
`win-x64` and `win-arm64` server payloads.

### Spike (2 days) — the gate

Before anything else, answer one question: **does SSMS 22 load a VisualStudio.Extensibility (out-of-process)
extension, and does its `LanguageServerProvider` activate?**

- Build the minimal `LanguageServerProvider` sample against a stdio echo server. Install into VS 2026 → must
  work (documented path). Install into SSMS 22 → observe.
- If it works: that is the model. Modern, out-of-proc, .NET-based, no `Microsoft.VisualStudio.Shell` in-proc
  package, installs without restarting.
- If it does not: fall back to the classic VSSDK VSIX with `ILanguageClient`
  (`Microsoft.VisualStudio.LanguageServer.Client`) — in-process, .NET Framework, but the long-established way
  LSP reaches the VS shell and the way existing SSMS extensions are built. Everything else in this plan is
  unchanged; only the activation class differs.
- If **neither** loads in SSMS 22: ship the VSIX for Visual Studio 2026 only, say so in the docs, and revisit
  when SSMS's extension story changes. Do not spend the phase fighting the host.

Record the answer, the SSMS build number it was tested against, and the pkgdef/manifest that worked — that is
the artifact of the spike.

---

## Goal

The same experience as the VS Code client, in the window a DBA already has open:

- **File → New → DocumentDb Query**, or right-click a collection in the DocumentDb tool window.
- A `.ddbq` editor with completion over the real document composition, live squiggles, hover, semantic
  colouring — all of it from the shared server, none of it reimplemented.
- **Ctrl+Shift+E** to run; results dock beside the T-SQL results the user already knows, with the
  **generated SQL** on its own tab — which is the tab that makes this make sense to a DBA, because it turns
  "some JSON tool" into "here is the `JSON_VALUE` predicate it will run against your table".

The pitch to an SSMS user is specific: *the DocumentDb tables in this database are opaque `Id / TypeName /
Data` rows in Object Explorer; this makes them queryable in the language the application uses, and shows you
the T-SQL underneath.*

## Non-goals

- No Object Explorer nodes, no SSMS-owned windows, no `Microsoft.SqlServer.Management.*` references (v1).
- No T-SQL features. SSMS owns those.
- No document editing grid.
- Not shipped through the Visual Studio Installer (that is for Microsoft workloads); a Marketplace VSIX and a
  direct download.

---

## Components

```
editors/visualstudio/
  Shiny.DocumentDb.VisualStudio/
    Extension.cs                 VisualStudioContribution root
    DocumentDbLanguageServer.cs  LanguageServerProvider (or ILanguageClient in the fallback shape)
    ServerLocator.cs             picks the win-x64 / win-arm64 payload, spawns it
    ExplorerToolWindow.cs        connections -> tables -> collections -> fields
    ResultsToolWindow.cs         grid / Generated SQL / Explain / Messages
    Commands/                    NewQuery, Run, RunAll, Cancel, SelectConnection, RefreshSchema
    Connections/                 profile list + DPAPI-protected connection strings
    server/win-x64/, server/win-arm64/   the self-contained server payload
  source.extension.vsixmanifest
```

### The server payload

Unlike VS Code, there is no SDK to `dotnet tool install` with — SSMS users are not required to have .NET
installed at all. So the VSIX **carries** the server: self-contained, single-file, `win-x64` and `win-arm64`,
built from the **SQL Server-only flavour** described in the server plan.

Size is the whole reason that flavour exists. `plans/tool-package-size.md` measured the admin tool at 152 MB
packed, with DuckDB natives 70% and SQLitePCLRaw 20% of it — none of which belongs in an SSMS extension.
Dropping to SQL Server only should land near 35 MB compressed for both RIDs; **measure it in the spike** and
if it overshoots, the fallback is the VS Code acquisition model plus a "install .NET 10" prompt.

Untrimmed: `Microsoft.Data.SqlClient` is reflection-heavy and `ShinyDocDbMyAdmin.Core` already turns the
trim/AOT analyzers off for exactly that reason.

### Editor registration

Content type / document type `documentdb`, extension `.ddbq`, mapped to the language server. Completion,
diagnostics, hover, signature help, semantic tokens and code actions all arrive over LSP — the VSIX writes
none of them. A minimal classification fallback keeps a file coloured before the server is up.

### Tool windows

**DocumentDb Explorer** — our own tree, same shape and same source as the VS Code explorer
(`documentdb/collections` + `documentdb/schema`): connection → table → collection (count) → field (kind,
occurrence %, indexed, encrypted). Context menu: New Query, Refresh Schema, Copy Path, Create Index.

**Results** — four tabs matching the VS Code client exactly (Results grid, Generated SQL, Explain, Messages),
because two front ends that disagree about what a result looks like is the thing `ShinyDocDbMyAdmin.Core`
exists to prevent.

### Connections

- Own profile list, connection strings protected with DPAPI (current user), stored under the extension's data
  folder — and, as in VS Code, offer to import the `shinydocdb` admin tool's profiles rather than retyping.
- **Stretch (only if the spike says the API is safe):** seed a connection from the active SSMS query window's
  server/database. It is a genuinely nice touch — "query the DocumentDb tables in the database I am already
  connected to" — but it reaches into host services the FAQ disclaims, so it must be optional, wrapped, and
  failure-tolerant. Never a dependency for anything else to work.
- Read-only by default, exactly as the server plan specifies; the mode is visible in the tool window title.

### Commands and keys

| Command | Key | Notes |
|---|---|---|
| Run statement at cursor | `Ctrl+Shift+E` | Matches SSMS muscle memory for "execute". |
| Run all | `Ctrl+Shift+Alt+E` | |
| Cancel | `Alt+Break` | |
| New DocumentDb Query | — | File → New, and the explorer context menu. |
| Select connection / Refresh schema | — | |

## Manifest and installation

- `InstallationTarget` for Visual Studio 2026 (`Microsoft.VisualStudio.Community`, the VS 2026 API version
  range) **and** for SSMS 22. The `IntegratedShell` target no longer exists in VS 2026 and must not be used.
- Managed assemblies **AnyCPU**, prefer-32-bit off. The native payload is the server's, in its own process.
- Signed VSIX; publish to the Visual Studio Marketplace and offer a direct `.vsix` download for locked-down
  environments.
- Documented uninstall/disable path, because an unsupported-host extension must be trivially removable when a
  DBA is triaging an SSMS problem.

## Testing

- Unit-testable pieces (`ServerLocator` RID selection, connection store, the LSP wiring seams) in a normal
  xUnit project; the VSIX itself is not covered by the repo's `dotnet test` run and must not be wired into
  `build.slnf`.
- A written manual matrix, run per SSMS/VS build and recorded with build numbers: install, activate, complete,
  diagnose, run, cancel, uninstall — on VS 2026 x64, SSMS 22 x64, SSMS 22 on Arm64.
- CI builds the VSIX on a Windows runner and publishes it as an artifact; it does not gate the main build.

## Risks

| Risk | Likelihood | Mitigation |
|---|---|---|
| SSMS refuses to load the extension model | Medium | The spike is the gate. Two activation shapes tried, and a VS-2026-only outcome is an acceptable answer, not a failure. |
| An SSMS update breaks activation | **High over time** — it is unsupported by policy | Keep the host surface tiny; pin and publish a tested-build matrix; the server is versioned separately so a host break never regresses VS Code. Say all of this in the docs, on the Marketplace page, and in the release note. |
| VSIX size | Medium | SQL Server-only server flavour; measured in the spike; acquisition-model fallback. |
| Arm64 | Low | Out-of-process server + AnyCPU shim + both RID payloads. |
| Maintenance cost of a Windows-only, IDE-coupled artifact | Medium | It is the *last* phase for a reason: it inherits a server and a UI design that two other front ends already exercise. If the spike is ugly, VS Code alone still delivers the feature. |

## Documentation

`documentdb/editors/ssms.mdx` — the support caveat **first and plainly**, then install, connect, run, the
generated-SQL tab, the tested-build matrix, and uninstall. `documentdb/editors/index.mdx` compares the two
hosts so nobody installs the wrong one. Release note: `feature`, with the unsupported-host caveat in the note
itself. `readme.md` tooling list updated.
