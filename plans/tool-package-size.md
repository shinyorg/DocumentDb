# Plan: shrink the admin tool package (`ShinyDocDbMyAdmin.Tui`)

**Status:** Designed, not started.
**Target version:** `13.0` (tool packaging + one new service in `ShinyDocDbMyAdmin.Core`; no core library changes).
**Packages touched:** `ShinyDocDbMyAdmin.Tui` (the `shinydocdb` dotnet tool), `ShinyDocDbMyAdmin.Core`.
**Not touched:** `Shiny.DocumentDb.DuckDb` keeps `DuckDB.NET.Data.Full`. Library consumers are unaffected — for a
normal `PackageReference`, NuGet resolves natives per-RID at publish and the current behaviour is correct.

> Self-contained build spec. Read `CLAUDE.md` (repo root) for the four-artifact rule (code+tests, docs site,
> skill, readme) before considering any commit "done". Branch off `v13`.

---

## Goal

`dotnet tool install -g ShinyDocDbMyAdmin.Tui` currently pulls a **152 MB** package to run a terminal UI. Get it
under **20 MB** without dropping DuckDB support, and without asking anyone to install a database engine by hand.

## Measurements (v12.5.0, `bin/Release`)

The package is 437.7 MB uncompressed across 192 files. Five files are 70% of it:

| Entry | Uncompressed |
|---|---|
| `runtimes/osx/native/libduckdb.dylib` (universal x64+arm64) | 110.3 MB |
| `runtimes/linux-x64/native/libduckdb.so` | 64.7 MB |
| `runtimes/linux-arm64/native/libduckdb.so` | 58.7 MB |
| `runtimes/win-arm64/native/duckdb.dll` | 39.0 MB |
| `runtimes/win-x64/native/duckdb.dll` | 34.2 MB |
| **DuckDB total** | **306.8 MB (70%)** |
| SQLitePCLRaw natives (all RIDs, both bundles) | ~89 MB (20%) |
| Everything else (managed: Oracle, OpenAI, Anthropic, Terminal.UI, …) | ~42 MB (10%) |

DuckDB is **embedded, not server-based** — there is no server to point at, so the whole vectorized engine,
optimizer, Parquet/CSV/JSON readers and extension host compile into one shared library. That is a real
constraint, not waste; the waste is shipping *five platforms' worth* to every user.

Secondary finding: **`DuckDB.NET.Bindings.Full` ships exactly 5 RIDs** (`linux-arm64`, `linux-x64`, `osx`,
`win-arm64`, `win-x64`). There is no exotic-RID pruning win on DuckDB — all five are load-bearing for someone.
The `browser-wasm` / `ios-*` / `linux-riscv64` / `linux-s390x` / `linux-mips64` entries in `runtimes/` come from
SQLitePCLRaw, and 13.6 MB of those are **iOS `.a` static libraries** — dead weight in a terminal tool by any
reading.

## Why "just reference it as a NuGet" is not available

A `DotnetTool` package has no dependencies. `PackAsTool` runs a publish and flattens the entire closure into
`tools/net10.0/any/`; the produced nuspec has no `<dependencies>` element at all. `dotnet tool install`
downloads exactly one nupkg and unzips it — there is no transitive restore at install time, ever. Whatever the
tool needs at runtime is either in the box or acquired by the tool itself.

So the choice is not "bundle vs. reference". It is **"bundle all five platforms" vs. "acquire the one platform
we are actually running on, on first use"**.

## Non-goals

- **No loss of DuckDB support.** Attaching to a DuckDB file must keep working, including on a machine with no
  DuckDB installed.
- **No change to the library packages.** `Shiny.DocumentDb.DuckDb` is not the problem and does not move.
- **No RID-specific tool packages** (see rejected alternatives).
- **No new provider trimming.** Which providers the tool supports is a product question, out of scope here.

## Design decisions (locked)

| Decision | Choice | Consequence |
|---|---|---|
| Trim mechanism | An **MSBuild target filtering `ResolvedFileToPublish`**, not `ExcludeAssets` | Deterministic. Operates on the final publish item list rather than on NuGet asset resolution — see the note below on why the existing exclusion does not hold. |
| DuckDB native source | **DuckDB's own GitHub release assets**, not the NuGet package | `libduckdb-<platform>.zip` is one platform (~35–110 MB). Re-downloading `DuckDB.NET.Bindings.Full` would fetch all five RIDs — the exact thing we are removing. |
| Acquisition timing | **On first DuckDB connection**, not at install and not at startup | Someone who never opens a DuckDB file never pays. Startup stays instant. |
| Cache location | `AppPaths.NativeDirectory` → `~/.shinydocdbmyadmin/native/duckdb/<version>/<rid>/` | Reuses the existing data-directory contract (`ShinyDocDbMyAdmin:DataDirectory` / `SHINYDOCDBMYADMIN_DATA` already override it). Survives tool updates; version-keyed so a DuckDB bump is a new directory, not a clobber. |
| Native binding | `NativeLibrary.SetDllImportResolver` on the `DuckDB.NET.Bindings` assembly, matching `"duckdb"` | Verified: the bindings' `DllImport` name is literally `duckdb`. One resolver, installed once, before the first P/Invoke. |
| Already-installed engine | **Honoured first.** If `duckdb` resolves through normal OS probing, use it and never download | Respects a system/Homebrew/apt install. Also the offline escape hatch. |
| Failure mode | A clear, actionable error naming the URL, the cache path, and the override env var | The one thing worse than a big download is a silent hang on a machine with no network. |
| Version pinning | The DuckDB version is an MSBuild constant flowed into the loader; **hash-pinned** per RID | The tool must not resolve "latest" at runtime. The managed bindings are ABI-tied to the engine version. See `project_duckdb_15x_regression` — 1.5.x is a live regression, so we stay on 1.4.4 and the pin must be explicit. |

### Why the existing `ExcludeAssets` does not hold — read before writing any of this

`ShinyDocDbMyAdmin.Core.csproj:78-79` already tries this for SQLite:

```xml
<PackageReference Include="SQLitePCLRaw.bundle_e_sqlite3" ExcludeAssets="all"/>
<PackageReference Include="SQLitePCLRaw.lib.e_sqlite3" ExcludeAssets="all"/>
```

**It is not working.** The v12.5.0 package still contains `e_sqlite3.dll`, `libe_sqlite3.so` (25.9 MB across
RIDs) and 13.6 MB of iOS `.a` files. The reason: `Shiny.DocumentDb.Sqlite` and
`Shiny.DocumentDb.Sqlite.VectorSupport` reference `Microsoft.Data.Sqlite` (not `.Core`), which drags
`bundle_e_sqlite3` in through a `ProjectReference` — and `ExcludeAssets` on a *sibling* `PackageReference` does
not suppress that flow.

Do not repeat the pattern for DuckDB. Phase 1 replaces it with a publish-time item filter, which is verifiable
by inspecting the produced package rather than by hoping about restore semantics.

---

## Phase 1 — deterministic publish trim

In `ShinyDocDbMyAdmin.Tui.csproj`, a target that runs after publish items are computed and removes native
payloads the tool cannot use:

```xml
<Target Name="TrimToolNatives" AfterTargets="ComputeFilesToPublish">
  <ItemGroup>
    <!-- DuckDB: acquired on demand by DuckDbNativeLoader. -->
    <ResolvedFileToPublish Remove="@(ResolvedFileToPublish)"
                           Condition="$([System.String]::Copy('%(RelativePath)').Contains('duckdb'))
                                      AND '%(Extension)' != '.dll' OR ..." />
    <!-- ...plus the mobile/wasm/exotic-CPU SQLitePCLRaw RIDs. -->
  </ItemGroup>
</Target>
```

*(Sketch, not final — the real condition must match on `RelativePath` starting `runtimes/` and must not catch the
managed `DuckDB.NET.*.dll`. Author it against the item list, then verify by unzipping the produced nupkg.)*

Removed in this phase:

- `runtimes/*/native/libduckdb.*` and `duckdb.dll` — **306.8 MB**
- `runtimes/{browser-wasm,ios-*,iossimulator-*,maccatalyst-*,android-*}/**` — SQLitePCLRaw mobile/wasm, **~20 MB**
- `runtimes/linux-{riscv64,s390x,mips64,armel,ppc64le,musl-s390x}/**` — CPU targets a terminal tool will not see,
  **~15 MB**

Kept: SQLite/SQLCipher natives for the five desktop RIDs. SQLite is the profile store — the tool cannot start
without it, so it is never a candidate for on-demand acquisition.

**Also fix the root cause while here:** switch `Shiny.DocumentDb.Sqlite` to `Microsoft.Data.Sqlite.Core` +
an explicit bundle reference, or accept the leak and let the publish filter handle it. Prefer the filter for
this release — changing the Sqlite provider's package reference is a library-consumer-visible change and does
not belong in a packaging fix. Leave a comment pointing here.

**Exit criteria:** `dotnet pack -c Release` produces a package with no `libduckdb`/`duckdb.dll` entry and no
mobile RID, and the tool still builds and runs against SQLite/Postgres/SQL Server. Expected: 152 MB → **~14 MB**.

## Phase 2 — `DuckDbNativeLoader` (in `ShinyDocDbMyAdmin.Core`)

```csharp
// Services/DuckDbNativeLoader.cs — called once, before the first DuckDB connection is opened.
public sealed class DuckDbNativeLoader(AppPaths paths, ILogger<DuckDbNativeLoader> logger)
{
    public Task<string?> EnsureAsync(IProgress<double>? progress, CancellationToken ct);
}
```

Order of operations:

1. **Probe first.** Try `NativeLibrary.TryLoad("duckdb", …)`. If a system engine is present and its version is
   compatible, install a resolver returning that handle and return — no download, no cache.
2. **Cache hit.** If `<NativeDirectory>/duckdb/<version>/<rid>/<lib>` exists, resolve to it.
3. **Download.** `https://github.com/duckdb/duckdb/releases/download/v<version>/libduckdb-<platform>.zip`,
   verified against the pinned SHA-256 for that RID, extracted to a temp directory and **atomically moved**
   into place (concurrent tool instances must not observe a half-extracted library).
4. **Resolve.** `NativeLibrary.SetDllImportResolver(typeof(DuckDBBindings).Assembly, …)` mapping `"duckdb"` to
   the cached path.

RID → asset map (verified against the v1.4.4 release):

| RID | Asset |
|---|---|
| `linux-x64` | `libduckdb-linux-amd64.zip` |
| `linux-arm64` | `libduckdb-linux-arm64.zip` |
| `osx-x64`, `osx-arm64` | `libduckdb-osx-universal.zip` |
| `win-x64` | `libduckdb-windows-amd64.zip` |
| `win-arm64` | `libduckdb-windows-arm64.zip` |

### Rules

- **`SetDllImportResolver` throws if called twice for the same assembly.** Guard with a single `Lazy<Task<…>>`
  or an interlocked flag. It must also be installed **before** any DuckDB P/Invoke — once the runtime has
  resolved the import, a later resolver is ignored, and the failure looks like an unrelated `DllNotFoundException`.
- **The UI must not block on a 100 MB download.** The connection flow shows progress and stays cancellable.
  Cancelling leaves no partial file in the cache.
- **Never fall back to "try without DuckDB".** A failed acquisition fails the connection with a message naming
  the asset URL, the cache path it wanted to write, and the manual override — so an air-gapped user can drop the
  library in by hand and proceed.
- **Offline/air-gapped is a first-class path**, not an afterthought: honour a
  `SHINYDOCDBMYADMIN_DUCKDB_PATH` override pointing at an existing library, and document it next to the
  download behaviour.
- **The web front end (`ShinyDocDbMyAdmin`) is out of scope** — it ships as a container image where a layer of
  natives is cheap and network at runtime may be blocked by policy. If it later shares the loader, the
  probe-first path already gives the right behaviour for an image that bakes the library in.

## Phase 3 — measure and document

Record before/after in the release note. State plainly in the docs that the *first* DuckDB connection downloads
an engine, how big it is, where it is cached, and how to pre-seed it offline. A surprise download is a support
ticket; a documented one is a footnote.

---

## Rejected alternatives

| Option | Why not |
|---|---|
| **RID-specific tool packages** (.NET 10 supports per-RID tool publishing) | Works, and keeps everything in-box. But it multiplies every release by five packages and five publish legs for one dependency's benefit, and the user still downloads ~55 MB to run a terminal UI. Revisit only if on-demand acquisition proves unreliable in the field. |
| **Drop DuckDB from the tool** | Free and instant, but a real capability loss — DuckDB is the analytical provider and reading a `.duckdb` file is exactly the ad-hoc job a terminal admin tool is for. |
| **`ExcludeAssets` on `DuckDB.NET.Data.Full` in the Tui** | The mechanism demonstrably does not hold across `ProjectReference` boundaries in this repo (see above). |
| **Switch the library to `DuckDB.NET.Data` (non-Full)** | Verified viable — the non-Full package is 258 KB, managed-only, published in lockstep. But it pushes "install DuckDB yourself" onto every library consumer to solve a problem only the *tool* has. Wrong layer. |
| **Range-request the NuGet package's zip central directory** to extract one RID | Clever, fragile, and unnecessary once GitHub release assets are per-platform. |

---

## Tests (`tests/ShinyDocDbMyAdmin.Tui.Tests`, plus a packaging check)

- **Packaging assertion (the one that actually protects this):** a test that runs `dotnet pack`, opens the
  produced nupkg, and asserts (a) no entry matches `runtimes/*/native/*duckdb*`, (b) no mobile/wasm RID
  directory is present, (c) the managed `DuckDB.NET.Data.dll` and `DuckDB.NET.Bindings.dll` *are* present, and
  (d) total uncompressed size is under a threshold. Without this, the trim silently regresses on the next
  package bump.
- `DuckDbNativeLoader` probe path: a pre-populated cache directory resolves without any network call (assert on
  a stub handler that a request was never issued).
- Download path against a local HTTP fixture: correct asset name per RID; hash mismatch → hard failure, nothing
  written to the cache; cancellation mid-download → nothing written to the cache.
- Two concurrent `EnsureAsync` calls produce one download and one valid library (no torn extract).
- `SHINYDOCDBMYADMIN_DUCKDB_PATH` override wins over cache and download.
- Failure message contains the URL, the cache path and the override name — assert on content, this is the
  air-gapped user's only lifeline.
- End-to-end, marked as requiring network and excluded from the default run: install the trimmed tool output,
  open a DuckDB file, confirm the engine is fetched and a query returns.
- Existing TUI screen-render tests must stay green — the trim must not remove anything the UI loads.

## Four-artifact checklist

- **Code + tests** — as above. Full suite green per `CLAUDE.md`; **Docker required** for the provider tests, so
  do not report success from a filtered subset.
- **Docs** — `documentdb/admin/` terminal-tool page: a "DuckDB engine download" section (when, how big, where
  cached, offline override). Release note against **13.0**, `type="enhancement"` — plus a
  `type="breaking"` note *only if* the offline behaviour changes for someone who is currently working
  air-gapped, which it does: today DuckDB works with no network, after this it needs one first-run download.
  Call that out explicitly.
- **Skill** — no change expected (no public API moves); confirm rather than assume.
- **readme.md** — if it quotes an install size or the provider list for the tool, update it.

## Risks

- **First-run network dependency is a real behaviour regression** for air-gapped DuckDB users. The
  `SHINYDOCDBMYADMIN_DUCKDB_PATH` override and the probe-first path are the mitigation, and the release note
  must lead with it rather than bury it. This is the single most likely complaint.
- **GitHub release assets are a third-party URL.** Asset names have been stable across DuckDB releases but are
  not an API contract. The RID→asset map is a constant in one file; a rename is a one-line fix, but it fails at
  *runtime for users*, not at build time. Consider a build-time test that HEADs each pinned URL so a rename
  surfaces in CI instead of in the field.
- **The trim target is a sharp instrument.** An over-broad condition silently removes something the tool needs
  and the failure appears only at runtime on one platform. The packaging assertion test is what makes this safe
  — write it first.
- **`SetDllImportResolver` ordering** is easy to get wrong and produces a confusing `DllNotFoundException`
  rather than a clear error. Install it from a single, obviously-first code path and test that path.
- **DuckDB version pinning interacts with a known regression** (`project_duckdb_15x_regression`: 1.5.x crashes
  the temporal-history query). The pinned engine version must track the pinned `DuckDB.NET.Data.Full` version
  in `Directory.Packages.props` — currently **1.4.4** — and the two must be bumped together. A comment in both
  places, or better, one MSBuild property both consume.
