# Shiny DocumentDb — Working Notes

Guidance for maintaining this repo. Code lives in `src/`, tests in `tests/`, the published Claude
Code skill in `skills/`, and the public documentation site in a **separate** repo at
`~/Desktop/dev/documentation` (rendered to https://shinylib.net/documentdb).

DocumentDb is a schema-free, multi-provider JSON document store. The core contract is
`IDocumentStore` in `Shiny.DocumentDb`; each backend (SQLite, LiteDB, MongoDB, Cosmos, DuckDB,
IndexedDB, MySQL, SQL Server, PostgreSQL, Oracle) is a separate provider package under `src/`, and
Orleans persistence sits on top of the same store contract.

## After every new feature or fix

A change is not "done" until the four artifacts below are in sync. Do all of them in the same
change unless there's a reason not to.

1. **Code + tests** (`src/`, `tests/`)
   - New behavior that lives on `IDocumentStore` should work against (or be explicitly scoped away
     from) every provider — note the provider compatibility tier in the release note when a feature
     is backend-specific (relational vs MongoDB vs Cosmos vs dev-only).
   - Run the relevant suite before considering the change complete — at minimum
     `dotnet test tests/Shiny.DocumentDb.Tests/Shiny.DocumentDb.Tests.csproj`, plus
     `tests/Shiny.DocumentDb.Orleans.Tests` for Orleans changes.

2. **Documentation site** (`~/Desktop/dev/documentation/src/content/docs/documentdb/`)
   - Update the relevant feature page (e.g. `crud.mdx`, `querying.mdx`, `orleans.mdx`,
     `<provider>.mdx`).
   - Add a **release note** — see the release-note rules below.
   - Pages are `.mdx`; release notes use the `<RN>` component
     (`import RN from '/src/components/ReleaseNote.astro'`), with `type="feature|enhancement|fix|breaking"`.

3. **Skill** (`skills/shiny-documentdb/SKILL.md`)
   - This is the source of the published `shiny-documentdb` Claude Code skill — the agent-facing
     "how to generate correct code" doc.
   - Keep `SKILL.md` aligned with the code. Update the `triggers:` keyword list near the top when a
     new public type / provider / API is introduced.
   - If the default or recommended pattern changes, the skill's default guidance must change too.

4. **readme.md** (repo root)
   - This file is packed into the NuGet package (`PackageReadmeFile` in `Directory.Build.props`).
     Update the feature list and any inline guidance when behavior changes.

## Release notes

Release notes live in the documentation repo at
`~/Desktop/dev/documentation/src/content/docs/documentdb/release-notes.mdx`.

**Which version does a note go against?** Use the `version` field in `version.json` (this repo uses
Nerdbank.GitVersioning) — **the raw version portion only** (strip any prerelease/build-metadata
suffix, e.g. `7.1.1-beta` → `7.1.1`).

**Heading style — match the existing file.** Feature/minor releases are headed by `major.minor`
(`## 7.1 - June 13, 2026`); patch releases use the full `major.minor.patch` (`## 5.2.2 - May 30,
2026`). Pick the heading that matches the kind of release you're cutting.

**If the version isn't released yet (beta / prerelease, or work-in-progress for the next version):**
- If a `## <version> TBD` heading already exists, **add the note under that existing section**. If
  you're modifying a feature that hasn't shipped yet (already an entry under a `TBD` section), edit
  that existing entry in place rather than adding a duplicate.
- If no section exists for that version yet, **create a new `## <version> TBD` heading** at the top
  and add the note there.

**If the version is a final release**, the section is dated (`## 7.1 - June 13, 2026`); add the note
under the matching dated section (or promote the `TBD` section to a dated one when cutting the
release).

Each note is a single `<RN>` line. Use `type="breaking"` for breaking changes (it's its own note
type here, not a flag). Newest version section stays at the top of the file.
