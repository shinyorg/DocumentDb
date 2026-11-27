# Shiny DocumentDb — Working Notes

Guidance for maintaining this repo. Code lives in `src/`, tests in `tests/`, the published Claude
Code skill in `skills/`, and the public documentation site in a **separate** repo at
`~/Desktop/dev/documentation` (rendered to https://shinylib.net/documentdb).

DocumentDb is a schema-free, multi-provider JSON document store. The core contract is
`IDocumentStore` in `Shiny.DocumentDb`; each backend (SQLite, LiteDB, MongoDB, Cosmos, DuckDB,
IndexedDB, MySQL, SQL Server, PostgreSQL, Oracle) is a separate provider package under `src/`, and
Orleans persistence sits on top of the same store contract.

## Branching & releases

`main` is the development branch; CI (`.github/workflows/ci.yml`) builds, tests and packs every push and
PR to it without publishing. **Releases come from tags only** — pushing `v14.5.0` (or `v14.5.0-beta.1`
for a prerelease) runs `release.yml`, which builds every package at that version (`-p:Version`), pushes
to NuGet, creates the GitHub release, then publishes the admin image + Docker Desktop extension under the
same version (moving `latest` and the Dockhand stack pin only for stable versions) and syncs the skills.
`release.yml` can also be run by hand with a version, which creates the tag. Nothing in the repo holds a
version; local builds are `0.0.0-dev` (`Directory.Build.props`).

## Removing or replacing code — no leftover cruft

This is an open-source library; keep the tree clean. When a member, type, or package is removed or
replaced, **delete it outright** — do not leave `[Obsolete]` shims, deprecated no-op methods, forwarding
stubs, dead flags, or "kept for back-compat" wrappers behind. A clean breaking change with a release note
is preferred over accumulating compatibility cruft. Update every call site, test, sample, and doc in the
same change so nothing references the removed thing. (Semantic versioning + release notes communicate the
break; the code stays lean.)

## After every new feature or fix

A change is not "done" until the four artifacts below are in sync. Do all of them in the same
change unless there's a reason not to.

1. **Code + tests** (`src/`, `tests/`)
   - New behavior that lives on `IDocumentStore` should work against (or be explicitly scoped away
     from) every provider — note the provider compatibility tier in the release note when a feature
     is backend-specific (relational vs MongoDB vs Cosmos vs dev-only).
   - **Always run the tests and ensure coverage after every feature/bugfix prompt** — a change is not done
     until the suite is green *and* the new/changed behavior has a test proving it. Run the **full** suite
     (`dotnet test --project tests/Shiny.DocumentDb.Tests/Shiny.DocumentDb.Tests.csproj`, plus
     `tests/Shiny.DocumentDb.Orleans.Tests` for Orleans changes), not just a filtered subset — parallel/global
     interactions (e.g. the process-wide `ActivitySource`/`Meter`) only surface in a full run.
   - **The full suite needs Docker** (the non-SQLite providers + Orleans use Testcontainers). If Docker is not
     running, **do not report success from a filtered subset** — tell the user Docker is off, ask them to start
     it and prompt you to try again, and only claim "all tests pass" once the full suite has actually run.
   - **Query-surface parity:** whenever you add a value/predicate function usable in a typed LINQ
     `Where`/`OrderBy` (e.g. a new `DocumentFunctions.*` or scalar), also wire it into the
     **string-expression grammar** (`FilterExpressionParser`) so `Where("…")`, the interpolated
     `Where($"…")`, `OrderBy("…")`, and `Project("…")` accept it too — the string API lowers to the same
     `QueryNode` IR, so parity is expected. Add the function name to `IsPredicateFunction`/`IsValueFunction`
     and emit the same call the LINQ path produces. Cover both surfaces (LINQ + string) in tests.

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

**Which version does a note go against?** There is no version file — the git tag *is* the version
(see "Branching & releases" above). Use the **next** version to be tagged: if the top of
`release-notes.mdx` already has a `## <version> TBD` section, that's it; otherwise take the latest
tag (`git describe --tags --abbrev=0`, e.g. `v14.4.1`) and bump it — patch for fixes, minor for
features, major for breaking. Use the raw version only (no `v`, no prerelease suffix).

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

## Blog posts (only when explicitly requested)

Do **not** write blog posts automatically as part of a fix/feature. Write them **only when the user asks**. When asked to blog a feature, produce **two** posts — first the docs-site version, then adapt it for the personal blog.

### 1. Docs site — `~/Desktop/dev/documentation`

- File: `src/content/docs/blog/YYYY/MM/<slug>.mdx` (current year/month folders; create the month folder if needed).
- Frontmatter:
  ```yaml
  ---
  title: '...'
  description: '...'
  date: YYYY-MM-DD
  authors:
    - allanritchie
  tags:
    - Release        # or Feature, AI, etc.
  ---
  ```
- Body is MDX. Reuse components where relevant, e.g. `import NugetBadge from '/src/components/NugetBadge.astro';` then `<NugetBadge name="Shiny.DocumentDb" />`.
- Voice: product/release-note tone — what shipped, breaking changes, code samples, how to use it. **No hero image** on this site.

### 2. Personal blog — `~/Desktop/dev/blog` (adapt the docs post)

- File: `src/content/blog/YYYY/MM/<slug>.mdx` (note: `content/blog`, not `content/docs/blog`).
- Frontmatter (different schema — see `src/content.config.ts`):
  ```yaml
  ---
  title: '...'
  description: '...'
  pubDate: 'Mon DD YYYY'                          # e.g. 'Jun 15 2026'
  heroImage: '../../../../assets/<slug>-hero.svg'
  tags: ['Shiny', '.NET']
  ---
  ```
- Voice: rework the docs post into a personal, first-person narrative ("Here's something that shouldn't be hard but is…", "So I built…") — story/motivation up front, not a dry changelog.
- **Hero image is required.** Create `src/assets/<slug>-hero.svg`:
  - SVG, `viewBox="0 0 1200 630"`, `width="1200" height="630"`.
  - Match the house style: dark navy/indigo gradient background (`#0f172a` → `#1e1b4b`), cyan/green/violet accent gradients, subtle glow filters, the feature name as the headline. Crib an existing one (e.g. `datasync-hero.svg`, `documentdb-orleans-hero.svg`) as a starting template.
