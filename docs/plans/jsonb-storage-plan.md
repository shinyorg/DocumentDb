# Plan: JSONB / native JSON storage across providers

_Status: investigation done, no code changes yet. Created 2026-06-22._

## Current state — document body column type per provider

| Provider | Column type | Native JSON? | Create-table location |
|----------|-------------|--------------|------------------------|
| PostgreSQL | `JSONB` | ✅ already binary JSON | `src/Shiny.DocumentDb.PostgreSql/PostgreSqlDatabaseProvider.cs:41` |
| MySQL | `JSON` | ✅ native | `src/Shiny.DocumentDb.MySql/MySqlDatabaseProvider.cs:28` |
| SQL Server | `JSON` | ✅ native | `src/Shiny.DocumentDb.SqlServer/SqlServerDatabaseProvider.cs:29` |
| DuckDB | `JSON` | ✅ native | `src/Shiny.DocumentDb.DuckDb/DuckDbDatabaseProvider.cs:36` |
| Oracle | `CLOB` + `CHECK (Data IS JSON)` | validated text (not native JSON type) | `src/Shiny.DocumentDb.Oracle/OracleDatabaseProvider.cs:39` |
| SQLite | `TEXT` | ❌ plain text, queried via `json_extract()` | `src/Shiny.DocumentDb.Sqlite/SqliteDatabaseProvider.cs:79` |

Shared notes:
- No shared base class for relational providers — each implements `IDatabaseProvider` independently (`src/Shiny.DocumentDb/IDatabaseProvider.cs`).
- All providers serialize/deserialize with System.Text.Json on the .NET side (`src/Shiny.DocumentDb/DocumentStore.cs:~2008` SerializeDocument/DeserializeDocument), then push filtering/merge into native JSON SQL functions.
- DuckDB & PostgreSQL cast the bound text parameter to their JSON type on write (`CAST(@data AS JSON)` / `CAST(@data AS JSONB)`).

## Key conclusion

JSONB is **not a generic, cross-provider option** — it is PostgreSQL-specific, and we are **already using it there**. MySQL / SQL Server / DuckDB already use their own native JSON column types. So "adopt JSONB everywhere" is not a real task. Only two providers have a meaningful native-JSON upgrade available:

### Candidate 1 — SQLite: `TEXT` → JSONB BLOB (the only real gap)

- SQLite has **no `JSONB` column type**, but since **3.45 (Jan 2024)** it has an internal JSONB binary format.
- Approach: store the body as a BLOB via the `jsonb()` function and query with `jsonb_extract()` instead of `json_extract()`. Benefit: no repeated text re-parse on reads/queries.
- Costs / risks:
  - **Migration** of existing `TEXT` rows to BLOB.
  - Stored value is **no longer human-readable** in the column.
  - Requires SQLite **>= 3.45** at runtime — verify the bundled SQLite version (`Microsoft.Data.Sqlite` / SQLitePCLRaw bundle) meets this on all targets (incl. iOS/Android — cross-check the vector-extension preload notes).
  - Touches read path, write path, query/merge SQL, and index expressions in `SqliteDatabaseProvider`.
- Open question: measure whether the re-parse cost is actually material for our workloads before committing.

### Candidate 2 — Oracle: `CLOB IS JSON` → native `JSON` type (OSON)

- Oracle **21c+** has a native `JSON` data type (OSON binary format); we currently use `CLOB` with a `CHECK ... IS JSON` constraint.
- Benefit: native binary storage + better JSON-function performance.
- Cost: **drops support for Oracle < 21c**. Decide whether that floor is acceptable (likely gate behind a feature/option, or document the minimum version).

## Out of scope / non-actions

- PostgreSQL, MySQL, SQL Server, DuckDB — already on the best native JSON type for each; no change.
- No new abstraction needed; changes (if any) are isolated per provider.

## Suggested next steps (when picked up)

1. Confirm bundled SQLite version >= 3.45 across all target platforms.
2. Prototype SQLite JSONB-BLOB path behind a flag; benchmark read/query vs current TEXT to justify the migration.
3. Decide Oracle minimum-version policy; if 21c+ acceptable, switch column to native `JSON`.
4. If shipping either: write migration, update tests, docs site (`<provider>.mdx` + release note), `skills/shiny-documentdb/SKILL.md`, and root `readme.md` per CLAUDE.md "after every feature" checklist.
