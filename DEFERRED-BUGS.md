# Deferred / Known Bugs — Pickup Backlog

Findings from the multi-agent code reviews (v11) that are **not yet fixed**. Each entry is written so a
future contributor/agent can pick it up cold: symptom, root cause with `file:line`, a proposed fix, the
affected files/providers, a test plan, and an effort/risk estimate.

The **HIGH** and most **MEDIUM** review findings are already fixed and verified (see the `11.0.0` release
notes and the `f805e4c` / `e73dda6` / `b99fdce` commits, plus the uncommitted working-tree changes). What
remains below is the deliberately-deferred tail: items that are large, cross-cutting, architectural, or only
testable in an environment we didn't have (browser / specific cloud).

Severity legend: 🟠 medium · 🟡 low · 📐 documented approximation (by-design; fix only if the trade-off changes).

---

## 1. 🟠 Backup `Restore` does not preserve `CreatedAt` / `UpdatedAt` (envelope v2)

**Symptom.** A backup → restore round trip rewrites every document's `CreatedAt` and `UpdatedAt` to the import
time, silently losing creation/modification history.

**Root cause.** The v1 backup envelope is only `{ id, docType, data }` (`BackupStreams.WriteRecord`,
`IDocumentBackup.cs:121`). Every write path binds both timestamp columns to `DateTimeOffset.UtcNow`
(`DocumentStore.Backup.cs:196`; native bulk-copy: Postgres/SqlServer/DuckDb `BulkCopyInsertAsync`). Cosmos even
comments it explicitly.

**Proposed fix (envelope v2, back-compatible).**
1. Add optional `createdAt` / `updatedAt` to the exported envelope; bump a format version marker in the stream
   header (or detect per-record). Export must `SELECT CreatedAt, UpdatedAt` from the source and include them.
2. Thread the timestamps through `RawDocument` / `RawBulkRow`.
3. Restore binds them per row. This is the hard part: the multi-row batch insert SQL uses a single `@now`
   (`IDatabaseProvider.BuildBatchInsertSql`). Add a provider `BuildBackupInsertSql` (or extend the batch SQL)
   that binds `@createdAt_i` / `@updatedAt_i` per row. Fall back to `now` when a v1 record carries no
   timestamps.

**Affected.** `DocumentStore.Backup.cs`, `BackupStreams`, `IDocumentBackup`, `RawDocument`/`RawBulkRow`, and the
batch-insert SQL on **every relational provider** (SQLite, PostgreSQL, MySQL, MariaDB, CockroachDB, SQL Server,
Oracle, DuckDB) + the Mongo/Cosmos backup stores.

**Test plan.** `BulkBackupTestsBase.Export_Then_Restore_RoundTrips` — assert `CreatedAt` survives; add a v1-record
(no timestamps) test asserting the `now` fallback. Run across the provider fixtures.

**Effort/risk.** Large. Cross-provider SQL + a format-version story. Docker-testable on all relational providers.

---

## 2. 🟠 Generated serialization path resolvers use the naming policy, not `JsonTypeInfo`

**Symptom.** For a `DocumentSerialization.JsonContext`-mode type whose source-generated context bakes
PascalCase JSON names *and* which also has a `MapVersion` / spatial / vector / computed mapping, the version
CAS read (and spatial/computed indexing) targets a missing key (`"version"` vs the stored `"Version"`) and
silently fails.

**Root cause.** `DocumentStoreOptions.ResolveVersionJsonPaths` (`:301`, and identically `:445`, `:541`, `:591`,
`:656`) computes JSON paths from `jsonOptions.PropertyNamingPolicy?.ConvertName(...)`, but a metadata-mode
`JsonTypeInfo` bakes its property names at *its* generation time and ignores the runtime naming policy. The
query translator correctly reads names off `JsonTypeInfo.Properties`; these resolvers don't.

**Proposed fix.** Derive the mapped JSON paths from the resolved `JsonTypeInfo.Properties` (match the CLR
member, read its `JsonPropertyInfo.Name`) instead of the raw naming policy — the same approach
`DocumentQueryExtensions.ResolveJsonPathWithType` already uses.

**Affected.** `DocumentStoreOptions.cs` (5 resolver sites).

**Test plan.** A `JsonContext`-mode fixture with a default (PascalCase) context + a `MapVersion` and a spatial
mapping; assert optimistic concurrency and a spatial query both work. In-memory/SQLite runnable.

**Effort/risk.** Medium. Touches shared path-resolution; regression-test the camelCase path too.

---

## 3. 🟠 `DocumentSerialization.Generated` + `[JsonPropertyName]` under trimming (AOT-only)

**Symptom.** Under NativeAOT/trimming, a `Generated`-mode type with `[JsonPropertyName("foo")]` on a property
used in a `Where`/`OrderBy` can emit the wrong JSON path (and ignore the attribute), silently returning empty
results — while the write side (which uses the baked name) writes `"foo"`.

**Root cause.** `MetadataEmitter.EmitPropHelper` sets `AttributeProvider = declaring.GetProperty(clr)` — a
reflection lookup (`GeneratedMetadata.cs:~292`, under an `IL2070` suppression). If property reflection metadata
is trimmed, `JsonPropertyNameResolver.cs:13-19` falls through to the naming-policy fallback, which returns
`typeof(object)` and does not honor `[JsonPropertyName]`.

**Proposed fix.** Have the generator emit the effective JSON name (and CLR type) into the metadata so the
resolvers don't depend on the runtime `GetProperty` reflection; or root the property metadata. Mirror how STJ's
own source generator preserves this.

**Affected.** `Shiny.DocumentDb.Generators/GeneratedMetadata.cs`, `Internal/JsonPropertyNameResolver.cs`.

**Test plan.** A published-AOT smoke test (or a trimming test harness) with a `[JsonPropertyName]` property in a
query. Hard to exercise in a normal `dotnet test` run — needs a trim/AOT publish.

**Effort/risk.** Medium; AOT-only, hard to unit-test without a publish pipeline.

---

## 4. 🟠 IndexedDB read-modify-write is not atomic (version CAS / insert-dup can be defeated)

**Symptom.** On Blazor WASM, two overlapping `Insert`/`Update`/`Upsert` on the same key can both pass their
check and both write (lost update / duplicate), because each does a JS `Get` in one transaction and a `Put` in
another with `await` points between. The single-threaded WASM scheduler narrows but doesn't close the window.

**Root cause.** `IndexedDbDocumentStore.cs` `Insert` (~202), `Update` (~335), `Upsert` (~403) do get-check-put
across two separate JS transactions.

**Proposed fix.** Add a JS function that performs get-check-put inside a single `readwrite` transaction (return
a discriminated result: inserted / updated / version-conflict / already-exists), and route the C# store's
insert/update/upsert through it so the CAS and uniqueness checks hold within one transaction.

**Affected.** `wwwroot/shiny-indexeddb.js`, `IndexedDbDocumentStore.cs`.

**Test plan.** Blazor WASM / a JS-DOM harness. Not exercisable in the current `dotnet test` setup.

**Effort/risk.** Medium; JS + interop; needs a browser/WASM test environment.

---

## 5. 🟠 DI-registered interceptors are captured once from the root provider (no scoped support)

**Symptom.** A **scoped** `IDocumentInterceptor` registration either throws under scope validation or becomes a
captive singleton; a **transient** one is instantiated once and reused forever. The "scope-aware
`CaptureActor` / scoped interceptor" story is unimplemented.

**Root cause.** `Interceptors.AttachServiceProvider` (`:172-191`) resolves `IEnumerable<IDocumentInterceptor>`
once from the root provider passed to the singleton store factory and caches it; `IDocumentStore` is a
singleton (`ServiceCollectionExtensions.cs:20`). There is no scope-carrier in the tree.

**Proposed fix (design first).** Decide the model: (a) resolve interceptors per-operation from an ambient
`IServiceScopeFactory` (so scoped/transient lifetimes are honored), carrying the scope on the write context; or
(b) explicitly document interceptors as singletons and validate against scoped registration with a clear error.
Option (a) is the "scoped interceptor" feature; (b) is the honest minimal fix.

**Affected.** `Internal/Interceptors.cs`, DI extension, write pipeline. Architectural.

**Test plan.** `InterceptorDiTests` — assert per-scope instancing (option a) or a clear throw (option b).

**Effort/risk.** Medium–large; architectural decision required before coding.

---

## 6. 🟡 DuckDB native bulk-copy breaks when a tenant column exists

**Symptom.** An `Insert`-mode `BulkImport` into a tenant-enabled DuckDB store throws a column-count mismatch,
where Postgres/SQL Server gracefully NULL the `TenantId`.

**Root cause.** `DuckDbDatabaseProvider.BulkCopyInsertAsync` uses `CreateAppender(tableName)` and appends exactly
5 values positionally (`:~432`); the appender requires a value for every table column, so the 6th (`TenantId`)
column breaks it. SQL Server maps by name to avoid exactly this.

**Proposed fix.** Detect the tenant column (or `options.TenantIdAccessor != null`) and either append the tenant
value / a NULL, or fall back to the multi-row insert path (skip bulk-copy) when a tenant column is present.

**Affected.** `Shiny.DocumentDb.DuckDb/DuckDbDatabaseProvider.cs`.

**Test plan.** DuckDB (in-memory, runnable) tenant-store `BulkImport` Insert.

**Effort/risk.** Small–medium. Note: tenant + bulk import is currently documented unsupported; the goal is
graceful parity with the other providers rather than a hard failure.

---

## 7. 🟡 DynamoDB Streams: `LATEST` on child shards can miss split records; cross-split ordering

**Symptom.** Records written to a newly-split/rolled child shard before the next `RefreshShards` are skipped;
per-key ordering across a split isn't guaranteed. Also a dead `if (!any) … else …` branch with identical
`Task.Delay` (`DynamoDbDocumentStore.ChangeFeed.cs:112-115`).

**Root cause.** `RefreshShardsAsync` (`:121-136`) always assigns `ShardIteratorType.LATEST` (`:132`) to new
shards and doesn't drain parent shards before children.

**Proposed fix.** Use `TRIM_HORIZON` for child shards (or start children from the parent's ending sequence
number), and read parent shards to completion before their children. Remove the dead branch.

**Affected.** `Shiny.DocumentDb.DynamoDb/DynamoDbDocumentStore.ChangeFeed.cs`.

**Test plan.** DynamoDB-local container with a table that reshards (hard to trigger deterministically) — or a
unit test over the shard-iteration logic factored out from the AWS SDK calls.

**Effort/risk.** Medium; container-only and reshard timing is awkward to test.

---

## 8. 🟡 Enum comparisons assume numeric JSON storage

**Symptom.** With a `JsonStringEnumConverter` configured (enums stored as strings), every enum `==`/`in`
comparison binds a number against text and silently matches nothing — on all relational providers, both LINQ
and string surfaces.

**Root cause.** `SqlPredicateEmitter.NormalizeValue` always boxes an `Enum` to its underlying numeric value
(`:257`); the string grammar coerces the same way. Neither consults the effective converter.

**Proposed fix.** Thread the resolved `JsonTypeInfo`/converter into value normalization: when the enum property
serializes as a string, bind the enum's JSON string name instead of its numeric value. Or document the
constraint (enums must be stored numerically) if threading the converter is too invasive.

**Affected.** `Internal/Query/SqlPredicateEmitter.cs`, `ExpressionLowerer.cs`, `FilterExpressionParser.cs`.

**Test plan.** A store configured with `JsonStringEnumConverter` + an enum `Where` — assert it matches.
SQLite/in-memory runnable.

**Effort/risk.** Medium; requires plumbing converter awareness into the emitter.

---

## 9. 🟡 Backup accounting / error-shape polish

- **`DocumentsWritten`/`DocumentsSkipped` wrong for MySQL `Replace`.** `ON DUPLICATE KEY UPDATE` affected-row
  count is driver-flag dependent (`UseAffectedRows=true` counts an update as 2 → negative `DocumentsSkipped`).
  Derive `written`/`skipped` from row intent for the merge/replace modes rather than the driver count.
  (`DocumentStore.Backup.cs:111`, `MySqlDatabaseProvider.cs`.) Small.
- **Native bulk-copy duplicate-key error not wrapped.** The multi-row path wraps a duplicate into a friendly
  `InvalidOperationException` (`DocumentStore.Backup.cs:~208`); the native bulk-copy branch surfaces the raw
  provider exception. Wrap it for a consistent error shape. Small.

---

## 10. 🟡 Temporal `Restore` of a removed document resets `CreatedAt` and the version counter

**Symptom.** Restoring a deleted document loses its original `CreatedAt` (re-stamped to now) and rewinds the body
`Version` to 1 (the history `Version` continues monotonically).

**Root cause.** `Restore` re-inserts via `Insert` when the live row is gone
(`DocumentStore.cs` Restore; `LiteDbDocumentStore.Temporal.cs`), and `Insert` stamps a fresh `CreatedAt` and
version 1.

**Proposed fix.** Arguably by-design (the doc comment doesn't promise `CreatedAt` preservation) — but if it
should preserve: restore the `CreatedAt` from the version being restored and set the body version to the
history version. Decide the contract first.

**Effort/risk.** Small–medium; needs a contract decision.

---

## 11. 🟡 Vector `Score` semantics differ between relational and document providers

**Symptom.** For Cosine/Euclidean, relational providers return a *distance* (lower = closer, ascending) while
MongoDB/CosmosDB return a *similarity* (higher = closer, descending) — the `Score` value's meaning and sort
direction flip by backend for the same metric. (DotProduct sign was already fixed on the relational side.)

**Root cause.** Atlas `vectorSearchScore` / Cosmos `VectorDistance` are normalized similarities; there's no
lossless conversion to the relational distance semantics.

**Proposed fix (design).** Either (a) document `Score` as provider-specific (current state — noted in release
notes), or (b) define a canonical `Score` and convert per provider where a well-defined transform exists. (b)
is lossy for the normalized Atlas score; prefer (a) unless a `NormalizedScore` companion is wanted.

**Effort/risk.** Design decision, not a clear bug. Untestable here (Atlas / Cosmos emulator).

---

## 12. 📐 Spatial known-limitations (documented approximations — fix only if the trade-off changes)

These are correct-by-approximation and already noted in the code/docs; listed for completeness, not as action
items:

- Antimeridian / pole handling in `GeoMath.BoundingBox` / `ExpandByMeters` beyond the envelope widen already
  added — the radius helpers still assume no ±180 wrap.
- Ray-casting boundary result for a point exactly on an edge/vertex is implementation-defined; the relate engine
  papers over it, but native pushdown vs the C# refine can disagree on shared boundaries.
- `WithinDistance` in a LINQ `Where` throws on SQL Server / DuckDB (no geodesic distance for arbitrary
  geometries) — use `store.GeoWithinDistance`.

---

### Suggested pickup order

1. **#1 Backup timestamp fidelity** (highest user value; well-scoped once the format-version story is decided).
2. **#2 JsonContext path resolvers** (real silent-failure bug; runnable tests).
3. **#8 enum-as-string** and **#6 DuckDB tenant bulk-copy** (bounded, runnable).
4. **#5 DI scoped interceptors** and **#4 IndexedDB atomicity** (need a design decision / a WASM test env).
5. **#3 AOT `[JsonPropertyName]`**, **#7 DynamoDB shards**, **#9/#10/#11 polish/design** as capacity allows.
