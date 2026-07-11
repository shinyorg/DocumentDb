# Deferred / Known Bugs — Pickup Backlog

Findings from the multi-agent code reviews (v11). Each entry is written so a future contributor/agent can pick it
up cold: symptom, root cause with `file:line`, a proposed fix, the affected files/providers, a test plan, and an
effort/risk estimate.

**Status (11.0.0): the entire backlog below is now resolved.** #1–#11 were fixed or resolved as documented design
decisions (see each entry and the `11.0.0` release notes); #12 remains a set of by-design documented spatial
approximations (no action). Items testable here (SQLite/in-memory) ship with regression tests; the ones gated on
an environment we don't have — AOT/trim publish (#3), Blazor WASM (#4), DynamoDB-local reshard (#7) — are
implemented and compiling, with the residual verification noted in the entry.

Severity legend: 🟠 medium · 🟡 low · 📐 documented approximation (by-design; fix only if the trade-off changes) ·
✅ fixed/resolved in 11.0.0.

---

## 1. ✅ FIXED — Backup `Restore` did not preserve `CreatedAt` / `UpdatedAt` (envelope v2)

**Fixed in 11.0.0** (see release notes). Implemented the back-compatible v2 envelope:
- `RawDocument` / `RawBulkRow` gained optional `CreatedAt` / `UpdatedAt`; `BackupRecord` gained `createdAt` /
  `updatedAt` (written by `BackupStreams.WriteRecord` only when present, so v1 output is unchanged and v1 input
  still parses).
- Export selects `CreatedAt, UpdatedAt` and reads them cross-provider via `ReadTimestamp` (handles SQLite ISO
  text vs native timestamptz).
- Import (Insert mode) binds them per row through a new `IDatabaseProvider.BuildBackupInsertSql` (default +
  a `JsonInsertValueExpr` cast hook; PG/DuckDb override the hook, Oracle overrides the method for its
  no-semicolon dialect), falling back to `now` for v1 rows. Native bulk-copy (PG/SQL Server/DuckDb) is skipped
  when any row carries a timestamp so the per-row multi-row insert runs instead.
- Mongo (`BuildEnvelope` gained `updatedAt`; export/import thread the timestamps) and Cosmos (`NewEnvelope`
  takes createdAt/updatedAt; export selects `c.createdAt, c.updatedAt`) preserve them too.

Regression test: `BackupTimestampFidelityTests` (SQLite) — backdates the source `CreatedAt`, asserts it survives
the round trip, plus a v1-row (no timestamps) test asserting the `now` fallback. The relational multi-row path
is shared, so the container providers get the same behavior.

---

## 2. ✅ FIXED — Mapped-property resolvers use the naming policy, not `JsonTypeInfo`

**Fixed in 11.0.0** (see release notes). The version / spatial / vector / computed / full-text JSON-path
resolvers now route through `DocumentStoreOptions.ResolveJsonName`, which reads the effective JSON name off the
resolved `JsonTypeInfo` (via `JsonPropertyNameResolver.ResolveProperty`) and honors `[JsonPropertyName]` and
source-generated contexts, falling back to the naming policy only when the type has no metadata (catches both
the no-resolver `InvalidOperationException` and the type-absent `NotSupportedException` from `GetTypeInfo`).
Regression test: `JsonContextPathResolverTests` (a `[JsonPropertyName]` override on a `MapVersionProperty`
column — the CAS predicate previously read the policy-derived path, threw a phantom `ConcurrencyException` on
the first in-order update; now passes).

*Original report:* For a `DocumentSerialization.JsonContext`-mode type whose source-generated context bakes a
different JSON name than the runtime policy would (or a `[JsonPropertyName]` override) *and* which also has a
`MapVersion` / spatial / vector / computed mapping, the version CAS read (and document-provider spatial/computed
indexing) targeted a missing key and silently failed. Root cause was
`DocumentStoreOptions.ResolveVersionJsonPaths` (and the spatial/vector/computed/full-text siblings) computing
JSON paths from `jsonOptions.PropertyNamingPolicy?.ConvertName(...)` — a metadata-mode `JsonTypeInfo` bakes its
property names at its own generation time and ignores the runtime naming policy.

Note: on SQLite the spatial/vector write and refine paths resolve their value from the CLR accessor, not the
JSON path, so that half of the bug only surfaces on the JSON-path-indexed document providers (Cosmos/Mongo);
the shared fix covers all of them.

---

## 3. ✅ FIXED — `DocumentSerialization.Generated` + `[JsonPropertyName]` under trimming (AOT-only)

**Fixed in 11.0.0** (see release notes). `GeneratedMetadata.EmitType` now emits a
`[DynamicDependency(DynamicallyAccessedMemberTypes.PublicProperties, typeof(Owner))]` for each declaring type on
the `Create_X` metadata method. This roots the property metadata so the `AttributeProvider = declaring.GetProperty(clr)`
set in `Prop<>` survives trimming — the generated getter/setter lambdas preserve the accessor *methods*, but the
`PropertyInfo` metadata that `GetProperty` needs can be trimmed independently, which is what left
`JsonPropertyNameResolver` falling back to the naming policy and ignoring `[JsonPropertyName]`.

Verified the attribute is emitted in the generated source and the `Generated`-context tests still pass; the
trimming behavior itself requires a trim/AOT publish to exercise end-to-end (not runnable in `dotnet test`).

---

## 4. ✅ FIXED (Insert + Update) — IndexedDB read-modify-write atomicity

**Fixed in 11.0.0** (see release notes). Added two atomic JS primitives that do get-check-put inside a single
`readwrite` transaction and route the C# store through them:
- `insertIfAbsent(store, recordJson)` → `inserted` / `exists` — backs `Insert`, closing the duplicate-key race.
- `updateIfVersionMatches(store, recordJson, checkVersion, expectedVersion, versionPath)` →
  `updated` / `missing` / `conflict:<storedVersion>` — backs `Update`, closing the optimistic-concurrency race
  and preserving the existing `createdAt`. C# bumps the in-memory version before the call and restores it on a
  conflict.

`Upsert` is intentionally left on the C# path: its RFC 7396 deep-merge (`MergeJson(existing, patch)`) must read
the existing body to compute the merged result, so it can't be precomputed into a single JS transaction without
reimplementing the merge in JS. Documented as a residual (the single-threaded WASM scheduler keeps the window
tiny). The JS/interop compiles; end-to-end verification needs a Blazor WASM / JS-DOM harness not available here.

**Affected.** `wwwroot/shiny-indexeddb.js`, `IndexedDbJsInterop.cs`, `IndexedDbDocumentStore.cs`.

---

## 5. ✅ RESOLVED (option b) — DI-registered interceptors are singleton-scoped

**Resolved in 11.0.0** (see release notes). Chose the honest minimal fix (option b): interceptors are contractually
singletons (the store is a singleton and resolves them once from the root provider), and a **Scoped** registration
now fails fast. `ServiceCollectionExtensions.ThrowIfScopedInterceptors` scans the `IServiceCollection` for a `Scoped`
`IDocumentInterceptor` / `IDocumentBulkInterceptor` and throws a clear `InvalidOperationException` directing the
user to register as `Singleton` (recommended) or `Transient`. The scan runs **inside the store factory** (at first
resolve), so it is **order-independent** — it catches an interceptor registered after `AddDocumentStore` too — and
doesn't depend on the container's `ValidateScopes` setting (it inspects descriptors, not resolved services). A
Transient interceptor is still resolved once and reused — documented, not an error.

Option (a) — per-operation scope resolution via `IServiceScopeFactory` — was declined: it's a feature
(scope-aware interceptors), not a bug fix, and would change the interceptor execution model.

Regression tests: `InterceptorDiTests.ScopedInterceptor_RegisteredBefore_Throws` /
`ScopedInterceptor_RegisteredAfter_Throws` (order-independence) / `SingletonInterceptorRegistration_Allowed`.

---

## 6. ✅ FIXED — DuckDB native bulk-copy breaks when a tenant column exists

**Fixed in 11.0.0** (see release notes). Added a provider capability `IDatabaseProvider.SupportsBulkCopyWithTenant`
(default `true`; DuckDB overrides `false`). `DocumentStore.Backup.FlushAsync` now only takes the native bulk-copy
path when `tenantIdAccessor == null || provider.SupportsBulkCopyWithTenant`, so a tenant-enabled DuckDB store
falls back to the multi-row `INSERT` (which omits `TenantId` → NULL), matching the Postgres `COPY (… named
columns)` and SQL Server named-`ColumnMappings` paths instead of throwing. Regression test:
`DuckDbTenantBulkImportTests` (previously threw *"has 6 columns but you specified only 5 values"*).

*Original report:* An `Insert`-mode `BulkImport` into a tenant-enabled DuckDB store threw a column-count mismatch
because `DuckDbDatabaseProvider.BulkCopyInsertAsync` uses `CreateAppender` and appends exactly 5 values
positionally; the appender requires a value for every column, so the trailing `TenantId` column broke it. Tenant
+ bulk import remains documented-unsupported (imported rows are NULL-tenant); the goal was graceful parity, not a
hard failure.

---

## 7. ✅ FIXED — DynamoDB Streams: `LATEST` on child shards; cross-split ordering; dead branch

**Fixed in 11.0.0** (see release notes). `RefreshShardsAsync` gained an `initial` flag: the initial subscribe
starts every shard at `LATEST` (a live feed, no history replay), but shards discovered later (split/roll children)
start at `TRIM_HORIZON` so records written before we discovered the child aren't skipped. A child shard is not
begun until its parent's iterator has drained (`iterators[parentId] == null` / untracked), preserving per-key
ordering across a split. The dead `if (!any) … else …` branch with identical `Task.Delay` was collapsed to a
single delay.

**Affected.** `Shiny.DocumentDb.DynamoDb/DynamoDbDocumentStore.ChangeFeed.cs`. Compiles; the reshard behavior
needs a DynamoDB-local container to exercise end-to-end (not runnable here).

---

## 8. ✅ FIXED — Enum comparisons assume numeric JSON storage

**Fixed in 11.0.0** (see release notes). Added `Internal/EnumJsonStorage.cs`, which determines (and caches per
`JsonSerializerOptions`) whether an enum serializes as a JSON string under the effective converter, and yields the
exact member-name string the write path persists. `ExpressionLowerer` now:
- extracts a string-stored enum field as **text** (`FieldClrType` → `typeof(string)`, so the provider's
  `JsonExtractTyped` doesn't cast it to `BIGINT`), and
- binds the compared constant as the enum's member-name string. Because the C# compiler folds an enum literal to
  its underlying `int` in the expression tree, the conversion is done in `LowerComparison` by recovering the enum
  type from the **field** operand (`Enum.ToObject` → member name); `IN` lists and typed enum constants (the string
  grammar) are handled at the `ConstantNode` sites.

Covers the typed LINQ surface and the string grammar (both lower to the same IR), CosmosDB (shares the lowerer),
**and MongoDB** — `MongoExpressionVisitor` has its own translator that likewise boxed enums to int
(`Convert.ToInt32`); it now binds the member-name string for a string-stored enum field (equality and `IN`), via
the same shared `EnumJsonStorage` helper. Numeric-stored enums are untouched (`SerializesAsString` returns false →
original behavior). Regression tests: `EnumStringStorageTests` (SQLite: equality / inequality / `IN` /
string-grammar / captured-variable) and `MongoEnumStringStorageTests` (equality / `IN`).

**Affected.** `Internal/EnumJsonStorage.cs` (new), `Internal/Query/ExpressionLowerer.cs`,
`Shiny.DocumentDb.MongoDb/MongoExpressionVisitor.cs`.

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

## 10. ✅ RESOLVED (by-design, documented) — Temporal `Restore` of a removed document resets `CreatedAt`/version

**Resolution (11.0.0):** contract decided and made explicit rather than changing behavior. Restoring a document
whose live row had been **removed** re-creates it as a *fresh live lifecycle* — `CreatedAt` is stamped to the
restore time and a mapped version counter restarts at 1 — while the append-only history (original creation +
removal tombstone) is preserved and keeps its own monotonic version sequence. When the live row still exists the
restore is an overwrite and the version bumps forward normally. This is now stated in the `Restore` XML doc
(`DocumentStore.cs`) and the temporal docs page, and it holds uniformly across all five temporal providers
(relational + LiteDB/Mongo/Cosmos/IndexedDB), so there's no cross-provider inconsistency.

Preserving the original `CreatedAt`/version on resurrection was considered and rejected: "undelete = re-create"
is a defensible and common semantic, and it keeps every provider consistent without a 5-implementation change to
bind an explicit `CreatedAt` on insert (the relational half of which is the same machinery as #1).

---

## 11. ✅ RESOLVED (option a, documented) — Vector `Score` semantics differ by provider

**Resolved in 11.0.0** (see release notes). Chose option (a): `Score` is documented as provider-specific rather
than force-converted to a canonical scale (Atlas's normalized `vectorSearchScore` has no lossless inverse to a
distance, so any conversion would be misleading). The `VectorResult<T>` XML doc now spells out that relational
providers return a distance (lower = closer) for Cosine/Euclidean while MongoDB/CosmosDB return a normalized
similarity (higher = closer) — same metric, opposite direction — and that results are always ordered nearest-first
regardless of provider, so callers should rely on ordering, not the raw `Score`, for portable ranking.

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

### Status — all cleared in 11.0.0

Every item above is resolved. Summary:

- **Fixed with runnable regression tests:** #1 backup timestamp fidelity, #2 JsonContext path resolvers,
  #6 DuckDB tenant bulk-copy, #8 enum-as-string, #9 backup accounting/error-shape, #5 scoped-interceptor guard.
- **Fixed, compiling, verification gated on an unavailable environment:** #3 AOT `[JsonPropertyName]` (needs a
  trim/AOT publish), #4 IndexedDB Insert/Update atomicity (needs Blazor WASM; Upsert merge left on C# path),
  #7 DynamoDB shard iteration (needs DynamoDB-local reshard).
- **Resolved as documented design decisions:** #10 temporal restore contract (fresh lifecycle), #11 vector
  `Score` provider-specific semantics.
- **By-design, no action:** #12 spatial approximations.
