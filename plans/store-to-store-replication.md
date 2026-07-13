# Plan: Store-to-store replication (`IDocumentReplicator`)

**Status:** Designed, not started.
**Target version:** `10.1` (new feature → minor bump off the `10.0.x` line in `version.json`). Additive —
no breaking changes to existing contracts. Phased across `10.1` (copy) → `10.2` (incremental) → `10.3`
(live); see [Phasing](#phasing).

> Self-contained build spec. Read `CLAUDE.md` (repo root) for the four-artifact rule (code+tests, docs
> site, skill, readme) before considering any commit "done". Branch off `v10`.

---

## Goal

One-way **replication** between two `IDocumentStore` instances: copy documents from a *source* store to a
*target* store, for provider migration (e.g. Cosmos → local SQLite), prod→dev cloning, warm standbys, and
ongoing mirroring. Three tiers, tapering by provider capability:

1. **Full / filtered copy** — snapshot the source (optionally a subset of types, with a filter and a
   transform) into the target. Universal.
2. **Incremental** — copy only what changed since a persisted cursor, using the intrinsic `UpdatedAt`
   column present on every provider. Universal for inserts/updates; delete handling is conditional.
3. **Live** — ride the source's native change feed and mirror continuously. Provider subset.

## Non-goals — and the relationship to AppDataSync

This is **not** `Shiny.DocumentDb.AppDataSync` (device↔server, bidirectional, offline-first, over HTTP,
with conflict resolution). To keep the two mental models apart the feature is named **replication**, never
"sync". Explicit non-goals:

- **No bidirectional sync.** Strictly source→target. No conflict resolution, vector clocks, or LWW merge.
  "Sync both ways" is two one-way replications the caller composes; we do not arbitrate conflicts.
- **No transport.** Both stores are live `IDocumentStore` handles in the same process. Replicating across a
  network boundary = point each store at its remote provider (Cosmos/Postgres/etc.); we don't ship a wire
  protocol. (For a file hop, `IDocumentBackup` export/restore already exists.)
- **No schema translation.** Bodies move as raw JSON. Reshaping is the caller's `Transform` hook, not an
  engine feature.

## Design decisions (locked)

| Decision | Choice | Consequence |
|---|---|---|
| Scope | Copy **+ incremental + live** | Full three-tier surface, phased. |
| Deletes | **Configurable per run** | `DeleteHandling.Ignore \| Mirror \| Tombstone` on `ReplicationOptions`. |
| Direction | **One-way only** | No conflict resolution; keeps the surface tractable. |

## Where it sits — reuse, don't reinvent

A store-to-store copy is **source read → (transform) → target write**, and both halves largely exist:

- **Target write side is done.** `IDocumentBackup.BulkImportAsync(IAsyncEnumerable<RawDocument>, BulkRestoreOptions)`
  already does chunked, type-homogeneous, resumable bulk writes with `BulkWriteMode` (Insert/Replace/Merge/
  SkipExisting) and provider capability gates (`SupportsBatchUpsert` for Merge, `SupportsBulkReplace` for
  Replace/SkipExisting). The replicator writes through this unchanged.
- **Source read side has a gap.** `IDocumentBackup.ExportAsync` reads only `Id, TypeName, Data`
  (`DocumentStore.Backup.cs:31`) — the wire format **drops `UpdatedAt`**. So the crude "pipe Export→Restore"
  works for a full copy but **cannot** drive incremental (no watermark) or transform-with-metadata. The one
  genuinely new store-side surface this feature adds is a raw reader that carries `UpdatedAt`.

So the deliverable is: **one new store-side capability** (`IDocumentReplicationSource`) + **one new
coordinator service** (`IDocumentReplicator`) that globs the existing read/write primitives together. No new
NuGet package — it lives in core under a `Replication/` folder, mirroring `Backup/`. It depends only on core
abstractions.

### Baseline that already works today (document it, don't gate on it)

```csharp
// Crude full clone — no new API. Loses UpdatedAt/version/temporal; whole-store only.
var pipe = new Pipe();
await Task.WhenAll(
    source.ExportAsync(pipe.Writer.AsStream()),
    target.RestoreAsync(pipe.Reader.AsStream()));
```

`IDocumentReplicator` is the productized version: per-type, filterable, transformable, incremental, live.

---

## Public API surface

### Coordinator — `Shiny.DocumentDb/Replication/IDocumentReplicator.cs`

```csharp
namespace Shiny.DocumentDb;

public interface IDocumentReplicator
{
    /// <summary>Full or filtered one-shot copy. Works for any source that is IDocumentReplicationSource
    /// (or falls back to IDocumentBackup export) into any target that is IDocumentBackup.</summary>
    Task<ReplicationResult> ReplicateAsync(
        IDocumentStore source, IDocumentStore target,
        ReplicationOptions? options = null, CancellationToken ct = default);

    /// <summary>Delta copy: reads only rows with UpdatedAt &gt; cursor, ordered ascending, and returns the
    /// next cursor (max UpdatedAt observed). Persist the returned cursor and hand it back next run.</summary>
    Task<ReplicationResult> ReplicateChangesAsync(
        IDocumentStore source, IDocumentStore target,
        ReplicationCursor cursor,
        ReplicationOptions? options = null, CancellationToken ct = default);

    /// <summary>Continuous mirror: subscribes to the source's native change feed (or in-process
    /// notifications) and applies each change to the target until the returned handle is disposed.
    /// Throws NotSupportedException if the source exposes neither feed.</summary>
    Task<IAsyncDisposable> ReplicateLiveAsync(
        IDocumentStore source, IDocumentStore target,
        ReplicationOptions? options = null, CancellationToken ct = default);
}
```

### Options / result — `Shiny.DocumentDb/Replication/ReplicationOptions.cs`

```csharp
public class ReplicationOptions
{
    /// <summary>Types (by resolved TypeName) to replicate. Null = every type the source enumerates.</summary>
    public IReadOnlyCollection<string>? DocTypes { get; set; }

    /// <summary>Collision behavior on the target. Reuses the backup enum. Default Replace (idempotent
    /// re-runs). Insert is fastest but throws on any pre-existing Id.</summary>
    public BulkWriteMode Mode { get; set; } = BulkWriteMode.Replace;

    /// <summary>Delete propagation strategy. Default Ignore (upsert-only). See <see cref="DeleteHandling"/>.</summary>
    public DeleteHandling Deletes { get; set; } = DeleteHandling.Ignore;

    /// <summary>ClearAll the target before writing (full-rebuild semantics; requires target is
    /// IDocumentMaintenance). Mutually reinforcing with Mode=Insert.</summary>
    public bool ClearTargetFirst { get; set; }

    /// <summary>Optional per-type WHERE fragment pushed to the source read (same surface as
    /// Query&lt;T&gt;(where)). Keyed by TypeName.</summary>
    public IReadOnlyDictionary<string, string>? Filters { get; set; }

    /// <summary>Per-record hook applied between read and write. Return a rewritten record (retype/reshape/
    /// redact/tenant-remap) or null to drop it. Body-level only — see caveats.</summary>
    public Func<ReplicationRecord, ReplicationRecord?>? Transform { get; set; }

    /// <summary>Rows per committed chunk on the target. Default 500 (matches BulkRestoreOptions).</summary>
    public int ChunkSize { get; set; } = 500;

    /// <summary>Reserved for per-type write concurrency. Default 1. Phase 1 honors only 1; &gt;1 is a no-op
    /// with a debug log until a later cut (SQLite is single-writer; relational needs pooled sessions).</summary>
    public int Parallelism { get; set; } = 1;

    public IProgress<ReplicationProgress>? Progress { get; set; }
}

public enum DeleteHandling
{
    /// <summary>Upsert-only. The target keeps rows the source has deleted. Cheapest, safest.</summary>
    Ignore,

    /// <summary>Exact mirror: after writing, enumerate source Ids vs target Ids per type and BatchRemove the
    /// difference. O(key-count) extra work; requires source + target ID enumeration. Full-copy path only.</summary>
    Mirror,

    /// <summary>Act on delete signals from the change source: temporal tombstones (incremental) or Removed
    /// change-feed events (live). A no-op where the source cannot surface deletes (logged).</summary>
    Tombstone
}

public sealed record ReplicationCursor(string Value)
{
    public static readonly ReplicationCursor Beginning = new("");
}

public readonly record struct ReplicationResult(
    long DocumentsRead,
    long DocumentsWritten,
    long DocumentsDeleted,
    long DocumentsSkipped,
    IReadOnlyDictionary<string, long> PerType,
    ReplicationCursor Cursor);

public readonly record struct ReplicationProgress(
    long DocumentsRead, long DocumentsWritten, long DocumentsDeleted, string? CurrentDocType);
```

### New store-side capability — `Shiny.DocumentDb/Replication/IDocumentReplicationSource.cs`

The one new store surface. Probe-for-capability, exactly like `IDocumentBackup` / `IDocumentMaintenance`.
Streams **raw** rows carrying the `UpdatedAt` watermark — no reflection, body verbatim.

```csharp
public interface IDocumentReplicationSource
{
    /// <summary>Streams raw document rows, forward-only, ordered by UpdatedAt ascending so a caller can
    /// checkpoint mid-stream. Body is bound as-is (never deserialized).</summary>
    IAsyncEnumerable<ReplicationRecord> ReadAsync(
        ReplicationReadOptions options, CancellationToken ct = default);

    /// <summary>Enumerates just the Ids (per type) currently present. Backs DeleteHandling.Mirror without
    /// materializing bodies.</summary>
    IAsyncEnumerable<(string DocType, string Id)> EnumerateIdsAsync(
        IReadOnlyCollection<string>? docTypes, CancellationToken ct = default);
}

public readonly record struct ReplicationRecord(
    string Id, string DocType, ReadOnlyMemory<byte> Data, DateTimeOffset UpdatedAt, bool IsDeleted = false);

public sealed class ReplicationReadOptions
{
    public IReadOnlyCollection<string>? DocTypes { get; set; }
    public DateTimeOffset? SinceUpdatedAt { get; set; }      // null = full read
    public IReadOnlyDictionary<string, string>? Filters { get; set; }

    /// <summary>Include soft-delete tombstones in the stream (IsDeleted=true). Honored only by temporal
    /// sources; silently ignored elsewhere.</summary>
    public bool IncludeDeletes { get; set; }
}
```

`ReplicationRecord → RawDocument` for the target write is a trivial projection (drop `UpdatedAt`/`IsDeleted`);
the write goes through the untouched `IDocumentBackup.BulkImportAsync`.

---

## How each tier executes

### Tier 1 — `ReplicateAsync` (full / filtered). Phase 1. All providers.

1. Resolve the source read: prefer `source is IDocumentReplicationSource` → `ReadAsync({DocTypes, Filters})`.
   Fallback for a source that only implements `IDocumentBackup`: adapt `ExportAsync`'s record stream
   (`UpdatedAt` unknown → `default`), which is fine because tier 1 ignores the watermark.
2. If `Transform` is set, map each `ReplicationRecord`; drop nulls.
3. Project to `RawDocument` and feed `target.BulkImportAsync(records, new BulkRestoreOptions {
   Mode = options.Mode, ClearExistingFirst = options.ClearTargetFirst, ChunkSize, Progress = adapter })`.
4. `DeleteHandling.Mirror`: after the write, for each type stream source Ids (`EnumerateIdsAsync`) into a
   `HashSet`, stream target Ids, and `BatchRemove` the target-only set. Requires both stores implement
   `IDocumentReplicationSource`; otherwise throw a clear `NotSupportedException` at the top of the call.
5. `DeleteHandling.Tombstone` on this tier degrades to `Ignore` with a one-line debug log (there is no change
   signal in a snapshot read).

### Tier 2 — `ReplicateChangesAsync` (incremental). Phase 2. All providers (inserts/updates).

The cursor is the max `UpdatedAt` (serialized ISO-8601 round-trippable) from the prior run;
`ReplicationCursor.Beginning` ("") means "from the epoch" = a full read.

1. Require `source is IDocumentReplicationSource` (throw otherwise, naming the provider).
2. `ReadAsync({ SinceUpdatedAt = parse(cursor), DocTypes, Filters, IncludeDeletes = Deletes==Tombstone })` —
   rows with `UpdatedAt > since`, ascending.
3. Transform + `BulkImportAsync` with `Mode` (default Replace — an updated row must overwrite).
4. Track `max(UpdatedAt)`; return it as `ReplicationResult.Cursor`.
5. **Deletes:** `Tombstone` works **only if the source is temporal** (`source is ITemporalDocumentStore` and
   surfaces soft-deletes as `IsDeleted` tombstones with an `UpdatedAt`). Where the source is not temporal, a
   deleted row simply stops appearing — invisible to a `> since` scan. Options honestly documented:
   - `Ignore` (default) — target accumulates orphans.
   - `Tombstone` — exact where source is temporal; **no-op + warning log** otherwise.
   - `Mirror` — supported but expensive: falls back to the tier-1 full key-diff each run (defeats the point
     of incremental for large stores; documented as a periodic-reconcile knob, not per-delta).

**Watermark caveats to bake into docs and tests:**
- **Clock skew / same-timestamp boundary.** Use `>` (strictly greater) and re-read the boundary tick is
  unsafe if many rows share a timestamp at millisecond resolution. Mitigation: cursor is
  `(UpdatedAt, Id)` compound — serialize both; the read is
  `WHERE UpdatedAt > @ts OR (UpdatedAt = @ts AND Id > @id)`. Prevents both dupes and skips at the boundary.
- **`UpdatedAt` is server-write time**, set by the store on write (`DateTimeOffset.UtcNow` at
  `BulkWriteChunkAsync`), not a logical version. Good enough for one-way replication; call out that it is not
  monotonic across a source clock change.

### Tier 3 — `ReplicateLiveAsync` (continuous). Phase 3. Provider subset.

1. Probe in order: `source is IChangeFeedDocumentStore` (native: Postgres LISTEN/NOTIFY, SQL Server change
   tracking, Cosmos change feed, DynamoDB Streams) → else `source is IObservableDocumentStore` (in-process,
   this-instance writes only) → else `NotSupportedException`.
2. The change feed is **per CLR type** (`SubscribeChanges<T>` / `NotifyOnChange<T>`). Live replication
   therefore needs the set of types to watch. Phase 3 takes them from a registered type list (the
   `DocumentContext`/`[Document]` registry) or an explicit `Type[]` overload of `ReplicateLiveAsync`. A
   late-bound (typeName-only) feed is a **follow-up** once a non-generic feed overload exists — noted as an
   open item, not built here.
3. Per change: `Inserted`/`Updated` (with `Document != null`) → `target.Upsert`/`Update`;
   `Removed` → `target.Remove` (honored under `Ignore`? No — a live Removed is a real signal; apply it unless
   `Deletes == Ignore`, in which case skip and count as skipped); `Cleared` → `target.Clear<T>()`.
   Property-level updates arrive with `Document == null` (see `DocumentChange<T>` doc) — for those, re-read
   the source doc by Id and upsert the full body.
4. Returns the `IAsyncDisposable` handle; disposing stops every per-type subscription.

**Live gap:** in-process `IObservableDocumentStore` only sees writes made through *that* store instance, so
live-mirroring a store other processes also write to requires the native `IChangeFeedDocumentStore`. Document
which providers offer which.

---

## Deletes — the asterisk, consolidated

| Tier | `Ignore` | `Mirror` | `Tombstone` |
|---|---|---|---|
| Full copy | upsert-only | ✅ key-diff (needs both `IDocumentReplicationSource`) | → degrades to Ignore (no signal) |
| Incremental | upsert-only | ⚠️ full key-diff each run (expensive) | ✅ iff source is temporal; else no-op+warn |
| Live | Removed events skipped | (n/a — use Tombstone) | ✅ from Removed/Cleared events |

The rule to communicate: **timestamp-incremental alone is insert/update-complete but delete-blind.** Exact
mirroring needs either a key-set diff (cost) or a delete-emitting change source (temporal / change feed).

## Cursor persistence

Callers own the cursor by default (return value in → out). Offer an optional convenience so recurring jobs
don't hand-roll it:

```csharp
public interface IReplicationCheckpointStore
{
    Task<ReplicationCursor> GetAsync(string replicationName, CancellationToken ct = default);
    Task SetAsync(string replicationName, ReplicationCursor cursor, CancellationToken ct = default);
}
```

Default implementation stores a `__replication_checkpoint` document (Id = `replicationName`) **in the target
store** via `IDocumentStore` — provider-agnostic, no new table. A `ReplicateChangesAsync` overload taking a
`string replicationName` reads/writes the checkpoint automatically.

## Provider capability matrix

| Capability the tier needs | Providers today |
|---|---|
| `IDocumentBackup` (target write) | all relational (SQLite/SQLCipher/MySQL/SqlServer/Postgres/Oracle/DuckDB) + Mongo + Cosmos |
| `IDocumentReplicationSource` (new; source read + Ids) | implement on the same set as `IDocumentBackup` |
| `IChangeFeedDocumentStore` (live) | Postgres, SQL Server, Cosmos, DynamoDB Streams |
| `ITemporalDocumentStore` (delete-aware incremental) | SQLite only today (per `project_temporal`) |

`Mode` gates inherited from `BulkImportAsync`: `Merge` requires `provider.SupportsBatchUpsert`;
`Replace`/`SkipExisting` require `provider.SupportsBulkReplace`. Surface the same `NotSupportedException`
messages — don't re-wrap.

## DI registration

The replicator is **stateless** → singleton, and takes both stores as method arguments (so it needs no keyed
resolution of its own — the caller resolves source/target by name and passes them):

```csharp
services.AddDocumentStore("prod",  o => o.DatabaseProvider = new CosmosDatabaseProvider(...));
services.AddDocumentStore("local", o => o.DatabaseProvider = new SqliteDatabaseProvider(...));
services.AddDocumentReplicator();                 // registers IDocumentReplicator (+ default checkpoint store)

// usage
var prod  = provider.GetRequiredKeyedService<IDocumentStore>("prod");
var local = provider.GetRequiredKeyedService<IDocumentStore>("local");
var result = await replicator.ReplicateAsync(source: prod, target: local,
    new ReplicationOptions { Mode = BulkWriteMode.Replace, Deletes = DeleteHandling.Mirror });
```

`AddDocumentReplicator` lives in `Shiny.DocumentDb.Extensions.DependencyInjection/ServiceCollectionExtensions.cs`
alongside the existing adds.

## Phasing

- **10.1 — Copy.** `IDocumentReplicator.ReplicateAsync` + `IDocumentReplicationSource` (read + Ids) on all
  relational providers + Mongo + Cosmos. `DeleteHandling.Ignore` and `Mirror`. Full four-artifact pass.
- **10.2 — Incremental.** `ReplicateChangesAsync`, compound `(UpdatedAt, Id)` cursor, `IReplicationCheckpointStore`
  + default target-backed impl + the `replicationName` overload. Temporal-source `Tombstone`.
- **10.3 — Live.** `ReplicateLiveAsync` over `IChangeFeedDocumentStore` / `IObservableDocumentStore`; typed +
  `Type[]` overloads. Late-bound feed noted as a follow-up.

Each phase is independently shippable and independently release-noted.

---

## Tests — `tests/Shiny.DocumentDb.Tests/ReplicationTests.cs` (+ provider matrix where feasible)

Phase 1:
1. **Full copy** SQLite→SQLite: N docs across 2 types, assert target counts + body equality.
2. **Filtered** by `DocTypes` and by `Filters` WHERE — only matching rows land.
3. **Transform** retypes / redacts a field; dropped (null) records don't land.
4. **Mode=Insert** into a non-empty target throws (duplicate Id); **Replace** is idempotent on re-run;
   **SkipExisting** leaves existing bodies untouched.
5. **ClearTargetFirst** wipes pre-existing target docs.
6. **Mirror deletes**: target has an extra doc absent from source → removed; result `DocumentsDeleted` == 1.
7. **Cross-provider** SQLite→Mongo (and Mongo→SQLite) smoke: body round-trips as raw JSON.
8. **Missing capability**: target not `IDocumentBackup`, or `Mirror` with a non-enumerable source → clear
   `NotSupportedException`.

Phase 2:
9. **Incremental** picks up only rows changed since the cursor; returned cursor advances; second run with the
   same cursor is a no-op.
10. **Boundary**: many rows sharing one `UpdatedAt` tick — compound cursor neither dupes nor skips.
11. **Temporal Tombstone** (SQLite source): a deleted source doc is removed from the target incrementally.
12. **Checkpoint store** round-trips a cursor in the target; `replicationName` overload resumes correctly.

Phase 3:
13. **Live in-process** (`IObservableDocumentStore`): writes to source appear in target; dispose stops it.
14. **Live Removed** deletes from target (unless `Ignore`); **Cleared** clears the type.

Run: `dotnet test tests/Shiny.DocumentDb.Tests/Shiny.DocumentDb.Tests.csproj --filter "FullyQualifiedName~ReplicationTests"`.

## Four artifacts (per phase)

- **Docs site** `~/Desktop/dev/documentation/.../documentdb/`: new `replication.mdx` (distinct from any
  AppDataSync page) — the three tiers, delete matrix, cursor persistence, provider matrix, the "not
  AppDataSync / one-way only" framing up top. Release note `<RN type="feature">` under `## 10.1 TBD` (create
  it) per the release-note rules.
- **Skill** `skills/shiny-documentdb/SKILL.md`: add `IDocumentReplicator` / `IDocumentReplicationSource` /
  `ReplicationOptions` / `DeleteHandling` to the `triggers:` list; a short "replicate one store to another"
  recipe; the delete/incremental caveats so generated code doesn't over-promise deletes.
- **readme.md** (repo root): add replication to the feature list.
- **Release notes** `release-notes.mdx`: one `<RN type="feature">` per phase.

## Edge cases / decisions to make during build

- **Same store as source and target.** Guard: `ReferenceEquals(source, target)` → `ArgumentException`.
- **Type-name collisions across providers.** TypeName resolution (ShortName vs FullName) can differ between
  source and target configs. Replicate on the **source's** resolved TypeName and write it verbatim; if the
  target resolves differently the row lands under the source name. Document; consider a `TypeNameMap` option
  later (the `Transform` hook already covers it manually).
- **Version/CAS + temporal do not ride along.** Bodies go through the raw `BulkImportAsync` lane, so the
  target's version int resets and no temporal history entry is written on the target for the replicated write
  (same limitation `IDocumentBackup` already documents). Fine for one-way; state it plainly.
- **`Parallelism > 1`.** Deferred. SQLite is single-writer; relational needs a pooled-session write fan-out.
  Default 1; `> 1` logs and proceeds serially until implemented.
- **Cancellation mid-run.** Per-chunk commits (default) make a cancelled copy resumable via incremental after
  the fact; `SingleTransaction` is not exposed on replication (a multi-GB copy in one txn is a foot-gun) —
  reconsider only if asked.
- **Live property-level updates** (`Document == null`) require a source re-read by Id — confirm that extra
  read is acceptable, or narrow live replication to full-document change paths in 10.3.
