# Plan: Orleans persistent stream provider (`AddDocumentDbStreams`)

**Status:** **BUILT — all phases** (August 10, 2026). Full suite green: 6182 core + 35 Orleans + 395 admin +
57 Aspire + 56 TUI + 13 generator + 15 MCP + 37 AspNetCore. (`WatchOutbox_YieldsEachRevisionOnce` fails, and
fails identically at unmodified HEAD in a clean worktree — a known pre-existing flake, not this work.)
**Target version:** `13.2` (new feature → minor bump off the `13.1.x` line in `version.json`). Additive to the
public contract; one **core** change landed with it (real `LockMode` SQL — see [Phase 0](#phase-0--make-lockmode-real-prerequisite)).

> **What changed during the build; this document has been updated to match the code.**
> 1. **CockroachDB shipped in Phase 1**, not Phase 3 — it inherits PostgreSQL's locking unchanged, so it
>    needed no work. It has no change feed, so it is poll-only, and its `SERIALIZABLE` transactions made an
>    enqueue retry on `40001` mandatory rather than optional.
> 2. **The backend gate is a capability, not a provider list.** `IDocumentStore.SupportsPessimisticLocking`
>    (new, derived from whether the provider emits locking SQL) is what the factory checks, so any backend
>    that grows real row locking qualifies without touching the stream provider.
> 3. **Rewind could not hook `QueueCacheMissException`.** The obvious design — let `SimpleQueueCache` raise a
>    miss and catch it — is wrong, and the tests caught it: an *empty* cache (the state after every restart,
>    i.e. exactly when a subscriber resumes) does not raise a miss, it returns a cursor that yields nothing.
>    A resume would then have silently replayed no history at all. Any non-null token now goes through storage
>    first and hands over to the live cache once storage is exhausted.
> 4. **Three document types cannot each set `cfg.Table`.** That property is a per-type table *override* and is
>    exclusive; sharing one table means setting `DocumentStoreOptions.TableName` and letting `TypeName`
>    discriminate, the way membership already did. Also caught by the tests, before any of them reached a
>    database.
**Package:** `Shiny.DocumentDb.Orleans` (the existing one — this is a fifth system store beside membership,
grain storage, reminders and the grain directory, not a new package).

> Self-contained build spec. Read `CLAUDE.md` (repo root) for the four-artifact rule (code+tests, docs site,
> skill, readme) before considering any commit "done". Branch off `v13`.

---

## Goal

Let an Orleans cluster run **durable streams on the database it already has a connection string for**:

```csharp
siloBuilder.AddDocumentDbStreams("Default", o =>
{
    o.DatabaseProvider = new PostgreSqlDatabaseProvider(connectionString);
    o.TotalQueueCount = 8;
});
```

The pitch is *single-store Orleans*. Membership, grain storage, reminders and the grain directory already sit
on `IDocumentStore`; streams are the last system store that forces a second piece of infrastructure into the
deployment. Today the choice is Orleans' in-memory provider (not durable) or Azure Event Hub / Azure Queue /
SQS (durable, but another service to run, pay for and secure). For a team already running PostgreSQL or SQL
Server, a table is the cheaper answer up to a real throughput ceiling.

Secondary benefit, and the one that ages best: the stream backlog is **queryable**. It is documents in the
store, so `ShinyDocDbMyAdmin` can show a stuck queue, a backlog depth per stream, and the actual event payload
that will not drain — an inspection story none of the cloud queue providers offer.

## Non-goals

- **Not an Event Hub replacement.** No competing on throughput. The target band is the outbox's: comfortable
  in the thousands of events/sec on PostgreSQL, not the hundreds of thousands. Say so in the docs, plainly,
  above the fold.
- **No exactly-once.** Orleans persistent streams are at-least-once by contract and this changes nothing.
  Consumers must be idempotent, same as with every other provider.
- **No transactional coupling to grain state.** Producing to a stream is not atomic with the producing grain's
  state write. (That is what the outbox is for, and it is why this feature's provider gate is *not* the
  outbox's `SupportsTransactions` gate — see below.)
- **Not a general message queue.** The public surface is Orleans' `IAsyncStream<T>`. No standalone
  producer/consumer API, no competing-consumer semantics outside Orleans' queue balancer.

## Provider gate — the big relational engines only

| Provider | Supported | Why |
|---|---|---|
| PostgreSQL | ✅ | Row locks, `LISTEN`/`NOTIFY` nudge, partitioned retention |
| SQL Server | ✅ | `UPDLOCK, HOLDLOCK`, query notifications |
| MySQL / MariaDB | ✅ | `FOR UPDATE`; poll-only (no nudge) |
| Oracle | ✅ | `FOR UPDATE`; poll-only |
| CockroachDB | ✅ | Inherits PostgreSQL's `FOR UPDATE` unchanged. No change feed → poll-only. `SERIALIZABLE` by default, so contended enqueues may surface retryable `40001` |
| SQLite / SQLCipher | ❌ | Local file → single-silo only, and a cluster's queues cannot live on one node's disk. Orleans' memory provider already covers single-silo; the only gap it would fill is "durable on one node", which is too thin to carry a supported tier |
| LiteDB | ❌ | As SQLite, plus a single-writer whole-file lock that the poll loop would sit inside. It is on the *outbox's* list only because `SupportsTransactions` is true (`LiteDbDocumentStore.cs:609`), and that gate does not apply here |
| DuckDB | ❌ | Analytical engine; small concurrent writes are its worst case |
| Cosmos / DynamoDB / Firestore / Azure Table | ❌ | Pay-per-request. A pulling agent polls each queue ~10×/sec; idle cost is charged forever whether or not anything is streaming |
| MongoDB / RavenDB / Redis / Amazon DocumentDb | ❌ (for now) | Plausible later — Mongo change streams would nudge well — but each needs its own sequencing answer and none is the headline scenario |

**The gate is enforced at registration**, not first use: the adapter factory throws while the silo is
starting, in the style of `OutboxProcessor.cs:31`. A silo that boots and then fails on the first event is
worse than one that will not boot.

**As built, the gate is a capability check rather than the list above**: it requires
`IDocumentStore.SupportsTransactions` and `IDocumentStore.SupportsPessimisticLocking`. The table is what that
resolves to today, not a hard-coded set — a backend that grows real row locking qualifies automatically, and
one that loses it stops qualifying, without anyone remembering to edit a list here.

## The two hard problems

Everything else here is assembly. These two decide whether the feature is correct and whether it is cheap.

### 1. Sequencing — commit order, not assignment order

Orleans wants a monotonic `long` per queue (`EventSequenceTokenV2`) and the receiver design below reads
`WHERE QueueId = @q AND Seq > @cursor ORDER BY Seq`. A native identity column or sequence (`BIGSERIAL`,
`IDENTITY`, `AUTO_INCREMENT`, Oracle `SEQUENCE`) looks like the obvious fit and **is a correctness trap**:
sequence values are handed out at *insert* time but rows become visible at *commit* time, and those orders
differ. Transaction A takes seq 5, transaction B takes 6, B commits first, the receiver reads up to 6 and
advances its cursor — then A commits and event 5 is **never delivered**. Silently. Under load only.

**Decision: a per-queue counter row, incremented under a real row lock inside the enqueue transaction.**

```
BEGIN
  counter = session.Get<StreamSequence>(queueId, LockMode.Update)   -- SELECT … FOR UPDATE
  seq     = counter.Next++
  insert the StreamEvent row with Seq = seq
  update counter
COMMIT
```

(As built, one Orleans batch is one row and therefore one sequence number — the batch container holds the
whole event list. Reserving a *range* only becomes necessary if a batch is ever split across rows.)

The lock is doing double duty: a second enqueuer blocks until the first *commits*, so assignment order and
commit order are the same order by construction. The sequence is also gap-free, which means the receiver's
cursor is exact and a gap is a real signal (a bug or a purge) rather than routine.

The cost is honest and worth stating in the docs: **enqueue throughput per queue is bounded by the lock hold
time on one row.** The scaling knob is queue count — `TotalQueueCount` (Orleans defaults to 8) is also the
number of counter rows, and streams hash across them. Doubling queues doubles enqueue concurrency. That is a
comprehensible dial, which is worth more than a cleverer scheme nobody can reason about at 3am.

Rejected alternatives, recorded so they are not re-litigated during build:

- *Native sequence + safety lag* (deliver only rows older than N ms) — probabilistic, not correct, and the
  failure mode is silent event loss.
- *Native sequence + visibility watermark* (PostgreSQL `pg_snapshot_xmin` / xid8, the Debezium approach) —
  correct and faster, but per-provider and deep. Worth revisiting for PostgreSQL specifically once the feature
  has users; not the first implementation.
- *GUID v7 ids as the token* — v7 sorts by time, but it is not a `long`, and cross-silo clock skew reorders
  it. Same silent-loss failure as the sequence trap.
- *CAS on the counter (no lock)* — contention becomes retry storms instead of queued waits, and it does not
  fix commit ordering.

### 2. Idle poll cost

Pulling agents call `GetQueueMessagesAsync` on `StreamPullingAgentOptions.GetQueueMsgsTimerPeriod` (default
100ms). Eight queues is ~80 queries/sec per silo *with nothing streaming*, multiplied by silo count. On the
gated providers each is a covered-index seek, but it is not free and it is permanent.

**Decision: adaptive backoff, nudged by the change feed where the provider has one.**

- Empty poll → back off (100ms → 250 → 500 → 1s, capped, configurable); a non-empty poll resets to the floor.
- Where the store implements `IChangeFeedDocumentStore` (PostgreSQL `LISTEN`/`NOTIFY`, SQL Server query
  notifications) an enqueue **wakes the receiver early**, so the backoff costs no real latency. This is the
  `OutboxNudge` pattern (`Outbox/OutboxWatchExtensions.cs:197`) — reuse the shape, do not re-derive it.
- `IObservableDocumentStore` (in-process) is deliberately **not** used as the nudge here. Unlike the outbox,
  the producer is usually on a different silo from the receiver, so an in-process signal would create a
  latency cliff that only shows up in multi-silo deployments — the worst kind of surprise.

The backoff ceiling is the tail-latency guarantee on MySQL/MariaDB/Oracle (no nudge available), so it belongs
in the options with a documented default, not buried as a constant.

## Phase 0 — make `LockMode` real (prerequisite)

`LockMode` (`src/Shiny.DocumentDb/LockMode.cs`) ships today as a **validated no-op**: `DocumentSession.cs:180`
throws if you use it outside a transaction and nothing else in the tree reads it — no provider emits `FOR
UPDATE`. Its own remarks call this out as "a per-provider enhancement". The sequencing design above depends on
it being real, so this lands first:

| Provider | Emit |
|---|---|
| PostgreSQL / CockroachDB | `FOR UPDATE` / `FOR SHARE` |
| MySQL / MariaDB | `FOR UPDATE` / `FOR SHARE` (`LOCK IN SHARE MODE` pre-8.0 — check the floor we support) |
| Oracle | `FOR UPDATE` (no `FOR SHARE` → throw for `LockMode.Share`) |
| SQL Server | `WITH (UPDLOCK, HOLDLOCK)` / `WITH (HOLDLOCK)` — a table hint, not a suffix, so it is a different insertion point in the SELECT builder |
| SQLite / DuckDB | Unchanged — the transaction boundary already locks the database; document that `Update` and `Share` are both satisfied by `BEGIN IMMEDIATE` |
| Non-relational | Unchanged — already throws for anything but `None` |

New hook on `IDatabaseProvider`, defaulted so no provider is forced to implement it:

```csharp
/// <summary>Row-locking clause appended to a SELECT issued with a LockMode, or the table hint
/// spliced after the FROM (SQL Server). Empty means the transaction boundary is the lock.</summary>
string BuildLockClause(LockMode mode) => string.Empty;
string BuildLockTableHint(LockMode mode) => string.Empty;
```

This is worth doing on its own merits — pessimistic locking is the missing half of the store-as-connection
work, and counter rows / leases / job claims all want it. Ship it as its own commit with its own release note
(`enhancement`), then build streams on top.

**Test it directly**, not only through streams: two sessions, one takes `LockMode.Update` on a row, assert the
second blocks until the first commits (and, on a provider matrix run, that the value it then reads is the
committed one). A lock that silently does nothing is exactly the bug that would make the stream sequencing
wrong in production and green in CI.

## Data model

Two document types, both internal to the package, both source-generated for AOT (follow
`OrleansSystemJsonContext.cs`). No custom DDL, no new table shape — same house style as
`MembershipDocument` / `GrainStateRecord`.

```csharp
sealed class StreamSequence            // one row per queue — the counter
{
    public string Id { get; set; }     // "{serviceId}/{queueId}"
    public int Version { get; set; }   // document CAS (unused on the locked path; kept for admin edits)
    public long Next { get; set; }     // next sequence number to hand out
}

sealed class StreamEvent               // one row per enqueued batch
{
    public string Id { get; set; }     // GUID v7 — storage identity only, never the sequence
    public string QueueId { get; set; }
    public long Seq { get; set; }
    public string? StreamNamespace { get; set; }     // denormalized for admin legibility only
    public string Payload { get; set; }              // Orleans-serialized batch container, base64
    public DateTimeOffset EnqueuedAt { get; set; }
    public DateTimeOffset? DeliveredAt { get; set; } // ack watermark → drives retention
}
```

A composite `(QueueId, Seq)` JSON expression index is created at adapter start so the receiver's range read
is a B-tree seek rather than a scan with a JSON extract per row. This is the single biggest performance
decision in the implementation.

**As built** there are *two* indexes — `(QueueId, Seq)` for the receiver's hot path and
`(QueueId, StreamId, Seq)` for rewind — created with `store.CreateIndexAsync(…)` rather than a
`ConfigureDocument` mapping: `MapComputedProperty` is for values *derived* from the document and not stored in
it, which is the wrong tool — `QueueId` and `Seq` are stored fields that need an index over their existing
JSON paths. `CreateIndexAsync` lives on `DocumentStore`, not `IDocumentStore`, so the `StoreFactory` escape
hatch logs a warning and leaves indexing to the caller. Worth confirming the emitted plan with the admin
`EXPLAIN` surface rather than trusting it.

The `RequestContext` is *not* a column — it rides inside the serialized batch container, which is where
Orleans expects it.

Cursors are **not** persisted per receiver in Phase 1: Orleans' queue balancer guarantees one pulling agent
per queue per cluster, so the receiver holds its cursor in memory and rebuilds it on failover from
`MAX(Seq) WHERE DeliveredAt != null`. Phase 2 persists a checkpoint row to shorten failover replay.

The retention sweep is safe against that rebuild: it only ever deletes *delivered* rows, and it deletes the
oldest first, so the highest delivered `Seq` still present is always the true watermark. If every delivered
row has aged out, the cursor rebuilds to 0 and the only rows left are undelivered ones — which is the correct
place to resume from anyway.

## Public API surface

```csharp
// Orleans.Hosting — matches the existing AddDocumentDbReminders / AddDocumentDbClustering shape
public static ISiloBuilder AddDocumentDbStreams(
    this ISiloBuilder builder,
    string name,
    Action<DocumentDbStreamOptions> configure);

public static IClientBuilder AddDocumentDbStreams(       // producer-side, client
    this IClientBuilder builder,
    string name,
    Action<DocumentDbStreamOptions> configure);

public class DocumentDbStreamOptions : OrleansStoreOptions   // inherits DatabaseProvider / StoreFactory / TableName / JSON knobs
{
    /// <summary>Number of queues streams hash across. Also the number of sequence counter rows, so it is
    /// the enqueue-concurrency dial. Default 8 (Orleans' own default).</summary>
    public int TotalQueueCount { get; set; } = 8;

    /// <summary>Floor of the adaptive poll backoff — the latency you pay on a provider with no change feed.</summary>
    public TimeSpan PollInterval { get; set; } = TimeSpan.FromMilliseconds(100);

    /// <summary>Ceiling of the adaptive poll backoff on an idle queue.</summary>
    public TimeSpan MaxPollInterval { get; set; } = TimeSpan.FromSeconds(1);

    /// <summary>Max event rows returned per poll.</summary>
    public int BatchSize { get; set; } = 100;

    /// <summary>How long delivered events are kept before the purge sweep removes them. Null keeps them
    /// forever (an audit trail, and a table that grows forever — say that in the XML doc).</summary>
    public TimeSpan? Retention { get; set; } = TimeSpan.FromHours(1);

    /// <summary>Use the store's native change feed to wake receivers early where available.</summary>
    public bool UseChangeFeedNudge { get; set; } = true;
}
```

Internal types, one per Orleans contract — none of them large:

| Type | Contract | Notes |
|---|---|---|
| `DocumentDbQueueAdapterFactory` | `IQueueAdapterFactory` | Builds the store via `OrleansDocumentStore.Build`, enforces the provider gate |
| `DocumentDbQueueAdapter` | `IQueueAdapter` | `QueueMessageBatchAsync` = the locked-counter enqueue above; `IsRewindable = false` in Phase 1 |
| `DocumentDbQueueReceiver` | `IQueueAdapterReceiver` | Cursor read + adaptive backoff + nudge |
| `DocumentDbBatchContainer` | `IBatchContainer` | Wraps `StreamEvent`; `SequenceToken` = `EventSequenceTokenV2(Seq)` |
| — | `IStreamQueueMapper` | `HashRingBasedStreamQueueMapper`, unchanged |
| — | `IQueueAdapterCache` | `SimpleQueueAdapterCache` in Phase 1 |
| — | `IStreamFailureHandler` | `NoOpStreamDeliveryFailureHandler` in Phase 1 |

## How delivery executes

**Enqueue** (`QueueMessageBatchAsync`) — one transaction: lock counter → reserve N → batch-insert events →
commit. `BatchUpsert`/batch insert already coalesces the row writes into one round-trip, so the cost is
roughly two round-trips plus the lock wait, per batch, not per event.

**Receive** (`GetQueueMessagesAsync`) — no claiming, no writes:

```sql
SELECT … WHERE QueueId = @q AND Seq > @cursor ORDER BY Seq LIMIT @batchSize
```

This is the design's whole efficiency argument. The outbox claims per message with an optimistic version bump
(`OutboxRunner.cs:161`) because it has competing consumers; Orleans guarantees a single agent per queue, so
the read path does zero writes. Advance the in-memory cursor, hand back batch containers.

**Ack** (`MessagesDeliveredAsync`) — stamp `DeliveredAt` on the delivered range with one set-based
`ExecuteUpdate` (a range predicate on `Seq`, not an id list). This is a watermark for retention and failover
replay, not a per-message state machine.

**Purge** — a timer per silo (leader-elected? no — idempotent `ExecuteDelete`, let them race) removing
`DeliveredAt < now - Retention`, mirroring `OutboxRunner.PurgeExpired`. On PostgreSQL, note in the docs that a
high-churn stream table wants autovacuum attention; time-range partitioning is a documented deployment
recipe, not an engine feature we ship.

**Failover** — the new agent reads `MAX(Seq) WHERE DeliveredAt != null` for its queue and resumes there.
Events delivered but not yet acked are redelivered. At-least-once, as promised.

## Phasing

| Phase | Version | Contents |
|---|---|---|
| **0** | `13.2` | ✅ **Done.** Real `LockMode` SQL (`BuildLockClause` / `BuildLockTableHint`), `SupportsPessimisticLocking`, dialect tests + a blocking conformance suite |
| **1** | `13.2` | ✅ **Done.** Streams on PostgreSQL / SQL Server / MySQL / MariaDB / Oracle / **CockroachDB**. `IsRewindable = false`, `SimpleQueueAdapterCache`, in-memory cursor. Aspire bridge via `DocumentDbOrleansFeatures.Streams` (opt-in, not in `All`) |
| **2** | `13.2` | ✅ **Done.** Durable rewind: `DocumentDbQueueCache` replays the events table for any non-null token, `IsRewindable = true`, second `(QueueId, StreamId, Seq)` index so replay reads one stream rather than the queue |
| **3** | `13.2` | ✅ **Done.** Persisted `StreamCheckpointDocument` per queue; `IStreamAdmin` + a **Streams** screen in both admin front ends (read-only by design); automatic retry of CockroachDB's retryable `40001` enqueue aborts |

### Still open (not built, and not blocking)

- **A CockroachDB contention soak at scale.** The retry path is exercised by the concurrency tests, but nobody
  has run sustained multi-silo load against a real Cockroach cluster. The counter row is the hottest row in
  the design and Cockroach is the one supported engine that aborts rather than waits.
- **A published throughput benchmark.** The docs say "thousands of events/sec on PostgreSQL" from the
  outbox's band, not from a measurement of this provider. Measure before that sentence hardens into a promise.
- **PostgreSQL visibility-watermark sequencing** (`pg_snapshot_xmin` / xid8, the Debezium approach) — would
  remove the per-queue lock serialization on one engine. Worth revisiting only once there are users whose
  enqueue rate is actually bounded by it.

Phase 1 must not ship claiming rewind. `IsRewindable` is a contract flag consumers branch on — shipping it
true with only a memory cache behind it produces "subscription resume works in dev, throws in prod".

## Tests — `tests/Shiny.DocumentDb.Orleans.Tests`

The existing fixtures (`Fixtures.cs`, `PostgresGrainStorageTests.cs`) already stand up Testcontainers silos;
follow that shape. **Docker is required for all of these** — per `CLAUDE.md`, a filtered subset is not a pass.

1. **Round-trip** — produce N events, consume, assert order and count per stream.
2. **Sequencing under concurrency** — the test that justifies the whole design: K producers × M batches
   concurrently onto one queue, then assert the delivered sequence is gap-free, strictly increasing, and that
   **no event is missing**. This must fail against a native-identity implementation; write it before the
   implementation and confirm it fails for the right reason.
3. **`LockMode` blocking** (Phase 0, provider matrix) — session A locks, session B blocks until commit.
4. **Failover** — kill the receiver mid-batch, assert redelivery from the ack watermark and no loss.
5. **Multi-silo** — two silos, producer on one, consumer on the other; asserts the nudge is not the
   correctness path.
6. **Idle cost** — poll count over a quiet interval stays within the backoff's bound. Guards the regression
   where someone "fixes" a latency complaint by pinning the interval to the floor.
7. **Purge/retention** — delivered events age out; undelivered ones never do.
8. **Provider gate** — `AddDocumentDbStreams` on SQLite throws at host start with a message naming the
   supported providers.
9. **Orleans' own stream conformance** — check whether `Microsoft.Orleans.TestingHost` exposes reusable
   stream provider conformance tests and run them if so; cheaper than inventing the edge cases.

## Four artifacts

1. **Code + tests** — above.
2. **Docs site** (`~/Desktop/dev/documentation/src/content/docs/documentdb/`) — new `orleans-streams.mdx` (or a
   section in `orleans.mdx`, decide when writing); the provider matrix table verbatim; the throughput
   disclaimer above the fold; the "why not SQLite/LiteDB" note, since users of those *will* ask. Release notes
   under a `## 13.2 TBD` heading: one `<RN type="feature">` for streams, one `<RN type="enhancement">` for
   real `LockMode`.
3. **Skill** (`skills/shiny-documentdb/SKILL.md`) — `AddDocumentDbStreams` in the Orleans section, the
   provider gate, and the `triggers:` keywords (`orleans streams`, `stream provider`, `IAsyncStream`).
4. **readme.md** — the Orleans bullet currently lists four system stores; make it five.

## Open questions to settle during build

- **Payload encoding.** Orleans' own serializer (opaque bytes, base64 in JSON) versus JSON. Bytes are faster
  and version-tolerant via Orleans' rules; JSON makes the admin inspection story real. Possibly a flag, but
  default to bytes and see whether the admin story survives without it.
- **Client-side producers.** `IClientBuilder` producers need the same locked-counter enqueue, which means an
  external client holding a DB connection. Reasonable for a trusted-network client, wrong for anything
  internet-facing. Decide whether the client-side overload ships in Phase 1 or waits.
- **`TotalQueueCount` changes.** Changing it re-hashes streams across queues, so in-flight events can land on
  a queue nobody reads. At minimum detect and warn at startup by comparing against the counter rows present;
  ideally refuse to start.
- **`ExecuteUpdate` on a `Seq` range** — confirm the multi-set form emits one statement with a range predicate
  rather than degrading to an id list at the batch sizes involved.
- **Retention vs rewind.** Phase 2's durable rewind is bounded by Phase 1's retention sweep. When rewind
  lands, `Retention` stops being a cleanup knob and becomes the rewind window — rename or re-document it then.
