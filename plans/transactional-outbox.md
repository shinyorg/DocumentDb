# Plan: Transactional outbox — engine + admin surface

**Status:** Designed, not started.
**Target version:** **`13.0`** (additive; one new `SupportsTransactions` capability flag). `version.json` reads
`13.0.0-beta`, so the `12.9` this originally said is no longer a version anyone can ship to.
**Packages:** core (`Shiny.DocumentDb`, new `Outbox/` folder — mirrors `Backup/` and `Seeding/`) **and** the admin
tool (`ShinyDocDbMyAdmin.Core` service + models, `ShinyDocDbMyAdmin` Blazor page, `ShinyDocDbMyAdmin.Tui` screen).
No third-party dependency: dispatch is an interface the caller implements or a one-line delegate.

> Self-contained build spec. Read `CLAUDE.md` (repo root) for the four-artifact rule (code+tests, docs site,
> skill, readme) before considering any commit "done". Branch off `v12`.

## One feature, two halves, shipped together

The engine and the admin surface are **a single deliverable**, not a feature plus a follow-up. An outbox is the
first thing in this library whose failure mode is *operational* rather than *data* — "the order was saved but the
email never went out" — and answering that means asking a live database four questions (how deep is the queue, is
it draining, what is dead-lettered, and why) and then doing exactly one thing about it: requeue. Shipping the
engine alone puts those four questions behind the *application's* DI container (`IOutboxAdmin`), which is the
wrong place during an incident: it needs a code change, a deploy, or a hand-rolled admin endpoint to inspect rows
that are already sitting in a database ShinyDocDbMyAdmin can open today.

The admin half is also *cheap enough that deferring it costs more than building it*. Outbox messages are ordinary
documents, so the admin needs no new SQL, no provider code and no schema knowledge: `DocumentAdminService.Filter.cs`
already opens `store.Collection(typeName)` — the schema-free lane, keyed on the same `TypeName` column the tool
browses by — and `IJsonDocumentQuery` already has `Count`, `OrderBy`, `Paginate`, `ExecuteUpdate(assignments)` and
`ExecuteDelete`. Every admin operation below is one of those five calls.

And the two halves constrain each other in ways that are painful to retrofit: the admin reads these rows from
another process, which is what forces the wire names to be pinned (below) and makes the dedicated table the right
default. Decide those once, at birth, with both consumers in view.

---

## Goal

Let an application record "this happened" **in the same transaction as the write that made it happen**, and have
it delivered exactly-once-per-effect (at-least-once with idempotent consumers) to a bus, an HTTP endpoint, or
an in-process mediator — with no second datastore and no dual-write window — and give an operator somewhere to
*look* when it does not arrive.

```csharp
// Automatic: map a document type to an event
options.ConfigureDocument<Order>(cfg => cfg.PublishToOutbox(ctx => new OrderChanged(ctx.Id!, ctx.Operation)));

// Or explicit, inside your own unit of work
await using var session = store.OpenSession();
session.Add(order).Enqueue(new OrderPlaced(order.Id, order.Total));
await session.SaveChanges();      // order row + outbox row commit together, or neither does

// Watch it drain — one await foreach, for a dashboard, a health check, or a test
await foreach (var msg in store.WatchOutbox(o => o.States = OutboxStates.DeadLettered, ct))
    logger.LogError("Outbox dead letter {Id}: {Error}", msg.Id, msg.Error);
```

## Why this is nearly free here

The hard part of an outbox is "write the event in the same transaction as the aggregate", and the store already
guarantees it — `IDocumentInterceptor.AfterWrite` is documented as running *inside the same transaction as the
write, after it succeeds and before commit*, and `DocumentWriteContext.Session` is a unit-of-work session bound
to that transaction. The XML docs on `Interceptors.cs` literally name the outbox as the motivating case. What is
missing is the productized piece: a message document, a claim/retry/dead-letter processor, a dispatch seam — and
the operator's view of all four.

## Non-goals

- **Not a message bus.** No transport, no serialization format negotiation, no consumer side. `IOutboxDispatcher`
  hands you a message; publishing it to MassTransit / Azure Service Bus / Shiny.Mediator / an HTTP call is your
  implementation (we ship samples for each, not packages).
- **Not change data capture.** `IChangeFeedDocumentStore` already exposes engine-level change streams. The outbox
  carries **domain events the application chose to publish**, which is a different (and smaller) set.
  `WatchOutbox` streams *that* set and nothing else — it observes outbox rows, not table changes.
- **No inbox / dedup store.** Consumer-side idempotency is the consumer's problem; we deliver at-least-once and
  say so.
- **No ordering guarantee across partitions.** FIFO is offered per partition key only.
- **No distributed transaction with the bus.** That is the whole point of an outbox.
- **The admin cannot dispatch, and must say so.** Dispatch is `IOutboxDispatcher` — an application-side
  implementation over a transport the admin has no access to. The admin can only make a message *eligible*
  (`AvailableAt = now`, dead-letter state cleared); the application's running `OutboxProcessor` delivers it. A
  "Dispatch now" button would be a lie of the same shape as the tool pretending it can run interceptors. The
  screen states this in plain text, once, near the actions.
- **The admin does not enqueue or edit payloads.** A hand-written domain event is not an admin concern; a payload
  edit is an ordinary document edit through Browse. And no outbox write is exposed to the AI assistant.

---

## Design decisions (locked)

### Engine

| Decision | Choice | Consequence |
|---|---|---|
| Storage | An ordinary document type (`OutboxMessage`), in **its own table** (`outbox` by default) via the existing `cfg.Table` mapping | No system table, no provider code, no schema — same choice the seeder ledger makes — but without mixing infrastructure rows into a business table. One line at registration; `OutboxOptions.TableName` overrides it. Kills the "`ClearAll` / backup / replication carry outbox rows" risk, and keeps the processor's hot poll off the business table. A transaction spans tables on one connection, so atomicity is unaffected. |
| Wire names | `[JsonPropertyName]` on every `OutboxMessage` property | The default naming policy is camelCase and `JsonSerializerOptions` is app-settable, so without pinning the stored keys vary per app. This type is explicitly meant to be read by other processes ("query it, back it up, replicate it") — the admin surface below is the first such reader — so its wire shape must be fixed at birth. The attribute beats any policy; a guard test asserts it under a hostile policy. |
| Enqueue seam | `IDocumentSession.Enqueue<TEvent>(evt)` + an interceptor-driven auto-map | Both the explicit and the "every write of `T` publishes" style, one implementation. |
| Transactionality | Requires a store whose `BeginTransaction` is real | Gated at registration by a new `IDocumentStore.SupportsTransactions`; a non-transactional provider throws at `AddDocumentOutbox` with a message naming the provider, rather than silently degrading to dual-write. |
| Claiming | Per-message optimistic-concurrency CAS, not a table lock | Portable across every transactional provider, uses the shipped row-version/ETag path, and multiple workers scale without a leader. |
| Delivery | At-least-once | Stated everywhere in the docs. A dispatcher that succeeds then crashes before the ack re-delivers. |
| Retry | Attempt counter + exponential backoff via `AvailableAt` | No timer state in memory; a restarted process resumes from the store. |
| Failure terminal | Dead-letter in place (`DeadLetteredAt` + `Error`), never delete | Operators can inspect and requeue — which is what the admin half is for. |
| Watching | `store.WatchOutbox(…)` — poll-with-notification-nudge, **read-only** | One `await foreach` for dashboards, health checks and tests. Polling is the correctness floor (see below); change notification only shortens the latency. Never claims, so it cannot steal work from the processor. |
| Trace continuity | `traceparent` captured at enqueue, restored at dispatch | The consumer's span links to the request that caused it — the store already emits spans, so this closes the loop. |

### Admin surface

| Decision | Choice | Consequence |
|---|---|---|
| Placement | A **database-level screen** (`/db/{profileId}/outbox`), not a type-workspace tab | The ops question is "what is stuck in this database", not "what facets does this type have". A tab would also be wrong-shaped: every other tab is a per-type facet, the outbox is one specific type. |
| Discoverability | Tab link on `DatabaseOverview` + a badge on the outbox row in `TableOverview`'s type list, both shown only when outbox rows exist | Same probe-before-showing rule as History / Vectors / Blobs: no dead ends. |
| Which table | Discovered by scanning every browsable table's type list for the outbox type, never by assuming a table name | Works whether the outbox sits in the dedicated `outbox` table, a shared table, or wherever the app mapped it — so a later change to the storage default cannot break this surface. |
| Data access | The JSON-collection lane (`OpenStore` + `OpenCollection`), never hand-rolled SQL | Reads and writes lower through the same pipeline the application uses; zero provider code; relational-only, exactly like the filter console. |
| Field addressing | The pinned names above, mirrored by an `OutboxFields` constants class, verified by the guard test | **`nameof` would be wrong** — `nameof(OutboxMessage.ProcessedAt)` is "ProcessedAt" and the stored key is "processedAt". |
| Writes | Requeue and purge only, both set-based, both behind `AssertWritable()` + a confirm modal | Read-only profiles get a read-only screen. Set-based means no interceptors and no temporal version — correct here, and worth a code comment saying why. |
| AI surface | One new read-only tool, `outbox_status` | The tool surface is read-only today (`browse_documents`, `get_document`, …); requeue stays a deliberate human click. |
| Front ends | Core service once, Blazor page + TUI screen over it | The `.Core` extraction exists precisely so a feature is not built twice. |

### Questions resolved during design

1. **Storage placement.** Dedicated `outbox` table by default, via the mapping that already exists
   (`options.ConfigureDocument<OutboxMessage>(cfg => cfg.Table = …)`) — one line at registration, no new
   mechanism. Independently, the admin
   discovers the outbox by *type* rather than by table name, so neither half is coupled to the other's choice.
2. **Temporal history: do nothing.** `DocumentQueryBase.ExecuteUpdateCore` / `ExecuteDeleteCore` run the bulk
   interceptor hooks and the SQL — they do **not** call into temporal history, so the library's own set-based
   writes leave no version either. `DocumentAdminService.RecordVersion` exists to close a different gap: the
   admin's *single-document* edits go straight to SQL where the library would have written a version. A set-based
   admin write therefore already behaves exactly like a set-based library write, and extending `RecordVersion` to
   it would make the admin *diverge* from the library rather than match it. Docs action only: generalise the
   existing "`Clear<T>` does not record per-document history" note in `temporal.mdx` to all set-based writes.
3. **Cosmos.** Gated out — see Risks.
4. **Field addressing.** `[JsonPropertyName]` + constants, not `nameof` — see the tables above.

---

## Public API surface (core)

```csharp
// src/Shiny.DocumentDb/Outbox/OutboxMessage.cs
/// <summary>A pending domain event. An ordinary document — query it, back it up, replicate it.</summary>
/// <remarks>Every property carries [JsonPropertyName] (camelCase, elided below for readability). The wire shape
/// is part of this type's contract because other processes read these rows — the admin tool among them; it must
/// not follow the app's PropertyNamingPolicy.</remarks>
public sealed class OutboxMessage
{
    public string Id { get; set; } = null!;             // Guid v7 → time-ordered ids for FIFO reads
    public required string MessageType { get; set; }    // assembly-qualified-ish logical name
    public required string Payload { get; set; }        // JSON
    public string? PartitionKey { get; set; }           // ordering scope; null = unordered
    public Dictionary<string, string>? Headers { get; set; }  // incl. traceparent
    public DateTimeOffset CreatedAt { get; set; }
    public DateTimeOffset AvailableAt { get; set; }     // backoff gate
    public int Attempts { get; set; }
    public DateTimeOffset? ProcessedAt { get; set; }
    public DateTimeOffset? DeadLetteredAt { get; set; }
    public string? Error { get; set; }
    public int Version { get; set; }                    // optimistic concurrency — the claim primitive
}

// src/Shiny.DocumentDb/Outbox/IOutboxDispatcher.cs
public interface IOutboxDispatcher
{
    /// <summary>Deliver one message. Throw to retry (with backoff); return normally to ack.</summary>
    Task Dispatch(OutboxMessage message, CancellationToken ct);
}

// src/Shiny.DocumentDb/Outbox/OutboxOptions.cs
public sealed class OutboxOptions
{
    /// <summary>The table/collection the messages live in. Applied via cfg.Table at registration.</summary>
    public string TableName { get; set; } = "outbox";
    public int BatchSize { get; set; } = 50;
    public int MaxParallelism { get; set; } = 4;
    public int MaxAttempts { get; set; } = 8;
    public TimeSpan PollInterval { get; set; } = TimeSpan.FromSeconds(5);
    public Func<int, TimeSpan> Backoff { get; set; } = attempt => TimeSpan.FromSeconds(Math.Pow(2, attempt));
    /// <summary>Delete acked messages older than this. Null keeps them forever (audit trail).</summary>
    public TimeSpan? Retention { get; set; } = TimeSpan.FromDays(7);
    /// <summary>Dispatch messages sharing a PartitionKey strictly in order, one at a time.</summary>
    public bool OrderedPartitions { get; set; }
}

// Registration
public static IServiceCollection AddDocumentOutbox<TDispatcher>(this IServiceCollection services, Action<OutboxOptions>? configure = null)
    where TDispatcher : class, IOutboxDispatcher;
public static IServiceCollection AddDocumentOutbox(this IServiceCollection services, Func<OutboxMessage, CancellationToken, Task> dispatch, Action<OutboxOptions>? configure = null);

// Enqueue — session extension, so it joins the caller's unit of work
public static IDocumentSession Enqueue<TEvent>(this IDocumentSession session, TEvent evt, string? partitionKey = null) where TEvent : class;

// Auto-publish — a DocumentTypeBuilder extension over the public interceptor surface (no member on any
// options class, and per-type configuration belongs in the type's own ConfigureDocument block — 13.0)
public static DocumentTypeBuilder<T> PublishToOutbox<T>(
    this DocumentTypeBuilder<T> cfg,
    Func<DocumentWriteContext, object?> eventFactory,
    OutboxOperations operations = OutboxOperations.All) where T : class;

// Operations — the in-process view. The admin tool answers the same questions from outside the app.
public interface IOutboxAdmin
{
    Task<int> PendingCount(CancellationToken ct = default);
    Task<IReadOnlyList<OutboxMessage>> DeadLetters(int take = 100, CancellationToken ct = default);
    Task<int> Requeue(IEnumerable<string> ids, CancellationToken ct = default);
    Task<int> PurgeProcessed(DateTimeOffset olderThan, CancellationToken ct = default);
}

// Watch — a read-only async stream of outbox revisions, for dashboards, health checks and tests.
// Observation only: it never claims, acks or mutates a message, so it cannot compete with the processor.
public static IAsyncEnumerable<OutboxMessage> WatchOutbox(
    this IDocumentStore store,
    Action<OutboxWatchOptions>? configure = null,
    CancellationToken cancellationToken = default);

/// <summary>Decoded variant — filters to one message type and deserializes the payload.</summary>
public static IAsyncEnumerable<OutboxEnvelope<TEvent>> WatchOutbox<TEvent>(
    this IDocumentStore store,
    Action<OutboxWatchOptions>? configure = null,
    CancellationToken cancellationToken = default) where TEvent : class;

public sealed record OutboxEnvelope<TEvent>(OutboxMessage Message, TEvent Event) where TEvent : class;

[Flags]
public enum OutboxStates { Pending = 1, DeadLettered = 2, Processed = 4, All = Pending | DeadLettered | Processed }

public sealed class OutboxWatchOptions
{
    /// <summary>Which states to yield. Pending is the interesting one; DeadLettered is the alerting one.</summary>
    public OutboxStates States { get; set; } = OutboxStates.Pending;
    /// <summary>Restrict to one ordering scope.</summary>
    public string? PartitionKey { get; set; }
    /// <summary>Floor on how often the table is read. Defaults to OutboxOptions.PollInterval.</summary>
    public TimeSpan? PollInterval { get; set; }
    /// <summary>Yield what is already queued before tailing. On by default — a dashboard that starts
    /// mid-backlog and shows nothing is a bug, not a feature.</summary>
    public bool IncludeExisting { get; set; } = true;
    public int BatchSize { get; set; } = 100;
}
```

`DocumentOperation` is an enum today, not `[Flags]` — the `operations` filter above needs either a `[Flags]`
attribute on it (additive, values stay) or a small `OutboxOperations` enum in the outbox namespace. **Decision:
add `[Flags]`-compatible values in a new `OutboxOperations` enum** rather than reshaping a core enum other
features switch on.

---

## Implementation — engine

### Enqueue (the transactional half)

`Enqueue` serializes the event, builds an `OutboxMessage` (Guid v7 id, `CreatedAt`/`AvailableAt` from the
ambient `TimeProvider`, `traceparent` from `Activity.Current`), and calls `session.Add(message)`. Because it
rides the caller's session, `SaveChanges()` commits aggregate and message in one transaction. Inside an
interceptor's `AfterWrite`, `ctx.Session.Enqueue(evt)` joins the *triggering write's* transaction — wrap in
`SuppressInterceptors()` so the outbox insert does not recurse into `PublishToOutbox`.

### `OutboxProcessor` (the delivery half) — `BackgroundService`

Loop, per poll interval:

1. **Select candidates** — `store.Query<OutboxMessage>().Where(m => m.ProcessedAt == null && m.DeadLetteredAt == null && m.AvailableAt <= now).OrderBy(m => m.Id).Paginate(0, BatchSize).ToList()`.
   (Guid v7 ids sort by creation time, so `OrderBy(Id)` is FIFO without a second index.)
2. **Claim each** — bump `Attempts` and push `AvailableAt` forward by the visibility timeout, then `Update` with
   the row version. `ConcurrencyException` ⇒ another worker took it ⇒ skip. This is the entire concurrency
   design: no leases table, no leader election, works on every provider with optimistic concurrency.
3. **Dispatch** — resolve `IOutboxDispatcher` **from a fresh DI scope per message** (so scoped consumers work),
   restore the trace context, and call it inside an `outbox.dispatch` span.
4. **Ack or fail** — success ⇒ set `ProcessedAt`; failure ⇒ record `Error`, and dead-letter when
   `Attempts >= MaxAttempts`.
5. **Retention sweep** — periodically `Query<OutboxMessage>().Where(processed && old).ExecuteDelete()`.

`OrderedPartitions` groups the batch by `PartitionKey` and dispatches each group sequentially (groups still run
in parallel up to `MaxParallelism`); a failed message blocks only its own partition.

### `WatchOutbox` (the observation half)

An extension over the public query surface — no member on any options class, no provider code. It is a
**monitor, not a delivery mechanism**, and three things about the outbox make the naive implementation wrong:

1. **A change feed alone under-delivers.** A failed message goes to `AvailableAt = now + backoff` and becomes
   pending again with **no write at all** — it is a *clock* event, not a change event. A purely
   notification-driven stream would never yield it. So the loop is a **poll**, and
   `IObservableDocumentStore.NotifyOnChange<OutboxMessage>` (or `IChangeFeedDocumentStore`) only *nudges* the
   poll to run early instead of waiting out the interval. Polling is the floor; notification is latency.
2. **In-process notifications are not cross-process.** `IObservableDocumentStore` raises only for writes made
   through *this* store instance — and an outbox is normally enqueued by several instances. Treating the
   notification as the source of truth would silently miss every other writer's messages. Hence (1) again: the
   nudge is an optimization the correctness does not depend on, so a provider with neither capability degrades
   to plain polling with no behavior change.
3. **Dedupe on `(Id, Version)`, not `Id`.** A message legitimately reappears — a retry bumps `Attempts`, a
   requeue clears `DeadLetteredAt`. Each of those is a new revision the watcher *should* yield, and `Version`
   (the concurrency counter already in the contract) increments on every one. A watermark over `Id` would be
   wrong twice over: requeue moves a row backwards through the states, and Guid v7 ordering says nothing about
   revisions.

`IncludeExisting` needs no special case — it is just whether the first poll yields its results or only records
their `(Id, Version)` pairs as already-seen.

**What it deliberately does not promise:** every revision. A message enqueued, dispatched and acked between two
polls is never observed, and that is correct for a monitor. A caller who needs to see every message must be the
dispatcher — `AddDocumentOutbox(dispatch)` — not a watcher. The XML doc says exactly this, because the failure
mode of getting it wrong (a second, competing consumer) is a duplicated side effect in production.

### Capability gate

Add to `IDocumentStore`:

```csharp
/// <summary>True when this store's <see cref="IDocumentSession.BeginTransaction"/> is a real transaction.
/// Features that require atomic multi-document writes (the outbox) gate on it.</summary>
bool SupportsTransactions => false;
```

Relational + Mongo (replica set) + LiteDB report true; Cosmos reports **false** (see Risks); Redis / Azure Table /
DynamoDB / Firestore / IndexedDB report according to their actual transactional scope, and `AddDocumentOutbox`
throws at startup where the guarantee cannot be met.

### Telemetry

Reuse the embedded `OperationTracker`: span `outbox.dispatch` (tags: message type, attempt, partition), counter
`db.client.outbox.dispatched` / `.dead_lettered`, histogram of dispatch duration, and an observable gauge of
pending depth. Pending depth is the metric operators actually alert on — do not skip it.

---

## Implementation — admin surface

### `ShinyDocDbMyAdmin.Core/Models/OutboxModels.cs`

```csharp
/// <summary>Which bucket a message is in, derived from the three nullable timestamps.</summary>
public enum OutboxState { Pending, Scheduled, DeadLettered, Processed }

/// <summary>One outbox row, flattened out of the JSON body for the grid.</summary>
public sealed record OutboxEntry(
    string Id,
    string MessageType,
    string? PartitionKey,
    DateTimeOffset CreatedAt,
    DateTimeOffset AvailableAt,
    int Attempts,
    OutboxState State,
    string? Error,
    string? TraceParent,
    JsonNode? Payload);

/// <summary>The health strip. <paramref name="OldestPendingAt"/> is the signal that matters.</summary>
public sealed record OutboxHealth(
    long Pending,
    long Scheduled,
    long DeadLettered,
    long Processed,
    DateTimeOffset? OldestPendingAt);

public sealed record OutboxFilter(
    OutboxState? State = null,
    string? MessageType = null,
    string? PartitionKey = null,
    int Offset = 0,
    int Take = 100);

/// <summary>Dead letters collapsed by message type + first line of the error.</summary>
public sealed record OutboxFailureGroup(string MessageType, string ErrorSummary, int Count, string SampleId);

/// <summary>Where the outbox lives in this database.</summary>
public sealed record OutboxLocation(string Table, string TypeName);
```

`OldestPendingAt` earns its place: a healthy busy system has a large pending count, while a system whose processor
died has an *old* one. Depth alone cannot tell those apart; age can, and it is the thing to alert on.

### `ShinyDocDbMyAdmin.Core/Services/DocumentAdminService.Outbox.cs`

```csharp
public Task<IReadOnlyList<OutboxLocation>> FindOutboxes(string profileId, CancellationToken ct = default);
public Task<OutboxHealth> GetOutboxHealth(string profileId, string table, CancellationToken ct = default);
public Task<IReadOnlyList<OutboxEntry>> ListOutbox(string profileId, string table, OutboxFilter filter, CancellationToken ct = default);
public Task<IReadOnlyList<OutboxFailureGroup>> GroupOutboxFailures(string profileId, string table, CancellationToken ct = default);
public Task<int> RequeueOutbox(string profileId, string table, IEnumerable<string> ids, CancellationToken ct = default);
public Task<int> RequeueAllDeadLettered(string profileId, string table, string? messageType = null, CancellationToken ct = default);
public Task<int> PurgeProcessedOutbox(string profileId, string table, DateTimeOffset olderThan, CancellationToken ct = default);
```

**Locating the outbox.** Two unknowns, both answered by one cheap scan rather than an assumption:

- *Which table.* The default is the dedicated `outbox` table, but a user can override `OutboxOptions.TableName`
  and a store predating that default may have it in the shared table. `FindOutboxes` walks the browsable tables
  from the cached `ListTables` and calls the existing `ListTypes` on each (already a single `GROUP BY`, and the
  table list is cached per profile), returning every hit. One hit is the normal case and the screen opens on it;
  several means a table picker in the header; none hides every entry point.
- *Which type name.* The store writes `TypeName` through `TypeNameResolution`, so the row may say `OutboxMessage`
  *or* `Shiny.DocumentDb.Outbox.OutboxMessage`. Match on equality with `nameof(OutboxMessage)` or a `"."`-suffix,
  and carry the **stored** name forward for every later call. A dotted name cannot be addressed by the collection
  lane at all (collection names are validated to `^[A-Za-z_][A-Za-z0-9_]{0,127}$` because they are interpolated
  into DDL) — `OpenCollection` already throws an actionable message for that, which the screen surfaces as a note
  pointing at the SQL console rather than an error page.

**Field names are constants, not `nameof`.** The stored keys are the serialized ones, which is why the engine pins
them with `[JsonPropertyName]`. The admin mirrors that in one place:

```csharp
/// <summary>The pinned wire names of <c>OutboxMessage</c>. Mirrors the [JsonPropertyName] attributes in
/// Shiny.DocumentDb; the guard test in the core suite fails if the two ever drift.</summary>
static class OutboxFields
{
    public const string MessageType = "messageType";
    public const string PartitionKey = "partitionKey";
    public const string CreatedAt = "createdAt";
    public const string AvailableAt = "availableAt";
    public const string Attempts = "attempts";
    public const string ProcessedAt = "processedAt";
    public const string DeadLetteredAt = "deadLetteredAt";
    public const string Error = "error";
}
```

**State is derived, not stored.** The four buckets are three canned filters plus a residual:

| State | Filter |
|---|---|
| Pending | `processedAt is null and deadLetteredAt is null and availableAt:date <= {now}` |
| Scheduled (retrying / backed off) | `processedAt is null and deadLetteredAt is null and availableAt:date > {now}` |
| Dead-lettered | `deadLetteredAt is not null` |
| Processed | `processedAt is not null` |

Built with the interpolated `Where($"…")` handler so `now` is a parameter, not a formatted literal. The `:date`
hint is required — the collection lane is schema-free, so an unhinted path compares as text.

**Requeue** is one `ExecuteUpdate` with the multi-set assignment overload (v13):

```csharp
await collection.Query()
    .Where($"{OutboxFields.DeadLetteredAt} is not null and id in ({ids})")
    .ExecuteUpdate(new Dictionary<string, object?>
    {
        [OutboxFields.DeadLetteredAt] = null,
        [OutboxFields.Error] = null,
        [OutboxFields.Attempts] = 0,
        [OutboxFields.AvailableAt] = now
    }, ct);
```

The dead-letter guard stays in the predicate even when the caller passed explicit ids: requeueing a message that is
merely *scheduled* would reset its backoff, and requeueing a *processed* one would redeliver a business event that
already happened. Both are rejected by the filter rather than by the UI alone.

**Purge** is `Where($"{ProcessedAt} is not null and {ProcessedAt}:date < {olderThan}").ExecuteDelete()` — mirroring
the engine's own retention sweep, and structurally incapable of touching a pending or dead-lettered row.

Neither write records a temporal version, which is correct and needs no `RecordVersion` call: the library's own
`ExecuteUpdate` / `ExecuteDelete` do not either (resolved question #2). Say so in a code comment so the next
person does not "fix" it.

**Failure grouping** is client-side over the dead-letter page (`IJsonDocumentQuery` has no `GroupBy` — that lives
on the typed lane), capped at the same 500-row ceiling the filter console uses, with the cap stated in the UI when
it bites. `ErrorSummary` is the first line of `Error`, trimmed of any trailing id/guid so two instances of the same
failure collapse into one row.

### Blazor: `Components/Pages/Outbox.razor`

Route `/db/{ProfileId}/outbox`, linked from `DatabaseOverview`'s tab strip when `FindOutboxes` returns a hit (and
carrying a `?table=` when there is more than one). Uses the existing `.statgrid`, `.panel`, `.data`, `.tabs`
markup — no new CSS beyond a state badge, which reuses the `.badge` variants already carrying `role` / `ro` colours.

1. **Health strip** — Pending / Scheduled / Dead-lettered / Processed, plus **Oldest pending**, rendered as an age
   ("4m", "6h") with the absolute timestamp on hover. When the oldest pending age exceeds a small multiple of
   `PollInterval`, the tile goes warning-coloured with the title "nothing has drained recently — is the processor
   running?". That sentence is the whole point of the screen.
2. **State chips** — Pending | Scheduled | Dead-lettered | Processed | All, plus free-text message-type and
   partition-key boxes. Each chip is a canned filter; the grid re-queries.
3. **Grid** — Id (link to the row in Browse), Message type, Partition, Created, Available, Attempts, State badge,
   Error (first line, truncated, full text on hover). Clicking a row opens the existing `DocumentDialog` with the
   payload pretty-printed via `JsonView` and the headers listed beneath, `traceparent` shown with a copy button so
   it can be pasted into an APM. No fetching of anything external.
4. **Failures panel** — visible only when there are dead letters: message type × error summary × count, each row
   with "Requeue these". This is the view that finds the one poison consumer, which the flat grid cannot.
5. **Actions** (hidden entirely on read-only profiles) — *Requeue selected*, *Requeue all dead letters* (optionally
   narrowed to the current message type), *Purge processed older than [7 days ▾]*. Each opens a `Modal` confirm
   stating the exact count and, for purge, that the rows are gone for good. Above the buttons, one line of
   permanent text: **"This tool cannot deliver messages. Requeue makes them eligible again; your application's
   outbox processor does the delivery."**
6. **Ordering warning** — when any selected message has a non-null `PartitionKey`, the requeue confirm adds:
   requeueing re-inserts behind messages already delivered for that key, so ordering for that partition is already
   broken by the dead-letter and will not be restored. Say it; do not silently reorder.

Auto-refresh: the health strip re-polls on a timer only while the tab is visible, off by default with a "Live"
toggle — an admin tool must not hold a connection open on a schedule (the same reasoning that makes
`AdminConnection` open and close around every operation, and that disposes the filter console's store per run).

### Terminal: `ShinyDocDbMyAdmin.Tui`

`Screens/OutboxScreen.cs` pushed from `DatabaseOverviewScreen` and registered as a global command ("Outbox") so it
is reachable from the palette — in a terminal the palette is the address bar, so anything clickable must be
nameable. Layout: health line in the status bar area, a `RowGrid` of entries (`OutboxRow` with `[Bindable]` in a
namespace — see the XenoAtom gotchas), `Enter` opens `JsonDocumentDialog` on the payload, `r` requeues the
selection behind a `Modal` confirm, `p` purges, `1-5` switch state chips. Screen-level `Commands()` expose Requeue
/ Purge / state switches by name. Read-only profiles omit the write commands entirely rather than failing on press.

### AI assistant

One new function in `AiToolSurface`: `outbox_status(profileId, table)` → the health record plus the top five
failure groups, as JSON. It answers "is anything stuck?" and "what is failing?" in one call, which is the pair of
questions worth a tool. Requeue and purge stay off the tool surface: the assistant may diagnose, the human acts.

---

## Build order

Four steps, each leaving the tree green. The point of the ordering is that the wire contract is fixed before
anything depends on it, and the admin surface has real rows to develop against.

1. **Contract first** — `OutboxMessage` with pinned `[JsonPropertyName]`, `OutboxOptions` (incl. `TableName`),
   `OutboxOperations`, `IOutboxDispatcher`, `IOutboxAdmin`, the `SupportsTransactions` gate + per-provider values,
   and the guard test. Nothing dispatches yet.
2. **Engine** — `Enqueue`, `PublishToOutbox`, `OutboxProcessor`, `WatchOutbox`, telemetry,
   `AddDocumentOutbox` (which applies the `cfg.Table` mapping), plus the full engine test list below.
3. **Admin Core + Blazor** — `DocumentAdminService.Outbox.cs`, the models, `Outbox.razor`, the `DatabaseOverview`
   / `TableOverview` entry points, and the `DemoDatabaseBuilder` seed rows (in all four states — without them the
   page and its screenshots are permanently empty).
4. **TUI + assistant** — `OutboxScreen`, palette commands, `outbox_status`, parity tests.

---

## Tests

### Engine — `tests/Shiny.DocumentDb.Tests/OutboxTests.cs`

- **Atomicity:** a dispatcher-visible message never exists for a rolled-back aggregate write (throw inside
  `SaveChanges` after `Enqueue`; assert zero messages).
- **`PublishToOutbox`:** insert/update/delete of `T` produce one message each, with the right operation; a
  suppressed-interceptor write produces none.
- **Claim race:** two processors over 100 messages deliver each exactly once (counting dispatcher).
- **Retry:** a throwing dispatcher increments `Attempts` and pushes `AvailableAt` by the backoff (`FakeTimeProvider`);
  after `MaxAttempts` the message is dead-lettered with the error text and never redelivered.
- **Requeue** clears the dead-letter state and redelivers.
- **Ordering:** with `OrderedPartitions`, messages sharing a key arrive in enqueue order even under parallelism;
  a stuck partition does not block others.
- **Retention** purges acked messages and never purges pending or dead-lettered ones.
- **`WatchOutbox` backlog:** messages enqueued *before* the `await foreach` are yielded when `IncludeExisting`
  is on, and skipped (with the tail still live) when it is off.
- **`WatchOutbox` revisions:** one message that fails twice then dead-letters yields three revisions, never a
  duplicate — `(Id, Version)` dedupe, asserted with `FakeTimeProvider` driving the backoff.
- **`WatchOutbox` clock-only transition:** a message whose `AvailableAt` elapses with no intervening write is
  still yielded — the test that a change feed alone would fail.
- **`WatchOutbox` does not claim:** a watcher running alongside a real processor changes neither the delivery
  count nor `Attempts` (counting dispatcher, assert exactly-once).
- **`WatchOutbox` without `IObservableDocumentStore`:** a store with no notification capability still yields on
  the poll interval — the nudge is optional.
- **Cancellation** breaks the `await foreach` promptly and disposes the subscription.
- **Trace continuity:** the dispatch span's parent is the enqueueing activity.
- **Gate:** `AddDocumentOutbox` on a non-transactional provider throws at startup, naming the provider.
- **Table mapping:** the messages land in `outbox`, not the default documents table, and follow `TableName` when
  overridden.
- **Wire-name guard:** serialize an `OutboxMessage` **twice** — once with the store's default options and once with
  a hostile `JsonSerializerOptions { PropertyNamingPolicy = JsonNamingPolicy.SnakeCaseLower }` — and assert both
  produce the same pinned key set. This is the test that proves `[JsonPropertyName]` is doing its job: the admin
  addresses these fields by serialized name, so a dropped attribute, a rename, or an app-supplied naming policy
  would otherwise turn every admin filter into a silent zero-row result — the worst failure mode available here,
  because an empty outbox screen reads as "nothing is stuck".
- Provider matrix: full pass on SQLite + PostgreSQL + SQL Server + Mongo; gate-throw assertions elsewhere.

### Admin — `tests/ShinyDocDbMyAdmin.Tests/OutboxIntegrationTests.cs`

SQLite-backed, following `FilterConsoleIntegrationTests` (seed rows through a real store, then drive
`DocumentAdminService`):

- `FindOutboxes` finds short and dotted type names, finds the outbox in a *mapped* table as well as a shared one,
  returns every hit when two tables hold outboxes, and returns empty for a database with none.
- Health counts land in the right buckets across all four states, including a message whose `AvailableAt` is in the
  future counting as Scheduled and not Pending; `OldestPendingAt` ignores scheduled/dead/processed rows.
- Requeue clears `DeadLetteredAt` / `Error`, zeroes `Attempts`, sets `AvailableAt` to now; it is a **no-op** on a
  processed message and on a scheduled one, even when passed explicitly by id.
- Purge deletes only processed rows older than the cut-off; pending and dead-lettered rows survive a purge with a
  cut-off in the far future.
- Failure grouping collapses same-type/same-error rows and reports the cap when the ceiling is hit.
- Read-only profile: every write path throws before touching the database.
- **End-to-end, the pair that justifies building both halves together:** run the engine against a store until a
  message dead-letters, requeue it *through `DocumentAdminService`*, and assert the engine's processor then
  delivers it. This is the only test that proves the two halves agree on the row shape.

### TUI — `tests/ShinyDocDbMyAdmin.Tui.Tests`

A `ScreenRenderTests` case for `OutboxScreen` (renders with rows, renders empty, hides write commands on a
read-only profile) and a `ConfigurationParityTests` entry so the two front ends keep the same command set.

Full suite (Docker up) before this is called done, per `CLAUDE.md`.

---

## Four-artifact checklist

- **Code + tests** — as above, all four build steps.
- **Docs**
  - New `outbox.mdx`: the dual-write problem, the two enqueue styles, the delivery contract (at-least-once,
    idempotent consumers), operational runbook (pending depth, dead letters, requeue, retention), provider tier
    table, and a worked Shiny.Mediator + a MassTransit dispatcher sample. Cross-link from `interceptors.mdx`
    (which already advertises the outbox use case) and `change-monitoring.mdx` (outbox vs change feed — when to
    use which).
  - New `admin/outbox.mdx`: what the screen shows, what the four states mean, the requeue/purge runbook, and a
    prominent "this tool does not dispatch" note. Link it from `outbox.mdx`'s runbook section and from
    `admin/index.mdx`'s feature list. Screenshot via the scripted puppeteer capture (needs the seed rows from
    build step 3).
  - `temporal.mdx`: generalise the existing `Clear<T>` note to all set-based writes.
  - `backup.mdx` / replication docs: the outbox table is carried by backup/`ClearAll`, and is now separable.
  - Release notes: `type="feature"` under `13.0` for the outbox, one line covering the admin surface.
- **Skill** — outbox section with the transactional-enqueue rule (*always* enqueue through `ctx.Session` / the
  caller's session, never `store.Insert` in a separate call), an admin-outbox line in the tooling section, and
  `triggers:` += outbox / domain events / integration events / dead letter / requeue.
- **readme.md** — feature bullet for the outbox; the admin tool's bullet list gains "outbox queue + dead-letter
  triage".

---

## Risks

- **Cosmos partition scope — resolved: gate it out.** "Same transaction" on Cosmos means "same partition key", and
  the store partitions by type name, so an outbox message and an `Order` are always in different logical partitions
  ⇒ no atomicity. No table or container choice changes that, and a per-document partition strategy has already been
  considered and declined. Report `SupportsTransactions => false` for Cosmos, throw at `AddDocumentOutbox` naming
  the provider, and point Cosmos users at `IChangeFeedDocumentStore`. A "documented Cosmos mode" would ship an
  outbox whose single guarantee does not hold — worse than shipping none.
- **Poison message blocking a partition** in ordered mode. Dead-lettering unblocks it, but the ordering guarantee
  is then broken for that key by definition. Document the trade-off; do not silently skip.
- **Requeue is a business-event redelivery.** The confirm modal must not read like a UI nicety. The consumer is
  required to be idempotent (at-least-once, by design), but the person clicking is often the one who is about to
  find out whether it actually is.
- **The admin is relational only.** `DocumentAdminService` sits on `IDatabaseProvider`, so a Mongo or LiteDB
  outbox — both of which pass the transactional gate — gets no screen. That is a whole-tool limitation rather than
  an outbox one; say so plainly on the admin page rather than let someone conclude their outbox is unsupported.
- **Field coupling survives in weakened form.** The pinned names plus the guard test remove the naming-policy
  hazard, but the admin still hardcodes the *set* of fields. A property added to `OutboxMessage` later will not
  appear in the grid until someone adds it — a missing column, not a wrong answer, which is the right way for this
  to fail.
- **Backup / replication carry the outbox table.** Mostly desirable (an outbox backup is a real recovery aid) and
  now separable — a caller who wants business data without in-flight infrastructure messages excludes one table
  instead of filtering rows out of a shared one. Call it out in the backup and replication docs.
