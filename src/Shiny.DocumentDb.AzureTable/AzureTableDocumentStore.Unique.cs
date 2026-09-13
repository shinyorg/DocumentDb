using System.Text.Json.Serialization.Metadata;
using Azure;
using Azure.Data.Tables;

namespace Shiny.DocumentDb.AzureTable;

// Unique indexes (MapUniqueIndex). Azure Table has no secondary unique index, so every unique key a document holds is
// a reservation row in the document's own partition — RowKey "~uq-{hash}", UqOwner = the document id, no Data — and
// it is written in the same entity-group transaction as the document: claiming a key and writing the document commit
// or fail together. Another document already holding the key makes the reservation's Add fail with 409. Reservation
// rows are skipped by every scan and point read (IsReservation). Documents stored before the index was mapped hold no
// reservations and are not back-filled.
public partial class AzureTableDocumentStore
{
    const string ReservationPrefix = "~uq-";
    const string ReservationPrefixEnd = "~uq.";
    const string OwnerColumn = "UqOwner";

    // A write guarded by the stored row's ETag loses to a concurrent writer; the reservation diff it was planned from
    // is then stale, so it is re-planned from a fresh read this many times before giving up.
    const int MaxReservationAttempts = 5;

    readonly record struct PlannedAction(TableTransactionAction Action, UniqueIndexEntry? Claimed, string DocumentId);

    IReadOnlyList<UniqueIndexMapping> UniqueIndexesFor(Type type) => this.options.Mappings.ResolveUniqueIndexes(type);

    static bool IsReservation(TableEntity entity) => entity.TryGetValue(OwnerColumn, out var owner) && owner != null;

    static string ReservationRowKey(UniqueIndexEntry entry) => ReservationPrefix + entry.Hash;

    IReadOnlyList<UniqueIndexEntry> EntriesOf<T>(string typeName, string json, Func<object> document) where T : class
        => UniqueIndexKeys.Compute(this.UniqueIndexesFor(typeof(T)), typeName, json, document, this.jsonOptions);

    IReadOnlyList<UniqueIndexEntry> EntriesOf<T>(string typeName, TableEntity stored, JsonTypeInfo<T>? typeInfo) where T : class
    {
        var json = (string)stored["Data"];
        return this.EntriesOf<T>(typeName, json, () => Deserialize(json, typeInfo, this.jsonOptions)!);
    }

    async Task<TableEntity?> GetReservationAsync(TableClient table, string partitionKey, string rowKey, CancellationToken ct)
    {
        var response = await table.GetEntityIfExistsAsync<TableEntity>(partitionKey, rowKey, cancellationToken: ct).ConfigureAwait(false);
        return response.HasValue && IsReservation(response.Value!) ? response.Value : null;
    }

    // The reservation actions that move document `id` from the keys it held (`before`) to the keys it will hold
    // (`after`): an Add per newly claimed key, a Delete per released one.
    async Task<List<PlannedAction>> PlanReservationsAsync<T>(
        TableClient table,
        JsonTypeInfo<T>? typeInfo,
        string partitionKey,
        string typeName,
        string id,
        IReadOnlyList<UniqueIndexEntry> before,
        IReadOnlyList<UniqueIndexEntry> after,
        CancellationToken ct) where T : class
    {
        var (added, removed) = UniqueIndexKeys.Diff(before, after);
        var plan = new List<PlannedAction>(added.Count + removed.Count);

        foreach (var entry in added)
        {
            var rowKey = ReservationRowKey(entry);
            var held = await this.GetReservationAsync(table, partitionKey, rowKey, ct).ConfigureAwait(false);
            if (held == null)
            {
                var reservation = new TableEntity(partitionKey, rowKey) { [OwnerColumn] = id };
                plan.Add(new PlannedAction(new TableTransactionAction(TableTransactionActionType.Add, reservation), entry, id));
            }
            else if (held.GetString(OwnerColumn) == id || await this.IsStaleAsync(table, typeInfo, typeName, held, entry, ct).ConfigureAwait(false))
            {
                // Already ours, or orphaned by a delete that bypassed the store: take it over, guarded by its ETag so a
                // concurrent claim of the same key cannot also succeed.
                held[OwnerColumn] = id;
                plan.Add(new PlannedAction(new TableTransactionAction(TableTransactionActionType.UpdateReplace, held, held.ETag), entry, id));
            }
            else
            {
                throw new UniqueConstraintException(typeName, entry.Mapping, id);
            }
        }

        foreach (var entry in removed)
        {
            var held = await this.GetReservationAsync(table, partitionKey, ReservationRowKey(entry), ct).ConfigureAwait(false);
            if (held != null && held.GetString(OwnerColumn) == id)
                plan.Add(new PlannedAction(new TableTransactionAction(TableTransactionActionType.Delete, held, held.ETag), null, id));
        }
        return plan;
    }

    // A reservation is stale when its owner no longer exists or no longer holds the key. Every store write moves a
    // document and its reservations together, so this only happens after an out-of-band delete or edit.
    async Task<bool> IsStaleAsync<T>(TableClient table, JsonTypeInfo<T>? typeInfo, string typeName, TableEntity reservation, UniqueIndexEntry entry, CancellationToken ct) where T : class
    {
        var owner = await this.GetEntityAsync(table, reservation.PartitionKey, reservation.GetString(OwnerColumn), ct).ConfigureAwait(false);
        return owner == null || this.EntriesOf(typeName, owner, typeInfo).All(e => e.Key != entry.Key);
    }

    // Submits one entity-group transaction. A failed reservation claim means another document took the key first and
    // surfaces as UniqueConstraintException; any other failure — a document's own 404/409/412 — propagates as the
    // TableTransactionFailedException (a RequestFailedException), so callers' existing status handling still applies.
    static async Task SubmitAsync(TableClient table, string typeName, IReadOnlyList<PlannedAction> actions, CancellationToken ct)
    {
        try
        {
            await table.SubmitTransactionAsync(actions.Select(a => a.Action), ct).ConfigureAwait(false);
        }
        catch (TableTransactionFailedException ex) when (ex.FailedTransactionActionIndex is int index && index < actions.Count && actions[index].Claimed != null)
        {
            var failed = actions[index];
            throw new UniqueConstraintException(typeName, failed.Claimed!.Mapping, failed.DocumentId, ex);
        }
    }

    async Task AddDocumentAsync<T>(TableClient table, TableEntity entity, string typeName, JsonTypeInfo<T>? typeInfo, T document, CancellationToken ct) where T : class
    {
        if (this.UniqueIndexesFor(typeof(T)).Count == 0)
        {
            await table.AddEntityAsync(entity, ct).ConfigureAwait(false);
            return;
        }

        var after = this.EntriesOf<T>(typeName, (string)entity["Data"], () => document);
        var actions = await this.PlanReservationsAsync(table, typeInfo, entity.PartitionKey, typeName, entity.RowKey, [], after, ct).ConfigureAwait(false);
        actions.Insert(0, new PlannedAction(new TableTransactionAction(TableTransactionActionType.Add, entity), null, entity.RowKey));
        await SubmitAsync(table, typeName, actions, ct).ConfigureAwait(false);
    }

    // A key two documents of the same batch share is rejected up front — neither reservation exists yet for a claim to
    // collide on. Each document's Add and its reservation Adds are packed into the same transaction; atomicity is per
    // transaction (100 actions), the boundary the plain batch insert already has.
    async Task BatchInsertWithReservationsAsync<T>(TableClient table, string typeName, IReadOnlyList<T> documents, IReadOnlyList<TableEntity> entities, JsonTypeInfo<T>? typeInfo, CancellationToken ct) where T : class
    {
        var claimed = new HashSet<string>(StringComparer.Ordinal);
        var groups = new List<List<PlannedAction>>(entities.Count);
        for (var i = 0; i < entities.Count; i++)
        {
            var entity = entities[i];
            var document = documents[i];
            var after = this.EntriesOf<T>(typeName, (string)entity["Data"], () => document);
            foreach (var entry in after)
            {
                if (!claimed.Add(entry.Key))
                    throw new UniqueConstraintException(typeName, entry.Mapping, entity.RowKey);
            }

            var group = await this.PlanReservationsAsync(table, typeInfo, entity.PartitionKey, typeName, entity.RowKey, [], after, ct).ConfigureAwait(false);
            group.Insert(0, new PlannedAction(new TableTransactionAction(TableTransactionActionType.Add, entity), null, entity.RowKey));
            groups.Add(group);
        }

        var pending = new List<PlannedAction>();
        foreach (var group in groups)
        {
            if (pending.Count + group.Count > TransactionChunkSize)
            {
                await SubmitAsync(table, typeName, pending, ct).ConfigureAwait(false);
                pending = new List<PlannedAction>();
            }
            pending.AddRange(group);
        }
        if (pending.Count > 0)
            await SubmitAsync(table, typeName, pending, ct).ConfigureAwait(false);
    }

    // Replaces a stored document. Without a unique index it is the plain replace it always was. With one, the replace
    // and its reservation changes commit together guarded by the stored row's ETag; when the caller did not ask for
    // optimistic concurrency (ETag.All) a lost race re-plans from a fresh read, so the replace stays last-writer-wins.
    async Task ReplaceDocumentAsync<T>(TableClient table, TableEntity existing, TableEntity replacement, ETag etag, string typeName, JsonTypeInfo<T>? typeInfo, Func<object>? document, CancellationToken ct) where T : class
    {
        if (this.UniqueIndexesFor(typeof(T)).Count == 0)
        {
            await table.UpdateEntityAsync(replacement, etag, TableUpdateMode.Replace, ct).ConfigureAwait(false);
            return;
        }

        var json = (string)replacement["Data"];
        var after = this.EntriesOf<T>(typeName, json, document ?? (() => Deserialize(json, typeInfo, this.jsonOptions)!));
        var current = existing;
        var attempts = 0;
        var done = false;
        while (!done)
        {
            var before = this.EntriesOf(typeName, current, typeInfo);
            var actions = await this.PlanReservationsAsync(table, typeInfo, current.PartitionKey, typeName, current.RowKey, before, after, ct).ConfigureAwait(false);
            var guard = etag == ETag.All ? current.ETag : etag;
            actions.Insert(0, new PlannedAction(new TableTransactionAction(TableTransactionActionType.UpdateReplace, replacement, guard), null, current.RowKey));
            try
            {
                await SubmitAsync(table, typeName, actions, ct).ConfigureAwait(false);
                done = true;
            }
            catch (RequestFailedException ex) when (ex.Status == 412 && etag == ETag.All && ++attempts < MaxReservationAttempts)
            {
                current = await this.GetEntityAsync(table, current.PartitionKey, current.RowKey, ct).ConfigureAwait(false)
                    ?? throw new InvalidOperationException($"No document of type '{typeName}' with Id '{current.RowKey}' was found to update.");
            }
        }
    }

    // Deletes a stored document and releases its reservations in one transaction. False when it is already gone.
    async Task<bool> DeleteDocumentAsync<T>(TableClient table, TableEntity existing, string typeName, JsonTypeInfo<T>? typeInfo, CancellationToken ct) where T : class
    {
        TableEntity? current = existing;
        var attempts = 0;
        var deleted = false;
        while (current != null && !deleted)
        {
            var before = this.EntriesOf(typeName, current, typeInfo);
            var actions = await this.PlanReservationsAsync(table, typeInfo, current.PartitionKey, typeName, current.RowKey, before, [], ct).ConfigureAwait(false);
            actions.Insert(0, new PlannedAction(new TableTransactionAction(TableTransactionActionType.Delete, current, current.ETag), null, current.RowKey));
            try
            {
                await SubmitAsync(table, typeName, actions, ct).ConfigureAwait(false);
                deleted = true;
            }
            catch (RequestFailedException ex) when (ex.Status == 404)
            {
                current = null;
            }
            catch (RequestFailedException ex) when (ex.Status == 412 && ++attempts < MaxReservationAttempts)
            {
                current = await this.GetEntityAsync(table, current.PartitionKey, current.RowKey, ct).ConfigureAwait(false);
            }
        }
        return deleted;
    }

    // Bulk delete with each document's reservation deletes packed into the same transaction, so a document and its keys
    // always leave together. Atomicity is per transaction (100 actions) — the boundary the plain bulk delete already has.
    async Task<int> DeleteDocumentsAsync<T>(TableClient table, string partitionKey, string typeName, IReadOnlyList<TableEntity> documents, JsonTypeInfo<T>? typeInfo, CancellationToken ct) where T : class
    {
        if (this.UniqueIndexesFor(typeof(T)).Count == 0)
            return await this.DeleteRowKeysAsync(table, partitionKey, documents.Select(d => d.RowKey).ToList(), ct).ConfigureAwait(false);

        var reservations = new Dictionary<string, TableEntity>(StringComparer.Ordinal);
        var filter = TableClient.CreateQueryFilter($"PartitionKey eq {partitionKey} and RowKey ge {ReservationPrefix} and RowKey lt {ReservationPrefixEnd}");
        await foreach (var reservation in table.QueryAsync<TableEntity>(filter, cancellationToken: ct).ConfigureAwait(false))
        {
            if (IsReservation(reservation))
                reservations[reservation.RowKey] = reservation;
        }

        var deleted = 0;
        var pendingDocs = 0;
        var pending = new List<TableTransactionAction>();
        foreach (var document in documents)
        {
            var group = new List<TableTransactionAction>
            {
                new(TableTransactionActionType.Delete, new TableEntity(partitionKey, document.RowKey), ETag.All)
            };
            foreach (var entry in this.EntriesOf(typeName, document, typeInfo))
            {
                if (reservations.TryGetValue(ReservationRowKey(entry), out var held) && held.GetString(OwnerColumn) == document.RowKey)
                    group.Add(new TableTransactionAction(TableTransactionActionType.Delete, held, ETag.All));
            }

            if (pending.Count + group.Count > TransactionChunkSize)
            {
                await table.SubmitTransactionAsync(pending, ct).ConfigureAwait(false);
                deleted += pendingDocs;
                pending.Clear();
                pendingDocs = 0;
            }
            pending.AddRange(group);
            pendingDocs++;
        }

        if (pending.Count > 0)
        {
            await table.SubmitTransactionAsync(pending, ct).ConfigureAwait(false);
            deleted += pendingDocs;
        }
        return deleted;
    }

    // Rollback of a compensated insert only knows the partition and id, so it releases whatever that id owns there.
    async Task DeleteOwnedAsync(TableClient table, string partitionKey, string id, CancellationToken ct)
    {
        var actions = new List<TableTransactionAction>
        {
            new(TableTransactionActionType.Delete, new TableEntity(partitionKey, id), ETag.All)
        };
        var filter = TableClient.CreateQueryFilter(
            $"PartitionKey eq {partitionKey} and RowKey ge {ReservationPrefix} and RowKey lt {ReservationPrefixEnd} and UqOwner eq {id}");
        await foreach (var reservation in table.QueryAsync<TableEntity>(filter, cancellationToken: ct).ConfigureAwait(false))
            actions.Add(new TableTransactionAction(TableTransactionActionType.Delete, reservation, ETag.All));

        await table.SubmitTransactionAsync(actions, ct).ConfigureAwait(false);
    }
}
