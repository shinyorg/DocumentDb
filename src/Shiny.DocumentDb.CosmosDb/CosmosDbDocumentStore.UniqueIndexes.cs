using System.Net;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;
using Microsoft.Azure.Cosmos;
using Shiny.DocumentDb.Internal;

namespace Shiny.DocumentDb.CosmosDb;

// Unique indexes (MapUniqueIndex) on Cosmos DB. A native unique key policy can only be set when a container is
// created, cannot be filtered and is not scoped to a type, so the store keeps its own index: one reservation item per
// claimed key, in the document's own container AND logical partition (/typeName). Sharing the partition is what makes
// it correct — every reservation a write claims is created, or re-locked with If-Match, in the same TransactionalBatch
// as the document write, so two documents can never both commit one key. Releasing a key happens after the commit and
// is best-effort: a leftover reservation names an owner that no longer holds the key, which the next claimant verifies
// before taking it over. Documents stored before an index was mapped hold no reservation and are not back-filled.
public partial class CosmosDbDocumentStore
{
    /// <summary>
    /// The predicate every document query carries: reservation items share the type's partition but have no
    /// <c>data</c>, so this keeps queries, counts and aggregates blind to them.
    /// </summary>
    internal const string DocumentsOnly = "IS_DEFINED(c.data)";

    // Bounds both loops that react to a concurrent writer: re-reading a document that changed under an If-Match write,
    // and re-resolving a reservation that changed under a claim.
    const int MaxUniqueWriteAttempts = 10;

    IReadOnlyList<UniqueIndexMapping> UniqueIndexesFor<T>() => this.options.Mappings.ResolveUniqueIndexes(typeof(T));

    // For the paths that only know the stored type name (backup restore, unit-of-work rollback).
    IReadOnlyList<UniqueIndexMapping> UniqueIndexesFor(string typeName)
        => this.options.Mappings.HasUniqueIndexes
            ? this.options.Mappings.UniqueIndexes
                .Where(m => TypeNameResolver.Resolve(m.DocumentType, this.options.TypeNameResolution) == typeName)
                .ToList()
            : [];

    // The entries a stored body occupies; the document is only deserialized when an index filter needs evaluating.
    Func<string, IReadOnlyList<UniqueIndexEntry>> UniqueEntriesOf<T>(IReadOnlyList<UniqueIndexMapping> indexes, string typeName, JsonTypeInfo<T>? typeInfo) where T : class
        => json => UniqueIndexKeys.Compute(indexes, typeName, json, () => Deserialize(json, typeInfo, this.jsonOptions)!, this.jsonOptions);

    Func<string, IReadOnlyList<UniqueIndexEntry>> UniqueEntriesOf(IReadOnlyList<UniqueIndexMapping> indexes, string typeName)
        => json => UniqueIndexKeys.Compute(indexes, typeName, json,
            () => JsonSerializer.Deserialize(json, this.jsonOptions.GetTypeInfo(indexes[0].DocumentType))!, this.jsonOptions);

    /// <summary>
    /// Read → rewrite → replace for one stored document — the shared body of SetProperty, RemoveProperty and a query's
    /// ExecuteUpdate. Returns false when the document is missing or <paramref name="admit"/> rejects it. With unique
    /// indexes the replace is If-Match-guarded and claims its new keys in the same batch, re-reading when a concurrent
    /// write lands in between.
    /// </summary>
    internal Task<bool> RewriteDataAsync<T>(
        Container container,
        string typeName,
        string id,
        Func<string, bool>? admit,
        Func<string, string> rewrite,
        JsonTypeInfo<T>? typeInfo,
        CancellationToken ct) where T : class
    {
        var indexes = this.UniqueIndexesFor<T>();
        var pk = new PartitionKey(typeName);
        return this.RetryWhileDocumentChangesAsync(async () =>
        {
            ItemResponse<CosmosDocument> response;
            try
            {
                response = await container.ReadItemAsync<CosmosDocument>(id, pk, cancellationToken: ct).ConfigureAwait(false);
            }
            catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.NotFound)
            {
                return false;
            }

            var doc = response.Resource;
            if (admit != null && !admit(doc.Data))
                return false;

            var previous = doc.Data;
            doc.Data = rewrite(previous);
            doc.UpdatedAt = DateTimeOffset.UtcNow.ToString("o");

            if (indexes.Count > 0)
            {
                await this.ReplaceWithUniqueIndexesAsync(container, doc, previous, response.ETag, this.UniqueEntriesOf(indexes, typeName, typeInfo),
                    status => status is HttpStatusCode.PreconditionFailed or HttpStatusCode.NotFound
                        ? new DocumentChangedException()
                        : DocumentWriteFailed(typeName, id, status),
                    ct).ConfigureAwait(false);
                return true;
            }

            try
            {
                await container.ReplaceItemAsync(doc, id, pk, cancellationToken: ct).ConfigureAwait(false);
                return true;
            }
            catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.NotFound)
            {
                return false;
            }
        });
    }

    // Creates a document and claims its keys in one batch. False when the document id is already taken.
    async Task<bool> TryCreateWithUniqueIndexesAsync(Container container, CosmosDocument document, Func<string, IReadOnlyList<UniqueIndexEntry>> entriesOf, CancellationToken ct)
    {
        try
        {
            await this.CommitUniqueBatchAsync(container, document.TypeName,
                entriesOf(document.Data).Select(e => new UniqueClaim(e, document.Id)).ToList(), 1,
                batch => batch.CreateItem(document),
                status => status == HttpStatusCode.Conflict ? new DocumentExistsException() : DocumentWriteFailed(document.TypeName, document.Id, status),
                entriesOf, ct).ConfigureAwait(false);
            return true;
        }
        catch (DocumentExistsException)
        {
            return false;
        }
    }

    // Replaces a document (If-Match the body it was read with) and claims the keys it gains in one batch, then
    // releases the keys it gave up.
    async Task ReplaceWithUniqueIndexesAsync(
        Container container,
        CosmosDocument replacement,
        string previousData,
        string? etag,
        Func<string, IReadOnlyList<UniqueIndexEntry>> entriesOf,
        Func<HttpStatusCode, Exception> documentFailure,
        CancellationToken ct)
    {
        var (added, removed) = UniqueIndexKeys.Diff(entriesOf(previousData), entriesOf(replacement.Data));
        await this.CommitUniqueBatchAsync(container, replacement.TypeName,
            added.Select(e => new UniqueClaim(e, replacement.Id)).ToList(), 1,
            batch => batch.ReplaceItem(replacement.Id, replacement, new TransactionalBatchItemRequestOptions { IfMatchEtag = etag }),
            documentFailure, entriesOf, ct).ConfigureAwait(false);

        await this.ReleaseUniqueEntriesAsync(container, replacement.TypeName, replacement.Id, removed, ct).ConfigureAwait(false);
    }

    // BatchInsert for a type with unique indexes: every chunk creates its documents and their reservations atomically.
    async Task<int> BatchCreateWithUniqueIndexesAsync(
        Container container,
        string typeName,
        IReadOnlyList<CosmosDocument> docs,
        Func<string, IReadOnlyList<UniqueIndexEntry>> entriesOf,
        CancellationToken ct)
    {
        var claimsByDoc = docs.Select(d => entriesOf(d.Data).Select(e => new UniqueClaim(e, d.Id)).ToList()).ToList();

        // Two documents of one batch claiming the same key would otherwise only surface as a 409 that never resolves.
        var claimed = new HashSet<string>(StringComparer.Ordinal);
        foreach (var claim in claimsByDoc.SelectMany(c => c))
        {
            if (!claimed.Add(claim.Entry.Key))
                throw new UniqueConstraintException(typeName, claim.Entry.Mapping, claim.OwnerId);
        }

        var inserted = 0;
        var start = 0;
        while (start < docs.Count)
        {
            // A transactional batch holds at most 100 operations — each document plus the reservations it claims.
            var end = start + 1;
            var operations = 1 + claimsByDoc[start].Count;
            while (end < docs.Count && operations + 1 + claimsByDoc[end].Count <= 100)
            {
                operations += 1 + claimsByDoc[end].Count;
                end++;
            }

            var chunkDocs = docs.Skip(start).Take(end - start).ToList();
            var chunkClaims = claimsByDoc.Skip(start).Take(end - start).SelectMany(c => c).ToList();
            await this.CommitUniqueBatchAsync(container, typeName, chunkClaims, chunkDocs.Count,
                batch =>
                {
                    foreach (var doc in chunkDocs)
                        batch.CreateItem(doc);
                },
                status => new InvalidOperationException($"Batch insert failed with status {status}. A document may have a duplicate Id."),
                entriesOf, ct).ConfigureAwait(false);

            inserted += chunkDocs.Count;
            start = end;
        }
        return inserted;
    }

    // Backup restore / bulk import of one raw row into a type with unique indexes — the same claim-in-batch writes as
    // the typed paths, keyed on the stored type name.
    Task<bool> ApplyUniqueRowAsync(
        Container container,
        IReadOnlyList<UniqueIndexMapping> indexes,
        string id,
        string docType,
        string data,
        string? createdAt,
        string? updatedAt,
        string now,
        BulkWriteMode mode,
        CancellationToken ct)
    {
        var pk = new PartitionKey(docType);
        var entriesOf = this.UniqueEntriesOf(indexes, docType);
        return this.RetryWhileDocumentChangesAsync(async () =>
        {
            ItemResponse<CosmosDocument>? existing = null;
            if (mode is BulkWriteMode.Merge or BulkWriteMode.Replace)
            {
                try
                {
                    existing = await container.ReadItemAsync<CosmosDocument>(id, pk, cancellationToken: ct).ConfigureAwait(false);
                }
                catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.NotFound)
                {
                    // Created below.
                }
            }

            if (existing == null)
            {
                if (await this.TryCreateWithUniqueIndexesAsync(container, NewEnvelope(id, docType, data, createdAt ?? now, updatedAt ?? now), entriesOf, ct).ConfigureAwait(false))
                    return true;

                return mode switch
                {
                    BulkWriteMode.SkipExisting => false,
                    BulkWriteMode.Merge or BulkWriteMode.Replace => throw new DocumentChangedException(),
                    _ => throw new InvalidOperationException($"A document of type '{docType}' with Id '{id}' already exists.")
                };
            }

            var previous = existing.Resource.Data;
            var replacement = mode == BulkWriteMode.Merge
                ? NewEnvelope(id, docType, MergeJson(previous, data), existing.Resource.CreatedAt, now)
                : NewEnvelope(id, docType, data, createdAt ?? now, updatedAt ?? now);

            await this.ReplaceWithUniqueIndexesAsync(container, replacement, previous, existing.ETag, entriesOf,
                status => status is HttpStatusCode.PreconditionFailed or HttpStatusCode.NotFound
                    ? new DocumentChangedException()
                    : DocumentWriteFailed(docType, id, status),
                ct).ConfigureAwait(false);
            return true;
        });
    }

    // Runs the document operations and the claims as one transactional batch. The document operations go first, so a
    // failure there (an id conflict, a stale If-Match) is reported as itself. A claim that collides is resolved —
    // re-locked when it is this document's own, taken over when its owner no longer holds the key, a
    // UniqueConstraintException when it does — and the batch retried.
    async Task CommitUniqueBatchAsync(
        Container container,
        string typeName,
        IReadOnlyList<UniqueClaim> claims,
        int documentOperationCount,
        Action<TransactionalBatch> addDocumentOperations,
        Func<HttpStatusCode, Exception> documentFailure,
        Func<string, IReadOnlyList<UniqueIndexEntry>> entriesOf,
        CancellationToken ct)
    {
        var pk = new PartitionKey(typeName);
        for (var attempt = 1; ; attempt++)
        {
            var batch = container.CreateTransactionalBatch(pk);
            addDocumentOperations(batch);
            foreach (var claim in claims)
            {
                var reservation = new CosmosUniqueReservation
                {
                    Id = claim.ReservationId,
                    TypeName = typeName,
                    UniqueIndex = claim.Entry.Mapping.Name,
                    OwnerId = claim.OwnerId
                };
                if (claim.ETag == null)
                    batch.CreateItem(reservation);
                else
                    batch.ReplaceItem(claim.ReservationId, reservation, new TransactionalBatchItemRequestOptions { IfMatchEtag = claim.ETag });
            }

            using var response = await batch.ExecuteAsync(ct).ConfigureAwait(false);
            if (response.IsSuccessStatusCode)
                return;

            var failed = FirstFailedOperation(response);
            if (failed < documentOperationCount)
                throw documentFailure(failed < response.Count ? response[failed].StatusCode : response.StatusCode);

            var collided = claims[failed - documentOperationCount];
            var status = response[failed].StatusCode;
            if (status is not (HttpStatusCode.Conflict or HttpStatusCode.PreconditionFailed or HttpStatusCode.NotFound))
                throw new InvalidOperationException(
                    $"CosmosDB rejected the unique-index reservation for '{typeName}' document '{collided.OwnerId}' with status {(int)status} ({status}).");

            if (attempt >= MaxUniqueWriteAttempts)
                throw new UniqueConstraintException(typeName, collided.Entry.Mapping, collided.OwnerId);

            await this.ResolveClaimAsync(container, typeName, collided, entriesOf, ct).ConfigureAwait(false);
        }
    }

    // The operation that actually failed; the rest of a failed batch report 424 Failed Dependency. A batch rejected as a
    // whole (too large, malformed) has no per-operation results, which reads as the first document operation.
    static int FirstFailedOperation(TransactionalBatchResponse response)
    {
        for (var i = 0; i < response.Count; i++)
        {
            if (!response[i].IsSuccessStatusCode && response[i].StatusCode != HttpStatusCode.FailedDependency)
                return i;
        }
        return 0;
    }

    async Task ResolveClaimAsync(Container container, string typeName, UniqueClaim claim, Func<string, IReadOnlyList<UniqueIndexEntry>> entriesOf, CancellationToken ct)
    {
        var pk = new PartitionKey(typeName);
        ItemResponse<CosmosUniqueReservation> held;
        try
        {
            held = await container.ReadItemAsync<CosmosUniqueReservation>(claim.ReservationId, pk, cancellationToken: ct).ConfigureAwait(false);
        }
        catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.NotFound)
        {
            // Released since the batch ran — create it afresh.
            claim.ETag = null;
            return;
        }

        // A reservation this document already owns is re-locked rather than left alone, so a concurrent takeover and this
        // write cannot both commit. Another document's is only taken over once that document no longer holds the key.
        if (held.Resource.OwnerId != claim.OwnerId && await OwnerHoldsAsync(container, pk, held.Resource.OwnerId, claim.Entry, entriesOf, ct).ConfigureAwait(false))
            throw new UniqueConstraintException(typeName, claim.Entry.Mapping, claim.OwnerId);

        claim.ETag = held.ETag;
    }

    static async Task<bool> OwnerHoldsAsync(Container container, PartitionKey pk, string ownerId, UniqueIndexEntry entry, Func<string, IReadOnlyList<UniqueIndexEntry>> entriesOf, CancellationToken ct)
    {
        try
        {
            var owner = await container.ReadItemAsync<CosmosDocument>(ownerId, pk, cancellationToken: ct).ConfigureAwait(false);
            return owner.Resource.Data != null && entriesOf(owner.Resource.Data).Any(e => e.Key == entry.Key);
        }
        catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.NotFound)
        {
            return false;
        }
    }

    // A reservation a write no longer needs is deleted only while it still names this document, and If-Match, so a
    // concurrent re-claim wins. One left behind is harmless: the next claimant sees its owner no longer holds the key.
    async Task ReleaseUniqueEntriesAsync(Container container, string typeName, string ownerId, IReadOnlyList<UniqueIndexEntry> released, CancellationToken ct)
    {
        var pk = new PartitionKey(typeName);
        foreach (var entry in released)
        {
            var reservationId = CosmosUniqueReservation.IdFor(entry);
            try
            {
                var held = await container.ReadItemAsync<CosmosUniqueReservation>(reservationId, pk, cancellationToken: ct).ConfigureAwait(false);
                if (held.Resource.OwnerId == ownerId)
                    await DeleteReservationAsync(container, pk, reservationId, held.ETag, ct).ConfigureAwait(false);
            }
            catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.NotFound)
            {
                // Already released.
            }
        }
    }

    /// <summary>Releases every reservation held by <paramref name="ownerIds"/> — called once those documents are deleted.</summary>
    internal async Task ReleaseReservationsOwnedByAsync(Container container, string typeName, IReadOnlyList<string> ownerIds, CancellationToken ct)
    {
        var pk = new PartitionKey(typeName);
        foreach (var owners in ownerIds.Chunk(500))
        {
            var query = new QueryDefinition("SELECT c.id, c.ownerId, c._etag FROM c WHERE IS_DEFINED(c.uniqueIndex) AND ARRAY_CONTAINS(@owners, c.ownerId)")
                .WithParameter("@owners", owners);

            var held = new List<CosmosUniqueReservation>();
            using (var iterator = container.GetItemQueryIterator<CosmosUniqueReservation>(query, requestOptions: new QueryRequestOptions { PartitionKey = pk }))
            {
                while (iterator.HasMoreResults)
                    held.AddRange(await iterator.ReadNextAsync(ct).ConfigureAwait(false));
            }

            foreach (var reservation in held)
                await DeleteReservationAsync(container, pk, reservation.Id, reservation.ETag, ct).ConfigureAwait(false);
        }
    }

    static async Task DeleteReservationAsync(Container container, PartitionKey pk, string reservationId, string? etag, CancellationToken ct)
    {
        try
        {
            await container.DeleteItemAsync<CosmosUniqueReservation>(reservationId, pk, new ItemRequestOptions { IfMatchEtag = etag }, ct).ConfigureAwait(false);
        }
        catch (CosmosException ex) when (ex.StatusCode is HttpStatusCode.NotFound or HttpStatusCode.PreconditionFailed)
        {
            // Released, or re-claimed, by another write in the meantime.
        }
    }

    Task RetryWhileDocumentChangesAsync(Func<Task> attempt)
        => this.RetryWhileDocumentChangesAsync(async () =>
        {
            await attempt().ConfigureAwait(false);
            return true;
        });

    async Task<TResult> RetryWhileDocumentChangesAsync<TResult>(Func<Task<TResult>> attempt)
    {
        for (var tries = 1; ; tries++)
        {
            try
            {
                return await attempt().ConfigureAwait(false);
            }
            catch (DocumentChangedException ex) when (tries >= MaxUniqueWriteAttempts)
            {
                throw new InvalidOperationException("The document kept changing concurrently while a unique-index write was being applied. Retry the write.", ex);
            }
            catch (DocumentChangedException)
            {
                // A concurrent write landed between the read and the If-Match batch — read again.
            }
        }
    }

    static Exception DocumentWriteFailed(string typeName, string id, HttpStatusCode status)
        => new InvalidOperationException($"CosmosDB rejected the write of '{typeName}' document '{id}' with status {(int)status} ({status}).");

    // One key a write claims. A null ETag creates the reservation; a set one replaces an existing reservation If-Match —
    // this document's own (re-locking it) or a stale one being taken over.
    sealed class UniqueClaim(UniqueIndexEntry entry, string ownerId)
    {
        public UniqueIndexEntry Entry => entry;
        public string OwnerId => ownerId;
        public string ReservationId { get; } = CosmosUniqueReservation.IdFor(entry);
        public string? ETag { get; set; }
    }

    // A concurrent write replaced (or created) the document between this write's read and its If-Match batch.
    sealed class DocumentChangedException : Exception;

    // A create's 409 on the document itself — the id is taken.
    sealed class DocumentExistsException : Exception;
}
