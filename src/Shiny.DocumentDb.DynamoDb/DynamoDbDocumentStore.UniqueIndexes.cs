using System.Text;
using System.Text.Json.Serialization.Metadata;
using Amazon.DynamoDBv2.Model;

namespace Shiny.DocumentDb.DynamoDb;

// Unique indexes (MapUniqueIndex). DynamoDB has no secondary unique constraint, so each index entry a document occupies
// is a reservation item — pk "unique#{hash}", sk "unique", naming its owner — written in the same TransactWriteItems
// call as the document, conditioned on being free or already ours. Reservation partitions never equal a type's
// partition key, so no query, count, change-feed record or blob scan ever sees them.
public partial class DynamoDbDocumentStore
{
    const string ReservationPrefix = "unique#";
    const string ReservationSk = "unique";
    const string OwnerAttr = "Owner";
    const string OwnerPkAttr = "OwnerPk";
    const string IndexAttr = "Index";
    const string ConditionalCheckFailed = "ConditionalCheckFailed";
    const int MaxTransactItems = 100;
    const int MaxTransactBytes = 3_500_000;
    const int MaxReclaimAttempts = 3;

    // One document write plus the reservations it claims and releases, sent as a unit.
    sealed record ReservedWrite(
        TransactWriteItem Item,
        string Id,
        int Bytes,
        IReadOnlyList<UniqueIndexEntry> Claim,
        List<UniqueIndexEntry> Release,
        Func<Exception, Exception> OnConditionFailed);

    bool HasUniqueIndexes<T>() => this.options.Mappings.ResolveUniqueIndexes(typeof(T)).Count > 0;

    IReadOnlyList<UniqueIndexEntry> UniqueEntries<T>(string typeName, string json, JsonTypeInfo<T>? typeInfo, T? document) where T : class
    {
        var indexes = this.options.Mappings.ResolveUniqueIndexes(typeof(T));
        if (indexes.Count == 0)
            return [];

        return UniqueIndexKeys.Compute(indexes, typeName, json,
            () => document ?? this.Materialize(json, typeInfo)
                ?? throw new InvalidOperationException($"Could not read a stored '{typeName}' document to evaluate its unique index filter."),
            this.jsonOptions);
    }

    // Writes a document whose unique-index entries may have changed. Without a change it is the plain conditional put;
    // otherwise the put and the reservation changes commit together.
    async Task PutDocumentAsync<T>(
        Dictionary<string, AttributeValue> item,
        string typeName,
        string partitionKey,
        string id,
        int? expectedVersion,
        IReadOnlyList<UniqueIndexEntry> before,
        IReadOnlyList<UniqueIndexEntry> after,
        JsonTypeInfo<T>? typeInfo,
        CancellationToken ct) where T : class
    {
        var (claim, release) = UniqueIndexKeys.Diff(before, after);
        if (claim.Count == 0 && release.Count == 0)
        {
            await this.PutWithVersionGuardAsync(item, typeName, id, expectedVersion, ct).ConfigureAwait(false);
            return;
        }

        var put = new Put { TableName = this.TableName, Item = item };
        if (expectedVersion != null)
        {
            put.ConditionExpression = "#v = :expected";
            put.ExpressionAttributeNames = new Dictionary<string, string> { ["#v"] = DynamoDbDocument.VersionAttr };
            put.ExpressionAttributeValues = new Dictionary<string, AttributeValue>
            {
                [":expected"] = new AttributeValue { N = expectedVersion.Value.ToString(System.Globalization.CultureInfo.InvariantCulture) }
            };
        }

        var write = new ReservedWrite(new TransactWriteItem { Put = put }, id, 0, claim, release.ToList(),
            ex => expectedVersion != null ? new ConcurrencyException(typeName, id, expectedVersion.Value) : ex);
        await this.TransactReservedAsync([write], typeName, partitionKey, typeInfo, ct).ConfigureAwait(false);
    }

    // Inserts a batch with its reservations. A duplicate inside the batch is rejected before anything is written; the
    // rest is sent in transactions of at most 100 items / ~3.5 MB, each atomic on its own.
    async Task BatchInsertReservedAsync<T>(
        IReadOnlyList<Dictionary<string, AttributeValue>> items,
        string typeName,
        string partitionKey,
        JsonTypeInfo<T>? typeInfo,
        IReadOnlyList<T> documents,
        CancellationToken ct) where T : class
    {
        var writes = new List<ReservedWrite>(items.Count);
        var seen = new HashSet<string>(StringComparer.Ordinal);
        for (var i = 0; i < items.Count; i++)
        {
            var item = items[i];
            var id = item[DynamoDbDocument.Sk].S;
            var json = DynamoDbDocument.GetData(item);
            var entries = this.UniqueEntries(typeName, json, typeInfo, documents[i]);
            var duplicate = entries.FirstOrDefault(e => !seen.Add(e.Key));
            if (duplicate != null)
                throw new UniqueConstraintException(typeName, duplicate.Mapping, id);

            var put = new Put { TableName = this.TableName, Item = item, ConditionExpression = "attribute_not_exists(sk)" };
            writes.Add(new ReservedWrite(new TransactWriteItem { Put = put }, id, Encoding.UTF8.GetByteCount(json), entries, [],
                ex => new InvalidOperationException($"A document of type '{typeName}' with Id '{id}' already exists.", ex)));
        }

        var chunk = new List<ReservedWrite>();
        var chunkItems = 0;
        var chunkBytes = 0;
        foreach (var write in writes)
        {
            var size = 1 + write.Claim.Count;
            if (chunk.Count > 0 && (chunkItems + size > MaxTransactItems || chunkBytes + write.Bytes > MaxTransactBytes))
            {
                await this.TransactReservedAsync(chunk, typeName, partitionKey, typeInfo, ct).ConfigureAwait(false);
                chunk = new List<ReservedWrite>();
                chunkItems = 0;
                chunkBytes = 0;
            }
            chunk.Add(write);
            chunkItems += size;
            chunkBytes += write.Bytes;
        }
        if (chunk.Count > 0)
            await this.TransactReservedAsync(chunk, typeName, partitionKey, typeInfo, ct).ConfigureAwait(false);
    }

    // Deletes one document and releases its reservations atomically.
    Task DeleteDocumentReservedAsync<T>(string typeName, string partitionKey, string id, Dictionary<string, AttributeValue> existing, JsonTypeInfo<T>? typeInfo, CancellationToken ct) where T : class
    {
        var release = this.UniqueEntries(typeName, DynamoDbDocument.GetData(existing), typeInfo, null).ToList();
        var delete = new Delete { TableName = this.TableName, Key = DynamoDbDocument.Key(partitionKey, id) };
        var write = new ReservedWrite(new TransactWriteItem { Delete = delete }, id, 0, [], release, ex => ex);
        return this.TransactReservedAsync([write], typeName, partitionKey, typeInfo, ct);
    }

    // Releases the reservations of documents already deleted by a set-based path (Clear, ExecuteDelete, BatchRemove).
    // Each release is conditioned on still naming the deleted owner, so a value another document has since claimed is
    // left alone; a release lost to a crash is reclaimed by the next writer (see TryReclaimStaleReservationAsync).
    async Task ReleaseReservationsAsync<T>(string typeName, string partitionKey, IEnumerable<Dictionary<string, AttributeValue>> deleted, JsonTypeInfo<T>? typeInfo, CancellationToken ct) where T : class
    {
        if (!this.HasUniqueIndexes<T>())
            return;

        foreach (var item in deleted)
        {
            var ownerId = item[DynamoDbDocument.Sk].S;
            foreach (var entry in this.UniqueEntries(typeName, DynamoDbDocument.GetData(item), typeInfo, null))
            {
                try
                {
                    await this.client.DeleteItemAsync(new DeleteItemRequest
                    {
                        TableName = this.TableName,
                        Key = ReservationKey(entry),
                        ConditionExpression = "#o = :o AND #op = :op",
                        ExpressionAttributeNames = OwnerNames(),
                        ExpressionAttributeValues = OwnerValues(partitionKey, ownerId)
                    }, ct).ConfigureAwait(false);
                }
                catch (ConditionalCheckFailedException)
                {
                    // Already released, or reclaimed by another document — not ours to remove.
                }
            }
        }
    }

    async Task TransactReservedAsync<T>(IReadOnlyList<ReservedWrite> writes, string typeName, string partitionKey, JsonTypeInfo<T>? typeInfo, CancellationToken ct) where T : class
    {
        for (var attempt = 0; ; attempt++)
        {
            var items = new List<TransactWriteItem>();
            var slots = new List<(ReservedWrite Write, UniqueIndexEntry? Claim, UniqueIndexEntry? Release)>();
            foreach (var write in writes)
            {
                items.Add(write.Item);
                slots.Add((write, null, null));
                foreach (var entry in write.Claim)
                {
                    items.Add(this.ClaimItem(entry, partitionKey, write.Id));
                    slots.Add((write, entry, null));
                }
                foreach (var entry in write.Release)
                {
                    items.Add(this.ReleaseItem(entry, partitionKey, write.Id));
                    slots.Add((write, null, entry));
                }
            }

            this.Log($"DynamoDB TRANSACT WRITE {items.Count} items (unique reservations) into {this.TableName}");
            try
            {
                await this.client.TransactWriteItemsAsync(new TransactWriteItemsRequest { TransactItems = items }, ct).ConfigureAwait(false);
                return;
            }
            catch (TransactionCanceledException ex)
            {
                var codes = CancellationCodes(ex);
                var failed = Enumerable.Range(0, Math.Min(codes.Count, slots.Count))
                    .Where(i => codes[i] == ConditionalCheckFailed)
                    .Select(i => slots[i])
                    .ToList();
                if (failed.Count == 0)
                    throw;

                foreach (var slot in failed.Where(s => s.Claim == null && s.Release == null))
                    throw slot.Write.OnConditionFailed(ex);

                // A reservation this document was to release now names someone else — it is no longer ours to drop.
                foreach (var slot in failed.Where(s => s.Release != null))
                    slot.Write.Release.Remove(slot.Release!);

                foreach (var slot in failed.Where(s => s.Claim != null))
                {
                    var reclaimed = attempt < MaxReclaimAttempts
                        && await this.TryReclaimStaleReservationAsync(slot.Claim!, typeName, typeInfo, ct).ConfigureAwait(false);
                    if (!reclaimed)
                        throw new UniqueConstraintException(typeName, slot.Claim!.Mapping, slot.Write.Id, ex);
                }
            }
        }
    }

    // A reservation is stale when its owner no longer exists or no longer holds that entry — left behind by a path
    // that is not atomic with its release (a crash mid-Clear, a unit-of-work rollback that deleted an insert). A stale
    // one is removed (conditioned on still naming that owner) so the caller can retry; a live one is a real conflict.
    async Task<bool> TryReclaimStaleReservationAsync<T>(UniqueIndexEntry entry, string typeName, JsonTypeInfo<T>? typeInfo, CancellationToken ct) where T : class
    {
        var reservation = await this.client.GetItemAsync(new GetItemRequest
        {
            TableName = this.TableName,
            Key = ReservationKey(entry),
            ConsistentRead = true
        }, ct).ConfigureAwait(false);
        if (!reservation.IsItemSet)
            return true;

        var ownerId = reservation.Item[OwnerAttr].S;
        var ownerPk = reservation.Item[OwnerPkAttr].S;
        var owner = await this.client.GetItemAsync(new GetItemRequest
        {
            TableName = this.TableName,
            Key = DynamoDbDocument.Key(ownerPk, ownerId),
            ConsistentRead = true
        }, ct).ConfigureAwait(false);

        var live = owner.IsItemSet
            && this.UniqueEntries(typeName, DynamoDbDocument.GetData(owner.Item), typeInfo, null).Any(e => e.Key == entry.Key);
        if (live)
            return false;

        try
        {
            await this.client.DeleteItemAsync(new DeleteItemRequest
            {
                TableName = this.TableName,
                Key = ReservationKey(entry),
                ConditionExpression = "#o = :o AND #op = :op",
                ExpressionAttributeNames = OwnerNames(),
                ExpressionAttributeValues = OwnerValues(ownerPk, ownerId)
            }, ct).ConfigureAwait(false);
        }
        catch (ConditionalCheckFailedException)
        {
            // Changed hands meanwhile — the retry will see the new owner.
        }
        return true;
    }

    TransactWriteItem ClaimItem(UniqueIndexEntry entry, string ownerPk, string ownerId)
    {
        var item = ReservationKey(entry);
        item[OwnerAttr] = new AttributeValue { S = ownerId };
        item[OwnerPkAttr] = new AttributeValue { S = ownerPk };
        item[IndexAttr] = new AttributeValue { S = entry.Mapping.Name };
        return new TransactWriteItem
        {
            Put = new Put
            {
                TableName = this.TableName,
                Item = item,
                ConditionExpression = "attribute_not_exists(pk) OR (#o = :o AND #op = :op)",
                ExpressionAttributeNames = OwnerNames(),
                ExpressionAttributeValues = OwnerValues(ownerPk, ownerId)
            }
        };
    }

    TransactWriteItem ReleaseItem(UniqueIndexEntry entry, string ownerPk, string ownerId) => new()
    {
        Delete = new Delete
        {
            TableName = this.TableName,
            Key = ReservationKey(entry),
            ConditionExpression = "attribute_not_exists(pk) OR (#o = :o AND #op = :op)",
            ExpressionAttributeNames = OwnerNames(),
            ExpressionAttributeValues = OwnerValues(ownerPk, ownerId)
        }
    };

    static Dictionary<string, AttributeValue> ReservationKey(UniqueIndexEntry entry)
        => DynamoDbDocument.Key(ReservationPrefix + entry.Hash, ReservationSk);

    static Dictionary<string, string> OwnerNames() => new() { ["#o"] = OwnerAttr, ["#op"] = OwnerPkAttr };

    static Dictionary<string, AttributeValue> OwnerValues(string ownerPk, string ownerId) => new()
    {
        [":o"] = new AttributeValue { S = ownerId },
        [":op"] = new AttributeValue { S = ownerPk }
    };

    // Per-item cancellation codes, in request order. DynamoDB Local has at times left CancellationReasons empty and only
    // listed the codes in the message ("… [ConditionalCheckFailed, None]"), so fall back to that.
    static IReadOnlyList<string> CancellationCodes(TransactionCanceledException ex)
    {
        if (ex.CancellationReasons is { Count: > 0 } reasons)
            return reasons.Select(r => r.Code ?? "None").ToList();

        var open = ex.Message.LastIndexOf('[');
        var close = ex.Message.LastIndexOf(']');
        return open >= 0 && close > open
            ? ex.Message[(open + 1)..close].Split(',').Select(c => c.Trim()).ToList()
            : [];
    }
}
