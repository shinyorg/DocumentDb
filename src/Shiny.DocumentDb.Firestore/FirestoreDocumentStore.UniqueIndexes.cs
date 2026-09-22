using System.Text.Json.Serialization.Metadata;
using Google.Cloud.Firestore;

namespace Shiny.DocumentDb.Firestore;

// Unique indexes (MapUniqueIndex). Firestore has no secondary unique constraint, so every index entry a document occupies
// is a reservation document in a "{collection}_unique" sibling collection — id = UniqueIndexEntry.Hash, owner = the
// document id. It lives outside the type's collection, so no Get, query, count, listener or cursor ever reaches one.
// A write to a type with unique indexes runs in one Firestore transaction that reads the documents and every reservation
// it touches, then commits the document writes, the claims and the releases together.
public partial class FirestoreDocumentStore
{
    const string ReservationOwner = "owner";

    IReadOnlyList<UniqueIndexMapping> UniqueIndexes<T>() => this.options.Mappings.ResolveUniqueIndexes(typeof(T));

    CollectionReference ReservationCollection(string collectionName) => this.db.Collection(collectionName + "_unique");

    // Documents per transaction: each writes itself plus, at worst, one claim and one release per index.
    int UniqueWriteChunkSize<T>() => Math.Max(1, BatchWriteChunkSize / (1 + 2 * this.UniqueIndexes<T>().Count));

    // The built map is the stored form; Firestore's own map type is Dictionary<string, object> at runtime.
    static IDictionary<string, object> AsStoredMap(Dictionary<string, object?> map) => (IDictionary<string, object>)(object)map;

    // Entries are always computed from the stored map form of the body, so a document's before and after entries compare
    // like-for-like even where the map round-trip normalizes a value (1.0 → 1).
    IReadOnlyList<UniqueIndexEntry> UniqueEntries<T>(string typeName, IDictionary<string, object> map, JsonTypeInfo<T>? typeInfo) where T : class
    {
        var json = FirestoreDocument.MapToJson(map);
        return UniqueIndexKeys.Compute(this.UniqueIndexes<T>(), typeName, json, () => Deserialize(json, typeInfo, this.jsonOptions)!, this.jsonOptions);
    }

    /// <summary>What one document's write amounts to, decided from its snapshot inside the transaction.</summary>
    readonly record struct UniqueWrite(string? Json, bool Delete)
    {
        public static UniqueWrite Skip => default;
        public static UniqueWrite Remove => new(null, true);
        public static UniqueWrite Put(string json) => new(json, false);
        public bool Writes => this.Json != null || this.Delete;
    }

    /// <summary>
    /// Writes the documents in <paramref name="ids"/> with their unique-index reservations kept in step — one transaction per
    /// chunk, so a chunk commits or fails as a whole. <paramref name="stage"/> sees each snapshot read inside the transaction
    /// and may throw (not found, version conflict) to abort it; it can run more than once if Firestore retries. Returns how
    /// many documents were written or deleted.
    /// </summary>
    Task<int> WriteWithUniqueIndexesAsync<T>(IReadOnlyList<string> ids, string typeName, JsonTypeInfo<T>? typeInfo, Func<DocumentSnapshot, UniqueWrite> stage, CancellationToken ct) where T : class
        => this.WriteWithUniqueIndexesAsync(ids, typeName, typeInfo, DateTime.UtcNow, stage, ct);

    /// <summary>
    /// As above, writing <paramref name="now"/> as the envelope <c>updatedAt</c> (and <c>createdAt</c> of a new document),
    /// so a caller that stamps a <see cref="DocumentMetadata"/> afterwards stamps the exact value stored.
    /// </summary>
    async Task<int> WriteWithUniqueIndexesAsync<T>(IReadOnlyList<string> ids, string typeName, JsonTypeInfo<T>? typeInfo, DateTime now, Func<DocumentSnapshot, UniqueWrite> stage, CancellationToken ct) where T : class
    {
        var written = 0;
        foreach (var chunk in ids.Chunk(this.UniqueWriteChunkSize<T>()))
            written += await this.db.RunTransactionAsync(tx => this.WriteUniqueChunkAsync(tx, chunk, typeName, typeInfo, now, stage, ct), cancellationToken: ct).ConfigureAwait(false);
        return written;
    }

    async Task<int> WriteUniqueChunkAsync<T>(Transaction tx, IReadOnlyList<string> ids, string typeName, JsonTypeInfo<T>? typeInfo, DateTime now, Func<DocumentSnapshot, UniqueWrite> stage, CancellationToken ct) where T : class
    {
        var collection = this.GetCollection<T>();
        var reservations = this.ReservationCollection(this.ResolveCollectionName<T>());
        var snapshots = await tx.GetAllSnapshotsAsync(ids.Select(id => collection.Document(id)), ct).ConfigureAwait(false);

        var puts = new List<(DocumentReference Reference, Dictionary<string, object?> Map)>();
        var deletes = new List<DocumentReference>();
        var claims = new Dictionary<string, (UniqueIndexEntry Entry, string Owner)>(StringComparer.Ordinal);
        var releases = new Dictionary<string, string>(StringComparer.Ordinal);

        foreach (var snap in snapshots)
        {
            var write = stage(snap);
            if (write.Writes)
            {
                var before = snap.Exists ? this.UniqueEntries(typeName, snap.ToDictionary(), typeInfo) : [];
                IReadOnlyList<UniqueIndexEntry> after = [];
                if (write.Json != null)
                {
                    var createdAt = snap.Exists ? FirestoreDocument.ReadTimestamps(snap.ToDictionary()).CreatedAt?.UtcDateTime ?? now : now;
                    var map = FirestoreDocument.BuildMap(write.Json, typeName, createdAt, now);
                    after = this.UniqueEntries(typeName, AsStoredMap(map), typeInfo);
                    puts.Add((snap.Reference, map));
                }
                else
                {
                    deletes.Add(snap.Reference);
                }

                var (added, removed) = UniqueIndexKeys.Diff(before, after);
                foreach (var entry in removed)
                    releases[entry.Hash] = snap.Id;
                foreach (var entry in added)
                {
                    // Two documents in this same transaction claiming one key.
                    if (claims.TryGetValue(entry.Hash, out var other) && other.Owner != snap.Id)
                        throw new UniqueConstraintException(typeName, entry.Mapping, snap.Id);
                    claims[entry.Hash] = (entry, snap.Id);
                }
            }
        }

        var owners = new Dictionary<string, string?>(StringComparer.Ordinal);
        var touched = claims.Keys.Concat(releases.Keys).Distinct(StringComparer.Ordinal).Select(hash => reservations.Document(hash)).ToList();
        if (touched.Count > 0)
        {
            foreach (var reservation in await tx.GetAllSnapshotsAsync(touched, ct).ConfigureAwait(false))
                owners[reservation.Id] = reservation.Exists ? reservation.GetValue<string>(ReservationOwner) : null;
        }

        foreach (var (hash, (entry, owner)) in claims)
        {
            var current = owners.GetValueOrDefault(hash);
            // Free, already this document's, or given up by its owner within this same transaction.
            var available = current == null || current == owner || (releases.TryGetValue(hash, out var releaser) && releaser == current);
            if (!available)
                throw new UniqueConstraintException(typeName, entry.Mapping, owner);
        }

        foreach (var (reference, map) in puts)
            tx.Set(reference, map, SetOptions.Overwrite);
        foreach (var reference in deletes)
            tx.Delete(reference);
        foreach (var (hash, releaser) in releases)
        {
            // Only release a reservation the document actually holds — a document stored before the index was mapped
            // holds none, and must not free a key another document owns.
            if (!claims.ContainsKey(hash) && owners.GetValueOrDefault(hash) == releaser)
                tx.Delete(reservations.Document(hash));
        }
        foreach (var (hash, (entry, owner)) in claims)
        {
            tx.Set(reservations.Document(hash), new Dictionary<string, object?>
            {
                [ReservationOwner] = owner,
                ["typeName"] = typeName,
                ["index"] = entry.Mapping.Name
            }, SetOptions.Overwrite);
        }
        return puts.Count + deletes.Count;
    }

    // A batch insert rejects a key shared inside the batch before anything is written, so a duplicate never leaves the
    // earlier chunks committed.
    void EnsureBatchKeysDistinct<T>(IEnumerable<(string Id, Dictionary<string, object?> Map)> pending, string typeName, JsonTypeInfo<T>? typeInfo) where T : class
    {
        var seen = new HashSet<string>(StringComparer.Ordinal);
        foreach (var (id, map) in pending)
        {
            foreach (var entry in this.UniqueEntries(typeName, AsStoredMap(map), typeInfo))
            {
                if (!seen.Add(entry.Hash))
                    throw new UniqueConstraintException(typeName, entry.Mapping, id);
            }
        }
    }

    // A compensating rollback only knows the collection and the id, so it finds the document's reservations by owner
    // rather than recomputing them.
    async Task DeleteTrackedDocumentAsync(string collectionName, string id, CancellationToken ct)
    {
        var document = this.db.Collection(collectionName).Document(id);
        if (!this.options.Mappings.HasUniqueIndexes)
        {
            await document.DeleteAsync(Precondition.None, ct).ConfigureAwait(false);
            return;
        }

        var batch = this.db.StartBatch();
        batch.Delete(document);
        var owned = await this.ReservationCollection(collectionName).WhereEqualTo(ReservationOwner, id).GetSnapshotAsync(ct).ConfigureAwait(false);
        foreach (var reservation in owned.Documents)
            batch.Delete(reservation.Reference);
        await batch.CommitAsync(ct).ConfigureAwait(false);
    }
}
