using System.Text.Json.Serialization.Metadata;
using LiteDB;

namespace Shiny.DocumentDb.LiteDb;

// Unique indexes (MapUniqueIndex). Bodies are stored as a JSON string, so LiteDB cannot index a value inside one;
// instead each index entry a document occupies is a reservation row in the collection's "_unique" sidecar, keyed by
// the entry hash and owned by the document. Reservations change in the same LiteDB transaction as the document
// write, so a violating write leaves nothing behind and never releases what the document already held.
public partial class LiteDbDocumentStore
{
    ILiteCollection<BsonDocument> GetUniqueCollection<T>()
        => this.db.GetCollection<BsonDocument>(this.ResolveCollectionName<T>() + "_unique");

    bool HasUniqueIndexes<T>() => this.options.Mappings.ResolveUniqueIndexes(typeof(T)).Count > 0;

    // The entries a body occupies; the document is only deserialized when an index filter has to be evaluated.
    IReadOnlyList<UniqueIndexEntry> UniqueEntries<T>(string typeName, string? json, JsonTypeInfo<T>? typeInfo) where T : class
    {
        var indexes = this.options.Mappings.ResolveUniqueIndexes(typeof(T));
        return indexes.Count == 0 || json == null
            ? []
            : UniqueIndexKeys.Compute(indexes, typeName, json, () => Deserialize(json, typeInfo, this.jsonOptions)!, this.jsonOptions);
    }

    // Moves the document's reservations from `before` to `after`. Every entry in `after` is claimed (idempotently for
    // one the document already holds, which also back-fills a document stored before the index was mapped), and the
    // conflict check runs before anything is written. Must run inside the transaction that writes the document.
    void SyncUniqueEntries<T>(string typeName, string id, IReadOnlyList<UniqueIndexEntry> before, IReadOnlyList<UniqueIndexEntry> after)
    {
        if (before.Count == 0 && after.Count == 0)
            return;

        var collection = this.GetUniqueCollection<T>();
        var owner = $"{typeName}:{id}";
        var conflict = after.FirstOrDefault(e => collection.FindById(e.Hash) is { } held && held["Owner"].AsString != owner);
        if (conflict != null)
            throw new UniqueConstraintException(typeName, conflict.Mapping, id);

        foreach (var entry in UniqueIndexKeys.Diff(before, after).Removed)
        {
            if (collection.FindById(entry.Hash) is { } held && held["Owner"].AsString == owner)
                collection.Delete(entry.Hash);
        }

        foreach (var entry in after)
        {
            collection.Upsert(new BsonDocument
            {
                ["_id"] = entry.Hash,
                ["Owner"] = owner,
                ["TypeName"] = typeName,
                ["Index"] = entry.Mapping.Name
            });
        }
    }

    // Runs a write in a LiteDB transaction when the type has unique indexes, so reservations and the document commit or
    // roll back together. Inside a unit of work the thread's transaction is already open (BeginTrans returns false), so
    // the write joins it and the unit decides the outcome.
    TResult WithUniqueTransaction<T, TResult>(Func<TResult> work)
    {
        if (!this.HasUniqueIndexes<T>())
            return work();

        var owns = this.db.BeginTrans();
        try
        {
            var result = work();
            if (owns)
                this.db.Commit();
            return result;
        }
        catch
        {
            if (owns)
                this.db.Rollback();
            throw;
        }
    }

    void WithUniqueTransaction<T>(Action work)
        => this.WithUniqueTransaction<T, bool>(() =>
        {
            work();
            return true;
        });
}
