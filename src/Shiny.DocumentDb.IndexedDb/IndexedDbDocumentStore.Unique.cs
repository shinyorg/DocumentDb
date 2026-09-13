using System.Globalization;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;

namespace Shiny.DocumentDb.IndexedDb;

// Unique indexes (MapUniqueIndex). Bodies are stored as a JSON string, so an IndexedDB index cannot reach a value
// inside one, and adding an object store would force a database version bump. Instead each index entry a document
// occupies is a reservation record in the document's own object store — keyed "~uq:<hash>", typed "~unique:<type>"
// so the typeName index never hands it to a read of a real type. The JS writeDocuments call re-checks the stored body,
// the reservations and the write inside one readwrite transaction, so a violating or stale write changes nothing.
public partial class IndexedDbDocumentStore
{
    const string ReservationKeyPrefix = "~uq:";

    static string ReservationTypeName(string typeName) => "~unique:" + typeName;

    bool HasUniqueIndexes<T>() => this.options.Mappings.ResolveUniqueIndexes(typeof(T)).Count > 0;

    // The entries a body occupies; the document is only deserialized when an index filter has to be evaluated.
    IReadOnlyList<UniqueIndexEntry> UniqueEntries<T>(string typeName, string? json, JsonTypeInfo<T>? typeInfo) where T : class
    {
        var indexes = this.options.Mappings.ResolveUniqueIndexes(typeof(T));
        return indexes.Count == 0 || json == null
            ? []
            : UniqueIndexKeys.Compute(indexes, typeName, json, () => Deserialize(json, typeInfo, this.jsonOptions)!, this.jsonOptions);
    }

    // One document change: `storedData` is the body read before the write (null when it must not exist yet), `record`
    // the body to store (null to delete). Every entry the new body occupies is claimed — idempotent for one already
    // held, which also back-fills a document stored before the index was mapped.
    UniqueWriteOp UniqueOp<T>(string typeName, string key, string? storedData, DocumentRecord? record, JsonTypeInfo<T>? typeInfo) where T : class
    {
        var before = this.UniqueEntries(typeName, storedData, typeInfo);
        var after = this.UniqueEntries(typeName, record?.Data, typeInfo);
        return new UniqueWriteOp
        {
            Key = key,
            ExpectedData = storedData,
            Record = record,
            Claims = after
                .Select(e => new UniqueClaim { Key = ReservationKeyPrefix + e.Hash, Index = e.Mapping.Name, TypeName = ReservationTypeName(typeName) })
                .ToArray(),
            Releases = UniqueIndexKeys.Diff(before, after).Removed
                .Select(e => ReservationKeyPrefix + e.Hash)
                .ToArray()
        };
    }

    async Task WriteUniqueAsync<T>(string storeName, string typeName, UniqueWriteOp[] ops) where T : class
    {
        var outcome = await IndexedDbJsInterop.WriteDocuments(storeName, JsonSerializer.Serialize(ops, IndexedDbInteropJsonContext.Default.UniqueWriteOpArray));
        if (outcome == "ok")
            return;

        var parts = outcome.Split(':', 3);
        var op = ops[int.Parse(parts[1], CultureInfo.InvariantCulture)];
        var id = op.Key[(typeName.Length + 1)..];
        if (parts[0] == "unique")
            throw new UniqueConstraintException(typeName, this.options.Mappings.ResolveUniqueIndexes(typeof(T)).First(m => m.Name == parts[2]), id);

        throw op.ExpectedData == null
            ? new InvalidOperationException($"A document of type '{typeName}' with Id '{id}' already exists.")
            : new InvalidOperationException($"Document '{id}' of type '{typeName}' changed while it was being written. Retry the write.");
    }

    // Put a single record, going through the reservation-checked write when the type has unique indexes.
    Task PutRecordAsync<T>(string storeName, string typeName, string? storedData, DocumentRecord record, JsonTypeInfo<T>? typeInfo) where T : class
        => this.HasUniqueIndexes<T>()
            ? this.WriteUniqueAsync<T>(storeName, typeName, [this.UniqueOp(typeName, record.Key, storedData, record, typeInfo)])
            : IndexedDbJsInterop.Put(storeName, SerializeRecord(record));

    // Delete a single record, releasing its reservations in the same transaction when the type has unique indexes.
    async Task<bool> RemoveRecordAsync<T>(string storeName, string typeName, string key) where T : class
    {
        if (!this.HasUniqueIndexes<T>())
            return await IndexedDbJsInterop.Remove(storeName, key);

        var storedJson = await IndexedDbJsInterop.Get(storeName, key);
        if (storedJson == null)
            return false;

        var stored = JsonSerializer.Deserialize(storedJson, IndexedDbInteropJsonContext.Default.DocumentRecord)!;
        await this.WriteUniqueAsync<T>(storeName, typeName, [this.UniqueOp<T>(typeName, key, stored.Data, null, null)]);
        return true;
    }
}

/// <summary>One document change for the JS <c>writeDocuments</c> call — see <see cref="IndexedDbDocumentStore"/>.</summary>
internal sealed class UniqueWriteOp
{
    public string Key { get; set; } = default!;
    public string? ExpectedData { get; set; }
    public DocumentRecord? Record { get; set; }
    public UniqueClaim[] Claims { get; set; } = [];
    public string[] Releases { get; set; } = [];
}

internal sealed class UniqueClaim
{
    public string Key { get; set; } = default!;
    public string Index { get; set; } = default!;
    public string TypeName { get; set; } = default!;
}
