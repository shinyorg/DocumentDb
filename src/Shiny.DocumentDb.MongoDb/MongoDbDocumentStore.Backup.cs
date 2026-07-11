using System.Text;
using System.Text.Json;
using MongoDB.Bson;
using MongoDB.Driver;
using Shiny.DocumentDb.Internal;

namespace Shiny.DocumentDb.MongoDb;

public partial class MongoDbDocumentStore : IDocumentBackup
{
    /// <inheritdoc />
    public async Task ExportAsync(Stream destination, BackupExportOptions? options = null, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(destination);
        options ??= new BackupExportOptions();

        var docTypes = options.DocTypes is { Count: > 0 }
            ? new HashSet<string>(options.DocTypes, StringComparer.Ordinal)
            : null;
        // Match the JSON shape Get<T>() / the history writer round-trips through.
        var jsonSettings = new MongoDB.Bson.IO.JsonWriterSettings { OutputMode = MongoDB.Bson.IO.JsonOutputMode.RelaxedExtendedJson };

        await using var writer = new Utf8JsonWriter(destination, new JsonWriterOptions { Indented = options.Indented });
        writer.WriteStartArray();

        using var cursor = await this.database
            .ListCollectionNamesAsync(cancellationToken: cancellationToken)
            .ConfigureAwait(false);
        var names = await cursor.ToListAsync(cancellationToken).ConfigureAwait(false);

        var written = 0;
        foreach (var name in names)
        {
            // Sidecar history collections are rebuilt by the write path, never exported.
            if (name.EndsWith("_history", StringComparison.Ordinal))
                continue;

            var collection = this.database.GetCollection<BsonDocument>(name);
            var filter = docTypes == null
                ? FilterDefinition<BsonDocument>.Empty
                : Builders<BsonDocument>.Filter.In(MongoFields.TypeName, docTypes);

            this.Log($"MongoDB EXPORT {name}");
            using var docCursor = await collection.Find(filter).ToCursorAsync(cancellationToken).ConfigureAwait(false);
            while (await docCursor.MoveNextAsync(cancellationToken).ConfigureAwait(false))
            {
                foreach (var doc in docCursor.Current)
                {
                    // Skip anything that is not a document envelope (defensive — only doc + history
                    // collections exist, and history is filtered out above).
                    if (!doc.Contains(MongoFields.Data) || !doc.Contains(MongoFields.TypeName) || !doc.Contains(MongoFields.DocId))
                        continue;

                    var docType = doc[MongoFields.TypeName].AsString;
                    if (docTypes != null && !docTypes.Contains(docType))
                        continue;

                    var id = doc[MongoFields.DocId].AsString;
                    var rawJson = doc[MongoFields.Data].AsBsonDocument.ToJson(jsonSettings);
                    BackupStreams.WriteRecord(writer, id, docType, rawJson, ReadBsonTimestamp(doc, MongoFields.CreatedAt), ReadBsonTimestamp(doc, MongoFields.UpdatedAt));

                    if (++written % 500 == 0)
                        await writer.FlushAsync(cancellationToken).ConfigureAwait(false);
                }
            }
            await writer.FlushAsync(cancellationToken).ConfigureAwait(false);
        }

        writer.WriteEndArray();
        await writer.FlushAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public Task<BulkRestoreResult> RestoreAsync(Stream source, BulkRestoreOptions? options = null, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(source);
        return this.BulkImportAsync(BackupStreams.ReadAsync(source, cancellationToken), options, cancellationToken);
    }

    /// <inheritdoc />
    public async Task<BulkRestoreResult> BulkImportAsync(
        IAsyncEnumerable<RawDocument> documents,
        BulkRestoreOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(documents);
        options ??= new BulkRestoreOptions();
        var chunkSize = Math.Max(1, options.ChunkSize);

        if (options.ClearExistingFirst)
            await this.ClearAll(cancellationToken).ConfigureAwait(false);

        long read = 0, written = 0, skipped = 0;
        var chunksCommitted = 0;

        // Buffer per docType — every chunk maps to a single Mongo collection via ResolveCollectionName,
        // and the composite _id keeps types disjoint even when several share a collection.
        var buffers = new Dictionary<string, List<BackupRow>>(StringComparer.Ordinal);

        async Task FlushAsync(string docType, List<BackupRow> rows)
        {
            if (rows.Count == 0)
                return;

            var collection = this.database.GetCollection<BsonDocument>(this.options.ResolveCollectionName(docType));
            var (w, s) = await this.BulkImportChunkAsync(collection, docType, rows, options.Mode, cancellationToken).ConfigureAwait(false);
            written += w;
            skipped += s;
            chunksCommitted++;
            options.Progress?.Report(new BulkProgress(read, written, chunksCommitted));
            rows.Clear();
        }

        await foreach (var doc in documents.WithCancellation(cancellationToken).ConfigureAwait(false))
        {
            read++;
            if (!buffers.TryGetValue(doc.DocType, out var rows))
            {
                rows = new List<BackupRow>(chunkSize);
                buffers[doc.DocType] = rows;
            }
            rows.Add(new BackupRow(doc.Id, Encoding.UTF8.GetString(doc.Data.Span), doc.CreatedAt?.UtcDateTime, doc.UpdatedAt?.UtcDateTime));
            if (rows.Count >= chunkSize)
                await FlushAsync(doc.DocType, rows).ConfigureAwait(false);
        }

        foreach (var pair in buffers)
            await FlushAsync(pair.Key, pair.Value).ConfigureAwait(false);

        return new BulkRestoreResult(read, written, skipped, chunksCommitted);
    }

    // A buffered backup row plus the optional exported timestamps (v2 backup); null → stamp now on import.
    readonly record struct BackupRow(string id, string dataJson, DateTime? createdAt, DateTime? updatedAt);

    // Reads a CreatedAt/UpdatedAt field off a stored envelope as a UTC DateTimeOffset, or null if absent.
    static DateTimeOffset? ReadBsonTimestamp(BsonDocument doc, string field)
    {
        if (!doc.TryGetValue(field, out var value) || !value.IsValidDateTime)
            return null;
        return new DateTimeOffset(value.ToUniversalTime(), TimeSpan.Zero);
    }

    async Task<(long written, long skipped)> BulkImportChunkAsync(
        IMongoCollection<BsonDocument> collection,
        string typeName,
        List<BackupRow> rows,
        BulkWriteMode mode,
        CancellationToken ct)
    {
        var now = DateTime.UtcNow;
        var models = new List<WriteModel<BsonDocument>>(rows.Count);

        switch (mode)
        {
            case BulkWriteMode.Insert:
                foreach (var r in rows)
                    models.Add(new InsertOneModel<BsonDocument>(BuildEnvelope(r.id, typeName, r.dataJson, now, r.createdAt, r.updatedAt)));
                break;

            case BulkWriteMode.Replace:
                foreach (var r in rows)
                    models.Add(new ReplaceOneModel<BsonDocument>(
                        Builders<BsonDocument>.Filter.Eq(MongoFields.Id, CompositeId(typeName, r.id)),
                        BuildEnvelope(r.id, typeName, r.dataJson, now))
                    {
                        IsUpsert = true
                    });
                break;

            case BulkWriteMode.SkipExisting:
                foreach (var r in rows)
                {
                    // $setOnInsert applies only when the upsert inserts — existing docs are left untouched.
                    // _id is omitted: the filter equality supplies it on insert.
                    var envelope = BuildEnvelope(r.id, typeName, r.dataJson, now);
                    envelope.Remove(MongoFields.Id);
                    models.Add(new UpdateOneModel<BsonDocument>(
                        Builders<BsonDocument>.Filter.Eq(MongoFields.Id, CompositeId(typeName, r.id)),
                        new BsonDocument("$setOnInsert", envelope))
                    {
                        IsUpsert = true
                    });
                }
                break;

            case BulkWriteMode.Merge:
                var compositeIds = rows.Select(r => CompositeId(typeName, r.id)).ToList();
                var existingDocs = await collection
                    .Find(Builders<BsonDocument>.Filter.In(MongoFields.Id, compositeIds))
                    .ToListAsync(ct)
                    .ConfigureAwait(false);
                var existingById = new Dictionary<string, BsonDocument>(existingDocs.Count);
                foreach (var d in existingDocs)
                    existingById[d[MongoFields.Id].AsString] = d[MongoFields.Data].AsBsonDocument;

                foreach (var r in rows)
                {
                    var compositeId = CompositeId(typeName, r.id);
                    var patchJson = StripNullProperties(r.dataJson);
                    if (existingById.TryGetValue(compositeId, out var existingData))
                    {
                        var merged = MergeJson(existingData.ToJson(), patchJson);
                        var update = Builders<BsonDocument>.Update
                            .Set(MongoFields.Data, BsonDocument.Parse(merged))
                            .Set(MongoFields.UpdatedAt, now);
                        models.Add(new UpdateOneModel<BsonDocument>(Builders<BsonDocument>.Filter.Eq(MongoFields.Id, compositeId), update));
                    }
                    else
                    {
                        models.Add(new InsertOneModel<BsonDocument>(BuildEnvelope(r.id, typeName, patchJson, now)));
                    }
                }
                break;

            default:
                throw new ArgumentOutOfRangeException(nameof(mode), mode, null);
        }

        this.Log($"MongoDB BULK IMPORT {collection.CollectionNamespace.CollectionName} mode={mode} ({models.Count})");
        try
        {
            var result = await collection
                .BulkWriteAsync(models, new BulkWriteOptions { IsOrdered = true }, ct)
                .ConfigureAwait(false);

            if (mode == BulkWriteMode.SkipExisting)
            {
                var inserted = result.Upserts.Count;
                return (inserted, rows.Count - inserted);
            }
            return (rows.Count, 0);
        }
        catch (MongoBulkWriteException ex) when (mode == BulkWriteMode.Insert && ex.WriteErrors.Any(e => e.Category == ServerErrorCategory.DuplicateKey))
        {
            throw new InvalidOperationException(
                $"A document of type '{typeName}' has a duplicate Id in the import chunk.", ex);
        }
    }
}
