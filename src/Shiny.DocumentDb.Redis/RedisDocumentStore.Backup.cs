using System.Text;
using System.Text.Json;
using Shiny.DocumentDb.Internal;
using StackExchange.Redis;

namespace Shiny.DocumentDb.Redis;

public partial class RedisDocumentStore : IDocumentBackup
{
    /// <inheritdoc />
    public async Task ExportAsync(Stream destination, BackupExportOptions? options = null, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(destination);
        options ??= new BackupExportOptions();
        await this.EnsureModulesAsync(cancellationToken).ConfigureAwait(false);

        var docTypes = options.DocTypes is { Count: > 0 }
            ? new HashSet<string>(options.DocTypes, StringComparer.Ordinal)
            : null;

        await using var writer = new Utf8JsonWriter(destination, new JsonWriterOptions { Indented = options.Indented });
        writer.WriteStartArray();

        var server = this.GetServer();
        var written = 0;
        await foreach (var key in server.KeysAsync(database: this.db.Database, pattern: $"{this.keyPrefix}doc:*").WithCancellation(cancellationToken).ConfigureAwait(false))
        {
            var env = await this.GetEnvelopeAsync(key!).ConfigureAwait(false);
            var docType = env?[RedisDocument.TypeName]?.GetValue<string>();
            var id = env?[RedisDocument.Id]?.GetValue<string>();
            var dataJson = env == null ? null : RedisDocument.GetDataJson(env);
            var include = env != null && docType != null && id != null && dataJson != null
                && (docTypes == null || docTypes.Contains(docType));

            if (include)
            {
                var created = RedisDocument.GetCreatedAt(env!);
                var updated = RedisDocument.GetUpdatedAt(env!);
                BackupStreams.WriteRecord(writer, id!, docType!, dataJson!,
                    created is { } c ? new DateTimeOffset(c, TimeSpan.Zero) : null,
                    updated is { } u ? new DateTimeOffset(u, TimeSpan.Zero) : null);

                if (++written % 500 == 0)
                    await writer.FlushAsync(cancellationToken).ConfigureAwait(false);
            }
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
        await this.EnsureModulesAsync(cancellationToken).ConfigureAwait(false);

        if (options.ClearExistingFirst)
            await this.ClearAll(cancellationToken).ConfigureAwait(false);

        long read = 0, written = 0, skipped = 0;
        var chunks = 0;
        var sinceChunk = 0;
        var chunkSize = Math.Max(1, options.ChunkSize);
        var now = DateTime.UtcNow;

        await foreach (var doc in documents.WithCancellation(cancellationToken).ConfigureAwait(false))
        {
            read++;
            var key = this.DocKey(doc.DocType, doc.Id);
            var dataJson = Encoding.UTF8.GetString(doc.Data.Span);
            var envelope = RedisDocument.BuildEnvelope(doc.Id, doc.DocType, dataJson, now, null,
                doc.CreatedAt?.UtcDateTime, doc.UpdatedAt?.UtcDateTime);

            var didWrite = await this.ImportOneAsync(key, envelope, dataJson, doc, now, options.Mode).ConfigureAwait(false);
            if (didWrite)
                written++;
            else
                skipped++;

            if (++sinceChunk >= chunkSize)
            {
                chunks++;
                sinceChunk = 0;
                options.Progress?.Report(new BulkProgress(read, written, chunks));
            }
        }

        if (sinceChunk > 0)
        {
            chunks++;
            options.Progress?.Report(new BulkProgress(read, written, chunks));
        }
        return new BulkRestoreResult(read, written, skipped, chunks);
    }

    async Task<bool> ImportOneAsync(string key, string envelope, string dataJson, RawDocument doc, DateTime now, BulkWriteMode mode)
    {
        // Imports keep unique-index reservations in step like any other write; the raw record carries only its type
        // name, so the indexes resolve by name and a filter is evaluated over the deserialized body.
        switch (mode)
        {
            case BulkWriteMode.Insert:
                await this.PersistAsync(key, envelope, doc.DocType, doc.Id, RedisWriteGuard.MustNotExist, null,
                    [], this.UniqueEntries(doc.DocType, dataJson, null), CancellationToken.None).ConfigureAwait(false);
                return true;

            case BulkWriteMode.Replace:
                await this.PersistAsync(key, envelope, doc.DocType, doc.Id, RedisWriteGuard.None, null,
                    await this.StoredUniqueEntriesAsync(doc.DocType, key).ConfigureAwait(false),
                    this.UniqueEntries(doc.DocType, dataJson, null), CancellationToken.None).ConfigureAwait(false);
                return true;

            case BulkWriteMode.SkipExisting:
                if (await this.db.KeyExistsAsync(key).ConfigureAwait(false))
                    return false;
                await this.PersistAsync(key, envelope, doc.DocType, doc.Id, RedisWriteGuard.None, null,
                    [], this.UniqueEntries(doc.DocType, dataJson, null), CancellationToken.None).ConfigureAwait(false);
                return true;

            case BulkWriteMode.Merge:
                var existing = await this.GetEnvelopeAsync(key).ConfigureAwait(false);
                if (existing == null)
                {
                    await this.PersistAsync(key, envelope, doc.DocType, doc.Id, RedisWriteGuard.None, null,
                        [], this.UniqueEntries(doc.DocType, dataJson, null), CancellationToken.None).ConfigureAwait(false);
                    return true;
                }
                var originalData = RedisDocument.GetDataJson(existing) ?? "{}";
                var merged = MergeJson(originalData, StripNullProperties(dataJson));
                var mergedEnvelope = RedisDocument.BuildEnvelope(doc.Id, doc.DocType, merged, now, null,
                    RedisDocument.GetCreatedAt(existing));
                await this.PersistAsync(key, mergedEnvelope, doc.DocType, doc.Id, RedisWriteGuard.None, null,
                    this.UniqueEntries(doc.DocType, originalData, null), this.UniqueEntries(doc.DocType, merged, null),
                    CancellationToken.None).ConfigureAwait(false);
                return true;

            default:
                throw new ArgumentOutOfRangeException(nameof(mode), mode, null);
        }
    }
}
