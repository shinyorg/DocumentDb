using System.Net;
using System.Text;
using System.Text.Json;
using Microsoft.Azure.Cosmos;
using Shiny.DocumentDb.Internal;

namespace Shiny.DocumentDb.CosmosDb;

public partial class CosmosDbDocumentStore : IDocumentBackup
{
    /// <inheritdoc />
    public async Task ExportAsync(Stream destination, BackupExportOptions? options = null, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(destination);
        options ??= new BackupExportOptions();

        var docTypeFilter = options.DocTypes is { Count: > 0 }
            ? new HashSet<string>(options.DocTypes, StringComparer.Ordinal)
            : null;

        await using var writer = new Utf8JsonWriter(destination, new JsonWriterOptions { Indented = options.Indented });
        writer.WriteStartArray();

        // Each container is partitioned by /typeName and may hold many types (the default container) or a
        // single mapped type. Sidecar history containers ("{name}_history") are rebuilt by the write path
        // on restore, so they are skipped here — matching the v1 backup contract.
        var containers = await this.ListDocumentContainerNamesAsync(cancellationToken).ConfigureAwait(false);
        var rowsSinceFlush = 0;
        foreach (var containerName in containers)
        {
            var container = this.database!.GetContainer(containerName);

            // c.data is the logical document body; the Cosmos system fields (_rid/_etag/_ts) and the
            // envelope columns (id/typeName/createdAt/updatedAt) live at the item root, so selecting
            // c.data alone yields the unwrapped body with no extra unwrapping required.
            var sql = "SELECT c.id, c.typeName, c.data, c.createdAt, c.updatedAt FROM c";
            if (docTypeFilter != null)
            {
                var names = docTypeFilter.ToList();
                sql += " WHERE c.typeName IN (" + string.Join(", ", names.Select((_, i) => "@t" + i)) + ")";
            }
            var queryDef = new QueryDefinition(sql);
            if (docTypeFilter != null)
            {
                var names = docTypeFilter.ToList();
                for (var i = 0; i < names.Count; i++)
                    queryDef.WithParameter("@t" + i, names[i]);
            }

            this.Log($"CosmosDB EXPORT {containerName}: {sql}");

            // Cross-partition query (no PartitionKey) so every type in a shared container is exported.
            using var iterator = container.GetItemQueryIterator<CosmosDocument>(queryDef);
            while (iterator.HasMoreResults)
            {
                var response = await iterator.ReadNextAsync(cancellationToken).ConfigureAwait(false);
                foreach (var doc in response)
                {
                    if (doc.Data == null)
                        continue;
                    BackupStreams.WriteRecord(writer, doc.Id, doc.TypeName, doc.Data, ParseCosmosTimestamp(doc.CreatedAt), ParseCosmosTimestamp(doc.UpdatedAt));
                    if (++rowsSinceFlush >= 500)
                    {
                        await writer.FlushAsync(cancellationToken).ConfigureAwait(false);
                        rowsSinceFlush = 0;
                    }
                }
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
    /// <remarks>
    /// Cosmos has no cross-document/cross-container transaction, so the import is best-effort: rows are applied
    /// as bounded concurrent waves of single-item operations (the same model the store's BatchUpsert/BatchRemove
    /// use, since parallel requests parallelize RU spend). <see cref="BulkRestoreOptions.SingleTransaction"/> is
    /// therefore not honored — each chunk is independent and a failure mid-import leaves prior chunks written.
    /// </remarks>
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
        var now = DateTimeOffset.UtcNow.ToString("o");

        var chunk = new List<(string id, string docType, string data, string? createdAt, string? updatedAt)>(chunkSize);

        async Task FlushChunkAsync()
        {
            if (chunk.Count == 0)
                return;

            // Resolve (and lazily create) every container the chunk touches once, up front — avoids
            // racing EnsureContainerAsync from inside the concurrent wave.
            var containers = new Dictionary<string, Container>(StringComparer.Ordinal);
            foreach (var docType in chunk.Select(r => r.docType).Distinct(StringComparer.Ordinal))
                containers[docType] = await this
                    .EnsureContainerAsync(this.options.ResolveContainerName(docType), cancellationToken)
                    .ConfigureAwait(false);

            for (var offset = 0; offset < chunk.Count; offset += CosmosBatchConcurrency)
            {
                cancellationToken.ThrowIfCancellationRequested();
                var end = Math.Min(offset + CosmosBatchConcurrency, chunk.Count);
                var wave = new List<Task<bool>>(end - offset);
                for (var i = offset; i < end; i++)
                {
                    var row = chunk[i];
                    wave.Add(this.ApplyRowAsync(containers[row.docType], row.id, row.docType, row.data, row.createdAt, row.updatedAt, now, options.Mode, cancellationToken));
                }

                foreach (var wrote in await Task.WhenAll(wave).ConfigureAwait(false))
                {
                    if (wrote)
                        written++;
                    else
                        skipped++;
                }
            }

            chunk.Clear();
            chunksCommitted++;
            options.Progress?.Report(new BulkProgress(read, written, chunksCommitted));
        }

        await foreach (var doc in documents.WithCancellation(cancellationToken).ConfigureAwait(false))
        {
            read++;
            chunk.Add((doc.Id, doc.DocType, Encoding.UTF8.GetString(doc.Data.Span),
                doc.CreatedAt?.ToString("o"), doc.UpdatedAt?.ToString("o")));
            if (chunk.Count >= chunkSize)
                await FlushChunkAsync().ConfigureAwait(false);
        }
        await FlushChunkAsync().ConfigureAwait(false);

        return new BulkRestoreResult(read, written, skipped, chunksCommitted);
    }

    // Applies a single raw row according to the write mode. Returns true when the row was written, false when
    // it was skipped (SkipExisting hitting an existing id). The body is wrapped in the same CosmosDocument
    // envelope the store's normal writes use, so c.data holds the logical document and queries keep working.
    async Task<bool> ApplyRowAsync(Container container, string id, string docType, string data, string? createdAt, string? updatedAt, string now, BulkWriteMode mode, CancellationToken ct)
    {
        var pk = new PartitionKey(docType);

        switch (mode)
        {
            case BulkWriteMode.Merge:
            {
                // RFC 7396 merge: read the existing body, deep-merge the incoming patch, replace. Falls back to
                // a create when the id is new. (Same merge helper the single-doc Upsert path uses.)
                try
                {
                    var resp = await container.ReadItemAsync<CosmosDocument>(id, pk, cancellationToken: ct).ConfigureAwait(false);
                    var existing = resp.Resource;
                    existing.Data = MergeJson(existing.Data, data);
                    existing.UpdatedAt = now;
                    await container.ReplaceItemAsync(existing, id, pk, cancellationToken: ct).ConfigureAwait(false);
                    return true;
                }
                catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.NotFound)
                {
                    await container.CreateItemAsync(NewEnvelope(id, docType, data, createdAt ?? now, updatedAt ?? now), pk, cancellationToken: ct).ConfigureAwait(false);
                    return true;
                }
            }

            case BulkWriteMode.Replace:
                // Cosmos upsert = create-or-replace the whole item, preserving the exported timestamps when present.
                await container.UpsertItemAsync(NewEnvelope(id, docType, data, createdAt ?? now, updatedAt ?? now), pk, cancellationToken: ct).ConfigureAwait(false);
                return true;

            case BulkWriteMode.SkipExisting:
                try
                {
                    await container.CreateItemAsync(NewEnvelope(id, docType, data, createdAt ?? now, updatedAt ?? now), pk, cancellationToken: ct).ConfigureAwait(false);
                    return true;
                }
                catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.Conflict)
                {
                    return false;
                }

            case BulkWriteMode.Insert:
            default:
                try
                {
                    // Fresh insert — preserve the exported createdAt/updatedAt (v2 backup); a v1 row (null) stamps now.
                    await container.CreateItemAsync(NewEnvelope(id, docType, data, createdAt ?? now, updatedAt ?? now), pk, cancellationToken: ct).ConfigureAwait(false);
                    return true;
                }
                catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.Conflict)
                {
                    throw new InvalidOperationException(
                        $"A document of type '{docType}' with Id '{id}' already exists.", ex);
                }
        }
    }

    static CosmosDocument NewEnvelope(string id, string docType, string data, string createdAt, string updatedAt) => new()
    {
        Id = id,
        TypeName = docType,
        Data = data,
        CreatedAt = createdAt,
        UpdatedAt = updatedAt
    };

    // Parses a stored ISO-8601 CreatedAt/UpdatedAt back to a DateTimeOffset for the export envelope, or null.
    static DateTimeOffset? ParseCosmosTimestamp(string? value)
        => DateTimeOffset.TryParse(value, System.Globalization.CultureInfo.InvariantCulture,
            System.Globalization.DateTimeStyles.RoundtripKind, out var parsed) ? parsed : null;

    async Task<List<string>> ListDocumentContainerNamesAsync(CancellationToken ct)
    {
        await this.initSemaphore.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            if (this.database == null)
            {
                var dbResponse = await this.client
                    .CreateDatabaseIfNotExistsAsync(this.options.DatabaseName, cancellationToken: ct)
                    .ConfigureAwait(false);
                this.database = dbResponse.Database;
            }

            var names = new List<string>();
            using var iterator = this.database.GetContainerQueryIterator<ContainerProperties>();
            while (iterator.HasMoreResults)
            {
                var page = await iterator.ReadNextAsync(ct).ConfigureAwait(false);
                foreach (var props in page)
                {
                    if (!props.Id.EndsWith("_history", StringComparison.Ordinal))
                        names.Add(props.Id);
                }
            }
            return names;
        }
        finally
        {
            this.initSemaphore.Release();
        }
    }
}
