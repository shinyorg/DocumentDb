using System.Data.Common;
using System.Text;
using System.Text.Json;
using Shiny.DocumentDb.Internal;

namespace Shiny.DocumentDb;

public partial class DocumentStore : IDocumentBackup
{
    /// <inheritdoc />
    public async Task ExportAsync(Stream destination, BackupExportOptions? options = null, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(destination);
        options ??= new BackupExportOptions();

        await using var writer = new Utf8JsonWriter(destination, new JsonWriterOptions { Indented = options.Indented });
        writer.WriteStartArray();
        foreach (var table in this.options.AllDocumentTableNames())
        {
            await this.ExportTableAsync(writer, table, options.DocTypes, cancellationToken).ConfigureAwait(false);
            await writer.FlushAsync(cancellationToken).ConfigureAwait(false);
        }
        writer.WriteEndArray();
        await writer.FlushAsync(cancellationToken).ConfigureAwait(false);
    }

    Task ExportTableAsync(Utf8JsonWriter writer, string table, IReadOnlyCollection<string>? docTypes, CancellationToken ct)
        => this.ExecuteAsync(table, async session =>
        {
            await using var cmd = session.CreateCommand();
            var sql = $"SELECT Id, TypeName, Data FROM {Qt(table)}";
            if (docTypes is { Count: > 0 })
            {
                var names = docTypes.ToList();
                sql += " WHERE TypeName IN (" + string.Join(", ", names.Select((_, i) => "@t" + i)) + ")";
                for (var i = 0; i < names.Count; i++)
                    AddParameter(cmd, "@t" + i, names[i]);
            }
            cmd.CommandText = sql + ";";
            this.Log(cmd.CommandText);

            await using var reader = await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false);
            while (await reader.ReadAsync(ct).ConfigureAwait(false))
                BackupStreams.WriteRecord(writer, reader.GetString(0), reader.GetString(1), reader.GetString(2));
        }, ct);

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

        if (options.Mode is BulkWriteMode.Replace or BulkWriteMode.SkipExisting && !this.provider.SupportsBulkReplace)
            throw new NotSupportedException(
                $"BulkWriteMode.{options.Mode} is not supported by provider '{this.provider.GetType().Name}'.");

        if (options.ClearExistingFirst)
            await this.ClearAll(cancellationToken).ConfigureAwait(false);

        long read = 0, written = 0, skipped = 0;
        var chunksCommitted = 0;

        await this.ExecuteAsync(this.options.TableName, async session =>
        {
            var initialized = new HashSet<string>(StringComparer.OrdinalIgnoreCase) { this.options.TableName };
            // Buffer by docType (the TypeName column) — a shared table holds many types but every multi-row
            // statement binds a single @typeName, so each chunk must be type-homogeneous.
            var buffers = new Dictionary<string, List<RawBulkRow>>(StringComparer.Ordinal);

            DbTransaction? singleTx = options.SingleTransaction
                ? await session.Connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false)
                : null;

            async Task FlushAsync(string docType, List<RawBulkRow> rows)
            {
                if (rows.Count == 0)
                    return;

                var table = this.options.ResolveTableName(docType);
                if (initialized.Add(table))
                    await this.EnsureTableInitializedAsync(session, table, cancellationToken).ConfigureAwait(false);

                var tx = singleTx;
                var ownTx = tx == null;
                tx ??= await session.Connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
                try
                {
                    long affected;
                    // Native bulk copy is skipped when tenancy is enabled and the provider's bulk path can't
                    // carry the extra TenantId column (DuckDB's positional appender); the multi-row INSERT
                    // fallback NULLs TenantId just like the PostgreSQL/SQL Server bulk paths do.
                    var canBulkCopy = this.provider.SupportsBulkCopy
                        && (this.tenantIdAccessor == null || this.provider.SupportsBulkCopyWithTenant);
                    if (options.Mode == BulkWriteMode.Insert && canBulkCopy)
                        affected = await this.provider.BulkCopyInsertAsync(session.Connection, tx, table, docType, rows, DateTimeOffset.UtcNow, cancellationToken).ConfigureAwait(false);
                    else
                        affected = await this.BulkWriteChunkAsync(session.Connection, tx, table, docType, rows, options.Mode, cancellationToken).ConfigureAwait(false);
                    if (ownTx)
                        await tx.CommitAsync(cancellationToken).ConfigureAwait(false);

                    written += affected;
                    skipped += rows.Count - affected;
                    if (ownTx)
                    {
                        chunksCommitted++;
                        options.Progress?.Report(new BulkProgress(read, written, chunksCommitted));
                    }
                }
                catch
                {
                    if (ownTx)
                        await tx.RollbackAsync(cancellationToken).ConfigureAwait(false);
                    throw;
                }
                finally
                {
                    if (ownTx)
                        await tx.DisposeAsync().ConfigureAwait(false);
                }
                rows.Clear();
            }

            try
            {
                await foreach (var doc in documents.WithCancellation(cancellationToken).ConfigureAwait(false))
                {
                    read++;
                    if (!buffers.TryGetValue(doc.DocType, out var rows))
                    {
                        rows = new List<RawBulkRow>(chunkSize);
                        buffers[doc.DocType] = rows;
                    }
                    rows.Add(new RawBulkRow(doc.Id, Encoding.UTF8.GetString(doc.Data.Span)));
                    if (rows.Count >= chunkSize)
                        await FlushAsync(doc.DocType, rows).ConfigureAwait(false);
                }

                foreach (var pair in buffers)
                    await FlushAsync(pair.Key, pair.Value).ConfigureAwait(false);

                if (singleTx != null)
                {
                    await singleTx.CommitAsync(cancellationToken).ConfigureAwait(false);
                    chunksCommitted++;
                    options.Progress?.Report(new BulkProgress(read, written, chunksCommitted));
                }
            }
            catch
            {
                if (singleTx != null)
                    await singleTx.RollbackAsync(cancellationToken).ConfigureAwait(false);
                throw;
            }
            finally
            {
                if (singleTx != null)
                    await singleTx.DisposeAsync().ConfigureAwait(false);
            }
        }, cancellationToken).ConfigureAwait(false);

        return new BulkRestoreResult(read, written, skipped, chunksCommitted);
    }

    async Task<int> BulkWriteChunkAsync(
        DbConnection connection,
        DbTransaction transaction,
        string table,
        string typeName,
        List<RawBulkRow> rows,
        BulkWriteMode mode,
        CancellationToken ct)
    {
        // Merge on a provider without a native multi-row upsert (i.e. not SQLite/DuckDB): fall back to the
        // single-doc merge-upsert per row inside this chunk's transaction. Every relational provider supports
        // it (native JSON_MERGE_PATCH / json_patch, or the read-merge-write fallback on PG/SQL Server).
        if (mode == BulkWriteMode.Merge && !this.provider.SupportsBatchUpsert)
        {
            var mergeSession = new DocumentStoreSession(connection, transaction);
            foreach (var row in rows)
                await this.MergeOrReplaceCoreAsync(mergeSession, table, row.Id, typeName, row.Data, null, null, merge: true, insertIfMissing: true, ct).ConfigureAwait(false);
            return rows.Count;
        }

        var sql = mode switch
        {
            BulkWriteMode.Insert => this.provider.BuildBatchInsertSql(table, rows.Count),
            BulkWriteMode.Replace => this.provider.BuildBatchReplaceSql(table, rows.Count),
            BulkWriteMode.SkipExisting => this.provider.BuildBatchSkipExistingSql(table, rows.Count),
            BulkWriteMode.Merge => this.provider.BuildBatchUpsertSql(table, rows.Count),
            _ => throw new ArgumentOutOfRangeException(nameof(mode), mode, null)
        };

        await using var cmd = connection.CreateCommand();
        cmd.Transaction = transaction;
        cmd.CommandText = sql;
        AddParameter(cmd, "@typeName", typeName);
        AddParameter(cmd, "@now", DateTimeOffset.UtcNow);
        for (var i = 0; i < rows.Count; i++)
        {
            AddParameter(cmd, $"@id_{i}", rows[i].Id);
            AddParameter(cmd, $"@data_{i}", rows[i].Data);
        }

        this.Log(cmd.CommandText);
        try
        {
            return await cmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
        }
        catch (Exception ex) when (mode == BulkWriteMode.Insert && this.provider.IsDuplicateKeyException(ex))
        {
            throw new InvalidOperationException(
                $"A document of type '{typeName}' has a duplicate Id in the import chunk.", ex);
        }
    }
}
