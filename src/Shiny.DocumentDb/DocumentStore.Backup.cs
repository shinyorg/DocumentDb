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
            await this.ExportTableAsync(writer, table, options, cancellationToken).ConfigureAwait(false);
            await writer.FlushAsync(cancellationToken).ConfigureAwait(false);
        }
        writer.WriteEndArray();
        await writer.FlushAsync(cancellationToken).ConfigureAwait(false);
    }

    Task ExportTableAsync(Utf8JsonWriter writer, string table, BackupExportOptions options, CancellationToken ct)
        => this.ExecuteAsync(table, async session =>
        {
            var includeBlobs = options.IncludeBlobs
                && this.provider.MaxBlobSize > 0
                && TableHasBlobMapping(this.options, table);

            await using var cmd = session.CreateCommand();
            var sql = $"SELECT Id, TypeName, Data, CreatedAt, UpdatedAt FROM {Qt(table)}";
            if (options.DocTypes is { Count: > 0 })
            {
                var names = options.DocTypes.ToList();
                sql += " WHERE TypeName IN (" + string.Join(", ", names.Select((_, i) => "@t" + i)) + ")";
                for (var i = 0; i < names.Count; i++)
                    AddParameter(cmd, "@t" + i, names[i]);
            }
            cmd.CommandText = sql + ";";
            this.Log(cmd.CommandText);

            if (!includeBlobs)
            {
                await using var reader = await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false);
                while (await reader.ReadAsync(ct).ConfigureAwait(false))
                    BackupStreams.WriteRecord(
                        writer, reader.GetString(0), reader.GetString(1), reader.GetString(2),
                        ReadTimestamp(reader, 3), ReadTimestamp(reader, 4));
                return;
            }

            // Buffer the document rows first, then read each doc's blobs — the shared-connection providers
            // (SQLite/DuckDB) can't run the blob query while the document reader is still open. Only the doc
            // metadata is buffered; payloads are read one document at a time.
            var docs = new List<(string Id, string TypeName, string Data, DateTimeOffset? Ca, DateTimeOffset? Ua)>();
            await using (var reader = await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false))
            {
                while (await reader.ReadAsync(ct).ConfigureAwait(false))
                    docs.Add((reader.GetString(0), reader.GetString(1), reader.GetString(2),
                              ReadTimestamp(reader, 3), ReadTimestamp(reader, 4)));
            }

            foreach (var d in docs)
            {
                var blobs = await this.ReadBlobsForExportAsync(session, table, d.Id, d.TypeName, ct).ConfigureAwait(false);
                BackupStreams.WriteRecord(writer, d.Id, d.TypeName, d.Data, d.Ca, d.Ua, blobs);
            }
        }, ct);

    async Task<IReadOnlyList<RawBlob>?> ReadBlobsForExportAsync(
        DocumentStoreSession session, string table, string id, string typeName, CancellationToken ct)
    {
        await using var cmd = session.CreateCommand();
        cmd.CommandText =
            $"SELECT BlobKey, Data, Length, ContentType, FileName, Hash, CreatedAt, UpdatedAt " +
            $"FROM {Qt(this.provider.BlobTableName(table))} WHERE Id = @id AND TypeName = @typeName";
        AddParameter(cmd, "@id", id);
        AddParameter(cmd, "@typeName", typeName);
        this.Log(cmd.CommandText);

        List<RawBlob>? list = null;
        await using var reader = await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false);
        while (await reader.ReadAsync(ct).ConfigureAwait(false))
        {
            var data = ReadBinary(reader, 1);
            if (data == null)
                continue;
            (list ??= new List<RawBlob>()).Add(new RawBlob(
                reader.GetString(0), data, reader.GetInt64(2),
                reader.IsDBNull(3) ? null : reader.GetString(3),
                reader.IsDBNull(4) ? null : reader.GetString(4),
                reader.IsDBNull(5) ? null : reader.GetString(5),
                ReadTimestamp(reader, 6) ?? default, ReadTimestamp(reader, 7) ?? default));
        }
        return list;
    }

    // Reads a CreatedAt/UpdatedAt column back as a DateTimeOffset across providers (SQLite stores ISO text,
    // the relational engines a native timestamp/timestamptz). Returns null on a NULL column or an unreadable
    // value so a v1 store still exports cleanly (the import then re-stamps).
    static DateTimeOffset? ReadTimestamp(DbDataReader reader, int ordinal)
    {
        if (reader.IsDBNull(ordinal))
            return null;
        try
        {
            return reader.GetFieldValue<DateTimeOffset>(ordinal);
        }
        catch (Exception ex) when (ex is InvalidCastException or FormatException)
        {
            try
            {
                var value = reader.GetValue(ordinal);
                return value switch
                {
                    DateTimeOffset dto => dto,
                    DateTime dt => new DateTimeOffset(DateTime.SpecifyKind(dt, DateTimeKind.Utc)),
                    string s when DateTimeOffset.TryParse(s, System.Globalization.CultureInfo.InvariantCulture,
                        System.Globalization.DateTimeStyles.RoundtripKind, out var parsed) => parsed,
                    _ => null
                };
            }
            catch (Exception inner) when (inner is InvalidCastException or FormatException)
            {
                return null;
            }
        }
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
                    // fallback NULLs TenantId just like the PostgreSQL/SQL Server bulk paths do. It's also
                    // skipped when any row carries an explicit CreatedAt/UpdatedAt (a v2 backup restore), since
                    // the native paths stamp the current time — the multi-row INSERT binds the exported
                    // timestamps per row instead.
                    var hasTimestamps = false;
                    foreach (var r in rows)
                    {
                        if (r.CreatedAt.HasValue || r.UpdatedAt.HasValue)
                        {
                            hasTimestamps = true;
                            break;
                        }
                    }
                    var canBulkCopy = this.provider.SupportsBulkCopy
                        && !hasTimestamps
                        && (this.tenantIdAccessor == null || this.provider.SupportsBulkCopyWithTenant);
                    if (options.Mode == BulkWriteMode.Insert && canBulkCopy)
                    {
                        try
                        {
                            affected = await this.provider.BulkCopyInsertAsync(session.Connection, tx, table, docType, rows, DateTimeOffset.UtcNow, cancellationToken).ConfigureAwait(false);
                        }
                        catch (Exception ex) when (this.provider.IsDuplicateKeyException(ex))
                        {
                            // Match the multi-row Insert path's friendly error shape instead of leaking the raw
                            // provider exception from the native bulk-copy branch.
                            throw new InvalidOperationException(
                                $"A document of type '{docType}' has a duplicate Id in the import chunk.", ex);
                        }
                    }
                    else
                    {
                        affected = await this.BulkWriteChunkAsync(session.Connection, tx, table, docType, rows, options.Mode, cancellationToken).ConfigureAwait(false);
                    }

                    // Restore blob payloads that travelled inline (v2 backup) into the sidecar, in the same
                    // transaction as their document rows. Skipped when the target store doesn't map blobs for
                    // this table (the sidecar wouldn't exist) or the provider has no blob support.
                    if (this.provider.MaxBlobSize > 0 && TableHasBlobMapping(this.options, table))
                        await this.RestoreChunkBlobsAsync(session.Connection, tx, table, docType, rows, cancellationToken).ConfigureAwait(false);

                    if (ownTx)
                        await tx.CommitAsync(cancellationToken).ConfigureAwait(false);

                    // Replace and Merge write every row by intent; the driver's affected-row count is unreliable
                    // for upserts (MySQL's ON DUPLICATE KEY UPDATE counts an update as 2 rows, which would make
                    // DocumentsSkipped negative), so derive the tallies from row intent for those modes. Insert
                    // (all-or-throw) and SkipExisting (affected = rows actually inserted) use the real count.
                    if (options.Mode is BulkWriteMode.Replace or BulkWriteMode.Merge)
                    {
                        written += rows.Count;
                    }
                    else
                    {
                        written += affected;
                        skipped += rows.Count - affected;
                    }
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
                    rows.Add(new RawBulkRow(doc.Id, Encoding.UTF8.GetString(doc.Data.Span), doc.CreatedAt, doc.UpdatedAt, doc.Blobs));
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

    async Task RestoreChunkBlobsAsync(
        DbConnection connection, DbTransaction transaction, string table, string typeName, List<RawBulkRow> rows, CancellationToken ct)
    {
        foreach (var row in rows)
        {
            if (row.Blobs == null)
                continue;
            foreach (var b in row.Blobs)
            {
                await using var cmd = connection.CreateCommand();
                cmd.Transaction = transaction;
                cmd.CommandText = this.provider.BuildBlobUpsertSql(table);
                AddParameter(cmd, "@id", row.Id);
                AddParameter(cmd, "@typeName", typeName);
                AddParameter(cmd, "@blobKey", b.Key);
                AddBinaryParameter(cmd, "@data", b.Data);
                AddParameter(cmd, "@length", b.Length);
                AddParameter(cmd, "@contentType", b.ContentType);
                AddParameter(cmd, "@fileName", b.FileName);
                AddParameter(cmd, "@hash", b.Hash);
                AddParameter(cmd, "@createdAt", b.CreatedAt);
                AddParameter(cmd, "@updatedAt", b.UpdatedAt);
                this.Log(cmd.CommandText);
                await cmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
            }
        }
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

        // Insert mode uses the backup-insert SQL, which binds CreatedAt/UpdatedAt per row (@ca_i / @ua_i) so an
        // exported v2 backup preserves the original timestamps; a v1 row (no timestamps) falls back to @now.
        // Replace/SkipExisting/Merge keep the shared @now — they target existing rows, not fresh inserts.
        var now = DateTimeOffset.UtcNow;
        var sql = mode switch
        {
            BulkWriteMode.Insert => this.provider.BuildBackupInsertSql(table, rows.Count),
            BulkWriteMode.Replace => this.provider.BuildBatchReplaceSql(table, rows.Count),
            BulkWriteMode.SkipExisting => this.provider.BuildBatchSkipExistingSql(table, rows.Count),
            BulkWriteMode.Merge => this.provider.BuildBatchUpsertSql(table, rows.Count),
            _ => throw new ArgumentOutOfRangeException(nameof(mode), mode, null)
        };

        await using var cmd = connection.CreateCommand();
        cmd.Transaction = transaction;
        cmd.CommandText = sql;
        AddParameter(cmd, "@typeName", typeName);
        if (mode != BulkWriteMode.Insert)
            AddParameter(cmd, "@now", now);
        for (var i = 0; i < rows.Count; i++)
        {
            AddParameter(cmd, $"@id_{i}", rows[i].Id);
            AddParameter(cmd, $"@data_{i}", rows[i].Data);
            if (mode == BulkWriteMode.Insert)
            {
                AddParameter(cmd, $"@ca_{i}", rows[i].CreatedAt ?? now);
                AddParameter(cmd, $"@ua_{i}", rows[i].UpdatedAt ?? now);
            }
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
