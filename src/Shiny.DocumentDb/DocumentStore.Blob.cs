using System.Data.Common;
using System.Security.Cryptography;
using Shiny.DocumentDb.Internal;

namespace Shiny.DocumentDb;

// Blob payloads live in the per-table sidecar (see IDatabaseProvider.BlobTableName) so document reads never
// materialize them. Only metadata rides along in the document body, written by DocumentBlobJsonConverter.
//
// Write order matters: PrepareBlobs stamps Key/Length/Hash onto the live document BEFORE serialization —
// otherwise the metadata persisted in the body would not describe the rows written to the sidecar.
public partial class DocumentStore : IBlobDocumentStore
{
    /// <inheritdoc />
    public long MaxBlobSize => this.provider.MaxBlobSize;

    internal bool HasBlobMappings(Type documentType)
        => this.options.ResolveBlobMappings(documentType).Count > 0;

    /// <summary>
    /// Stamps keys, lengths and (when configured) hashes onto every blob carried by the document, enforcing
    /// the size ceiling, and returns the full set of blobs the document now claims. Must run before the
    /// document is serialized.
    /// </summary>
    internal IReadOnlyList<PreparedBlob> PrepareBlobs(Type documentType, object document)
    {
        var mappings = this.options.ResolveBlobMappings(documentType);
        if (mappings.Count == 0)
            return Array.Empty<PreparedBlob>();

        if (this.provider.MaxBlobSize == 0)
            throw new NotSupportedException(
                $"'{documentType.Name}' maps blob properties but this provider does not support blobs (MaxBlobSize is 0).");

        var prepared = new List<PreparedBlob>();
        foreach (var mapping in mappings)
        {
            var value = mapping.GetValue(document);
            if (value == null)
                continue;

            if (!mapping.IsCollection)
            {
                var blob = (DocumentBlob)value;
                this.Stamp(mapping, blob, mapping.Key!);
                prepared.Add(new PreparedBlob(blob.Key, blob));
            }
            else
            {
                foreach (var blob in (IList<DocumentBlob>)value)
                {
                    if (blob == null)
                        continue;

                    // Collection items own their keys so reordering the list does not re-point sidecar rows.
                    var key = string.IsNullOrWhiteSpace(blob.Key) ? Guid.CreateVersion7().ToString("N") : blob.Key;
                    this.Stamp(mapping, blob, key);
                    prepared.Add(new PreparedBlob(blob.Key, blob));
                }
            }
        }

        var duplicate = prepared.GroupBy(x => x.Key, StringComparer.OrdinalIgnoreCase).FirstOrDefault(g => g.Count() > 1);
        if (duplicate != null)
            throw new InvalidOperationException(
                $"Two blobs on '{documentType.Name}' resolve to the same key '{duplicate.Key}'. Give collection items distinct keys.");

        return prepared;
    }

    void Stamp(BlobMapping mapping, DocumentBlob blob, string key)
    {
        blob.Key = key;

        if (blob.IsPendingWrite)
        {
            var payload = blob.RawBytes!;
            var ceiling = mapping.MaxSize > 0 && mapping.MaxSize < this.provider.MaxBlobSize
                ? mapping.MaxSize
                : this.provider.MaxBlobSize;

            if (payload.LongLength > ceiling)
                throw new NotSupportedException(
                    $"Blob '{key}' is {payload.LongLength:N0} bytes, exceeding the {ceiling:N0} byte limit for this provider/mapping.");

            blob.Length = payload.LongLength;
            blob.UpdatedAt = DateTimeOffset.UtcNow;

            if (mapping.ComputeHash)
                blob.Hash = Convert.ToHexString(SHA256.HashData(payload)).ToLowerInvariant();
        }
    }

    /// <summary>
    /// Writes pending payloads to the sidecar and, when <paramref name="prune"/> is set, drops rows for keys
    /// the document no longer claims. Runs on the caller's session so it joins the surrounding transaction.
    /// </summary>
    internal async Task BlobSyncAsync(
        DocumentStoreSession session,
        Type documentType,
        string tableName,
        string id,
        string typeName,
        IReadOnlyList<PreparedBlob> blobs,
        bool prune,
        CancellationToken ct)
    {
        if (!this.HasBlobMappings(documentType) || this.provider.MaxBlobSize == 0)
            return;

        foreach (var (key, blob) in blobs)
        {
            // Only blobs carrying new bytes are rewritten — a Get → Upsert round trip must not resend payloads.
            if (!blob.IsPendingWrite)
                continue;

            await using var cmd = session.CreateCommand();
            cmd.CommandText = this.provider.BuildBlobUpsertSql(tableName);
            AddParameter(cmd, "@id", id);
            AddParameter(cmd, "@typeName", typeName);
            AddParameter(cmd, "@blobKey", key);
            AddBinaryParameter(cmd, "@data", blob.RawBytes!);
            AddParameter(cmd, "@length", blob.Length);
            AddParameter(cmd, "@contentType", blob.ContentType);
            AddParameter(cmd, "@fileName", blob.FileName);
            AddParameter(cmd, "@hash", blob.Hash);
            AddParameter(cmd, "@createdAt", blob.CreatedAt);
            AddParameter(cmd, "@updatedAt", blob.UpdatedAt);
            this.Log(cmd.CommandText);
            await cmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);

            blob.IsPendingWrite = false;
        }

        if (!prune)
            return;

        var keptParams = new List<string>(blobs.Count);
        await using var pruneCmd = session.CreateCommand();
        for (var i = 0; i < blobs.Count; i++)
        {
            var name = "@keep" + i;
            keptParams.Add(name);
            AddParameter(pruneCmd, name, blobs[i].Key);
        }
        pruneCmd.CommandText = this.provider.BuildBlobDeleteMissingSql(tableName, keptParams);
        AddParameter(pruneCmd, "@id", id);
        AddParameter(pruneCmd, "@typeName", typeName);
        this.Log(pruneCmd.CommandText);
        await pruneCmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
    }

    /// <summary>Cascade: drops every payload belonging to a document that is being removed.</summary>
    internal async Task BlobDeleteAllAsync(
        DocumentStoreSession session, Type documentType, string tableName, string id, string typeName, CancellationToken ct)
    {
        if (!this.HasBlobMappings(documentType) || this.provider.MaxBlobSize == 0)
            return;

        await using var cmd = session.CreateCommand();
        cmd.CommandText = this.provider.BuildBlobDeleteAllSql(tableName);
        AddParameter(cmd, "@id", id);
        AddParameter(cmd, "@typeName", typeName);
        this.Log(cmd.CommandText);
        await cmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
    }

    /// <summary>Bulk cascade: drops every payload for a type when its documents are cleared.</summary>
    internal async Task BlobClearAsync(
        DocumentStoreSession session, Type documentType, string tableName, string typeName, CancellationToken ct)
    {
        if (!this.HasBlobMappings(documentType) || this.provider.MaxBlobSize == 0)
            return;

        await using var cmd = session.CreateCommand();
        cmd.CommandText = $"DELETE FROM {this.provider.QuoteTable(this.provider.BlobTableName(tableName))} WHERE TypeName = @typeName";
        AddParameter(cmd, "@typeName", typeName);
        this.Log(cmd.CommandText);
        await cmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
    }

    // ── IBlobDocumentStore ─────────────────────────────────────────────────

    /// <summary>
    /// Stamps a per-document loader onto every blob a freshly-materialized document carries, so each can
    /// self-load. Cheap dictionary miss for non-blob types.
    /// </summary>
    internal void AttachBlobLoaders<T>(T document) where T : class
    {
        var mappings = this.options.ResolveBlobMappings(typeof(T));
        if (mappings.Count == 0 || document == null)
            return;

        var docId = this.idCache.GetOrCreate<T>(null).GetIdAsString(document);
        var loader = new DocumentBlobLoader(this, this.ResolveTableName<T>(), this.ResolveTypeName<T>(), docId);

        foreach (var mapping in mappings)
        {
            var value = mapping.GetValue(document);
            if (value == null)
                continue;

            if (mapping.IsCollection)
                ((DocumentBlobCollection)value).StampLoader(loader);
            else
                ((DocumentBlob)value).Loader = loader;
        }
    }

    /// <inheritdoc />
    public Task BatchLoadBlobs<T>(IEnumerable<T> documents, CancellationToken cancellationToken = default) where T : class
    {
        ArgumentNullException.ThrowIfNull(documents);
        return this.LoadBlobsCoreAsync(documents, cancellationToken);
    }

    async Task LoadBlobsCoreAsync<T>(IEnumerable<T> documents, CancellationToken cancellationToken) where T : class
    {
        var mappings = this.options.ResolveBlobMappings(typeof(T));
        if (mappings.Count == 0)
            throw new InvalidOperationException(
                $"'{typeof(T).Name}' has no mapped blob properties. Call MapBlob<{typeof(T).Name}>(…) at startup.");

        if (this.provider.MaxBlobSize == 0)
            throw new NotSupportedException("Blobs are not supported by this provider.");

        // index every unloaded blob by (document id, blob key) so one pass over the result set can attach them
        var accessor = this.idCache.GetOrCreate<T>(null);
        var wanted = new Dictionary<(string Id, string Key), List<DocumentBlob>>();
        foreach (var document in documents)
        {
            if (document == null)
                continue;

            var docId = accessor.GetIdAsString(document);
            foreach (var mapping in mappings)
            {
                foreach (var blob in EnumerateBlobs(mapping, document))
                {
                    if (blob.IsLoaded || string.IsNullOrEmpty(blob.Key))
                        continue;

                    var key = (docId, blob.Key);
                    if (!wanted.TryGetValue(key, out var list))
                        wanted[key] = list = new List<DocumentBlob>();
                    list.Add(blob);
                }
            }
        }
        if (wanted.Count == 0)
            return;

        var ids = wanted.Keys.Select(k => k.Id).Distinct().ToList();
        var tableName = this.ResolveTableName<T>();
        var typeName = this.ResolveTypeName<T>();

        await this.ExecuteAsync(tableName, async session =>
        {
            await using var cmd = session.CreateCommand();
            var idParams = new List<string>(ids.Count);
            for (var i = 0; i < ids.Count; i++)
            {
                var name = "@id" + i;
                idParams.Add(name);
                AddParameter(cmd, name, ids[i]);
            }
            cmd.CommandText = this.provider.BuildBlobReadBatchSql(tableName, idParams);
            AddParameter(cmd, "@typeName", typeName);
            this.Log(cmd.CommandText);

            await using var reader = await cmd.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
            while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            {
                var rowId = reader.GetString(0);
                var rowKey = reader.GetString(1);
                if (!wanted.TryGetValue((rowId, rowKey), out var targets))
                    continue;

                var payload = ReadBinary(reader, 2);
                if (payload == null)
                    continue;

                foreach (var blob in targets)
                    blob.Attach(payload);
            }
        }, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task<byte[]?> GetBlob<T>(object id, string key, CancellationToken cancellationToken = default)
        where T : class
    {
        ArgumentNullException.ThrowIfNull(id);
        ArgumentException.ThrowIfNullOrWhiteSpace(key);

        if (this.provider.MaxBlobSize == 0)
            throw new NotSupportedException("Blobs are not supported by this provider.");

        var tableName = this.ResolveTableName<T>();
        var typeName = this.ResolveTypeName<T>();
        var resolvedId = this.idCache.GetOrCreate<T>(null).ResolveId(id);

        return await this.ExecuteAsync(tableName, async session =>
        {
            await using var cmd = session.CreateCommand();
            cmd.CommandText = this.provider.BuildBlobReadSql(tableName);
            AddParameter(cmd, "@id", resolvedId);
            AddParameter(cmd, "@typeName", typeName);
            AddParameter(cmd, "@blobKey", key);
            this.Log(cmd.CommandText);

            await using var reader = await cmd.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
            return await reader.ReadAsync(cancellationToken).ConfigureAwait(false)
                ? ReadBinary(reader, 0)
                : null;
        }, cancellationToken).ConfigureAwait(false);
    }

    // ── self-load primitives (called by DocumentBlobLoader) ─────────────────

    async Task<byte[]?> LoadOneBlobAsync(string tableName, string typeName, string docId, string blobKey, CancellationToken ct)
        => await this.ExecuteAsync(tableName, async session =>
        {
            await using var cmd = session.CreateCommand();
            cmd.CommandText = this.provider.BuildBlobReadSql(tableName);
            AddParameter(cmd, "@id", docId);
            AddParameter(cmd, "@typeName", typeName);
            AddParameter(cmd, "@blobKey", blobKey);
            this.Log(cmd.CommandText);

            await using var reader = await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false);
            return await reader.ReadAsync(ct).ConfigureAwait(false) ? ReadBinary(reader, 0) : null;
        }, ct).ConfigureAwait(false);

    async Task LoadBlobsForKeysAsync(string tableName, string typeName, string docId, IReadOnlyList<DocumentBlob> blobs, CancellationToken ct)
    {
        var byKey = new Dictionary<string, List<DocumentBlob>>(StringComparer.Ordinal);
        foreach (var blob in blobs)
        {
            if (blob.IsLoaded || string.IsNullOrEmpty(blob.Key))
                continue;
            if (!byKey.TryGetValue(blob.Key, out var list))
                byKey[blob.Key] = list = new List<DocumentBlob>();
            list.Add(blob);
        }
        if (byKey.Count == 0)
            return;

        await this.ExecuteAsync(tableName, async session =>
        {
            await using var cmd = session.CreateCommand();
            var keyParams = new List<string>(byKey.Count);
            var i = 0;
            foreach (var key in byKey.Keys)
            {
                var name = "@k" + i++;
                keyParams.Add(name);
                AddParameter(cmd, name, key);
            }
            cmd.CommandText = this.provider.BuildBlobReadForKeysSql(tableName, keyParams);
            AddParameter(cmd, "@id", docId);
            AddParameter(cmd, "@typeName", typeName);
            this.Log(cmd.CommandText);

            await using var reader = await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false);
            while (await reader.ReadAsync(ct).ConfigureAwait(false))
            {
                if (!byKey.TryGetValue(reader.GetString(0), out var targets))
                    continue;
                var payload = ReadBinary(reader, 1);
                if (payload == null)
                    continue;
                foreach (var blob in targets)
                    blob.Attach(payload);
            }
        }, ct).ConfigureAwait(false);
    }

    // ── helpers ────────────────────────────────────────────────────────────

    static IEnumerable<DocumentBlob> EnumerateBlobs(BlobMapping mapping, object document)
    {
        var value = mapping.GetValue(document);
        if (value == null)
            yield break;

        if (!mapping.IsCollection)
        {
            yield return (DocumentBlob)value;
            yield break;
        }

        foreach (var blob in (IList<DocumentBlob>)value)
        {
            if (blob != null)
                yield return blob;
        }
    }

    static byte[]? ReadBinary(DbDataReader reader, int ordinal)
        => reader.IsDBNull(ordinal) ? null : (byte[])reader.GetValue(ordinal);

    static void AddBinaryParameter(DbCommand cmd, string name, byte[] value)
    {
        var p = cmd.CreateParameter();
        p.ParameterName = name;
        p.DbType = System.Data.DbType.Binary;
        p.Value = value;
        cmd.Parameters.Add(p);
    }

    /// <summary>Per-document loader stamped onto blobs at hydration; serves every blob on one document.</summary>
    sealed class DocumentBlobLoader(DocumentStore store, string tableName, string typeName, string docId) : IBlobLoader
    {
        public Task<byte[]?> LoadOneAsync(string blobKey, CancellationToken ct)
            => store.LoadOneBlobAsync(tableName, typeName, docId, blobKey, ct);

        public Task LoadManyAsync(IReadOnlyList<DocumentBlob> blobs, CancellationToken ct)
            => store.LoadBlobsForKeysAsync(tableName, typeName, docId, blobs, ct);
    }
}

/// <summary>A blob resolved from a live document together with the sidecar key it will be written under.</summary>
internal readonly record struct PreparedBlob(string Key, DocumentBlob Blob);
