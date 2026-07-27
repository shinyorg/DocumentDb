using System.Collections.Concurrent;
using System.Data.Common;
using Shiny.DocumentDb.Internal;

namespace Shiny.DocumentDb;

public partial class DocumentStore
{
    /// <summary>Telemetry for the JSON-collection surface — the same tracker every other operation uses.</summary>
    internal Diagnostics.OperationTracker Tracker => this.tracker;

    readonly ConcurrentDictionary<(string Name, string IdProperty), IJsonDocumentCollection> namedCollections = new();
    readonly ConcurrentDictionary<Type, IJsonDocumentCollection> typedCollections = new();

    /// <inheritdoc />
    public IJsonDocumentCollection Collection(string name, string idProperty = "id")
    {
        // The only place a collection name is minted, so it is the choke point where the name is validated
        // before it can reach a WHERE literal or an index identifier.
        DynamicNames.ValidateCollection(name);
        ArgumentException.ThrowIfNullOrWhiteSpace(idProperty);
        return this.namedCollections.GetOrAdd((name, idProperty),
            key => new JsonDocumentCollection(this, this.BuildTarget(key.Name, key.IdProperty)));
    }

    /// <inheritdoc />
    public IJsonDocumentCollection Collection(Type type)
    {
        ArgumentNullException.ThrowIfNull(type);
        return this.typedCollections.GetOrAdd(type, t => new JsonDocumentCollection(this, this.BuildTarget(t)));
    }

    // ── Shared delete cores ─────────────────────────────────────────────
    // Type-free at the SQL layer; the sidecar and history helpers take the (nullable) document type, so a
    // schema-free collection simply has none to purge.

    /// <summary>
    /// Deletes one row and everything hanging off it. <paramref name="afterWrite"/> runs inside the same
    /// session as the delete, before it returns, so a transactional after-hook still sees the write.
    /// </summary>
    internal async Task<bool> RemoveRowCoreAsync(
        JsonLaneTarget target,
        string resolvedId,
        Action<DbCommand>? appendFilters,
        Func<Task>? afterWrite,
        CancellationToken ct)
    {
        return await this.ExecuteAsync(target.TableName, async session =>
        {
            await using var cmd = session.CreateCommand();
            var sql = $"DELETE FROM {Qt(target.TableName)} WHERE Id = @id AND TypeName = @typeName";
            sql += GetTenantFilter() ?? "";
            cmd.CommandText = sql + ";";
            AddParameter(cmd, "@id", resolvedId);
            AddParameter(cmd, "@typeName", target.TypeName);
            this.AddTenantParam(cmd);
            appendFilters?.Invoke(cmd);

            this.Log(cmd.CommandText);
            var rows = await cmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
            if (rows > 0)
            {
                if (target.DocumentType is { } documentType)
                {
                    await this.BlobDeleteAllAsync(session, documentType, target.TableName, resolvedId, target.TypeName, ct).ConfigureAwait(false);
                    await this.SpatialDeleteAsync(session, documentType, target.TableName, resolvedId, target.TypeName, ct).ConfigureAwait(false);
                    await this.VectorDeleteAsync(session, documentType, target.TableName, target.TypeName, resolvedId, ct).ConfigureAwait(false);
                    await this.AppendHistoryAsync(session, documentType, target.TableName, resolvedId, target.TypeName, TemporalOperation.Removed, null, ct).ConfigureAwait(false);
                }
                if (afterWrite != null)
                    await afterWrite().ConfigureAwait(false);
            }
            return rows > 0;
        }, ct).ConfigureAwait(false);
    }

    internal Task<int> BatchRemoveCoreAsync(JsonLaneTarget target, IReadOnlyList<string> resolvedIds, CancellationToken ct)
        => this.ExecuteAsync(target.TableName, async session =>
        {
            await using var transaction = await session.Connection.BeginTransactionAsync(ct).ConfigureAwait(false);
            try
            {
                DbCommand TxCreateCommand()
                {
                    var c = session.Connection.CreateCommand();
                    c.Transaction = transaction;
                    return c;
                }
                var n = await BatchDeleteByIdsCoreAsync(
                    target.TableName, target.TypeName, resolvedIds, TxCreateCommand, this.provider, this.logging, ct).ConfigureAwait(false);
                await transaction.CommitAsync(ct).ConfigureAwait(false);
                return n;
            }
            catch
            {
                await transaction.RollbackAsync(ct).ConfigureAwait(false);
                throw;
            }
        }, ct);

    internal Task<int> ClearCoreAsync(JsonLaneTarget target, Action<DbCommand>? appendFilters, CancellationToken ct)
        => this.ExecuteAsync(target.TableName, async session =>
        {
            await using var cmd = session.CreateCommand();
            var sql = $"DELETE FROM {Qt(target.TableName)} WHERE TypeName = @typeName";
            sql += GetTenantFilter() ?? "";
            cmd.CommandText = sql + ";";
            AddParameter(cmd, "@typeName", target.TypeName);
            this.AddTenantParam(cmd);
            appendFilters?.Invoke(cmd);

            this.Log(cmd.CommandText);
            var rows = await cmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
            if (rows > 0 && target.DocumentType is { } documentType)
            {
                await this.SpatialClearAsync(session, documentType, target.TableName, target.TypeName, ct).ConfigureAwait(false);
                await this.VectorClearAsync(session, documentType, target.TableName, target.TypeName, ct).ConfigureAwait(false);
                await this.BlobClearAsync(session, documentType, target.TableName, target.TypeName, ct).ConfigureAwait(false);
            }
            return rows;
        }, ct);

    internal Task CreateJsonIndexCoreAsync(JsonLaneTarget target, IReadOnlyList<string> jsonPaths, CancellationToken ct)
    {
        var indexName = IndexExpressionHelper.BuildIndexName(target.TypeName, jsonPaths);
        return this.ExecuteAsync(target.TableName, async session =>
        {
            await using var cmd = session.CreateCommand();
            cmd.CommandText = this.provider.BuildCreateJsonIndexSql(indexName, target.TableName, jsonPaths, target.TypeName);
            this.Log(cmd.CommandText);
            await cmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
        }, ct);
    }

    /// <summary>Builds a query bound to <paramref name="target"/> rather than to a CLR type.</summary>
    internal Internal.DocumentQuery<System.Text.Json.Nodes.JsonObject> BuildCollectionQuery(JsonLaneTarget target)
        => new(
            this,
            jsonTypeInfo: null,
            target.TypeName,
            target.TableName,
            target.TypeInfo,
            static json => System.Text.Json.Nodes.JsonNode.Parse(json)!.AsObject(),
            doc => target.Ids.ReadStorageId(doc));
}
