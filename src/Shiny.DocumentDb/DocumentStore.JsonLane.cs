using System.Collections.Concurrent;
using System.Data.Common;
using System.Linq.Expressions;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.Json.Serialization.Metadata;
using Shiny.DocumentDb.Internal;
using Shiny.DocumentDb.Internal.Query;

namespace Shiny.DocumentDb;

public partial class DocumentStore
{
    // Update = full replace (update-only); UpdateMerge = RFC 7396 merge (update-only);
    // Upsert = merge-or-insert; UpsertReplace = replace-or-insert.
    enum JsonWriteKind { Insert, Update, Upsert, UpdateMerge, UpsertReplace }

    readonly ConcurrentDictionary<Type, JsonLaneIdAccessor> jsonIdAccessors = new();

    // ── Public late-bound JSON lane ─────────────────────────────────────

    public Task<int> Insert(Type type, JsonNode document, CancellationToken cancellationToken = default)
        => this.tracker.Track("insert", type.Name, () => this.WriteJsonAsync(type, document, JsonWriteKind.Insert, cancellationToken), r => r);

    public Task<int> Update(Type type, JsonNode document, CancellationToken cancellationToken = default)
        => this.tracker.Track("update", type.Name, () => this.WriteJsonAsync(type, document, JsonWriteKind.Update, cancellationToken), r => r);

    public Task<int> Update(Type type, JsonNode document, bool patch, CancellationToken cancellationToken = default)
        => this.tracker.Track(patch ? "update_patch" : "update", type.Name, () => this.WriteJsonAsync(type, document, patch ? JsonWriteKind.UpdateMerge : JsonWriteKind.Update, cancellationToken), r => r);

    public Task<int> Upsert(Type type, JsonNode document, CancellationToken cancellationToken = default)
        => this.tracker.Track("upsert", type.Name, () => this.WriteJsonAsync(type, document, JsonWriteKind.Upsert, cancellationToken), r => r);

    public Task<int> Upsert(Type type, JsonNode document, bool patchIfUpdate, CancellationToken cancellationToken = default)
        => this.tracker.Track(patchIfUpdate ? "upsert" : "upsert_replace", type.Name, () => this.WriteJsonAsync(type, document, patchIfUpdate ? JsonWriteKind.Upsert : JsonWriteKind.UpsertReplace, cancellationToken), r => r);

    public Task<JsonNode?> Get(Type type, object id, CancellationToken cancellationToken = default)
        => this.tracker.Track("get", type.Name, () => this.GetJsonImpl(type, id, cancellationToken), r => r is null ? 0 : 1);

    Task<JsonNode?> GetJsonImpl(Type type, object id, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(type);
        ArgumentNullException.ThrowIfNull(id);
        var typeName = this.ResolveTypeName(type);
        var tableName = this.ResolveTableName(type);
        var resolvedId = this.GetJsonIdAccessor(type).ResolveId(id);
        return this.ExecuteAsync(tableName, async session =>
        {
            await using var cmd = session.CreateCommand();
            var sql = $"SELECT Data FROM {Qt(tableName)} WHERE Id = @id AND TypeName = @typeName";
            sql += GetTenantFilter() ?? "";
            cmd.CommandText = sql + ";";
            AddParameter(cmd, "@id", resolvedId);
            AddParameter(cmd, "@typeName", typeName);
            this.AddTenantParam(cmd);
            this.AppendGlobalFilters(cmd, type);

            this.Log(cmd.CommandText);
            var result = await cmd.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
            return result is string json ? JsonNode.Parse(json) : null;
        }, cancellationToken);
    }

    public Task<IReadOnlyList<JsonNode>> Query(Type type, string whereClause, object? parameters = null, CancellationToken cancellationToken = default)
        => this.tracker.Track("query", type.Name, () => this.QueryJsonImpl(type, whereClause, parameters, cancellationToken), r => r.Count);

    Task<IReadOnlyList<JsonNode>> QueryJsonImpl(Type type, string whereClause, object? parameters, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(type);
        var typeName = this.ResolveTypeName(type);
        var tableName = this.ResolveTableName(type);
        return this.ExecuteAsync(tableName, async session =>
        {
            await using var cmd = session.CreateCommand();
            var sql = $"SELECT Data FROM {Qt(tableName)} WHERE TypeName = @typeName{GetTenantFilter() ?? ""} AND ({whereClause})";
            cmd.CommandText = sql + ";";
            AddParameter(cmd, "@typeName", typeName);
            this.AddTenantParam(cmd);
            BindParameters(cmd, parameters);

            this.Log(cmd.CommandText);
            return await ReadListAsync(cmd, json => (JsonNode)JsonNode.Parse(json)!, cancellationToken).ConfigureAwait(false);
        }, cancellationToken);
    }

    public IAsyncEnumerable<JsonNode> QueryStream(Type type, string whereClause, object? parameters = null, CancellationToken cancellationToken = default)
        => this.tracker.TrackStream("query_stream", type.Name, this.QueryStreamJsonImpl(type, whereClause, parameters, cancellationToken), cancellationToken);

    IAsyncEnumerable<JsonNode> QueryStreamJsonImpl(Type type, string whereClause, object? parameters, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(type);
        var typeName = this.ResolveTypeName(type);
        var tableName = this.ResolveTableName(type);
        return this.ReadStreamAsync(
            tableName,
            cmd =>
            {
                var sql = $"SELECT Data FROM {Qt(tableName)} WHERE TypeName = @typeName{GetTenantFilter() ?? ""} AND ({whereClause})";
                cmd.CommandText = sql + ";";
                AddParameter(cmd, "@typeName", typeName);
                this.AddTenantParam(cmd);
                BindParameters(cmd, parameters);
            },
            json => (JsonNode)JsonNode.Parse(json)!,
            cancellationToken);
    }

    // ── Write dispatch ──────────────────────────────────────────────────

    async Task<int> WriteJsonAsync(Type type, JsonNode document, JsonWriteKind kind, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(type);
        ArgumentNullException.ThrowIfNull(document);

        var typeName = this.ResolveTypeName(type);
        var tableName = this.ResolveTableName(type);
        var versionMapping = this.options.ResolveVersionMapping(type);
        var spatialMapping = this.options.ResolveSpatialMapping(type);
        var vectorMapping = this.options.ResolveVectorMapping(type);
        var idAccessor = this.GetJsonIdAccessor(type);

        if (document is JsonObject singleObj)
        {
            // The node is written in place: a generated Id (and bumped version) is injected onto the
            // caller's object, mirroring the typed Insert<T>/Update<T> contract.
            string? publishedId = null;
            await this.ExecuteAsync(tableName, async session =>
            {
                var written = await this.WriteOneJsonAsync(session, type, typeName, tableName, singleObj, kind, idAccessor, versionMapping, spatialMapping, vectorMapping, ct).ConfigureAwait(false);
                publishedId = written?.Id;
                return 0;
            }, ct).ConfigureAwait(false);
            // publishedId stays null when an interceptor cancelled the write — nothing was written, so nothing
            // is published and the document is not counted.
            if (publishedId == null)
                return 0;
            this.PublishJsonChange(type, kind, publishedId, singleObj);
            return 1;
        }

        if (document is JsonArray array)
        {
            var elements = new List<JsonObject>(array.Count);
            foreach (var element in array)
            {
                if (element is not JsonObject o)
                    throw new ArgumentException(
                        "Every element of a JsonArray passed to the JSON write lane must be a JsonObject.");
                elements.Add(o);
            }

            var written = new List<(string Id, JsonObject Obj)>(elements.Count);
            await this.ExecuteAsync(tableName, async session =>
            {
                await using var transaction = await session.Connection.BeginTransactionAsync(ct).ConfigureAwait(false);
                try
                {
                    var txSession = new DocumentStoreSession(session.Connection, transaction);
                    foreach (var obj in elements)
                    {
                        var result = await this.WriteOneJsonAsync(txSession, type, typeName, tableName, obj, kind, idAccessor, versionMapping, spatialMapping, vectorMapping, ct).ConfigureAwait(false);
                        if (result != null)
                            written.Add((result.Value.Id, obj));
                    }
                    await transaction.CommitAsync(ct).ConfigureAwait(false);
                }
                catch
                {
                    await transaction.RollbackAsync(ct).ConfigureAwait(false);
                    throw;
                }
                return 0;
            }, ct).ConfigureAwait(false);

            foreach (var (id, obj) in written)
                this.PublishJsonChange(type, kind, id, obj);
            return written.Count;
        }

        throw new ArgumentException(
            $"{kind}(Type, JsonNode) requires a JsonObject (one document) or JsonArray (many). Received a {document.GetType().Name}.");
    }

    // Null when an interceptor cancelled this document's write (nothing written, nothing published, not counted).
    async Task<(string Id, int? Version)?> WriteOneJsonAsync(
        DocumentStoreSession session, Type type, string typeName, string tableName,
        JsonObject obj, JsonWriteKind kind,
        JsonLaneIdAccessor idAccessor, VersionMapping? versionMapping,
        SpatialMapping? spatialMapping, VectorMapping? vectorMapping,
        CancellationToken ct)
    {
        // Mode predicates: update-only (no insert) vs upsert (insert-when-absent), and merge vs replace.
        var isUpdateMode = kind is JsonWriteKind.Update or JsonWriteKind.UpdateMerge;
        var isUpsertMode = kind is JsonWriteKind.Upsert or JsonWriteKind.UpsertReplace;
        var isMerge = kind is JsonWriteKind.Upsert or JsonWriteKind.UpdateMerge;

        var op = kind switch
        {
            JsonWriteKind.Insert => DocumentOperation.Insert,
            _ when isUpdateMode => DocumentOperation.Update,
            _ => DocumentOperation.Upsert
        };
        // Interceptors fire with Document == null (no CLR instance); GetJson()/GetJsonDocument() expose the
        // supplied body. Object-mutating interceptors are a no-op on this lane.
        var ctx = this.NewJsonWriteContext(op, typeName, type, obj.ToJsonString(this.jsonOptions));
        if (!await this.RunBeforeWriteAsync(ctx, ct).ConfigureAwait(false))
            return null;

        var isDefaultId = idAccessor.IsDefaultId(obj);

        // Mapped-property presence: Insert/Update always; Upsert only when there is no Id (guaranteed insert).
        if (!isUpsertMode || isDefaultId)
            ValidateMappedPresence(obj, typeName, spatialMapping, vectorMapping);

        string id;
        if (isDefaultId)
        {
            if (isUpdateMode)
                throw new InvalidOperationException(
                    $"Update requires a non-default Id on the '{typeName}' document.");
            if (idAccessor.Kind == IdKind.String)
                throw new InvalidOperationException(
                    $"Insert requires a non-empty string Id on '{typeName}'. String Id properties are not auto-generated.");

            id = await this.GenerateIdAsync(session, idAccessor.Kind, tableName, typeName, ct).ConfigureAwait(false);
            idAccessor.WriteId(obj, id);
        }
        else
        {
            id = idAccessor.ReadStorageId(obj)!;
        }

        int? expectedVersion = null;
        int? newVersion = null;
        if (versionMapping != null)
        {
            var member = versionMapping.JsonPath;
            switch (kind)
            {
                case JsonWriteKind.Insert:
                    JsonLaneNodes.WriteVersion(obj, member, 1);
                    newVersion = 1;
                    break;

                case JsonWriteKind.Update:
                    var expected = JsonLaneNodes.ReadVersion(obj, member);
                    JsonLaneNodes.WriteVersion(obj, member, expected + 1);
                    expectedVersion = expected;
                    newVersion = expected + 1;
                    break;

                case JsonWriteKind.UpdateMerge:
                    // A partial merge that carries a version (> 0) does optimistic concurrency: CAS on it and
                    // bump. A partial JsonObject with no version leaves the key absent — don't CAS, and don't
                    // write a version into the patch (that would clobber the stored value through the merge).
                    var expectedMerge = JsonLaneNodes.ReadVersion(obj, member);
                    if (expectedMerge > 0)
                    {
                        JsonLaneNodes.WriteVersion(obj, member, expectedMerge + 1);
                        expectedVersion = expectedMerge;
                        newVersion = expectedMerge + 1;
                    }
                    break;

                case JsonWriteKind.Upsert or JsonWriteKind.UpsertReplace:
                    var current = JsonLaneNodes.ReadVersion(obj, member);
                    newVersion = current > 0 ? current + 1 : 1;
                    JsonLaneNodes.WriteVersion(obj, member, newVersion.Value);
                    expectedVersion = current > 0 ? current : null;
                    break;
            }
        }

        var json = obj.ToJsonString(this.jsonOptions);

        switch (kind)
        {
            case JsonWriteKind.Insert:
                await this.InsertCoreAsync(session, tableName, id, typeName, json, ct).ConfigureAwait(false);
                break;

            case JsonWriteKind.Update:
                await this.UpdateCoreAsync(session, tableName, id, typeName, json, expectedVersion, versionMapping?.JsonPath, cmd => this.AppendGlobalFilters(cmd, type), ct).ConfigureAwait(false);
                break;

            case JsonWriteKind.Upsert:
                await this.UpsertMergeCoreAsync(session, tableName, id, typeName, json, expectedVersion, versionMapping?.JsonPath, ct).ConfigureAwait(false);
                break;

            case JsonWriteKind.UpdateMerge:
                await this.MergeOrReplaceCoreAsync(session, tableName, id, typeName, json, expectedVersion, versionMapping?.JsonPath, merge: true, insertIfMissing: false, ct).ConfigureAwait(false);
                break;

            case JsonWriteKind.UpsertReplace:
                await this.MergeOrReplaceCoreAsync(session, tableName, id, typeName, json, expectedVersion, versionMapping?.JsonPath, merge: false, insertIfMissing: true, ct).ConfigureAwait(false);
                break;
        }

        await this.SpatialUpsertFromNodeAsync(session, tableName, id, typeName, spatialMapping, obj, ct).ConfigureAwait(false);
        await this.VectorUpsertFromNodeAsync(session, tableName, typeName, id, vectorMapping, obj, ct).ConfigureAwait(false);

        var temporalOp = kind == JsonWriteKind.Insert ? TemporalOperation.Inserted : TemporalOperation.Updated;
        // Merge modes change the row in place; pass null so history reads back the post-merge document.
        var providedJson = isMerge ? null : json;
        await this.AppendHistoryAsync(session, type, tableName, id, typeName, temporalOp, providedJson, ct).ConfigureAwait(false);

        await this.RunAfterWriteAsync(ctx, id, newVersion, ct).ConfigureAwait(false);
        return (id, newVersion);
    }

    static void ValidateMappedPresence(JsonObject obj, string typeName, SpatialMapping? spatial, VectorMapping? vector)
    {
        List<string>? missing = null;
        if (spatial != null && JsonLaneNodes.IsMemberMissing(obj, spatial.JsonPath))
            (missing ??= new()).Add(spatial.JsonPath);
        if (vector != null && JsonLaneNodes.IsMemberMissing(obj, vector.JsonPath))
            (missing ??= new()).Add(vector.JsonPath);

        if (missing == null)
            return;

        var noun = missing.Count == 1 ? "property" : "properties";
        throw new InvalidOperationException(
            $"Document of type '{typeName}' is missing mapped {noun}: {string.Join(", ", missing)}. " +
            "The JSON write lane stores the body AS-IS, so every mapped property must be present " +
            "(use JSON null to indicate no value).");
    }

    async Task SpatialUpsertFromNodeAsync(DocumentStoreSession session, string tableName, string id, string typeName, SpatialMapping? mapping, JsonObject obj, CancellationToken ct)
    {
        if (mapping == null)
            return;
        var sql = this.provider.BuildSpatialUpsertSql(tableName);
        if (sql == null)
            return;

        obj.TryGetPropertyValue(mapping.JsonPath, out var member);
        if (member is null)
            return; // JSON null / missing → deliberate "no location", skip sidecar

        var geometry = member.Deserialize<Geometry>(this.jsonOptions);
        if (geometry is null)
            return;

        var envelope = geometry.GetEnvelope();
        await using var cmd = session.CreateCommand();
        cmd.CommandText = sql;
        AddParameter(cmd, "@spatialDocId", id);
        AddParameter(cmd, "@spatialTypeName", typeName);
        AddParameter(cmd, "@spatialMinLat", envelope.MinLatitude);
        AddParameter(cmd, "@spatialMaxLat", envelope.MaxLatitude);
        AddParameter(cmd, "@spatialMinLng", envelope.MinLongitude);
        AddParameter(cmd, "@spatialMaxLng", envelope.MaxLongitude);
        if (this.provider.RequiresSpatialWkt)
            AddParameter(cmd, "@spatialWkt", Internal.Spatial.WktWriter.ToWkt(geometry));
        if (this.provider.RequiresSpatialGeoJson)
            AddParameter(cmd, "@spatialGeoJson", Internal.Spatial.SpatialJson.ToGeoJson(geometry));
        this.Log(cmd.CommandText);
        await cmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
    }

    async Task VectorUpsertFromNodeAsync(DocumentStoreSession session, string tableName, string typeName, string id, VectorMapping? mapping, JsonObject obj, CancellationToken ct)
    {
        if (!this.provider.SupportsVector || mapping == null)
            return;

        obj.TryGetPropertyValue(mapping.JsonPath, out var member);
        var vec = JsonLaneNodes.ReadVector(member, mapping.JsonPath);
        if (vec.Length == 0)
            return; // JSON null / empty → skip, mirroring the typed empty-embedding rule

        if (vec.Length != mapping.Dimensions)
            throw new ArgumentException(
                $"Vector for document '{id}' of type '{typeName}' has {vec.Length} elements; expected {mapping.Dimensions}.");

        var sql = this.provider.BuildVectorUpsertSql(tableName, typeName, mapping);
        if (sql == null)
            return;

        await using var cmd = session.CreateCommand();
        cmd.CommandText = sql;
        AddParameter(cmd, "@vecDocId", id);
        AddParameter(cmd, "@vecTypeName", typeName);
        AddParameter(cmd, "@embedding", this.provider.FormatVectorParameter(vec, mapping));
        this.Log(cmd.CommandText);
        await cmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
    }

    void PublishJsonChange(Type type, JsonWriteKind kind, string id, JsonObject obj)
    {
        var changeType = kind == JsonWriteKind.Insert ? DocumentChangeType.Inserted : DocumentChangeType.Updated;
        this.broadcaster.Publish(type, changeType, id, obj.ToJsonString(this.jsonOptions), this.jsonOptions);
    }

    // ── Non-generic helpers ─────────────────────────────────────────────

    string ResolveTypeName(Type type) => TypeNameResolver.Resolve(type, this.options.TypeNameResolution);

    string ResolveTableName(Type type) => this.options.ResolveTableName(this.ResolveTypeName(type));

    JsonLaneIdAccessor GetJsonIdAccessor(Type type)
        => this.jsonIdAccessors.GetOrAdd(type, t =>
            JsonLaneIdAccessor.Create(t, this.options.ResolveIdPropertyName(t) ?? "Id", this.jsonOptions));

    DocumentWriteContext? NewJsonWriteContext(DocumentOperation op, string typeName, Type type, string rawJson)
    {
        var pipeline = this.options.Interceptors;
        if (!pipeline.HasPerDoc || DocumentOperationScope.Suppressed)
            return null;

        var (scope, interceptors) = pipeline.ResolveWrite(DocumentOperationScope.CurrentServices);
        if (interceptors.Count == 0)
            return null;

        return new DocumentWriteContext
        {
            Operation = op,
            Source = DocumentOperationScope.Current,
            DocumentType = type,
            TypeName = typeName,
            Id = null,
            Document = null,
            RawJson = rawJson,
            // On the late-bound JSON lane ctx.Store is the top-level store (committed-state / best-effort tier —
            // this lane is not routed through the implicit one-op unit), and ctx.Services is the ambient scope.
            Store = this,
            Services = scope,
            Interceptors = interceptors
        };
    }

    void AppendGlobalFilters(DbCommand cmd, Type type)
    {
        var filters = this.options.ResolveQueryFilters(type);
        if (filters.Count == 0)
            return;

        JsonTypeInfo info;
        try
        {
            info = this.jsonOptions.GetTypeInfo(type);
        }
        catch (Exception ex)
        {
            throw new InvalidOperationException(
                $"Global query filters for '{type.Name}' require a resolvable JsonTypeInfo. " +
                "Configure a JsonSerializerContext via DocumentStoreOptions.JsonSerializerOptions, or register the type.", ex);
        }

        var parameter = filters[0].Predicate.Parameters[0];
        Expression body = filters[0].Predicate.Body;
        for (var i = 1; i < filters.Count; i++)
        {
            var next = filters[i].Predicate;
            var replaced = new JsonLaneParameterReplacer(next.Parameters[0], parameter).Visit(next.Body);
            body = Expression.AndAlso(body, replaced);
        }

        var node = ExpressionLowerer.Lower(body, this.jsonOptions, info);
        var (clause, parms) = SqlPredicateEmitter.Emit(node, this.provider);

        var sql = cmd.CommandText.TrimEnd();
        var hasTrailingSemicolon = sql.EndsWith(';');
        if (hasTrailingSemicolon)
            sql = sql.Substring(0, sql.Length - 1).TrimEnd();
        cmd.CommandText = sql + $" AND ({clause})" + (hasTrailingSemicolon ? ";" : "");
        foreach (var kv in parms)
            AddParameter(cmd, kv.Key, kv.Value);
    }
}

sealed class JsonLaneParameterReplacer(ParameterExpression from, ParameterExpression to) : ExpressionVisitor
{
    protected override Expression VisitParameter(ParameterExpression node)
        => node == from ? to : base.VisitParameter(node);
}
