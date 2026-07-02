using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.Json.Serialization.Metadata;
using Azure;
using Azure.Data.Tables;
using Shiny.DocumentDb.Internal;
using Shiny.DocumentDb.Internal.Query;

namespace Shiny.DocumentDb.AzureTable;

/// <summary>
/// Azure Table Storage (and Cosmos DB Table API) implementation of <see cref="IDocumentStore"/>.
/// Documents are stored in a single table with <c>PartitionKey = typeName</c> and <c>RowKey = id</c>.
/// Rich queries evaluate client-side (LiteDB model) after a single-partition native query; optimistic
/// concurrency is backed by the Table <c>ETag</c> (If-Match).
/// </summary>
public partial class AzureTableDocumentStore : DocumentProviderBase, IDocumentStore, IDocumentMaintenance, IObservableDocumentStore, IUnitOfWorkEngine, IDisposable, IAsyncDisposable
{
    // Azure Table caps a single String property at 64 KiB (32 K UTF-16 chars) and a whole entity at 1 MB.
    // Guard the serialized body against the per-property cap with a clear error instead of a raw storage 413.
    const int MaxBodyChars = 32 * 1024;
    const int TransactionChunkSize = 100;

    readonly AzureTableDocumentStoreOptions options;
    readonly TableClient table;
    readonly JsonSerializerOptions jsonOptions;
    readonly IdAccessorCache idCache;
    readonly Action<string>? logging;
    readonly SemaphoreSlim initSemaphore = new(1, 1);
    readonly ChangeBroadcaster broadcaster = new();
    readonly Dictionary<Type, IReadOnlyList<IndexedMapping>> indexedMappings = new();
    List<Action>? pendingChanges;
    bool tableInitialized;

    public AzureTableDocumentStore(AzureTableDocumentStoreOptions options)
    {
        this.options = options;
        this.jsonOptions = options.JsonSerializerOptions ?? new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
            WriteIndented = false
        };
        this.logging = options.Logging;
        this.idCache = new IdAccessorCache(options.ResolveIdPropertyName, options.IdConverters);

        var serviceClient = ResolveServiceClient(options);
        this.table = serviceClient.GetTableClient(options.TableName);

        options.ResolveVersionJsonPaths(this.jsonOptions);

        foreach (var group in options.IndexedSpecs.GroupBy(s => s.Type))
            this.indexedMappings[group.Key] = group
                .Select(s => AzureTablePromoted.Build(s.Segments, this.jsonOptions))
                .ToList();
    }

    internal IReadOnlyList<IndexedMapping> ResolveIndexed(Type type)
        => this.indexedMappings.TryGetValue(type, out var m) ? m : Array.Empty<IndexedMapping>();

    internal ChangeBroadcaster Broadcaster => this.broadcaster;

    /// <inheritdoc />
    public IAsyncEnumerable<DocumentChange<T>> NotifyOnChange<T>(CancellationToken cancellationToken = default) where T : class
        => this.broadcaster.Observe<T>(cancellationToken);

    void PublishChange<T>(DocumentChangeType changeType, string id, T? document) where T : class
    {
        var change = new DocumentChange<T> { ChangeType = changeType, Id = id, Document = document };
        if (this.pendingChanges != null)
            this.pendingChanges.Add(() => this.broadcaster.Publish(change));
        else if (this.broadcaster.HasSubscribers<T>())
            this.broadcaster.Publish(change);
    }

    static TableServiceClient ResolveServiceClient(AzureTableDocumentStoreOptions options)
    {
        if (options.TableServiceClient != null)
            return options.TableServiceClient;

        if (!string.IsNullOrWhiteSpace(options.ConnectionString))
            return new TableServiceClient(options.ConnectionString);

        if (options.ServiceUri == null)
            throw new InvalidOperationException(
                "Azure Table store requires a ConnectionString, a ServiceUri + credential, or a TableServiceClient.");

        if (options.TokenCredential != null)
            return new TableServiceClient(options.ServiceUri, options.TokenCredential);
        if (options.SharedKeyCredential != null)
            return new TableServiceClient(options.ServiceUri, options.SharedKeyCredential);
        if (options.SasCredential != null)
            return new TableServiceClient(options.ServiceUri, options.SasCredential);

        throw new InvalidOperationException(
            "A ServiceUri was supplied without a credential. Set TokenCredential, SharedKeyCredential, or SasCredential.");
    }

    public void Dispose() => this.initSemaphore.Dispose();
    public ValueTask DisposeAsync() { this.initSemaphore.Dispose(); return ValueTask.CompletedTask; }

    void Log(string message) => this.logging?.Invoke(message);

    internal override InterceptorPipeline Interceptors => this.options.Interceptors;

    string ResolveTypeName<T>() => TypeNameResolver.Resolve(typeof(T), this.options.TypeNameResolution);
    string ResolvePartitionKey<T>() => this.options.ResolvePartitionKey(typeof(T), this.ResolveTypeName<T>());

    async Task<TableClient> GetTableAsync(CancellationToken ct)
    {
        if (this.tableInitialized || !this.options.AutoCreateTable)
            return this.table;

        await this.initSemaphore.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            if (!this.tableInitialized)
            {
                await this.table.CreateIfNotExistsAsync(ct).ConfigureAwait(false);
                this.tableInitialized = true;
            }
        }
        finally
        {
            this.initSemaphore.Release();
        }
        return this.table;
    }

    JsonTypeInfo<T>? FindTypeInfo<T>(JsonTypeInfo<T>? provided)
    {
        if (provided != null)
            return provided;

        if (this.jsonOptions.TryGetTypeInfo(typeof(T), out var info) && info is JsonTypeInfo<T> typed)
            return typed;

        if (!this.options.UseReflectionFallback)
            throw new InvalidOperationException(
                $"No JsonTypeInfo registered for type '{typeof(T).FullName}'. " +
                $"Register it in your JsonSerializerContext or pass a JsonTypeInfo<{typeof(T).Name}> explicitly.");

        return null;
    }

    [UnconditionalSuppressMessage("Trimming", "IL2026", Justification = "Reflection path only used when typeInfo is null.")]
    [UnconditionalSuppressMessage("AOT", "IL3050", Justification = "Reflection path only used when typeInfo is null.")]
    static string Serialize<T>(T value, JsonTypeInfo<T>? typeInfo, JsonSerializerOptions options)
        => typeInfo != null ? JsonSerializer.Serialize(value, typeInfo) : JsonSerializer.Serialize(value, options);

    [UnconditionalSuppressMessage("Trimming", "IL2026", Justification = "Reflection path only used when typeInfo is null.")]
    [UnconditionalSuppressMessage("AOT", "IL3050", Justification = "Reflection path only used when typeInfo is null.")]
    static T? Deserialize<T>(string json, JsonTypeInfo<T>? typeInfo, JsonSerializerOptions options)
        => typeInfo != null ? JsonSerializer.Deserialize(json, typeInfo) : JsonSerializer.Deserialize<T>(json, options);

    [UnconditionalSuppressMessage("Trimming", "IL2026", Justification = "Reflection path only used when typeInfo is null.")]
    [UnconditionalSuppressMessage("AOT", "IL3050", Justification = "Reflection path only used when typeInfo is null.")]
    static string ResolvePropertyPath<T>(Expression<Func<T, object>> property, JsonSerializerOptions options, JsonTypeInfo<T>? typeInfo)
        => typeInfo != null
            ? IndexExpressionHelper.ResolveJsonPath(property, options, typeInfo)
            : IndexExpressionHelper.ResolveJsonPath(property, options);

    static string GuardBodySize(string json, string typeName, string id)
    {
        if (json.Length > MaxBodyChars)
            throw new NotSupportedException(
                $"Document '{typeName}' Id '{id}' serializes to {json.Length:N0} characters, exceeding the Azure Table " +
                $"64 KB per-property limit (~{MaxBodyChars:N0} chars). Split the document, store the large field externally, " +
                "or use a provider without the per-property cap (e.g. Cosmos, MongoDB).");
        return json;
    }

    string GenerateId<T>(IdAccessor<T> accessor) where T : class
        => accessor.Kind switch
        {
            IdKind.Guid => Guid.NewGuid().ToString("N"),
            IdKind.String => Guid.NewGuid().ToString(),
            IdKind.Custom => accessor.GenerateOrThrow(),
            IdKind.Int or IdKind.Long => throw new NotSupportedException(
                $"Azure Table cannot auto-generate Int/Long Ids for '{typeof(T).Name}' (no cheap MAX). " +
                "Use a Guid or string Id, or assign the Int/Long Id explicitly before Insert."),
            _ => throw new InvalidOperationException($"Unsupported Id kind: {accessor.Kind}")
        };

    TableEntity CreateEntity(Type docType, string partitionKey, string id, string json, string createdAt, string updatedAt)
    {
        var entity = new TableEntity(partitionKey, id)
        {
            ["Data"] = json,
            ["CreatedAt"] = createdAt,
            ["UpdatedAt"] = updatedAt
        };
        var mappings = this.ResolveIndexed(docType);
        if (mappings.Count > 0)
        {
            var data = JsonNode.Parse(json)!.AsObject();
            foreach (var m in mappings)
            {
                var value = AzureTablePromoted.ReadValue(data, m);
                if (value != null)
                    entity[m.ColumnName] = value;
            }
        }
        return entity;
    }

    // ── IDocumentStore ──────────────────────────────────────────────────

    public IDocumentQuery<T> Query<T>(JsonTypeInfo<T>? jsonTypeInfo = null) where T : class
    {
        var typeInfo = this.FindTypeInfo(jsonTypeInfo);
        return new AzureTableDocumentQuery<T>(this, typeInfo);
    }

    public async Task Insert<T>(T document, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class
    {
        var typeInfo = this.FindTypeInfo(jsonTypeInfo);
        var accessor = this.idCache.GetOrCreate(typeInfo);
        var typeName = this.ResolveTypeName<T>();
        var partitionKey = this.options.ResolvePartitionKey(typeof(T), typeName);
        var versionMapping = this.options.ResolveVersionMapping(typeof(T));
        var table = await this.GetTableAsync(cancellationToken).ConfigureAwait(false);

        var ctx = this.NewWriteContext(DocumentOperation.Insert, typeName, null, document);
        await this.RunBeforeWriteAsync(ctx, cancellationToken).ConfigureAwait(false);
        if (ctx?.Document is T mutated)
            document = mutated;

        string id;
        if (accessor.IsDefaultId(document))
        {
            if (accessor.Kind == IdKind.String)
                throw new InvalidOperationException(
                    $"Insert requires a non-empty string Id on '{typeof(T).Name}'. " +
                    "String Id properties are not auto-generated during Insert.");

            id = this.GenerateId(accessor);
            accessor.SetId(document, id);
        }
        else
        {
            id = accessor.GetIdAsString(document);
        }

        versionMapping?.SetVersion(document, 1);
        var now = DateTimeOffset.UtcNow.ToString("o");
        var json = GuardBodySize(Serialize(document, typeInfo, this.jsonOptions), typeName, id);
        var entity = this.CreateEntity(typeof(T), partitionKey, id, json, now, now);

        this.Log($"AzureTable INSERT {this.options.TableName} PK={partitionKey} RK={id}");
        try
        {
            await table.AddEntityAsync(entity, cancellationToken).ConfigureAwait(false);
        }
        catch (RequestFailedException ex) when (ex.Status == 409)
        {
            throw new InvalidOperationException(
                $"A document of type '{typeName}' with Id '{id}' already exists.", ex);
        }
        await this.RunAfterWriteAsync(ctx, id, versionMapping?.GetVersion(document) ?? 1, cancellationToken).ConfigureAwait(false);
        this.PublishChange(DocumentChangeType.Inserted, id, document);
    }

    public async Task<int> BatchInsert<T>(IEnumerable<T> documents, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class
    {
        var typeInfo = this.FindTypeInfo(jsonTypeInfo);
        var accessor = this.idCache.GetOrCreate(typeInfo);
        var typeName = this.ResolveTypeName<T>();
        var partitionKey = this.options.ResolvePartitionKey(typeof(T), typeName);
        var versionMapping = this.options.ResolveVersionMapping(typeof(T));
        var table = await this.GetTableAsync(cancellationToken).ConfigureAwait(false);

        var srcList = documents as IReadOnlyList<T> ?? documents.ToList();

        DocumentWriteContext[]? ctxs = null;
        if (this.HasPerDocInterceptors)
        {
            var mutable = srcList.ToList();
            ctxs = await this.RunBeforeWriteBatchAsync(mutable, typeName, cancellationToken).ConfigureAwait(false);
            srcList = mutable;
        }

        var entities = new List<TableEntity>(srcList.Count);
        foreach (var document in srcList)
        {
            string id;
            if (accessor.IsDefaultId(document))
            {
                if (accessor.Kind == IdKind.String)
                    throw new InvalidOperationException(
                        $"Insert requires a non-empty string Id on '{typeof(T).Name}'.");
                id = this.GenerateId(accessor);
                accessor.SetId(document, id);
            }
            else
            {
                id = accessor.GetIdAsString(document);
            }

            versionMapping?.SetVersion(document, 1);
            var now = DateTimeOffset.UtcNow.ToString("o");
            var json = GuardBodySize(Serialize(document, typeInfo, this.jsonOptions), typeName, id);
            entities.Add(this.CreateEntity(typeof(T), partitionKey, id, json, now, now));
        }

        if (entities.Count == 0)
            return 0;

        this.Log($"AzureTable BATCH INSERT {entities.Count} docs into {this.options.TableName} PK={partitionKey}");
        try
        {
            foreach (var chunk in entities.Chunk(TransactionChunkSize))
            {
                var actions = chunk.Select(e => new TableTransactionAction(TableTransactionActionType.Add, e));
                await table.SubmitTransactionAsync(actions, cancellationToken).ConfigureAwait(false);
            }
        }
        catch (TableTransactionFailedException ex) when (ex.Status == 409)
        {
            throw new InvalidOperationException(
                $"Batch insert failed for type '{typeName}': a document has a duplicate Id.", ex);
        }

        for (var i = 0; i < srcList.Count; i++)
        {
            if (ctxs != null)
                await this.RunAfterWriteAsync(ctxs[i], accessor.GetIdAsString(srcList[i]), versionMapping?.GetVersion(srcList[i]) ?? 1, cancellationToken).ConfigureAwait(false);
        }
        foreach (var document in srcList)
            this.PublishChange(DocumentChangeType.Inserted, accessor.GetIdAsString(document), document);
        return entities.Count;
    }

    public async Task Update<T>(T document, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class
    {
        var typeInfo = this.FindTypeInfo(jsonTypeInfo);
        var accessor = this.idCache.GetOrCreate(typeInfo);
        var versionMapping = this.options.ResolveVersionMapping(typeof(T));
        var typeName = this.ResolveTypeName<T>();
        var partitionKey = this.options.ResolvePartitionKey(typeof(T), typeName);
        var table = await this.GetTableAsync(cancellationToken).ConfigureAwait(false);

        var ctx = this.NewWriteContext(DocumentOperation.Update, typeName, null, document);
        await this.RunBeforeWriteAsync(ctx, cancellationToken).ConfigureAwait(false);
        if (ctx?.Document is T mutated)
            document = mutated;

        if (accessor.IsDefaultId(document))
            throw new InvalidOperationException(
                $"Update requires a non-default Id on the document. " +
                $"Set the Id property on '{typeof(T).Name}' before calling Update.");

        var id = accessor.GetIdAsString(document);
        var existing = await this.GetEntityAsync(table, partitionKey, id, cancellationToken).ConfigureAwait(false);
        if (existing == null || !this.PassesFiltersForStored<T>(existing, typeInfo))
            throw new InvalidOperationException(
                $"No document of type '{typeName}' with Id '{id}' was found to update.");

        var etag = ETag.All;
        int? expectedVersion = null;
        if (versionMapping != null)
        {
            var ev = versionMapping.GetVersion(document);
            var storedVersion = ReadStoredVersion(existing, versionMapping);
            if (storedVersion != ev)
                throw new ConcurrencyException(typeName, id, ev, storedVersion);
            versionMapping.SetVersion(document, ev + 1);
            expectedVersion = ev;
            etag = existing.ETag;
        }

        var now = DateTimeOffset.UtcNow.ToString("o");
        var json = GuardBodySize(Serialize(document, typeInfo, this.jsonOptions), typeName, id);
        var entity = this.CreateEntity(typeof(T), partitionKey, id, json, (string)existing["CreatedAt"], now);

        this.Log($"AzureTable UPDATE {this.options.TableName} PK={partitionKey} RK={id}");
        try
        {
            await table.UpdateEntityAsync(entity, etag, TableUpdateMode.Replace, cancellationToken).ConfigureAwait(false);
        }
        catch (RequestFailedException ex) when (ex.Status == 412 && expectedVersion != null)
        {
            throw new ConcurrencyException(typeName, id, expectedVersion.Value);
        }
        await this.RunAfterWriteAsync(ctx, id, versionMapping?.GetVersion(document), cancellationToken).ConfigureAwait(false);
        this.PublishChange(DocumentChangeType.Updated, id, document);
    }

    public async Task Upsert<T>(T patch, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class
    {
        var typeInfo = this.FindTypeInfo(jsonTypeInfo);
        var accessor = this.idCache.GetOrCreate(typeInfo);
        var versionMapping = this.options.ResolveVersionMapping(typeof(T));
        var typeName = this.ResolveTypeName<T>();
        var partitionKey = this.options.ResolvePartitionKey(typeof(T), typeName);
        var table = await this.GetTableAsync(cancellationToken).ConfigureAwait(false);

        var ctx = this.NewWriteContext(DocumentOperation.Upsert, typeName, null, patch);
        await this.RunBeforeWriteAsync(ctx, cancellationToken).ConfigureAwait(false);
        if (ctx?.Document is T mutated)
            patch = mutated;

        if (accessor.IsDefaultId(patch))
            throw new InvalidOperationException(
                $"Upsert requires a non-default Id on the document. " +
                $"Set the Id property on '{typeof(T).Name}' before calling Upsert.");

        var id = accessor.GetIdAsString(patch);
        var existing = await this.GetEntityAsync(table, partitionKey, id, cancellationToken).ConfigureAwait(false);
        var now = DateTimeOffset.UtcNow.ToString("o");

        if (existing == null)
        {
            versionMapping?.SetVersion(patch, 1);
            var patchJson = StripNullProperties(Serialize(patch, typeInfo, this.jsonOptions));
            patchJson = GuardBodySize(patchJson, typeName, id);
            var entity = this.CreateEntity(typeof(T), partitionKey, id, patchJson, now, now);

            this.Log($"AzureTable UPSERT (insert) {this.options.TableName} PK={partitionKey} RK={id}");
            try
            {
                await table.AddEntityAsync(entity, cancellationToken).ConfigureAwait(false);
            }
            catch (RequestFailedException ex) when (ex.Status == 409)
            {
                // Lost the insert race — fall through to a merge on the now-present row.
                await this.MergeUpsert(table, partitionKey, typeName, id, patch, typeInfo, versionMapping, now, cancellationToken).ConfigureAwait(false);
            }
        }
        else
        {
            await this.MergeUpsertExisting(table, partitionKey, typeName, id, patch, typeInfo, versionMapping, existing, now, cancellationToken).ConfigureAwait(false);
        }

        await this.RunAfterWriteAsync(ctx, id, versionMapping?.GetVersion(patch), cancellationToken).ConfigureAwait(false);
        this.PublishChange(DocumentChangeType.Updated, id, patch);
    }

    async Task MergeUpsert<T>(TableClient table, string partitionKey, string typeName, string id, T patch, JsonTypeInfo<T>? typeInfo, VersionMapping? versionMapping, string now, CancellationToken ct) where T : class
    {
        var existing = await this.GetEntityAsync(table, partitionKey, id, ct).ConfigureAwait(false);
        if (existing == null)
            throw new InvalidOperationException($"Upsert race could not be resolved for type '{typeName}' Id '{id}'.");
        await this.MergeUpsertExisting(table, partitionKey, typeName, id, patch, typeInfo, versionMapping, existing, now, ct).ConfigureAwait(false);
    }

    async Task MergeUpsertExisting<T>(TableClient table, string partitionKey, string typeName, string id, T patch, JsonTypeInfo<T>? typeInfo, VersionMapping? versionMapping, TableEntity existing, string now, CancellationToken ct) where T : class
    {
        var etag = ETag.All;
        int? guardVersion = null;
        if (versionMapping != null)
        {
            var expectedVersion = versionMapping.GetVersion(patch);
            var storedVersion = ReadStoredVersion(existing, versionMapping);
            if (expectedVersion > 0 && storedVersion != expectedVersion)
                throw new ConcurrencyException(typeName, id, expectedVersion, storedVersion);
            versionMapping.SetVersion(patch, storedVersion + 1);
            if (expectedVersion > 0)
            {
                guardVersion = expectedVersion;
                etag = existing.ETag;
            }
        }

        var patchJson = StripNullProperties(Serialize(patch, typeInfo, this.jsonOptions));
        var merged = GuardBodySize(MergeJson((string)existing["Data"], patchJson), typeName, id);
        var entity = this.CreateEntity(typeof(T), partitionKey, id, merged, (string)existing["CreatedAt"], now);

        this.Log($"AzureTable UPSERT (merge) {this.options.TableName} PK={partitionKey} RK={id}");
        try
        {
            await table.UpdateEntityAsync(entity, etag, TableUpdateMode.Replace, ct).ConfigureAwait(false);
        }
        catch (RequestFailedException ex) when (ex.Status == 412 && guardVersion != null)
        {
            throw new ConcurrencyException(typeName, id, guardVersion.Value);
        }
    }

    [UnconditionalSuppressMessage("Trimming", "IL2026", Justification = "Value serialization uses reflection when type is unknown.")]
    [UnconditionalSuppressMessage("AOT", "IL3050", Justification = "Value serialization uses reflection when type is unknown.")]
    public async Task<bool> SetProperty<T>(object id, Expression<Func<T, object>> property, object? value, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class
    {
        var typeInfo = this.FindTypeInfo(jsonTypeInfo);
        var resolvedId = this.idCache.GetOrCreate(typeInfo).ResolveId(id);
        var jsonPath = ResolvePropertyPath(property, this.jsonOptions, typeInfo);
        var typeName = this.ResolveTypeName<T>();
        var partitionKey = this.options.ResolvePartitionKey(typeof(T), typeName);
        var table = await this.GetTableAsync(cancellationToken).ConfigureAwait(false);

        var existing = await this.GetEntityAsync(table, partitionKey, resolvedId, cancellationToken).ConfigureAwait(false);
        if (existing == null || !this.PassesFiltersForStored<T>(existing, typeInfo))
            return false;

        var node = JsonNode.Parse((string)existing["Data"])!.AsObject();
        SetNestedProperty(node, jsonPath, value == null ? null : JsonNode.Parse(JsonSerializer.Serialize(value, this.jsonOptions)));
        var entity = this.CreateEntity(typeof(T), partitionKey, resolvedId, GuardBodySize(node.ToJsonString(), typeName, resolvedId), (string)existing["CreatedAt"], DateTimeOffset.UtcNow.ToString("o"));

        this.Log($"AzureTable SET PROPERTY {this.options.TableName} RK={resolvedId} Path={jsonPath}");
        await table.UpdateEntityAsync(entity, ETag.All, TableUpdateMode.Replace, cancellationToken).ConfigureAwait(false);
        this.PublishChange<T>(DocumentChangeType.Updated, resolvedId, null);
        return true;
    }

    public async Task<bool> RemoveProperty<T>(object id, Expression<Func<T, object>> property, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class
    {
        var typeInfo = this.FindTypeInfo(jsonTypeInfo);
        var resolvedId = this.idCache.GetOrCreate(typeInfo).ResolveId(id);
        var jsonPath = ResolvePropertyPath(property, this.jsonOptions, typeInfo);
        var typeName = this.ResolveTypeName<T>();
        var partitionKey = this.options.ResolvePartitionKey(typeof(T), typeName);
        var table = await this.GetTableAsync(cancellationToken).ConfigureAwait(false);

        var existing = await this.GetEntityAsync(table, partitionKey, resolvedId, cancellationToken).ConfigureAwait(false);
        if (existing == null || !this.PassesFiltersForStored<T>(existing, typeInfo))
            return false;

        var node = JsonNode.Parse((string)existing["Data"])!.AsObject();
        RemoveNestedProperty(node, jsonPath);
        var entity = this.CreateEntity(typeof(T), partitionKey, resolvedId, GuardBodySize(node.ToJsonString(), typeName, resolvedId), (string)existing["CreatedAt"], DateTimeOffset.UtcNow.ToString("o"));

        this.Log($"AzureTable REMOVE PROPERTY {this.options.TableName} RK={resolvedId} Path={jsonPath}");
        await table.UpdateEntityAsync(entity, ETag.All, TableUpdateMode.Replace, cancellationToken).ConfigureAwait(false);
        this.PublishChange<T>(DocumentChangeType.Updated, resolvedId, null);
        return true;
    }

    public async Task<T?> Get<T>(object id, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class
    {
        var typeInfo = this.FindTypeInfo(jsonTypeInfo);
        var resolvedId = this.idCache.GetOrCreate(typeInfo).ResolveId(id);
        var typeName = this.ResolveTypeName<T>();
        var partitionKey = this.options.ResolvePartitionKey(typeof(T), typeName);
        var table = await this.GetTableAsync(cancellationToken).ConfigureAwait(false);

        this.Log($"AzureTable GET {this.options.TableName} PK={partitionKey} RK={resolvedId}");
        var existing = await this.GetEntityAsync(table, partitionKey, resolvedId, cancellationToken).ConfigureAwait(false);
        if (existing == null)
            return null;

        var doc = Deserialize((string)existing["Data"], typeInfo, this.jsonOptions);
        if (doc != null && !this.PassesGlobalFilters(doc))
            return null;
        return doc;
    }

    public async Task<JsonPatchDocument<T>?> GetDiff<T>(object id, T modified, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class
    {
        var typeInfo = this.FindTypeInfo(jsonTypeInfo);
        var resolvedId = this.idCache.GetOrCreate(typeInfo).ResolveId(id);
        var typeName = this.ResolveTypeName<T>();
        var partitionKey = this.options.ResolvePartitionKey(typeof(T), typeName);
        var table = await this.GetTableAsync(cancellationToken).ConfigureAwait(false);

        var existing = await this.GetEntityAsync(table, partitionKey, resolvedId, cancellationToken).ConfigureAwait(false);
        if (existing == null || !this.PassesFiltersForStored<T>(existing, typeInfo))
            return null;

        var modifiedJson = Serialize(modified, typeInfo, this.jsonOptions);
        return JsonDiff.CreatePatch<T>((string)existing["Data"], modifiedJson, this.jsonOptions);
    }

    /// <summary>
    /// Runs a raw <b>OData</b> <c>$filter</c> fragment (Azure Table's native query language) scoped to
    /// the type's partition. The filter targets native columns — the promoted columns declared with
    /// <c>MapIndexedProperty&lt;T&gt;</c> (referenced by their CLR/JSON property name) and the built-in
    /// <c>PartitionKey</c>/<c>RowKey</c>/<c>Timestamp</c> — not fields inside the opaque JSON body.
    /// <c>parameters</c> supplies <c>@name</c> token substitutions.
    /// </summary>
    public async Task<IReadOnlyList<T>> Query<T>(string whereClause, JsonTypeInfo<T>? jsonTypeInfo = null, object? parameters = null, CancellationToken cancellationToken = default) where T : class
    {
        var typeInfo = this.FindTypeInfo(jsonTypeInfo);
        var results = new List<T>();
        await foreach (var doc in this.QueryStream(whereClause, typeInfo, parameters, cancellationToken).ConfigureAwait(false))
            results.Add(doc);
        return results;
    }

    public async IAsyncEnumerable<T> QueryStream<T>(string whereClause, JsonTypeInfo<T>? jsonTypeInfo = null, object? parameters = null, [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default) where T : class
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(whereClause);
        var typeInfo = this.FindTypeInfo(jsonTypeInfo);
        var partitionKey = this.ResolvePartitionKey<T>();
        var table = await this.GetTableAsync(cancellationToken).ConfigureAwait(false);

        var userFilter = this.RewriteColumns<T>(BindParameters(whereClause, parameters));
        var filter = $"PartitionKey eq {AzureTablePromoted.Literal(partitionKey)} and ({userFilter})";
        this.Log($"AzureTable QUERY {this.options.TableName} filter={filter}");

        await foreach (var entity in table.QueryAsync<TableEntity>(filter, cancellationToken: cancellationToken).ConfigureAwait(false))
        {
            var doc = Deserialize((string)entity["Data"], typeInfo, this.jsonOptions);
            if (doc != null && this.PassesGlobalFilters(doc))
                yield return doc;
        }
    }

    // Rewrites bare promoted-property names in a user OData filter to their native idx_ column names.
    string RewriteColumns<T>(string filter) where T : class
    {
        foreach (var m in this.ResolveIndexed(typeof(T)))
        {
            var clrLeaf = m.ClrPath.Contains('.') ? m.ClrPath[(m.ClrPath.LastIndexOf('.') + 1)..] : m.ClrPath;
            filter = System.Text.RegularExpressions.Regex.Replace(
                filter, $@"\b{System.Text.RegularExpressions.Regex.Escape(clrLeaf)}\b", m.ColumnName);
        }
        return filter;
    }

    static string BindParameters(string filter, object? parameters)
    {
        if (parameters == null)
            return filter;
        foreach (var (key, value) in EnumerateParameters(parameters))
            filter = filter.Replace("@" + key, AzureTablePromoted.Literal(value));
        return filter;
    }

    [UnconditionalSuppressMessage("Trimming", "IL2075", Justification = "Parameter binding reads public properties of a caller-supplied anonymous object.")]
    static IEnumerable<(string Key, object? Value)> EnumerateParameters(object parameters)
    {
        if (parameters is IDictionary<string, object?> dict)
        {
            foreach (var kv in dict)
                yield return (kv.Key, kv.Value);
            yield break;
        }
        foreach (var prop in parameters.GetType().GetProperties())
            yield return (prop.Name, prop.GetValue(parameters));
    }

    public async Task<int> Count<T>(string? whereClause = null, object? parameters = null, CancellationToken cancellationToken = default) where T : class
    {
        if (!string.IsNullOrWhiteSpace(whereClause))
        {
            var n = 0;
            await foreach (var _ in this.QueryStream<T>(whereClause, null, parameters, cancellationToken).ConfigureAwait(false))
                n++;
            return n;
        }

        var typeInfo = this.FindTypeInfo<T>(null);
        var typeName = this.ResolveTypeName<T>();
        var partitionKey = this.options.ResolvePartitionKey(typeof(T), typeName);
        var table = await this.GetTableAsync(cancellationToken).ConfigureAwait(false);
        var hasFilters = this.options.ResolveQueryFilters(typeof(T)).Count > 0;

        this.Log($"AzureTable COUNT {this.options.TableName} PK={partitionKey}");
        var filter = TableClient.CreateQueryFilter($"PartitionKey eq {partitionKey}");
        var select = hasFilters ? new[] { "Data" } : new[] { "RowKey" };
        var count = 0;
        await foreach (var entity in table.QueryAsync<TableEntity>(filter, select: select, cancellationToken: cancellationToken).ConfigureAwait(false))
        {
            if (!hasFilters)
            {
                count++;
                continue;
            }
            var doc = Deserialize((string)entity["Data"], typeInfo, this.jsonOptions);
            if (doc != null && this.PassesGlobalFilters(doc))
                count++;
        }
        return count;
    }

    public async Task<bool> Remove<T>(object id, CancellationToken cancellationToken = default) where T : class
    {
        var resolvedId = this.idCache.GetOrCreate<T>(null).ResolveId(id);
        var typeName = this.ResolveTypeName<T>();
        var partitionKey = this.options.ResolvePartitionKey(typeof(T), typeName);
        var table = await this.GetTableAsync(cancellationToken).ConfigureAwait(false);

        var ctx = this.NewWriteContext<T>(DocumentOperation.Delete, typeName, id, null);
        await this.RunBeforeWriteAsync(ctx, cancellationToken).ConfigureAwait(false);

        if (this.options.ResolveQueryFilters(typeof(T)).Count > 0)
        {
            var existing = await this.GetEntityAsync(table, partitionKey, resolvedId, cancellationToken).ConfigureAwait(false);
            if (existing == null || !this.PassesFiltersForStored<T>(existing, null))
                return false;
        }

        this.Log($"AzureTable DELETE {this.options.TableName} PK={partitionKey} RK={resolvedId}");
        Response response;
        try
        {
            response = await table.DeleteEntityAsync(partitionKey, resolvedId, ETag.All, cancellationToken).ConfigureAwait(false);
        }
        catch (RequestFailedException ex) when (ex.Status == 404)
        {
            return false;
        }
        if (response.Status == 404)
            return false;

        await this.RunAfterWriteAsync(ctx, id, null, cancellationToken).ConfigureAwait(false);
        this.PublishChange<T>(DocumentChangeType.Removed, resolvedId, null);
        return true;
    }

    public async Task<int> Clear<T>(CancellationToken cancellationToken = default) where T : class
    {
        var typeInfo = this.FindTypeInfo<T>(null);
        var typeName = this.ResolveTypeName<T>();
        var partitionKey = this.options.ResolvePartitionKey(typeof(T), typeName);
        var table = await this.GetTableAsync(cancellationToken).ConfigureAwait(false);
        var hasFilters = this.options.ResolveQueryFilters(typeof(T)).Count > 0;

        var bulkCtx = this.NewBulkContext<T>(DocumentOperation.Clear, typeName);
        await this.RunBeforeBulkAsync(bulkCtx, cancellationToken).ConfigureAwait(false);

        this.Log($"AzureTable CLEAR {this.options.TableName} PK={partitionKey}");
        var filter = TableClient.CreateQueryFilter($"PartitionKey eq {partitionKey}");
        var select = hasFilters ? new[] { "RowKey", "Data" } : new[] { "RowKey" };
        var rowKeys = new List<string>();
        await foreach (var entity in table.QueryAsync<TableEntity>(filter, select: select, cancellationToken: cancellationToken).ConfigureAwait(false))
        {
            if (hasFilters)
            {
                var doc = Deserialize((string)entity["Data"], typeInfo, this.jsonOptions);
                if (doc == null || !this.PassesGlobalFilters(doc))
                    continue;
            }
            rowKeys.Add(entity.RowKey);
        }

        var count = await this.DeleteRowKeysAsync(table, partitionKey, rowKeys, cancellationToken).ConfigureAwait(false);
        await this.RunAfterBulkAsync(bulkCtx, count, cancellationToken).ConfigureAwait(false);
        if (count > 0)
            this.PublishChange<T>(DocumentChangeType.Cleared, "", null);
        return count;
    }

    /// <inheritdoc />
    public async Task ClearAll(CancellationToken cancellationToken = default)
    {
        var table = await this.GetTableAsync(cancellationToken).ConfigureAwait(false);
        this.Log($"AzureTable CLEAR ALL {this.options.TableName}");

        // Scan every partition and delete grouped by PartitionKey (each transaction is single-partition).
        var byPartition = new Dictionary<string, List<string>>();
        await foreach (var entity in table.QueryAsync<TableEntity>(select: new[] { "PartitionKey", "RowKey" }, cancellationToken: cancellationToken).ConfigureAwait(false))
        {
            if (!byPartition.TryGetValue(entity.PartitionKey, out var list))
                byPartition[entity.PartitionKey] = list = new List<string>();
            list.Add(entity.RowKey);
        }

        foreach (var (partitionKey, rowKeys) in byPartition)
            await this.DeleteRowKeysAsync(table, partitionKey, rowKeys, cancellationToken).ConfigureAwait(false);
    }

    async Task<int> DeleteRowKeysAsync(TableClient table, string partitionKey, IReadOnlyList<string> rowKeys, CancellationToken ct)
    {
        var deleted = 0;
        foreach (var chunk in rowKeys.Chunk(TransactionChunkSize))
        {
            var actions = chunk.Select(rk =>
                new TableTransactionAction(TableTransactionActionType.Delete, new TableEntity(partitionKey, rk) { ETag = ETag.All }));
            await table.SubmitTransactionAsync(actions, ct).ConfigureAwait(false);
            deleted += chunk.Length;
        }
        return deleted;
    }

    // ── Batch overrides (native transactions per partition) ──────────────

    public async Task<int> BatchRemove<T>(IEnumerable<object> ids, CancellationToken cancellationToken = default) where T : class
    {
        var accessor = this.idCache.GetOrCreate<T>(null);
        var typeName = this.ResolveTypeName<T>();
        var partitionKey = this.options.ResolvePartitionKey(typeof(T), typeName);
        var table = await this.GetTableAsync(cancellationToken).ConfigureAwait(false);

        // Only delete ids that exist (BatchRemove ignores missing ids and returns the actual delete count).
        var existing = new List<string>();
        foreach (var id in ids)
        {
            var rk = accessor.ResolveId(id);
            var entity = await this.GetEntityAsync(table, partitionKey, rk, cancellationToken).ConfigureAwait(false);
            if (entity != null)
                existing.Add(rk);
        }
        return await this.DeleteRowKeysAsync(table, partitionKey, existing, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public UnitOfWork CreateUnitOfWork() => new(this);

    async Task IUnitOfWorkEngine.RunUnitAsync(Func<IDocumentStore, CancellationToken, Task> work, CancellationToken cancellationToken)
    {
        var tracker = new AzureTableTransactionalStore(this);
        var buffer = new List<Action>();
        this.pendingChanges = buffer;
        try
        {
            await work(tracker, cancellationToken).ConfigureAwait(false);
        }
        catch
        {
            await tracker.RollbackAsync(cancellationToken).ConfigureAwait(false);
            throw;
        }
        finally
        {
            this.pendingChanges = null;
        }
        foreach (var emit in buffer)
            emit();
    }

    // ── Internal helpers used by AzureTableDocumentQuery ────────────────

    internal IAsyncEnumerable<T> LoadDocumentsAsync<T>(JsonTypeInfo<T>? typeInfo, CancellationToken ct = default) where T : class
        => this.LoadDocumentsAsync(typeInfo, null, ct);

    // Optional pushdownFilter is an OData fragment over promoted columns that shrinks the candidate set;
    // the query still re-applies the full predicate client-side, so it need only be a superset.
    internal async IAsyncEnumerable<T> LoadDocumentsAsync<T>(JsonTypeInfo<T>? typeInfo, string? pushdownFilter, [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct = default) where T : class
    {
        var partitionKey = this.ResolvePartitionKey<T>();
        var table = await this.GetTableAsync(ct).ConfigureAwait(false);
        var filter = $"PartitionKey eq {AzureTablePromoted.Literal(partitionKey)}";
        if (!string.IsNullOrEmpty(pushdownFilter))
            filter += $" and ({pushdownFilter})";
        this.Log($"AzureTable LOAD {this.options.TableName} filter={filter}");
        await foreach (var entity in table.QueryAsync<TableEntity>(filter, select: new[] { "Data" }, cancellationToken: ct).ConfigureAwait(false))
        {
            var doc = Deserialize((string)entity["Data"], typeInfo, this.jsonOptions);
            if (doc != null)
                yield return doc;
        }
    }

    // Builds the OData pushdown fragment (or null) for a set of predicates using the promoted columns.
    internal string? BuildPushdownFilter<T>(IEnumerable<Expression<Func<T, bool>>> predicates) where T : class
    {
        var mappings = this.ResolveIndexed(typeof(T));
        if (mappings.Count == 0)
            return null;
        var byPath = mappings.ToDictionary(m => m.ClrPath, m => m);
        var clauses = AzureTablePromoted.ExtractClauses(predicates, byPath);
        return AzureTablePromoted.ToODataFilter(clauses);
    }

    internal async Task<int> DeleteWhereAsync<T>(Func<T, bool> predicate, JsonTypeInfo<T>? typeInfo, CancellationToken ct) where T : class
    {
        var partitionKey = this.ResolvePartitionKey<T>();
        var table = await this.GetTableAsync(ct).ConfigureAwait(false);
        var filter = TableClient.CreateQueryFilter($"PartitionKey eq {partitionKey}");
        var toDelete = new List<string>();
        await foreach (var entity in table.QueryAsync<TableEntity>(filter, select: new[] { "RowKey", "Data" }, cancellationToken: ct).ConfigureAwait(false))
        {
            var doc = Deserialize((string)entity["Data"], typeInfo, this.jsonOptions);
            if (doc != null && predicate(doc))
                toDelete.Add(entity.RowKey);
        }
        return await this.DeleteRowKeysAsync(table, partitionKey, toDelete, ct).ConfigureAwait(false);
    }

    [UnconditionalSuppressMessage("Trimming", "IL2026", Justification = "Value serialization uses reflection when type is unknown.")]
    [UnconditionalSuppressMessage("AOT", "IL3050", Justification = "Value serialization uses reflection when type is unknown.")]
    internal async Task<int> UpdatePropertyWhereAsync<T>(Func<T, bool> predicate, string jsonPath, object? value, JsonTypeInfo<T>? typeInfo, CancellationToken ct) where T : class
    {
        var typeName = this.ResolveTypeName<T>();
        var partitionKey = this.ResolvePartitionKey<T>();
        var table = await this.GetTableAsync(ct).ConfigureAwait(false);
        var filter = TableClient.CreateQueryFilter($"PartitionKey eq {partitionKey}");
        var matched = new List<TableEntity>();
        await foreach (var entity in table.QueryAsync<TableEntity>(filter, cancellationToken: ct).ConfigureAwait(false))
        {
            var doc = Deserialize((string)entity["Data"], typeInfo, this.jsonOptions);
            if (doc != null && predicate(doc))
                matched.Add(entity);
        }

        var now = DateTimeOffset.UtcNow.ToString("o");
        foreach (var entity in matched)
        {
            var node = JsonNode.Parse((string)entity["Data"])!.AsObject();
            SetNestedProperty(node, jsonPath, value == null ? null : JsonNode.Parse(JsonSerializer.Serialize(value, this.jsonOptions)));
            var updated = this.CreateEntity(typeof(T), partitionKey, entity.RowKey, GuardBodySize(node.ToJsonString(), typeName, entity.RowKey), (string)entity["CreatedAt"], now);
            await table.UpdateEntityAsync(updated, ETag.All, TableUpdateMode.Replace, ct).ConfigureAwait(false);
        }
        return matched.Count;
    }

    internal AzureTableDocumentStoreOptions Options => this.options;
    internal JsonSerializerOptions JsonOptions => this.jsonOptions;
    internal InterceptorPipeline InterceptorPipeline => this.options.Interceptors;
    internal string ResolveTypeNameFor<T>() => this.ResolveTypeName<T>();
    internal string ResolvePartitionKeyFor<T>() => this.ResolvePartitionKey<T>();

    // ── Private helpers ─────────────────────────────────────────────────

    async Task<TableEntity?> GetEntityAsync(TableClient table, string partitionKey, string rowKey, CancellationToken ct)
    {
        var response = await table.GetEntityIfExistsAsync<TableEntity>(partitionKey, rowKey, cancellationToken: ct).ConfigureAwait(false);
        return response.HasValue ? response.Value : null;
    }

    static int ReadStoredVersion(TableEntity existing, VersionMapping mapping)
    {
        var node = JsonNode.Parse((string)existing["Data"])!.AsObject();
        return node[mapping.JsonPath]?.GetValue<int>() ?? 0;
    }

    bool PassesGlobalFilters<T>(T document) where T : class
    {
        var filters = this.options.ResolveQueryFilters(typeof(T));
        if (filters.Count == 0)
            return true;
        foreach (var f in filters)
        {
            var compiled = ExpressionInterpreter.Interpret((Expression<Func<T, bool>>)f.Predicate);
            if (!compiled(document))
                return false;
        }
        return true;
    }

    bool PassesFiltersForStored<T>(TableEntity existing, JsonTypeInfo<T>? typeInfo) where T : class
    {
        if (this.options.ResolveQueryFilters(typeof(T)).Count == 0)
            return true;
        var doc = Deserialize((string)existing["Data"], typeInfo, this.jsonOptions);
        return doc != null && this.PassesGlobalFilters(doc);
    }

    static string StripNullProperties(string json) => JsonMergePatch.StripNullsRecursive(json);

    static string MergeJson(string originalJson, string patchJson)
    {
        var original = JsonNode.Parse(originalJson)?.AsObject();
        var patch = JsonNode.Parse(patchJson)?.AsObject();
        if (original == null || patch == null)
            return patchJson;

        foreach (var prop in patch)
        {
            if (prop.Value is JsonObject patchObj && original[prop.Key] is JsonObject origObj)
                original[prop.Key] = JsonNode.Parse(MergeJson(origObj.ToJsonString(), patchObj.ToJsonString()));
            else
                original[prop.Key] = prop.Value?.DeepClone();
        }
        return original.ToJsonString();
    }

    static void SetNestedProperty(JsonObject node, string path, JsonNode? value)
    {
        var parts = path.Split('.');
        var current = node;
        for (var i = 0; i < parts.Length - 1; i++)
        {
            if (current[parts[i]] is not JsonObject child)
            {
                child = new JsonObject();
                current[parts[i]] = child;
            }
            current = child;
        }
        current[parts[^1]] = value;
    }

    static void RemoveNestedProperty(JsonObject node, string path)
    {
        var parts = path.Split('.');
        var current = node;
        for (var i = 0; i < parts.Length - 1; i++)
        {
            if (current[parts[i]] is not JsonObject child)
                return;
            current = child;
        }
        current.Remove(parts[^1]);
    }

    // ── Compensating transaction wrapper ────────────────────────────────

    sealed class AzureTableTransactionalStore(AzureTableDocumentStore inner) : CompensatingStore
    {
        protected override IDocumentStore Inner => inner;

        public override async Task Insert<T>(T document, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default)
        {
            await inner.Insert(document, jsonTypeInfo, cancellationToken).ConfigureAwait(false);
            var accessor = inner.idCache.GetOrCreate(inner.FindTypeInfo(jsonTypeInfo));
            this.TrackInsert(inner.ResolvePartitionKey<T>(), accessor.GetIdAsString(document));
        }

        protected override async Task DeleteTrackedAsync(string partitionKey, string id, CancellationToken ct)
        {
            var table = await inner.GetTableAsync(ct).ConfigureAwait(false);
            await table.DeleteEntityAsync(partitionKey, id, ETag.All, ct).ConfigureAwait(false);
        }
    }
}
