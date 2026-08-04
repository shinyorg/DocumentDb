using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.Json.Serialization.Metadata;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using Shiny.DocumentDb.Internal;
using Shiny.DocumentDb.Internal.Query;

namespace Shiny.DocumentDb.DynamoDb;

/// <summary>
/// Amazon DynamoDB implementation of <see cref="IDocumentStore"/>. Documents live in a single table with
/// partition key <c>pk = typeName</c> and sort key <c>sk = id</c>. Rich queries evaluate client-side
/// (LiteDB model) after a single-partition native <c>Query</c>; optimistic concurrency is enforced with a
/// conditional write on a top-level <c>Version</c> attribute.
/// </summary>
public partial class DynamoDbDocumentStore : DocumentProviderBase, IDocumentStore, IDocumentMaintenance, IObservableDocumentStore, IChangeFeedDocumentStore, IUnitOfWorkEngine, IDisposable, IAsyncDisposable
{
    // DynamoDB caps a single item at 400 KB. Guard the serialized body with a clear error.
    const int MaxItemBytes = 390 * 1024;
    const int BatchWriteChunkSize = 25;

    readonly DynamoDbDocumentStoreOptions options;
    readonly IAmazonDynamoDB client;
    readonly bool ownsClient;
    readonly JsonSerializerOptions jsonOptions;
    readonly IdAccessorCache idCache;
    Action<string>? logging;
    readonly SemaphoreSlim initSemaphore = new(1, 1);
    readonly Dictionary<Type, IReadOnlyList<IndexedMapping>> indexedMappings = new();
    // AsyncLocal (not a plain field) so concurrent units of work and direct writes each buffer change
    // notifications independently and don't clobber one another.
    bool tableInitialized;

    /// <summary>Constructs the store and wires DI-registered interceptors from <paramref name="serviceProvider"/>
    /// (so container-registered <see cref="IDocumentInterceptor"/>s fire alongside options-registered ones, and a
    /// scoped interceptor can resolve <see cref="DocumentWriteContext.Services"/>).</summary>
    public DynamoDbDocumentStore(DynamoDbDocumentStoreOptions options, IServiceProvider serviceProvider) : this(options)
    {
        this.AttachServiceProvider(serviceProvider);
        this.logging = DocumentStoreLogging.Compose(this.logging, this.Logger);
    }

    public DynamoDbDocumentStore(DynamoDbDocumentStoreOptions options)
    {
        this.options = options;
        this.jsonOptions = options.JsonSerializerOptions ?? new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
            WriteIndented = false
        };
        this.logging = options.Logging;
        this.idCache = new IdAccessorCache(options.ResolveIdPropertyName, options.IdConverters);

        if (options.Client != null)
        {
            this.client = options.Client;
            this.ownsClient = false;
        }
        else
        {
            var config = new AmazonDynamoDBConfig();
            if (!string.IsNullOrWhiteSpace(options.ServiceUrl))
                config.ServiceURL = options.ServiceUrl;
            else if (options.Region != null)
                config.RegionEndpoint = options.Region;

            this.client = options.Credentials != null
                ? new AmazonDynamoDBClient(options.Credentials, config)
                : new AmazonDynamoDBClient(config);
            this.ownsClient = true;
        }

        options.ResolveVersionJsonPaths(this.jsonOptions);
        DocumentConfigurationValidator.Validate(options);

        foreach (var group in options.IndexedSpecs.GroupBy(s => s.Type))
            this.indexedMappings[group.Key] = group
                .Select(s => DynamoDbPromoted.Build(s.Segments, this.jsonOptions))
                .ToList();
    }

    internal IReadOnlyList<IndexedMapping> ResolveIndexed(Type type)
        => this.indexedMappings.TryGetValue(type, out var m) ? m : Array.Empty<IndexedMapping>();


    /// <inheritdoc />
    public IAsyncEnumerable<DocumentChange<T>> NotifyOnChange<T>(CancellationToken cancellationToken = default) where T : class
        => this.Broadcaster.Observe<T>(cancellationToken);

    Dictionary<string, AttributeValue> BuildItem(Type docType, string partitionKey, string id, string json, string createdAt, string updatedAt, int? version)
    {
        var item = DynamoDbDocument.Build(partitionKey, id, json, createdAt, updatedAt, version);
        var mappings = this.ResolveIndexed(docType);
        if (mappings.Count > 0)
        {
            var data = JsonNode.Parse(json)!.AsObject();
            foreach (var m in mappings)
            {
                var value = DynamoDbPromoted.ReadValue(data, m);
                if (value != null)
                    item[m.AttributeName] = value;
            }
        }
        return item;
    }

    public void Dispose()
    {
        if (this.ownsClient)
            this.client.Dispose();
        this.streamsClient?.Dispose();
        this.initSemaphore.Dispose();
    }

    public ValueTask DisposeAsync()
    {
        this.Dispose();
        return ValueTask.CompletedTask;
    }

    void Log(string message) => this.logging?.Invoke(message);

    protected override InterceptorPipeline Interceptors => this.options.Interceptors;
    protected override DocumentMappingRegistry Mappings => this.options.Mappings;
    protected override IdAccessorCache IdCache => this.idCache;
    protected override JsonTypeInfo<T>? ResolveTypeInfo<T>(JsonTypeInfo<T>? provided) where T : class => this.FindTypeInfo(provided);
    protected override string ResolveDocumentTypeName<T>() where T : class => this.ResolveTypeName<T>();

    string TableName => this.options.TableName;
    string ResolveTypeName<T>() => TypeNameResolver.Resolve(typeof(T), this.options.TypeNameResolution);
    string ResolvePartitionKey<T>() => this.options.ResolvePartitionKey(typeof(T), this.ResolveTypeName<T>());

    async Task EnsureTableAsync(CancellationToken ct)
    {
        if (this.tableInitialized)
            return;

        await this.initSemaphore.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            if (this.tableInitialized)
                return;

            var exists = await this.TableExistsAsync(ct).ConfigureAwait(false);
            if (!exists)
            {
                if (!this.options.AutoCreateTable)
                    throw new InvalidOperationException(
                        $"DynamoDB table '{this.TableName}' does not exist. Create it (pk HASH / sk RANGE) or set AutoCreateTable = true.");

                await this.CreateTableAsync(ct).ConfigureAwait(false);
            }
            this.tableInitialized = true;
        }
        finally
        {
            this.initSemaphore.Release();
        }
    }

    async Task<bool> TableExistsAsync(CancellationToken ct)
    {
        try
        {
            var resp = await this.client.DescribeTableAsync(this.TableName, ct).ConfigureAwait(false);
            return resp.Table.TableStatus == TableStatus.ACTIVE;
        }
        catch (ResourceNotFoundException)
        {
            return false;
        }
    }

    async Task CreateTableAsync(CancellationToken ct)
    {
        this.Log($"DynamoDB CREATE TABLE {this.TableName}");
        await this.client.CreateTableAsync(new CreateTableRequest
        {
            TableName = this.TableName,
            BillingMode = BillingMode.PAY_PER_REQUEST,
            KeySchema =
            [
                new KeySchemaElement(DynamoDbDocument.Pk, KeyType.HASH),
                new KeySchemaElement(DynamoDbDocument.Sk, KeyType.RANGE)
            ],
            AttributeDefinitions =
            [
                new AttributeDefinition(DynamoDbDocument.Pk, ScalarAttributeType.S),
                new AttributeDefinition(DynamoDbDocument.Sk, ScalarAttributeType.S)
            ],
            // Enable a stream so IChangeFeedDocumentStore.SubscribeChanges can observe writes.
            StreamSpecification = new StreamSpecification
            {
                StreamEnabled = true,
                StreamViewType = StreamViewType.NEW_AND_OLD_IMAGES
            }
        }, ct).ConfigureAwait(false);

        // Wait for the table to become ACTIVE before first use.
        for (var attempt = 0; attempt < 60; attempt++)
        {
            var resp = await this.client.DescribeTableAsync(this.TableName, ct).ConfigureAwait(false);
            if (resp.Table.TableStatus == TableStatus.ACTIVE)
                return;
            await Task.Delay(500, ct).ConfigureAwait(false);
        }
        throw new TimeoutException($"DynamoDB table '{this.TableName}' did not become ACTIVE in time.");
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
        if (Encoding.UTF8.GetByteCount(json) > MaxItemBytes)
            throw new NotSupportedException(
                $"Document '{typeName}' Id '{id}' exceeds the DynamoDB 400 KB item limit. " +
                "Split the document, store the large field externally (e.g. S3), or use a provider without the item cap.");
        return json;
    }

    string GenerateId<T>(IdAccessor<T> accessor) where T : class
        => accessor.Kind switch
        {
            IdKind.Guid => Guid.NewGuid().ToString("N"),
            IdKind.String => Guid.NewGuid().ToString(),
            IdKind.Custom => accessor.GenerateOrThrow(),
            IdKind.Int or IdKind.Long => throw new NotSupportedException(
                $"DynamoDB cannot auto-generate Int/Long Ids for '{typeof(T).Name}' (no cheap MAX). " +
                "Use a Guid or string Id, or assign the Int/Long Id explicitly before Insert."),
            _ => throw new InvalidOperationException($"Unsupported Id kind: {accessor.Kind}")
        };

    // ── IDocumentStore ──────────────────────────────────────────────────

    public IDocumentQuery<T> Query<T>(JsonTypeInfo<T>? jsonTypeInfo = null) where T : class
    {
        var typeInfo = this.FindTypeInfo(jsonTypeInfo);
        return new DynamoDbDocumentQuery<T>(this, typeInfo);
    }

    public Task Insert<T>(T document, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class
        => this.Tracker.Track("insert", typeof(T).Name, () => this.InsertImpl(document, jsonTypeInfo, cancellationToken));

    async Task InsertImpl<T>(T document, JsonTypeInfo<T>? jsonTypeInfo, CancellationToken cancellationToken) where T : class
    {
        var write = await this.BeginWriteAsync(DocumentOperation.Insert, document, null, jsonTypeInfo, cancellationToken).ConfigureAwait(false);
        if (!write.Proceed)
            return;
        var (typeInfo, typeName, versionMapping) = (write.TypeInfo, write.TypeName, write.VersionMapping);
        var accessor = write.Accessor;
        document = write.Doc;
        var partitionKey = this.options.ResolvePartitionKey(typeof(T), typeName);
        await this.EnsureTableAsync(cancellationToken).ConfigureAwait(false);

        var id = this.ResolveInsertId(write, accessor => this.GenerateId(accessor));

        versionMapping?.SetVersion(document, 1);
        var now = DateTimeOffset.UtcNow.ToString("o");
        var preparedBlobs = this.PrepareBlobs(document);
        var json = GuardBodySize(Serialize(document, typeInfo, this.jsonOptions), typeName, id);
        var item = this.BuildItem(typeof(T), partitionKey, id, json, now, now, versionMapping != null ? 1 : null);

        this.Log($"DynamoDB PUT (insert) {this.TableName} pk={partitionKey} sk={id}");
        await this.SyncBlobsAsync<T>(id, typeName, preparedBlobs, prune: false, cancellationToken).ConfigureAwait(false);
        try
        {
            await this.client.PutItemAsync(new PutItemRequest
            {
                TableName = this.TableName,
                Item = item,
                ConditionExpression = "attribute_not_exists(sk)"
            }, cancellationToken).ConfigureAwait(false);
        }
        catch (ConditionalCheckFailedException ex)
        {
            throw new InvalidOperationException(
                $"A document of type '{typeName}' with Id '{id}' already exists.", ex);
        }
        await this.CompleteWriteAsync(write, id, versionMapping?.GetVersion(document) ?? 1, DocumentChangeType.Inserted, document, cancellationToken).ConfigureAwait(false);
    }

    public Task<int> BatchInsert<T>(IEnumerable<T> documents, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class
        => this.Tracker.Track("batch_insert", typeof(T).Name, () => this.BatchInsertImpl(documents, jsonTypeInfo, cancellationToken), r => r);

    async Task<int> BatchInsertImpl<T>(IEnumerable<T> documents, JsonTypeInfo<T>? jsonTypeInfo, CancellationToken cancellationToken) where T : class
    {
        var typeInfo = this.FindTypeInfo(jsonTypeInfo);
        var accessor = this.idCache.GetOrCreate(typeInfo);
        var typeName = this.ResolveTypeName<T>();
        var partitionKey = this.options.ResolvePartitionKey(typeof(T), typeName);
        var versionMapping = this.options.ResolveVersionMapping(typeof(T));
        await this.EnsureTableAsync(cancellationToken).ConfigureAwait(false);

        var srcList = documents as IReadOnlyList<T> ?? documents.ToList();

        DocumentWriteContext[]? ctxs = null;
        if (this.HasPerDocInterceptors)
        {
            var mutable = srcList.ToList();
            ctxs = await this.RunBeforeWriteBatchAsync(mutable, typeName, cancellationToken).ConfigureAwait(false);
            srcList = mutable;
        }

        var items = new List<Dictionary<string, AttributeValue>>(srcList.Count);
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
            items.Add(this.BuildItem(typeof(T), partitionKey, id, json, now, now, versionMapping != null ? 1 : null));
        }

        if (items.Count == 0)
            return 0;

        this.Log($"DynamoDB BATCH WRITE {items.Count} docs into {this.TableName}");
        foreach (var chunk in items.Chunk(BatchWriteChunkSize))
        {
            var writes = chunk
                .Select(i => new WriteRequest { PutRequest = new PutRequest { Item = i } })
                .ToList();
            await this.BatchWriteWithRetryAsync(writes, cancellationToken).ConfigureAwait(false);
        }

        for (var i = 0; i < srcList.Count; i++)
        {
            if (ctxs != null)
                await this.RunAfterWriteAsync(ctxs[i], accessor.GetIdAsString(srcList[i]), versionMapping?.GetVersion(srcList[i]) ?? 1, cancellationToken).ConfigureAwait(false);
        }
        foreach (var document in srcList)
            this.PublishChange(DocumentChangeType.Inserted, accessor.GetIdAsString(document), document);
        return items.Count;
    }

    async Task BatchWriteWithRetryAsync(List<WriteRequest> writes, CancellationToken ct)
    {
        var pending = writes;
        for (var attempt = 0; attempt < 5 && pending.Count > 0; attempt++)
        {
            var response = await this.client.BatchWriteItemAsync(new BatchWriteItemRequest
            {
                RequestItems = new Dictionary<string, List<WriteRequest>> { [this.TableName] = pending }
            }, ct).ConfigureAwait(false);

            if (response.UnprocessedItems != null && response.UnprocessedItems.TryGetValue(this.TableName, out var unprocessed) && unprocessed.Count > 0)
            {
                pending = unprocessed;
                await Task.Delay(50 * (attempt + 1), ct).ConfigureAwait(false);
            }
            else
            {
                return;
            }
        }
        if (pending.Count > 0)
            throw new InvalidOperationException(
                $"DynamoDB batch write left {pending.Count} unprocessed items after retries on table '{this.TableName}'.");
    }

    public Task Update<T>(T document, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class
        => this.Tracker.Track("update", typeof(T).Name, () => this.UpdateImpl(document, jsonTypeInfo, cancellationToken));

    async Task UpdateImpl<T>(T document, JsonTypeInfo<T>? jsonTypeInfo, CancellationToken cancellationToken) where T : class
    {
        var write = await this.BeginWriteAsync(DocumentOperation.Update, document, null, jsonTypeInfo, cancellationToken).ConfigureAwait(false);
        if (!write.Proceed)
            return;
        var (typeInfo, typeName, versionMapping) = (write.TypeInfo, write.TypeName, write.VersionMapping);
        var accessor = write.Accessor;
        document = write.Doc;
        var partitionKey = this.options.ResolvePartitionKey(typeof(T), typeName);
        await this.EnsureTableAsync(cancellationToken).ConfigureAwait(false);

        var id = this.RequireDocumentId(write);
        var existing = await this.GetItemAsync(partitionKey, id, cancellationToken).ConfigureAwait(false);
        if (existing == null || !this.PassesFiltersForStored<T>(existing, typeInfo))
            throw new InvalidOperationException(
                $"No document of type '{typeName}' with Id '{id}' was found to update.");

        int? expectedVersion = null;
        if (versionMapping != null)
        {
            var ev = versionMapping.GetVersion(document);
            var storedVersion = DynamoDbDocument.GetVersion(existing);
            if (storedVersion != ev)
                throw new ConcurrencyException(typeName, id, ev, storedVersion);
            versionMapping.SetVersion(document, ev + 1);
            expectedVersion = ev;
        }

        var now = DateTimeOffset.UtcNow.ToString("o");
        var preparedBlobs = this.PrepareBlobs(document);
        var json = GuardBodySize(Serialize(document, typeInfo, this.jsonOptions), typeName, id);
        var item = this.BuildItem(typeof(T), partitionKey, id, json, DynamoDbDocument.GetCreatedAt(existing), now, versionMapping != null ? expectedVersion + 1 : null);

        await this.SyncBlobsAsync<T>(id, typeName, preparedBlobs, prune: true, cancellationToken).ConfigureAwait(false);
        await this.PutWithVersionGuardAsync(item, typeName, id, expectedVersion, cancellationToken).ConfigureAwait(false);
        await this.CompleteWriteAsync(write, id, versionMapping?.GetVersion(document), DocumentChangeType.Updated, document, cancellationToken).ConfigureAwait(false);
    }

    public Task Upsert<T>(T patch, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class
        => this.Tracker.Track("upsert", typeof(T).Name, () => this.UpsertImpl(patch, jsonTypeInfo, cancellationToken));

    async Task UpsertImpl<T>(T patch, JsonTypeInfo<T>? jsonTypeInfo, CancellationToken cancellationToken) where T : class
    {
        var write = await this.BeginWriteAsync(DocumentOperation.Upsert, patch, null, jsonTypeInfo, cancellationToken).ConfigureAwait(false);
        if (!write.Proceed)
            return;
        var (typeInfo, typeName, versionMapping) = (write.TypeInfo, write.TypeName, write.VersionMapping);
        var accessor = write.Accessor;
        patch = write.Doc;
        var partitionKey = this.options.ResolvePartitionKey(typeof(T), typeName);
        await this.EnsureTableAsync(cancellationToken).ConfigureAwait(false);

        if (accessor.IsDefaultId(patch))
            throw new InvalidOperationException(
                $"Upsert requires a non-default Id on the document. " +
                $"Set the Id property on '{typeof(T).Name}' before calling Upsert.");

        var id = accessor.GetIdAsString(patch);
        var existing = await this.GetItemAsync(partitionKey, id, cancellationToken).ConfigureAwait(false);
        var now = DateTimeOffset.UtcNow.ToString("o");
        await this.SyncBlobsAsync<T>(id, typeName, this.PrepareBlobs(patch), prune: false, cancellationToken).ConfigureAwait(false);

        if (existing == null)
        {
            versionMapping?.SetVersion(patch, 1);
            var patchJson = GuardBodySize(StripNullProperties(Serialize(patch, typeInfo, this.jsonOptions)), typeName, id);
            var item = this.BuildItem(typeof(T), partitionKey, id, patchJson, now, now, versionMapping != null ? 1 : null);

            this.Log($"DynamoDB UPSERT (insert) {this.TableName} pk={partitionKey} sk={id}");
            await this.client.PutItemAsync(new PutItemRequest { TableName = this.TableName, Item = item }, cancellationToken).ConfigureAwait(false);
        }
        else
        {
            int? guardVersion = null;
            if (versionMapping != null)
            {
                var expectedVersion = versionMapping.GetVersion(patch);
                var storedVersion = DynamoDbDocument.GetVersion(existing);
                if (expectedVersion > 0 && storedVersion != expectedVersion)
                    throw new ConcurrencyException(typeName, id, expectedVersion, storedVersion);
                versionMapping.SetVersion(patch, storedVersion + 1);
                if (expectedVersion > 0)
                    guardVersion = expectedVersion;
            }

            var patchJson = StripNullProperties(Serialize(patch, typeInfo, this.jsonOptions));
            var merged = GuardBodySize(MergeJson(DynamoDbDocument.GetData(existing), patchJson), typeName, id);
            var newVersion = versionMapping != null ? versionMapping.GetVersion(patch) : (int?)null;
            var item = this.BuildItem(typeof(T), partitionKey, id, merged, DynamoDbDocument.GetCreatedAt(existing), now, newVersion);

            this.Log($"DynamoDB UPSERT (merge) {this.TableName} pk={partitionKey} sk={id}");
            await this.PutWithVersionGuardAsync(item, typeName, id, guardVersion, cancellationToken).ConfigureAwait(false);
        }

        await this.CompleteWriteAsync(write, id, versionMapping?.GetVersion(patch), DocumentChangeType.Updated, patch, cancellationToken).ConfigureAwait(false);
    }

    async Task PutWithVersionGuardAsync(Dictionary<string, AttributeValue> item, string typeName, string id, int? expectedVersion, CancellationToken ct)
    {
        var request = new PutItemRequest { TableName = this.TableName, Item = item };
        if (expectedVersion != null)
        {
            request.ConditionExpression = "#v = :expected";
            request.ExpressionAttributeNames = new Dictionary<string, string> { ["#v"] = DynamoDbDocument.VersionAttr };
            request.ExpressionAttributeValues = new Dictionary<string, AttributeValue>
            {
                [":expected"] = new AttributeValue { N = expectedVersion.Value.ToString(System.Globalization.CultureInfo.InvariantCulture) }
            };
        }

        try
        {
            await this.client.PutItemAsync(request, ct).ConfigureAwait(false);
        }
        catch (ConditionalCheckFailedException) when (expectedVersion != null)
        {
            throw new ConcurrencyException(typeName, id, expectedVersion.Value);
        }
    }

    public Task<bool> SetProperty<T>(object id, Expression<Func<T, object>> property, object? value, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class
        => this.Tracker.Track("set_property", typeof(T).Name, () => this.SetPropertyImpl(id, property, value, jsonTypeInfo, cancellationToken), r => r ? 1 : 0);

    [UnconditionalSuppressMessage("Trimming", "IL2026", Justification = "Value serialization uses reflection when type is unknown.")]
    [UnconditionalSuppressMessage("AOT", "IL3050", Justification = "Value serialization uses reflection when type is unknown.")]
    async Task<bool> SetPropertyImpl<T>(object id, Expression<Func<T, object>> property, object? value, JsonTypeInfo<T>? jsonTypeInfo, CancellationToken cancellationToken) where T : class
    {
        var typeInfo = this.FindTypeInfo(jsonTypeInfo);
        var resolvedId = this.idCache.GetOrCreate(typeInfo).ResolveId(id);
        var jsonPath = ResolvePropertyPath(property, this.jsonOptions, typeInfo);
        var typeName = this.ResolveTypeName<T>();
        var partitionKey = this.options.ResolvePartitionKey(typeof(T), typeName);
        await this.EnsureTableAsync(cancellationToken).ConfigureAwait(false);

        var existing = await this.GetItemAsync(partitionKey, resolvedId, cancellationToken).ConfigureAwait(false);
        if (existing == null || !this.PassesFiltersForStored<T>(existing, typeInfo))
            return false;

        var node = JsonNode.Parse(DynamoDbDocument.GetData(existing))!.AsObject();
        SetNestedProperty(node, jsonPath, value == null ? null : JsonNode.Parse(JsonSerializer.Serialize(value, this.jsonOptions)));
        var item = this.BuildItem(typeof(T), partitionKey, resolvedId, GuardBodySize(node.ToJsonString(), typeName, resolvedId),
            DynamoDbDocument.GetCreatedAt(existing), DateTimeOffset.UtcNow.ToString("o"), NullableVersion(existing));

        this.Log($"DynamoDB SET PROPERTY {this.TableName} sk={resolvedId} Path={jsonPath}");
        await this.client.PutItemAsync(new PutItemRequest { TableName = this.TableName, Item = item }, cancellationToken).ConfigureAwait(false);
        return true;
    }

    public Task<bool> RemoveProperty<T>(object id, Expression<Func<T, object>> property, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class
        => this.Tracker.Track("remove_property", typeof(T).Name, () => this.RemovePropertyImpl(id, property, jsonTypeInfo, cancellationToken), r => r ? 1 : 0);

    async Task<bool> RemovePropertyImpl<T>(object id, Expression<Func<T, object>> property, JsonTypeInfo<T>? jsonTypeInfo, CancellationToken cancellationToken) where T : class
    {
        var typeInfo = this.FindTypeInfo(jsonTypeInfo);
        var resolvedId = this.idCache.GetOrCreate(typeInfo).ResolveId(id);
        var jsonPath = ResolvePropertyPath(property, this.jsonOptions, typeInfo);
        var typeName = this.ResolveTypeName<T>();
        var partitionKey = this.options.ResolvePartitionKey(typeof(T), typeName);
        await this.EnsureTableAsync(cancellationToken).ConfigureAwait(false);

        var existing = await this.GetItemAsync(partitionKey, resolvedId, cancellationToken).ConfigureAwait(false);
        if (existing == null || !this.PassesFiltersForStored<T>(existing, typeInfo))
            return false;

        var node = JsonNode.Parse(DynamoDbDocument.GetData(existing))!.AsObject();
        RemoveNestedProperty(node, jsonPath);
        var item = this.BuildItem(typeof(T), partitionKey, resolvedId, GuardBodySize(node.ToJsonString(), typeName, resolvedId),
            DynamoDbDocument.GetCreatedAt(existing), DateTimeOffset.UtcNow.ToString("o"), NullableVersion(existing));

        this.Log($"DynamoDB REMOVE PROPERTY {this.TableName} sk={resolvedId} Path={jsonPath}");
        await this.client.PutItemAsync(new PutItemRequest { TableName = this.TableName, Item = item }, cancellationToken).ConfigureAwait(false);
        return true;
    }

    public Task<T?> Get<T>(object id, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class
        => this.Tracker.Track("get", typeof(T).Name, () => this.GetImpl(id, jsonTypeInfo, cancellationToken), r => r is null ? 0 : 1);

    async Task<T?> GetImpl<T>(object id, JsonTypeInfo<T>? jsonTypeInfo, CancellationToken cancellationToken) where T : class
    {
        var typeInfo = this.FindTypeInfo(jsonTypeInfo);
        var resolvedId = this.idCache.GetOrCreate(typeInfo).ResolveId(id);
        var typeName = this.ResolveTypeName<T>();
        var partitionKey = this.options.ResolvePartitionKey(typeof(T), typeName);
        await this.EnsureTableAsync(cancellationToken).ConfigureAwait(false);

        this.Log($"DynamoDB GET {this.TableName} pk={partitionKey} sk={resolvedId}");
        var existing = await this.GetItemAsync(partitionKey, resolvedId, cancellationToken).ConfigureAwait(false);
        if (existing == null)
            return null;

        var doc = this.Materialize(DynamoDbDocument.GetData(existing), typeInfo);
        if (doc != null && !this.PassesGlobalFilters(doc))
            return null;
        return doc;
    }

    public Task<JsonPatchDocument<T>?> GetDiff<T>(object id, T modified, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class
        => this.Tracker.Track("get_diff", typeof(T).Name, () => this.GetDiffImpl(id, modified, jsonTypeInfo, cancellationToken), r => r is null ? 0 : 1);

    async Task<JsonPatchDocument<T>?> GetDiffImpl<T>(object id, T modified, JsonTypeInfo<T>? jsonTypeInfo, CancellationToken cancellationToken) where T : class
    {
        var typeInfo = this.FindTypeInfo(jsonTypeInfo);
        var resolvedId = this.idCache.GetOrCreate(typeInfo).ResolveId(id);
        var typeName = this.ResolveTypeName<T>();
        var partitionKey = this.options.ResolvePartitionKey(typeof(T), typeName);
        await this.EnsureTableAsync(cancellationToken).ConfigureAwait(false);

        var existing = await this.GetItemAsync(partitionKey, resolvedId, cancellationToken).ConfigureAwait(false);
        if (existing == null || !this.PassesFiltersForStored<T>(existing, typeInfo))
            return null;

        var modifiedJson = Serialize(modified, typeInfo, this.jsonOptions);
        return JsonDiff.CreatePatch<T>(DynamoDbDocument.GetData(existing), modifiedJson, this.jsonOptions);
    }

    /// <summary>
    /// Runs a raw <b>PartiQL</b> <c>WHERE</c> fragment (DynamoDB's native query language) scoped to the
    /// type's partition. The condition targets top-level attributes — the promoted attributes declared with
    /// <c>MapIndexedProperty&lt;T&gt;</c> (referenced by their CLR/JSON property name) — not fields inside
    /// the opaque JSON body. <c>parameters</c> supplies <c>@name</c> token substitutions.
    /// </summary>
    public Task<IReadOnlyList<T>> Query<T>(string whereClause, JsonTypeInfo<T>? jsonTypeInfo = null, object? parameters = null, CancellationToken cancellationToken = default) where T : class
        => this.Tracker.Track("query", typeof(T).Name, () => this.QueryImpl(whereClause, jsonTypeInfo, parameters, cancellationToken), r => r.Count);

    async Task<IReadOnlyList<T>> QueryImpl<T>(string whereClause, JsonTypeInfo<T>? jsonTypeInfo, object? parameters, CancellationToken cancellationToken) where T : class
    {
        var typeInfo = this.FindTypeInfo(jsonTypeInfo);
        var results = new List<T>();
        await foreach (var doc in this.QueryStream(whereClause, typeInfo, parameters, cancellationToken).ConfigureAwait(false))
            results.Add(doc);
        return results;
    }

    public IAsyncEnumerable<T> QueryStream<T>(string whereClause, JsonTypeInfo<T>? jsonTypeInfo = null, object? parameters = null, CancellationToken cancellationToken = default) where T : class
        => this.Tracker.TrackStream("query_stream", typeof(T).Name, this.QueryStreamImpl(whereClause, jsonTypeInfo, parameters, cancellationToken), cancellationToken);

    async IAsyncEnumerable<T> QueryStreamImpl<T>(string whereClause, JsonTypeInfo<T>? jsonTypeInfo, object? parameters, [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken) where T : class
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(whereClause);
        var typeInfo = this.FindTypeInfo(jsonTypeInfo);
        var partitionKey = this.ResolvePartitionKey<T>();
        await this.EnsureTableAsync(cancellationToken).ConfigureAwait(false);

        var condition = this.RewriteAttributes<T>(BindParameters(whereClause, parameters));
        var statement = $"SELECT * FROM \"{this.TableName}\" WHERE pk = '{partitionKey.Replace("'", "''")}' AND ({condition})";
        this.Log($"DynamoDB PARTIQL {statement}");

        string? nextToken = null;
        do
        {
            var resp = await this.client.ExecuteStatementAsync(new ExecuteStatementRequest
            {
                Statement = statement,
                NextToken = nextToken,
                ConsistentRead = this.options.ConsistentRead
            }, cancellationToken).ConfigureAwait(false);

            foreach (var item in resp.Items)
            {
                var doc = this.Materialize(DynamoDbDocument.GetData(item), typeInfo);
                if (doc != null && this.PassesGlobalFilters(doc))
                    yield return doc;
            }
            nextToken = resp.NextToken;
        }
        while (nextToken != null);
    }

    // Rewrites bare promoted-property names in a user PartiQL condition to their native idx_ attribute names.
    string RewriteAttributes<T>(string condition) where T : class
    {
        foreach (var m in this.ResolveIndexed(typeof(T)))
        {
            var clrLeaf = m.ClrPath.Contains('.') ? m.ClrPath[(m.ClrPath.LastIndexOf('.') + 1)..] : m.ClrPath;
            condition = System.Text.RegularExpressions.Regex.Replace(
                condition, $@"\b{System.Text.RegularExpressions.Regex.Escape(clrLeaf)}\b", m.AttributeName);
        }
        return condition;
    }

    static string BindParameters(string clause, object? parameters)
    {
        if (parameters == null)
            return clause;
        foreach (var (key, value) in EnumerateParameters(parameters))
            clause = clause.Replace("@" + key, PartiQlLiteral(value));
        return clause;
    }

    static string PartiQlLiteral(object? value) => value switch
    {
        null => "NULL",
        bool b => b ? "true" : "false",
        string s => "'" + s.Replace("'", "''") + "'",
        Guid g => "'" + g + "'",
        IFormattable f => f.ToString(null, System.Globalization.CultureInfo.InvariantCulture),
        _ => "'" + value + "'"
    };

    [UnconditionalSuppressMessage("Trimming", "IL2075",
        Justification = "Reflection over an anonymous-type parameter bag. DAM cannot be applied to an object "
                      + "parameter (IL2098), so this branch is genuinely not trim-safe: under trimming/AOT pass the "
                      + "IDictionary<string, object?> overload, which takes the fully-analyzable path above.")]
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

    public Task<int> Count<T>(string? whereClause = null, object? parameters = null, CancellationToken cancellationToken = default) where T : class
        => this.Tracker.Track("count", typeof(T).Name, () => this.CountImpl<T>(whereClause, parameters, cancellationToken), r => r);

    async Task<int> CountImpl<T>(string? whereClause, object? parameters, CancellationToken cancellationToken) where T : class
    {
        if (!string.IsNullOrWhiteSpace(whereClause))
        {
            var n = 0;
            await foreach (var _ in this.QueryStream<T>(whereClause, null, parameters, cancellationToken).ConfigureAwait(false))
                n++;
            return n;
        }

        var typeInfo = this.FindTypeInfo<T>(null);
        var partitionKey = this.ResolvePartitionKey<T>();
        var hasFilters = this.options.ResolveQueryFilters(typeof(T)).Count > 0;

        this.Log($"DynamoDB COUNT {this.TableName} pk={partitionKey}");
        if (!hasFilters)
        {
            var count = 0;
            await this.EnsureTableAsync(cancellationToken).ConfigureAwait(false);
            Dictionary<string, AttributeValue>? lastKey = null;
            do
            {
                var resp = await this.client.QueryAsync(new QueryRequest
                {
                    TableName = this.TableName,
                    KeyConditionExpression = "pk = :pk",
                    ExpressionAttributeValues = new Dictionary<string, AttributeValue> { [":pk"] = new() { S = partitionKey } },
                    Select = Select.COUNT,
                    ExclusiveStartKey = lastKey,
                    ConsistentRead = this.options.ConsistentRead
                }, cancellationToken).ConfigureAwait(false);
                count += resp.Count ?? 0;
                lastKey = resp.LastEvaluatedKey is { Count: > 0 } ? resp.LastEvaluatedKey : null;
            }
            while (lastKey != null);
            return count;
        }

        var filtered = 0;
        await foreach (var doc in this.LoadDocumentsAsync(typeInfo, cancellationToken).ConfigureAwait(false))
            if (this.PassesGlobalFilters(doc))
                filtered++;
        return filtered;
    }

    public Task<bool> Remove<T>(object id, CancellationToken cancellationToken = default) where T : class
        => this.Tracker.Track("remove", typeof(T).Name, () => this.RemoveImpl<T>(id, cancellationToken), r => r ? 1 : 0);

    async Task<bool> RemoveImpl<T>(object id, CancellationToken cancellationToken) where T : class
    {
        var resolvedId = this.idCache.GetOrCreate<T>(null).ResolveId(id);
        var typeName = this.ResolveTypeName<T>();
        var partitionKey = this.options.ResolvePartitionKey(typeof(T), typeName);
        await this.EnsureTableAsync(cancellationToken).ConfigureAwait(false);

        var write = await this.BeginWriteAsync<T>(DocumentOperation.Delete, null, id, null, cancellationToken).ConfigureAwait(false);
        if (!write.Proceed)
            return write.CancelResult;

        if (this.options.ResolveQueryFilters(typeof(T)).Count > 0)
        {
            var existing = await this.GetItemAsync(partitionKey, resolvedId, cancellationToken).ConfigureAwait(false);
            if (existing == null || !this.PassesFiltersForStored<T>(existing, null))
                return false;
        }

        this.Log($"DynamoDB DELETE {this.TableName} pk={partitionKey} sk={resolvedId}");
        var response = await this.client.DeleteItemAsync(new DeleteItemRequest
        {
            TableName = this.TableName,
            Key = DynamoDbDocument.Key(partitionKey, resolvedId),
            ReturnValues = ReturnValue.ALL_OLD
        }, cancellationToken).ConfigureAwait(false);

        var existed = response.Attributes is { Count: > 0 };
        if (existed)
        {
            await this.DeleteBlobsAsync<T>(resolvedId, typeName, cancellationToken).ConfigureAwait(false);
            await this.RunAfterWriteAsync(write.Context, id, null, cancellationToken).ConfigureAwait(false);
            this.PublishChange<T>(DocumentChangeType.Removed, resolvedId, null);
        }
        return existed;
    }

    public Task<int> Clear<T>(CancellationToken cancellationToken = default) where T : class
        => this.Tracker.Track("clear", typeof(T).Name, () => this.ClearImpl<T>(cancellationToken), r => r);

    async Task<int> ClearImpl<T>(CancellationToken cancellationToken) where T : class
    {
        var typeInfo = this.FindTypeInfo<T>(null);
        var typeName = this.ResolveTypeName<T>();
        var partitionKey = this.options.ResolvePartitionKey(typeof(T), typeName);
        await this.EnsureTableAsync(cancellationToken).ConfigureAwait(false);
        var hasFilters = this.options.ResolveQueryFilters(typeof(T)).Count > 0;

        var bulkCtx = this.NewBulkContext<T>(DocumentOperation.Clear, typeName);
        if (!await this.RunBeforeBulkAsync(bulkCtx, cancellationToken).ConfigureAwait(false))
            return bulkCtx!.CancelAffected;

        this.Log($"DynamoDB CLEAR {this.TableName} pk={partitionKey}");
        var keys = new List<string>();
        await foreach (var item in this.QueryPartitionAsync(partitionKey, cancellationToken).ConfigureAwait(false))
        {
            if (hasFilters)
            {
                var doc = this.Materialize(DynamoDbDocument.GetData(item), typeInfo);
                if (doc == null || !this.PassesGlobalFilters(doc))
                    continue;
            }
            keys.Add(item[DynamoDbDocument.Sk].S);
        }

        var count = await this.DeleteKeysAsync(partitionKey, keys, cancellationToken).ConfigureAwait(false);
        await this.RunAfterBulkAsync(bulkCtx, count, cancellationToken).ConfigureAwait(false);
        if (count > 0)
            this.PublishChange<T>(DocumentChangeType.Cleared, "", null);
        return count;
    }

    /// <inheritdoc />
    public async Task ClearAll(CancellationToken cancellationToken = default)
    {
        await this.EnsureTableAsync(cancellationToken).ConfigureAwait(false);
        this.Log($"DynamoDB CLEAR ALL {this.TableName}");

        var byPartition = new Dictionary<string, List<string>>();
        Dictionary<string, AttributeValue>? lastKey = null;
        do
        {
            var resp = await this.client.ScanAsync(new ScanRequest
            {
                TableName = this.TableName,
                ProjectionExpression = "pk, sk",
                ExclusiveStartKey = lastKey
            }, cancellationToken).ConfigureAwait(false);

            foreach (var item in resp.Items)
            {
                var pk = item[DynamoDbDocument.Pk].S;
                if (!byPartition.TryGetValue(pk, out var list))
                    byPartition[pk] = list = new List<string>();
                list.Add(item[DynamoDbDocument.Sk].S);
            }
            lastKey = resp.LastEvaluatedKey is { Count: > 0 } ? resp.LastEvaluatedKey : null;
        }
        while (lastKey != null);

        foreach (var (partitionKey, keys) in byPartition)
            await this.DeleteKeysAsync(partitionKey, keys, cancellationToken).ConfigureAwait(false);
    }

    async Task<int> DeleteKeysAsync(string partitionKey, IReadOnlyList<string> ids, CancellationToken ct)
    {
        var deleted = 0;
        foreach (var chunk in ids.Chunk(BatchWriteChunkSize))
        {
            var writes = chunk
                .Select(sk => new WriteRequest { DeleteRequest = new DeleteRequest { Key = DynamoDbDocument.Key(partitionKey, sk) } })
                .ToList();
            await this.BatchWriteWithRetryAsync(writes, ct).ConfigureAwait(false);
            deleted += chunk.Length;
        }
        return deleted;
    }

    // ── Batch override (native batch delete) ─────────────────────────────

    public Task<int> BatchRemove<T>(IEnumerable<object> ids, CancellationToken cancellationToken = default) where T : class
        => this.Tracker.Track("batch_remove", typeof(T).Name, () => this.BatchRemoveImpl<T>(ids, cancellationToken), r => r);

    async Task<int> BatchRemoveImpl<T>(IEnumerable<object> ids, CancellationToken cancellationToken) where T : class
    {
        var accessor = this.idCache.GetOrCreate<T>(null);
        var partitionKey = this.ResolvePartitionKey<T>();
        await this.EnsureTableAsync(cancellationToken).ConfigureAwait(false);

        var existing = new List<string>();
        foreach (var id in ids)
        {
            var sk = accessor.ResolveId(id);
            var item = await this.GetItemAsync(partitionKey, sk, cancellationToken).ConfigureAwait(false);
            if (item != null)
                existing.Add(sk);
        }
        return await this.DeleteKeysAsync(partitionKey, existing, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />

    Task IUnitOfWorkEngine.RunUnitAsync(Func<IDocumentStore, CancellationToken, Task> work, CancellationToken cancellationToken)
        => this.Tracker.Track("transaction", "(transaction)", () => this.RunUnitImpl(work, cancellationToken));

    async Task RunUnitImpl(Func<IDocumentStore, CancellationToken, Task> work, CancellationToken cancellationToken)
    {
        var tracker = new DynamoDbTransactionalStore(this);
        var buffer = new List<Action>();
        this.PendingChanges.Value = buffer;
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
            this.PendingChanges.Value = null;
        }
        foreach (var emit in buffer)
            emit();
    }

    // ── Internal helpers used by DynamoDbDocumentQuery ──────────────────

    internal IAsyncEnumerable<T> LoadDocumentsAsync<T>(JsonTypeInfo<T>? typeInfo, CancellationToken ct = default) where T : class
        => this.LoadDocumentsAsync(typeInfo, null, ct);

    // The optional pushdown is a FilterExpression over promoted attributes that shrinks the candidate set;
    // the query re-applies the full predicate client-side, so it need only be a superset.
    internal async IAsyncEnumerable<T> LoadDocumentsAsync<T>(JsonTypeInfo<T>? typeInfo, (string Expression, Dictionary<string, string> Names, Dictionary<string, AttributeValue> Values)? pushdown, [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct = default) where T : class
    {
        var partitionKey = this.ResolvePartitionKey<T>();
        await this.EnsureTableAsync(ct).ConfigureAwait(false);
        await foreach (var item in this.QueryPartitionAsync(partitionKey, pushdown, ct).ConfigureAwait(false))
        {
            var doc = this.Materialize(DynamoDbDocument.GetData(item), typeInfo);
            if (doc != null)
                yield return doc;
        }
    }

    internal (string Expression, Dictionary<string, string> Names, Dictionary<string, AttributeValue> Values)? BuildPushdown<T>(IEnumerable<Expression<Func<T, bool>>> predicates) where T : class
    {
        var mappings = this.ResolveIndexed(typeof(T));
        if (mappings.Count == 0)
            return null;
        var byPath = mappings.ToDictionary(m => m.ClrPath, m => m);
        var clauses = DynamoDbPromoted.ExtractClauses(predicates, byPath);
        return DynamoDbPromoted.ToFilterExpression(clauses);
    }

    IAsyncEnumerable<Dictionary<string, AttributeValue>> QueryPartitionAsync(string partitionKey, CancellationToken ct = default)
        => this.QueryPartitionAsync(partitionKey, null, ct);

    async IAsyncEnumerable<Dictionary<string, AttributeValue>> QueryPartitionAsync(string partitionKey, (string Expression, Dictionary<string, string> Names, Dictionary<string, AttributeValue> Values)? pushdown, [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct = default)
    {
        Dictionary<string, AttributeValue>? lastKey = null;
        do
        {
            var values = new Dictionary<string, AttributeValue> { [":pk"] = new() { S = partitionKey } };
            var request = new QueryRequest
            {
                TableName = this.TableName,
                KeyConditionExpression = "pk = :pk",
                ExclusiveStartKey = lastKey,
                ConsistentRead = this.options.ConsistentRead
            };
            if (pushdown is { } f)
            {
                request.FilterExpression = f.Expression;
                request.ExpressionAttributeNames = f.Names;
                foreach (var kv in f.Values)
                    values[kv.Key] = kv.Value;
            }
            request.ExpressionAttributeValues = values;

            var resp = await this.client.QueryAsync(request, ct).ConfigureAwait(false);

            foreach (var item in resp.Items)
                yield return item;

            lastKey = resp.LastEvaluatedKey is { Count: > 0 } ? resp.LastEvaluatedKey : null;
        }
        while (lastKey != null);
    }

    internal async Task<int> DeleteWhereAsync<T>(Func<T, bool> predicate, JsonTypeInfo<T>? typeInfo, CancellationToken ct) where T : class
    {
        var partitionKey = this.ResolvePartitionKey<T>();
        await this.EnsureTableAsync(ct).ConfigureAwait(false);
        var toDelete = new List<string>();
        await foreach (var item in this.QueryPartitionAsync(partitionKey, ct).ConfigureAwait(false))
        {
            var doc = this.Materialize(DynamoDbDocument.GetData(item), typeInfo);
            if (doc != null && predicate(doc))
                toDelete.Add(item[DynamoDbDocument.Sk].S);
        }
        return await this.DeleteKeysAsync(partitionKey, toDelete, ct).ConfigureAwait(false);
    }

    [UnconditionalSuppressMessage("Trimming", "IL2026", Justification = "Value serialization uses reflection when type is unknown.")]
    [UnconditionalSuppressMessage("AOT", "IL3050", Justification = "Value serialization uses reflection when type is unknown.")]
    internal async Task<int> UpdatePropertyWhereAsync<T>(Func<T, bool> predicate, string jsonPath, object? value, JsonTypeInfo<T>? typeInfo, CancellationToken ct) where T : class
    {
        var typeName = this.ResolveTypeName<T>();
        var partitionKey = this.ResolvePartitionKey<T>();
        await this.EnsureTableAsync(ct).ConfigureAwait(false);
        var matched = new List<Dictionary<string, AttributeValue>>();
        await foreach (var item in this.QueryPartitionAsync(partitionKey, ct).ConfigureAwait(false))
        {
            var doc = this.Materialize(DynamoDbDocument.GetData(item), typeInfo);
            if (doc != null && predicate(doc))
                matched.Add(item);
        }

        var now = DateTimeOffset.UtcNow.ToString("o");
        foreach (var existing in matched)
        {
            var node = JsonNode.Parse(DynamoDbDocument.GetData(existing))!.AsObject();
            SetNestedProperty(node, jsonPath, value == null ? null : JsonNode.Parse(JsonSerializer.Serialize(value, this.jsonOptions)));
            var updated = this.BuildItem(typeof(T), partitionKey, existing[DynamoDbDocument.Sk].S,
                GuardBodySize(node.ToJsonString(), typeName, existing[DynamoDbDocument.Sk].S),
                DynamoDbDocument.GetCreatedAt(existing), now, NullableVersion(existing));
            await this.client.PutItemAsync(new PutItemRequest { TableName = this.TableName, Item = updated }, ct).ConfigureAwait(false);
        }
        return matched.Count;
    }

    internal DynamoDbDocumentStoreOptions Options => this.options;
    internal JsonSerializerOptions JsonOptions => this.jsonOptions;
    internal InterceptorPipeline InterceptorPipeline => this.options.Interceptors;
    /// <summary>Everything the shared query base needs from this store, built once per root query.</summary>
    internal DocumentQueryContext<T> BuildQueryContext<T>(JsonTypeInfo<T>? typeInfo) where T : class
        => new()
        {
            TypeName = this.ResolveTypeName<T>(),
            Tracker = this.Tracker,
            Interceptors = this.options.Interceptors,
            JsonOptions = this.jsonOptions,
            TypeInfo = typeInfo,
            Filters = QueryContextFilters.Resolve<T>(this.options.ResolveQueryFilters(typeof(T))),
            GetId = this.idCache.GetOrCreate(typeInfo).GetIdAsString
        };

    internal string ResolveTypeNameFor<T>() => this.ResolveTypeName<T>();
    internal string ResolvePartitionKeyFor<T>() => this.ResolvePartitionKey<T>();

    // ── Private helpers ─────────────────────────────────────────────────

    async Task<Dictionary<string, AttributeValue>?> GetItemAsync(string partitionKey, string id, CancellationToken ct)
    {
        var response = await this.client.GetItemAsync(new GetItemRequest
        {
            TableName = this.TableName,
            Key = DynamoDbDocument.Key(partitionKey, id),
            ConsistentRead = this.options.ConsistentRead
        }, ct).ConfigureAwait(false);
        return response.IsItemSet ? response.Item : null;
    }

    static int? NullableVersion(Dictionary<string, AttributeValue> existing)
        => existing.ContainsKey(DynamoDbDocument.VersionAttr) ? DynamoDbDocument.GetVersion(existing) : null;

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

    bool PassesFiltersForStored<T>(Dictionary<string, AttributeValue> existing, JsonTypeInfo<T>? typeInfo) where T : class
    {
        if (this.options.ResolveQueryFilters(typeof(T)).Count == 0)
            return true;
        var doc = this.Materialize(DynamoDbDocument.GetData(existing), typeInfo);
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

    sealed class DynamoDbTransactionalStore(DynamoDbDocumentStore inner) : CompensatingStore
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
            await inner.client.DeleteItemAsync(new DeleteItemRequest
            {
                TableName = inner.TableName,
                Key = DynamoDbDocument.Key(partitionKey, id)
            }, ct).ConfigureAwait(false);
        }
    }
}
