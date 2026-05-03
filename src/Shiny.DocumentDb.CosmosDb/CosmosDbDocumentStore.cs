using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Linq.Expressions;
using System.Net;
using System.Runtime.CompilerServices;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.Json.Serialization.Metadata;
using Microsoft.Azure.Cosmos;
using Shiny.DocumentDb.Internal;

namespace Shiny.DocumentDb.CosmosDb;

public class CosmosDbDocumentStore : IDocumentStore, IAsyncDisposable, IDisposable
{
    readonly CosmosDbDocumentStoreOptions options;
    readonly CosmosClient client;
    readonly bool ownsClient;
    readonly JsonSerializerOptions jsonOptions;
    readonly IdAccessorCache idCache;
    readonly Action<string>? logging;
    readonly SemaphoreSlim initSemaphore = new(1, 1);
    readonly HashSet<string> initializedContainers = new(StringComparer.OrdinalIgnoreCase);
    Database? database;

    public CosmosDbDocumentStore(CosmosDbDocumentStoreOptions options)
    {
        this.options = options;
        this.jsonOptions = options.JsonSerializerOptions ?? new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
            WriteIndented = false
        };
        this.logging = options.Logging;
        this.idCache = new IdAccessorCache(options.ResolveIdPropertyName);

        if (options.CosmosClient != null)
        {
            this.client = options.CosmosClient;
            this.ownsClient = false;
        }
        else
        {
            this.client = new CosmosClient(options.ConnectionString, new CosmosClientOptions
            {
                SerializerOptions = new CosmosSerializationOptions
                {
                    PropertyNamingPolicy = CosmosPropertyNamingPolicy.CamelCase
                }
            });
            this.ownsClient = true;
        }

        options.ResolveSpatialJsonPaths(this.jsonOptions);
    }

    public bool SupportsSpatial => this.options.spatialMappings.Count > 0;

    public void Dispose()
    {
        if (this.ownsClient)
            this.client.Dispose();
    }

    public async ValueTask DisposeAsync()
    {
        if (this.ownsClient)
            this.client.Dispose();
    }

    void Log(string message) => this.logging?.Invoke(message);

    string ResolveTypeName<T>() => TypeNameResolver.Resolve(typeof(T), this.options.TypeNameResolution);

    string ResolveContainerName<T>() => this.options.ResolveContainerName(this.ResolveTypeName<T>());

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

    async Task<Container> GetContainerAsync<T>(CancellationToken ct)
    {
        var containerName = this.ResolveContainerName<T>();
        return await this.EnsureContainerAsync(containerName, ct).ConfigureAwait(false);
    }

    async Task<Container> EnsureContainerAsync(string containerName, CancellationToken ct)
    {
        if (this.initializedContainers.Contains(containerName))
            return this.database!.GetContainer(containerName);

        await this.initSemaphore.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            if (this.initializedContainers.Contains(containerName))
                return this.database!.GetContainer(containerName);

            if (this.database == null)
            {
                var dbResponse = await this.client.CreateDatabaseIfNotExistsAsync(
                    this.options.DatabaseName, cancellationToken: ct).ConfigureAwait(false);
                this.database = dbResponse.Database;
            }

            var containerProperties = new ContainerProperties(containerName, "/typeName")
            {
                DefaultTimeToLive = -1 // No automatic expiry
            };

            // Add spatial indexes for mapped spatial properties
            foreach (var mapping in this.options.spatialMappings.Values)
            {
                containerProperties.IndexingPolicy.SpatialIndexes.Add(
                    new SpatialPath { Path = $"/data/{mapping.JsonPath}/*" });
            }

            await this.database.CreateContainerIfNotExistsAsync(
                containerProperties, this.options.DefaultThroughput, cancellationToken: ct).ConfigureAwait(false);

            this.initializedContainers.Add(containerName);
            return this.database.GetContainer(containerName);
        }
        finally
        {
            this.initSemaphore.Release();
        }
    }

    string GenerateId<T>(IdAccessor<T> accessor) where T : class
    {
        return accessor.Kind switch
        {
            IdKind.Guid => Guid.NewGuid().ToString("N"),
            IdKind.String => Guid.NewGuid().ToString(),
            // Int/Long auto-generation requires querying max — handled in GenerateIdAsync
            _ => throw new InvalidOperationException($"Use GenerateIdAsync for {accessor.Kind} IDs.")
        };
    }

    async Task<string> GenerateNumericIdAsync<T>(IdAccessor<T> accessor, string typeName, Container container, CancellationToken ct) where T : class
    {
        var query = new QueryDefinition("SELECT VALUE MAX(StringToNumber(c.id)) FROM c WHERE c.typeName = @typeName")
            .WithParameter("@typeName", typeName);

        using var iterator = container.GetItemQueryIterator<long?>(query, requestOptions: new QueryRequestOptions
        {
            PartitionKey = new PartitionKey(typeName)
        });

        long max = 0;
        while (iterator.HasMoreResults)
        {
            var response = await iterator.ReadNextAsync(ct).ConfigureAwait(false);
            foreach (var val in response)
            {
                if (val.HasValue && val.Value > max)
                    max = val.Value;
            }
        }

        return (max + 1).ToString(CultureInfo.InvariantCulture);
    }

    // ── IDocumentStore ──────────────────────────────────────────────────

    public IDocumentQuery<T> Query<T>(JsonTypeInfo<T>? jsonTypeInfo = null) where T : class
    {
        var typeInfo = this.FindTypeInfo(jsonTypeInfo);
        return new CosmosDbDocumentQuery<T>(this, typeInfo);
    }

    public async Task Insert<T>(T document, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class
    {
        var typeInfo = this.FindTypeInfo(jsonTypeInfo);
        var accessor = this.idCache.GetOrCreate(typeInfo);
        var typeName = this.ResolveTypeName<T>();
        var container = await this.GetContainerAsync<T>(cancellationToken).ConfigureAwait(false);

        string id;
        if (accessor.IsDefaultId(document))
        {
            if (accessor.Kind == IdKind.String)
                throw new InvalidOperationException(
                    $"Insert requires a non-empty string Id on '{typeof(T).Name}'. " +
                    "String Id properties are not auto-generated during Insert.");

            id = accessor.Kind is IdKind.Int or IdKind.Long
                ? await this.GenerateNumericIdAsync(accessor, typeName, container, cancellationToken).ConfigureAwait(false)
                : this.GenerateId(accessor);
            accessor.SetId(document, id);
        }
        else
        {
            id = accessor.GetIdAsString(document);
        }

        var json = Serialize(document, typeInfo, this.jsonOptions);
        var cosmosDoc = new CosmosDocument
        {
            Id = id,
            TypeName = typeName,
            Data = json,
            CreatedAt = DateTimeOffset.UtcNow.ToString("o"),
            UpdatedAt = DateTimeOffset.UtcNow.ToString("o")
        };

        this.Log($"CosmosDB CREATE {this.ResolveContainerName<T>()} Id={id}");
        try
        {
            await container.CreateItemAsync(cosmosDoc, new PartitionKey(typeName), cancellationToken: cancellationToken).ConfigureAwait(false);
        }
        catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.Conflict)
        {
            throw new InvalidOperationException(
                $"A document of type '{typeName}' with Id '{id}' already exists.", ex);
        }
    }

    public async Task<int> BatchInsert<T>(IEnumerable<T> documents, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class
    {
        var typeInfo = this.FindTypeInfo(jsonTypeInfo);
        var accessor = this.idCache.GetOrCreate(typeInfo);
        var typeName = this.ResolveTypeName<T>();
        var container = await this.GetContainerAsync<T>(cancellationToken).ConfigureAwait(false);

        var docs = new List<CosmosDocument>();
        long nextInt = -1;

        foreach (var document in documents)
        {
            string id;
            if (accessor.IsDefaultId(document))
            {
                if (accessor.Kind == IdKind.String)
                    throw new InvalidOperationException(
                        $"Insert requires a non-empty string Id on '{typeof(T).Name}'.");

                if (accessor.Kind is IdKind.Int or IdKind.Long)
                {
                    if (nextInt < 0)
                    {
                        var seed = await this.GenerateNumericIdAsync(accessor, typeName, container, cancellationToken).ConfigureAwait(false);
                        nextInt = long.Parse(seed, CultureInfo.InvariantCulture);
                    }
                    else
                    {
                        nextInt++;
                    }
                    id = nextInt.ToString(CultureInfo.InvariantCulture);
                }
                else
                {
                    id = this.GenerateId(accessor);
                }
                accessor.SetId(document, id);
            }
            else
            {
                id = accessor.GetIdAsString(document);
            }

            var json = Serialize(document, typeInfo, this.jsonOptions);
            docs.Add(new CosmosDocument
            {
                Id = id,
                TypeName = typeName,
                Data = json,
                CreatedAt = DateTimeOffset.UtcNow.ToString("o"),
                UpdatedAt = DateTimeOffset.UtcNow.ToString("o")
            });
        }

        if (docs.Count == 0)
            return 0;

        this.Log($"CosmosDB BATCH INSERT {docs.Count} docs into {this.ResolveContainerName<T>()}");

        // CosmosDB transactional batch limited to 100 items per batch
        var totalInserted = 0;
        foreach (var chunk in docs.Chunk(100))
        {
            var batch = container.CreateTransactionalBatch(new PartitionKey(typeName));
            foreach (var doc in chunk)
                batch.CreateItem(doc);

            using var batchResponse = await batch.ExecuteAsync(cancellationToken).ConfigureAwait(false);
            if (!batchResponse.IsSuccessStatusCode)
            {
                throw new InvalidOperationException(
                    $"Batch insert failed with status {batchResponse.StatusCode}. " +
                    "A document may have a duplicate Id.");
            }
            totalInserted += chunk.Length;
        }

        return totalInserted;
    }

    public async Task Update<T>(T document, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class
    {
        var typeInfo = this.FindTypeInfo(jsonTypeInfo);
        var accessor = this.idCache.GetOrCreate(typeInfo);

        if (accessor.IsDefaultId(document))
            throw new InvalidOperationException(
                $"Update requires a non-default Id on the document. " +
                $"Set the Id property on '{typeof(T).Name}' before calling Update.");

        var id = accessor.GetIdAsString(document);
        var typeName = this.ResolveTypeName<T>();
        var container = await this.GetContainerAsync<T>(cancellationToken).ConfigureAwait(false);

        // Verify exists
        try
        {
            await container.ReadItemAsync<CosmosDocument>(id, new PartitionKey(typeName), cancellationToken: cancellationToken).ConfigureAwait(false);
        }
        catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.NotFound)
        {
            throw new InvalidOperationException(
                $"No document of type '{typeName}' with Id '{id}' was found to update.");
        }

        var json = Serialize(document, typeInfo, this.jsonOptions);
        var cosmosDoc = new CosmosDocument
        {
            Id = id,
            TypeName = typeName,
            Data = json,
            CreatedAt = DateTimeOffset.UtcNow.ToString("o"), // Ideally preserve original
            UpdatedAt = DateTimeOffset.UtcNow.ToString("o")
        };

        this.Log($"CosmosDB REPLACE {this.ResolveContainerName<T>()} Id={id}");
        await container.ReplaceItemAsync(cosmosDoc, id, new PartitionKey(typeName), cancellationToken: cancellationToken).ConfigureAwait(false);
    }

    public async Task Upsert<T>(T patch, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class
    {
        var typeInfo = this.FindTypeInfo(jsonTypeInfo);
        var accessor = this.idCache.GetOrCreate(typeInfo);

        if (accessor.IsDefaultId(patch))
            throw new InvalidOperationException(
                $"Upsert requires a non-default Id on the document. " +
                $"Set the Id property on '{typeof(T).Name}' before calling Upsert.");

        var id = accessor.GetIdAsString(patch);
        var typeName = this.ResolveTypeName<T>();
        var container = await this.GetContainerAsync<T>(cancellationToken).ConfigureAwait(false);

        var patchJson = Serialize(patch, typeInfo, this.jsonOptions);
        patchJson = StripNullProperties(patchJson);
        var now = DateTimeOffset.UtcNow.ToString("o");

        // Try to read existing
        CosmosDocument? existing = null;
        try
        {
            var response = await container.ReadItemAsync<CosmosDocument>(id, new PartitionKey(typeName), cancellationToken: cancellationToken).ConfigureAwait(false);
            existing = response.Resource;
        }
        catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.NotFound)
        {
            // Will insert
        }

        if (existing == null)
        {
            var cosmosDoc = new CosmosDocument
            {
                Id = id,
                TypeName = typeName,
                Data = patchJson,
                CreatedAt = now,
                UpdatedAt = now
            };

            this.Log($"CosmosDB UPSERT (insert) {this.ResolveContainerName<T>()} Id={id}");
            await container.CreateItemAsync(cosmosDoc, new PartitionKey(typeName), cancellationToken: cancellationToken).ConfigureAwait(false);
        }
        else
        {
            var merged = MergeJson(existing.Data, patchJson);
            existing.Data = merged;
            existing.UpdatedAt = now;

            this.Log($"CosmosDB UPSERT (merge) {this.ResolveContainerName<T>()} Id={id}");
            await container.ReplaceItemAsync(existing, id, new PartitionKey(typeName), cancellationToken: cancellationToken).ConfigureAwait(false);
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
        var container = await this.GetContainerAsync<T>(cancellationToken).ConfigureAwait(false);

        try
        {
            // Read, modify, replace
            var response = await container.ReadItemAsync<CosmosDocument>(resolvedId, new PartitionKey(typeName), cancellationToken: cancellationToken).ConfigureAwait(false);
            var doc = response.Resource;
            var node = JsonNode.Parse(doc.Data)!.AsObject();
            SetNestedProperty(node, jsonPath, value == null ? null : JsonNode.Parse(JsonSerializer.Serialize(value, this.jsonOptions)));
            doc.Data = node.ToJsonString();
            doc.UpdatedAt = DateTimeOffset.UtcNow.ToString("o");

            this.Log($"CosmosDB SET PROPERTY {this.ResolveContainerName<T>()} Id={resolvedId} Path={jsonPath}");
            await container.ReplaceItemAsync(doc, resolvedId, new PartitionKey(typeName), cancellationToken: cancellationToken).ConfigureAwait(false);
            return true;
        }
        catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.NotFound)
        {
            return false;
        }
    }

    public async Task<bool> RemoveProperty<T>(object id, Expression<Func<T, object>> property, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class
    {
        var typeInfo = this.FindTypeInfo(jsonTypeInfo);
        var resolvedId = this.idCache.GetOrCreate(typeInfo).ResolveId(id);
        var jsonPath = ResolvePropertyPath(property, this.jsonOptions, typeInfo);
        var typeName = this.ResolveTypeName<T>();
        var container = await this.GetContainerAsync<T>(cancellationToken).ConfigureAwait(false);

        try
        {
            var response = await container.ReadItemAsync<CosmosDocument>(resolvedId, new PartitionKey(typeName), cancellationToken: cancellationToken).ConfigureAwait(false);
            var doc = response.Resource;
            var node = JsonNode.Parse(doc.Data)!.AsObject();
            RemoveNestedProperty(node, jsonPath);
            doc.Data = node.ToJsonString();
            doc.UpdatedAt = DateTimeOffset.UtcNow.ToString("o");

            this.Log($"CosmosDB REMOVE PROPERTY {this.ResolveContainerName<T>()} Id={resolvedId} Path={jsonPath}");
            await container.ReplaceItemAsync(doc, resolvedId, new PartitionKey(typeName), cancellationToken: cancellationToken).ConfigureAwait(false);
            return true;
        }
        catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.NotFound)
        {
            return false;
        }
    }

    public async Task<T?> Get<T>(object id, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class
    {
        var typeInfo = this.FindTypeInfo(jsonTypeInfo);
        var resolvedId = this.idCache.GetOrCreate(typeInfo).ResolveId(id);
        var typeName = this.ResolveTypeName<T>();
        var container = await this.GetContainerAsync<T>(cancellationToken).ConfigureAwait(false);

        this.Log($"CosmosDB READ {this.ResolveContainerName<T>()} Id={resolvedId}");
        try
        {
            var response = await container.ReadItemAsync<CosmosDocument>(resolvedId, new PartitionKey(typeName), cancellationToken: cancellationToken).ConfigureAwait(false);
            return Deserialize(response.Resource.Data, typeInfo, this.jsonOptions);
        }
        catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.NotFound)
        {
            return null;
        }
    }

    public async Task<JsonPatchDocument<T>?> GetDiff<T>(object id, T modified, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class
    {
        var typeInfo = this.FindTypeInfo(jsonTypeInfo);
        var resolvedId = this.idCache.GetOrCreate(typeInfo).ResolveId(id);
        var typeName = this.ResolveTypeName<T>();
        var container = await this.GetContainerAsync<T>(cancellationToken).ConfigureAwait(false);

        try
        {
            var response = await container.ReadItemAsync<CosmosDocument>(resolvedId, new PartitionKey(typeName), cancellationToken: cancellationToken).ConfigureAwait(false);
            var originalJson = response.Resource.Data;
            var modifiedJson = Serialize(modified, typeInfo, this.jsonOptions);
            return JsonDiff.CreatePatch<T>(originalJson, modifiedJson, this.jsonOptions);
        }
        catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.NotFound)
        {
            return null;
        }
    }

    public async Task<IReadOnlyList<T>> Query<T>(string whereClause, JsonTypeInfo<T>? jsonTypeInfo = null, object? parameters = null, CancellationToken cancellationToken = default) where T : class
    {
        var typeInfo = this.FindTypeInfo(jsonTypeInfo);
        var typeName = this.ResolveTypeName<T>();
        var container = await this.GetContainerAsync<T>(cancellationToken).ConfigureAwait(false);

        var sql = $"SELECT c.data FROM c WHERE c.typeName = @typeName AND ({whereClause})";
        var queryDef = new QueryDefinition(sql).WithParameter("@typeName", typeName);
        BindParameters(queryDef, parameters);

        this.Log(sql);
        return await ExecuteQueryAsync(container, queryDef, typeName, typeInfo, cancellationToken).ConfigureAwait(false);
    }

    public async IAsyncEnumerable<T> QueryStream<T>(string whereClause, JsonTypeInfo<T>? jsonTypeInfo = null, object? parameters = null, [EnumeratorCancellation] CancellationToken cancellationToken = default) where T : class
    {
        var typeInfo = this.FindTypeInfo(jsonTypeInfo);
        var typeName = this.ResolveTypeName<T>();
        var container = await this.GetContainerAsync<T>(cancellationToken).ConfigureAwait(false);

        var sql = $"SELECT c.data FROM c WHERE c.typeName = @typeName AND ({whereClause})";
        var queryDef = new QueryDefinition(sql).WithParameter("@typeName", typeName);
        BindParameters(queryDef, parameters);

        this.Log(sql);
        using var iterator = container.GetItemQueryIterator<CosmosDocument>(queryDef, requestOptions: new QueryRequestOptions
        {
            PartitionKey = new PartitionKey(typeName)
        });

        while (iterator.HasMoreResults)
        {
            var response = await iterator.ReadNextAsync(cancellationToken).ConfigureAwait(false);
            foreach (var doc in response)
            {
                var result = Deserialize(doc.Data, typeInfo, this.jsonOptions);
                if (result != null)
                    yield return result;
            }
        }
    }

    public async Task<int> Count<T>(string? whereClause = null, object? parameters = null, CancellationToken cancellationToken = default) where T : class
    {
        var typeName = this.ResolveTypeName<T>();
        var container = await this.GetContainerAsync<T>(cancellationToken).ConfigureAwait(false);

        var sql = "SELECT VALUE COUNT(1) FROM c WHERE c.typeName = @typeName";
        if (!string.IsNullOrWhiteSpace(whereClause))
            sql += $" AND ({whereClause})";

        var queryDef = new QueryDefinition(sql).WithParameter("@typeName", typeName);
        BindParameters(queryDef, parameters);

        this.Log(sql);
        using var iterator = container.GetItemQueryIterator<int>(queryDef, requestOptions: new QueryRequestOptions
        {
            PartitionKey = new PartitionKey(typeName)
        });

        var result = await iterator.ReadNextAsync(cancellationToken).ConfigureAwait(false);
        return result.FirstOrDefault();
    }

    public async Task<bool> Remove<T>(object id, CancellationToken cancellationToken = default) where T : class
    {
        var resolvedId = this.idCache.GetOrCreate<T>(null).ResolveId(id);
        var typeName = this.ResolveTypeName<T>();
        var container = await this.GetContainerAsync<T>(cancellationToken).ConfigureAwait(false);

        this.Log($"CosmosDB DELETE {this.ResolveContainerName<T>()} Id={resolvedId}");
        try
        {
            await container.DeleteItemAsync<CosmosDocument>(resolvedId, new PartitionKey(typeName), cancellationToken: cancellationToken).ConfigureAwait(false);
            return true;
        }
        catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.NotFound)
        {
            return false;
        }
    }

    public async Task<int> Clear<T>(CancellationToken cancellationToken = default) where T : class
    {
        var typeName = this.ResolveTypeName<T>();
        var container = await this.GetContainerAsync<T>(cancellationToken).ConfigureAwait(false);

        // Query all IDs, then delete each
        var sql = "SELECT c.id FROM c WHERE c.typeName = @typeName";
        var queryDef = new QueryDefinition(sql).WithParameter("@typeName", typeName);

        this.Log($"CosmosDB CLEAR {this.ResolveContainerName<T>()}");
        var ids = new List<string>();
        using var iterator = container.GetItemQueryIterator<CosmosDocument>(queryDef, requestOptions: new QueryRequestOptions
        {
            PartitionKey = new PartitionKey(typeName)
        });

        while (iterator.HasMoreResults)
        {
            var response = await iterator.ReadNextAsync(cancellationToken).ConfigureAwait(false);
            ids.AddRange(response.Select(d => d.Id));
        }

        foreach (var id in ids)
        {
            await container.DeleteItemAsync<CosmosDocument>(id, new PartitionKey(typeName), cancellationToken: cancellationToken).ConfigureAwait(false);
        }

        return ids.Count;
    }

    public async Task RunInTransaction(Func<IDocumentStore, Task> operation, CancellationToken cancellationToken = default)
    {
        var tracker = new CosmosDbTransactionalStore(this);
        try
        {
            await operation(tracker).ConfigureAwait(false);
        }
        catch
        {
            await tracker.RollbackAsync(cancellationToken).ConfigureAwait(false);
            throw;
        }
    }

    // ── Spatial queries ──────────────────────────────────────────────────

    public async Task<IReadOnlyList<SpatialResult<T>>> WithinRadius<T>(
        GeoPoint center,
        double radiusMeters,
        Expression<Func<T, bool>>? filter = null,
        CancellationToken cancellationToken = default) where T : class
    {
        var mapping = this.options.ResolveSpatialMapping(typeof(T))
            ?? throw new NotSupportedException($"No spatial property mapped for type '{typeof(T).Name}'. Call MapSpatialProperty<{typeof(T).Name}>() in options.");

        var typeInfo = this.FindTypeInfo<T>(null);
        var typeName = this.ResolveTypeName<T>();
        var container = await this.GetContainerAsync<T>(cancellationToken).ConfigureAwait(false);

        var geoJsonPoint = $"{{\"type\":\"Point\",\"coordinates\":[{center.Longitude.ToString(CultureInfo.InvariantCulture)},{center.Latitude.ToString(CultureInfo.InvariantCulture)}]}}";

        var sql = new StringBuilder();
        sql.Append($"SELECT VALUE c.data FROM c WHERE c.typeName = @typeName AND ST_DISTANCE(c.data.{mapping.JsonPath}, {geoJsonPoint}) <= @radius");

        var queryDef = new QueryDefinition(string.Empty);
        Dictionary<string, object?>? filterParams = null;

        if (filter != null)
        {
            var translated = CosmosExpressionVisitor.Translate(filter, this.jsonOptions, typeInfo);
            sql.Append($" AND ({translated.sql})");
            filterParams = translated.parameters;
        }

        queryDef = new QueryDefinition(sql.ToString())
            .WithParameter("@typeName", typeName)
            .WithParameter("@radius", radiusMeters);

        if (filterParams != null)
        {
            foreach (var kvp in filterParams)
                queryDef.WithParameter(kvp.Key, kvp.Value);
        }

        this.Log(sql.ToString());
        var docs = await this.ExecuteRawQueryAsync(container, queryDef, typeName, typeInfo, cancellationToken).ConfigureAwait(false);

        var results = new List<SpatialResult<T>>();
        foreach (var doc in docs)
        {
            var point = mapping.GetGeoPoint(doc);
            var distance = Internal.GeoMath.HaversineDistance(center, point);
            results.Add(new SpatialResult<T> { Document = doc, DistanceMeters = distance });
        }

        results.Sort((a, b) => a.DistanceMeters.CompareTo(b.DistanceMeters));
        return results;
    }

    public async Task<IReadOnlyList<T>> WithinBoundingBox<T>(
        GeoBoundingBox box,
        Expression<Func<T, bool>>? filter = null,
        CancellationToken cancellationToken = default) where T : class
    {
        var mapping = this.options.ResolveSpatialMapping(typeof(T))
            ?? throw new NotSupportedException($"No spatial property mapped for type '{typeof(T).Name}'. Call MapSpatialProperty<{typeof(T).Name}>() in options.");

        var typeInfo = this.FindTypeInfo<T>(null);
        var typeName = this.ResolveTypeName<T>();
        var container = await this.GetContainerAsync<T>(cancellationToken).ConfigureAwait(false);

        var polygon = string.Format(
            CultureInfo.InvariantCulture,
            "{{\"type\":\"Polygon\",\"coordinates\":[[[{0},{1}],[{2},{1}],[{2},{3}],[{0},{3}],[{0},{1}]]]}}",
            box.MinLongitude, box.MinLatitude, box.MaxLongitude, box.MaxLatitude);

        var sql = new StringBuilder();
        sql.Append($"SELECT VALUE c.data FROM c WHERE c.typeName = @typeName AND ST_WITHIN(c.data.{mapping.JsonPath}, {polygon})");

        Dictionary<string, object?>? filterParams = null;
        if (filter != null)
        {
            var translated = CosmosExpressionVisitor.Translate(filter, this.jsonOptions, typeInfo);
            sql.Append($" AND ({translated.sql})");
            filterParams = translated.parameters;
        }

        var queryDef = new QueryDefinition(sql.ToString())
            .WithParameter("@typeName", typeName);

        if (filterParams != null)
        {
            foreach (var kvp in filterParams)
                queryDef.WithParameter(kvp.Key, kvp.Value);
        }

        this.Log(sql.ToString());
        return await this.ExecuteRawQueryAsync(container, queryDef, typeName, typeInfo, cancellationToken).ConfigureAwait(false);
    }

    public async Task<IReadOnlyList<SpatialResult<T>>> NearestNeighbors<T>(
        GeoPoint center,
        int count,
        Expression<Func<T, bool>>? filter = null,
        CancellationToken cancellationToken = default) where T : class
    {
        var mapping = this.options.ResolveSpatialMapping(typeof(T))
            ?? throw new NotSupportedException($"No spatial property mapped for type '{typeof(T).Name}'. Call MapSpatialProperty<{typeof(T).Name}>() in options.");

        var typeInfo = this.FindTypeInfo<T>(null);
        var typeName = this.ResolveTypeName<T>();
        var container = await this.GetContainerAsync<T>(cancellationToken).ConfigureAwait(false);

        var geoJsonPoint = $"{{\"type\":\"Point\",\"coordinates\":[{center.Longitude.ToString(CultureInfo.InvariantCulture)},{center.Latitude.ToString(CultureInfo.InvariantCulture)}]}}";

        var sql = new StringBuilder();
        sql.Append($"SELECT VALUE c.data FROM c WHERE c.typeName = @typeName");

        Dictionary<string, object?>? filterParams = null;
        if (filter != null)
        {
            var translated = CosmosExpressionVisitor.Translate(filter, this.jsonOptions, typeInfo);
            sql.Append($" AND ({translated.sql})");
            filterParams = translated.parameters;
        }

        sql.Append($" ORDER BY ST_DISTANCE(c.data.{mapping.JsonPath}, {geoJsonPoint})");
        sql.Append(" OFFSET 0 LIMIT @count");

        var queryDef = new QueryDefinition(sql.ToString())
            .WithParameter("@typeName", typeName)
            .WithParameter("@count", count);

        if (filterParams != null)
        {
            foreach (var kvp in filterParams)
                queryDef.WithParameter(kvp.Key, kvp.Value);
        }

        this.Log(sql.ToString());
        var docs = await this.ExecuteRawQueryAsync(container, queryDef, typeName, typeInfo, cancellationToken).ConfigureAwait(false);

        var results = new List<SpatialResult<T>>();
        foreach (var doc in docs)
        {
            var point = mapping.GetGeoPoint(doc);
            var distance = Internal.GeoMath.HaversineDistance(center, point);
            results.Add(new SpatialResult<T> { Document = doc, DistanceMeters = distance });
        }

        return results;
    }

    async Task<IReadOnlyList<T>> ExecuteRawQueryAsync<T>(
        Container container,
        QueryDefinition queryDef,
        string typeName,
        JsonTypeInfo<T>? typeInfo,
        CancellationToken ct) where T : class
    {
        var results = new List<T>();
        using var iterator = container.GetItemQueryIterator<string>(queryDef, requestOptions: new QueryRequestOptions
        {
            PartitionKey = new PartitionKey(typeName)
        });

        while (iterator.HasMoreResults)
        {
            var response = await iterator.ReadNextAsync(ct).ConfigureAwait(false);
            foreach (var json in response)
            {
                var result = Deserialize(json, typeInfo, this.jsonOptions);
                if (result != null)
                    results.Add(result);
            }
        }

        return results;
    }

    static void SetNestedProperty(JsonObject node, string path, JsonNode? value)
    {
        var parts = path.Split('.');
        var current = node;
        for (var i = 0; i < parts.Length - 1; i++)
        {
            var next = current[parts[i]];
            if (next is JsonObject obj)
                current = obj;
            else
                return;
        }
        current[parts[^1]] = value;
    }

    static void RemoveNestedProperty(JsonObject node, string path)
    {
        var parts = path.Split('.');
        var current = node;
        for (var i = 0; i < parts.Length - 1; i++)
        {
            var next = current[parts[i]];
            if (next is JsonObject obj)
                current = obj;
            else
                return;
        }
        current.Remove(parts[^1]);
    }

    // ── Internal helpers used by CosmosDbDocumentQuery ──────────────────

    internal async Task<IReadOnlyList<T>> ExecuteQueryAsync<T>(
        Container container,
        QueryDefinition queryDef,
        string typeName,
        JsonTypeInfo<T>? typeInfo,
        CancellationToken ct) where T : class
    {
        var results = new List<T>();
        using var iterator = container.GetItemQueryIterator<CosmosDocument>(queryDef, requestOptions: new QueryRequestOptions
        {
            PartitionKey = new PartitionKey(typeName)
        });

        while (iterator.HasMoreResults)
        {
            var response = await iterator.ReadNextAsync(ct).ConfigureAwait(false);
            foreach (var doc in response)
            {
                var result = Deserialize(doc.Data, typeInfo, this.jsonOptions);
                if (result != null)
                    results.Add(result);
            }
        }

        return results.AsReadOnly();
    }

    internal async Task<long> ExecuteCountQueryAsync(
        Container container,
        QueryDefinition queryDef,
        string typeName,
        CancellationToken ct)
    {
        using var iterator = container.GetItemQueryIterator<long>(queryDef, requestOptions: new QueryRequestOptions
        {
            PartitionKey = new PartitionKey(typeName)
        });

        var response = await iterator.ReadNextAsync(ct).ConfigureAwait(false);
        return response.FirstOrDefault();
    }

    internal async Task<double> ExecuteScalarDoubleQueryAsync(
        Container container,
        QueryDefinition queryDef,
        string typeName,
        CancellationToken ct)
    {
        using var iterator = container.GetItemQueryIterator<double>(queryDef, requestOptions: new QueryRequestOptions
        {
            PartitionKey = new PartitionKey(typeName)
        });

        var response = await iterator.ReadNextAsync(ct).ConfigureAwait(false);
        return response.FirstOrDefault();
    }

    internal Task<Container> GetContainerForTypeAsync<T>(CancellationToken ct) => this.GetContainerAsync<T>(ct);
    internal string ResolveTypeNameFor<T>() => this.ResolveTypeName<T>();
    internal string ResolveContainerNameFor<T>() => this.ResolveContainerName<T>();
    internal JsonSerializerOptions JsonOptions => this.jsonOptions;
    internal IdAccessorCache IdCache => this.idCache;

    // ── Private helpers ────────────────────────────────────────────────

    static string StripNullProperties(string json)
    {
        var node = JsonNode.Parse(json);
        if (node is not JsonObject obj)
            return json;

        foreach (var key in obj.Where(kv => kv.Value is null).Select(kv => kv.Key).ToList())
            obj.Remove(key);

        return obj.ToJsonString();
    }

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

    [UnconditionalSuppressMessage("Trimming", "IL2075", Justification = "Parameter binding via reflection is intentional.")]
    static void BindParameters(QueryDefinition queryDef, object? parameters)
    {
        if (parameters is null)
            return;

        if (parameters is IDictionary<string, object> dict)
        {
            foreach (var kv in dict)
                queryDef.WithParameter(kv.Key.StartsWith('@') ? kv.Key : $"@{kv.Key}", kv.Value);
            return;
        }

        foreach (var prop in parameters.GetType().GetProperties())
        {
            var name = prop.Name.StartsWith('@') ? prop.Name : $"@{prop.Name}";
            queryDef.WithParameter(name, prop.GetValue(parameters));
        }
    }

    // ── Compensating transaction wrapper ────────────────────────────────

    sealed class CosmosDbTransactionalStore(CosmosDbDocumentStore inner) : IDocumentStore
    {
        readonly List<(string typeName, string id)> insertedDocs = new();

        public async Task Insert<T>(T document, JsonTypeInfo<T>? jsonTypeInfo, CancellationToken ct) where T : class
        {
            await inner.Insert(document, jsonTypeInfo, ct).ConfigureAwait(false);
            var typeInfo = inner.FindTypeInfo(jsonTypeInfo);
            var accessor = inner.idCache.GetOrCreate(typeInfo);
            var id = accessor.GetIdAsString(document);
            var typeName = inner.ResolveTypeName<T>();
            insertedDocs.Add((typeName, id));
        }

        internal async Task RollbackAsync(CancellationToken ct)
        {
            foreach (var (typeName, id) in insertedDocs)
            {
                try
                {
                    var containerName = inner.options.ResolveContainerName(typeName);
                    var container = await inner.EnsureContainerAsync(containerName, ct).ConfigureAwait(false);
                    await container.DeleteItemAsync<CosmosDocument>(id, new PartitionKey(typeName), cancellationToken: ct).ConfigureAwait(false);
                }
                catch
                {
                    // Best-effort rollback
                }
            }
        }

        // Delegate all other operations to inner store
        public IDocumentQuery<T> Query<T>(JsonTypeInfo<T>? ti) where T : class => inner.Query(ti);
        public Task<T?> Get<T>(object id, JsonTypeInfo<T>? ti, CancellationToken ct) where T : class => inner.Get(id, ti, ct);
        public Task Update<T>(T doc, JsonTypeInfo<T>? ti, CancellationToken ct) where T : class => inner.Update(doc, ti, ct);
        public Task Upsert<T>(T doc, JsonTypeInfo<T>? ti, CancellationToken ct) where T : class => inner.Upsert(doc, ti, ct);
        public Task<bool> SetProperty<T>(object id, Expression<Func<T, object>> p, object? v, JsonTypeInfo<T>? ti, CancellationToken ct) where T : class => inner.SetProperty(id, p, v, ti, ct);
        public Task<bool> RemoveProperty<T>(object id, Expression<Func<T, object>> p, JsonTypeInfo<T>? ti, CancellationToken ct) where T : class => inner.RemoveProperty(id, p, ti, ct);
        public Task<JsonPatchDocument<T>?> GetDiff<T>(object id, T modified, JsonTypeInfo<T>? ti, CancellationToken ct) where T : class => inner.GetDiff(id, modified, ti, ct);
        public Task<IReadOnlyList<T>> Query<T>(string where, JsonTypeInfo<T>? ti, object? p, CancellationToken ct) where T : class => inner.Query(where, ti, p, ct);
        public IAsyncEnumerable<T> QueryStream<T>(string where, JsonTypeInfo<T>? ti, object? p, CancellationToken ct) where T : class => inner.QueryStream(where, ti, p, ct);
        public Task<int> Count<T>(string? where, object? p, CancellationToken ct) where T : class => inner.Count<T>(where, p, ct);
        public Task<bool> Remove<T>(object id, CancellationToken ct) where T : class => inner.Remove<T>(id, ct);
        public Task<int> Clear<T>(CancellationToken ct) where T : class => inner.Clear<T>(ct);
        public Task RunInTransaction(Func<IDocumentStore, Task> op, CancellationToken ct) => inner.RunInTransaction(op, ct);
        public Task<int> BatchInsert<T>(IEnumerable<T> docs, JsonTypeInfo<T>? ti, CancellationToken ct) where T : class => inner.BatchInsert(docs, ti, ct);
    }
}
