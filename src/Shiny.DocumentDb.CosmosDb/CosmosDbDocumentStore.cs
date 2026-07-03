using Shiny.DocumentDb.Internal.Query;
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

public partial class CosmosDbDocumentStore : DocumentProviderBase, IDocumentStore, ITemporalDocumentStore, IChangeFeedDocumentStore, IDocumentMaintenance, IUnitOfWorkEngine, IAsyncDisposable, IDisposable
{
    /// <inheritdoc />
    public async Task ClearAll(CancellationToken cancellationToken = default)
    {
        await this.initSemaphore.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (this.database == null)
            {
                var dbResponse = await this.client
                    .CreateDatabaseIfNotExistsAsync(this.options.DatabaseName, cancellationToken: cancellationToken)
                    .ConfigureAwait(false);
                this.database = dbResponse.Database;
            }

            var names = new List<string>();
            using (var iterator = this.database.GetContainerQueryIterator<ContainerProperties>())
            {
                while (iterator.HasMoreResults)
                {
                    var page = await iterator.ReadNextAsync(cancellationToken).ConfigureAwait(false);
                    foreach (var props in page)
                        names.Add(props.Id);
                }
            }

            // Dropping the containers wipes their data; EnsureContainerAsync recreates them lazily on
            // next access with the correct partition key / indexing policy.
            foreach (var name in names)
                await this.database.GetContainer(name)
                    .DeleteContainerAsync(cancellationToken: cancellationToken)
                    .ConfigureAwait(false);

            this.initializedContainers.Clear();
        }
        finally
        {
            this.initSemaphore.Release();
        }
    }

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
        this.idCache = new IdAccessorCache(options.ResolveIdPropertyName, options.IdConverters);

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

        options.ResolveVersionJsonPaths(this.jsonOptions);
        options.ResolveSpatialJsonPaths(this.jsonOptions);
        options.ResolveVectorJsonPaths(this.jsonOptions);
        options.ResolveFullTextJsonPaths(this.jsonOptions);
        options.ResolveComputedJsonNames(this.jsonOptions);
    }

    public bool SupportsSpatial => this.options.spatialMappings.Count > 0;
    public bool SupportsVector => this.options.vectorMappings.Count > 0;
    public bool SupportsFullText => this.options.fullTextMappings.Count > 0;

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

            // Vector embedding policy + indexing for mapped vector properties.
            // Cosmos requires both: a VectorEmbeddingPolicy entry that declares the path,
            // dimension, and distance function; and a VectorIndexPath in the indexing policy
            // that declares the ANN index type.
            if (this.options.vectorMappings.Count > 0)
            {
                var embeddings = new System.Collections.ObjectModel.Collection<Embedding>();
                foreach (var mapping in this.options.vectorMappings.Values)
                {
                    embeddings.Add(new Embedding
                    {
                        Path = $"/data/{mapping.JsonPath}",
                        DataType = VectorDataType.Float32,
                        Dimensions = mapping.Dimensions,
                        DistanceFunction = mapping.Metric switch
                        {
                            VectorDistance.Cosine => DistanceFunction.Cosine,
                            VectorDistance.Euclidean => DistanceFunction.Euclidean,
                            VectorDistance.DotProduct => DistanceFunction.DotProduct,
                            _ => throw new NotSupportedException($"CosmosDB does not support {mapping.Metric} distance.")
                        }
                    });

                    containerProperties.IndexingPolicy.VectorIndexes.Add(new VectorIndexPath
                    {
                        Path = $"/data/{mapping.JsonPath}",
                        Type = mapping.IndexKind switch
                        {
                            VectorIndexKind.DiskAnn => VectorIndexType.DiskANN,
                            VectorIndexKind.Flat => VectorIndexType.Flat,
                            VectorIndexKind.QuantizedFlat => VectorIndexType.QuantizedFlat,
                            _ => VectorIndexType.DiskANN
                        }
                    });
                }
                containerProperties.VectorEmbeddingPolicy = new VectorEmbeddingPolicy(embeddings);
            }

            // Full-text policy + indexes for mapped full-text properties.
            if (this.options.fullTextMappings.Count > 0)
            {
                var ftPaths = new System.Collections.ObjectModel.Collection<FullTextPath>();
                foreach (var mapping in this.options.fullTextMappings.Values)
                {
                    var lang = CosmosFullTextLanguage(mapping.Language);
                    foreach (var jsonPath in mapping.JsonPaths)
                    {
                        ftPaths.Add(new FullTextPath { Path = $"/data/{jsonPath}", Language = lang });
                        containerProperties.IndexingPolicy.FullTextIndexes.Add(
                            new FullTextIndexPath { Path = $"/data/{jsonPath}" });
                    }
                }
                containerProperties.FullTextPolicy = new FullTextPolicy
                {
                    DefaultLanguage = "en-US",
                    FullTextPaths = ftPaths
                };
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

    static string CosmosFullTextLanguage(FullTextLanguage language) => language switch
    {
        FullTextLanguage.German => "de-DE",
        FullTextLanguage.Spanish => "es-ES",
        FullTextLanguage.French => "fr-FR",
        FullTextLanguage.Italian => "it-IT",
        FullTextLanguage.Portuguese => "pt-BR",
        FullTextLanguage.Dutch => "nl-NL",
        FullTextLanguage.Russian => "ru-RU",
        _ => "en-US"
    };

    // ── Full-text search (Cosmos DB full-text policy + FullTextScore RANK) ──

    public async Task<IReadOnlyList<FullTextResult<T>>> FullTextSearch<T>(
        string searchText,
        int maxResults = 50,
        Expression<Func<T, bool>>? filter = null,
        CancellationToken cancellationToken = default) where T : class
    {
        var mapping = this.options.ResolveFullTextMapping(typeof(T))
            ?? throw new NotSupportedException(
                $"No full-text property mapped for type '{typeof(T).Name}'. Call MapFullTextProperty<{typeof(T).Name}>() in options.");
        ArgumentException.ThrowIfNullOrEmpty(searchText);
        if (maxResults <= 0)
            throw new ArgumentOutOfRangeException(nameof(maxResults));

        var typeInfo = this.FindTypeInfo<T>(null);
        var typeName = this.ResolveTypeName<T>();
        var container = await this.EnsureContainerAsync(this.ResolveContainerName<T>(), cancellationToken).ConfigureAwait(false);

        var terms = FullTextMappingFactory.Tokenize(searchText);
        if (terms.Count == 0)
            return Array.Empty<FullTextResult<T>>();

        // Tokens are alphanumeric → safe to embed as Cosmos string literals.
        var termLiterals = string.Join(", ", terms.Select(t => "\"" + t + "\""));
        var paths = mapping.JsonPaths.Select(p => $"c.data.{p}").ToList();
        var contains = string.Join(" OR ", paths.Select(p => $"FullTextContainsAny({p}, {termLiterals})"));
        var scores = paths.Select(p => $"FullTextScore({p}, {termLiterals})").ToList();
        // FullTextScore is only valid in ORDER BY RANK; combine multiple fields with reciprocal rank fusion.
        var rank = scores.Count == 1 ? scores[0] : $"RRF({string.Join(", ", scores)})";

        // FullTextScore cannot be projected, so the score is synthesized from rank order; over-fetch
        // when a post-filter is present so it doesn't starve the top-N.
        var fetch = filter == null ? maxResults : maxResults * 4;
        var sql = $"SELECT TOP {fetch} c.data FROM c WHERE c.typeName = @typeName AND ({contains}) ORDER BY RANK {rank}";
        var queryDef = new QueryDefinition(sql).WithParameter("@typeName", typeName);

        var postFilter = filter == null ? null : ExpressionInterpreter.Interpret(filter);
        var results = new List<FullTextResult<T>>();
        var position = 0;
        using var iterator = container.GetItemQueryIterator<CosmosDocument>(queryDef, requestOptions: new QueryRequestOptions
        {
            PartitionKey = new PartitionKey(typeName)
        });
        while (iterator.HasMoreResults && results.Count < maxResults)
        {
            var response = await iterator.ReadNextAsync(cancellationToken).ConfigureAwait(false);
            foreach (var doc in response)
            {
                var obj = Deserialize(doc.Data, typeInfo, this.jsonOptions);
                if (obj == null) continue;
                if (postFilter != null && !postFilter(obj)) continue;
                results.Add(new FullTextResult<T> { Document = obj, Score = 1.0 / ++position });
                if (results.Count >= maxResults) break;
            }
        }
        return results;
    }

    string GenerateId<T>(IdAccessor<T> accessor) where T : class
    {
        return accessor.Kind switch
        {
            IdKind.Guid => Guid.NewGuid().ToString("N"),
            IdKind.String => Guid.NewGuid().ToString(),
            IdKind.Custom => accessor.GenerateOrThrow(),
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

    internal override InterceptorPipeline Interceptors => this.options.Interceptors;

    public async Task Insert<T>(T document, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class
    {
        var typeInfo = this.FindTypeInfo(jsonTypeInfo);
        var accessor = this.idCache.GetOrCreate(typeInfo);
        var typeName = this.ResolveTypeName<T>();
        var versionMapping = this.options.ResolveVersionMapping(typeof(T));

        var ctx = this.NewWriteContext(DocumentOperation.Insert, typeName, null, document);
        await this.RunBeforeWriteAsync(ctx, cancellationToken).ConfigureAwait(false);
        if (ctx?.Document is T mutatedDoc)
            document = mutatedDoc;

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

        versionMapping?.SetVersion(document, 1);
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
        await this.AppendHistoryAsync<T>(id, typeName, TemporalOperation.Inserted, json, cancellationToken).ConfigureAwait(false);
        await this.RunAfterWriteAsync(ctx, id, versionMapping?.GetVersion(document) ?? 1, cancellationToken).ConfigureAwait(false);
    }

    public async Task<int> BatchInsert<T>(IEnumerable<T> documents, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class
    {
        var typeInfo = this.FindTypeInfo(jsonTypeInfo);
        var accessor = this.idCache.GetOrCreate(typeInfo);
        var typeName = this.ResolveTypeName<T>();
        var versionMapping = this.options.ResolveVersionMapping(typeof(T));
        var container = await this.GetContainerAsync<T>(cancellationToken).ConfigureAwait(false);

        var srcList = documents as IReadOnlyList<T> ?? documents.ToList();

        // Per-doc BeforeWrite before serialization.
        DocumentWriteContext[]? ctxs = null;
        if (this.HasPerDocInterceptors)
        {
            var mutable = srcList.ToList();
            ctxs = await this.RunBeforeWriteBatchAsync(mutable, typeName, cancellationToken).ConfigureAwait(false);
            srcList = mutable;
        }

        var docs = new List<CosmosDocument>();
        long nextInt = -1;

        foreach (var document in srcList)
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

            versionMapping?.SetVersion(document, 1);
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

        for (var i = 0; i < docs.Count; i++)
        {
            await this.AppendHistoryAsync<T>(docs[i].Id, typeName, TemporalOperation.Inserted, docs[i].Data, cancellationToken).ConfigureAwait(false);
            if (ctxs != null)
                await this.RunAfterWriteAsync(ctxs[i], docs[i].Id, versionMapping?.GetVersion(srcList[i]) ?? 1, cancellationToken).ConfigureAwait(false);
        }

        return totalInserted;
    }

    public async Task Update<T>(T document, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class
    {
        var typeInfo = this.FindTypeInfo(jsonTypeInfo);
        var accessor = this.idCache.GetOrCreate(typeInfo);
        var versionMapping = this.options.ResolveVersionMapping(typeof(T));
        var typeName = this.ResolveTypeName<T>();

        var ctx = this.NewWriteContext(DocumentOperation.Update, typeName, null, document);
        await this.RunBeforeWriteAsync(ctx, cancellationToken).ConfigureAwait(false);
        if (ctx?.Document is T mutatedDoc)
            document = mutatedDoc;

        if (accessor.IsDefaultId(document))
            throw new InvalidOperationException(
                $"Update requires a non-default Id on the document. " +
                $"Set the Id property on '{typeof(T).Name}' before calling Update.");

        var id = accessor.GetIdAsString(document);
        var container = await this.GetContainerAsync<T>(cancellationToken).ConfigureAwait(false);

        // Verify exists and check version
        ItemResponse<CosmosDocument> existingResponse;
        try
        {
            existingResponse = await container.ReadItemAsync<CosmosDocument>(id, new PartitionKey(typeName), cancellationToken: cancellationToken).ConfigureAwait(false);
        }
        catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.NotFound)
        {
            throw new InvalidOperationException(
                $"No document of type '{typeName}' with Id '{id}' was found to update.");
        }

        if (this.options.ResolveQueryFilters(typeof(T)).Count > 0)
        {
            var existingDoc = Deserialize(existingResponse.Resource.Data, typeInfo, this.jsonOptions);
            if (existingDoc == null || !this.PassesGlobalFilters(existingDoc))
                throw new InvalidOperationException(
                    $"No document of type '{typeName}' with Id '{id}' was found to update.");
        }

        int? expectedVersion = null;
        ItemRequestOptions? requestOptions = null;
        if (versionMapping != null)
        {
            var ev = versionMapping.GetVersion(document);
            var storedNode = JsonNode.Parse(existingResponse.Resource.Data)!.AsObject();
            var storedVersion = storedNode[versionMapping.JsonPath]?.GetValue<int>() ?? 0;
            if (storedVersion != ev)
                throw new ConcurrencyException(typeName, id, ev, storedVersion);
            versionMapping.SetVersion(document, ev + 1);
            expectedVersion = ev;

            // Native ETag precondition closes the read→replace race: if another writer commits
            // between the read above and this replace, Cosmos rejects with 412 instead of clobbering.
            requestOptions = new ItemRequestOptions { IfMatchEtag = existingResponse.ETag };
        }

        var json = Serialize(document, typeInfo, this.jsonOptions);
        var cosmosDoc = new CosmosDocument
        {
            Id = id,
            TypeName = typeName,
            Data = json,
            CreatedAt = existingResponse.Resource.CreatedAt,
            UpdatedAt = DateTimeOffset.UtcNow.ToString("o")
        };

        this.Log($"CosmosDB REPLACE {this.ResolveContainerName<T>()} Id={id}");
        try
        {
            await container.ReplaceItemAsync(cosmosDoc, id, new PartitionKey(typeName), requestOptions, cancellationToken).ConfigureAwait(false);
        }
        catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.PreconditionFailed)
        {
            throw new ConcurrencyException(typeName, id, expectedVersion!.Value);
        }
        await this.AppendHistoryAsync<T>(id, typeName, TemporalOperation.Updated, json, cancellationToken).ConfigureAwait(false);
        await this.RunAfterWriteAsync(ctx, id, versionMapping?.GetVersion(document), cancellationToken).ConfigureAwait(false);
    }

    public async Task Upsert<T>(T patch, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class
    {
        var typeInfo = this.FindTypeInfo(jsonTypeInfo);
        var accessor = this.idCache.GetOrCreate(typeInfo);
        var versionMapping = this.options.ResolveVersionMapping(typeof(T));
        var typeName = this.ResolveTypeName<T>();

        var ctx = this.NewWriteContext(DocumentOperation.Upsert, typeName, null, patch);
        await this.RunBeforeWriteAsync(ctx, cancellationToken).ConfigureAwait(false);
        if (ctx?.Document is T mutatedPatch)
            patch = mutatedPatch;

        if (accessor.IsDefaultId(patch))
            throw new InvalidOperationException(
                $"Upsert requires a non-default Id on the document. " +
                $"Set the Id property on '{typeof(T).Name}' before calling Upsert.");

        var id = accessor.GetIdAsString(patch);
        var container = await this.GetContainerAsync<T>(cancellationToken).ConfigureAwait(false);

        var now = DateTimeOffset.UtcNow.ToString("o");

        // Try to read existing
        CosmosDocument? existing = null;
        string? existingEtag = null;
        try
        {
            var response = await container.ReadItemAsync<CosmosDocument>(id, new PartitionKey(typeName), cancellationToken: cancellationToken).ConfigureAwait(false);
            existing = response.Resource;
            existingEtag = response.ETag;
        }
        catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.NotFound)
        {
            // Will insert
        }

        if (existing == null)
        {
            versionMapping?.SetVersion(patch, 1);
            var patchJson = Serialize(patch, typeInfo, this.jsonOptions);
            patchJson = StripNullProperties(patchJson);

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
            await this.AppendHistoryAsync<T>(id, typeName, TemporalOperation.Updated, null, cancellationToken).ConfigureAwait(false);
            await this.RunAfterWriteAsync(ctx, id, versionMapping?.GetVersion(patch), cancellationToken).ConfigureAwait(false);
        }
        else
        {
            int? guardVersion = null;
            ItemRequestOptions? requestOptions = null;
            if (versionMapping != null)
            {
                var expectedVersion = versionMapping.GetVersion(patch);
                var storedNode = JsonNode.Parse(existing.Data)!.AsObject();
                var storedVersion = storedNode[versionMapping.JsonPath]?.GetValue<int>() ?? 0;
                if (expectedVersion > 0 && storedVersion != expectedVersion)
                    throw new ConcurrencyException(typeName, id, expectedVersion, storedVersion);
                versionMapping.SetVersion(patch, storedVersion + 1);

                // Only guard when the caller supplied a version to check against; a blind upsert
                // (version 0) keeps last-write-wins semantics. The ETag closes the read→replace race.
                if (expectedVersion > 0)
                {
                    guardVersion = expectedVersion;
                    requestOptions = new ItemRequestOptions { IfMatchEtag = existingEtag };
                }
            }

            var patchJson = Serialize(patch, typeInfo, this.jsonOptions);
            patchJson = StripNullProperties(patchJson);

            var merged = MergeJson(existing.Data, patchJson);
            existing.Data = merged;
            existing.UpdatedAt = now;

            this.Log($"CosmosDB UPSERT (merge) {this.ResolveContainerName<T>()} Id={id}");
            try
            {
                await container.ReplaceItemAsync(existing, id, new PartitionKey(typeName), requestOptions, cancellationToken).ConfigureAwait(false);
            }
            catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.PreconditionFailed)
            {
                throw new ConcurrencyException(typeName, id, guardVersion!.Value);
            }
            await this.AppendHistoryAsync<T>(id, typeName, TemporalOperation.Updated, null, cancellationToken).ConfigureAwait(false);
            await this.RunAfterWriteAsync(ctx, id, versionMapping?.GetVersion(patch), cancellationToken).ConfigureAwait(false);
        }
    }

    // CosmosDB has no cross-document transaction for heterogeneous upsert/update/delete (its unit of work
    // is a compensating tracker, not a transaction). The batch methods reuse the proven single-doc methods
    // but issue them concurrently in bounded waves — the real CosmosDB win, since parallel requests
    // parallelize RU spend. Batches are therefore best-effort, not atomic (consistent with the provider's
    // unit-of-work behaviour).
    const int CosmosBatchConcurrency = 100;

    async Task<int> RunBatchConcurrentlyAsync<TItem>(IReadOnlyList<TItem> items, Func<TItem, Task> action, CancellationToken ct)
    {
        for (var offset = 0; offset < items.Count; offset += CosmosBatchConcurrency)
        {
            ct.ThrowIfCancellationRequested();
            var end = Math.Min(offset + CosmosBatchConcurrency, items.Count);
            var wave = new List<Task>(end - offset);
            for (var i = offset; i < end; i++)
                wave.Add(action(items[i]));
            await Task.WhenAll(wave).ConfigureAwait(false);
        }
        return items.Count;
    }

    public Task<int> BatchUpsert<T>(IEnumerable<T> patches, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class
    {
        var list = patches as IReadOnlyList<T> ?? patches.ToList();
        return list.Count == 0
            ? Task.FromResult(0)
            : this.RunBatchConcurrentlyAsync(list, p => this.Upsert(p, jsonTypeInfo, cancellationToken), cancellationToken);
    }

    public Task<int> BatchUpdate<T>(IEnumerable<T> documents, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class
    {
        var list = documents as IReadOnlyList<T> ?? documents.ToList();
        return list.Count == 0
            ? Task.FromResult(0)
            : this.RunBatchConcurrentlyAsync(list, d => this.Update(d, jsonTypeInfo, cancellationToken), cancellationToken);
    }

    public async Task<int> BatchRemove<T>(IEnumerable<object> ids, CancellationToken cancellationToken = default) where T : class
    {
        var idList = ids as IReadOnlyList<object> ?? ids.ToList();
        if (idList.Count == 0)
            return 0;

        var removed = 0;
        for (var offset = 0; offset < idList.Count; offset += CosmosBatchConcurrency)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var end = Math.Min(offset + CosmosBatchConcurrency, idList.Count);
            var wave = new List<Task<bool>>(end - offset);
            for (var i = offset; i < end; i++)
                wave.Add(this.Remove<T>(idList[i], cancellationToken));
            foreach (var r in await Task.WhenAll(wave).ConfigureAwait(false))
                if (r) removed++;
        }
        return removed;
    }

    // Deletes a known id-list with bounded concurrency, ignoring rows that have already vanished — used by
    // Clear and the query-side ExecuteDelete in place of the old one-at-a-time loops.
    internal static async Task DeleteItemsConcurrentlyAsync(Container container, string typeName, IReadOnlyList<string> ids, CancellationToken ct)
    {
        for (var offset = 0; offset < ids.Count; offset += CosmosBatchConcurrency)
        {
            ct.ThrowIfCancellationRequested();
            var end = Math.Min(offset + CosmosBatchConcurrency, ids.Count);
            var wave = new List<Task>(end - offset);
            for (var i = offset; i < end; i++)
                wave.Add(DeleteIgnoreMissingAsync(container, ids[i], typeName, ct));
            await Task.WhenAll(wave).ConfigureAwait(false);
        }
    }

    static async Task DeleteIgnoreMissingAsync(Container container, string id, string typeName, CancellationToken ct)
    {
        try
        {
            await container.DeleteItemAsync<CosmosDocument>(id, new PartitionKey(typeName), cancellationToken: ct).ConfigureAwait(false);
        }
        catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.NotFound)
        {
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
            await this.AppendHistoryAsync<T>(resolvedId, typeName, TemporalOperation.Updated, null, cancellationToken).ConfigureAwait(false);
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
            await this.AppendHistoryAsync<T>(resolvedId, typeName, TemporalOperation.Updated, null, cancellationToken).ConfigureAwait(false);
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
            var doc = Deserialize(response.Resource.Data, typeInfo, this.jsonOptions);
            if (doc != null && !this.PassesGlobalFilters(doc))
                return null;
            return doc;
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

        var ctx = this.NewWriteContext<T>(DocumentOperation.Delete, typeName, id, null);
        await this.RunBeforeWriteAsync(ctx, cancellationToken).ConfigureAwait(false);

        if (this.options.ResolveQueryFilters(typeof(T)).Count > 0)
        {
            // Read first; if the doc fails the filter, do not delete.
            CosmosDocument? existing;
            try
            {
                var resp = await container.ReadItemAsync<CosmosDocument>(resolvedId, new PartitionKey(typeName), cancellationToken: cancellationToken).ConfigureAwait(false);
                existing = resp.Resource;
            }
            catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.NotFound)
            {
                return false;
            }
            var doc = Deserialize<T>(existing.Data, null, this.jsonOptions);
            if (doc == null || !this.PassesGlobalFilters(doc))
                return false;
        }

        this.Log($"CosmosDB DELETE {this.ResolveContainerName<T>()} Id={resolvedId}");
        try
        {
            await container.DeleteItemAsync<CosmosDocument>(resolvedId, new PartitionKey(typeName), cancellationToken: cancellationToken).ConfigureAwait(false);
            await this.AppendHistoryAsync<T>(resolvedId, typeName, TemporalOperation.Removed, null, cancellationToken).ConfigureAwait(false);
            await this.RunAfterWriteAsync(ctx, id, null, cancellationToken).ConfigureAwait(false);
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
        var hasFilters = this.options.ResolveQueryFilters(typeof(T)).Count > 0;

        var bulkCtx = this.NewBulkContext<T>(DocumentOperation.Clear, typeName);
        await this.RunBeforeBulkAsync(bulkCtx, cancellationToken).ConfigureAwait(false);

        // Query all IDs, then delete each
        var sql = hasFilters
            ? "SELECT c.id, c.data FROM c WHERE c.typeName = @typeName"
            : "SELECT c.id FROM c WHERE c.typeName = @typeName";
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
            foreach (var d in response)
            {
                if (hasFilters)
                {
                    var doc = Deserialize<T>(d.Data, null, this.jsonOptions);
                    if (doc == null || !this.PassesGlobalFilters(doc))
                        continue;
                }
                ids.Add(d.Id);
            }
        }

        await DeleteItemsConcurrentlyAsync(container, typeName, ids, cancellationToken).ConfigureAwait(false);

        await this.RunAfterBulkAsync(bulkCtx, ids.Count, cancellationToken).ConfigureAwait(false);
        return ids.Count;
    }

    /// <inheritdoc />
    public UnitOfWork CreateUnitOfWork() => new(this);

    async Task IUnitOfWorkEngine.RunUnitAsync(Func<IDocumentStore, CancellationToken, Task> work, CancellationToken cancellationToken)
    {
        var tracker = new CosmosDbTransactionalStore(this);
        try
        {
            await work(tracker, cancellationToken).ConfigureAwait(false);
        }
        catch
        {
            await tracker.RollbackAsync(cancellationToken).ConfigureAwait(false);
            throw;
        }
    }

    // ── Native change feed: Cosmos Change Feed (latest-version pull model) ──

    /// <inheritdoc />
    /// <remarks>
    /// Uses the latest-version change feed scoped to the type's partition. It delivers created and
    /// modified documents (with the full body) but not deletes, and does not distinguish inserts
    /// from updates — every change is reported as <see cref="DocumentChangeType.Updated"/>.
    /// </remarks>
    public async Task<IAsyncDisposable> SubscribeChanges<T>(
        Func<DocumentChange<T>, CancellationToken, Task> onChange,
        CancellationToken cancellationToken = default) where T : class
    {
        ArgumentNullException.ThrowIfNull(onChange);
        var typeInfo = this.FindTypeInfo<T>(null);
        var typeName = this.ResolveTypeName<T>();
        var container = await this.GetContainerAsync<T>(cancellationToken).ConfigureAwait(false);
        var feedRange = FeedRange.FromPartitionKey(new PartitionKey(typeName));
        return new ChangeFeedSubscription(cancellationToken,
            token => this.RunChangeFeedAsync(container, feedRange, typeInfo, onChange, token));
    }

    async Task RunChangeFeedAsync<T>(
        Container container,
        FeedRange feedRange,
        JsonTypeInfo<T>? typeInfo,
        Func<DocumentChange<T>, CancellationToken, Task> onChange,
        CancellationToken token) where T : class
    {
        using var iterator = container.GetChangeFeedIterator<CosmosDocument>(
            ChangeFeedStartFrom.Now(feedRange), ChangeFeedMode.LatestVersion);
        var idleDelay = TimeSpan.FromSeconds(2);

        try
        {
            while (!token.IsCancellationRequested)
            {
                FeedResponse<CosmosDocument> response;
                try
                {
                    response = await iterator.ReadNextAsync(token).ConfigureAwait(false);
                }
                catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.NotModified)
                {
                    await Task.Delay(idleDelay, token).ConfigureAwait(false);
                    continue;
                }

                if (response.StatusCode == HttpStatusCode.NotModified)
                {
                    await Task.Delay(idleDelay, token).ConfigureAwait(false);
                    continue;
                }

                foreach (var doc in response)
                {
                    var document = Deserialize(doc.Data, typeInfo, this.jsonOptions);
                    await onChange(
                        new DocumentChange<T> { ChangeType = DocumentChangeType.Updated, Id = doc.Id, Document = document },
                        token).ConfigureAwait(false);
                }
            }
        }
        catch (OperationCanceledException) { }
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
            if (point is not null)
            {
                var distance = Internal.GeoMath.HaversineDistance(center, point.Value);
                results.Add(new SpatialResult<T> { Document = doc, DistanceMeters = distance });
            }
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
            if (point is not null)
            {
                var distance = Internal.GeoMath.HaversineDistance(center, point.Value);
                results.Add(new SpatialResult<T> { Document = doc, DistanceMeters = distance });
            }
        }

        return results;
    }

    public async Task<IReadOnlyList<VectorResult<T>>> NearestVectors<T>(
        ReadOnlyMemory<float> query,
        int k,
        Expression<Func<T, bool>>? filter = null,
        CancellationToken cancellationToken = default) where T : class
    {
        var mapping = this.options.ResolveVectorMapping(typeof(T))
            ?? throw new NotSupportedException(
                $"No vector property mapped for type '{typeof(T).Name}'. Call MapVectorProperty<{typeof(T).Name}>() in options.");

        if (query.Length != mapping.Dimensions)
            throw new ArgumentException(
                $"Query vector has {query.Length} dimensions; mapping expects {mapping.Dimensions}.", nameof(query));

        if (k <= 0)
            throw new ArgumentOutOfRangeException(nameof(k));

        var typeInfo = this.FindTypeInfo<T>(null);
        var typeName = this.ResolveTypeName<T>();
        var container = await this.GetContainerAsync<T>(cancellationToken).ConfigureAwait(false);

        // Build a [1,2,3] literal for the embedded vector — Cosmos SQL accepts JSON-array
        // literals as the query vector for VectorDistance().
        var queryLiteral = new StringBuilder();
        queryLiteral.Append('[');
        var span = query.Span;
        for (var i = 0; i < span.Length; i++)
        {
            if (i > 0) queryLiteral.Append(',');
            queryLiteral.Append(span[i].ToString("R", CultureInfo.InvariantCulture));
        }
        queryLiteral.Append(']');

        var distExpr = $"VectorDistance(c.data.{mapping.JsonPath}, {queryLiteral})";

        var sql = new StringBuilder();
        sql.Append($"SELECT TOP @k c.data, {distExpr} AS score FROM c WHERE c.typeName = @typeName");

        Dictionary<string, object?>? filterParams = null;
        if (filter != null)
        {
            var translated = CosmosExpressionVisitor.Translate(filter, this.jsonOptions, typeInfo);
            sql.Append($" AND ({translated.sql})");
            filterParams = translated.parameters;
        }

        sql.Append($" ORDER BY {distExpr}");

        var queryDef = new QueryDefinition(sql.ToString())
            .WithParameter("@typeName", typeName)
            .WithParameter("@k", k);

        if (filterParams != null)
            foreach (var kvp in filterParams)
                queryDef.WithParameter(kvp.Key, kvp.Value);

        this.Log(sql.ToString());

        // ExecuteRawQueryAsync returns documents only; the score column is lost by that path.
        // Run a tailored read here that captures both columns.
        var results = new List<VectorResult<T>>();
        using var iterator = container.GetItemQueryStreamIterator(queryDef,
            requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey(typeName) });

        while (iterator.HasMoreResults)
        {
            using var response = await iterator.ReadNextAsync(cancellationToken).ConfigureAwait(false);
            response.EnsureSuccessStatusCode();
            using var reader = new StreamReader(response.Content);
            var json = await reader.ReadToEndAsync(cancellationToken).ConfigureAwait(false);

            using var jdoc = JsonDocument.Parse(json);
            if (!jdoc.RootElement.TryGetProperty("Documents", out var docs))
                continue;
            foreach (var row in docs.EnumerateArray())
            {
                if (!row.TryGetProperty("data", out var dataEl))
                    continue;
                var doc = Deserialize(dataEl.GetRawText(), typeInfo, this.jsonOptions);
                if (doc == null) continue;
                float score = float.NaN;
                if (row.TryGetProperty("score", out var s) && s.ValueKind == JsonValueKind.Number)
                    score = s.GetSingle();
                results.Add(new VectorResult<T> { Document = doc, Score = score });
            }
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
    internal InterceptorPipeline InterceptorPipeline => this.options.Interceptors;
    internal CosmosDbDocumentStoreOptions Options => this.options;

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

    // ── Private helpers ────────────────────────────────────────────────

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

    sealed class CosmosDbTransactionalStore(CosmosDbDocumentStore inner) : CompensatingStore
    {
        protected override IDocumentStore Inner => inner;

        public override async Task Insert<T>(T document, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default)
        {
            await inner.Insert(document, jsonTypeInfo, cancellationToken).ConfigureAwait(false);
            var accessor = inner.idCache.GetOrCreate(inner.FindTypeInfo(jsonTypeInfo));
            this.TrackInsert(inner.ResolveTypeName<T>(), accessor.GetIdAsString(document));
        }

        protected override async Task DeleteTrackedAsync(string typeName, string id, CancellationToken ct)
        {
            var container = await inner.EnsureContainerAsync(inner.options.ResolveContainerName(typeName), ct).ConfigureAwait(false);
            await container.DeleteItemAsync<CosmosDocument>(id, new PartitionKey(typeName), cancellationToken: ct).ConfigureAwait(false);
        }
    }
}
