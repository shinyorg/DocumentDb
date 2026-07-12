using System.Globalization;
using System.Net;
using System.Text.Json.Serialization.Metadata;
using Microsoft.Azure.Cosmos;
using Shiny.DocumentDb.Internal;

namespace Shiny.DocumentDb.CosmosDb;

public partial class CosmosDbDocumentStore
{
    // Temporal history lives in a sidecar "{container}_history" container, partitioned by /typeName like
    // the main container. Each version is a CosmosHistoryDocument keyed "{documentId}__v{version}".
    async Task<Container> EnsureHistoryContainerAsync<T>(CancellationToken ct)
    {
        var historyName = this.ResolveContainerName<T>() + "_history";
        if (this.initializedContainers.ContainsKey(historyName))
            return this.database!.GetContainer(historyName);

        await this.initSemaphore.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            if (this.initializedContainers.ContainsKey(historyName))
                return this.database!.GetContainer(historyName);

            if (this.database == null)
            {
                var dbResponse = await this.client.CreateDatabaseIfNotExistsAsync(
                    this.options.DatabaseName, cancellationToken: ct).ConfigureAwait(false);
                this.database = dbResponse.Database;
            }

            var props = new ContainerProperties(historyName, "/typeName") { DefaultTimeToLive = -1 };
            await this.database.CreateContainerIfNotExistsAsync(
                props, this.options.DefaultThroughput, cancellationToken: ct).ConfigureAwait(false);

            this.initializedContainers.TryAdd(historyName, 0);
            return this.database.GetContainer(historyName);
        }
        finally
        {
            this.initSemaphore.Release();
        }
    }

    void EnsureTemporal<T>()
    {
        if (this.options.ResolveTemporalMapping(typeof(T)) == null)
            throw new InvalidOperationException(
                $"Type '{typeof(T).Name}' is not configured for temporal history. " +
                $"Call options.MapTemporal<{typeof(T).Name}>() during setup.");
    }

    /// <summary>
    /// Closes the current open version and appends a new one to the history container, then applies
    /// retention. For writes that don't carry the full post-image (Upsert merge, SetProperty,
    /// RemoveProperty), <paramref name="providedJson"/> is null and the post-image is read back from the
    /// main container. Removed writes store a null-body tombstone. No-op for non-temporal types.
    /// </summary>
    internal async Task AppendHistoryAsync<T>(string id, string typeName, TemporalOperation operation, string? providedJson, CancellationToken ct) where T : class
    {
        var mapping = this.options.ResolveTemporalMapping(typeof(T));
        if (mapping == null)
            return;

        var hist = await this.EnsureHistoryContainerAsync<T>(ct).ConfigureAwait(false);

        var data = providedJson;
        if (data == null && operation != TemporalOperation.Removed)
        {
            var main = await this.GetContainerAsync<T>(ct).ConfigureAwait(false);
            try
            {
                var resp = await main.ReadItemAsync<CosmosDocument>(id, new PartitionKey(typeName), cancellationToken: ct).ConfigureAwait(false);
                data = resp.Resource.Data;
            }
            catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.NotFound)
            {
                data = null;
            }
        }

        var existing = await this.LoadDocItemsAsync(hist, typeName, id, ct).ConfigureAwait(false);
        var now = DateTimeOffset.UtcNow;
        var nowStr = now.ToString("o");
        var actor = mapping.CaptureActor?.Invoke();

        // Close the currently-open version (there is at most one).
        foreach (var open in existing.Where(e => e.ValidTo == null))
        {
            open.ValidTo = nowStr;
            await hist.ReplaceItemAsync(open, open.Id, new PartitionKey(typeName), cancellationToken: ct).ConfigureAwait(false);
        }

        var nextVersion = existing.Select(e => e.Version).DefaultIfEmpty(0L).Max() + 1;
        var item = new CosmosHistoryDocument
        {
            Id = $"{id}__v{nextVersion}",
            DocumentId = id,
            TypeName = typeName,
            Version = nextVersion,
            ValidFrom = nowStr,
            ValidTo = null,
            Operation = operation.ToString(),
            Actor = actor,
            Data = data
        };
        await hist.CreateItemAsync(item, new PartitionKey(typeName), cancellationToken: ct).ConfigureAwait(false);

        if (mapping.Retention == null && mapping.MaxVersions == null)
            return;

        // The post-insert set: existing rows (the open one now closed in memory) plus the new row.
        var rows = existing.Select(ToEntry).ToList();
        rows.Add(ToEntry(item));
        var toDelete = new List<long>();
        if (mapping.Retention != null)
        {
            var cutoff = now - mapping.Retention.Value;
            toDelete.AddRange(TemporalHistory.PrunableByAge(rows, cutoff).Select(e => e.Version));
        }
        if (mapping.MaxVersions != null)
            toDelete.AddRange(TemporalHistory.PrunableByCount(rows, mapping.MaxVersions.Value).Select(e => e.Version));

        foreach (var v in toDelete.Distinct())
        {
            try
            {
                await hist.DeleteItemAsync<CosmosHistoryDocument>($"{id}__v{v}", new PartitionKey(typeName), cancellationToken: ct).ConfigureAwait(false);
            }
            catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.NotFound)
            {
                // Already gone.
            }
        }
    }

    static HistoryEntry ToEntry(CosmosHistoryDocument d) => new()
    {
        Id = d.DocumentId,
        TypeName = d.TypeName,
        Version = d.Version,
        ValidFrom = ParseDto(d.ValidFrom),
        ValidTo = d.ValidTo == null ? null : ParseDto(d.ValidTo),
        Operation = d.Operation,
        Actor = d.Actor,
        Data = d.Data
    };

    static DateTimeOffset ParseDto(string s)
        => DateTimeOffset.Parse(s, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind);

    async Task<List<CosmosHistoryDocument>> LoadDocItemsAsync(Container hist, string typeName, string docId, CancellationToken ct)
    {
        var query = new QueryDefinition("SELECT * FROM c WHERE c.documentId = @id").WithParameter("@id", docId);
        var list = new List<CosmosHistoryDocument>();
        using var it = hist.GetItemQueryIterator<CosmosHistoryDocument>(query,
            requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey(typeName) });
        while (it.HasMoreResults)
            list.AddRange(await it.ReadNextAsync(ct).ConfigureAwait(false));
        return list;
    }

    async Task<List<HistoryEntry>> LoadDocRowsAsync<T>(string typeName, string docId, CancellationToken ct)
    {
        var hist = await this.EnsureHistoryContainerAsync<T>(ct).ConfigureAwait(false);
        return (await this.LoadDocItemsAsync(hist, typeName, docId, ct).ConfigureAwait(false)).Select(ToEntry).ToList();
    }

    async Task<List<HistoryEntry>> LoadTypeRowsAsync<T>(string typeName, CancellationToken ct)
    {
        var hist = await this.EnsureHistoryContainerAsync<T>(ct).ConfigureAwait(false);
        var query = new QueryDefinition("SELECT * FROM c WHERE c.typeName = @typeName").WithParameter("@typeName", typeName);
        var list = new List<HistoryEntry>();
        using var it = hist.GetItemQueryIterator<CosmosHistoryDocument>(query,
            requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey(typeName) });
        while (it.HasMoreResults)
            list.AddRange((await it.ReadNextAsync(ct).ConfigureAwait(false)).Select(ToEntry));
        return list;
    }

    async Task<string?> ReadVersionDataAsync<T>(string typeName, string docId, long version, CancellationToken ct)
    {
        var hist = await this.EnsureHistoryContainerAsync<T>(ct).ConfigureAwait(false);
        try
        {
            var resp = await hist.ReadItemAsync<CosmosHistoryDocument>($"{docId}__v{version}", new PartitionKey(typeName), cancellationToken: ct).ConfigureAwait(false);
            return resp.Resource.Data;
        }
        catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.NotFound)
        {
            return null;
        }
    }

    // ── ITemporalDocumentStore ──────────────────────────────────────────

    public Task<IReadOnlyList<DocumentVersion<T>>> History<T>(object id, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class
        => this.Tracker.Track("history", typeof(T).Name, () => this.HistoryImpl(id, jsonTypeInfo, cancellationToken), r => r.Count);

    async Task<IReadOnlyList<DocumentVersion<T>>> HistoryImpl<T>(object id, JsonTypeInfo<T>? jsonTypeInfo, CancellationToken cancellationToken) where T : class
    {
        this.EnsureTemporal<T>();
        var typeInfo = this.FindTypeInfo(jsonTypeInfo);
        var resolvedId = this.idCache.GetOrCreate(typeInfo).ResolveId(id);
        var typeName = this.ResolveTypeName<T>();
        var rows = await this.LoadDocRowsAsync<T>(typeName, resolvedId, cancellationToken).ConfigureAwait(false);
        return TemporalHistory.History(rows)
            .Select(e => TemporalHistory.ToVersion<T>(e, j => Deserialize(j, typeInfo, this.jsonOptions)))
            .ToList();
    }

    public Task<T?> AsOf<T>(object id, DateTimeOffset asOf, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class
        => this.Tracker.Track("as_of", typeof(T).Name, () => this.AsOfImpl(id, asOf, jsonTypeInfo, cancellationToken), r => r is null ? 0 : 1);

    async Task<T?> AsOfImpl<T>(object id, DateTimeOffset asOf, JsonTypeInfo<T>? jsonTypeInfo, CancellationToken cancellationToken) where T : class
    {
        this.EnsureTemporal<T>();
        var typeInfo = this.FindTypeInfo(jsonTypeInfo);
        var resolvedId = this.idCache.GetOrCreate(typeInfo).ResolveId(id);
        var typeName = this.ResolveTypeName<T>();
        var rows = await this.LoadDocRowsAsync<T>(typeName, resolvedId, cancellationToken).ConfigureAwait(false);
        var entry = TemporalHistory.AsOf(rows, asOf.ToUniversalTime());
        return entry?.Data == null ? null : Deserialize(entry.Data, typeInfo, this.jsonOptions);
    }

    public Task<IReadOnlyList<T>> AsOfAll<T>(DateTimeOffset asOf, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class
        => this.Tracker.Track("as_of_all", typeof(T).Name, () => this.AsOfAllImpl(asOf, jsonTypeInfo, cancellationToken), r => r.Count);

    async Task<IReadOnlyList<T>> AsOfAllImpl<T>(DateTimeOffset asOf, JsonTypeInfo<T>? jsonTypeInfo, CancellationToken cancellationToken) where T : class
    {
        this.EnsureTemporal<T>();
        var typeInfo = this.FindTypeInfo(jsonTypeInfo);
        var typeName = this.ResolveTypeName<T>();
        var rows = await this.LoadTypeRowsAsync<T>(typeName, cancellationToken).ConfigureAwait(false);
        return TemporalHistory.AsOfAll(rows, asOf.ToUniversalTime())
            .Select(e => Deserialize(e.Data!, typeInfo, this.jsonOptions)!)
            .ToList();
    }

    public Task<IReadOnlyList<DocumentVersion<T>>> ChangesByActor<T>(string actor, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class
        => this.Tracker.Track("changes_by_actor", typeof(T).Name, () => this.ChangesByActorImpl(actor, jsonTypeInfo, cancellationToken), r => r.Count);

    async Task<IReadOnlyList<DocumentVersion<T>>> ChangesByActorImpl<T>(string actor, JsonTypeInfo<T>? jsonTypeInfo, CancellationToken cancellationToken) where T : class
    {
        ArgumentNullException.ThrowIfNull(actor);
        this.EnsureTemporal<T>();
        var typeInfo = this.FindTypeInfo(jsonTypeInfo);
        var typeName = this.ResolveTypeName<T>();
        var rows = await this.LoadTypeRowsAsync<T>(typeName, cancellationToken).ConfigureAwait(false);
        return TemporalHistory.ByActor(rows, actor)
            .Select(e => TemporalHistory.ToVersion<T>(e, j => Deserialize(j, typeInfo, this.jsonOptions)))
            .ToList();
    }

    public Task<IReadOnlyList<DocumentVersion<T>>> ChangesBetween<T>(DateTimeOffset from, DateTimeOffset to, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class
        => this.Tracker.Track("changes_between", typeof(T).Name, () => this.ChangesBetweenImpl(from, to, jsonTypeInfo, cancellationToken), r => r.Count);

    async Task<IReadOnlyList<DocumentVersion<T>>> ChangesBetweenImpl<T>(DateTimeOffset from, DateTimeOffset to, JsonTypeInfo<T>? jsonTypeInfo, CancellationToken cancellationToken) where T : class
    {
        this.EnsureTemporal<T>();
        var typeInfo = this.FindTypeInfo(jsonTypeInfo);
        var typeName = this.ResolveTypeName<T>();
        var rows = await this.LoadTypeRowsAsync<T>(typeName, cancellationToken).ConfigureAwait(false);
        return TemporalHistory.Between(rows, from.ToUniversalTime(), to.ToUniversalTime())
            .Select(e => TemporalHistory.ToVersion<T>(e, j => Deserialize(j, typeInfo, this.jsonOptions)))
            .ToList();
    }

    public Task<T?> Restore<T>(object id, long version, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class
        => this.Tracker.Track("restore", typeof(T).Name, () => this.RestoreImpl(id, version, jsonTypeInfo, cancellationToken), r => r is null ? 0 : 1);

    async Task<T?> RestoreImpl<T>(object id, long version, JsonTypeInfo<T>? jsonTypeInfo, CancellationToken cancellationToken) where T : class
    {
        this.EnsureTemporal<T>();
        var typeInfo = this.FindTypeInfo(jsonTypeInfo);
        var resolvedId = this.idCache.GetOrCreate(typeInfo).ResolveId(id);
        var typeName = this.ResolveTypeName<T>();

        var json = await this.ReadVersionDataAsync<T>(typeName, resolvedId, version, cancellationToken).ConfigureAwait(false);
        var doc = json != null ? Deserialize(json, typeInfo, this.jsonOptions) : null;
        if (doc == null)
            return null;

        var versionMapping = this.options.ResolveVersionMapping(typeof(T));
        var current = await this.Get(id, typeInfo, cancellationToken).ConfigureAwait(false);
        if (current == null)
        {
            await this.Insert(doc, typeInfo, cancellationToken).ConfigureAwait(false);
        }
        else
        {
            versionMapping?.SetVersion(doc, versionMapping.GetVersion(current));
            await this.Update(doc, typeInfo, cancellationToken).ConfigureAwait(false);
        }
        return doc;
    }

    public Task<JsonPatchDocument<T>?> GetDiffBetween<T>(object id, long fromVersion, long toVersion, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class
        => this.Tracker.Track("get_diff_between", typeof(T).Name, () => this.GetDiffBetweenImpl(id, fromVersion, toVersion, jsonTypeInfo, cancellationToken), r => r is null ? 0 : 1);

    async Task<JsonPatchDocument<T>?> GetDiffBetweenImpl<T>(object id, long fromVersion, long toVersion, JsonTypeInfo<T>? jsonTypeInfo, CancellationToken cancellationToken) where T : class
    {
        this.EnsureTemporal<T>();
        var typeInfo = this.FindTypeInfo(jsonTypeInfo);
        var resolvedId = this.idCache.GetOrCreate(typeInfo).ResolveId(id);
        var typeName = this.ResolveTypeName<T>();

        var fromJson = await this.ReadVersionDataAsync<T>(typeName, resolvedId, fromVersion, cancellationToken).ConfigureAwait(false);
        var toJson = await this.ReadVersionDataAsync<T>(typeName, resolvedId, toVersion, cancellationToken).ConfigureAwait(false);
        if (fromJson == null || toJson == null)
            return null;
        return JsonDiff.CreatePatch<T>(fromJson, toJson, this.jsonOptions);
    }
}
