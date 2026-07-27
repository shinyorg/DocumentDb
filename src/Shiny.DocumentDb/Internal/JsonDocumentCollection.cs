using System.Text.Json.Nodes;

namespace Shiny.DocumentDb.Internal;

/// <summary>
/// The one implementation behind <see cref="IJsonDocumentCollection"/>. Everything is expressed against a
/// <see cref="JsonLaneTarget"/>, so a name-keyed and a type-keyed collection run the same code — the
/// difference is entirely in what the target carries.
/// </summary>
sealed class JsonDocumentCollection : IJsonDocumentCollection
{
    readonly DocumentStore store;
    readonly JsonLaneTarget target;

    internal JsonDocumentCollection(DocumentStore store, JsonLaneTarget target)
    {
        this.store = store;
        this.target = target;
    }

    public string Name => this.target.TypeName;
    public Type? DocumentType => this.target.DocumentType;
    public string IdProperty => this.target.Ids.MemberName;

    // ── Writes ──────────────────────────────────────────────────────────

    public Task<string> Insert(JsonObject document, CancellationToken cancellationToken = default)
        => this.store.Tracker.Track(
            "insert", this.Name,
            async () => await this.store.WriteOneAsync(this.target, document, DocumentStore.JsonWriteKind.Insert, cancellationToken).ConfigureAwait(false)
                        ?? throw new InvalidOperationException("The insert was cancelled by an interceptor, so no id was assigned."),
            _ => 1);

    public Task<int> Insert(JsonArray documents, CancellationToken cancellationToken = default)
        => this.store.Tracker.Track("insert", this.Name,
            () => this.store.WriteJsonAsync(this.target, documents, DocumentStore.JsonWriteKind.Insert, cancellationToken), r => r);

    public Task<int> Update(JsonNode document, bool patch = false, CancellationToken cancellationToken = default)
        => this.store.Tracker.Track(patch ? "update_patch" : "update", this.Name,
            () => this.store.WriteJsonAsync(this.target, document,
                patch ? DocumentStore.JsonWriteKind.UpdateMerge : DocumentStore.JsonWriteKind.Update, cancellationToken), r => r);

    public Task<int> Upsert(JsonNode document, bool patchIfUpdate = true, CancellationToken cancellationToken = default)
        => this.store.Tracker.Track(patchIfUpdate ? "upsert" : "upsert_replace", this.Name,
            () => this.store.WriteJsonAsync(this.target, document,
                patchIfUpdate ? DocumentStore.JsonWriteKind.Upsert : DocumentStore.JsonWriteKind.UpsertReplace, cancellationToken), r => r);

    public Task<int> BatchUpsert(IEnumerable<JsonObject> documents, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(documents);
        var list = documents as IReadOnlyList<JsonObject> ?? documents.ToList();
        return this.store.Tracker.Track("batch_upsert", this.Name,
            () => this.store.WriteManyAsync(this.target, list, DocumentStore.JsonWriteKind.Upsert, cancellationToken), r => r);
    }

    // ── Reads ───────────────────────────────────────────────────────────

    public Task<JsonObject?> Get(object id, CancellationToken cancellationToken = default)
        => this.store.Tracker.Track("get", this.Name, async () =>
        {
            var node = await this.store.GetJsonImpl(this.target, id, cancellationToken).ConfigureAwait(false);
            return node?.AsObject();
        }, r => r is null ? 0 : 1);

    public Task<IReadOnlyList<JsonNode>> Query(string whereClause, object? parameters = null, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(whereClause);
        return this.store.Tracker.Track("query", this.Name,
            () => this.store.QueryJsonImpl(this.target, whereClause, parameters, cancellationToken), r => r.Count);
    }

    public IAsyncEnumerable<JsonNode> QueryStream(string whereClause, object? parameters = null, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(whereClause);
        return this.store.Tracker.TrackStream("query_stream", this.Name,
            this.store.QueryStreamJsonImpl(this.target, whereClause, parameters, cancellationToken), cancellationToken);
    }

    public IJsonDocumentQuery Query()
        => new JsonDocumentQuery(this.target, this.store.BuildCollectionQuery(this.target));

    // ── Deletes ─────────────────────────────────────────────────────────

    public Task<bool> Remove(object id, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(id);
        var resolvedId = this.target.Ids.ResolveId(id);
        return this.store.Tracker.Track("remove", this.Name,
            () => this.store.RemoveRowCoreAsync(this.target, resolvedId, null, null, cancellationToken), r => r ? 1 : 0);
    }

    public Task<int> BatchRemove(IEnumerable<object> ids, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(ids);
        var resolved = ids.Select(this.target.Ids.ResolveId).ToList();
        if (resolved.Count == 0)
            return Task.FromResult(0);

        return this.store.Tracker.Track("batch_remove", this.Name,
            () => this.store.BatchRemoveCoreAsync(this.target, resolved, cancellationToken), r => r);
    }

    public Task<int> Clear(CancellationToken cancellationToken = default)
        => this.store.Tracker.Track("clear", this.Name,
            () => this.store.ClearCoreAsync(this.target, null, cancellationToken), r => r);

    // ── DDL ─────────────────────────────────────────────────────────────

    public Task CreateIndex(CancellationToken cancellationToken = default, params string[] jsonPaths)
    {
        ArgumentNullException.ThrowIfNull(jsonPaths);
        if (jsonPaths.Length == 0)
            throw new ArgumentException("At least one JSON path is required.", nameof(jsonPaths));

        // The second raw-string API that bypasses the parser: paths are resolved (and, schema-free,
        // validated) before they reach the interpolated DDL.
        var resolved = jsonPaths
            .Select(p => this.target.TypeInfo is { } info
                ? JsonPathFieldBinder.ResolvePath(info, p).JsonPath
                : DynamicPathResolver.ResolveJsonPathWithType(p).JsonPath)
            .ToList();

        return this.store.CreateJsonIndexCoreAsync(this.target, resolved, cancellationToken);
    }
}
