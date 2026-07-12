using System.Diagnostics.CodeAnalysis;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;
using MongoDB.Bson;
using MongoDB.Driver;
using Shiny.DocumentDb.Internal;

namespace Shiny.DocumentDb.MongoDb;

public partial class MongoDbDocumentStore
{
    // The history sidecar mirrors the relational {table}_history shape, as BsonDocuments:
    //   _id = "{typeName}:{id}:{version}", Id, TypeName, Version (Int64), ValidFrom / ValidTo
    //   (BSON UTC dates; ValidTo null while current), Operation, Actor (nullable),
    //   Data (the post-image JSON string, null for a Removed tombstone).
    IMongoCollection<BsonDocument> HistoryCollection<T>()
        => this.database.GetCollection<BsonDocument>(this.ResolveCollectionName<T>() + "_history");

    void EnsureTemporal<T>()
    {
        if (this.options.ResolveTemporalMapping(typeof(T)) == null)
            throw new InvalidOperationException(
                $"Type '{typeof(T).Name}' is not configured for temporal history. " +
                $"Call options.MapTemporal<{typeof(T).Name}>() during setup.");
    }

    static FilterDefinition<BsonDocument> DocFilter(string id, string typeName)
        => Builders<BsonDocument>.Filter.And(
            Builders<BsonDocument>.Filter.Eq("Id", id),
            Builders<BsonDocument>.Filter.Eq("TypeName", typeName));

    /// <summary>
    /// Closes the current open version and appends a new one to the history sidecar, then applies
    /// retention. For writes that don't carry the full post-image (Upsert merge, SetProperty,
    /// RemoveProperty), <paramref name="providedJson"/> is null and the post-image is read back from the
    /// main collection. Removed writes store a null-body tombstone. No-op for non-temporal types.
    /// </summary>
    internal async Task AppendHistoryAsync<T>(string id, string typeName, TemporalOperation operation, string? providedJson, CancellationToken ct) where T : class
    {
        var mapping = this.options.ResolveTemporalMapping(typeof(T));
        if (mapping == null)
            return;

        var histCol = this.HistoryCollection<T>();
        var docFilter = DocFilter(id, typeName);
        var existing = await histCol.Find(docFilter).ToListAsync(ct).ConfigureAwait(false);

        var data = providedJson;
        if (data == null && operation != TemporalOperation.Removed)
        {
            var mainFilter = Builders<BsonDocument>.Filter.Eq(MongoFields.Id, CompositeId(typeName, id));
            var main = await this.GetCollection<T>().Find(mainFilter).FirstOrDefaultAsync(ct).ConfigureAwait(false);
            data = main?[MongoFields.Data].AsBsonDocument.ToJson(
                new MongoDB.Bson.IO.JsonWriterSettings { OutputMode = MongoDB.Bson.IO.JsonOutputMode.RelaxedExtendedJson });
        }

        var now = DateTime.UtcNow;
        var actor = mapping.CaptureActor?.Invoke();

        // Close the currently-open version (there is at most one).
        var openFilter = Builders<BsonDocument>.Filter.And(docFilter, Builders<BsonDocument>.Filter.Eq("ValidTo", BsonNull.Value));
        await histCol.UpdateManyAsync(openFilter, Builders<BsonDocument>.Update.Set("ValidTo", now), cancellationToken: ct).ConfigureAwait(false);

        var nextVersion = existing.Select(d => d["Version"].AsInt64).DefaultIfEmpty(0L).Max() + 1;
        var newDoc = new BsonDocument
        {
            { "_id", $"{typeName}:{id}:{nextVersion}" },
            { "Id", id },
            { "TypeName", typeName },
            { "Version", nextVersion },
            { "ValidFrom", now },
            { "ValidTo", BsonNull.Value },
            { "Operation", operation.ToString() },
            { "Actor", actor == null ? BsonNull.Value : new BsonString(actor) },
            { "Data", data == null ? BsonNull.Value : new BsonString(data) }
        };
        await histCol.InsertOneAsync(newDoc, cancellationToken: ct).ConfigureAwait(false);

        if (mapping.Retention == null && mapping.MaxVersions == null)
            return;

        // Reload the post-insert set (the open row is now closed, plus the new row) for retention.
        var rows = (await histCol.Find(docFilter).ToListAsync(ct).ConfigureAwait(false)).Select(ToEntry).ToList();
        var toDelete = new List<long>();
        if (mapping.Retention != null)
        {
            var cutoff = new DateTimeOffset(now, TimeSpan.Zero) - mapping.Retention.Value;
            toDelete.AddRange(TemporalHistory.PrunableByAge(rows, cutoff).Select(e => e.Version));
        }
        if (mapping.MaxVersions != null)
            toDelete.AddRange(TemporalHistory.PrunableByCount(rows, mapping.MaxVersions.Value).Select(e => e.Version));

        if (toDelete.Count > 0)
        {
            var ids = toDelete.Distinct().Select(v => (BsonValue)$"{typeName}:{id}:{v}").ToList();
            await histCol.DeleteManyAsync(Builders<BsonDocument>.Filter.In("_id", ids), ct).ConfigureAwait(false);
        }
    }

    static HistoryEntry ToEntry(BsonDocument d) => new()
    {
        Id = d["Id"].AsString,
        TypeName = d["TypeName"].AsString,
        Version = d["Version"].AsInt64,
        ValidFrom = new DateTimeOffset(d["ValidFrom"].ToUniversalTime()),
        ValidTo = d["ValidTo"].IsBsonNull ? null : new DateTimeOffset(d["ValidTo"].ToUniversalTime()),
        Operation = d["Operation"].AsString,
        Actor = d["Actor"].IsBsonNull ? null : d["Actor"].AsString,
        Data = d["Data"].IsBsonNull ? null : d["Data"].AsString
    };

    [UnconditionalSuppressMessage("Trimming", "IL2026", Justification = "Reflection path only used when typeInfo is null.")]
    [UnconditionalSuppressMessage("AOT", "IL3050", Justification = "Reflection path only used when typeInfo is null.")]
    T? DeserializeJson<T>(string json, JsonTypeInfo<T>? typeInfo)
        => typeInfo != null ? JsonSerializer.Deserialize(json, typeInfo) : JsonSerializer.Deserialize<T>(json, this.jsonOptions);

    async Task<List<HistoryEntry>> LoadDocRowsAsync<T>(string typeName, string id, CancellationToken ct)
        => (await this.HistoryCollection<T>().Find(DocFilter(id, typeName)).ToListAsync(ct).ConfigureAwait(false))
            .Select(ToEntry).ToList();

    async Task<List<HistoryEntry>> LoadTypeRowsAsync<T>(string typeName, CancellationToken ct)
        => (await this.HistoryCollection<T>().Find(Builders<BsonDocument>.Filter.Eq("TypeName", typeName)).ToListAsync(ct).ConfigureAwait(false))
            .Select(ToEntry).ToList();

    async Task<string?> ReadVersionDataAsync<T>(string typeName, string id, long version, CancellationToken ct)
    {
        var d = await this.HistoryCollection<T>()
            .Find(Builders<BsonDocument>.Filter.Eq("_id", $"{typeName}:{id}:{version}"))
            .FirstOrDefaultAsync(ct).ConfigureAwait(false);
        return d == null || d["Data"].IsBsonNull ? null : d["Data"].AsString;
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
            .Select(e => TemporalHistory.ToVersion<T>(e, j => this.DeserializeJson(j, typeInfo)))
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
        return entry?.Data == null ? null : this.DeserializeJson(entry.Data, typeInfo);
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
            .Select(e => this.DeserializeJson(e.Data!, typeInfo)!)
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
            .Select(e => TemporalHistory.ToVersion<T>(e, j => this.DeserializeJson(j, typeInfo)))
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
            .Select(e => TemporalHistory.ToVersion<T>(e, j => this.DeserializeJson(j, typeInfo)))
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
        var doc = json != null ? this.DeserializeJson(json, typeInfo) : null;
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
        => this.Tracker.Track("get_diff_between", typeof(T).Name, () => this.GetDiffBetweenImpl(id, fromVersion, toVersion, jsonTypeInfo, cancellationToken));

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
