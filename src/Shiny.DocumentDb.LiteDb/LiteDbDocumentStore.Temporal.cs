using System.Globalization;
using System.Text.Json.Serialization.Metadata;
using LiteDB;
using Shiny.DocumentDb.Internal;

namespace Shiny.DocumentDb.LiteDb;

public partial class LiteDbDocumentStore
{
    // The history sidecar mirrors the relational {table}_history shape, stored as BsonDocuments:
    //   _id = "{typeName}:{id}:{version}", Id, TypeName, Version (Int64),
    //   ValidFrom / ValidTo (ISO-8601 "o" strings, like the main envelope's timestamps),
    //   Operation, Actor (nullable), Data (nullable — null is a Removed tombstone).
    ILiteCollection<BsonDocument> HistoryCollection<T>()
        => this.db.GetCollection<BsonDocument>(this.ResolveCollectionName<T>() + "_history");

    void EnsureTemporal<T>()
    {
        if (this.options.ResolveTemporalMapping(typeof(T)) == null)
            throw new InvalidOperationException(
                $"Type '{typeof(T).Name}' is not configured for temporal history. " +
                $"Call options.MapTemporal<{typeof(T).Name}>() during setup.");
    }

    /// <summary>
    /// Closes the current open version and appends a new one to the history sidecar, then applies
    /// retention. For writes that don't carry the full post-image (Upsert merge, SetProperty,
    /// RemoveProperty), <paramref name="providedJson"/> is null and the post-image is read back from the
    /// main collection. Removed writes store a null-body tombstone. No-op for non-temporal types.
    /// </summary>
    internal void AppendHistory<T>(string id, string typeName, TemporalOperation operation, string? providedJson) where T : class
    {
        var mapping = this.options.ResolveTemporalMapping(typeof(T));
        if (mapping == null)
            return;

        var histCol = this.HistoryCollection<T>();
        var existing = histCol
            .Find(LiteDB.Query.And(LiteDB.Query.EQ("Id", id), LiteDB.Query.EQ("TypeName", typeName)))
            .ToList();

        var data = providedJson;
        if (data == null && operation != TemporalOperation.Removed)
        {
            var main = this.GetCollection<T>().FindById($"{typeName}:{id}");
            data = main != null && !main["Data"].IsNull ? main["Data"].AsString : null;
        }

        var now = DateTimeOffset.UtcNow;
        var nowStr = now.ToString("o");
        var actor = mapping.CaptureActor?.Invoke();

        // Close the currently-open version (there is at most one).
        foreach (var open in existing.Where(d => d["ValidTo"].IsNull))
        {
            open["ValidTo"] = nowStr;
            histCol.Update(open);
        }

        var nextVersion = existing.Select(d => d["Version"].AsInt64).DefaultIfEmpty(0L).Max() + 1;
        var newDoc = new BsonDocument
        {
            ["_id"] = $"{typeName}:{id}:{nextVersion}",
            ["Id"] = id,
            ["TypeName"] = typeName,
            ["Version"] = nextVersion,
            ["ValidFrom"] = nowStr,
            ["ValidTo"] = BsonValue.Null,
            ["Operation"] = operation.ToString(),
            ["Actor"] = actor == null ? BsonValue.Null : new BsonValue(actor),
            ["Data"] = data == null ? BsonValue.Null : new BsonValue(data)
        };
        histCol.Insert(newDoc);

        if (mapping.Retention == null && mapping.MaxVersions == null)
            return;

        // The post-insert set: existing rows (the open one now closed) plus the new row.
        var rows = existing.Select(ToEntry).ToList();
        rows.Add(ToEntry(newDoc));

        if (mapping.Retention != null)
        {
            var cutoff = now - mapping.Retention.Value;
            foreach (var e in TemporalHistory.PrunableByAge(rows, cutoff))
                histCol.Delete($"{typeName}:{id}:{e.Version}");
        }
        if (mapping.MaxVersions != null)
        {
            foreach (var e in TemporalHistory.PrunableByCount(rows, mapping.MaxVersions.Value))
                histCol.Delete($"{typeName}:{id}:{e.Version}");
        }
    }

    static HistoryEntry ToEntry(BsonDocument d) => new()
    {
        Id = d["Id"].AsString,
        TypeName = d["TypeName"].AsString,
        Version = d["Version"].AsInt64,
        ValidFrom = ParseDto(d["ValidFrom"].AsString),
        ValidTo = d["ValidTo"].IsNull ? null : ParseDto(d["ValidTo"].AsString),
        Operation = d["Operation"].AsString,
        Actor = d["Actor"].IsNull ? null : d["Actor"].AsString,
        Data = d["Data"].IsNull ? null : d["Data"].AsString
    };

    static DateTimeOffset ParseDto(string s)
        => DateTimeOffset.Parse(s, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind);

    List<HistoryEntry> LoadDocRows<T>(string typeName, string id)
        => this.HistoryCollection<T>()
            .Find(LiteDB.Query.And(LiteDB.Query.EQ("Id", id), LiteDB.Query.EQ("TypeName", typeName)))
            .Select(ToEntry)
            .ToList();

    List<HistoryEntry> LoadTypeRows<T>(string typeName)
        => this.HistoryCollection<T>()
            .Find(LiteDB.Query.EQ("TypeName", typeName))
            .Select(ToEntry)
            .ToList();

    string? ReadVersionData<T>(string typeName, string id, long version)
    {
        var d = this.HistoryCollection<T>().FindById($"{typeName}:{id}:{version}");
        return d == null || d["Data"].IsNull ? null : d["Data"].AsString;
    }

    // ── ITemporalDocumentStore ──────────────────────────────────────────

    public Task<IReadOnlyList<DocumentVersion<T>>> History<T>(object id, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class
        => this.Tracker.Track("history", typeof(T).Name, () => this.HistoryImpl(id, jsonTypeInfo, cancellationToken), r => r.Count);

    Task<IReadOnlyList<DocumentVersion<T>>> HistoryImpl<T>(object id, JsonTypeInfo<T>? jsonTypeInfo, CancellationToken cancellationToken) where T : class
    {
        this.EnsureTemporal<T>();
        var typeInfo = this.FindTypeInfo(jsonTypeInfo);
        var resolvedId = this.idCache.GetOrCreate(typeInfo).ResolveId(id);
        var typeName = this.ResolveTypeName<T>();
        var rows = this.LoadDocRows<T>(typeName, resolvedId);
        var list = TemporalHistory.History(rows)
            .Select(e => TemporalHistory.ToVersion<T>(e, j => Deserialize(j, typeInfo, this.jsonOptions)))
            .ToList();
        return Task.FromResult<IReadOnlyList<DocumentVersion<T>>>(list);
    }

    public Task<T?> AsOf<T>(object id, DateTimeOffset asOf, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class
        => this.Tracker.Track("as_of", typeof(T).Name, () => this.AsOfImpl(id, asOf, jsonTypeInfo, cancellationToken), r => r is null ? 0 : 1);

    Task<T?> AsOfImpl<T>(object id, DateTimeOffset asOf, JsonTypeInfo<T>? jsonTypeInfo, CancellationToken cancellationToken) where T : class
    {
        this.EnsureTemporal<T>();
        var typeInfo = this.FindTypeInfo(jsonTypeInfo);
        var resolvedId = this.idCache.GetOrCreate(typeInfo).ResolveId(id);
        var typeName = this.ResolveTypeName<T>();
        var rows = this.LoadDocRows<T>(typeName, resolvedId);
        var entry = TemporalHistory.AsOf(rows, asOf.ToUniversalTime());
        var result = entry?.Data == null ? null : Deserialize(entry.Data, typeInfo, this.jsonOptions);
        return Task.FromResult(result);
    }

    public Task<IReadOnlyList<T>> AsOfAll<T>(DateTimeOffset asOf, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class
        => this.Tracker.Track("as_of_all", typeof(T).Name, () => this.AsOfAllImpl(asOf, jsonTypeInfo, cancellationToken), r => r.Count);

    Task<IReadOnlyList<T>> AsOfAllImpl<T>(DateTimeOffset asOf, JsonTypeInfo<T>? jsonTypeInfo, CancellationToken cancellationToken) where T : class
    {
        this.EnsureTemporal<T>();
        var typeInfo = this.FindTypeInfo(jsonTypeInfo);
        var typeName = this.ResolveTypeName<T>();
        var rows = this.LoadTypeRows<T>(typeName);
        var list = TemporalHistory.AsOfAll(rows, asOf.ToUniversalTime())
            .Select(e => Deserialize(e.Data!, typeInfo, this.jsonOptions)!)
            .ToList();
        return Task.FromResult<IReadOnlyList<T>>(list);
    }

    public Task<IReadOnlyList<DocumentVersion<T>>> ChangesByActor<T>(string actor, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class
        => this.Tracker.Track("changes_by_actor", typeof(T).Name, () => this.ChangesByActorImpl(actor, jsonTypeInfo, cancellationToken), r => r.Count);

    Task<IReadOnlyList<DocumentVersion<T>>> ChangesByActorImpl<T>(string actor, JsonTypeInfo<T>? jsonTypeInfo, CancellationToken cancellationToken) where T : class
    {
        ArgumentNullException.ThrowIfNull(actor);
        this.EnsureTemporal<T>();
        var typeInfo = this.FindTypeInfo(jsonTypeInfo);
        var typeName = this.ResolveTypeName<T>();
        var rows = this.LoadTypeRows<T>(typeName);
        var list = TemporalHistory.ByActor(rows, actor)
            .Select(e => TemporalHistory.ToVersion<T>(e, j => Deserialize(j, typeInfo, this.jsonOptions)))
            .ToList();
        return Task.FromResult<IReadOnlyList<DocumentVersion<T>>>(list);
    }

    public Task<IReadOnlyList<DocumentVersion<T>>> ChangesBetween<T>(DateTimeOffset from, DateTimeOffset to, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class
        => this.Tracker.Track("changes_between", typeof(T).Name, () => this.ChangesBetweenImpl(from, to, jsonTypeInfo, cancellationToken), r => r.Count);

    Task<IReadOnlyList<DocumentVersion<T>>> ChangesBetweenImpl<T>(DateTimeOffset from, DateTimeOffset to, JsonTypeInfo<T>? jsonTypeInfo, CancellationToken cancellationToken) where T : class
    {
        this.EnsureTemporal<T>();
        var typeInfo = this.FindTypeInfo(jsonTypeInfo);
        var typeName = this.ResolveTypeName<T>();
        var rows = this.LoadTypeRows<T>(typeName);
        var list = TemporalHistory.Between(rows, from.ToUniversalTime(), to.ToUniversalTime())
            .Select(e => TemporalHistory.ToVersion<T>(e, j => Deserialize(j, typeInfo, this.jsonOptions)))
            .ToList();
        return Task.FromResult<IReadOnlyList<DocumentVersion<T>>>(list);
    }

    public Task<T?> Restore<T>(object id, long version, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class
        => this.Tracker.Track("restore", typeof(T).Name, () => this.RestoreImpl(id, version, jsonTypeInfo, cancellationToken), r => r is null ? 0 : 1);

    async Task<T?> RestoreImpl<T>(object id, long version, JsonTypeInfo<T>? jsonTypeInfo, CancellationToken cancellationToken) where T : class
    {
        this.EnsureTemporal<T>();
        var typeInfo = this.FindTypeInfo(jsonTypeInfo);
        var resolvedId = this.idCache.GetOrCreate(typeInfo).ResolveId(id);
        var typeName = this.ResolveTypeName<T>();

        var json = this.ReadVersionData<T>(typeName, resolvedId, version);
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

    Task<JsonPatchDocument<T>?> GetDiffBetweenImpl<T>(object id, long fromVersion, long toVersion, JsonTypeInfo<T>? jsonTypeInfo, CancellationToken cancellationToken) where T : class
    {
        this.EnsureTemporal<T>();
        var typeInfo = this.FindTypeInfo(jsonTypeInfo);
        var resolvedId = this.idCache.GetOrCreate(typeInfo).ResolveId(id);
        var typeName = this.ResolveTypeName<T>();

        var fromJson = this.ReadVersionData<T>(typeName, resolvedId, fromVersion);
        var toJson = this.ReadVersionData<T>(typeName, resolvedId, toVersion);
        if (fromJson == null || toJson == null)
            return Task.FromResult<JsonPatchDocument<T>?>(null);
        return Task.FromResult<JsonPatchDocument<T>?>(JsonDiff.CreatePatch<T>(fromJson, toJson, this.jsonOptions));
    }
}
