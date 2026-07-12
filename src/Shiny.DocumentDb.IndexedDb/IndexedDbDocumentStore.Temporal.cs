using System.Globalization;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;
using Shiny.DocumentDb.Internal;

namespace Shiny.DocumentDb.IndexedDb;

public partial class IndexedDbDocumentStore
{
    // Temporal history lives in a "{store}_history" object store with the same key/typeName schema as
    // the main stores, so the existing JS interop (put / getAllByTypeName / remove / batchDelete)
    // manages it. Keys are "{typeName}:{id}:{version}".
    string ResolveHistoryStoreName<T>() => this.options.ResolveHistoryStoreName(this.ResolveTypeName<T>());

    void EnsureTemporal<T>()
    {
        if (this.options.ResolveTemporalMapping(typeof(T)) == null)
            throw new InvalidOperationException(
                $"Type '{typeof(T).Name}' is not configured for temporal history. " +
                $"Call options.MapTemporal<{typeof(T).Name}>() during setup.");
    }

    static string SerializeHistory(HistoryRecord record)
        => JsonSerializer.Serialize(record, IndexedDbInteropJsonContext.Default.HistoryRecord);

    static HistoryRecord[] DeserializeHistory(string json)
        => JsonSerializer.Deserialize(json, IndexedDbInteropJsonContext.Default.HistoryRecordArray) ?? Array.Empty<HistoryRecord>();

    /// <summary>
    /// Closes the current open version and appends a new one to the history store, then applies
    /// retention. For writes that don't carry the full post-image (Removed), <paramref name="providedJson"/>
    /// is null. Removed writes store a null-body tombstone. No-op for non-temporal types.
    /// </summary>
    internal async Task AppendHistoryAsync<T>(string id, string typeName, TemporalOperation operation, string? providedJson) where T : class
    {
        var mapping = this.options.ResolveTemporalMapping(typeof(T));
        if (mapping == null)
            return;

        await this.EnsureModuleAsync();
        var histStore = this.ResolveHistoryStoreName<T>();

        var data = providedJson;
        if (data == null && operation != TemporalOperation.Removed)
        {
            var mainJson = await IndexedDbJsInterop.Get(this.ResolveStoreName<T>(), $"{typeName}:{id}");
            if (mainJson != null)
                data = JsonSerializer.Deserialize(mainJson, IndexedDbInteropJsonContext.Default.DocumentRecord)!.Data;
        }

        var existing = DeserializeHistory(await IndexedDbJsInterop.GetAllByTypeName(histStore, typeName))
            .Where(r => r.Id == id)
            .ToList();

        var now = DateTimeOffset.UtcNow;
        var nowStr = now.ToString("o");
        var actor = mapping.CaptureActor?.Invoke();

        // Close the currently-open version (there is at most one).
        foreach (var open in existing.Where(r => r.ValidTo == null))
        {
            open.ValidTo = nowStr;
            await IndexedDbJsInterop.Put(histStore, SerializeHistory(open));
        }

        var nextVersion = existing.Select(r => r.Version).DefaultIfEmpty(0L).Max() + 1;
        var newRecord = new HistoryRecord
        {
            Key = $"{typeName}:{id}:{nextVersion}",
            Id = id,
            TypeName = typeName,
            Version = nextVersion,
            ValidFrom = nowStr,
            ValidTo = null,
            Operation = operation.ToString(),
            Actor = actor,
            Data = data
        };
        await IndexedDbJsInterop.Put(histStore, SerializeHistory(newRecord));

        if (mapping.Retention == null && mapping.MaxVersions == null)
            return;

        var rows = existing.Select(ToEntry).ToList();
        rows.Add(ToEntry(newRecord));
        var toDelete = new List<long>();
        if (mapping.Retention != null)
        {
            var cutoff = now - mapping.Retention.Value;
            toDelete.AddRange(TemporalHistory.PrunableByAge(rows, cutoff).Select(e => e.Version));
        }
        if (mapping.MaxVersions != null)
            toDelete.AddRange(TemporalHistory.PrunableByCount(rows, mapping.MaxVersions.Value).Select(e => e.Version));

        var keys = toDelete.Distinct().Select(v => $"{typeName}:{id}:{v}").ToArray();
        if (keys.Length > 0)
            await IndexedDbJsInterop.BatchDelete(histStore, keys);
    }

    static HistoryEntry ToEntry(HistoryRecord r) => new()
    {
        Id = r.Id,
        TypeName = r.TypeName,
        Version = r.Version,
        ValidFrom = ParseDto(r.ValidFrom),
        ValidTo = r.ValidTo == null ? null : ParseDto(r.ValidTo),
        Operation = r.Operation,
        Actor = r.Actor,
        Data = r.Data
    };

    static DateTimeOffset ParseDto(string s)
        => DateTimeOffset.Parse(s, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind);

    async Task<List<HistoryEntry>> LoadDocRowsAsync<T>(string typeName, string id)
    {
        await this.EnsureModuleAsync();
        var histStore = this.ResolveHistoryStoreName<T>();
        return DeserializeHistory(await IndexedDbJsInterop.GetAllByTypeName(histStore, typeName))
            .Where(r => r.Id == id)
            .Select(ToEntry)
            .ToList();
    }

    async Task<List<HistoryEntry>> LoadTypeRowsAsync<T>(string typeName)
    {
        await this.EnsureModuleAsync();
        var histStore = this.ResolveHistoryStoreName<T>();
        return DeserializeHistory(await IndexedDbJsInterop.GetAllByTypeName(histStore, typeName))
            .Select(ToEntry)
            .ToList();
    }

    async Task<string?> ReadVersionDataAsync<T>(string typeName, string id, long version)
    {
        await this.EnsureModuleAsync();
        var histStore = this.ResolveHistoryStoreName<T>();
        var json = await IndexedDbJsInterop.Get(histStore, $"{typeName}:{id}:{version}");
        if (json == null)
            return null;
        return JsonSerializer.Deserialize(json, IndexedDbInteropJsonContext.Default.HistoryRecord)!.Data;
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
        var rows = await this.LoadDocRowsAsync<T>(typeName, resolvedId);
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
        var rows = await this.LoadDocRowsAsync<T>(typeName, resolvedId);
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
        var rows = await this.LoadTypeRowsAsync<T>(typeName);
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
        var rows = await this.LoadTypeRowsAsync<T>(typeName);
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
        var rows = await this.LoadTypeRowsAsync<T>(typeName);
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

        var json = await this.ReadVersionDataAsync<T>(typeName, resolvedId, version);
        var doc = json != null ? Deserialize(json, typeInfo, this.jsonOptions) : null;
        if (doc == null)
            return null;

        var versionMapping = this.options.ResolveVersionMapping(typeof(T));
        var current = await this.Get(id, typeInfo, cancellationToken);
        if (current == null)
        {
            await this.Insert(doc, typeInfo, cancellationToken);
        }
        else
        {
            versionMapping?.SetVersion(doc, versionMapping.GetVersion(current));
            await this.Update(doc, typeInfo, cancellationToken);
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

        var fromJson = await this.ReadVersionDataAsync<T>(typeName, resolvedId, fromVersion);
        var toJson = await this.ReadVersionDataAsync<T>(typeName, resolvedId, toVersion);
        if (fromJson == null || toJson == null)
            return null;
        return JsonDiff.CreatePatch<T>(fromJson, toJson, this.jsonOptions);
    }
}
