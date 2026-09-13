using System.Collections.Concurrent;
using System.Globalization;
using System.Text.Json;
using Shiny.DocumentDb.Internal;
using StackExchange.Redis;

namespace Shiny.DocumentDb.Redis;

// Unique indexes (MapUniqueIndex). Redis has no secondary unique constraint, so each index entry a document occupies
// is a reservation key {prefix}uq:{hash} whose value is the owning document id. The claim, the document write and the
// release of entries the write gives up all run in one Lua script, so a conflicting reservation leaves nothing
// written. Reservations live outside the doc: namespace, so the RediSearch prefix, key scans, the keyspace change
// feed and backup export never see them.
public partial class RedisDocumentStore
{
    enum RedisWriteGuard { None, MustNotExist, Version }

    readonly ConcurrentDictionary<string, IReadOnlyList<UniqueIndexMapping>> uniqueIndexesByType = new(StringComparer.Ordinal);

    // KEYS: [1] document, [2..claims+1] reservations to claim, [claims+2..] reservations to release.
    // ARGV: [1] owner id, [2] envelope, [3] guard (none|nx|cas), [4] expected version as "[N]", [5] claim count.
    // Returns 1 written, -1 not found, 0 version conflict, -2 already exists, -100-i conflict on claim i (0-based).
    const string WriteScript = @"
local claims = tonumber(ARGV[5])
if ARGV[3] == 'nx' then
  if redis.call('EXISTS', KEYS[1]) == 1 then return -2 end
elseif ARGV[3] == 'cas' then
  if redis.call('EXISTS', KEYS[1]) == 0 then return -1 end
  if redis.call('JSON.GET', KEYS[1], '$.version') ~= ARGV[4] then return 0 end
end
for i = 2, claims + 1 do
  local owner = redis.call('GET', KEYS[i])
  if owner and owner ~= ARGV[1] then return -100 - (i - 2) end
end
for i = 2, claims + 1 do
  redis.call('SET', KEYS[i], ARGV[1])
end
redis.call('JSON.SET', KEYS[1], '$', ARGV[2])
for i = claims + 2, #KEYS do
  if redis.call('GET', KEYS[i]) == ARGV[1] then redis.call('DEL', KEYS[i]) end
end
return 1";

    // KEYS: [1] document, [2..] reservations it holds. ARGV: [1] owner id. Returns the number of documents deleted.
    const string DeleteScript = @"
local deleted = redis.call('DEL', KEYS[1])
if deleted == 1 then
  for i = 2, #KEYS do
    if redis.call('GET', KEYS[i]) == ARGV[1] then redis.call('DEL', KEYS[i]) end
  end
end
return deleted";

    // KEYS: [1] reservation, [2] owner document. ARGV: [1] owner id, [2] the owner's JSON as inspected ('' = absent).
    // Releases the reservation only if neither it nor the owner document changed since the caller judged it stale.
    const string ReleaseStaleScript = @"
if redis.call('GET', KEYS[1]) ~= ARGV[1] then return 0 end
local current = redis.call('JSON.GET', KEYS[2])
if ARGV[2] == '' then
  if current then return 0 end
elseif current ~= ARGV[2] then
  return 0
end
redis.call('DEL', KEYS[1])
return 1";

    const int MaxUniqueWriteAttempts = 3;

    string UniqueKey(UniqueIndexEntry entry) => $"{this.keyPrefix}uq:{entry.Hash}";

    IReadOnlyList<UniqueIndexMapping> UniqueIndexesFor(string typeName)
        => this.uniqueIndexesByType.GetOrAdd(typeName, name => this.options.Mappings.UniqueIndexes
            .Where(m => TypeNameResolver.Resolve(m.DocumentType, this.options.TypeNameResolution) == name)
            .ToList());

    // The entries a stored or about-to-be-stored body occupies. The document is only materialized when an index has a
    // filter; without a typed factory it is deserialized through the store's serializer options.
    IReadOnlyList<UniqueIndexEntry> UniqueEntries(string typeName, string dataJson, Func<object>? document)
    {
        var indexes = this.UniqueIndexesFor(typeName);
        if (indexes.Count == 0)
            return [];

        return UniqueIndexKeys.Compute(indexes, typeName, dataJson,
            document ?? (() => JsonSerializer.Deserialize(dataJson, this.jsonOptions.GetTypeInfo(indexes[0].DocumentType))!),
            this.jsonOptions);
    }

    async Task<IReadOnlyList<UniqueIndexEntry>> StoredUniqueEntriesAsync(string typeName, string key)
    {
        if (this.UniqueIndexesFor(typeName).Count == 0)
            return [];

        var env = await this.GetEnvelopeAsync(key).ConfigureAwait(false);
        var dataJson = env == null ? null : RedisDocument.GetDataJson(env);
        return dataJson == null ? [] : this.UniqueEntries(typeName, dataJson, null);
    }

    /// <summary>
    /// Writes a document envelope while claiming <paramref name="after"/> and releasing what <paramref name="before"/>
    /// held that <paramref name="after"/> does not. Without entries on either side it is exactly the plain write the
    /// guard calls for.
    /// </summary>
    async Task PersistAsync(
        string key,
        string envelope,
        string typeName,
        string id,
        RedisWriteGuard guard,
        int? expectedVersion,
        IReadOnlyList<UniqueIndexEntry> before,
        IReadOnlyList<UniqueIndexEntry> after,
        CancellationToken ct)
    {
        if (before.Count == 0 && after.Count == 0)
        {
            switch (guard)
            {
                case RedisWriteGuard.MustNotExist:
                    if (!await this.SetJsonIfNotExistsAsync(key, envelope).ConfigureAwait(false))
                        throw new InvalidOperationException($"A document of type '{typeName}' with Id '{id}' already exists.");
                    return;
                case RedisWriteGuard.Version:
                    await this.WriteWithVersionGuardAsync(key, envelope, typeName, id, expectedVersion, ct).ConfigureAwait(false);
                    return;
                default:
                    await this.SetJsonAsync(key, envelope).ConfigureAwait(false);
                    return;
            }
        }

        // Every current entry is claimed, not just the new ones: re-claiming an entry this document already owns is a
        // no-op, and it back-fills the reservation for a document stored before the index was mapped.
        var released = UniqueIndexKeys.Diff(before, after).Removed;
        var keys = new RedisKey[1 + after.Count + released.Count];
        keys[0] = key;
        for (var i = 0; i < after.Count; i++)
            keys[1 + i] = this.UniqueKey(after[i]);
        for (var i = 0; i < released.Count; i++)
            keys[1 + after.Count + i] = this.UniqueKey(released[i]);

        var args = new RedisValue[]
        {
            id,
            envelope,
            guard switch { RedisWriteGuard.MustNotExist => "nx", RedisWriteGuard.Version => "cas", _ => "none" },
            expectedVersion is { } v ? $"[{v.ToString(CultureInfo.InvariantCulture)}]" : "",
            after.Count
        };

        for (var attempt = 1; ; attempt++)
        {
            var code = (long)await this.db.ScriptEvaluateAsync(WriteScript, keys, args).ConfigureAwait(false);
            switch (code)
            {
                case 1:
                    return;
                case -1:
                    throw new InvalidOperationException($"No document of type '{typeName}' with Id '{id}' was found to update.");
                case 0:
                    throw new ConcurrencyException(typeName, id, expectedVersion!.Value);
                case -2:
                    throw new InvalidOperationException($"A document of type '{typeName}' with Id '{id}' already exists.");
            }

            var conflict = after[(int)(-100 - code)];
            if (attempt >= MaxUniqueWriteAttempts || !await this.ReleaseIfStaleAsync(typeName, conflict).ConfigureAwait(false))
                throw new UniqueConstraintException(typeName, conflict.Mapping, id);
        }
    }

    /// <summary>
    /// Checks, before a multi-document write commits anything, that no two documents in it share an entry and that
    /// no entry is held by another stored document — so a batch conflict fails before its first write.
    /// </summary>
    async Task EnsureClaimableAsync(string typeName, IReadOnlyList<(string Id, IReadOnlyList<UniqueIndexEntry> Entries)> writes)
    {
        var claimedBy = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (var (id, entries) in writes)
        {
            foreach (var entry in entries)
            {
                if (claimedBy.TryGetValue(entry.Key, out var other) && other != id)
                    throw new UniqueConstraintException(typeName, entry.Mapping, id);
                claimedBy[entry.Key] = id;

                var owner = (string?)await this.db.StringGetAsync(this.UniqueKey(entry)).ConfigureAwait(false);
                if (owner != null && owner != id && !await this.ReleaseIfStaleAsync(typeName, entry).ConfigureAwait(false))
                    throw new UniqueConstraintException(typeName, entry.Mapping, id);
            }
        }
    }

    // A reservation is stale when its owner no longer exists or no longer holds the entry — left behind by a write
    // that raced another on the same document, or by a document removed outside the store. Returns true when the
    // caller should retry: the reservation is gone, or changed under us.
    async Task<bool> ReleaseIfStaleAsync(string typeName, UniqueIndexEntry entry)
    {
        var reservation = this.UniqueKey(entry);
        var owner = (string?)await this.db.StringGetAsync(reservation).ConfigureAwait(false);
        if (owner == null)
            return true;

        var ownerKey = this.DocKey(typeName, owner);
        var raw = await this.db.ExecuteAsync("JSON.GET", ownerKey).ConfigureAwait(false);
        var rawJson = raw.IsNull ? null : (string?)raw;
        var env = RedisDocument.ParseEnvelope(rawJson);
        var dataJson = env == null ? null : RedisDocument.GetDataJson(env);
        if (dataJson != null && this.UniqueEntries(typeName, dataJson, null).Any(e => e.Key == entry.Key))
            return false;

        await this.db.ScriptEvaluateAsync(ReleaseStaleScript,
            new RedisKey[] { reservation, ownerKey },
            new RedisValue[] { owner, rawJson ?? "" }).ConfigureAwait(false);
        return true;
    }

    /// <summary>Deletes a document and releases the reservations it holds, atomically.</summary>
    async Task<bool> DeleteDocumentAsync(string typeName, string id)
    {
        var key = this.DocKey(typeName, id);
        if (this.UniqueIndexesFor(typeName).Count == 0)
            return await this.DeleteKeyAsync(key).ConfigureAwait(false);

        var held = await this.StoredUniqueEntriesAsync(typeName, key).ConfigureAwait(false);
        var keys = new RedisKey[1 + held.Count];
        keys[0] = key;
        for (var i = 0; i < held.Count; i++)
            keys[1 + i] = this.UniqueKey(held[i]);

        var deleted = (long)await this.db.ScriptEvaluateAsync(DeleteScript, keys, new RedisValue[] { id }).ConfigureAwait(false);
        return deleted > 0;
    }

    string IdFromDocKey(string typeName, string key) => key[this.DocIndexPrefix(typeName).Length..];
}
