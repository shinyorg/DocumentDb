using System.Text.Json.Serialization.Metadata;
using Raven.Client.Documents.Operations.CompareExchange;
using Raven.Client.Documents.Session;

namespace Shiny.DocumentDb.RavenDb;

// Unique indexes (MapUniqueIndex) as compare-exchange reservations. The document body is opaque to RavenDB, so no
// index can see the key; instead each index entry a document occupies is a compare-exchange value keyed
// "uq/{hash}" whose value names the owning document. Compare-exchange puts are cluster-consistent CAS writes, so two
// writers can never both claim a key.
//
// A write claims the entries it adds before saving the document, releases those claims if the save fails, and
// releases the entries it dropped after the save succeeds. That is compensating rather than transactional (a
// cluster-wide session would be atomic, but it forbids the attachments blobs ride on and replaces the change-vector
// optimistic concurrency every write path relies on). A crash between the steps can leave a reservation whose owner
// does not hold the value; the next claim on that key verifies the owner and reclaims it once the reservation is
// older than the grace period — the grace keeps a claim whose document save is still in flight from being stolen.
public partial class RavenDbDocumentStore
{
    const string ReservationPrefix = "uq/";
    const int MaxClaimAttempts = 5;
    const int ReservationPageSize = 1024;
    static readonly TimeSpan ReservationGrace = TimeSpan.FromSeconds(30);

    static string ReservationKey(UniqueIndexEntry entry) => ReservationPrefix + entry.Hash;

    IReadOnlyList<UniqueIndexEntry> UniqueEntriesOf<T>(string typeName, T document, string json) where T : class
    {
        var indexes = this.options.Mappings.ResolveUniqueIndexes(typeof(T));
        return indexes.Count == 0
            ? Array.Empty<UniqueIndexEntry>()
            : UniqueIndexKeys.Compute(indexes, typeName, document, json, this.jsonOptions);
    }

    IReadOnlyList<UniqueIndexEntry> StoredUniqueEntries<T>(string typeName, string json, JsonTypeInfo<T>? typeInfo) where T : class
    {
        var indexes = this.options.Mappings.ResolveUniqueIndexes(typeof(T));
        return indexes.Count == 0
            ? Array.Empty<UniqueIndexEntry>()
            : UniqueIndexKeys.Compute(indexes, typeName, json, () => Deserialize(json, typeInfo, this.jsonOptions)!, this.jsonOptions);
    }

    static (IReadOnlyList<UniqueIndexEntry> Added, IReadOnlyList<UniqueIndexEntry> Removed) UniqueChanges(
        IReadOnlyList<UniqueIndexEntry> before,
        IReadOnlyList<UniqueIndexEntry> after)
        => before.Count == 0 && after.Count == 0
            ? (Array.Empty<UniqueIndexEntry>(), Array.Empty<UniqueIndexEntry>())
            : UniqueIndexKeys.Diff(before, after);

    /// <summary>
    /// Claims every entry for the document <paramref name="id"/>, adding the claims it newly took to
    /// <paramref name="into"/>. On a conflict everything claimed into that set is released before the throw.
    /// </summary>
    async Task<UniqueClaims> ClaimUniqueAsync<T>(
        IReadOnlyList<UniqueIndexEntry> entries,
        string typeName,
        string id,
        JsonTypeInfo<T>? typeInfo,
        CancellationToken ct,
        UniqueClaims? into = null) where T : class
    {
        var claims = into ?? new UniqueClaims();
        if (entries.Count == 0)
            return claims;

        var owner = RavenDbDocument.RavenId(typeName, id);
        try
        {
            foreach (var entry in entries)
                await this.ClaimOneAsync(entry, typeName, id, owner, typeInfo, claims, ct).ConfigureAwait(false);
        }
        catch
        {
            await this.ReleaseUniqueAsync(claims.Fresh).ConfigureAwait(false);
            throw;
        }
        return claims;
    }

    async Task ClaimOneAsync<T>(UniqueIndexEntry entry, string typeName, string id, string owner, JsonTypeInfo<T>? typeInfo, UniqueClaims claims, CancellationToken ct) where T : class
    {
        var key = ReservationKey(entry);
        var operations = this.ravenStore.Operations;
        for (var attempt = 0; attempt < MaxClaimAttempts; attempt++)
        {
            var existing = await operations.SendAsync(new GetCompareExchangeValueOperation<UniqueReservation>(key), token: ct).ConfigureAwait(false);
            var ownedAlready = existing?.Value?.Owner == owner;
            if (existing?.Value != null && !ownedAlready && !await this.IsStaleReservationAsync(existing.Value, entry, typeInfo, ct).ConfigureAwait(false))
                throw new UniqueConstraintException(typeName, entry.Mapping, id);

            // CAS against the index just read: 0 creates, anything else replaces exactly the reservation we judged
            // (our own, or a stale one) — a concurrent claimer makes this fail and the loop re-reads.
            var claim = new UniqueReservation { Owner = owner, ClaimedAtUtcTicks = DateTime.UtcNow.Ticks };
            var result = await operations.SendAsync(new PutCompareExchangeValueOperation<UniqueReservation>(key, claim, existing?.Index ?? 0), token: ct).ConfigureAwait(false);
            if (result.Successful)
            {
                // A reservation the document already held stays its own if the write fails, so only new claims are
                // compensated.
                if (!ownedAlready)
                    claims.Fresh.Add((key, owner));
                return;
            }
        }
        throw new UniqueConstraintException(typeName, entry.Mapping, id);
    }

    // Stale = old enough that no save can still be in flight for it, and its owner no longer holds the value.
    async Task<bool> IsStaleReservationAsync<T>(UniqueReservation reservation, UniqueIndexEntry entry, JsonTypeInfo<T>? typeInfo, CancellationToken ct) where T : class
    {
        if (DateTime.UtcNow.Ticks - reservation.ClaimedAtUtcTicks < ReservationGrace.Ticks)
            return false;

        using var session = this.NewRavenSession();
        var holder = await session.LoadAsync<RavenDbDocument>(reservation.Owner, ct).ConfigureAwait(false);
        return holder == null
            || this.StoredUniqueEntries(holder.TypeName, holder.DataJson, typeInfo).All(e => e.Key != entry.Key);
    }

    /// <summary>Saves the session, releasing <paramref name="claims"/> if the save fails.</summary>
    async Task SaveClaimedAsync(IAsyncDocumentSession session, UniqueClaims claims, CancellationToken ct)
    {
        try
        {
            await session.SaveChangesAsync(ct).ConfigureAwait(false);
        }
        catch
        {
            await this.ReleaseUniqueAsync(claims.Fresh).ConfigureAwait(false);
            throw;
        }
    }

    Task ReleaseUniqueAsync(IReadOnlyList<UniqueIndexEntry> entries, string ownerRavenId)
        => entries.Count == 0
            ? Task.CompletedTask
            : this.ReleaseUniqueAsync(entries.Select(e => (ReservationKey(e), ownerRavenId)).ToList());

    // Deletes each reservation still held by its owner. Never cancelled and never throws: it runs after the document
    // write already happened (or as compensation for one that failed), and a reservation left behind is reclaimed.
    async Task ReleaseUniqueAsync(IReadOnlyList<(string Key, string Owner)> reservations)
    {
        var operations = this.ravenStore.Operations;
        foreach (var (key, owner) in reservations)
        {
            try
            {
                var existing = await operations.SendAsync(new GetCompareExchangeValueOperation<UniqueReservation>(key)).ConfigureAwait(false);
                if (existing?.Value?.Owner == owner)
                    await operations.SendAsync(new DeleteCompareExchangeValueOperation<UniqueReservation>(key, existing.Index)).ConfigureAwait(false);
            }
            catch
            {
                // Best-effort — the next claim on this key reclaims it after the grace period.
            }
        }
    }

    // ClearAll wipes the database's documents, so every reservation goes with them.
    async Task ReleaseAllReservationsAsync(CancellationToken ct)
    {
        var operations = this.ravenStore.Operations;
        var more = true;
        while (more)
        {
            var page = await operations
                .SendAsync(new GetCompareExchangeValuesOperation<UniqueReservation>(ReservationPrefix, 0, ReservationPageSize), token: ct)
                .ConfigureAwait(false);
            var deleted = 0;
            foreach (var (key, value) in page)
            {
                var result = await operations.SendAsync(new DeleteCompareExchangeValueOperation<UniqueReservation>(key, value.Index), token: ct).ConfigureAwait(false);
                if (result.Successful)
                    deleted++;
            }
            more = page.Count == ReservationPageSize && deleted > 0;
        }
    }

    // A unit of work's rollback of a tracked insert: the same delete, plus the release of the document's reservations.
    internal async Task DeleteTrackedDocumentAsync<T>(string typeName, string id, CancellationToken ct) where T : class
    {
        var ravenId = RavenDbDocument.RavenId(typeName, id);
        using var session = this.NewRavenSession();
        var wrapper = await session.LoadAsync<RavenDbDocument>(ravenId, ct).ConfigureAwait(false);
        if (wrapper == null)
            return;

        var held = this.StoredUniqueEntries<T>(typeName, wrapper.DataJson, null);
        session.Delete(wrapper);
        await session.SaveChangesAsync(ct).ConfigureAwait(false);
        await this.ReleaseUniqueAsync(held, ravenId).ConfigureAwait(false);
    }

    internal bool HasUniqueIndexes<T>() => this.options.Mappings.ResolveUniqueIndexes(typeof(T)).Count > 0;

    /// <summary>The compare-exchange value behind one unique-index reservation.</summary>
    sealed class UniqueReservation
    {
        /// <summary>The RavenDB id (<c>{typeName}/{id}</c>) of the document holding the value.</summary>
        public string Owner { get; set; } = "";

        /// <summary>When the claim was made (UTC ticks) — reservations younger than the grace period are never reclaimed.</summary>
        public long ClaimedAtUtcTicks { get; set; }
    }

    /// <summary>The reservations a write newly took, so a failed write can give them back.</summary>
    sealed class UniqueClaims
    {
        public List<(string Key, string Owner)> Fresh { get; } = new();
    }
}
