using System.Text.Json.Serialization.Metadata;

namespace Shiny.DocumentDb.Internal;

/// <summary>
/// A single persisted history version, in a backend-agnostic shape. The document stores
/// (LiteDB / MongoDB / CosmosDB / IndexedDB) read their sidecar rows into this and let
/// <see cref="TemporalHistory"/> apply the shared selection and retention logic, so every provider
/// matches the relational <see cref="DocumentStore"/> semantics exactly.
/// </summary>
sealed class HistoryEntry
{
    public required string Id { get; init; }
    public required string TypeName { get; init; }
    public required long Version { get; init; }
    public required DateTimeOffset ValidFrom { get; init; }
    public DateTimeOffset? ValidTo { get; set; }
    public required string Operation { get; init; }
    public string? Actor { get; init; }

    /// <summary>The serialized document body, or null for a <see cref="TemporalOperation.Removed"/> tombstone.</summary>
    public string? Data { get; init; }
}

/// <summary>
/// Pure (storage-free) temporal logic shared by the non-relational document stores. Mirrors the SQL
/// the relational providers build in <c>IDatabaseProvider.BuildHistory*Sql</c>.
/// </summary>
static class TemporalHistory
{
    /// <summary>The next version number for a document, given its current rows. Versions are monotonic, starting at 1.</summary>
    public static long NextVersion(IEnumerable<HistoryEntry> docRows)
        => docRows.Select(r => r.Version).DefaultIfEmpty(0L).Max() + 1;

    /// <summary>
    /// The version of a single document in effect at <paramref name="asOf"/>: validity started at or
    /// before the instant and had not yet ended. Returns null if the document did not exist then.
    /// </summary>
    public static HistoryEntry? AsOf(IEnumerable<HistoryEntry> docRows, DateTimeOffset asOf)
        => docRows
            .Where(r => r.ValidFrom <= asOf && (r.ValidTo == null || r.ValidTo > asOf))
            .OrderByDescending(r => r.Version)
            .FirstOrDefault();

    /// <summary>
    /// The live (non-tombstone) rows in effect at <paramref name="asOf"/> across a type's history — the
    /// fleet-wide point-in-time snapshot. Exactly one row per document is ever in effect at an instant.
    /// </summary>
    public static IEnumerable<HistoryEntry> AsOfAll(IEnumerable<HistoryEntry> typeRows, DateTimeOffset asOf)
        => typeRows.Where(r =>
            r.ValidFrom <= asOf &&
            (r.ValidTo == null || r.ValidTo > asOf) &&
            r.Data != null);

    /// <summary>All versions of one document, oldest first.</summary>
    public static IEnumerable<HistoryEntry> History(IEnumerable<HistoryEntry> docRows)
        => docRows.OrderBy(r => r.Version);

    /// <summary>Every version authored by <paramref name="actor"/>, oldest first.</summary>
    public static IEnumerable<HistoryEntry> ByActor(IEnumerable<HistoryEntry> typeRows, string actor)
        => typeRows
            .Where(r => r.Actor == actor)
            .OrderBy(r => r.ValidFrom).ThenBy(r => r.Id).ThenBy(r => r.Version);

    /// <summary>Every version whose <see cref="HistoryEntry.ValidFrom"/> falls in <c>[from, to)</c>, oldest first.</summary>
    public static IEnumerable<HistoryEntry> Between(IEnumerable<HistoryEntry> typeRows, DateTimeOffset from, DateTimeOffset to)
        => typeRows
            .Where(r => r.ValidFrom >= from && r.ValidFrom < to)
            .OrderBy(r => r.ValidFrom).ThenBy(r => r.Id).ThenBy(r => r.Version);

    /// <summary>Closed versions whose validity ended before <paramref name="cutoff"/> (age-based retention).</summary>
    public static IEnumerable<HistoryEntry> PrunableByAge(IEnumerable<HistoryEntry> docRows, DateTimeOffset cutoff)
        => docRows.Where(r => r.ValidTo != null && r.ValidTo < cutoff);

    /// <summary>
    /// Versions to drop so that only the newest <paramref name="keep"/> survive (count-based retention).
    /// Versions are monotonic, so the cutoff is <c>MAX(Version) - keep</c>.
    /// </summary>
    public static IEnumerable<HistoryEntry> PrunableByCount(IEnumerable<HistoryEntry> docRows, int keep)
    {
        var rows = docRows as ICollection<HistoryEntry> ?? docRows.ToList();
        if (rows.Count == 0)
            return Array.Empty<HistoryEntry>();
        var cutoff = rows.Max(r => r.Version) - keep;
        return rows.Where(r => r.Version <= cutoff);
    }

    /// <summary>Materializes a <see cref="HistoryEntry"/> into the public <see cref="DocumentVersion{T}"/>.</summary>
    public static DocumentVersion<T> ToVersion<T>(HistoryEntry e, Func<string, T?> deserialize) where T : class
        => new()
        {
            Id = e.Id,
            Version = e.Version,
            ValidFrom = e.ValidFrom,
            ValidTo = e.ValidTo,
            Operation = Enum.Parse<TemporalOperation>(e.Operation),
            Actor = e.Actor,
            Document = e.Data == null ? null : deserialize(e.Data)
        };
}
