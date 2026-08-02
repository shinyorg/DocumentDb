using ShinyDocDbMyAdmin.Services;

namespace ShinyDocDbMyAdmin.Models;

/// <summary>A JSON path that holds embedding-shaped values, as found by sampling document bodies.</summary>
/// <param name="Path">Dotted path into the document body.</param>
/// <param name="Dimensions">The most common length seen. See <paramref name="Ragged"/>.</param>
/// <param name="Ragged">True when the sample held more than one length - a real problem, so it is surfaced.</param>
public sealed record VectorFieldInfo(
    string Path,
    int Dimensions,
    bool Ragged,
    int Occurrences,
    int SampleSize
)
{
    public int PercentPresent => this.SampleSize == 0 ? 0 : (int)Math.Round(this.Occurrences * 100.0 / this.SampleSize);
}

/// <summary>What the tool could establish about a type's <c>{table}_vec_{type}</c> sidecar.</summary>
/// <param name="Unavailable">
/// Why the sidecar could not be read or written, or null when it can be. The known case is SQLite:
/// the <c>vec0</c> virtual table needs the sqlite-vec extension, which this tool does not load.
/// </param>
public sealed record VectorSidecarInfo(
    string TableName,
    bool ProviderSupportsVectors,
    bool Present,
    long? RowCount,
    string? Unavailable
)
{
    /// <summary>True when the tool can keep the sidecar in step with document writes.</summary>
    public bool Maintainable => this.Present && this.Unavailable is null;
}

/// <summary>
/// One embedding found in a document body, reduced to its statistics. The components themselves are
/// deliberately not kept: a page showing a thousand documents would otherwise push a thousand
/// 1536-element arrays over the circuit to render a table of numbers.
/// </summary>
public sealed record VectorEntry(
    string DocumentId,
    int Dimensions,
    double Norm,
    float Min,
    float Max,
    double Mean,
    bool HasNonFinite,
    IReadOnlyList<float> Preview
)
{
    /// <summary>A vector of all zeros - almost always a document whose embedding was never generated.</summary>
    public bool IsZero => this.Norm == 0;

    public bool IsNormalised => VectorMath.IsNormalised(this.Norm);

    public static VectorEntry Describe(string documentId, float[] values, int previewLength = 8)
    {
        float min = float.PositiveInfinity, max = float.NegativeInfinity;
        double sum = 0;
        var nonFinite = false;

        foreach (var v in values)
        {
            if (float.IsFinite(v))
            {
                min = Math.Min(min, v);
                max = Math.Max(max, v);
                sum += v;
            }
            else
            {
                nonFinite = true;
            }
        }

        return new VectorEntry(
            documentId,
            values.Length,
            VectorMath.Norm(values),
            float.IsFinite(min) ? min : 0,
            float.IsFinite(max) ? max : 0,
            values.Length == 0 ? 0 : sum / values.Length,
            nonFinite,
            [.. values.Take(previewLength)]);
    }
}

/// <summary>The embeddings gathered for one view of one path, with what the scan cost.</summary>
/// <param name="ScannedIds">
/// Every document id the scan saw, including those with no vector. Kept so the sidecar can be
/// reconciled against it without paying for a second pass over the type.
/// </param>
public sealed record VectorSet(
    string Path,
    IReadOnlyList<VectorEntry> Entries,
    int DocumentsScanned,
    int DocumentsWithoutVector,
    bool Truncated,
    IReadOnlySet<string> ScannedIds
)
{
    public bool IsEmpty => this.Entries.Count == 0;

    /// <summary>Distinct dimensions present, commonest first. More than one entry here is a fault.</summary>
    public IReadOnlyList<(int Dimensions, int Count)> DimensionGroups =>
        [.. this.Entries
            .GroupBy(e => e.Dimensions)
            .Select(g => (Dimensions: g.Key, Count: g.Count()))
            .OrderByDescending(x => x.Count)];

    public int ZeroCount => this.Entries.Count(e => e.IsZero);
    public int NonFiniteCount => this.Entries.Count(e => e.HasNonFinite);
    public int NormalisedCount => this.Entries.Count(e => e.IsNormalised);
    public double AverageNorm => this.Entries.Count == 0 ? 0 : this.Entries.Average(e => e.Norm);
}

/// <summary>
/// How the sidecar and the document bodies compare. The bodies are the source of truth: the sidecar is
/// an index built from them, so anything here that is not zero means a search will return wrong answers.
/// </summary>
public sealed record VectorHealth(
    VectorSidecarInfo Sidecar,
    string Path,
    int DocumentsWithVector,
    int DocumentsScanned,
    bool Truncated,
    IReadOnlyList<string> MissingFromSidecar,
    IReadOnlyList<string> OrphanedInSidecar,
    IReadOnlyList<int> DimensionsPresent
)
{
    public bool Ragged => this.DimensionsPresent.Count > 1;

    public bool IsHealthy =>
        this.MissingFromSidecar.Count == 0 && this.OrphanedInSidecar.Count == 0 && !this.Ragged;

    /// <summary>
    /// True when the tool could not compare at all, so "healthy" would be a claim it cannot make.
    /// </summary>
    public bool Unknown => this.Sidecar.Present && this.Sidecar.RowCount is null;
}

/// <summary>One result of a similarity search, scored so that smaller is nearer.</summary>
public sealed record VectorNeighbour(
    string DocumentId,
    double Score,
    double Norm,
    int Dimensions,
    string Summary
);

/// <summary>What the tool could establish about a type's full-text index.</summary>
/// <param name="ProviderSupportsFullText">False on a backend with no full-text at all.</param>
/// <param name="Present">True when this type has been indexed.</param>
/// <param name="RequiresRebuild">
/// True when the index is a snapshot rather than engine-maintained (DuckDB), so writes leave it stale
/// until something rebuilds it — the only case where full-text behaves like the vector sidecar.
/// </param>
/// <param name="Unavailable">Why the check could not be made, or null when it could.</param>
public sealed record FullTextIndexInfo(
    bool ProviderSupportsFullText,
    bool Present,
    bool RequiresRebuild,
    string? Unavailable
);

/// <summary>One ranked full-text match.</summary>
public sealed record FullTextHit(string DocumentId, double Score, string Json);

/// <summary>The matches for one search, with the SQL that produced them.</summary>
public sealed record FullTextResults(IReadOnlyList<FullTextHit> Hits, TimeSpan Elapsed, string Sql);

/// <summary>What a sidecar write actually did, so the caller can report it rather than assume it.</summary>
public enum VectorSyncOutcome
{
    /// <summary>The type has no vector sidecar; nothing to do.</summary>
    NotApplicable,

    /// <summary>The sidecar was updated.</summary>
    Synced,

    /// <summary>
    /// The sidecar exists but this tool cannot write it, so it is now stale. Reported, never swallowed.
    /// </summary>
    Unavailable,

    /// <summary>
    /// The document body held no single unambiguous embedding, so the sidecar was left alone rather
    /// than guessed at.
    /// </summary>
    Ambiguous
}
