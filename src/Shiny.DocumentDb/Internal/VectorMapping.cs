namespace Shiny.DocumentDb.Internal;

/// <summary>
/// Registration record produced by <c>MapVectorProperty&lt;T&gt;</c>. Public because
/// <see cref="IDatabaseProvider"/> SQL builders need to read its dimensions, metric, and
/// index settings — there is no behavior here for consumers to override.
/// </summary>
public class VectorMapping
{
    public required Type DocumentType { get; init; }
    public required string PropertyName { get; init; }
    public string JsonPath { get; set; } = null!;
    public required int Dimensions { get; init; }
    public required VectorDistance Metric { get; init; }

    /// <summary>
    /// The effective index kind. When the caller did not name one, this is filled in with the provider's own
    /// default (DiskANN on Cosmos, HNSW elsewhere) by <see cref="DocumentMappingRegistry.ResolveVectorIndexKinds"/>
    /// at store construction.
    /// </summary>
    public VectorIndexKind IndexKind { get; set; }

    /// <summary>The index kind the caller asked for, or null to take the provider's default.</summary>
    internal VectorIndexKind? RequestedIndexKind { get; init; }

    public required VectorIndexOptions IndexOptions { get; init; }
    public required Func<object, ReadOnlyMemory<float>> GetVector { get; init; }
    public required Action<object, ReadOnlyMemory<float>> SetVector { get; init; }
}
