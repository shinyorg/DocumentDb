namespace Shiny.DocumentDb;

/// <summary>
/// Strongly-typed knobs for the common ANN index parameters plus an escape hatch
/// (<see cref="ProviderHints"/>) for the long tail of provider-specific settings.
/// Unknown values in <see cref="ProviderHints"/> are silently ignored per provider.
/// </summary>
public class VectorIndexOptions
{
    /// <summary>HNSW parameter M — graph node degree. pgvector default 16, DuckDB default 16.</summary>
    public int? HnswM { get; set; }

    /// <summary>HNSW efConstruction — build-time accuracy/speed tradeoff. pgvector default 64.</summary>
    public int? HnswEfConstruction { get; set; }

    /// <summary>HNSW efSearch — query-time accuracy/speed tradeoff. pgvector default 40.</summary>
    public int? HnswEfSearch { get; set; }

    /// <summary>IVF list count — pgvector ivfflat.</summary>
    public int? IvfLists { get; set; }

    /// <summary>
    /// Provider-specific hints not covered by the strongly-typed properties.
    /// Unknown keys are silently ignored per provider.
    /// </summary>
    public Dictionary<string, object> ProviderHints { get; } = new();
}
