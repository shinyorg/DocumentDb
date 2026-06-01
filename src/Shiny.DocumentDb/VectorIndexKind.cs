namespace Shiny.DocumentDb;

/// <summary>
/// Index strategy to use when provisioning the vector column on the underlying provider.
/// Providers that don't recognize the chosen kind fall back to <see cref="None"/> (flat scan).
/// </summary>
public enum VectorIndexKind
{
    /// <summary>No index — flat scan over every row.</summary>
    None,

    /// <summary>Explicit flat index where the provider distinguishes (e.g. Cosmos).</summary>
    Flat,

    /// <summary>Hierarchical Navigable Small Worlds — preferred default for ANN.</summary>
    Hnsw,

    /// <summary>IVF flat (e.g. pgvector ivfflat).</summary>
    Ivf,

    /// <summary>DiskANN — SQL Server 2025, CosmosDB.</summary>
    DiskAnn,

    /// <summary>Quantized flat — CosmosDB.</summary>
    QuantizedFlat
}
