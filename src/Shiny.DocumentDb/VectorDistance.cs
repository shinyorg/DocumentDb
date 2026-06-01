namespace Shiny.DocumentDb;

/// <summary>
/// Distance / similarity metric used for vector search. See per-provider documentation
/// for which metrics each backend supports.
/// </summary>
public enum VectorDistance
{
    /// <summary>Cosine distance. Always surfaced to the caller as distance in [0, 2].</summary>
    Cosine,

    /// <summary>Euclidean (L2) distance.</summary>
    Euclidean,

    /// <summary>Negative inner product — higher = closer. Surfaced as the raw inner product.</summary>
    DotProduct,

    /// <summary>Hamming distance — bit vectors only. Throws on providers that don't support it.</summary>
    Hamming
}
