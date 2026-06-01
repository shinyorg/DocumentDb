namespace Shiny.DocumentDb;

/// <summary>
/// A document returned from a vector search with its computed score.
/// Score semantics depend on the configured <see cref="VectorDistance"/>:
/// for <see cref="VectorDistance.Cosine"/> and <see cref="VectorDistance.Euclidean"/>
/// it is a distance (lower = closer); for <see cref="VectorDistance.DotProduct"/>
/// it is the raw inner product (higher = closer); for <see cref="VectorDistance.Hamming"/>
/// it is the bit-difference count.
/// </summary>
public class VectorResult<T> where T : class
{
    public required T Document { get; init; }
    public float Score { get; init; }
}
