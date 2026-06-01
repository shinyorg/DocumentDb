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
    public required VectorIndexKind IndexKind { get; init; }
    public required VectorIndexOptions IndexOptions { get; init; }
    public required Func<object, ReadOnlyMemory<float>> GetVector { get; init; }
    public required Action<object, ReadOnlyMemory<float>> SetVector { get; init; }
}
