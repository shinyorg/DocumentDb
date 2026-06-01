namespace Shiny.DocumentDb.Tests.Fixtures;

/// <summary>
/// Shared document type used by every provider's vector integration tests.
/// The embedding is 4-dim so we can hand-construct vectors and reason about
/// which one should be "nearest" without floating-point ambiguity.
/// </summary>
public class VectorDoc
{
    public string Id { get; set; } = "";
    public string Tag { get; set; } = "";
    public ReadOnlyMemory<float> Embedding { get; set; }

    public static ReadOnlyMemory<float> V(float a, float b, float c, float d) => new[] { a, b, c, d };
}
