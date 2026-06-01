namespace Shiny.DocumentDb.Tests.Fixtures;

/// <summary>
/// Shared seed + query-vector data so every provider's vector test exercises the same
/// arithmetic. Vectors are 4-dim unit-ish so cosine/L2/dot all give a clear winner.
/// </summary>
public static class VectorTestSeed
{
    // Five well-separated unit-ish vectors. With queryVector = (1, 0, 0, 0) the
    // expected ordering nearest-first (cosine distance) is: u1, u2, u3, u4, u5.
    public static IReadOnlyList<VectorDoc> Docs { get; } = new[]
    {
        new VectorDoc { Id = "u1", Tag = "a", Embedding = VectorDoc.V(1.00f, 0.00f, 0.00f, 0.00f) },
        new VectorDoc { Id = "u2", Tag = "a", Embedding = VectorDoc.V(0.80f, 0.20f, 0.00f, 0.00f) },
        new VectorDoc { Id = "u3", Tag = "b", Embedding = VectorDoc.V(0.50f, 0.50f, 0.00f, 0.00f) },
        new VectorDoc { Id = "u4", Tag = "b", Embedding = VectorDoc.V(0.10f, 0.90f, 0.00f, 0.00f) },
        new VectorDoc { Id = "u5", Tag = "c", Embedding = VectorDoc.V(0.00f, 0.00f, 1.00f, 0.00f) },
    };

    public static ReadOnlyMemory<float> Query => VectorDoc.V(1f, 0f, 0f, 0f);
}
