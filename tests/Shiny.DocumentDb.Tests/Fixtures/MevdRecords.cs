using Microsoft.Extensions.AI;
using Microsoft.Extensions.VectorData;

namespace Shiny.DocumentDb.Tests.Fixtures;

// MEVD record types for the Microsoft.Extensions.VectorData connector suites.
//
// VectorRecordRegistry is process-wide (a record type's key/vector shape belongs to the type, not to one
// store), so each provider's suite gets its own record type — otherwise two suites racing to map the same
// type under different collection names would collide.
//
// Every vector property is named Embedding so it lands on the JSON path `embedding`, which is what the
// MongoDB Atlas fixture's search index is built over.

/// <summary>SQLite suite record. String key, so the collection is <c>VectorStoreCollection&lt;string, MevdNote&gt;</c>.</summary>
public class MevdNote
{
    [VectorStoreKey]
    public string Id { get; set; } = "";

    [VectorStoreData(IsIndexed = true)]
    public string Tag { get; set; } = "";

    [VectorStoreData]
    public string Text { get; set; } = "";

    [VectorStoreVector(4, DistanceFunction = DistanceFunction.CosineDistance)]
    public ReadOnlyMemory<float> Embedding { get; set; }
}

/// <summary>PostgreSQL/pgvector suite record.</summary>
public class MevdArticle
{
    [VectorStoreKey]
    public string Id { get; set; } = "";

    [VectorStoreData(IsIndexed = true)]
    public string Tag { get; set; } = "";

    [VectorStoreVector(4, DistanceFunction = DistanceFunction.CosineDistance, IndexKind = IndexKind.Hnsw)]
    public ReadOnlyMemory<float> Embedding { get; set; }
}

/// <summary>MongoDB Atlas suite record.</summary>
public class MevdMemo
{
    [VectorStoreKey]
    public string Id { get; set; } = "";

    [VectorStoreData(IsIndexed = true)]
    public string Tag { get; set; } = "";

    [VectorStoreVector(4, DistanceFunction = DistanceFunction.CosineSimilarity)]
    public ReadOnlyMemory<float> Embedding { get; set; }
}

/// <summary>Undecorated — the record used to prove a <see cref="VectorStoreCollectionDefinition"/> drives the mapping.</summary>
public class MevdPlainNote
{
    public string Id { get; set; } = "";
    public string Tag { get; set; } = "";
    public ReadOnlyMemory<float> Embedding { get; set; }
}

/// <summary>Two vector properties — DocumentDb maps one embedding per type, so this must be rejected.</summary>
public class MevdMultiVectorNote
{
    [VectorStoreKey]
    public string Id { get; set; } = "";

    [VectorStoreVector(4)]
    public ReadOnlyMemory<float> Title { get; set; }

    [VectorStoreVector(4)]
    public ReadOnlyMemory<float> Body { get; set; }
}

/// <summary>A distance function with no DocumentDb equivalent.</summary>
public class MevdManhattanNote
{
    [VectorStoreKey]
    public string Id { get; set; } = "";

    [VectorStoreVector(4, DistanceFunction = DistanceFunction.ManhattanDistance)]
    public ReadOnlyMemory<float> Embedding { get; set; }
}

/// <summary>A key type DocumentDb cannot store an id as.</summary>
public class MevdBadKeyNote
{
    [VectorStoreKey]
    public double Id { get; set; }

    [VectorStoreVector(4)]
    public ReadOnlyMemory<float> Embedding { get; set; }
}

/// <summary>Declares a MEVD StorageName, which DocumentDb takes from System.Text.Json instead.</summary>
public class MevdRenamedNote
{
    [VectorStoreKey]
    public string Id { get; set; } = "";

    [VectorStoreData(StorageName = "t")]
    public string Tag { get; set; } = "";

    [VectorStoreVector(4)]
    public ReadOnlyMemory<float> Embedding { get; set; }
}

/// <summary>The LiteDB negative-path record — never actually mapped, since the store rejects the connector first.</summary>
public class MevdLiteNote
{
    [VectorStoreKey]
    public string Id { get; set; } = "";

    [VectorStoreVector(4)]
    public ReadOnlyMemory<float> Embedding { get; set; }
}

/// <summary>
/// Deterministic stand-in for a real embedding model: the leading characters of the text become the vector,
/// so a test can predict which record a text query lands nearest to.
/// </summary>
public sealed class FakeEmbeddingGenerator(int dimensions) : IEmbeddingGenerator<string, Embedding<float>>
{
    public int Calls { get; private set; }

    public Task<GeneratedEmbeddings<Embedding<float>>> GenerateAsync(
        IEnumerable<string> values,
        EmbeddingGenerationOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        this.Calls++;
        var embeddings = values.Select(value =>
        {
            var vector = new float[dimensions];
            for (var i = 0; i < dimensions && i < value.Length; i++)
                vector[i] = value[i] == '1' ? 1f : 0f;
            return new Embedding<float>(vector);
        });
        return Task.FromResult(new GeneratedEmbeddings<Embedding<float>>(embeddings));
    }

    public object? GetService(Type serviceType, object? serviceKey = null)
        => serviceKey == null && serviceType.IsInstanceOfType(this) ? this : null;

    public void Dispose() { }
}

/// <summary>Shared seed for the MEVD suites, mirroring <see cref="VectorTestSeed"/>'s arithmetic.</summary>
public static class MevdSeed
{
    public static ReadOnlyMemory<float> V(float a, float b, float c, float d) => new[] { a, b, c, d };

    /// <summary>Nearest-first against <see cref="Query"/>: n1, n2, n3, n4, n5.</summary>
    public static IReadOnlyList<(string Id, string Tag, ReadOnlyMemory<float> Embedding)> Rows { get; } =
    [
        ("n1", "a", V(1.00f, 0.00f, 0.00f, 0.00f)),
        ("n2", "a", V(0.80f, 0.20f, 0.00f, 0.00f)),
        ("n3", "b", V(0.50f, 0.50f, 0.00f, 0.00f)),
        ("n4", "b", V(0.10f, 0.90f, 0.00f, 0.00f)),
        ("n5", "c", V(0.00f, 0.00f, 1.00f, 0.00f))
    ];

    public static ReadOnlyMemory<float> Query => V(1f, 0f, 0f, 0f);

    public static IEnumerable<MevdNote> Notes()
        => Rows.Select(r => new MevdNote { Id = r.Id, Tag = r.Tag, Text = $"note {r.Id}", Embedding = r.Embedding });

    public static IEnumerable<MevdArticle> Articles()
        => Rows.Select(r => new MevdArticle { Id = r.Id, Tag = r.Tag, Embedding = r.Embedding });

    public static IEnumerable<MevdMemo> Memos()
        => Rows.Select(r => new MevdMemo { Id = r.Id, Tag = r.Tag, Embedding = r.Embedding });
}
