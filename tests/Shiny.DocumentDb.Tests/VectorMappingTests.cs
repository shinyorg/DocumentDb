using Shiny.DocumentDb.Sqlite;
using Xunit;

namespace Shiny.DocumentDb.Tests;

/// <summary>
/// Unit-level coverage for vector mapping registration, validation, and capability surface.
/// Integration coverage (actual ANN search) requires the sqlite-vec native binary and lives
/// in a separately-gated suite.
/// </summary>
public class VectorMappingTests
{
    sealed class Doc
    {
        public string Id { get; set; } = "";
        public string Content { get; set; } = "";
        public ReadOnlyMemory<float> Embedding { get; set; }
    }

    [Fact]
    public void MapVectorProperty_StoresMappingWithSuppliedSettings()
    {
        var opts = new DocumentStoreOptions
        {
            DatabaseProvider = new SqliteDatabaseProvider("Data Source=:memory:")
        };

        opts.MapVectorProperty<Doc>(d => d.Embedding,
            dimensions: 1536,
            metric: VectorDistance.Cosine,
            indexKind: VectorIndexKind.Hnsw,
            configureIndex: i =>
            {
                i.HnswM = 24;
                i.HnswEfConstruction = 128;
                i.ProviderHints["sqlite.postFilterMultiplier"] = 8;
            });

        var mapping = opts.ResolveVectorMapping(typeof(Doc));
        Assert.NotNull(mapping);
        Assert.Equal(1536, mapping!.Dimensions);
        Assert.Equal(VectorDistance.Cosine, mapping.Metric);
        Assert.Equal(VectorIndexKind.Hnsw, mapping.IndexKind);
        Assert.Equal(24, mapping.IndexOptions.HnswM);
        Assert.Equal(128, mapping.IndexOptions.HnswEfConstruction);
        Assert.Equal(8, mapping.IndexOptions.ProviderHints["sqlite.postFilterMultiplier"]);
    }

    [Fact]
    public void MapVectorProperty_ZeroDimensions_Throws()
    {
        var opts = new DocumentStoreOptions
        {
            DatabaseProvider = new SqliteDatabaseProvider("Data Source=:memory:")
        };

        Assert.Throws<ArgumentOutOfRangeException>(() =>
            opts.MapVectorProperty<Doc>(d => d.Embedding, dimensions: 0));
    }

    [Fact]
    public void MapVectorProperty_NonPropertyAccess_Throws()
    {
        var opts = new DocumentStoreOptions
        {
            DatabaseProvider = new SqliteDatabaseProvider("Data Source=:memory:")
        };

        Assert.Throws<ArgumentException>(() =>
            opts.MapVectorProperty<Doc>(d => new ReadOnlyMemory<float>(new float[1]), dimensions: 1));
    }

    [Fact]
    public void MapVectorProperty_AotOverload_RoundTripsViaSetter()
    {
        var opts = new DocumentStoreOptions
        {
            DatabaseProvider = new SqliteDatabaseProvider("Data Source=:memory:")
        };

        opts.MapVectorProperty<Doc>(
            "Embedding",
            d => d.Embedding,
            (d, v) => d.Embedding = v,
            dimensions: 3,
            metric: VectorDistance.Euclidean);

        var mapping = opts.ResolveVectorMapping(typeof(Doc));
        Assert.NotNull(mapping);

        var doc = new Doc();
        var sample = new ReadOnlyMemory<float>(new[] { 1f, 2f, 3f });
        mapping!.SetVector(doc, sample);
        Assert.Equal(3, doc.Embedding.Length);
        Assert.Equal(1f, doc.Embedding.Span[0]);

        var got = mapping.GetVector(doc);
        Assert.Equal(3, got.Length);
    }

    [Fact]
    public async Task DocumentStore_WithoutVectorMapping_NearestVectors_Throws()
    {
        var opts = new DocumentStoreOptions
        {
            DatabaseProvider = new SqliteDatabaseProvider("Data Source=:memory:")
            {
                EnableVectorExtension = false
            }
        };
        using var store = new DocumentStore(opts);

        Assert.False(store.SupportsVector);
        await Assert.ThrowsAsync<NotSupportedException>(() =>
            store.NearestVectors<Doc>(new ReadOnlyMemory<float>(new float[3]), 5));
    }

    [Fact]
    public async Task DocumentStore_WrongDimensionQueryVector_Throws_AtMappedType()
    {
        var opts = new DocumentStoreOptions
        {
            DatabaseProvider = new SqliteDatabaseProvider("Data Source=:memory:")
            {
                EnableVectorExtension = true
            }
        };
        opts.MapVectorProperty<Doc>(d => d.Embedding, dimensions: 4);

        using var store = new DocumentStore(opts);
        // Wrong dimension surfaces synchronously as an ArgumentException — no DB round-trip needed.
        await Assert.ThrowsAsync<ArgumentException>(() =>
            store.NearestVectors<Doc>(new ReadOnlyMemory<float>(new float[3]), 5));
    }

    [Fact]
    public void VectorIndexOptions_DefaultsAreNull()
    {
        var o = new VectorIndexOptions();
        Assert.Null(o.HnswM);
        Assert.Null(o.HnswEfConstruction);
        Assert.Null(o.HnswEfSearch);
        Assert.Null(o.IvfLists);
        Assert.Empty(o.ProviderHints);
    }

    [Fact]
    public async Task Insert_VectorWithWrongDimension_Throws()
    {
        var opts = new DocumentStoreOptions
        {
            DatabaseProvider = new SqliteDatabaseProvider("Data Source=:memory:")
            {
                // SupportsVector must be true for the upsert path to be exercised. The
                // extension load step itself is not required: dimension validation runs
                // before the sidecar SQL is issued.
                EnableVectorExtension = true
            }
        };
        opts.MapVectorProperty<Doc>(d => d.Embedding, dimensions: 8);
        using var store = new DocumentStore(opts);

        var bad = new Doc { Id = "x", Embedding = new ReadOnlyMemory<float>(new float[3]) };
        await Assert.ThrowsAsync<ArgumentException>(() => store.Insert(bad));
    }

    [Fact]
    public void MapVectorProperty_AotOverload_StoresEnumValues()
    {
        var opts = new DocumentStoreOptions
        {
            DatabaseProvider = new SqliteDatabaseProvider("Data Source=:memory:")
        };

        opts.MapVectorProperty<Doc>(
            "Embedding",
            d => d.Embedding,
            (d, v) => d.Embedding = v,
            dimensions: 768,
            metric: VectorDistance.DotProduct,
            indexKind: VectorIndexKind.Ivf,
            configureIndex: i => i.IvfLists = 200);

        var mapping = opts.ResolveVectorMapping(typeof(Doc));
        Assert.NotNull(mapping);
        Assert.Equal(768, mapping!.Dimensions);
        Assert.Equal(VectorDistance.DotProduct, mapping.Metric);
        Assert.Equal(VectorIndexKind.Ivf, mapping.IndexKind);
        Assert.Equal(200, mapping.IndexOptions.IvfLists);
    }

    [Fact]
    public async Task Insert_DefaultEmbedding_StoresDocumentWithoutVectorWrite()
    {
        // Vector field at default (zero-length) is skipped on write — the document still
        // persists, just without an embedding entry. This is the "vector not generated yet"
        // path that the auto-embed hook would normally fill in.
        var opts = new DocumentStoreOptions
        {
            DatabaseProvider = new SqliteDatabaseProvider("Data Source=:memory:")
            {
                EnableVectorExtension = false   // extension not loaded — proves we don't touch it
            }
        };
        opts.MapVectorProperty<Doc>(d => d.Embedding, dimensions: 4);

        using var store = new DocumentStore(opts);
        await store.Insert(new Doc { Id = "x", Content = "no embedding" });

        var got = await store.Get<Doc>("x");
        Assert.NotNull(got);
        Assert.Equal(0, got!.Embedding.Length);
    }
}
