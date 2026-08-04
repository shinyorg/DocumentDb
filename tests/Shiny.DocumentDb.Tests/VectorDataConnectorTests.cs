using Microsoft.Extensions.AI;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.VectorData;
using Shiny.DocumentDb.Extensions.VectorData;
using Shiny.DocumentDb.Tests.Fixtures;
using Xunit;

namespace Shiny.DocumentDb.Tests;

/// <summary>
/// The Microsoft.Extensions.VectorData connector over SQLite + sqlite-vec — the leg that needs no Docker, so it
/// carries the bulk of the surface. The provider-specific legs (pgvector pre-filtering, Atlas score direction)
/// live in <see cref="PostgreSqlVectorDataConnectorTests"/> and <see cref="MongoDbVectorDataConnectorTests"/>.
/// </summary>
[Collection("SQLite")]
public class VectorDataConnectorTests : IDisposable
{
    readonly SqliteDatabaseFixture fx;
    readonly string dbPath;
    readonly bool vecAvailable;

    public VectorDataConnectorTests(SqliteDatabaseFixture fx)
    {
        this.fx = fx;
        this.vecAvailable = SqliteDatabaseFixture.FindVec0BinaryPath() != null;
        this.dbPath = Path.Combine(Path.GetTempPath(), $"mevd_{Guid.NewGuid():N}.db");
    }

    public void Dispose()
    {
        if (File.Exists(this.dbPath)) File.Delete(this.dbPath);
    }

    bool Skipped => !this.vecAvailable;

    IDocumentStore CreateStore() => this.fx.CreateVectorStore(this.dbPath, o => o.MapVectorRecord<MevdNote>());

    async Task<VectorStoreCollection<string, MevdNote>> SeedAsync()
    {
        var store = this.CreateStore();
        var collection = new DocumentDbVectorStore(store).GetCollection<string, MevdNote>(nameof(MevdNote));
        await collection.UpsertAsync(MevdSeed.Notes());
        return collection;
    }

    // ── Wiring ──────────────────────────────────────────────────────────

    [Fact]
    public void Constructor_Throws_WhenProviderHasNoVectorSupport()
    {
        var store = new LiteDbDatabaseFixture().CreateStore($"mevd_lite_{Guid.NewGuid():N}");
        var ex = Assert.Throws<NotSupportedException>(() => new DocumentDbVectorStore(store));
        Assert.Contains("does not support vector search", ex.Message);
    }

    [Fact]
    public void GetCollection_Throws_WhenRecordWasNeverMapped()
    {
        if (this.Skipped) { Assert.Skip("sqlite-vec native binary not present next to test assembly."); return; }

        var store = this.CreateStore();
        var vectorStore = new DocumentDbVectorStore(store);

        // MevdLiteNote is deliberately never passed to MapVectorRecord anywhere in the suite.
        var ex = Assert.Throws<InvalidOperationException>(
            () => vectorStore.GetCollection<string, MevdLiteNote>(nameof(MevdLiteNote)));
        Assert.Contains("MapVectorRecord", ex.Message);
    }

    [Fact]
    public void GetCollection_Throws_WhenKeyTypeDoesNotMatchTheRecord()
    {
        if (this.Skipped) { Assert.Skip("sqlite-vec native binary not present next to test assembly."); return; }

        var vectorStore = new DocumentDbVectorStore(this.CreateStore());
        var ex = Assert.Throws<ArgumentException>(() => vectorStore.GetCollection<Guid, MevdNote>(nameof(MevdNote)));
        Assert.Contains("must be String", ex.Message);
    }

    [Fact]
    public void GetDynamicCollection_Throws()
    {
        if (this.Skipped) { Assert.Skip("sqlite-vec native binary not present next to test assembly."); return; }

        var vectorStore = new DocumentDbVectorStore(this.CreateStore());
        Assert.Throws<NotSupportedException>(
            () => vectorStore.GetDynamicCollection("anything", new VectorStoreCollectionDefinition()));
    }

    [Fact]
    public async Task Store_ListsAndResolvesMappedCollections()
    {
        if (this.Skipped) { Assert.Skip("sqlite-vec native binary not present next to test assembly."); return; }

        var vectorStore = new DocumentDbVectorStore(this.CreateStore());

        var names = new List<string>();
        await foreach (var name in vectorStore.ListCollectionNamesAsync())
            names.Add(name);

        Assert.Contains(nameof(MevdNote), names);
        Assert.True(await vectorStore.CollectionExistsAsync(nameof(MevdNote)));
        Assert.False(await vectorStore.CollectionExistsAsync("not-a-collection"));
    }

    [Fact]
    public async Task GetService_ExposesTheUnderlyingStoreAndMetadata()
    {
        if (this.Skipped) { Assert.Skip("sqlite-vec native binary not present next to test assembly."); return; }

        var store = this.CreateStore();
        var vectorStore = new DocumentDbVectorStore(store);
        var collection = vectorStore.GetCollection<string, MevdNote>(nameof(MevdNote));

        Assert.Same(store, vectorStore.GetService(typeof(IDocumentStore)));
        Assert.Same(store, collection.GetService(typeof(IDocumentStore)));
        Assert.Equal(DocumentDbVectorStore.SystemName, ((VectorStoreMetadata)vectorStore.GetService(typeof(VectorStoreMetadata))!).VectorStoreSystemName);
        Assert.Equal(nameof(MevdNote), ((VectorStoreCollectionMetadata)collection.GetService(typeof(VectorStoreCollectionMetadata))!).CollectionName);
        Assert.Null(vectorStore.GetService(typeof(IDocumentStore), "keyed"));

        await Task.CompletedTask;
    }

    // ── CRUD ────────────────────────────────────────────────────────────

    [Fact]
    public async Task Upsert_Get_Delete_RoundTrips()
    {
        if (this.Skipped) { Assert.Skip("sqlite-vec native binary not present next to test assembly."); return; }

        var collection = await this.SeedAsync();

        var fetched = await collection.GetAsync("n1", new RecordRetrievalOptions { IncludeVectors = true });
        Assert.NotNull(fetched);
        Assert.Equal("a", fetched!.Tag);
        Assert.Equal(4, fetched.Embedding.Length);

        // Upsert replaces rather than merge-patches: the cleared Tag must not resurrect the stored value.
        await collection.UpsertAsync(new MevdNote { Id = "n1", Tag = "", Text = "replaced", Embedding = MevdSeed.Query });
        var replaced = await collection.GetAsync("n1");
        Assert.Equal("", replaced!.Tag);
        Assert.Equal("replaced", replaced.Text);

        await collection.DeleteAsync("n1");
        Assert.Null(await collection.GetAsync("n1"));

        // Deleting what is not there succeeds, per the MEVD contract.
        await collection.DeleteAsync("n1");
    }

    [Fact]
    public async Task Get_OmitsVectorsUnlessAsked()
    {
        if (this.Skipped) { Assert.Skip("sqlite-vec native binary not present next to test assembly."); return; }

        var collection = await this.SeedAsync();

        var without = await collection.GetAsync("n1");
        Assert.Equal(0, without!.Embedding.Length);

        var with = await collection.GetAsync("n1", new RecordRetrievalOptions { IncludeVectors = true });
        Assert.Equal(4, with!.Embedding.Length);
    }

    [Fact]
    public async Task GetByKeys_ReturnsTheSubsetThatExists()
    {
        if (this.Skipped) { Assert.Skip("sqlite-vec native binary not present next to test assembly."); return; }

        var collection = await this.SeedAsync();

        var ids = new List<string>();
        await foreach (var note in collection.GetAsync(["n1", "nope", "n3"]))
            ids.Add(note.Id);

        Assert.Equal(["n1", "n3"], ids);
    }

    [Fact]
    public async Task DeleteByKeys_RemovesTheBatch()
    {
        if (this.Skipped) { Assert.Skip("sqlite-vec native binary not present next to test assembly."); return; }

        var collection = await this.SeedAsync();
        await collection.DeleteAsync(["n1", "n2"]);

        Assert.Null(await collection.GetAsync("n1"));
        Assert.Null(await collection.GetAsync("n2"));
        Assert.NotNull(await collection.GetAsync("n3"));
    }

    [Fact]
    public async Task GetByFilter_AppliesTopSkipAndOrdering()
    {
        if (this.Skipped) { Assert.Skip("sqlite-vec native binary not present next to test assembly."); return; }

        var collection = await this.SeedAsync();

        var descending = new List<string>();
        await foreach (var note in collection.GetAsync(n => n.Tag == "a" || n.Tag == "b", top: 10,
            new FilteredRecordRetrievalOptions<MevdNote> { OrderBy = o => o.Descending(n => n.Id) }))
        {
            descending.Add(note.Id);
        }
        Assert.Equal(["n4", "n3", "n2", "n1"], descending);

        var page = new List<string>();
        await foreach (var note in collection.GetAsync(n => n.Tag == "a" || n.Tag == "b", top: 2,
            new FilteredRecordRetrievalOptions<MevdNote> { OrderBy = o => o.Ascending(n => n.Id), Skip = 1 }))
        {
            page.Add(note.Id);
        }
        Assert.Equal(["n2", "n3"], page);
    }

    [Fact]
    public async Task EnsureCollectionDeleted_ClearsTheDocuments()
    {
        if (this.Skipped) { Assert.Skip("sqlite-vec native binary not present next to test assembly."); return; }

        var collection = await this.SeedAsync();
        await collection.EnsureCollectionExistsAsync();
        Assert.True(await collection.CollectionExistsAsync());

        await collection.EnsureCollectionDeletedAsync();
        Assert.Null(await collection.GetAsync("n1"));
    }

    // ── Search ──────────────────────────────────────────────────────────

    [Fact]
    public async Task Search_ReturnsNearestFirst()
    {
        if (this.Skipped) { Assert.Skip("sqlite-vec native binary not present next to test assembly."); return; }

        var collection = await this.SeedAsync();

        var hits = await collection.SearchAsync(MevdSeed.Query, top: 3).ToListAsync();

        Assert.Equal(["n1", "n2", "n3"], hits.Select(h => h.Record.Id));
        Assert.All(hits, h => Assert.NotNull(h.Score));
    }

    [Fact]
    public async Task Search_PassesTheFilterThroughToTheAnnQuery()
    {
        if (this.Skipped) { Assert.Skip("sqlite-vec native binary not present next to test assembly."); return; }

        var collection = await this.SeedAsync();

        // n1 and n2 are the vector-nearest, and both are excluded by the filter — proving the predicate reaches
        // the search rather than being applied to an already-truncated top-k.
        var hits = await collection
            .SearchAsync(MevdSeed.Query, top: 5, new VectorSearchOptions<MevdNote> { Filter = n => n.Tag == "b" })
            .ToListAsync();

        Assert.Equal(["n3", "n4"], hits.Select(h => h.Record.Id));
    }

    [Fact]
    public async Task Search_HonoursSkip()
    {
        if (this.Skipped) { Assert.Skip("sqlite-vec native binary not present next to test assembly."); return; }

        var collection = await this.SeedAsync();

        var hits = await collection
            .SearchAsync(MevdSeed.Query, top: 2, new VectorSearchOptions<MevdNote> { Skip = 1 })
            .ToListAsync();

        Assert.Equal(["n2", "n3"], hits.Select(h => h.Record.Id));
    }

    [Fact]
    public async Task Search_HonoursScoreThresholdAsAFloor()
    {
        if (this.Skipped) { Assert.Skip("sqlite-vec native binary not present next to test assembly."); return; }

        var collection = await this.SeedAsync();

        var all = await collection.SearchAsync(MevdSeed.Query, top: 5).ToListAsync();
        var cutoff = all[2].Score!.Value;

        var kept = await collection
            .SearchAsync(MevdSeed.Query, top: 5, new VectorSearchOptions<MevdNote> { ScoreThreshold = cutoff })
            .ToListAsync();

        // MEVD defines the threshold as a floor. SQLite reports cosine *distance*, so the floor keeps the
        // farthest matches — the documented consequence of not normalizing scores across backends.
        Assert.Equal(all.Where(h => h.Score >= cutoff).Select(h => h.Record.Id), kept.Select(h => h.Record.Id));
        Assert.DoesNotContain("n1", kept.Select(h => h.Record.Id));
    }

    [Fact]
    public async Task Search_OmitsVectorsUnlessAsked()
    {
        if (this.Skipped) { Assert.Skip("sqlite-vec native binary not present next to test assembly."); return; }

        var collection = await this.SeedAsync();

        var stripped = await collection.SearchAsync(MevdSeed.Query, top: 1).ToListAsync();
        Assert.Equal(0, stripped[0].Record.Embedding.Length);

        var included = await collection
            .SearchAsync(MevdSeed.Query, top: 1, new VectorSearchOptions<MevdNote> { IncludeVectors = true })
            .ToListAsync();
        Assert.Equal(4, included[0].Record.Embedding.Length);
    }

    [Fact]
    public async Task Search_AcceptsFloatArrayAndEmbedding()
    {
        if (this.Skipped) { Assert.Skip("sqlite-vec native binary not present next to test assembly."); return; }

        var collection = await this.SeedAsync();

        var fromArray = await collection.SearchAsync(new[] { 1f, 0f, 0f, 0f }, top: 1).ToListAsync();
        Assert.Equal("n1", fromArray[0].Record.Id);

        var fromEmbedding = await collection
            .SearchAsync(new Embedding<float>(MevdSeed.Query), top: 1)
            .ToListAsync();
        Assert.Equal("n1", fromEmbedding[0].Record.Id);
    }

    [Fact]
    public async Task Search_EmbedsAStringQuery_WhenAGeneratorIsAvailable()
    {
        if (this.Skipped) { Assert.Skip("sqlite-vec native binary not present next to test assembly."); return; }

        var store = this.fx.CreateVectorStore(this.dbPath, o => o.MapVectorRecord<MevdNote>());
        var generator = new FakeEmbeddingGenerator(4);
        var collection = new DocumentDbVectorStore(store, generator).GetCollection<string, MevdNote>(nameof(MevdNote));
        await collection.UpsertAsync(MevdSeed.Notes());

        // "1000" embeds to (1, 0, 0, 0) — the same query vector the other search tests use.
        var hits = await collection.SearchAsync("1000", top: 1).ToListAsync();

        Assert.Equal("n1", hits[0].Record.Id);
        Assert.Equal(1, generator.Calls);
    }

    [Fact]
    public async Task Search_Throws_WhenAStringQueryHasNoGenerator()
    {
        if (this.Skipped) { Assert.Skip("sqlite-vec native binary not present next to test assembly."); return; }

        var collection = await this.SeedAsync();

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(
            () => collection.SearchAsync("some text", top: 1).ToListAsync());
        Assert.Contains("IEmbeddingGenerator", ex.Message);
    }

    [Fact]
    public async Task Search_Throws_OnADimensionMismatch()
    {
        if (this.Skipped) { Assert.Skip("sqlite-vec native binary not present next to test assembly."); return; }

        var collection = await this.SeedAsync();

        await Assert.ThrowsAsync<ArgumentException>(
            () => collection.SearchAsync(new[] { 1f, 0f }, top: 1).ToListAsync());
    }

    [Fact]
    public async Task Search_Throws_WhenVectorPropertySelectsAnotherProperty()
    {
        if (this.Skipped) { Assert.Skip("sqlite-vec native binary not present next to test assembly."); return; }

        var collection = await this.SeedAsync();

        var ex = await Assert.ThrowsAsync<NotSupportedException>(() => collection
            .SearchAsync(MevdSeed.Query, top: 1, new VectorSearchOptions<MevdNote> { VectorProperty = n => n.Tag })
            .ToListAsync());
        Assert.Contains("one embedding per document type", ex.Message);
    }

    // ── Record model ────────────────────────────────────────────────────

    [Fact]
    public async Task Definition_DrivesTheMapping_ForAnUndecoratedRecord()
    {
        if (this.Skipped) { Assert.Skip("sqlite-vec native binary not present next to test assembly."); return; }

        var definition = new VectorStoreCollectionDefinition
        {
            Properties =
            [
                new VectorStoreKeyProperty(nameof(MevdPlainNote.Id), typeof(string)),
                new VectorStoreDataProperty(nameof(MevdPlainNote.Tag), typeof(string)),
                new VectorStoreVectorProperty(nameof(MevdPlainNote.Embedding), typeof(ReadOnlyMemory<float>), 4)
                {
                    DistanceFunction = DistanceFunction.CosineDistance
                }
            ]
        };

        var store = this.fx.CreateVectorStore(this.dbPath, o => o.MapVectorRecord<MevdPlainNote>("plain-notes", definition));
        var collection = new DocumentDbVectorStore(store).GetCollection<string, MevdPlainNote>("plain-notes");

        await collection.UpsertAsync(MevdSeed.Rows.Select(r => new MevdPlainNote { Id = r.Id, Tag = r.Tag, Embedding = r.Embedding }));
        var hits = await collection.SearchAsync(MevdSeed.Query, top: 2).ToListAsync();

        Assert.Equal("plain-notes", collection.Name);
        Assert.Equal(["n1", "n2"], hits.Select(h => h.Record.Id));
    }

    [Fact]
    public void MapVectorRecord_Throws_ForAMultiVectorRecord()
    {
        var options = new DocumentStoreOptions { DatabaseProvider = this.fx.CreateProvider() };
        var ex = Assert.Throws<InvalidOperationException>(() => options.MapVectorRecord<MevdMultiVectorNote>());
        Assert.Contains("one embedding per document type", ex.Message);
    }

    [Fact]
    public void MapVectorRecord_Throws_ForAnUnsupportedDistanceFunction()
    {
        var options = new DocumentStoreOptions { DatabaseProvider = this.fx.CreateProvider() };
        var ex = Assert.Throws<NotSupportedException>(() => options.MapVectorRecord<MevdManhattanNote>());
        Assert.Contains(DistanceFunction.ManhattanDistance, ex.Message);
    }

    [Fact]
    public void MapVectorRecord_Throws_WhenAPropertyDeclaresAStorageName()
    {
        var options = new DocumentStoreOptions { DatabaseProvider = this.fx.CreateProvider() };
        var ex = Assert.Throws<NotSupportedException>(() => options.MapVectorRecord<MevdRenamedNote>());
        Assert.Contains("JsonPropertyName", ex.Message);
    }

    [Fact]
    public void MapVectorRecord_Throws_ForAnUnsupportedKeyType()
    {
        var options = new DocumentStoreOptions { DatabaseProvider = this.fx.CreateProvider() };
        var ex = Assert.Throws<NotSupportedException>(() => options.MapVectorRecord<MevdBadKeyNote>());
        Assert.Contains("string, Guid, int, or long", ex.Message);
    }

    [Fact]
    public void MapVectorRecord_Throws_OnAProviderWithoutVectorSupport()
    {
        var options = new Shiny.DocumentDb.LiteDb.LiteDbDocumentStoreOptions
        {
            ConnectionString = Path.Combine(Path.GetTempPath(), $"mevd_lite_{Guid.NewGuid():N}.db")
        };
        Assert.Throws<NotSupportedException>(() => ((IDocumentStoreOptions)options).MapVectorRecord<MevdLiteNote>());
    }

    [Fact]
    public async Task AddDocumentDbVectorStore_RegistersOneStoreBehindBothFaces()
    {
        if (this.Skipped) { Assert.Skip("sqlite-vec native binary not present next to test assembly."); return; }

        var binary = SqliteDatabaseFixture.FindVec0BinaryPath()!;
        var services = new ServiceCollection();
        services.AddSingleton<IEmbeddingGenerator<string, Embedding<float>>>(new FakeEmbeddingGenerator(4));
        services.AddDocumentDbVectorStore(o =>
        {
            o.DatabaseProvider = new Shiny.DocumentDb.Sqlite.SqliteDatabaseProvider($"Data Source={this.dbPath}")
            {
                EnableVectorExtension = true,
                VectorExtensionPath = binary
            };
            o.MapVectorRecord<MevdNote>();
        });

        var sp = services.BuildServiceProvider();
        var vectorStore = sp.GetRequiredService<VectorStore>();

        // Same store behind both faces — one connection pool, not two.
        Assert.Same(sp.GetRequiredService<IDocumentStore>(), vectorStore.GetService(typeof(IDocumentStore)));

        var collection = vectorStore.GetCollection<string, MevdNote>(nameof(MevdNote));
        await collection.UpsertAsync(MevdSeed.Notes());

        // The generator came from DI, so a text query works without any explicit wiring.
        var hits = await collection.SearchAsync("1000", top: 1).ToListAsync();
        Assert.Equal("n1", hits[0].Record.Id);
    }

    [Fact]
    public void MapIndexKind_LeavesTheProviderDefault_ForUnsetAndDynamic()
    {
        Assert.Null(VectorRecordModel.MapIndexKind(null));
        Assert.Null(VectorRecordModel.MapIndexKind(IndexKind.Dynamic));
        Assert.Equal(VectorIndexKind.Hnsw, VectorRecordModel.MapIndexKind(IndexKind.Hnsw));
        Assert.Equal(VectorIndexKind.Ivf, VectorRecordModel.MapIndexKind(IndexKind.IvfFlat));
        Assert.Equal(VectorIndexKind.DiskAnn, VectorRecordModel.MapIndexKind(IndexKind.DiskAnn));
        Assert.Throws<NotSupportedException>(() => VectorRecordModel.MapIndexKind("not-an-index"));
    }

    [Fact]
    public void MapDistance_CollapsesTheSimilarityAndDistanceSpellings()
    {
        Assert.Equal(VectorDistance.Cosine, VectorRecordModel.MapDistance(DistanceFunction.CosineDistance));
        Assert.Equal(VectorDistance.Cosine, VectorRecordModel.MapDistance(DistanceFunction.CosineSimilarity));
        Assert.Equal(VectorDistance.Euclidean, VectorRecordModel.MapDistance(DistanceFunction.EuclideanDistance));
        Assert.Equal(VectorDistance.DotProduct, VectorRecordModel.MapDistance(DistanceFunction.DotProductSimilarity));
        Assert.Equal(VectorDistance.DotProduct, VectorRecordModel.MapDistance(DistanceFunction.NegativeDotProductSimilarity));
        Assert.Equal(VectorDistance.Hamming, VectorRecordModel.MapDistance(DistanceFunction.HammingDistance));
        Assert.Throws<NotSupportedException>(() => VectorRecordModel.MapDistance(DistanceFunction.EuclideanSquaredDistance));
    }
}

static class AsyncEnumerableTestExtensions
{
    public static async Task<List<T>> ToListAsync<T>(this IAsyncEnumerable<T> source)
    {
        var list = new List<T>();
        await foreach (var item in source)
            list.Add(item);
        return list;
    }
}
