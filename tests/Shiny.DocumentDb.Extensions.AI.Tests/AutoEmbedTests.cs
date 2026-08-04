using Microsoft.Extensions.AI;
using Microsoft.Extensions.DependencyInjection;
using Shiny.DocumentDb.Sqlite;
using Xunit;

namespace Shiny.DocumentDb.Extensions.AI.Tests;

public class AutoEmbedTests
{
    sealed class Memo
    {
        public string Id { get; set; } = "";
        public string Content { get; set; } = "";
        public ReadOnlyMemory<float> Embedding { get; set; }
    }

    sealed class CountingEmbeddingGenerator : IEmbeddingGenerator<string, Embedding<float>>
    {
        public int CallCount;
        public List<string> LastBatch = new();
        public int Dimensions { get; init; } = 8;

        public Task<GeneratedEmbeddings<Embedding<float>>> GenerateAsync(
            IEnumerable<string> values,
            EmbeddingGenerationOptions? options = null,
            CancellationToken cancellationToken = default)
        {
            this.LastBatch = values.ToList();
            this.CallCount += this.LastBatch.Count;

            var generated = new GeneratedEmbeddings<Embedding<float>>();
            foreach (var v in this.LastBatch)
            {
                // Deterministic stub: each call sets vec[0] to the input length.
                var vec = new float[this.Dimensions];
                vec[0] = v.Length;
                generated.Add(new Embedding<float>(vec));
            }
            return Task.FromResult(generated);
        }

        public EmbeddingGeneratorMetadata Metadata => new();
        public object? GetService(Type serviceType, object? serviceKey = null) => null;
        public void Dispose() { }
    }

    // ── Explicit-generator overload (container-free `new DocumentStore(options)` path) ──────

    static DocumentStore BuildStore(CountingEmbeddingGenerator gen)
    {
        var opts = new DocumentStoreOptions
        {
            DatabaseProvider = new SqliteDatabaseProvider("Data Source=:memory:")
        };
        opts.ConfigureDocument<Memo>(cfg => cfg.MapVectorProperty(d => d.Embedding, dimensions: gen.Dimensions));
        opts.AutoEmbedOnInsert<Memo>(
                gen,
                sourceSelector: d => d.Content,
                targetSetter: (d, v) => d.Embedding = v,
                targetGetter: d => d.Embedding);
        return new DocumentStore(opts);
    }

    [Fact]
    public async Task Insert_PopulatesVectorFromText()
    {
        var gen = new CountingEmbeddingGenerator();
        using var store = BuildStore(gen);

        var memo = new Memo { Id = "m1", Content = "hello" };
        await store.Insert(memo);

        Assert.Equal(1, gen.CallCount);
        Assert.Equal(gen.Dimensions, memo.Embedding.Length);
        Assert.Equal(5f, memo.Embedding.Span[0]); // "hello".Length == 5
    }

    [Fact]
    public async Task Insert_EmptyContent_SkipsGenerator()
    {
        var gen = new CountingEmbeddingGenerator();
        using var store = BuildStore(gen);

        var memo = new Memo { Id = "m1", Content = "" };
        await store.Insert(memo);

        Assert.Equal(0, gen.CallCount);
        Assert.Equal(0, memo.Embedding.Length);
    }

    [Fact]
    public async Task Insert_PrePopulatedVector_SkipsGenerator()
    {
        var gen = new CountingEmbeddingGenerator();
        using var store = BuildStore(gen);

        var explicitVec = new float[gen.Dimensions];
        explicitVec[0] = 42f;
        var memo = new Memo { Id = "m1", Content = "hello", Embedding = explicitVec };
        await store.Insert(memo);

        Assert.Equal(0, gen.CallCount);
        Assert.Equal(42f, memo.Embedding.Span[0]); // explicit value won
    }

    [Fact]
    public async Task Upsert_PopulatesVector()
    {
        var gen = new CountingEmbeddingGenerator();
        using var store = BuildStore(gen);

        await store.Upsert(new Memo { Id = "m1", Content = "abc" });
        Assert.Equal(1, gen.CallCount);
    }

    [Fact]
    public async Task BatchInsert_EmbedsOncePerDocument()
    {
        var gen = new CountingEmbeddingGenerator();
        using var store = BuildStore(gen);

        var memos = new[]
        {
            new Memo { Id = "m1", Content = "a" },
            new Memo { Id = "m2", Content = "bb" },
            new Memo { Id = "m3", Content = "ccc" }
        };
        await store.BatchInsert(memos);

        Assert.Equal(3, gen.CallCount);
        Assert.Equal(1f, memos[0].Embedding.Span[0]);
        Assert.Equal(2f, memos[1].Embedding.Span[0]);
        Assert.Equal(3f, memos[2].Embedding.Span[0]);
    }

    // ── DI overload (generator resolved from the write's scope — fix #2) ────────────────────

    static ServiceProvider BuildDiProvider(Action<IServiceCollection> registerGenerator)
    {
        var services = new ServiceCollection();
        registerGenerator(services);
        services.AddDocumentStore(o =>
        {
            o.DatabaseProvider = new SqliteDatabaseProvider("Data Source=:memory:");
            o.ConfigureDocument<Memo>(cfg => cfg.MapVectorProperty(d => d.Embedding, dimensions: 8));
            o.AutoEmbedOnInsert<Memo>(
                    sourceSelector: d => d.Content,
                    targetSetter: (d, v) => d.Embedding = v,
                    targetGetter: d => d.Embedding);
        });
        return services.BuildServiceProvider();
    }

    [Fact]
    public async Task Insert_ResolvesGeneratorFromDI()
    {
        var gen = new CountingEmbeddingGenerator();
        await using var sp = BuildDiProvider(s => s.AddSingleton<IEmbeddingGenerator<string, Embedding<float>>>(gen));
        var store = sp.GetRequiredService<IDocumentStore>();

        var memo = new Memo { Id = "m1", Content = "hello" };
        await store.Insert(memo);

        Assert.Equal(1, gen.CallCount);
        Assert.Equal(5f, memo.Embedding.Span[0]);
    }

    [Fact]
    public async Task Insert_NoGeneratorRegistered_ThrowsHelpful()
    {
        await using var sp = BuildDiProvider(_ => { /* no generator registered */ });
        var store = sp.GetRequiredService<IDocumentStore>();

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(
            () => store.Insert(new Memo { Id = "m1", Content = "hi" }));
        Assert.Contains("IEmbeddingGenerator", ex.Message);
    }

    [Fact]
    public async Task ScopedSession_UsesTheSessionScopesGenerator()
    {
        // A scoped generator: each DI scope gets its own instance. A write through a scoped session must
        // resolve THAT scope's generator (ctx.Services flows the session scope), not the root's.
        await using var sp = BuildDiProvider(s =>
            s.AddScoped<IEmbeddingGenerator<string, Embedding<float>>>(_ => new CountingEmbeddingGenerator()));
        var store = sp.GetRequiredService<IDocumentStore>();

        using var scope = sp.CreateScope();
        var scopedGen = (CountingEmbeddingGenerator)scope.ServiceProvider
            .GetRequiredService<IEmbeddingGenerator<string, Embedding<float>>>();

        await using var session = store.OpenSession(scope.ServiceProvider);
        session.Add(new Memo { Id = "m1", Content = "hello" });
        await session.SaveChanges();

        Assert.Equal(1, scopedGen.CallCount); // the scope's own generator embedded the write
    }
}
