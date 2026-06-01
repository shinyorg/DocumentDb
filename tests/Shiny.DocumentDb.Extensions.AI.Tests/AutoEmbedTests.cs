using Microsoft.Extensions.AI;
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

    static DocumentStore BuildStore(CountingEmbeddingGenerator gen)
    {
        var opts = new DocumentStoreOptions
        {
            DatabaseProvider = new SqliteDatabaseProvider("Data Source=:memory:")
        };
        opts.MapVectorProperty<Memo>(d => d.Embedding, dimensions: gen.Dimensions)
            .AutoEmbedOnInsert<Memo>(
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
    public async Task Upsert_RoutesThroughBeforeInsertHook()
    {
        var gen = new CountingEmbeddingGenerator();
        using var store = BuildStore(gen);

        await store.Upsert(new Memo { Id = "m1", Content = "abc" });
        Assert.Equal(1, gen.CallCount);
    }

    [Fact]
    public async Task BatchInsert_RunsHookOncePerDocument()
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

    [Fact]
    public async Task OnBeforeInsert_MultipleHooks_RunInRegistrationOrder()
    {
        var order = new List<string>();
        var opts = new DocumentStoreOptions
        {
            DatabaseProvider = new SqliteDatabaseProvider("Data Source=:memory:")
        };
        opts.OnBeforeInsert<Memo>((m, _) => { order.Add("first"); return Task.CompletedTask; });
        opts.OnBeforeInsert<Memo>((m, _) => { order.Add("second"); return Task.CompletedTask; });

        using var store = new DocumentStore(opts);
        await store.Insert(new Memo { Id = "m1", Content = "x" });

        Assert.Equal(new[] { "first", "second" }, order);
    }

    [Fact]
    public async Task OnBeforeInsert_NotInvokedForOtherTypes()
    {
        sealed_LocalDocs.OtherDoc.Invocations = 0;
        var opts = new DocumentStoreOptions
        {
            DatabaseProvider = new SqliteDatabaseProvider("Data Source=:memory:")
        };
        opts.OnBeforeInsert<Memo>((m, _) =>
        {
            sealed_LocalDocs.OtherDoc.Invocations++;
            return Task.CompletedTask;
        });

        using var store = new DocumentStore(opts);
        // Insert a non-Memo document — Memo hook should not fire.
        await store.Insert(new sealed_LocalDocs.OtherDoc { Id = "o1", Name = "n" });

        Assert.Equal(0, sealed_LocalDocs.OtherDoc.Invocations);
    }
}

internal static class sealed_LocalDocs
{
    public sealed class OtherDoc
    {
        public static int Invocations;
        public string Id { get; set; } = "";
        public string Name { get; set; } = "";
    }
}
