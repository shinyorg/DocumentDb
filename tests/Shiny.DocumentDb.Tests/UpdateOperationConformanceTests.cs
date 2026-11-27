using System.Text.Json.Nodes;
using Shiny.DocumentDb.Tests.Fixtures;
using Xunit;

namespace Shiny.DocumentDb.Tests;

public class CounterDoc
{
    public string Id { get; set; } = "";
    public string Category { get; set; } = "";
    public int Hits { get; set; }
    public int? Optional { get; set; }
    public decimal Balance { get; set; }
    public string? Note { get; set; }
    public List<string> Tags { get; set; } = [];
    public List<CounterLine>? Lines { get; set; }
    public CounterStats Stats { get; set; } = new();
}

public class CounterLine
{
    public string Sku { get; set; } = "";
    public int Qty { get; set; }
}

public class CounterStats
{
    public int Views { get; set; }
}

/// <summary>
/// The <see cref="IDocumentUpdateBuilder{T}"/> operations beyond <c>Set</c> — Increment, Unset, Push, Pull,
/// AddToSet — asserted identically on every provider.
/// </summary>
public abstract class UpdateOperationConformanceTestsBase(IDocumentStoreFixture fixture)
{
    protected readonly IDocumentStoreFixture Fixture = fixture;

    async Task<IDocumentStore> SeededAsync()
    {
        var store = this.Fixture.CreateStore($"u{Guid.NewGuid():N}");
        await store.Insert(new CounterDoc { Id = "a", Category = "x", Hits = 1, Balance = 10.5m, Note = "keep", Tags = ["red"], Stats = new() { Views = 2 } });
        await store.Insert(new CounterDoc { Id = "b", Category = "x", Hits = 5, Optional = 3, Tags = ["red", "blue", "red"] });
        await store.Insert(new CounterDoc { Id = "c", Category = "y", Hits = 100 });
        return store;
    }

    [Fact]
    public async Task Increment_AddsToEveryMatch_AndLeavesTheRestAlone()
    {
        var store = await SeededAsync();

        var affected = await store.Query<CounterDoc>().Where(x => x.Category == "x").ExecuteUpdate(b => b.Increment(x => x.Hits, 2));

        Assert.Equal(2, affected);
        Assert.Equal(3, (await store.Get<CounterDoc>("a"))!.Hits);
        Assert.Equal(7, (await store.Get<CounterDoc>("b"))!.Hits);
        Assert.Equal(100, (await store.Get<CounterDoc>("c"))!.Hits);
    }

    [Fact]
    public async Task Increment_Negative_Subtracts()
    {
        var store = await SeededAsync();
        await store.Query<CounterDoc>().Where(x => x.Id == "c").ExecuteUpdate(b => b.Increment(x => x.Hits, -40));
        Assert.Equal(60, (await store.Get<CounterDoc>("c"))!.Hits);
    }

    [Fact]
    public async Task Increment_NullOrMissing_StartsFromZero()
    {
        var store = await SeededAsync();
        await store.Query<CounterDoc>().ExecuteUpdate(b => b.Increment(x => x.Optional, 4));

        Assert.Equal(4, (await store.Get<CounterDoc>("a"))!.Optional);
        Assert.Equal(7, (await store.Get<CounterDoc>("b"))!.Optional);
    }

    [Fact]
    public async Task Increment_Decimal_KeepsTheFraction()
    {
        var store = await SeededAsync();
        await store.Query<CounterDoc>().Where(x => x.Id == "a").ExecuteUpdate(b => b.Increment(x => x.Balance, 0.25m));
        Assert.Equal(10.75m, (await store.Get<CounterDoc>("a"))!.Balance);
    }

    [Fact]
    public async Task Increment_NestedProperty()
    {
        var store = await SeededAsync();
        await store.Query<CounterDoc>().Where(x => x.Id == "a").ExecuteUpdate(b => b.Increment(x => x.Stats.Views, 3));
        Assert.Equal(5, (await store.Get<CounterDoc>("a"))!.Stats.Views);
    }

    [Fact]
    public async Task Increment_IsVisibleToQueriesOnTheProperty()
    {
        var store = await SeededAsync();
        await store.Query<CounterDoc>().ExecuteUpdate(b => b.Increment(x => x.Hits, 10));

        var big = await store.Query<CounterDoc>().Where(x => x.Hits > 14).OrderBy(x => x.Id).ToList();
        Assert.Equal(["b", "c"], big.Select(x => x.Id));
    }

    [Fact]
    public async Task Unset_RemovesTheProperty()
    {
        var store = await SeededAsync();
        await store.Query<CounterDoc>().Where(x => x.Id == "a").ExecuteUpdate(b => b.Unset(x => x.Note));

        Assert.Null((await store.Get<CounterDoc>("a"))!.Note);
        Assert.Equal(1, await store.Query<CounterDoc>().Where(x => x.Note == null && x.Id == "a").Count());
    }

    [Fact]
    public async Task Push_AppendsAndCreatesMissingArrays()
    {
        var store = await SeededAsync();
        await store.Query<CounterDoc>().Where(x => x.Category == "x").ExecuteUpdate(b => b.Push(x => x.Tags, "green"));
        await store.Query<CounterDoc>().Where(x => x.Id == "c").ExecuteUpdate(b => b.Push(x => x.Lines, new CounterLine { Sku = "s1", Qty = 2 }));

        Assert.Equal(["red", "green"], (await store.Get<CounterDoc>("a"))!.Tags);
        Assert.Equal(["red", "blue", "red", "green"], (await store.Get<CounterDoc>("b"))!.Tags);
        var lines = (await store.Get<CounterDoc>("c"))!.Lines;
        Assert.NotNull(lines);
        Assert.Equal("s1", Assert.Single(lines!).Sku);
    }

    [Fact]
    public async Task Pull_RemovesEveryEqualElement()
    {
        var store = await SeededAsync();
        await store.Query<CounterDoc>().ExecuteUpdate(b => b.Pull(x => x.Tags, "red"));

        Assert.Empty((await store.Get<CounterDoc>("a"))!.Tags);
        Assert.Equal(["blue"], (await store.Get<CounterDoc>("b"))!.Tags);
        Assert.Empty((await store.Get<CounterDoc>("c"))!.Tags);
    }

    [Fact]
    public async Task Pull_MatchesObjectsByValue()
    {
        var store = await SeededAsync();
        await store.Query<CounterDoc>().Where(x => x.Id == "c").ExecuteUpdate(b => b.Push(x => x.Lines, new CounterLine { Sku = "s1", Qty = 1 }));
        await store.Query<CounterDoc>().Where(x => x.Id == "c").ExecuteUpdate(b => b.Push(x => x.Lines, new CounterLine { Sku = "s2", Qty = 1 }));

        await store.Query<CounterDoc>().Where(x => x.Id == "c").ExecuteUpdate(b => b.Pull(x => x.Lines, new CounterLine { Sku = "s1", Qty = 1 }));

        Assert.Equal("s2", Assert.Single((await store.Get<CounterDoc>("c"))!.Lines!).Sku);
    }

    [Fact]
    public async Task AddToSet_AppendsOnlyWhenAbsent()
    {
        var store = await SeededAsync();
        await store.Query<CounterDoc>().ExecuteUpdate(b => b.AddToSet(x => x.Tags, "blue"));

        Assert.Equal(["red", "blue"], (await store.Get<CounterDoc>("a"))!.Tags);
        Assert.Equal(["red", "blue", "red"], (await store.Get<CounterDoc>("b"))!.Tags);
        Assert.Equal(["blue"], (await store.Get<CounterDoc>("c"))!.Tags);
    }

    [Fact]
    public async Task Operations_CombineWithSet_InOneCall()
    {
        var store = await SeededAsync();
        var affected = await store.Query<CounterDoc>().Where(x => x.Id == "b").ExecuteUpdate(b => b
            .Set(x => x.Category, "z")
            .Increment(x => x.Hits, 1)
            .AddToSet(x => x.Tags, "gold"));

        Assert.Equal(1, affected);
        var b = (await store.Get<CounterDoc>("b"))!;
        Assert.Equal("z", b.Category);
        Assert.Equal(6, b.Hits);
        Assert.Contains("gold", b.Tags);
    }

    [Fact]
    public async Task SamePropertyTwice_Throws()
    {
        var store = await SeededAsync();
        await Assert.ThrowsAsync<ArgumentException>(() =>
            store.Query<CounterDoc>().ExecuteUpdate(b => b.Increment(x => x.Hits, 1).Set(x => x.Hits, 3)));
    }
}

public class UpdateOperationUnitTests
{
    static readonly System.Text.Json.JsonSerializerOptions Options = new() { PropertyNamingPolicy = System.Text.Json.JsonNamingPolicy.CamelCase };

    [Fact]
    public void Applier_IncrementPreservesIntegers()
    {
        var node = JsonNode.Parse("""{"hits":1}""")!.AsObject();
        Internal.JsonUpdateApplier.Apply(node, "hits", DocumentUpdateOperation.Increment(2), Options);
        Assert.Equal("""{"hits":3}""", node.ToJsonString());
    }

    [Fact]
    public void Applier_IncrementCreatesIntermediateObjects()
    {
        var node = JsonNode.Parse("{}")!.AsObject();
        Internal.JsonUpdateApplier.Apply(node, "stats.views", DocumentUpdateOperation.Increment(1), Options);
        Assert.Equal("""{"stats":{"views":1}}""", node.ToJsonString());
    }

    [Fact]
    public void Applier_PullOnNullLeavesNull()
    {
        var node = JsonNode.Parse("""{"tags":null}""")!.AsObject();
        Internal.JsonUpdateApplier.Apply(node, "tags", DocumentUpdateOperation.Pull("x"), Options);
        Assert.Equal("""{"tags":null}""", node.ToJsonString());
    }

    [Fact]
    public void Applier_PushOntoAScalar_Throws()
        => Assert.Throws<InvalidOperationException>(() =>
            Internal.JsonUpdateApplier.Apply(JsonNode.Parse("""{"tags":"x"}""")!.AsObject(), "tags", DocumentUpdateOperation.Push("y"), Options));

    [Fact]
    public void Increment_RejectsNonNumbers()
        => Assert.Throws<ArgumentException>(() => DocumentUpdateOperation.Increment("1"));

    [Fact]
    public void Operations_ShowInTheBulkInterceptorAssignments()
    {
        var op = DocumentUpdateOperation.AddToSet("x");
        Assert.Equal(DocumentUpdateKind.AddToSet, op.Kind);
        Assert.Equal("x", op.Operand);
    }
}
