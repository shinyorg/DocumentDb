using System.Text.Json;
using Shiny.DocumentDb.Tests.Fixtures;
using Xunit;

namespace Shiny.DocumentDb.Tests;

/// <summary>
/// Grouped-aggregation coverage for the client-side (in-memory) providers (LiteDB / IndexedDB). These
/// group by materializing the filtered set and running the compiled projection, so only the typed LINQ
/// surface applies — the string grammar and ToQueryString are relational/Mongo-only.
/// </summary>
public abstract class InMemoryGroupByTestsBase : IDisposable
{
    readonly IDocumentStore store;
    static readonly TestJsonContext ctx = new(new JsonSerializerOptions { PropertyNamingPolicy = JsonNamingPolicy.CamelCase });

    protected InMemoryGroupByTestsBase(IDocumentStoreFixture fixture)
        => this.store = fixture.CreateStore($"t{Guid.NewGuid():N}");

    public void Dispose() => (this.store as IDisposable)?.Dispose();

    async Task SeedAsync()
    {
        async Task Add(string id, string status, string region, decimal total, int month, string state)
            => await this.store.Insert(new Sale
            {
                Id = id, Status = status, Region = region, Total = total, Quantity = 1,
                ShippingAddress = new Address { State = state },
                CreatedAt = new DateTimeOffset(2026, month, 15, 0, 0, 0, TimeSpan.Zero)
            }, ctx.Sale);

        await Add("s1", "Shipped", "West", 100m, 1, "OR");
        await Add("s2", "Pending", "East", 200m, 2, "WA");
        await Add("s3", "Shipped", "West", 300m, 1, "OR");
        await Add("s4", "Shipped", "East", 50m, 3, "MA");
        await Add("s5", "Pending", "West", 400m, 2, "CA");
    }

    [Fact]
    public async Task SingleKey_Rollup()
    {
        await this.SeedAsync();
        var rows = await this.store.Query(ctx.Sale)
            .GroupBy(s => s.Status)
            .Select(g => new StatusRollup
            {
                Status = g.Key, Count = g.Count(), Revenue = g.Sum(s => s.Total),
                MaxTotal = g.Max(s => s.Total), AvgTotal = g.Avg(s => s.Total)
            }, ctx.StatusRollup)
            .ToList();

        Assert.Equal(2, rows.Count);
        var shipped = rows.Single(r => r.Status == "Shipped");
        Assert.Equal(3, shipped.Count);
        Assert.Equal(450m, shipped.Revenue);
        Assert.Equal(300m, shipped.MaxTotal);
        Assert.Equal(150d, shipped.AvgTotal, 3);
        Assert.Equal(600m, rows.Single(r => r.Status == "Pending").Revenue);
    }

    [Fact]
    public async Task MultiKey_And_Having_And_OrderBy()
    {
        await this.SeedAsync();

        var multi = await this.store.Query(ctx.Sale)
            .GroupBy(s => new { s.Status, s.Region })
            .Select(g => new RegionStatusRollup { Status = g.Key.Status, Region = g.Key.Region, Count = g.Count(), Revenue = g.Sum(s => s.Total) }, ctx.RegionStatusRollup)
            .ToList();
        Assert.Equal(4, multi.Count);
        Assert.Equal(400m, multi.Single(r => r.Status == "Shipped" && r.Region == "West").Revenue);

        var having = await this.store.Query(ctx.Sale)
            .GroupBy(s => s.Status)
            .Having(g => g.Sum(s => s.Total) > 500m)
            .Select(g => new StatusRollup { Status = g.Key, Revenue = g.Sum(s => s.Total) }, ctx.StatusRollup)
            .ToList();
        Assert.Single(having);
        Assert.Equal("Pending", having[0].Status);

        var top = await this.store.Query(ctx.Sale)
            .GroupBy(s => s.Status)
            .Select(g => new StatusRollup { Status = g.Key, Revenue = g.Sum(s => s.Total) }, ctx.StatusRollup)
            .OrderByDescending(r => r.Revenue)
            .Paginate(0, 1)
            .ToList();
        Assert.Single(top);
        Assert.Equal("Pending", top[0].Status);
    }

    [Fact]
    public async Task DerivedKey_ByMonth()
    {
        await this.SeedAsync();
        var rows = await this.store.Query(ctx.Sale)
            .GroupBy(s => s.CreatedAt.Month)
            .Select(g => new MonthRollup { Month = g.Key, Revenue = g.Sum(s => s.Total) }, ctx.MonthRollup)
            .ToList();

        Assert.Equal(3, rows.Count);
        Assert.Equal(400m, rows.Single(r => r.Month == 1).Revenue);
        Assert.Equal(600m, rows.Single(r => r.Month == 2).Revenue);
    }

    [Fact]
    public async Task MinMax_OverDateAndString_ReturnsTypedExtremes()
    {
        // Regression: the in-memory interpreter forced Convert.ToDouble for Min/Max, which throws on strings and
        // dates. It must compare by the value's natural ordering, mirroring the relational typed MIN/MAX.
        await this.SeedAsync();
        var rows = await this.store.Query(ctx.Sale)
            .GroupBy(s => s.Status)
            .Select(g => new StatusExtremesRollup
            {
                Status = g.Key,
                FirstCreated = g.Min(s => s.CreatedAt),
                LastCreated = g.Max(s => s.CreatedAt),
                MinRegion = g.Min(s => s.Region),
                MaxRegion = g.Max(s => s.Region)
            }, ctx.StatusExtremesRollup)
            .ToList();

        var shipped = rows.Single(r => r.Status == "Shipped");   // Jan/Jan/Mar, West/West/East
        Assert.Equal(new DateTimeOffset(2026, 1, 15, 0, 0, 0, TimeSpan.Zero), shipped.FirstCreated);
        Assert.Equal(new DateTimeOffset(2026, 3, 15, 0, 0, 0, TimeSpan.Zero), shipped.LastCreated);
        Assert.Equal("East", shipped.MinRegion);
        Assert.Equal("West", shipped.MaxRegion);
    }

    [Fact]
    public async Task StringGrammar_Throws()
    {
        await this.SeedAsync();
        Assert.Throws<NotSupportedException>(() => this.store.Query(ctx.Sale).GroupBy("status"));
    }
}
