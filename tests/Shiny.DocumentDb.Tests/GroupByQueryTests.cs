using System.Text.Json;
using Shiny.DocumentDb.Tests.Fixtures;
using Xunit;

namespace Shiny.DocumentDb.Tests;

public abstract class GroupByQueryTestsBase : IDisposable
{
    protected readonly IDatabaseFixture Fixture;
    protected readonly IDocumentStore store;
    static readonly TestJsonContext ctx = new(new JsonSerializerOptions { PropertyNamingPolicy = JsonNamingPolicy.CamelCase });

    protected GroupByQueryTestsBase(IDatabaseFixture fixture)
    {
        this.Fixture = fixture;
        this.store = new DocumentStore(new DocumentStoreOptions
        {
            DatabaseProvider = fixture.CreateProvider(),
            JsonSerializerOptions = ctx.Options,
            TableName = $"t{Guid.NewGuid():N}"
        });
    }

    public void Dispose() => (this.store as IDisposable)?.Dispose();

    // In-memory providers (LiteDB/IndexedDB) group client-side: no string grammar, no ToQueryString.
    protected virtual bool SupportsStringGrammar => true;
    protected virtual bool SupportsQueryString => true;

    async Task SeedSalesAsync()
    {
        async Task Add(string id, string status, string region, decimal total, int qty, string state, int month)
            => await this.store.Insert(new Sale
            {
                Id = id,
                Status = status,
                Region = region,
                Total = total,
                Quantity = qty,
                ShippingAddress = new Address { State = state, City = state + "-city" },
                CreatedAt = new DateTimeOffset(2026, month, 15, 0, 0, 0, TimeSpan.Zero)
            }, ctx.Sale);

        await Add("s1", "Shipped", "West", 100m, 2, "OR", 1);
        await Add("s2", "Pending", "East", 200m, 1, "WA", 2);
        await Add("s3", "Shipped", "West", 300m, 3, "OR", 1);
        await Add("s4", "Shipped", "East", 50m, 1, "MA", 3);
        await Add("s5", "Pending", "West", 400m, 4, "CA", 2);
    }

    [Fact]
    public async Task SingleKey_Rollup()
    {
        await this.SeedSalesAsync();

        var rows = await this.store.Query(ctx.Sale)
            .GroupBy(s => s.Status)
            .Select(g => new StatusRollup
            {
                Status = g.Key,
                Count = g.Count(),
                Revenue = g.Sum(s => s.Total),
                MaxTotal = g.Max(s => s.Total),
                AvgTotal = g.Avg(s => s.Total)
            }, ctx.StatusRollup)
            .ToList();

        Assert.Equal(2, rows.Count);
        var shipped = rows.Single(r => r.Status == "Shipped");
        Assert.Equal(3, shipped.Count);
        Assert.Equal(450m, shipped.Revenue);
        Assert.Equal(300m, shipped.MaxTotal);
        Assert.Equal(150d, shipped.AvgTotal, 3);
        var pending = rows.Single(r => r.Status == "Pending");
        Assert.Equal(2, pending.Count);
        Assert.Equal(600m, pending.Revenue);
    }

    [Fact]
    public async Task SingleKey_WithWherePreFilter()
    {
        await this.SeedSalesAsync();

        var rows = await this.store.Query(ctx.Sale)
            .Where(s => s.Region == "West")
            .GroupBy(s => s.Status)
            .Select(g => new StatusRollup { Status = g.Key, Count = g.Count(), Revenue = g.Sum(s => s.Total) }, ctx.StatusRollup)
            .ToList();

        Assert.Equal(2, rows.Count);
        Assert.Equal(400m, rows.Single(r => r.Status == "Shipped").Revenue); // s1 + s3
        Assert.Equal(400m, rows.Single(r => r.Status == "Pending").Revenue); // s5
    }

    [Fact]
    public async Task NestedKey_Rollup()
    {
        await this.SeedSalesAsync();

        var rows = await this.store.Query(ctx.Sale)
            .GroupBy(s => s.ShippingAddress.State)
            .Select(g => new CountryRollup { Country = g.Key, Count = g.Count() }, ctx.CountryRollup)
            .ToList();

        Assert.Equal(4, rows.Count);
        Assert.Equal(2, rows.Single(r => r.Country == "OR").Count);
        Assert.Equal(1, rows.Single(r => r.Country == "WA").Count);
    }

    [Fact]
    public async Task MultiKey_Rollup()
    {
        await this.SeedSalesAsync();

        var rows = await this.store.Query(ctx.Sale)
            .GroupBy(s => new { s.Status, s.Region })
            .Select(g => new RegionStatusRollup
            {
                Status = g.Key.Status,
                Region = g.Key.Region,
                Count = g.Count(),
                Revenue = g.Sum(s => s.Total)
            }, ctx.RegionStatusRollup)
            .ToList();

        Assert.Equal(4, rows.Count);
        Assert.Equal(400m, rows.Single(r => r.Status == "Shipped" && r.Region == "West").Revenue);
        Assert.Equal(50m, rows.Single(r => r.Status == "Shipped" && r.Region == "East").Revenue);
        Assert.Equal(200m, rows.Single(r => r.Status == "Pending" && r.Region == "East").Revenue);
        Assert.Equal(400m, rows.Single(r => r.Status == "Pending" && r.Region == "West").Revenue);
    }

    [Fact]
    public async Task DerivedKey_ByMonth()
    {
        await this.SeedSalesAsync();

        var rows = await this.store.Query(ctx.Sale)
            .GroupBy(s => s.CreatedAt.Month)
            .Select(g => new MonthRollup { Month = g.Key, Revenue = g.Sum(s => s.Total) }, ctx.MonthRollup)
            .ToList();

        Assert.Equal(3, rows.Count);
        Assert.Equal(400m, rows.Single(r => r.Month == 1).Revenue); // s1 + s3
        Assert.Equal(600m, rows.Single(r => r.Month == 2).Revenue); // s2 + s5
        Assert.Equal(50m, rows.Single(r => r.Month == 3).Revenue);  // s4
    }

    [Fact]
    public async Task Having_FiltersGroups()
    {
        await this.SeedSalesAsync();

        var rows = await this.store.Query(ctx.Sale)
            .GroupBy(s => s.Status)
            .Having(g => g.Sum(s => s.Total) > 500m)
            .Select(g => new StatusRollup { Status = g.Key, Count = g.Count(), Revenue = g.Sum(s => s.Total) }, ctx.StatusRollup)
            .ToList();

        Assert.Single(rows);
        Assert.Equal("Pending", rows[0].Status);
        Assert.Equal(600m, rows[0].Revenue);
    }

    [Fact]
    public async Task GroupedOrderBy_AndPaginate()
    {
        await this.SeedSalesAsync();

        var rows = await this.store.Query(ctx.Sale)
            .GroupBy(s => s.Status)
            .Select(g => new StatusRollup { Status = g.Key, Count = g.Count(), Revenue = g.Sum(s => s.Total) }, ctx.StatusRollup)
            .OrderByDescending(r => r.Revenue)
            .Paginate(0, 1)
            .ToList();

        Assert.Single(rows);
        Assert.Equal("Pending", rows[0].Status); // 600 > 450
    }

    [Fact]
    public async Task EmptyAggregateSource_CoalescesToZero()
    {
        await this.store.Insert(new Sale { Id = "x", Status = "New", Region = "West", Total = 0m }, ctx.Sale);

        var rows = await this.store.Query(ctx.Sale)
            .GroupBy(s => s.Status)
            .Select(g => new StatusRollup { Status = g.Key, Count = g.Count(), Revenue = g.Sum(s => s.Total) }, ctx.StatusRollup)
            .ToList();

        Assert.Single(rows);
        Assert.Equal(0m, rows[0].Revenue);
        Assert.Equal(1, rows[0].Count);
    }

    [Fact]
    public async Task Count_ReturnsGroupCount()
    {
        await this.SeedSalesAsync();

        var groups = await this.store.Query(ctx.Sale)
            .GroupBy(s => s.Status)
            .Select(g => new StatusRollup { Status = g.Key, Count = g.Count() }, ctx.StatusRollup)
            .Count();

        Assert.Equal(2, groups);
    }

    [Fact]
    public async Task StringGrammar_ProjectAndHaving_MatchesLinq()
    {
        if (!this.SupportsStringGrammar)
            Assert.Skip("Provider groups client-side and does not support the string grammar.");
        await this.SeedSalesAsync();

        var stringRows = await this.store.Query(ctx.Sale)
            .GroupBy("status")
            .Having("sum(total) > 500")
            .Project("status, count() as count, sum(total) as revenue")
            .ToList();

        Assert.Single(stringRows);
        Assert.Equal("Pending", (string)stringRows[0]["status"]!);
        Assert.Equal(2, (int)stringRows[0]["count"]!);
        Assert.Equal(600m, (decimal)stringRows[0]["revenue"]!);
    }

    [Fact]
    public async Task StringGrammar_ParityWithLinq()
    {
        if (!this.SupportsStringGrammar)
            Assert.Skip("Provider groups client-side and does not support the string grammar.");
        await this.SeedSalesAsync();

        var linq = await this.store.Query(ctx.Sale)
            .GroupBy(s => s.Status)
            .Select(g => new StatusRollup { Status = g.Key, Count = g.Count(), Revenue = g.Sum(s => s.Total) }, ctx.StatusRollup)
            .ToList();

        var str = await this.store.Query(ctx.Sale)
            .GroupBy("status")
            .Project("status, count() as count, sum(total) as revenue")
            .ToList();

        Assert.Equal(linq.Count, str.Count);
        foreach (var row in str)
        {
            var status = (string)row["status"]!;
            var match = linq.Single(r => r.Status == status);
            Assert.Equal(match.Count, (int)row["count"]!);
            Assert.Equal(match.Revenue, (decimal)row["revenue"]!);
        }
    }

    [Fact]
    public async Task ToQueryString_EmitsGroupByAndHaving()
    {
        if (!this.SupportsQueryString)
            Assert.Skip("Provider does not produce a query string.");
        var qs = this.store.Query(ctx.Sale)
            .GroupBy(s => s.Status)
            .Having(g => g.Sum(s => s.Total) > 500m)
            .Select(g => new StatusRollup { Status = g.Key, Count = g.Count(), Revenue = g.Sum(s => s.Total) }, ctx.StatusRollup)
            .ToQueryString();

        Assert.Contains("GROUP BY", qs.Sql);
        Assert.Contains("HAVING", qs.Sql);
    }
}
