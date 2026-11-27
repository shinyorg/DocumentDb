using Shiny.DocumentDb.Tests.Fixtures;
using Xunit;

namespace Shiny.DocumentDb.Tests;

public class IncCustomer
{
    public string Id { get; set; } = "";
    public string Name { get; set; } = "";
}

public class IncProduct
{
    public Guid Id { get; set; }
    public string Title { get; set; } = "";
}

public class IncOrder
{
    public string Id { get; set; } = "";
    public string? CustomerId { get; set; }
    public List<Guid> ProductIds { get; set; } = [];
    public Guid? FeaturedProductId { get; set; }
    public int Total { get; set; }
}

/// <summary><c>Include</c> asserted identically on every provider.</summary>
public abstract class IncludeConformanceTestsBase(IDocumentStoreFixture fixture)
{
    protected readonly IDocumentStoreFixture Fixture = fixture;

    static readonly Guid Apple = Guid.Parse("11111111-1111-1111-1111-111111111111");
    static readonly Guid Pear = Guid.Parse("22222222-2222-2222-2222-222222222222");
    static readonly Guid Plum = Guid.Parse("33333333-3333-3333-3333-333333333333");

    async Task<IDocumentStore> SeededAsync()
    {
        var store = this.Fixture.CreateStore($"i{Guid.NewGuid():N}");
        await store.Insert(new IncCustomer { Id = "c1", Name = "Ada" });
        await store.Insert(new IncCustomer { Id = "c2", Name = "Alan" });
        await store.Insert(new IncCustomer { Id = "c3", Name = "Grace" });
        await store.Insert(new IncProduct { Id = Apple, Title = "Apple" });
        await store.Insert(new IncProduct { Id = Pear, Title = "Pear" });
        await store.Insert(new IncProduct { Id = Plum, Title = "Plum" });
        await store.Insert(new IncOrder { Id = "o1", CustomerId = "c1", ProductIds = [Apple, Pear], FeaturedProductId = Apple, Total = 10 });
        await store.Insert(new IncOrder { Id = "o2", CustomerId = "c2", ProductIds = [Pear], Total = 20 });
        await store.Insert(new IncOrder { Id = "o3", CustomerId = "c1", ProductIds = [], Total = 30 });
        await store.Insert(new IncOrder { Id = "o4", CustomerId = null, Total = 40 });
        await store.Insert(new IncOrder { Id = "o5", CustomerId = "missing", Total = 50 });
        return store;
    }

    [Fact]
    public async Task ToList_LoadsTheReferencedDocuments()
    {
        var store = await SeededAsync();
        var customers = new Dictionary<string, IncCustomer>();

        var orders = await store.Query<IncOrder>().Include(o => o.CustomerId!, customers).ToList();

        Assert.Equal(5, orders.Count);
        Assert.Equal(["c1", "c2"], customers.Keys.OrderBy(x => x));
        Assert.Equal("Ada", customers["c1"].Name);
    }

    [Fact]
    public async Task OnlyTheReferencesOfMatchingResults_AreLoaded()
    {
        var store = await SeededAsync();
        var customers = new Dictionary<string, IncCustomer>();

        await store.Query<IncOrder>().Where(o => o.Total >= 20).Include(o => o.CustomerId!, customers).ToList();

        Assert.Equal(["c1", "c2"], customers.Keys.OrderBy(x => x));
        customers.Clear();
        await store.Query<IncOrder>().Where(o => o.Total == 20).Include(o => o.CustomerId!, customers).ToList();
        Assert.Equal("c2", Assert.Single(customers.Keys));
    }

    [Fact]
    public async Task CollectionsOfKeys_AndSeveralIncludes()
    {
        var store = await SeededAsync();
        var customers = new Dictionary<string, IncCustomer>();
        var products = new Dictionary<Guid, IncProduct>();

        await store.Query<IncOrder>()
            .Include(o => o.CustomerId!, customers)
            .Include(o => o.ProductIds, products)
            .ToList();

        Assert.Equal(2, customers.Count);
        Assert.Equal([Apple, Pear], products.Keys.OrderBy(x => x));
        Assert.Equal("Pear", products[Pear].Title);
    }

    [Fact]
    public async Task NullableStructKeys()
    {
        var store = await SeededAsync();
        var products = new Dictionary<Guid, IncProduct>();

        await store.Query<IncOrder>().Include(o => o.FeaturedProductId, products).ToList();

        Assert.Equal(Apple, Assert.Single(products.Keys));
    }

    [Fact]
    public async Task BuilderCallsAfterInclude_KeepIt()
    {
        var store = await SeededAsync();
        var customers = new Dictionary<string, IncCustomer>();

        var first = await store.Query<IncOrder>()
            .Include(o => o.CustomerId!, customers)
            .Where(o => o.Total > 15)
            .OrderBy(o => o.Total)
            .FirstOrDefault();

        Assert.Equal("o2", first!.Id);
        Assert.Equal("c2", Assert.Single(customers.Keys));
    }

    [Fact]
    public async Task Streaming_AndPaging_LoadTheReferences()
    {
        var store = await SeededAsync();
        var streamed = new Dictionary<string, IncCustomer>();
        await foreach (var _ in store.Query<IncOrder>().Include(o => o.CustomerId!, streamed).ToAsyncEnumerable())
        {
        }
        Assert.Equal(2, streamed.Count);

        var paged = new Dictionary<string, IncCustomer>();
        var page = await store.Query<IncOrder>().OrderBy(o => o.Total).Include(o => o.CustomerId!, paged).ToCursorPage(null, 2);
        Assert.Equal(2, page.Items.Count);
        Assert.Equal(["c1", "c2"], paged.Keys.OrderBy(x => x));
    }

    [Fact]
    public async Task CountsAndAggregates_IgnoreTheInclude()
    {
        var store = await SeededAsync();
        var customers = new Dictionary<string, IncCustomer>();

        Assert.Equal(5, await store.Query<IncOrder>().Include(o => o.CustomerId!, customers).Count());
        Assert.Empty(customers);
    }
}
