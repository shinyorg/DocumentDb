using Shiny.DocumentDb.Tests.Fixtures;
using Xunit;

namespace Shiny.DocumentDb.Tests;

public abstract class Payment
{
    public string Id { get; set; } = "";
    public string Kind { get; set; } = "";
    public decimal Amount { get; set; }
    public bool Archived { get; set; }
}

public class CardPayment : Payment
{
    public string Last4 { get; set; } = "";
}

public class PremiumCardPayment : CardPayment
{
    public string Tier { get; set; } = "";
}

public class CashPayment : Payment
{
    public decimal Tendered { get; set; }
}

/// <summary>
/// <c>MapDerivedTypes</c> asserted identically on every provider: derived types share the base type's storage, read
/// back as their own type, and querying a derived type returns only it (and its own derived types).
/// </summary>
public abstract class HierarchyConformanceTestsBase(IDocumentStoreFixture fixture)
{
    protected readonly IDocumentStoreFixture Fixture = fixture;

    static void Configure(IDocumentStoreOptions o)
        => o.ConfigureDocument<Payment>(cfg => cfg
            .MapDerivedTypes(x => x.Kind, d => d
                .Add<CardPayment>("card")
                .Add<PremiumCardPayment>("premium")
                .Add<CashPayment>("cash"))
            .AddQueryFilter(x => !x.Archived));

    async Task<IDocumentStore> SeededAsync()
    {
        var store = this.Fixture.CreateStore($"h{Guid.NewGuid():N}", Configure);
        await store.Insert(new CardPayment { Id = "c1", Amount = 10, Last4 = "4242" });
        await store.Insert<Payment>(new CashPayment { Id = "k1", Amount = 20, Tendered = 50 });
        await store.Insert(new PremiumCardPayment { Id = "p1", Amount = 30, Last4 = "1111", Tier = "gold" });
        await store.Insert(new CashPayment { Id = "k2", Amount = 40, Tendered = 40, Archived = true });
        return store;
    }

    [Fact]
    public async Task QueryingTheBase_ReturnsEveryDerivedType_AsItsOwnType()
    {
        var store = await SeededAsync();
        var all = await store.Query<Payment>().OrderBy(x => x.Amount).ToList();

        Assert.Collection(all,
            x => Assert.Equal("4242", Assert.IsType<CardPayment>(x).Last4),
            x => Assert.Equal(50, Assert.IsType<CashPayment>(x).Tendered),
            x => Assert.Equal("gold", Assert.IsType<PremiumCardPayment>(x).Tier));
    }

    [Fact]
    public async Task QueryingADerivedType_ReturnsOnlyIt_AndItsOwnDerivedTypes()
    {
        var store = await SeededAsync();

        var cards = await store.Query<CardPayment>().OrderBy(x => x.Amount).ToList();
        Assert.Equal(["c1", "p1"], cards.Select(x => x.Id));
        Assert.IsType<PremiumCardPayment>(cards[1]);

        Assert.Equal("p1", Assert.Single(await store.Query<PremiumCardPayment>().ToList()).Id);
        Assert.Equal("k1", Assert.Single(await store.Query<CashPayment>().ToList()).Id);
        Assert.Equal(2, await store.Query<CardPayment>().Count());
    }

    [Fact]
    public async Task DerivedProperties_AreQueryable()
    {
        var store = await SeededAsync();
        var hit = await store.Query<CardPayment>().Where(x => x.Last4 == "1111").ToList();
        Assert.Equal("p1", Assert.Single(hit).Id);
    }

    [Fact]
    public async Task Get_ReturnsTheStoredType_OrNullForASibling()
    {
        var store = await SeededAsync();

        Assert.IsType<CardPayment>(await store.Get<Payment>("c1"));
        Assert.IsType<CardPayment>(await store.Get<CardPayment>("c1"));
        Assert.Null(await store.Get<CashPayment>("c1"));
    }

    [Fact]
    public async Task Writes_SetTheDiscriminator()
    {
        var store = this.Fixture.CreateStore($"h{Guid.NewGuid():N}", Configure);
        var payment = new CashPayment { Id = "x", Kind = "wrong", Amount = 1 };
        await store.Insert(payment);

        Assert.Equal("cash", payment.Kind);
        Assert.Equal(1, await store.Query<Payment>().Where(x => x.Kind == "cash").Count());
    }

    [Fact]
    public async Task BaseQueryFilters_ApplyToDerivedQueries()
    {
        var store = await SeededAsync();
        Assert.Empty(await store.Query<CashPayment>().Where(x => x.Id == "k2").ToList());
        Assert.Equal(2, await store.Query<CashPayment>().IgnoreQueryFilters().Count());
    }

    [Fact]
    public async Task UpdatesAndSetBasedWrites_StayWithinTheType()
    {
        var store = await SeededAsync();
        var card = (await store.Get<CardPayment>("c1"))!;
        card.Last4 = "0000";
        await store.Update(card);

        var affected = await store.Query<CashPayment>().ExecuteUpdate(x => x.Amount, 99m);

        Assert.Equal(1, affected);
        Assert.Equal("0000", (await store.Get<CardPayment>("c1"))!.Last4);
        Assert.Equal(10, (await store.Get<Payment>("c1"))!.Amount);
        Assert.Equal(99, (await store.Get<Payment>("k1"))!.Amount);
    }

    [Fact]
    public async Task Remove_ThroughADerivedType()
    {
        var store = await SeededAsync();
        Assert.True(await store.Remove<CardPayment>("c1"));
        Assert.Null(await store.Get<Payment>("c1"));
        Assert.Equal(2, await store.Query<Payment>().Count());
    }
}

public class HierarchyUnitTests
{
    public class Animal
    {
        public string Id { get; set; } = "";
        public string Type { get; set; } = "";
    }

    public class Dog : Animal;

    public class Rock
    {
        public string Id { get; set; } = "";
        public string Kind { get; } = "";
    }

    [Fact]
    public void DerivedTypes_StoreUnderTheBaseTypeName()
    {
        var options = new DocumentStoreOptions { DatabaseProvider = new Shiny.DocumentDb.Sqlite.SqliteDatabaseProvider("Data Source=:memory:") };
        options.ConfigureDocument<Animal>(cfg => cfg.MapDerivedTypes(x => x.Type, d => d.Add<Dog>("dog"), baseValue: "animal"));

        Assert.Equal("Animal", Internal.TypeNameResolver.Resolve(typeof(Dog), TypeNameResolution.ShortName));
    }

    [Fact]
    public void ReadOnlyDiscriminator_IsRejected()
    {
        var options = new DocumentStoreOptions { DatabaseProvider = new Shiny.DocumentDb.Sqlite.SqliteDatabaseProvider("Data Source=:memory:") };
        Assert.Throws<ArgumentException>(() => options.ConfigureDocument<Rock>(cfg => cfg.MapDerivedTypes(x => x.Kind, d => { })));
    }
}
