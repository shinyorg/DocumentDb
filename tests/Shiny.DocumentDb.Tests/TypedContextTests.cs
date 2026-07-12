using Microsoft.Extensions.DependencyInjection;
using Shiny.DocumentDb.Sqlite;
using Shiny.DocumentDb.Tests.Fixtures;
using Xunit;

namespace Shiny.DocumentDb.Tests;

// End-to-end coverage of the source-generated DocumentContext. The SQLite variant runs with
// UseReflectionFallback = false through the generated ConfigureModel so serialization carries with no
// reflection (AOT proof); the LiteDB variant proves the generated sets forward correctly on a different
// provider/store.
public abstract class TypedContextTestsBase : IDisposable
{
    protected readonly IDocumentStore store;
    protected readonly SampleDocumentContext db;

    protected TypedContextTestsBase(IDocumentStore store)
    {
        this.store = store;
        this.db = new SampleDocumentContext(store.OpenSession());
    }

    public void Dispose() => (this.store as IDisposable)?.Dispose();

    [Fact]
    public void Generated_sets_are_exposed_with_correct_names()
    {
        // Users (pluralized), Catalog (Set= override), Orders (pluralized) — all DocumentSet<T>.
        Assert.IsType<DocumentSet<User>>(this.db.Users);
        Assert.IsType<DocumentSet<Product>>(this.db.Catalog);
        Assert.IsType<DocumentSet<Order>>(this.db.Orders);
    }

    [Fact]
    public async Task Insert_and_Get_roundtrip()
    {
        await this.db.Users.Insert(new User { Id = "u1", Name = "Allan", Age = 41, Email = "a@x.com" });

        var fetched = await this.db.Users.Get("u1");

        Assert.NotNull(fetched);
        Assert.Equal("Allan", fetched!.Name);
        Assert.Equal(41, fetched.Age);
    }

    [Fact]
    public async Task Where_translates_through_generated_metadata()
    {
        await this.db.Users.Insert(new User { Id = "u1", Name = "Alice", Age = 17 });
        await this.db.Users.Insert(new User { Id = "u2", Name = "Bob", Age = 35 });
        await this.db.Users.Insert(new User { Id = "u3", Name = "Carol", Age = 22 });

        var adults = await this.db.Users.Where(u => u.Age >= 18).ToList();

        Assert.Equal(2, adults.Count);
        Assert.DoesNotContain(adults, u => u.Name == "Alice");
    }

    [Fact]
    public async Task Nested_property_where_resolves()
    {
        await this.db.Orders.Insert(new Order
        {
            Id = "o1",
            CustomerName = "Allan",
            ShippingAddress = new Address { City = "Ottawa", State = "ON" }
        });
        await this.db.Orders.Insert(new Order
        {
            Id = "o2",
            CustomerName = "Bob",
            ShippingAddress = new Address { City = "Toronto", State = "ON" }
        });

        var ottawa = await this.db.Orders.Where(o => o.ShippingAddress.City == "Ottawa").ToList();

        Assert.Single(ottawa);
        Assert.Equal("o1", ottawa[0].Id);
    }

    [Fact]
    public async Task Update_Upsert_Remove()
    {
        await this.db.Users.Insert(new User { Id = "u1", Name = "Al", Age = 40 });
        await this.db.Users.Update(new User { Id = "u1", Name = "Allan", Age = 41 });
        Assert.Equal("Allan", (await this.db.Users.Get("u1"))!.Name);

        await this.db.Users.Upsert(new User { Id = "u1", Name = "Allan R", Age = 41 });
        Assert.Equal("Allan R", (await this.db.Users.Get("u1"))!.Name);

        Assert.True(await this.db.Users.Remove("u1"));
        Assert.Null(await this.db.Users.Get("u1"));
    }

    [Fact]
    public async Task Set_override_routes_to_its_own_type()
    {
        await this.db.Catalog.Insert(new Product { Id = "p1", Title = "Widget", Price = 9.99m });

        var p = await this.db.Catalog.Get("p1");

        Assert.NotNull(p);
        Assert.Equal("Widget", p!.Title);
        Assert.Equal(9.99m, p.Price);
    }

    [Fact]
    public async Task Count_reflects_inserts()
    {
        await this.db.Users.BatchInsert(new[]
        {
            new User { Id = "u1", Name = "A" },
            new User { Id = "u2", Name = "B" }
        });

        Assert.Equal(2, await this.db.Users.Count());
    }

    [Fact]
    public async Task UnitOfWork_commits_atomically()
    {
        var uow = this.db.Session;
        uow.Add(new User { Id = "u1", Name = "A" });
        uow.Add(new User { Id = "u2", Name = "B" });
        Assert.Equal(0, await this.db.Users.Count());   // not visible until SaveChanges

        await uow.SaveChanges();

        Assert.Equal(2, await this.db.Users.Count());
    }
}

[Collection("SQLite")]
public class SqliteTypedContextTests : TypedContextTestsBase
{
    public SqliteTypedContextTests(SqliteDatabaseFixture fixture) : base(Build(fixture)) { }

    // Relational path: generated ConfigureModel lowers [Document] mappings + chains the JsonContext, and
    // strict mode forces the AOT-safe resolve.
    static IDocumentStore Build(SqliteDatabaseFixture fixture)
    {
        var opts = new DocumentStoreOptions
        {
            DatabaseProvider = fixture.CreateProvider(),
            TableName = $"t{Guid.NewGuid():N}",
            UseReflectionFallback = false
        };
        SampleDocumentContext.ConfigureModel(opts);
        return new DocumentStore(opts);
    }
}

[Collection("LiteDB")]
public class LiteDbTypedContextTests : TypedContextTestsBase
{
    // Non-relational store: the generated sets forward over IDocumentStore unchanged (reflection serialization).
    public LiteDbTypedContextTests(LiteDbDatabaseFixture fixture) : base(fixture.CreateStore($"t{Guid.NewGuid():N}")) { }
}

// The generated AddSampleDocumentContext DI extension wires ConfigureModel + the scoped context.
public class TypedContextDiTests
{
    [Fact]
    public async Task Generated_DI_extension_registers_and_resolves()
    {
        var services = new ServiceCollection();
        services.AddSampleDocumentContext(o =>
        {
            o.DatabaseProvider = new SqliteDatabaseProvider("Data Source=:memory:");
            o.TableName = $"t{Guid.NewGuid():N}";
            o.UseReflectionFallback = false;
        });

        await using var provider = services.BuildServiceProvider();
        using var scope = provider.CreateScope();
        var db = scope.ServiceProvider.GetRequiredService<SampleDocumentContext>();

        await db.Users.Insert(new User { Id = "u1", Name = "DI", Age = 1 });
        var fetched = await db.Users.Get("u1");

        Assert.NotNull(fetched);
        Assert.Equal("DI", fetched!.Name);
    }
}
