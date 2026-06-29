using Shiny.DocumentDb.Tests.Fixtures;
using Xunit;

namespace Shiny.DocumentDb.Tests;

// DocumentSerialization.Generated end-to-end on SQLite with UseReflectionFallback = false: the generator's
// own metadata resolver (no hand-written JsonSerializerContext) must carry serialization AND drive the query
// translator with zero reflection.
[Collection("SQLite")]
public class GeneratedContextTests : IDisposable
{
    readonly IDocumentStore store;
    readonly GeneratedDocumentContext db;

    public GeneratedContextTests(SqliteDatabaseFixture fixture)
    {
        var opts = new DocumentStoreOptions
        {
            DatabaseProvider = fixture.CreateProvider(),
            TableName = $"t{Guid.NewGuid():N}",
            UseReflectionFallback = false
        };
        GeneratedDocumentContext.ConfigureModel(opts);   // chains the generated resolver
        this.store = new DocumentStore(opts);
        this.db = new GeneratedDocumentContext(this.store);
    }

    public void Dispose() => (this.store as IDisposable)?.Dispose();

    [Fact]
    public async Task Simple_type_roundtrips_with_generated_metadata()
    {
        await this.db.Users.Insert(new User { Id = "u1", Name = "Allan", Age = 41, Email = "a@x.com" });

        var u = await this.db.Users.Get("u1");
        Assert.Equal("Allan", u!.Name);
        Assert.Equal("a@x.com", u.Email);

        var adults = await this.db.Users.Where(x => x.Age >= 18).ToList();
        Assert.Single(adults);
    }

    [Fact]
    public async Task Complex_type_roundtrips_every_member_kind()
    {
        var doc = new GenModel
        {
            Id = "g1",
            Count = 3,
            OptionalCount = 7,
            Level = Priority.High,
            Price = 19.95m,
            CreatedAt = new DateTimeOffset(2026, 6, 29, 10, 0, 0, TimeSpan.Zero),
            Tags = ["a", "b"],
            Scores = [10, 20, 30],
            Home = new Address { City = "Ottawa", State = "ON" }
        };
        await this.db.GenModels.Insert(doc);   // GenModel set name = GenModels (pluralized)

        var fetched = await this.db.GenModels.Get("g1");

        Assert.NotNull(fetched);
        Assert.Equal(3, fetched!.Count);
        Assert.Equal(7, fetched.OptionalCount);
        Assert.Equal(Priority.High, fetched.Level);
        Assert.Equal(19.95m, fetched.Price);
        Assert.Equal(new DateTimeOffset(2026, 6, 29, 10, 0, 0, TimeSpan.Zero), fetched.CreatedAt);
        Assert.Equal(["a", "b"], fetched.Tags);
        Assert.Equal([10, 20, 30], fetched.Scores);
        Assert.Equal("Ottawa", fetched.Home!.City);
    }

    [Fact]
    public async Task Null_nullable_roundtrips()
    {
        await this.db.GenModels.Insert(new GenModel { Id = "g2", Count = 1, OptionalCount = null, Home = null });

        var fetched = await this.db.GenModels.Get("g2");

        Assert.NotNull(fetched);
        Assert.Null(fetched!.OptionalCount);
        Assert.Null(fetched.Home);
    }

    [Fact]
    public async Task Query_by_primitive_enum_and_nested_property()
    {
        await this.db.GenModels.Insert(new GenModel { Id = "g1", Count = 5, Level = Priority.High, Home = new Address { City = "Ottawa" } });
        await this.db.GenModels.Insert(new GenModel { Id = "g2", Count = 1, Level = Priority.Low, Home = new Address { City = "Toronto" } });

        Assert.Single(await this.db.GenModels.Where(x => x.Count >= 3).ToList());
        Assert.Single(await this.db.GenModels.Where(x => x.Level == Priority.High).ToList());

        var ottawa = await this.db.GenModels.Where(x => x.Home!.City == "Ottawa").ToList();
        Assert.Single(ottawa);
        Assert.Equal("g1", ottawa[0].Id);
    }
}
