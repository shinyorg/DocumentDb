using System.Diagnostics.CodeAnalysis;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Text.Json.Serialization.Metadata;
using Shiny.DocumentDb.Tests.Fixtures;
using Xunit;

namespace Shiny.DocumentDb.Tests;

// DEFERRED-BUGS #8 on MongoDB: MongoExpressionVisitor unconditionally boxed enums to int, so a string-stored
// enum (JsonStringEnumConverter) matched nothing. It now binds the member-name string the write path persisted.
[Collection("MongoDB")]
public class MongoEnumStringStorageTests : IDisposable
{
    sealed class Ticket
    {
        public string Id { get; set; } = "";
        public Priority Level { get; set; }
    }

    readonly IDocumentStore store;

    [UnconditionalSuppressMessage("Trimming", "IL2026",
        Justification = "Test fixture: the reflection resolver is the subject under test — this suite verifies the "
                      + "string-stored-enum lowering against a reflection-configured store, which is the exact "
                      + "configuration an AOT app would not use.")]
    [UnconditionalSuppressMessage("AOT", "IL3050",
        Justification = "Test fixture; see the IL2026 justification.")]
    public MongoEnumStringStorageTests(MongoDbDatabaseFixture db)
    {
        var jsonOptions = new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
            TypeInfoResolver = new DefaultJsonTypeInfoResolver(),
            Converters = { new JsonStringEnumConverter() }
        };
        this.store = db.CreateStoreWithJsonOptions($"t{Guid.NewGuid():N}", jsonOptions);
    }

    public void Dispose() => (this.store as IDisposable)?.Dispose();

    async Task SeedAsync()
    {
        await this.store.Insert(new Ticket { Id = "t1", Level = Priority.Low });
        await this.store.Insert(new Ticket { Id = "t2", Level = Priority.High });
        await this.store.Insert(new Ticket { Id = "t3", Level = Priority.High });
        await this.store.Insert(new Ticket { Id = "t4", Level = Priority.Normal });
    }

    [Fact]
    public async Task Equality_MatchesStringStoredEnum()
    {
        await this.SeedAsync();
        var high = await this.store.Query<Ticket>().Where(t => t.Level == Priority.High).ToList();
        Assert.Equal(["t2", "t3"], high.Select(t => t.Id).OrderBy(x => x));
    }

    [Fact]
    public async Task In_MatchesStringStoredEnum()
    {
        await this.SeedAsync();
        var wanted = new List<Priority> { Priority.Low, Priority.High };
        var results = await this.store.Query<Ticket>().Where(t => wanted.Contains(t.Level)).ToList();
        Assert.Equal(["t1", "t2", "t3"], results.Select(t => t.Id).OrderBy(x => x));
    }
}
