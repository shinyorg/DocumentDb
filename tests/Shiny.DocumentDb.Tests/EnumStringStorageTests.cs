using System.Text.Json;
using System.Text.Json.Serialization;
using System.Text.Json.Serialization.Metadata;
using Microsoft.Data.Sqlite;
using Shiny.DocumentDb.Sqlite;
using Shiny.DocumentDb.Tests.Fixtures;
using Xunit;

namespace Shiny.DocumentDb.Tests;

// Regression for DEFERRED-BUGS #8: with a JsonStringEnumConverter configured, enums are stored as their member
// name (a JSON string). The relational query pipeline used to always bind an enum comparison as its numeric
// value AND cast the extracted field to an integer, so every enum ==/!=/in predicate matched nothing. The
// lowerer now detects string-stored enums (via EnumJsonStorage) and binds the member-name string while
// extracting the field as text. Covers both the LINQ surface and the string grammar (which lowers to the same
// expression tree).
public class EnumStringStorageTests : IDisposable
{
    sealed class Ticket
    {
        public string Id { get; set; } = "";
        public string Title { get; set; } = "";
        public Priority Level { get; set; }
    }

    readonly SqliteConnection holdOpen;
    readonly DocumentStore store;

    public EnumStringStorageTests()
    {
        var connectionString = $"Data Source=enum_{Guid.NewGuid():N};Mode=Memory;Cache=Shared";
        this.holdOpen = new SqliteConnection(connectionString);
        this.holdOpen.Open();

        var jsonOptions = new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
            TypeInfoResolver = new DefaultJsonTypeInfoResolver(),
            Converters = { new JsonStringEnumConverter() }
        };
        this.store = new DocumentStore(new DocumentStoreOptions
        {
            DatabaseProvider = new SqliteDatabaseProvider(connectionString),
            TableName = $"t{Guid.NewGuid():N}",
            JsonSerializerOptions = jsonOptions
        });
    }

    public void Dispose()
    {
        this.store.Dispose();
        this.holdOpen.Dispose();
    }

    async Task SeedAsync()
    {
        await this.store.Insert(new Ticket { Id = "t1", Title = "A", Level = Priority.Low });
        await this.store.Insert(new Ticket { Id = "t2", Title = "B", Level = Priority.High });
        await this.store.Insert(new Ticket { Id = "t3", Title = "C", Level = Priority.High });
        await this.store.Insert(new Ticket { Id = "t4", Title = "D", Level = Priority.Normal });
    }

    [Fact]
    public async Task Equality_MatchesStringStoredEnum_Linq()
    {
        await this.SeedAsync();

        var high = await this.store.Query<Ticket>().Where(t => t.Level == Priority.High).ToList();
        Assert.Equal(["t2", "t3"], high.Select(t => t.Id).OrderBy(x => x));
    }

    [Fact]
    public async Task Inequality_IncludesEverythingElse_Linq()
    {
        await this.SeedAsync();

        var notHigh = await this.store.Query<Ticket>().Where(t => t.Level != Priority.High).ToList();
        Assert.Equal(["t1", "t4"], notHigh.Select(t => t.Id).OrderBy(x => x));
    }

    [Fact]
    public async Task In_MatchesStringStoredEnum_Linq()
    {
        await this.SeedAsync();

        var wanted = new List<Priority> { Priority.Low, Priority.High };
        var results = await this.store.Query<Ticket>().Where(t => wanted.Contains(t.Level)).ToList();
        Assert.Equal(["t1", "t2", "t3"], results.Select(t => t.Id).OrderBy(x => x));
    }

    [Fact]
    public async Task Equality_MatchesStringStoredEnum_StringGrammar()
    {
        await this.SeedAsync();

        var high = await this.store.Query<Ticket>().Where("Level == 'High'").ToList();
        Assert.Equal(["t2", "t3"], high.Select(t => t.Id).OrderBy(x => x));
    }

    [Fact]
    public async Task Captured_MatchesStringStoredEnum_Linq()
    {
        await this.SeedAsync();

        var wanted = Priority.Normal;
        var results = await this.store.Query<Ticket>().Where(t => t.Level == wanted).ToList();
        Assert.Equal(["t4"], results.Select(t => t.Id));
    }
}
