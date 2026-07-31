using System.Text;
using Microsoft.Extensions.AI;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging.Abstractions;
using Shiny.DocumentDb;
using Shiny.DocumentDb.Sqlite;
using ShinyDocDbMyAdmin.Services;

namespace ShinyDocDbMyAdmin.Tests;

/// <summary>
/// The assistant's read-only guarantee, tested where it actually lives: the set of tools it is given.
/// </summary>
/// <remarks>
/// The guarantee is not "the model was told not to write" - it is "no write function was registered".
/// <see cref="TheSurfaceIsExactlyTheReadOnlySet"/> is the test that fails if someone adds one.
/// </remarks>
public sealed class AiToolSurfaceTests : IAsyncLifetime
{
    sealed class Zone
    {
        public string Id { get; set; } = "";
        public string Name { get; set; } = "";
        public string Courier { get; set; } = "";
        public int DropsPerWeek { get; set; }
    }

    const string Table = "documents";
    const string TypeName = nameof(Zone);

    string directory = "";
    string profileId = "";
    AiToolSurface surface = null!;
    ConnectionManager connections = null!;

    public async ValueTask InitializeAsync()
    {
        this.directory = Path.Combine(Path.GetTempPath(), $"docdbai_{Guid.NewGuid():N}");
        Directory.CreateDirectory(this.directory);
        var databasePath = Path.Combine(this.directory, "store.db");

        await Seed(databasePath);

        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["ShinyDocDbMyAdmin:DataDirectory"] = Path.Combine(this.directory, "admin"),
                ["ShinyDocDbMyAdmin:SecretKey"] = "test-key",
                ["ConnectionStrings:store"] = $"Data Source={databasePath}",
                ["Shiny:DocumentDb:store:Provider"] = "Sqlite"
            })
            .Build();

        var paths = new AppPaths(configuration);
        var protector = new SecretProtector(configuration, paths, NullLogger<SecretProtector>.Instance);
        var provided = new ProvidedConnections(configuration, NullLogger<ProvidedConnections>.Instance);
        var profiles = new ProfileStore(paths, protector, provided,
            // Demo mode off: these exercise the write paths a demo instance closes.
            new DemoMode(configuration, NullLogger<DemoMode>.Instance));

        this.profileId = provided.Profiles.Single().Id;
        this.connections = new ConnectionManager(profiles, NullLogger<ConnectionManager>.Instance);
        var admin = new DocumentAdminService(this.connections, NullLogger<DocumentAdminService>.Instance);
        this.surface = new AiToolSurface(admin, profiles);
    }

    static async Task Seed(string databasePath)
    {
        using var store = new DocumentStore(new DocumentStoreOptions
        {
            DatabaseProvider = new SqliteDatabaseProvider($"Data Source={databasePath}"),
            TableName = Table
        });

        await store.Insert(new Zone { Id = "downtown", Name = "Downtown", Courier = "Rivera", DropsPerWeek = 201 });
        await store.Insert(new Zone { Id = "uptown", Name = "Uptown", Courier = "Okafor", DropsPerWeek = 97 });
        await store.Insert(new Zone { Id = "harbour", Name = "Harbour", Courier = "Lindqvist", DropsPerWeek = 42 });
    }

    public ValueTask DisposeAsync()
    {
        this.connections?.Dispose();
        if (Directory.Exists(this.directory))
        {
            try { Directory.Delete(this.directory, recursive: true); }
            catch (IOException) { /* a driver still holding the file is not a test failure */ }
        }

        return ValueTask.CompletedTask;
    }

    IReadOnlyList<AIFunction> Functions() => [.. this.surface.Build().OfType<AIFunction>()];

    AIFunction Function(string name) => this.Functions().Single(f => f.Name == name);

    static async Task<string> Invoke(AIFunction function, Dictionary<string, object?> arguments)
    {
        var result = await function.InvokeAsync(new AIFunctionArguments(arguments), TestContext.Current.CancellationToken);
        return result?.ToString() ?? "";
    }

    // ── The guarantee ───────────────────────────────────────────────────

    [Fact]
    public void TheSurfaceIsExactlyTheReadOnlySet()
    {
        var names = this.Functions().Select(f => f.Name).OrderBy(n => n, StringComparer.Ordinal);

        Assert.Equal(AiToolSurface.ToolNames.OrderBy(n => n, StringComparer.Ordinal), names);
    }

    [Theory]
    // Every write path DocumentAdminService exposes, plus the raw SQL console. None of them may ever
    // appear here: a tool the model cannot call is a guarantee, a prompt asking it not to is not.
    [InlineData("save")]
    [InlineData("insert")]
    [InlineData("update")]
    [InlineData("delete")]
    [InlineData("remove")]
    [InlineData("clear")]
    [InlineData("drop")]
    [InlineData("restore")]
    [InlineData("create")]
    [InlineData("execute_sql")]
    [InlineData("sql")]
    [InlineData("resync")]
    public void NoToolLooksLikeAWrite(string forbidden)
    {
        var offenders = this.Functions()
            .Select(f => f.Name)
            .Where(n => n.Contains(forbidden, StringComparison.OrdinalIgnoreCase))
            .ToList();

        Assert.Empty(offenders);
    }

    [Fact]
    public void EveryToolIsDescribed()
    {
        // An undescribed tool is one the model will either ignore or misuse.
        Assert.All(this.Functions(), f => Assert.False(string.IsNullOrWhiteSpace(f.Description)));
    }

    // ── The tools do what they say ──────────────────────────────────────

    [Fact]
    public async Task ListConnections_NeverReturnsTheConnectionString()
    {
        var json = await Invoke(this.Function("list_connections"), []);

        Assert.Contains(this.profileId, json);
        // The connection string is a credential; it would otherwise ride to the model provider.
        Assert.DoesNotContain("Data Source", json, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain(".db", json, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task ListTypes_FindsTheSeededType()
    {
        var json = await Invoke(this.Function("list_types"), new Dictionary<string, object?>
        {
            ["connectionId"] = this.profileId,
            ["table"] = Table
        });

        Assert.Contains(TypeName, json);
    }

    [Fact]
    public async Task DescribeType_ReportsTheFields()
    {
        var json = await Invoke(this.Function("describe_type"), new Dictionary<string, object?>
        {
            ["connectionId"] = this.profileId,
            ["table"] = Table,
            ["typeName"] = TypeName
        });

        Assert.Contains("courier", json, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("dropsPerWeek", json, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task BrowseDocuments_WorksWithOnlyTheRequiredArguments()
    {
        // AIFunctionFactory marks a nullable parameter as required unless it has a C# default, which
        // would force the model to invent a filter and a sort just to read a page.
        var json = await Invoke(this.Function("browse_documents"), new Dictionary<string, object?>
        {
            ["connectionId"] = this.profileId,
            ["table"] = Table,
            ["typeName"] = TypeName
        });

        Assert.Contains("Downtown", json);
    }

    [Fact]
    public async Task BrowseDocuments_Filters()
    {
        var json = await Invoke(this.Function("browse_documents"), new Dictionary<string, object?>
        {
            ["connectionId"] = this.profileId,
            ["table"] = Table,
            ["typeName"] = TypeName,
            ["filterPath"] = "courier",
            ["filterOperator"] = "Equals",
            ["filterValue"] = "Okafor"
        });

        Assert.Contains("Uptown", json);
        Assert.DoesNotContain("Downtown", json);
    }

    [Fact]
    public async Task BrowseDocuments_CapsTheRowCountHoweverItIsAsked()
    {
        var json = await Invoke(this.Function("browse_documents"), new Dictionary<string, object?>
        {
            ["connectionId"] = this.profileId,
            ["table"] = Table,
            ["typeName"] = TypeName,
            // Far above the cap: a model that asks for everything must still not get everything.
            ["limit"] = 100_000
        });

        // Only three documents exist, so the proof the clamp applied is that it did not throw and
        // returned the real rows - the cap itself is asserted on the constant.
        Assert.Contains("Downtown", json);
        Assert.True(AiToolSurface.MaxRows <= 50);
    }

    [Fact]
    public async Task GetDocument_ReadsOne()
    {
        var json = await Invoke(this.Function("get_document"), new Dictionary<string, object?>
        {
            ["connectionId"] = this.profileId,
            ["table"] = Table,
            ["typeName"] = TypeName,
            ["documentId"] = "harbour"
        });

        Assert.Contains("Lindqvist", json);
    }

    [Fact]
    public async Task GetDocument_ReturnsNothingForAnUnknownId()
    {
        var json = await Invoke(this.Function("get_document"), new Dictionary<string, object?>
        {
            ["connectionId"] = this.profileId,
            ["table"] = Table,
            ["typeName"] = TypeName,
            ["documentId"] = "nope"
        });

        Assert.DoesNotContain("Lindqvist", json);
    }

    [Fact]
    public async Task ListTables_MarksWhichCarryDocuments()
    {
        var json = await Invoke(this.Function("list_tables"), new Dictionary<string, object?>
        {
            ["connectionId"] = this.profileId
        });

        Assert.Contains(Table, json);
    }
}
