using System.Text.Json.Nodes;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging.Abstractions;
using Shiny.DocumentDb;
using Shiny.DocumentDb.Sqlite;
using ShinyDocDbMyAdmin.Models;
using ShinyDocDbMyAdmin.Services;

namespace ShinyDocDbMyAdmin.Tests;

/// <summary>
/// The filter console: DocumentDb's string grammar, run through the library's own parser and
/// translator rather than a second implementation living in this tool.
/// </summary>
/// <remarks>
/// Worth exercising against a real database because the interesting parts are exactly the ones a unit
/// test over strings cannot reach — that the grammar compiles to SQL the engine accepts, that the
/// <c>:type</c> hint changes the answer, and that pointing a store at someone else's database does not
/// quietly create anything in it.
/// </remarks>
public sealed class FilterConsoleIntegrationTests : IAsyncLifetime
{
    const string Table = "documents";
    const string TypeName = "Order";

    string directory = "";
    string databasePath = "";
    string profileId = "";
    DocumentAdminService admin = null!;
    ConnectionManager connections = null!;

    public async ValueTask InitializeAsync()
    {
        this.directory = Path.Combine(Path.GetTempPath(), $"docdbfilter_{Guid.NewGuid():N}");
        Directory.CreateDirectory(this.directory);
        this.databasePath = Path.Combine(this.directory, "store.db");

        await this.Seed();

        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["ShinyDocDbMyAdmin:DataDirectory"] = Path.Combine(this.directory, "admin"),
                ["ShinyDocDbMyAdmin:SecretKey"] = "test-key",
                ["ConnectionStrings:store"] = $"Data Source={this.databasePath}",
                ["Shiny:DocumentDb:store:Provider"] = "Sqlite"
            })
            .Build();

        var paths = new AppPaths(configuration);
        var protector = new SecretProtector(configuration, paths, NullLogger<SecretProtector>.Instance);
        var provided = new ProvidedConnections(configuration, NullLogger<ProvidedConnections>.Instance);
        var profiles = new ProfileStore(paths, protector, provided);

        this.profileId = provided.Profiles.Single().Id;
        this.connections = new ConnectionManager(profiles, NullLogger<ConnectionManager>.Instance);
        this.admin = new DocumentAdminService(this.connections, NullLogger<DocumentAdminService>.Instance);
    }

    async Task Seed()
    {
        using var store = new DocumentStore(new DocumentStoreOptions
        {
            DatabaseProvider = new SqliteDatabaseProvider($"Data Source={this.databasePath}"),
            TableName = Table
        });

        var orders = store.Collection(TypeName);
        await orders.Insert(new JsonObject { ["id"] = "o1", ["status"] = "Shipped", ["total"] = 120, ["customer"] = new JsonObject { ["name"] = "bob" } });
        await orders.Insert(new JsonObject { ["id"] = "o2", ["status"] = "Shipped", ["total"] = 9, ["customer"] = new JsonObject { ["name"] = "alice" } });
        await orders.Insert(new JsonObject { ["id"] = "o3", ["status"] = "Pending", ["total"] = 100, ["customer"] = new JsonObject { ["name"] = "carol" } });
        await orders.Insert(new JsonObject { ["id"] = "o4", ["status"] = "Shipped", ["total"] = 450, ["customer"] = new JsonObject { ["name"] = "bob" } });
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

    Task<FilterResult> Run(FilterQuery query) =>
        this.admin.ExecuteFilter(this.profileId, Table, TypeName, query);

    static IEnumerable<string> Ids(FilterResult result) =>
        result.Rows.Select(r => r["id"]!.GetValue<string>());

    [Fact]
    public async Task NoFilter_ReturnsEveryDocumentOfTheType()
    {
        var result = await this.Run(new FilterQuery());

        Assert.Equal(4, result.Rows.Count);
        Assert.Equal(4, result.TotalCount);
    }

    [Fact]
    public async Task Where_FiltersOnAStringField()
    {
        var result = await this.Run(new FilterQuery(Where: "status == 'Shipped'"));

        Assert.Equal(["o1", "o2", "o4"], Ids(result).Order());
    }

    [Fact]
    public async Task Where_FiltersOnANestedPath()
    {
        var result = await this.Run(new FilterQuery(Where: "customer.name == 'bob'"));

        Assert.Equal(["o1", "o4"], Ids(result).Order());
    }

    [Fact]
    public async Task Where_CombinesConditions()
    {
        var result = await this.Run(new FilterQuery(Where: "status == 'Shipped' and total:number > 100"));

        Assert.Equal(["o1", "o4"], Ids(result).Order());
    }

    [Fact]
    public async Task AHintedOrderBy_SortsNumerically()
    {
        var hinted = await this.Run(new FilterQuery(OrderBy: "total:number desc"));

        Assert.Equal(["o4", "o1", "o3", "o2"], Ids(hinted));
    }

    [Fact]
    public async Task OnSqlite_TheHintChangesNothing_WhichIsWhyTheConsoleSaysToUseItAnyway()
    {
        // SQLite's json_extract returns a number, so both forms sort the same here and this test cannot
        // demonstrate the trap. It exists to pin the SQLite half of a per-provider difference: on the
        // providers whose plain extract returns text, the unhinted form sorts lexicographically and puts
        // "9" after "450". Hence the console's advice to hint regardless of the backend in front of you.
        var hinted = await this.Run(new FilterQuery(OrderBy: "total:number desc"));
        var bare = await this.Run(new FilterQuery(OrderBy: "total desc"));

        Assert.Equal(Ids(hinted), Ids(bare));
    }

    [Fact]
    public async Task Count_IsOfEveryMatch_NotOfThePage()
    {
        var result = await this.Run(new FilterQuery(Where: "status == 'Shipped'", Take: 2));

        Assert.Equal(2, result.Rows.Count);
        Assert.Equal(3, result.TotalCount);
        Assert.True(result.PageFull);
    }

    [Fact]
    public async Task Project_NarrowsTheColumnsAtTheDatabase()
    {
        var result = await this.Run(new FilterQuery(Project: "status, upper(status) as shout"));

        Assert.Contains("shout", result.Columns);
        Assert.DoesNotContain("customer", result.Columns);
        Assert.Contains(result.Rows, r => r["shout"]!.GetValue<string>() == "SHIPPED");
    }

    [Fact]
    public async Task TheCompiledSql_IsReturned_AndBindsValuesAsParameters()
    {
        var result = await this.Run(new FilterQuery(Where: "status == 'Shipped'"));

        Assert.Contains("SELECT", result.Sql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains(Table, result.Sql);

        // The literal is bound, not concatenated in - which is the thing worth showing the user.
        Assert.Contains(result.Parameters, p => Equals(p.Value, "Shipped"));
        Assert.DoesNotContain("'Shipped'", result.Sql);
    }

    [Fact]
    public async Task Columns_AreDiscoveredFromTheDocuments()
    {
        var result = await this.Run(new FilterQuery(Where: "id == 'o1'"));

        Assert.Equal(["id", "status", "total", "customer"], result.Columns);
    }

    [Fact]
    public async Task AGrammarError_SurfacesTheParsersOwnMessage()
    {
        var ex = await Assert.ThrowsAnyAsync<Exception>(
            () => this.Run(new FilterQuery(Where: "status === 'Shipped'")));

        Assert.False(string.IsNullOrWhiteSpace(ex.Message));
    }

    [Fact]
    public async Task ADottedTypeName_IsRefusedWithAnActionableMessage()
    {
        // TypeNameResolution.FullName writes dotted type names. The tool can browse them; the
        // collection lane validates names to ^[A-Za-z_][A-Za-z0-9_]*$ and will not address them.
        var ex = await Assert.ThrowsAsync<InvalidOperationException>(
            () => this.admin.ExecuteFilter(this.profileId, Table, "MyApp.Models.Order", new FilterQuery()));

        Assert.Contains("TypeNameResolution.FullName", ex.Message);
        Assert.Contains("SQL console", ex.Message);
    }

    [Fact]
    public async Task TheConsole_NeverCreatesATable()
    {
        // The store the console opens sets SkipTableInitialization, so querying a table that is not
        // there fails instead of bringing one into existence behind the user's back.
        await Assert.ThrowsAnyAsync<Exception>(
            () => this.admin.ExecuteFilter(this.profileId, "ghost_table", TypeName, new FilterQuery()));

        await using var connection = new SqliteConnection($"Data Source={this.databasePath}");
        await connection.OpenAsync();
        await using var cmd = connection.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM sqlite_master WHERE name = 'ghost_table';";
        Assert.Equal(0L, Convert.ToInt64(await cmd.ExecuteScalarAsync()));
    }

    // ── Generation ──────────────────────────────────────────────────────

    [Fact]
    public async Task Analyze_LearnsTheShapeFromTheRealDocuments()
    {
        var shape = await this.admin.AnalyzeShape(this.profileId, Table, TypeName);

        Assert.Equal(4, shape.Samples);
        Assert.Equal("id", shape.IdProperty);
        Assert.Equal(["id", "status", "total", "customer"], shape.Fields.Select(f => f.Name));

        // The four seeded orders use two distinct statuses, so that is the set to draw from.
        var status = shape.Fields.Single(f => f.Name == "status");
        Assert.Equal("one of 2", status.Strategy);
        Assert.Equal(["Shipped", "Pending"], status.Shape.Values.Select(v => v.GetValue<string>()));

        // total spans 9 .. 450 across the fixture, and the range is what generation draws from.
        Assert.Equal("number 9 … 450", shape.Fields.Single(f => f.Name == "total").Strategy);
    }

    [Fact]
    public async Task AnalyzingATypeWithNoDocuments_SaysWhyRatherThanInventingASchema()
    {
        var ex = await Assert.ThrowsAsync<InvalidOperationException>(
            () => this.admin.AnalyzeShape(this.profileId, Table, "Nothing"));

        Assert.Contains("copies the shape", ex.Message);
    }

    [Fact]
    public async Task GeneratedRecords_AreWrittenAndAreQueryable()
    {
        var shape = await this.admin.AnalyzeShape(this.profileId, Table, TypeName);
        var preview = DocumentAdminService.Preview(shape, 200);

        var result = await this.admin.Commit(this.profileId, Table, TypeName, preview);

        Assert.Equal(200, result.Written);
        Assert.Equal(204, (await this.Run(new FilterQuery())).TotalCount);

        // And they are real documents, not blobs the grammar cannot see.
        var shipped = await this.Run(new FilterQuery(Where: "status == 'Shipped'"));
        Assert.True(shipped.TotalCount > 1);
    }

    [Fact]
    public async Task GeneratedValues_StayInsideTheObservedRange()
    {
        var shape = await this.admin.AnalyzeShape(this.profileId, Table, TypeName);
        await this.admin.Commit(this.profileId, Table, TypeName, DocumentAdminService.Preview(shape, 300));

        // The seeded totals span 10 .. 450; nothing generated may fall outside that.
        var outside = await this.Run(new FilterQuery(Where: "total:number < 9 or total:number > 451"));

        Assert.Equal(0, outside.TotalCount);
    }

    [Fact]
    public async Task CommitReproducesExactlyWhatThePreviewShowed()
    {
        var shape = await this.admin.AnalyzeShape(this.profileId, Table, TypeName);
        var preview = DocumentAdminService.Preview(shape, 10);

        await this.admin.Commit(this.profileId, Table, TypeName, preview);

        var first = preview.Sample[0];
        var stored = await this.admin.GetDocument(
            this.profileId, Table, TypeName, first["id"]!.GetValue<string>());

        Assert.NotNull(stored);
        Assert.Equal(
            first["status"]!.GetValue<string>(),
            JsonNode.Parse(stored.Json)!["status"]!.GetValue<string>());
    }

    [Fact]
    public async Task GeneratedIdsDoNotCollideWithTheExistingOnes()
    {
        var shape = await this.admin.AnalyzeShape(this.profileId, Table, TypeName);

        // Would throw on a duplicate id if the sequence restarted at o1.
        var result = await this.admin.Commit(this.profileId, Table, TypeName, DocumentAdminService.Preview(shape, 50));

        Assert.Equal(50, result.Written);
    }

    [Fact]
    public async Task NoSidecarsOnThisType_MeansNothingIsReportedAsStale()
    {
        var shape = await this.admin.AnalyzeShape(this.profileId, Table, TypeName);

        var result = await this.admin.Commit(this.profileId, Table, TypeName, DocumentAdminService.Preview(shape, 5));

        Assert.Empty(result.StaleSidecars);
    }

    [Fact]
    public async Task TheGrammarAgreesWithTheLibrary()
    {
        // The whole justification for going through IDocumentStore here rather than hand-rolling the
        // grammar: the console must answer what the application would.
        const string filter = "status == 'Shipped' and total:number > 100";

        var console = await this.Run(new FilterQuery(Where: filter, OrderBy: "total:number desc"));

        using var store = new DocumentStore(new DocumentStoreOptions
        {
            DatabaseProvider = new SqliteDatabaseProvider($"Data Source={this.databasePath}"),
            TableName = Table
        });
        var library = await store.Collection(TypeName).Query()
            .Where(filter)
            .OrderBy("total:number desc")
            .ToList();

        Assert.Equal(library.Select(r => r["id"]!.GetValue<string>()), Ids(console));
    }
}
