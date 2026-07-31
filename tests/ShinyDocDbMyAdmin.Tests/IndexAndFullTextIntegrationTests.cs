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
/// Index management, query plans and full-text search against a real database.
/// </summary>
/// <remarks>
/// SQLite is the one backend that exercises all three without a container: it has EXPLAIN QUERY PLAN, a
/// catalog that lists every index, and FTS5. What it does not have is per-index size or scan counters,
/// so those stay null here — the shape of that gap is asserted rather than glossed over.
/// </remarks>
public sealed class IndexAndFullTextIntegrationTests : IAsyncLifetime
{
    sealed class Article
    {
        public string Id { get; set; } = "";
        public string Title { get; set; } = "";
        public string Body { get; set; } = "";
        public string Tier { get; set; } = "";
    }

    const string Table = "documents";
    const string TypeName = nameof(Article);

    string directory = "";
    string databasePath = "";
    string profileId = "";
    DocumentAdminService admin = null!;
    ConnectionManager connections = null!;

    public async ValueTask InitializeAsync()
    {
        this.directory = Path.Combine(Path.GetTempPath(), $"docdbidx_{Guid.NewGuid():N}");
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
        var profiles = new ProfileStore(paths, protector, provided,
            // Demo mode off: these exercise the write paths a demo instance closes.
            new DemoMode(configuration, NullLogger<DemoMode>.Instance));

        this.profileId = provided.Profiles.Single().Id;
        this.connections = new ConnectionManager(profiles, NullLogger<ConnectionManager>.Instance);
        this.admin = new DocumentAdminService(this.connections, NullLogger<DocumentAdminService>.Instance);
    }

    /// <summary>Writes through the library so the FTS5 table and its triggers are created for real.</summary>
    async Task Seed()
    {
        var options = new DocumentStoreOptions
        {
            DatabaseProvider = new SqliteDatabaseProvider($"Data Source={this.databasePath}"),
            TableName = Table
        };
        options.MapFullTextProperty<Article>([a => a.Title, a => a.Body]);

        // Temporal too, so the "what can generation not maintain" tests have a real history sidecar
        // with rows for this type rather than an empty table.
        options.MapTemporal<Article>();

        using var store = new DocumentStore(options);
        await store.Insert(new Article { Id = "a1", Title = "Orleans persistence", Body = "grain storage over documents", Tier = "gold" });
        await store.Insert(new Article { Id = "a2", Title = "Vector search", Body = "embeddings and nearest neighbours", Tier = "gold" });
        await store.Insert(new Article { Id = "a3", Title = "Spatial queries", Body = "polygons, orleans and geography", Tier = "silver" });
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

    // ── Index creation and listing ──────────────────────────────────────

    [Fact]
    public async Task ASinglePathIndex_IsCreatedAndListed()
    {
        await this.admin.CreateIndex(this.profileId, Table, TypeName, "tier");

        var index = Assert.Single(await this.admin.ListIndexes(this.profileId, Table));
        Assert.Equal("idx_json_Article_tier", index.Name);
        Assert.Equal(TypeName, index.TypeName);
        Assert.Equal(["tier"], index.Paths);
        Assert.False(index.IsComposite);
    }

    [Fact]
    public async Task ACompositeIndex_IsCreated_AndItsPathsAreRecoveredFromTheName()
    {
        await this.admin.CreateIndex(this.profileId, Table, TypeName, ["tier", "title"]);

        var index = Assert.Single(await this.admin.ListIndexes(this.profileId, Table));
        Assert.Equal("idx_json_Article_tier__title", index.Name);
        Assert.Equal(["tier", "title"], index.Paths);
        Assert.True(index.IsComposite);
        Assert.Equal("tier, title", index.Path);
    }

    [Fact]
    public async Task ListAllIndexes_ShowsIndexesThisToolDidNotCreate()
    {
        await this.admin.CreateIndex(this.profileId, Table, TypeName, "tier");
        await this.ExecuteRaw("CREATE INDEX handmade_typename_id ON documents (TypeName, Id);");

        var all = await this.admin.ListAllIndexes(this.profileId, Table);

        var ours = Assert.Single(all, i => i.Name == "idx_json_Article_tier");
        Assert.True(ours.IsDocumentDb);
        Assert.Equal(TypeName, ours.Json!.TypeName);

        var theirs = Assert.Single(all, i => i.Name == "handmade_typename_id");
        Assert.False(theirs.IsDocumentDb);
        Assert.Null(theirs.Json);
        Assert.Contains("TypeName", theirs.Definition);
    }

    [Fact]
    public async Task ListAllIndexes_ReportsNoSizeOrScans_OnAnEngineThatKeepsNeither()
    {
        await this.admin.CreateIndex(this.profileId, Table, TypeName, "tier");

        var index = Assert.Single(await this.admin.ListAllIndexes(this.profileId, Table), i => i.IsDocumentDb);

        // SQLite tracks neither, and the tool says so rather than showing a fabricated zero.
        Assert.Null(index.SizeBytes);
        Assert.Null(index.Scans);
    }

    [Fact]
    public async Task ADroppedIndex_LeavesTheListing()
    {
        await this.admin.CreateIndex(this.profileId, Table, TypeName, "tier");
        await this.admin.DropIndex(this.profileId, Table, "idx_json_Article_tier");

        Assert.Empty(await this.admin.ListIndexes(this.profileId, Table));
    }

    [Fact]
    public async Task APathThatIsNotAPlainDottedIdentifier_IsRefused()
    {
        // The path is interpolated into DDL, not bound, so anything odd is refused outright.
        await Assert.ThrowsAsync<ArgumentException>(
            () => this.admin.CreateIndex(this.profileId, Table, TypeName, "tier); DROP TABLE documents;--"));
    }

    // ── Suggestions ─────────────────────────────────────────────────────

    [Fact]
    public async Task UnindexedFieldsInAQuery_AreSuggested()
    {
        var suggestions = await this.admin.SuggestIndexPaths(
            this.profileId, Table, TypeName, new FilterQuery(Where: "tier == 'gold'"));

        Assert.Equal(["tier"], suggestions);
    }

    [Fact]
    public async Task AnAlreadyIndexedField_IsNotSuggested()
    {
        await this.admin.CreateIndex(this.profileId, Table, TypeName, "tier");

        var suggestions = await this.admin.SuggestIndexPaths(
            this.profileId, Table, TypeName, new FilterQuery(Where: "tier == 'gold'"));

        Assert.Empty(suggestions);
    }

    [Fact]
    public async Task AFieldCoveredByACompositeIndex_IsNotSuggested()
    {
        await this.admin.CreateIndex(this.profileId, Table, TypeName, ["tier", "title"]);

        var suggestions = await this.admin.SuggestIndexPaths(
            this.profileId, Table, TypeName, new FilterQuery(Where: "tier == 'gold' and title == 'x'"));

        Assert.Empty(suggestions);
    }

    [Fact]
    public async Task OrderByFields_AreSuggestedToo()
    {
        var suggestions = await this.admin.SuggestIndexPaths(
            this.profileId, Table, TypeName, new FilterQuery(OrderBy: "title desc"));

        Assert.Equal(["title"], suggestions);
    }

    // ── Query plans ─────────────────────────────────────────────────────

    [Fact]
    public async Task Explain_ReturnsAPlan()
    {
        var result = await this.admin.ExecuteFilter(this.profileId, Table, TypeName, new FilterQuery(Where: "tier == 'gold'"));

        var plan = await this.admin.Explain(this.profileId, result.Sql, result.Parameters);

        Assert.True(plan.Supported);
        Assert.False(plan.IsEmpty);
        Assert.Contains(plan.Lines, l => l.Contains("documents", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public async Task Explain_DoesNotRunTheStatement()
    {
        // The plan is the point; executing a user's statement to describe it would be a trap on a
        // read-only connection.
        var plan = await this.admin.Explain(
            this.profileId,
            "SELECT Id FROM documents WHERE TypeName = @typeName",
            new Dictionary<string, object?> { ["@typeName"] = TypeName });

        Assert.True(plan.Supported);
        Assert.Equal(3, await this.CountDocuments());
    }

    [Fact]
    public async Task Explain_ShowsAnIndexOnceOneExists()
    {
        var query = new FilterQuery(Where: "tier == 'gold'");
        var before = await this.admin.ExecuteFilter(this.profileId, Table, TypeName, query);
        var scanPlan = await this.admin.Explain(this.profileId, before.Sql, before.Parameters);

        await this.admin.CreateIndex(this.profileId, Table, TypeName, "tier");

        var after = await this.admin.ExecuteFilter(this.profileId, Table, TypeName, query);
        var indexedPlan = await this.admin.Explain(this.profileId, after.Sql, after.Parameters);

        // The loop the console closes: the plan changes, and the new index is named in it.
        Assert.DoesNotContain(scanPlan.Lines, l => l.Contains("idx_json_Article_tier", StringComparison.Ordinal));
        Assert.Contains(indexedPlan.Lines, l => l.Contains("idx_json_Article_tier", StringComparison.Ordinal));
    }

    [Fact]
    public async Task ExplainingNothing_IsRejected()
        => await Assert.ThrowsAsync<ArgumentException>(() => this.admin.Explain(this.profileId, "   "));

    // ── Full text ───────────────────────────────────────────────────────

    [Fact]
    public async Task AMappedType_IsReportedAsIndexed()
    {
        var info = await this.admin.DescribeFullTextIndex(this.profileId, Table, TypeName);

        Assert.True(info.ProviderSupportsFullText);
        Assert.True(info.Present);
        Assert.Null(info.Unavailable);

        // SQLite's index is trigger-maintained by the engine, so nothing here can put it out of step.
        Assert.False(info.RequiresRebuild);
    }

    [Fact]
    public async Task AnUnmappedType_IsReportedAsNotIndexed()
    {
        var info = await this.admin.DescribeFullTextIndex(this.profileId, Table, "Unmapped");

        Assert.True(info.ProviderSupportsFullText);
        Assert.False(info.Present);
    }

    [Fact]
    public async Task Search_RanksMatchesWithoutAnyRegisteredMapping()
    {
        var results = await this.admin.SearchFullText(this.profileId, Table, TypeName, "orleans");

        Assert.Equal(2, results.Hits.Count);
        Assert.Equal(["a1", "a3"], results.Hits.Select(h => h.DocumentId).Order());

        // Higher is more relevant, and the ordering must already be applied.
        Assert.True(results.Hits[0].Score >= results.Hits[1].Score);
    }

    [Fact]
    public async Task Search_CoversEveryMappedField()
    {
        // "embeddings" is in Body only; "Spatial" in Title only. Both are in the one combined index.
        Assert.Equal("a2", Assert.Single((await this.admin.SearchFullText(this.profileId, Table, TypeName, "embeddings")).Hits).DocumentId);
        Assert.Equal("a3", Assert.Single((await this.admin.SearchFullText(this.profileId, Table, TypeName, "spatial")).Hits).DocumentId);
    }

    [Fact]
    public async Task Search_ReturnsNothingForATermThatIsNotThere()
        => Assert.Empty((await this.admin.SearchFullText(this.profileId, Table, TypeName, "kubernetes")).Hits);

    [Fact]
    public async Task Search_FindsADocumentThisToolJustWrote()
    {
        // The claim that matters for full text: the engine maintains the index through the tool's own
        // raw-SQL writes, so unlike the vector sidecar there is nothing to re-index by hand.
        await this.admin.SaveDocument(
            this.profileId, Table, TypeName, "a4",
            """{"id":"a4","title":"Kubernetes operators","body":"running silos on a cluster","tier":"gold"}""",
            isNew: true);

        var hits = (await this.admin.SearchFullText(this.profileId, Table, TypeName, "kubernetes")).Hits;

        Assert.Equal("a4", Assert.Single(hits).DocumentId);
    }

    [Fact]
    public async Task Search_StopsFindingADocumentThisToolDeleted()
    {
        await this.admin.DeleteDocuments(this.profileId, Table, TypeName, ["a2"]);

        Assert.Empty((await this.admin.SearchFullText(this.profileId, Table, TypeName, "embeddings")).Hits);
    }

    [Fact]
    public async Task Search_ReportsTheSqlItRan()
    {
        var results = await this.admin.SearchFullText(this.profileId, Table, TypeName, "orleans");

        Assert.Contains("documents_fts", results.Sql);
    }

    [Fact]
    public async Task SearchingForNothing_IsRejected()
        => await Assert.ThrowsAsync<ArgumentException>(
            () => this.admin.SearchFullText(this.profileId, Table, TypeName, "  "));

    // ── Generation and the sidecars it cannot maintain ──────────────────

    [Fact]
    public async Task GeneratingIntoATemporalType_ReportsTheHistoryAsStale()
    {
        // Article is MapTemporal-mapped in the fixture, so its rows do have history — and generated
        // ones will not, because the bulk lane maintains no sidecars.
        var shape = await this.admin.AnalyzeShape(this.profileId, Table, TypeName);

        var result = await this.admin.Commit(this.profileId, Table, TypeName, DocumentAdminService.Preview(shape, 5));

        Assert.Equal(5, result.Written);
        Assert.Contains("temporal history", result.StaleSidecars);
    }

    [Fact]
    public async Task ATypeSharingATableWithATemporalOne_IsNotReportedAsStale()
    {
        // The regression this guards: documents_history exists because *Article* is mapped, and a
        // table-existence check would wrongly blame every other type sharing the table.
        //
        // Seeded with raw SQL rather than SaveDocument on purpose. This tool appends a version for
        // any type in a table that has a history sidecar - it has no CLR type to tell it which types
        // are actually mapped - so writing through it would manufacture the very history this test
        // asserts is absent.
        await this.ExecuteRaw(
            "INSERT INTO documents (Id, TypeName, Data, CreatedAt, UpdatedAt) VALUES " +
            "('ord-1','Order','{\"id\":\"ord-1\",\"status\":\"Shipped\",\"total\":10}','2026-01-01','2026-01-01')," +
            "('ord-2','Order','{\"id\":\"ord-2\",\"status\":\"Pending\",\"total\":20}','2026-01-01','2026-01-01');");

        var shape = await this.admin.AnalyzeShape(this.profileId, Table, "Order");
        var result = await this.admin.Commit(this.profileId, Table, "Order", DocumentAdminService.Preview(shape, 5));

        Assert.Equal(5, result.Written);
        Assert.Empty(result.StaleSidecars);
    }

    [Fact]
    public async Task GeneratedArticles_AreImmediatelyFullTextSearchable()
    {
        // The engine maintains the FTS index through the bulk insert too, so full text is *not*
        // among the things generation leaves stale.
        var shape = await this.admin.AnalyzeShape(this.profileId, Table, TypeName);
        var result = await this.admin.Commit(this.profileId, Table, TypeName, DocumentAdminService.Preview(shape, 40));

        var hits = (await this.admin.SearchFullText(this.profileId, Table, TypeName, "orleans")).Hits;

        Assert.True(hits.Count > 1, $"expected the generated rows to be searchable, got {hits.Count}");
        Assert.DoesNotContain("full text", result.StaleSidecars);
    }

    [Fact]
    public async Task GeneratingTwiceFromOneAnalysis_CollidesOnIds()
    {
        // Pins why the UI re-analyzes after every commit: the shape carries where the id sequence had
        // reached, so a second batch from the same analysis mints ids that already exist.
        var stale = await this.admin.AnalyzeShape(this.profileId, Table, TypeName);
        await this.admin.Commit(this.profileId, Table, TypeName, DocumentAdminService.Preview(stale, 5));

        await Assert.ThrowsAnyAsync<Exception>(
            () => this.admin.Commit(this.profileId, Table, TypeName, DocumentAdminService.Preview(stale, 5)));

        // Re-learning is all it takes.
        var fresh = await this.admin.AnalyzeShape(this.profileId, Table, TypeName);
        var result = await this.admin.Commit(this.profileId, Table, TypeName, DocumentAdminService.Preview(fresh, 5));

        Assert.Equal(5, result.Written);
    }

    // ── Helpers ─────────────────────────────────────────────────────────

    async Task ExecuteRaw(string sql)
    {
        var connection = await this.admin.Connect(this.profileId);
        await connection.Execute(async (db, ct) =>
        {
            await using var cmd = Ado.Command(db, sql);
            await cmd.ExecuteNonQueryAsync(ct);
        });

        this.admin.InvalidateSchema(this.profileId);
    }

    async Task<long> CountDocuments()
    {
        await using var connection = new SqliteConnection($"Data Source={this.databasePath}");
        await connection.OpenAsync();
        await using var cmd = connection.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM documents;";
        return Convert.ToInt64(await cmd.ExecuteScalarAsync());
    }
}
