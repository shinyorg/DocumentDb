using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging.Abstractions;
using Shiny.DocumentDb;
using Shiny.DocumentDb.Sqlite;
using ShinyDocDbMyAdmin.Models;
using ShinyDocDbMyAdmin.Services;

namespace ShinyDocDbMyAdmin.Tests;

/// <summary>
/// What the tool decides a database <b>is</b> — the classification and the verdict, against a real store
/// standing next to tables that merely look like one.
/// </summary>
/// <remarks>
/// <para>
/// The decoys are the point. Before this, classification ran unanchored name-substring rules <i>before</i>
/// gathering any evidence, so a business table called <c>audit_history</c> or <c>geo_spatial_index</c> was
/// reported as a DocumentDb sidecar, and a documents table called <c>orders_history</c> never reached the
/// envelope probe at all — it was called history and disappeared from the explorer, the filter console and
/// the assistant. Every test below fails against that classifier.
/// </para>
/// <para>
/// SQLite is enough for all of it: it is the one backend with a catalog, an index list, FTS5 and R*Tree
/// without a container. What it has no module for here is sqlite-vec, so the vector sidecar and its id map
/// are created as plain tables with the names the provider computes — which is exactly what the
/// classification reads, and the only thing these tests claim about them.
/// </para>
/// </remarks>
public sealed class DatabaseIdentityTests : IAsyncLifetime
{
    sealed class Order
    {
        public string Id { get; set; } = "";
        public string Customer { get; set; } = "";
        public string Notes { get; set; } = "";
        public GeoPoint? Where { get; set; }
    }

    const string Table = "documents";

    string directory = "";
    string databasePath = "";
    string profileId = "";
    DocumentAdminService admin = null!;
    ConnectionManager connections = null!;
    ProfileStore profiles = null!;

    public ValueTask InitializeAsync()
    {
        this.directory = Path.Combine(Path.GetTempPath(), $"docdbident_{Guid.NewGuid():N}");
        Directory.CreateDirectory(this.directory);
        this.databasePath = Path.Combine(this.directory, "store.db");
        this.Build();
        return ValueTask.CompletedTask;
    }

    void Build()
    {
        this.connections?.Dispose();

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
        this.profiles = new ProfileStore(paths, protector, provided,
            new DemoMode(configuration, NullLogger<DemoMode>.Instance));

        this.profileId = provided.Profiles.Single().Id;
        this.connections = new ConnectionManager(this.profiles, NullLogger<ConnectionManager>.Instance);
        this.admin = new DocumentAdminService(this.connections, NullLogger<DocumentAdminService>.Instance);
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

    // ── Seeding ─────────────────────────────────────────────────────────

    /// <summary>Writes through the library, so every sidecar is created by the DDL that really creates it.</summary>
    async Task SeedStore()
    {
        var options = new DocumentStoreOptions
        {
            DatabaseProvider = new SqliteDatabaseProvider($"Data Source={this.databasePath}"),
            TableName = Table
        };

        options.ConfigureDocument<Order>(cfg => cfg
            .MapTemporal()
            .MapSpatialProperty(x => x.Where)
            .MapFullTextProperty([x => x.Notes]));

        using var store = new DocumentStore(options);
        await store.Insert(new Order { Id = "o1", Customer = "ann", Notes = "first", Where = new GeoPoint(51.5, -0.1) });
        await store.Insert(new Order { Id = "o2", Customer = "bo", Notes = "second", Where = new GeoPoint(48.9, 2.4) });
    }

    async Task Sql(params string[] statements)
    {
        await using var db = new SqliteConnection($"Data Source={this.databasePath}");
        await db.OpenAsync(TestContext.Current.CancellationToken);

        foreach (var statement in statements)
        {
            await using var cmd = db.CreateCommand();
            cmd.CommandText = statement;
            await cmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
        }
    }

    /// <summary>The blob sidecar, created by the provider's own DDL rather than by hand.</summary>
    async Task SeedBlobSidecar()
    {
        var provider = new SqliteDatabaseProvider($"Data Source={this.databasePath}");
        await this.Sql(((IDatabaseProvider)provider).BuildCreateBlobTableSql(Table)!);
    }

    /// <summary>
    /// The vector sidecar and its id map. Named by the provider, created as plain tables: sqlite-vec's
    /// native module is not available on every platform the suite runs on, and nothing here depends on what
    /// is <i>in</i> them — the classification matches the names the provider computes.
    /// </summary>
    async Task SeedVectorSidecar()
    {
        var provider = (IDatabaseProvider)new SqliteDatabaseProvider($"Data Source={this.databasePath}");
        var vec = provider.VectorTableName(Table, nameof(Order));

        await this.Sql(
            $"CREATE TABLE {vec} (rowid INTEGER PRIMARY KEY, embedding BLOB);",
            $"CREATE TABLE {Table}_vec_map_{nameof(Order)} (rowid INTEGER PRIMARY KEY, docId TEXT, typeName TEXT);");
    }

    /// <summary>
    /// Tables named exactly like the substring rules used to look for, belonging to someone else entirely.
    /// </summary>
    Task SeedDecoys() => this.Sql(
        "CREATE TABLE audit_history (id INTEGER PRIMARY KEY, actor TEXT, at TEXT);",
        "CREATE TABLE customer_blobs (id INTEGER PRIMARY KEY, payload BLOB);",
        "CREATE TABLE geo_spatial_index (id INTEGER PRIMARY KEY, cell TEXT);",
        "CREATE TABLE invoice_vec_lines (id INTEGER PRIMARY KEY, amount REAL);",
        "CREATE TABLE search_fts_cache (id INTEGER PRIMARY KEY, term TEXT);",

        // Half an envelope. The three columns a coincidence has, without the two it does not.
        "CREATE TABLE orders (Id TEXT, TypeName TEXT, Data TEXT);");

    async Task<IReadOnlyDictionary<string, TableInfo>> Classified()
        => (await this.admin.ListTables(this.profileId, refresh: true, TestContext.Current.CancellationToken))
            .ToDictionary(t => t.Name, StringComparer.Ordinal);

    // ── Classification ──────────────────────────────────────────────────

    [Fact]
    public async Task EverySidecarIsNamedFromItsDocumentsTable_AndEveryDecoyIsForeign()
    {
        await this.SeedStore();
        await this.SeedBlobSidecar();
        await this.SeedVectorSidecar();
        await this.SeedDecoys();

        var tables = await this.Classified();

        Assert.Equal(TableRole.Documents, tables[Table].Role);
        Assert.Equal(TableConfidence.Confirmed, tables[Table].Confidence);
        Assert.Null(tables[Table].Owner);

        // Ours, and each one says whose it is.
        AssertOwned(tables, $"{Table}_history", TableRole.History);
        AssertOwned(tables, $"{Table}_blobs", TableRole.Blobs);
        AssertOwned(tables, $"{Table}_spatial", TableRole.Spatial);
        AssertOwned(tables, $"{Table}_spatial_map", TableRole.Spatial);
        AssertOwned(tables, $"{Table}_spatial_node", TableRole.Spatial);
        AssertOwned(tables, $"{Table}_spatial_rowid", TableRole.Spatial);
        AssertOwned(tables, $"{Table}_spatial_parent", TableRole.Spatial);
        AssertOwned(tables, $"{Table}_vec_{nameof(Order)}", TableRole.Vector);
        AssertOwned(tables, $"{Table}_vec_map_{nameof(Order)}", TableRole.Vector);
        AssertOwned(tables, $"{Table}_fts", TableRole.FullText);
        AssertOwned(tables, $"{Table}_fts_data", TableRole.FullText);
        AssertOwned(tables, $"{Table}_fts_idx", TableRole.FullText);
        AssertOwned(tables, $"{Table}_fts_config", TableRole.FullText);

        // Not ours, whatever they are called. Every one of these was a DocumentDb sidecar before.
        foreach (var decoy in new[]
                 {
                     "audit_history", "customer_blobs", "geo_spatial_index",
                     "invoice_vec_lines", "search_fts_cache", "orders"
                 })
        {
            Assert.Equal(TableRole.Foreign, tables[decoy].Role);
            Assert.Null(tables[decoy].Owner);
            Assert.False(tables[decoy].IsBrowsable);
        }
    }

    static void AssertOwned(IReadOnlyDictionary<string, TableInfo> tables, string name, TableRole role)
    {
        var table = Assert.Contains(name, tables);
        Assert.Equal(role, table.Role);
        Assert.Equal(Table, table.Owner);
        Assert.False(table.IsBrowsable);
        Assert.True(table.IsOwned);
    }

    [Fact]
    public async Task ADocumentsTableNamedLikeASidecar_IsStillDocuments()
    {
        // The regression this replaces: `orders_history` matched the history rule, so it was never probed
        // and never browsable - a whole table hidden because of what someone called it.
        await this.Sql($"""
            CREATE TABLE orders_history (
                Id TEXT NOT NULL, TypeName TEXT NOT NULL, Data TEXT NOT NULL,
                CreatedAt TEXT NOT NULL, UpdatedAt TEXT NOT NULL,
                PRIMARY KEY (Id, TypeName));
            """);

        var table = (await this.Classified())["orders_history"];

        Assert.Equal(TableRole.Documents, table.Role);
        Assert.True(table.IsBrowsable);
        Assert.Null(table.Owner);
    }

    [Fact]
    public async Task AHandMadeEnvelopeTable_IsProbable_AndStillBrowsable()
    {
        // Five columns of the right name and nothing else: no typename index, no JSON index, no rows. Most
        // likely ours, and nothing proves it - so the tool says exactly that instead of picking a side.
        await this.Sql("""
            CREATE TABLE handmade (
                Id TEXT, TypeName TEXT, Data BLOB, CreatedAt TEXT, UpdatedAt TEXT);
            """);

        var table = (await this.Classified())["handmade"];

        Assert.Equal(TableRole.Documents, table.Role);
        Assert.Equal(TableConfidence.Probable, table.Confidence);
        Assert.True(table.IsBrowsable);
        Assert.NotNull(table.Feature);

        var identity = await this.admin.GetIdentity(this.profileId, ct: TestContext.Current.CancellationToken);
        Assert.True(identity.Participates);
        Assert.Equal(IdentityConfidence.Probable, identity.Confidence);
    }

    [Fact]
    public async Task SamplingConfirmsATableTheCatalogCannot()
    {
        await this.Sql(
            "CREATE TABLE handmade (Id TEXT, TypeName TEXT, Data BLOB, CreatedAt TEXT, UpdatedAt TEXT);",
            "INSERT INTO handmade VALUES ('h1', 'Order', '{\"id\":\"h1\"}', '2026-01-01', '2026-01-01');");

        var catalogOnly = await this.admin.GetIdentity(this.profileId, refresh: true, ct: TestContext.Current.CancellationToken);
        Assert.Equal(IdentityConfidence.Probable, catalogOnly.Confidence);

        // The opt-in signal: one row that parses as JSON under a non-empty type. It is the only read here
        // that touches data rather than catalog, which is why the caller has to ask for it.
        var sampled = await this.admin.GetIdentity(
            this.profileId, sampleRows: true, refresh: true, TestContext.Current.CancellationToken);

        Assert.Equal(IdentityConfidence.Confirmed, sampled.Confidence);
    }

    // ── The verdict ─────────────────────────────────────────────────────

    [Fact]
    public async Task ADatabaseOfForeignTablesOnly_DoesNotParticipate()
    {
        await this.SeedDecoys();

        var identity = await this.admin.GetIdentity(this.profileId, ct: TestContext.Current.CancellationToken);

        Assert.False(identity.Participates);
        Assert.Equal(IdentityConfidence.None, identity.Confidence);
        Assert.Equal(0, identity.DocumentTables);
        Assert.Equal(6, identity.ForeignTables);
        Assert.Empty(identity.Features);
        Assert.NotEmpty(identity.Reasons);
        Assert.Contains("Not a DocumentDb database", identity.Summary, StringComparison.Ordinal);

        Assert.DoesNotContain((await this.Classified()).Values, t => t.IsBrowsable);
    }

    [Fact]
    public async Task TheVerdictListsTheFeaturesTheDatabaseCanBeProvenToUse()
    {
        await this.SeedStore();
        await this.SeedBlobSidecar();
        await this.SeedVectorSidecar();
        await this.SeedDecoys();

        var identity = await this.admin.GetIdentity(this.profileId, refresh: true, ct: TestContext.Current.CancellationToken);

        Assert.True(identity.Participates);
        Assert.Equal(IdentityConfidence.Confirmed, identity.Confidence);
        Assert.Equal(1, identity.DocumentTables);
        Assert.Equal(1, identity.TypeCount);
        Assert.Equal(6, identity.ForeignTables);
        Assert.True(identity.OwnedSidecars > 0);

        Assert.Equal(["temporal", "blobs", "spatial", "vectors", "full text"], identity.Features);

        // Never listed, because nothing in this database records it - see the soft-delete tests.
        Assert.DoesNotContain("soft delete", identity.Features);
    }

    [Fact]
    public async Task TheOutboxIsAFeature_FoundFromTheTypesRatherThanATableName()
    {
        await this.SeedStore();

        // The outbox is ordinary documents, and its table name is configurable - so it is recognised by the
        // type stored in it, in whatever table that turns out to be.
        await this.admin.CreateDocumentsTable(this.profileId, "queue", TestContext.Current.CancellationToken);
        await this.Sql(
            "INSERT INTO queue VALUES ('m1', 'OutboxMessage', '{\"id\":\"m1\"}', '2026-01-01', '2026-01-01');");

        var identity = await this.admin.GetIdentity(this.profileId, refresh: true, ct: TestContext.Current.CancellationToken);

        Assert.Contains("outbox", identity.Features);
        Assert.Equal(2, identity.DocumentTables);
    }

    [Fact]
    public async Task TestConnectionReportsTheSameVerdictTheOverviewShows()
    {
        await this.SeedStore();

        var identity = await this.admin.GetIdentity(this.profileId, refresh: true, ct: TestContext.Current.CancellationToken);
        var sentence = await this.admin.TestConnection(this.profileId, TestContext.Current.CancellationToken);

        Assert.Contains(identity.Summary, sentence, StringComparison.Ordinal);
    }

    [Fact]
    public async Task AProfileThatHidesForeignTables_StillCountsThem()
    {
        await this.SeedStore();
        await this.SeedDecoys();

        // HideForeignTables is a front-end preference. The classification never drops a table, so the
        // verdict's counts always describe the whole database however it is displayed.
        var identity = await this.admin.GetIdentity(this.profileId, refresh: true, ct: TestContext.Current.CancellationToken);
        var tables = await this.Classified();

        Assert.Equal(identity.ForeignTables, tables.Values.Count(t => t.Role == TableRole.Foreign));
        Assert.Equal(6, identity.ForeignTables);
    }
}
