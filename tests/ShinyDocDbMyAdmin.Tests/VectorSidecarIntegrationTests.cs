using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging.Abstractions;
using Shiny.DocumentDb;
using Shiny.DocumentDb.Sqlite.VectorSupport;
using ShinyDocDbMyAdmin.Models;
using ShinyDocDbMyAdmin.Services;

namespace ShinyDocDbMyAdmin.Tests;

/// <summary>
/// The admin tool against a real store, on a real file, with a real <c>vec0</c> sidecar.
/// </summary>
/// <remarks>
/// These are the tests that matter for the sidecar fix, and they need the sidecar to actually exist:
/// the tool used to write documents straight past <c>{table}_vec_{type}</c>, which no unit test over
/// JSON could catch. sqlite-vec is registered as a process-global auto-extension here so the admin's
/// own connections can reach the virtual table - the shipped app does not bundle the natives, and
/// deliberately degrades instead.
/// </remarks>
public sealed class VectorSidecarIntegrationTests : IAsyncLifetime
{
    sealed class Doc
    {
        public string Id { get; set; } = "";
        public string Title { get; set; } = "";
        public ReadOnlyMemory<float> Embedding { get; set; }
    }

    const string Table = "documents";
    const string TypeName = nameof(Doc);

    /// <summary>
    /// Deliberately above <see cref="VectorMath.MinDimensions"/>: discovery has no mapping to consult
    /// and refuses to call a four-number array an embedding, because a coordinate pair or an RGB
    /// triplet is one too.
    /// </summary>
    const int Dimensions = 16;

    /// <summary>A vector with the given leading components, zero-padded to <see cref="Dimensions"/>.</summary>
    static float[] V(params float[] head)
    {
        var values = new float[Dimensions];
        head.CopyTo(values, 0);
        return values;
    }

    static string Body(string id, string title, float[] values)
        => $$"""{"id":"{{id}}","title":"{{title}}","embedding":{{VectorMath.Format(values)}}}""";

    static bool VecAvailable =>
        File.Exists(Path.Combine(AppContext.BaseDirectory, "vec0.dylib")) ||
        File.Exists(Path.Combine(AppContext.BaseDirectory, "vec0.so")) ||
        File.Exists(Path.Combine(AppContext.BaseDirectory, "vec0.dll"));

    string directory = "";
    string databasePath = "";
    string profileId = "";
    DocumentAdminService admin = null!;
    ConnectionManager connections = null!;

    public async ValueTask InitializeAsync()
    {
        this.directory = Path.Combine(Path.GetTempPath(), $"docdbadmin_{Guid.NewGuid():N}");
        Directory.CreateDirectory(this.directory);
        this.databasePath = Path.Combine(this.directory, "store.db");

        if (VecAvailable)
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

    /// <summary>Writes the documents through the library, so the sidecar starts out correct.</summary>
    async Task Seed()
    {
        var options = new DocumentStoreOptions
        {
            DatabaseProvider = SqliteVec.CreateProvider($"Data Source={this.databasePath}")
        };
        options.ConfigureDocument<Doc>(cfg => cfg.MapVectorProperty(d => d.Embedding, Dimensions, metric: VectorDistance.Cosine));

        using var store = new DocumentStore(options);
        await store.Insert(new Doc { Id = "a", Title = "east", Embedding = V(1) });
        await store.Insert(new Doc { Id = "b", Title = "north", Embedding = V(0, 1) });
        await store.Insert(new Doc { Id = "c", Title = "east-ish", Embedding = V(0.9f, 0.1f) });
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

    static bool Skip()
    {
        if (VecAvailable)
            return false;

        Assert.Skip("bundled sqlite-vec loadable not present next to test assembly.");
        return true;
    }

    Task<VectorSidecarInfo> Sidecar() =>
        this.admin.DescribeVectorSidecar(this.profileId, Table, TypeName, refresh: true);

    // ── Discovery ───────────────────────────────────────────────────────

    [Fact]
    public async Task TheEmbeddingProperty_IsDiscoveredFromTheBodies()
    {
        if (Skip()) return;

        var paths = await this.admin.FindVectorPaths(this.profileId, Table, TypeName);

        var field = Assert.Single(paths);
        Assert.Equal("embedding", field.Path);
        Assert.Equal(Dimensions, field.Dimensions);
        Assert.False(field.Ragged);
        Assert.Equal(3, field.Occurrences);
    }

    [Fact]
    public async Task TheSidecarIsFound_AndIsWritable_WhenVecIsLoaded()
    {
        if (Skip()) return;

        var sidecar = await this.Sidecar();

        Assert.Equal($"{Table}_vec_{TypeName}", sidecar.TableName);
        Assert.True(sidecar.Present);
        Assert.Null(sidecar.Unavailable);
        Assert.True(sidecar.Maintainable);
        Assert.Equal(3, sidecar.RowCount);

        // The provider's own SupportsVector is a configuration flag on SQLite, and the tool does not
        // set it - so the sidecar has to be found by looking for it, not by asking the provider.
        Assert.False(sidecar.ProviderSupportsVectors);
    }

    [Fact]
    public async Task AFreshlySeededStore_IsHealthy()
    {
        if (Skip()) return;

        var health = await this.LoadHealth();

        Assert.True(health.IsHealthy);
        Assert.Equal(3, health.DocumentsWithVector);
        Assert.Empty(health.MissingFromSidecar);
        Assert.Empty(health.OrphanedInSidecar);
        Assert.Equal([Dimensions], health.DimensionsPresent);
    }

    async Task<VectorHealth> LoadHealth()
    {
        var set = await this.admin.LoadVectors(this.profileId, Table, TypeName, "embedding");
        return await this.admin.CheckVectorHealth(this.profileId, Table, TypeName, set);
    }

    // ── The fix: writes carry the sidecar ───────────────────────────────

    [Fact]
    public async Task DeletingADocument_RemovesItsSidecarRow()
    {
        if (Skip()) return;

        await this.admin.DeleteDocuments(this.profileId, Table, TypeName, ["b"]);

        Assert.Equal(2, (await this.Sidecar()).RowCount);
        Assert.Empty((await this.LoadHealth()).OrphanedInSidecar);
    }

    [Fact]
    public async Task EditingADocument_ReIndexesItsEmbedding()
    {
        if (Skip()) return;

        // "b" pointed north; point it east and it should become the nearest thing to east. The
        // library reads the sidecar, not the body - so this only passes if the save re-indexed.
        await this.admin.SaveDocument(
            this.profileId, Table, TypeName, "b", Body("b", "north", V(1)), isNew: false);

        var hits = await this.NearestViaTheLibrary(V(1), k: 1);

        Assert.Equal("b", hits[0]);
    }

    [Fact]
    public async Task EditingADocument_ReportsThatTheSidecarWasSynced()
    {
        if (Skip()) return;

        var outcome = await this.admin.SaveDocument(
            this.profileId, Table, TypeName, "a", Body("a", "east", V(1)), isNew: false);

        Assert.Equal(VectorSyncOutcome.Synced, outcome);
    }

    [Fact]
    public async Task ABodyWithNoSingleEmbedding_LeavesTheSidecarAlone_AndSaysSo()
    {
        if (Skip()) return;

        // Two candidate arrays: which one the sidecar column means is not knowable from the body.
        var outcome = await this.admin.SaveDocument(
            this.profileId, Table, TypeName, "a",
            $$"""{"id":"a","embedding":{{VectorMath.Format(V(1))}},"other":{{VectorMath.Format(V(0, 0, 1))}}}""",
            isNew: false);

        Assert.Equal(VectorSyncOutcome.Ambiguous, outcome);
        Assert.Equal(3, (await this.Sidecar()).RowCount);
    }

    [Fact]
    public async Task InsertingADocument_IndexesIt()
    {
        if (Skip()) return;

        await this.admin.SaveDocument(
            this.profileId, Table, TypeName, "d", Body("d", "west", V(-1)), isNew: true);

        Assert.Equal(4, (await this.Sidecar()).RowCount);
        Assert.True((await this.LoadHealth()).IsHealthy);
    }

    [Fact]
    public async Task ClearingTheType_EmptiesTheSidecar()
    {
        if (Skip()) return;

        await this.admin.ClearType(this.profileId, Table, TypeName);

        Assert.Equal(0, (await this.Sidecar()).RowCount);
    }

    // ── Repair ──────────────────────────────────────────────────────────

    [Fact]
    public async Task DriftIsDetected_AndRebuildRepairsIt()
    {
        if (Skip()) return;

        // Manufacture exactly the state the old write path produced: a document the index has never
        // heard of, and a row pointing at a document that is gone.
        var ghost = Body("ghost", "up", V(0, 0, 1)).Replace("'", "''");
        var connection = await this.admin.Connect(this.profileId);
        await connection.Execute(async (db, ct) =>
        {
            await using var raw = Ado.Command(db,
                $"INSERT INTO documents (Id, TypeName, Data, CreatedAt, UpdatedAt) " +
                $"VALUES ('ghost', 'Doc', '{ghost}', '2026-01-01', '2026-01-01');" +
                $"DELETE FROM documents WHERE Id = 'b';");

            await raw.ExecuteNonQueryAsync(ct);
        });

        var before = await this.LoadHealth();
        Assert.False(before.IsHealthy);
        Assert.Equal(["ghost"], before.MissingFromSidecar);
        Assert.Equal(["b"], before.OrphanedInSidecar);

        var written = await this.admin.ResyncVectorSidecar(this.profileId, Table, TypeName, "embedding");

        Assert.Equal(3, written);
        Assert.True((await this.LoadHealth()).IsHealthy);

        // And the rebuilt index answers: "ghost" points along the third axis and nothing else does.
        Assert.Equal("ghost", (await this.NearestViaTheLibrary(V(0, 0, 1), k: 1))[0]);
    }

    [Fact]
    public async Task ClearingOneVector_RemovesItFromBothTheBodyAndTheIndex()
    {
        if (Skip()) return;

        await this.admin.WriteVector(this.profileId, Table, TypeName, "c", "embedding", values: null);

        Assert.Equal(2, (await this.Sidecar()).RowCount);
        Assert.Null(await this.admin.ReadVector(this.profileId, Table, TypeName, "c", "embedding"));
        Assert.True((await this.LoadHealth()).IsHealthy);
    }

    // ── Search ──────────────────────────────────────────────────────────

    [Fact]
    public async Task NearestNeighbours_RankFromADocumentsOwnEmbedding()
    {
        if (Skip()) return;

        var probe = await this.admin.ReadVector(this.profileId, Table, TypeName, "a", "embedding");
        Assert.NotNull(probe);

        var hits = await this.admin.FindNearest(
            this.profileId, Table, TypeName, "embedding", probe, k: 3, VectorDistance.Cosine);

        Assert.Equal(["a", "c", "b"], hits.Select(h => h.DocumentId));
        Assert.Equal(0, hits[0].Score, 6);
        Assert.Equal("east", hits[0].Summary);
    }

    [Fact]
    public async Task NearestNeighbours_AgreeWithTheEnginesOwnAnswer()
    {
        if (Skip()) return;

        var mine = await this.admin.FindNearest(
            this.profileId, Table, TypeName, "embedding", V(1), k: 2, VectorDistance.Cosine);

        var theirs = await this.NearestViaTheLibrary(V(1), k: 2);

        Assert.Equal(theirs, mine.Select(h => h.DocumentId));
    }

    [Fact]
    public async Task AProbeOfTheWrongDimension_MatchesNothing_RatherThanRankingByPrefix()
    {
        if (Skip()) return;

        var hits = await this.admin.FindNearest(
            this.profileId, Table, TypeName, "embedding", new float[Dimensions * 2], k: 3);

        Assert.Empty(hits);
    }

    /// <summary>Asks the library - and therefore the sidecar - who the nearest documents are.</summary>
    async Task<IReadOnlyList<string>> NearestViaTheLibrary(float[] probe, int k)
    {
        var options = new DocumentStoreOptions
        {
            DatabaseProvider = SqliteVec.CreateProvider($"Data Source={this.databasePath}")
        };
        options.ConfigureDocument<Doc>(cfg => cfg.MapVectorProperty(d => d.Embedding, Dimensions, metric: VectorDistance.Cosine));

        using var store = new DocumentStore(options);
        var hits = await store.NearestVectors<Doc>(probe, k);
        return [.. hits.Select(h => h.Document.Id)];
    }
}
