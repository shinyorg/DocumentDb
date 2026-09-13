using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging.Abstractions;
using ShinyDocDbMyAdmin.Models;
using ShinyDocDbMyAdmin.Providers;
using ShinyDocDbMyAdmin.Services;

namespace ShinyDocDbMyAdmin.Tests;

/// <summary>
/// Data that names a provider this build no longer administers - DuckDB, removed in 13.5. A saved profile
/// and an exported bundle can both still say <c>DuckDb</c>; the host-declared form is covered in
/// <see cref="ProvidedConnectionsTests"/>.
/// </summary>
public sealed class UnsupportedProviderTests : IDisposable
{
    readonly string directory;
    readonly IConfiguration configuration;
    readonly AppPaths paths;

    public UnsupportedProviderTests()
    {
        this.directory = Path.Combine(Path.GetTempPath(), $"docdbunsupported_{Guid.NewGuid():N}");
        Directory.CreateDirectory(this.directory);

        this.configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["ShinyDocDbMyAdmin:DataDirectory"] = Path.Combine(this.directory, "admin"),
                ["ShinyDocDbMyAdmin:SecretKey"] = "test-key"
            })
            .Build();

        this.paths = new AppPaths(this.configuration);
    }

    public void Dispose()
    {
        SqliteConnection.ClearAllPools();
        if (Directory.Exists(this.directory))
        {
            try { Directory.Delete(this.directory, recursive: true); }
            catch (IOException) { /* a driver still holding the file is not a test failure */ }
        }
    }

    CancellationToken Ct => TestContext.Current.CancellationToken;

    DemoMode Demo() => new(this.configuration, NullLogger<DemoMode>.Instance);

    // A new instance each time, like a restarted tool: the clean-up runs once per store.
    ProfileStore Profiles()
    {
        var protector = new SecretProtector(this.configuration, this.paths, NullLogger<SecretProtector>.Instance);
        var provided = new ProvidedConnections(this.configuration, NullLogger<ProvidedConnections>.Instance);
        var providedAi = new ProvidedAiSettings(this.configuration, NullLogger<ProvidedAiSettings>.Instance);
        return new ProfileStore(this.paths, protector, provided, providedAi, this.Demo());
    }

    // Puts a saved profile into the shape a pre-13.5 DuckDB profile has on disk. ConnectionProfile can no
    // longer be given that provider, so it has to go in underneath the store.
    async Task RewriteProvider(string profileId, string provider)
    {
        await using var connection = new SqliteConnection($"Data Source={this.paths.ProfileDatabasePath}");
        await connection.OpenAsync(this.Ct);

        await using var command = connection.CreateCommand();
        command.CommandText = "UPDATE connections SET Data = json_set(Data, '$.provider', $provider) WHERE Id = $id";
        command.Parameters.AddWithValue("$provider", provider);
        command.Parameters.AddWithValue("$id", profileId);

        Assert.Equal(1, await command.ExecuteNonQueryAsync(this.Ct));
    }

    [Fact]
    public async Task A_saved_profile_for_a_removed_provider_is_deleted_rather_than_breaking_the_list()
    {
        var before = this.Profiles();
        var kept = new ConnectionProfile { Name = "orders", Provider = ProviderKind.PostgreSql };
        var retired = new ConnectionProfile { Name = "analytics", Provider = ProviderKind.Sqlite, UploadedFileName = "analytics.duckdb" };
        await before.Save(kept, "Host=db", null, this.Ct);
        await before.Save(retired, "Data Source=analytics.duckdb", null, this.Ct);

        var upload = this.paths.UploadDirectoryFor(retired.Id);
        Directory.CreateDirectory(upload);
        await File.WriteAllTextAsync(Path.Combine(upload, "analytics.duckdb"), "not really a database", this.Ct);

        await this.RewriteProvider(retired.Id, "DuckDb");

        var after = this.Profiles();
        Assert.Equal("orders", Assert.Single(await after.List(this.Ct)).Name);
        Assert.Null(await after.Get(retired.Id, this.Ct));

        // The uploaded file belonged to the profile, so it goes the way an ordinary delete takes it.
        Assert.False(Directory.Exists(upload));
    }

    [Fact]
    public async Task A_lookup_by_id_clears_it_without_the_list_being_read_first()
    {
        var before = this.Profiles();
        var retired = new ConnectionProfile { Name = "analytics", Provider = ProviderKind.Sqlite };
        await before.Save(retired, "Data Source=analytics.duckdb", null, this.Ct);

        await this.RewriteProvider(retired.Id, "DuckDb");

        Assert.Null(await this.Profiles().Get(retired.Id, this.Ct));
    }

    [Fact]
    public async Task A_bundle_naming_a_removed_provider_is_refused_with_the_provider_named()
    {
        var profiles = this.Profiles();
        var transfer = new ConnectionTransferService(profiles, this.Demo(), NullLogger<ConnectionTransferService>.Instance);

        await profiles.Save(new ConnectionProfile { Name = "orders", Provider = ProviderKind.PostgreSql }, "Host=db", null, this.Ct);
        var json = ConnectionTransferService.Serialize(await transfer.Export(null, this.Ct))
            .Replace("\"PostgreSql\"", "\"DuckDb\"");

        var preview = await transfer.Preview(json, null, this.Ct);

        Assert.Empty(preview.Candidates);
        var problem = Assert.IsType<string>(preview.Problem);
        Assert.Contains("DuckDb", problem);

        // The file is valid JSON - saying otherwise sends someone hunting for a syntax error that is not there.
        Assert.DoesNotContain("not valid JSON", problem);
    }
}
