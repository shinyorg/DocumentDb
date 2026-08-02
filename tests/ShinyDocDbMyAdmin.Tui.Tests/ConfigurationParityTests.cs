using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using ShinyDocDbMyAdmin.Models;
using ShinyDocDbMyAdmin.Providers;
using ShinyDocDbMyAdmin.Services;
using ShinyDocDbMyAdmin.Tui;
using ShinyDocDbMyAdmin.Tui.Cli;

namespace ShinyDocDbMyAdmin.Tui.Tests;

/// <summary>
/// The claim that makes the terminal front end worth having: it and the web front end are the same
/// tool, not two tools that agree on a file format.
/// </summary>
/// <remarks>
/// These are the tests that would fail if the shared Core project were ever forked - if the terminal
/// app grew its own profile store, its own secret handling or its own bundle serializer. A round-trip
/// through one app's services and back through the other's is the only assertion that catches that.
/// </remarks>
public sealed class ConfigurationParityTests : IDisposable
{
    readonly string directory;

    public ConfigurationParityTests()
    {
        this.directory = Path.Combine(Path.GetTempPath(), $"docdbparity_{Guid.NewGuid():N}");
        Directory.CreateDirectory(this.directory);
        SQLitePCL.Batteries_V2.Init();
    }

    public void Dispose()
    {
        try
        {
            Directory.Delete(this.directory, recursive: true);
        }
        catch (IOException)
        {
            // Not worth failing a green test over.
        }
    }

    /// <summary>The web front end's registrations, built by hand the way its Program.cs does.</summary>
    (ProfileStore Profiles, ConnectionTransferService Transfer) BuildWebSide(string dataDirectory)
    {
        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["ShinyDocDbMyAdmin:DataDirectory"] = dataDirectory
            })
            .Build();

        var paths = new AppPaths(configuration);
        var demo = new DemoMode(configuration, NullLogger<DemoMode>.Instance);
        var protector = new SecretProtector(configuration, paths, NullLogger<SecretProtector>.Instance);
        var provided = new ProvidedConnections(configuration, NullLogger<ProvidedConnections>.Instance);
        var profiles = new ProfileStore(paths, protector, provided, demo);

        return (profiles, new ConnectionTransferService(profiles, demo, NullLogger<ConnectionTransferService>.Instance));
    }

    /// <summary>The terminal front end's, built the way it actually builds them.</summary>
    static ServiceProvider BuildTerminalSide(string dataDirectory)
        => AdminHost.Build(new CommandLineOptions { DataDirectory = dataDirectory, NoAi = true });

    [Fact]
    public async Task Both_front_ends_read_the_same_profile_store()
    {
        var shared = Path.Combine(this.directory, "shared");

        var web = this.BuildWebSide(shared);
        await web.Profiles.Save(
            new ConnectionProfile { Name = "Written by the web app", Provider = ProviderKind.PostgreSql },
            "Host=localhost;Database=orders;Username=postgres",
            null);

        using var terminal = BuildTerminalSide(shared);
        var seen = await terminal.GetRequiredService<ProfileStore>().List();

        var found = Assert.Single(seen);
        Assert.Equal("Written by the web app", found.Name);
        Assert.Equal(ProviderKind.PostgreSql, found.Provider);
    }

    [Fact]
    public async Task A_secret_saved_by_one_front_end_decrypts_in_the_other()
    {
        var shared = Path.Combine(this.directory, "secrets");
        const string connectionString = "Host=localhost;Database=orders;Username=postgres;Password=hunter2";

        using (var terminal = BuildTerminalSide(shared))
        {
            await terminal.GetRequiredService<ProfileStore>().Save(
                new ConnectionProfile { Name = "prod", Provider = ProviderKind.PostgreSql },
                connectionString,
                null);
        }

        var web = this.BuildWebSide(shared);
        var profile = Assert.Single(await web.Profiles.List());

        // The same instance key file, so the ciphertext one wrote is plaintext to the other.
        Assert.Equal(connectionString, web.Profiles.RevealConnectionString(profile));
    }

    [Fact]
    public async Task A_bundle_exported_from_the_terminal_imports_into_the_web_app()
    {
        var source = Path.Combine(this.directory, "source");
        var target = Path.Combine(this.directory, "target");

        string json;
        using (var terminal = BuildTerminalSide(source))
        {
            var profiles = terminal.GetRequiredService<ProfileStore>();
            await profiles.Save(
                new ConnectionProfile { Name = "staging", Provider = ProviderKind.MySql, ReadOnly = true },
                "Server=localhost;Database=staging;User Id=root",
                null);

            await profiles.SaveQuery(new SavedQuery
            {
                ProfileId = (await profiles.List()).Single().Id,
                Name = "recent orders",
                Sql = "SELECT * FROM documents WHERE TypeName = 'Order'"
            });

            var bundle = await terminal.GetRequiredService<ConnectionTransferService>().Export("correct horse battery staple");
            json = ConnectionTransferService.Serialize(bundle);
        }

        var web = this.BuildWebSide(target);
        var preview = await web.Transfer.Preview(json, "correct horse battery staple");

        Assert.Null(preview.Problem);
        var candidate = Assert.Single(preview.Candidates);
        Assert.Equal("staging", candidate.Name);
        Assert.True(candidate.Connection.ReadOnly);
        Assert.Single(candidate.Connection.SavedQueries);

        var outcome = await web.Transfer.Import(
            preview.Bundle,
            new Dictionary<string, ImportAction> { [candidate.Connection.Id] = ImportAction.Add },
            "correct horse battery staple");

        Assert.Equal(1, outcome.Added);
        Assert.Empty(outcome.Errors);

        var imported = Assert.Single(await web.Profiles.List());
        Assert.Equal("staging", imported.Name);
        Assert.Equal(ProviderKind.MySql, imported.Provider);

        // The point of the passphrase: the secret travelled, so the imported connection is usable
        // rather than a name with a blank where its credentials were.
        Assert.Equal("Server=localhost;Database=staging;User Id=root", web.Profiles.RevealConnectionString(imported));
    }

    [Fact]
    public async Task A_bundle_exported_from_the_web_app_imports_into_the_terminal()
    {
        var source = Path.Combine(this.directory, "websource");
        var target = Path.Combine(this.directory, "tuitarget");

        var web = this.BuildWebSide(source);
        await web.Profiles.Save(
            new ConnectionProfile { Name = "analytics", Provider = ProviderKind.DuckDb },
            "Data Source=/data/analytics.duckdb",
            null);

        var json = ConnectionTransferService.Serialize(await web.Transfer.Export(null));

        using var terminal = BuildTerminalSide(target);
        var transfer = terminal.GetRequiredService<ConnectionTransferService>();

        var preview = await transfer.Preview(json, null);
        Assert.Null(preview.Problem);

        var candidate = Assert.Single(preview.Candidates);
        var outcome = await transfer.Import(
            preview.Bundle,
            new Dictionary<string, ImportAction> { [candidate.Connection.Id] = ImportAction.Add },
            null);

        Assert.Equal(1, outcome.Added);

        var imported = Assert.Single(await terminal.GetRequiredService<ProfileStore>().List());
        Assert.Equal("analytics", imported.Name);
        Assert.Equal(ProviderKind.DuckDb, imported.Provider);
    }

    [Fact]
    public async Task An_export_without_a_passphrase_carries_no_secrets()
    {
        var shared = Path.Combine(this.directory, "nosecrets");

        using var terminal = BuildTerminalSide(shared);
        await terminal.GetRequiredService<ProfileStore>().Save(
            new ConnectionProfile { Name = "prod", Provider = ProviderKind.SqlServer },
            "Server=prod;Database=orders;User Id=sa;Password=hunter2",
            null);

        var bundle = await terminal.GetRequiredService<ConnectionTransferService>().Export(null);

        Assert.False(bundle.IncludesSecrets);
        var connection = Assert.Single(bundle.Connections);
        Assert.Null(connection.ConnectionString);
        Assert.DoesNotContain("hunter2", ConnectionTransferService.Serialize(bundle));
    }
}
