using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging.Abstractions;
using ShinyDocDbMyAdmin.Models;
using ShinyDocDbMyAdmin.Providers;
using ShinyDocDbMyAdmin.Services;

namespace ShinyDocDbMyAdmin.Tests;

/// <summary>
/// Moving the connection list between instances - and, mostly, what happens to the secrets.
/// </summary>
public sealed class ConnectionTransferTests : IDisposable
{
    readonly string directory;
    readonly ProfileStore profiles;
    readonly ConnectionTransferService transfers;

    public ConnectionTransferTests()
    {
        this.directory = Path.Combine(Path.GetTempPath(), $"docdbxfer_{Guid.NewGuid():N}");
        Directory.CreateDirectory(this.directory);

        (this.profiles, this.transfers) = this.Build(demoMode: false);
    }

    (ProfileStore, ConnectionTransferService) Build(bool demoMode)
    {
        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["ShinyDocDbMyAdmin:DataDirectory"] = Path.Combine(this.directory, "admin"),
                ["ShinyDocDbMyAdmin:SecretKey"] = "test-key",
                [DemoMode.ConfigurationKey] = demoMode ? "true" : "false"
            })
            .Build();

        var paths = new AppPaths(configuration);
        var protector = new SecretProtector(configuration, paths, NullLogger<SecretProtector>.Instance);
        var provided = new ProvidedConnections(configuration, NullLogger<ProvidedConnections>.Instance);
        var demo = new DemoMode(configuration, NullLogger<DemoMode>.Instance);
        var store = new ProfileStore(paths, protector, provided, demo);

        return (store, new ConnectionTransferService(store, demo, NullLogger<ConnectionTransferService>.Instance));
    }

    public void Dispose()
    {
        if (Directory.Exists(this.directory))
        {
            try { Directory.Delete(this.directory, recursive: true); }
            catch (IOException) { /* a driver still holding the file is not a test failure */ }
        }
    }

    CancellationToken Ct => TestContext.Current.CancellationToken;

    async Task<ConnectionProfile> Seed(string name = "Orders", string connectionString = "Host=db;Password=hunter2")
    {
        var profile = new ConnectionProfile { Name = name, Provider = ProviderKind.PostgreSql };
        await this.profiles.Save(profile, connectionString, null, this.Ct);
        return profile;
    }

    // ── Export ──────────────────────────────────────────────────────────

    [Fact]
    public async Task WithoutAPassphrase_NoSecretsLeaveTheInstance()
    {
        await this.Seed();

        var bundle = await this.transfers.Export(null, this.Ct);
        var json = ConnectionTransferService.Serialize(bundle);

        Assert.False(bundle.IncludesSecrets);
        Assert.Null(Assert.Single(bundle.Connections).ConnectionString);

        // The whole point of the default: the file is not a credential.
        Assert.DoesNotContain("hunter2", json);
        Assert.DoesNotContain("Host=db", json);

        // What it does carry is enough to rebuild the list by hand.
        Assert.Contains("Orders", json);
        Assert.Contains("PostgreSql", json);
    }

    [Fact]
    public async Task WithAPassphrase_SecretsTravelButOnlyAsCiphertext()
    {
        await this.Seed();

        var bundle = await this.transfers.Export("correct horse", this.Ct);
        var json = ConnectionTransferService.Serialize(bundle);

        Assert.True(bundle.IncludesSecrets);
        Assert.NotNull(bundle.Salt);
        Assert.NotNull(bundle.KeyDerivation);
        Assert.DoesNotContain("hunter2", json);
    }

    [Fact]
    public async Task EachExport_UsesAFreshSalt()
    {
        await this.Seed();

        var first = await this.transfers.Export("same passphrase", this.Ct);
        var second = await this.transfers.Export("same passphrase", this.Ct);

        // Otherwise the same passphrase produces the same key every time, and two exports become
        // a crib for each other.
        Assert.NotEqual(first.Salt, second.Salt);
        Assert.NotEqual(first.Connections[0].ConnectionString, second.Connections[0].ConnectionString);
    }

    [Fact]
    public async Task HostProvidedConnections_AreNotExported()
    {
        // They are declared by an AppHost or by configuration, so they are not this instance's to
        // hand out - and an import would produce something the target could not edit either.
        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["ShinyDocDbMyAdmin:DataDirectory"] = Path.Combine(this.directory, "provided"),
                ["ShinyDocDbMyAdmin:SecretKey"] = "test-key",
                ["ConnectionStrings:fromhost"] = "Data Source=host.db",
                ["Shiny:DocumentDb:fromhost:Provider"] = "Sqlite"
            })
            .Build();

        var paths = new AppPaths(configuration);
        var protector = new SecretProtector(configuration, paths, NullLogger<SecretProtector>.Instance);
        var provided = new ProvidedConnections(configuration, NullLogger<ProvidedConnections>.Instance);
        var demo = new DemoMode(configuration, NullLogger<DemoMode>.Instance);
        var store = new ProfileStore(paths, protector, provided, demo);
        var service = new ConnectionTransferService(store, demo, NullLogger<ConnectionTransferService>.Instance);

        var bundle = await service.Export(null, this.Ct);

        Assert.Empty(bundle.Connections);
    }

    // ── Round trip ──────────────────────────────────────────────────────

    [Fact]
    public async Task ARoundTrip_RestoresTheSecret()
    {
        await this.Seed();
        var exported = ConnectionTransferService.Serialize(await this.transfers.Export("s3cret", this.Ct));

        // A fresh instance, with its own SecretKey - which is exactly the case the instance key
        // cannot serve and the passphrase can.
        var (otherProfiles, otherTransfers) = this.BuildSeparateInstance();

        var preview = await otherTransfers.Preview(exported, "s3cret", this.Ct);
        Assert.Null(preview.Problem);

        var decisions = preview.Candidates.ToDictionary(c => c.Connection.Id, _ => ImportAction.Add);
        var outcome = await otherTransfers.Import(preview.Bundle, decisions, "s3cret", this.Ct);

        Assert.Equal(1, outcome.Added);
        Assert.Empty(outcome.Errors);

        var landed = Assert.Single(await otherProfiles.List(this.Ct));
        Assert.Equal("Orders", landed.Name);
        Assert.Equal("Host=db;Password=hunter2", otherProfiles.RevealConnectionString(landed));
    }

    [Fact]
    public async Task TheWrongPassphrase_IsRejectedBeforeAnythingIsWritten()
    {
        await this.Seed();
        var exported = ConnectionTransferService.Serialize(await this.transfers.Export("s3cret", this.Ct));

        var (otherProfiles, otherTransfers) = this.BuildSeparateInstance();

        var preview = await otherTransfers.Preview(exported, "wrong", this.Ct);

        Assert.NotNull(preview.Problem);
        Assert.Contains("passphrase", preview.Problem, StringComparison.OrdinalIgnoreCase);

        // And if someone pushes past the preview, the import refuses rather than writing half of it.
        var outcome = await otherTransfers.Import(
            preview.Bundle,
            preview.Candidates.ToDictionary(c => c.Connection.Id, _ => ImportAction.Add),
            "wrong",
            this.Ct);

        Assert.Equal(0, outcome.Added);
        Assert.Empty(await otherProfiles.List(this.Ct));
    }

    [Fact]
    public async Task SavedQueriesAndAssistantSettings_TravelWithTheirConnection()
    {
        var profile = await this.Seed();
        await this.profiles.SaveQuery(
            new SavedQuery { ProfileId = profile.Id, Name = "Recent", Sql = "SELECT 1" }, this.Ct);
        await this.profiles.SaveAiSettings(
            new AiConnectionSettings
            {
                ProfileId = profile.Id,
                Provider = AiProviderKind.OpenAI,
                Model = "gpt-4o-mini",
                Enabled = true
            },
            "sk-key",
            this.Ct);

        var exported = ConnectionTransferService.Serialize(await this.transfers.Export("pp", this.Ct));
        var (otherProfiles, otherTransfers) = this.BuildSeparateInstance();

        var preview = await otherTransfers.Preview(exported, "pp", this.Ct);
        await otherTransfers.Import(
            preview.Bundle,
            preview.Candidates.ToDictionary(c => c.Connection.Id, _ => ImportAction.Add),
            "pp",
            this.Ct);

        var landed = Assert.Single(await otherProfiles.List(this.Ct));
        Assert.Equal("Recent", Assert.Single(await otherProfiles.ListQueries(landed.Id, this.Ct)).Name);

        var ai = await otherProfiles.GetAiSettings(landed.Id, this.Ct);
        Assert.NotNull(ai);
        Assert.Equal("gpt-4o-mini", ai.Model);
        Assert.Equal("sk-key", otherProfiles.RevealApiKey(ai));
    }

    // ── The review step ─────────────────────────────────────────────────

    [Fact]
    public async Task AnExistingConnection_DefaultsToSkip()
    {
        await this.Seed();
        var exported = ConnectionTransferService.Serialize(await this.transfers.Export(null, this.Ct));

        // Re-imported onto the instance that wrote it.
        var preview = await this.transfers.Preview(exported, null, this.Ct);

        var candidate = Assert.Single(preview.Candidates);
        Assert.True(candidate.Conflicts);
        Assert.Equal(ImportAction.Skip, candidate.Suggested);
    }

    [Fact]
    public async Task ANewConnection_DefaultsToAdd()
    {
        await this.Seed();
        var exported = ConnectionTransferService.Serialize(await this.transfers.Export(null, this.Ct));

        var (_, otherTransfers) = this.BuildSeparateInstance();
        var preview = await otherTransfers.Preview(exported, null, this.Ct);

        var candidate = Assert.Single(preview.Candidates);
        Assert.False(candidate.Conflicts);
        Assert.Equal(ImportAction.Add, candidate.Suggested);
    }

    [Fact]
    public async Task Skipping_WritesNothing()
    {
        await this.Seed();
        var exported = ConnectionTransferService.Serialize(await this.transfers.Export(null, this.Ct));

        var (otherProfiles, otherTransfers) = this.BuildSeparateInstance();
        var preview = await otherTransfers.Preview(exported, null, this.Ct);

        var outcome = await otherTransfers.Import(
            preview.Bundle,
            preview.Candidates.ToDictionary(c => c.Connection.Id, _ => ImportAction.Skip),
            null,
            this.Ct);

        Assert.Equal(1, outcome.Skipped);
        Assert.Empty(await otherProfiles.List(this.Ct));
    }

    [Fact]
    public async Task Replacing_OverwritesInPlaceRatherThanDuplicating()
    {
        await this.Seed(connectionString: "Host=old");
        var exported = ConnectionTransferService.Serialize(await this.transfers.Export("pp", this.Ct));

        // Change it locally, then re-import over the top.
        var existing = (await this.profiles.List(this.Ct)).Single();
        await this.profiles.Save(existing, "Host=changed", null, this.Ct);

        var preview = await this.transfers.Preview(exported, "pp", this.Ct);
        var outcome = await this.transfers.Import(
            preview.Bundle,
            preview.Candidates.ToDictionary(c => c.Connection.Id, _ => ImportAction.Replace),
            "pp",
            this.Ct);

        Assert.Equal(1, outcome.Replaced);

        var landed = Assert.Single(await this.profiles.List(this.Ct));
        Assert.Equal("Host=old", this.profiles.RevealConnectionString(landed));
    }

    // ── Bad input ───────────────────────────────────────────────────────

    [Fact]
    public async Task RubbishJson_IsReportedRatherThanThrown()
    {
        var preview = await this.transfers.Preview("this is not json", null, this.Ct);

        Assert.NotNull(preview.Problem);
        Assert.Empty(preview.Candidates);
    }

    [Fact]
    public async Task AnEmptyBundle_IsReported()
    {
        var preview = await this.transfers.Preview("""{"version":1,"connections":[]}""", null, this.Ct);

        Assert.NotNull(preview.Problem);
    }

    [Fact]
    public async Task ANewerFormat_IsRefusedRatherThanMisread()
    {
        var preview = await this.transfers.Preview(
            """{"version":99,"connections":[{"id":"x","name":"Later","provider":"Sqlite"}]}""",
            null,
            this.Ct);

        Assert.NotNull(preview.Problem);
        Assert.Contains("newer version", preview.Problem);
    }

    [Fact]
    public async Task ASecretBearingFile_AsksForThePassphrase()
    {
        await this.Seed();
        var exported = ConnectionTransferService.Serialize(await this.transfers.Export("pp", this.Ct));

        var (_, otherTransfers) = this.BuildSeparateInstance();
        var preview = await otherTransfers.Preview(exported, null, this.Ct);

        Assert.True(preview.NeedsPassphrase);
    }

    // ── Demo mode ───────────────────────────────────────────────────────

    [Fact]
    public async Task DemoMode_RefusesBothDirections()
    {
        var (_, demoTransfers) = this.Build(demoMode: true);

        await Assert.ThrowsAsync<InvalidOperationException>(() => demoTransfers.Export(null, this.Ct));
        await Assert.ThrowsAsync<InvalidOperationException>(() => demoTransfers.Preview("{}", null, this.Ct));
        await Assert.ThrowsAsync<InvalidOperationException>(
            () => demoTransfers.Import(new ConnectionBundle(), new Dictionary<string, ImportAction>(), null, this.Ct));
    }

    /// <summary>A second instance with its own data directory - the case an export exists to serve.</summary>
    (ProfileStore, ConnectionTransferService) BuildSeparateInstance()
    {
        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["ShinyDocDbMyAdmin:DataDirectory"] = Path.Combine(this.directory, $"other_{Guid.NewGuid():N}"),
                // A different instance key on purpose: the passphrase, not this, is what carries a
                // secret across.
                ["ShinyDocDbMyAdmin:SecretKey"] = "a-completely-different-key"
            })
            .Build();

        var paths = new AppPaths(configuration);
        var protector = new SecretProtector(configuration, paths, NullLogger<SecretProtector>.Instance);
        var provided = new ProvidedConnections(configuration, NullLogger<ProvidedConnections>.Instance);
        var demo = new DemoMode(configuration, NullLogger<DemoMode>.Instance);
        var store = new ProfileStore(paths, protector, provided, demo);

        return (store, new ConnectionTransferService(store, demo, NullLogger<ConnectionTransferService>.Instance));
    }
}
