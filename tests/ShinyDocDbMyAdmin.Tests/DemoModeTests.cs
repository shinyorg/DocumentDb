using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging.Abstractions;
using ShinyDocDbMyAdmin.Models;
using ShinyDocDbMyAdmin.Providers;
using ShinyDocDbMyAdmin.Services;

namespace ShinyDocDbMyAdmin.Tests;

/// <summary>
/// The public-playground switch: what it closes, what it deliberately leaves open, and that closing
/// it is enforced below the UI rather than only by hiding buttons.
/// </summary>
public sealed class DemoModeTests
{
    static DemoMode Build(bool? demo)
    {
        var values = new Dictionary<string, string?>();
        if (demo is not null)
            values[DemoMode.ConfigurationKey] = demo.Value ? "true" : "false";

        var configuration = new ConfigurationBuilder().AddInMemoryCollection(values).Build();
        return new DemoMode(configuration, NullLogger<DemoMode>.Instance);
    }

    [Fact]
    public void OffByDefault()
    {
        var mode = Build(null);

        Assert.False(mode.IsEnabled);
        Assert.True(mode.CanManageConnections);
        Assert.True(mode.CanTransferSettings);
        Assert.True(mode.CanImportData);
    }

    [Fact]
    public void DemoMode_ClosesWhatItShould()
    {
        var mode = Build(true);

        Assert.True(mode.IsEnabled);
        Assert.False(mode.CanManageConnections);
        Assert.False(mode.CanTransferSettings);
        Assert.False(mode.CanImportData);
    }

    [Fact]
    public void ExportingData_StaysOpen()
    {
        // Deliberate: export is most of what someone is at a playground to try, it only reads, and
        // the data is a published sample. If this ever flips, it should be because someone decided
        // to - not because a blanket "demo means no transfers" rule swept it up.
        Assert.True(Build(true).CanExportData);
        Assert.True(Build(false).CanExportData);
    }

    [Fact]
    public void RebuildingTheSample_IsNeverOffered()
    {
        // Not conditional on the mode: nothing in the tool rebuilds the demo database, so a visitor
        // cannot wipe what everyone else came to look at.
        Assert.False(Build(true).CanRebuildDemoDatabase);
        Assert.False(Build(false).CanRebuildDemoDatabase);
    }

    [Theory]
    [InlineData("true")]
    [InlineData("True")]
    [InlineData("TRUE")]
    public void TheFlagIsCaseInsensitive(string value)
    {
        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?> { [DemoMode.ConfigurationKey] = value })
            .Build();

        Assert.True(DemoMode.IsEnabledIn(configuration));
    }

    [Fact]
    public void TheStaticReaderAgreesWithTheInstance()
    {
        // Program.cs reads it statically, before the container exists, to decide what to register at
        // all. The two must not be able to disagree.
        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?> { [DemoMode.ConfigurationKey] = "true" })
            .Build();

        Assert.Equal(
            DemoMode.IsEnabledIn(configuration),
            new DemoMode(configuration, NullLogger<DemoMode>.Instance).IsEnabled);
    }

    [Fact]
    public void TheKeysAreTheDocumentedOnes()
        // Named in the readme, the docs, the compose file and the UI's refusal screens.
        => Assert.Equal("ShinyDocDbMyAdmin:DemoMode", DemoMode.ConfigurationKey);

    // ── Enforcement, not decoration ─────────────────────────────────────

    [Fact]
    public void Asserts_ThrowInDemoMode()
    {
        var mode = Build(true);

        Assert.Throws<InvalidOperationException>(mode.AssertCanManageConnections);
        Assert.Throws<InvalidOperationException>(mode.AssertCanTransferSettings);

        // The import refusal has to mention that export still works, or it reads as "transfers are
        // broken" rather than "this half is deliberate".
        var ex = Assert.Throws<InvalidOperationException>(mode.AssertCanImportData);
        Assert.Contains("export", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Asserts_AreSilentOtherwise()
    {
        var mode = Build(false);

        mode.AssertCanManageConnections();
        mode.AssertCanTransferSettings();
        mode.AssertCanImportData();
    }

    [Fact]
    public async Task SavingAConnection_IsRefusedInDemoMode()
    {
        // The guard that matters: hiding the button is presentation, this is the actual door.
        var directory = Path.Combine(Path.GetTempPath(), $"docdbdemo_{Guid.NewGuid():N}");
        Directory.CreateDirectory(directory);
        try
        {
            var configuration = new ConfigurationBuilder()
                .AddInMemoryCollection(new Dictionary<string, string?>
                {
                    ["ShinyDocDbMyAdmin:DataDirectory"] = directory,
                    ["ShinyDocDbMyAdmin:SecretKey"] = "test-key",
                    [DemoMode.ConfigurationKey] = "true"
                })
                .Build();

            var paths = new AppPaths(configuration);
            var protector = new SecretProtector(configuration, paths, NullLogger<SecretProtector>.Instance);
            var provided = new ProvidedConnections(configuration, NullLogger<ProvidedConnections>.Instance);
            var demo = new DemoMode(configuration, NullLogger<DemoMode>.Instance);
            var profiles = new ProfileStore(paths, protector, provided, demo);

            var profile = new ConnectionProfile { Name = "Sneaky", Provider = ProviderKind.Sqlite };

            await Assert.ThrowsAsync<InvalidOperationException>(
                () => profiles.Save(profile, "Data Source=x.db", null, TestContext.Current.CancellationToken));

            await Assert.ThrowsAsync<InvalidOperationException>(
                () => profiles.Delete(profile.Id, TestContext.Current.CancellationToken));
        }
        finally
        {
            try { Directory.Delete(directory, recursive: true); }
            catch (IOException) { /* a driver still holding the file is not a test failure */ }
        }
    }
}
