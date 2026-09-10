using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging.Abstractions;
using ShinyDocDbMyAdmin.Models;
using ShinyDocDbMyAdmin.Providers;
using ShinyDocDbMyAdmin.Services;

namespace ShinyDocDbMyAdmin.Tests;

/// <summary>
/// What <see cref="ProfileStore"/> does once the host has configured the assistant: it wins over
/// anything stored, and the tool refuses to change it. Every read path in both front ends goes
/// through <c>GetAiSettings</c>, so proving it here proves it for the chat, the panels and the
/// "does this connection have an assistant" checks alike.
/// </summary>
public sealed class HostProvidedAiTests : IDisposable
{
    readonly string directory;
    readonly ProfileStore profiles;
    readonly string providedProfileId;

    public HostProvidedAiTests()
    {
        this.directory = Path.Combine(Path.GetTempPath(), $"docdbhostai_{Guid.NewGuid():N}");
        Directory.CreateDirectory(this.directory);

        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["ShinyDocDbMyAdmin:DataDirectory"] = Path.Combine(this.directory, "admin"),
                ["ShinyDocDbMyAdmin:SecretKey"] = "test-key",
                ["ConnectionStrings:store"] = $"Data Source={Path.Combine(this.directory, "store.db")}",
                ["Shiny:DocumentDb:store:Provider"] = "Sqlite",

                // What an AppHost's WithAi(...) writes.
                ["ShinyDocDbMyAdmin:Ai:Provider"] = "Anthropic",
                ["ShinyDocDbMyAdmin:Ai:Model"] = "claude-sonnet-4-5-20250929",
                ["ShinyDocDbMyAdmin:Ai:ApiKey"] = "sk-ant-from-the-host",
                ["ShinyDocDbMyAdmin:Ai:AllowInsert"] = "true"
            })
            .Build();

        var paths = new AppPaths(configuration);
        var protector = new SecretProtector(configuration, paths, NullLogger<SecretProtector>.Instance);
        var provided = new ProvidedConnections(configuration, NullLogger<ProvidedConnections>.Instance);
        var providedAi = new ProvidedAiSettings(configuration, NullLogger<ProvidedAiSettings>.Instance);

        this.profiles = new ProfileStore(paths, protector, provided, providedAi,
            new DemoMode(configuration, NullLogger<DemoMode>.Instance));
        this.providedProfileId = provided.Profiles.Single().Id;
    }

    public void Dispose()
    {
        if (Directory.Exists(this.directory))
        {
            try { Directory.Delete(this.directory, recursive: true); }
            catch (IOException) { /* a driver still holding the file is not a test failure */ }
        }
    }

    [Fact]
    public async Task GetAiSettings_ReturnsTheHostConfiguration_WithTheKeyEncrypted()
    {
        var settings = await this.profiles.GetAiSettings(this.providedProfileId, TestContext.Current.CancellationToken);

        Assert.NotNull(settings);
        Assert.Equal(AiProviderKind.Anthropic, settings.Provider);
        Assert.Equal("claude-sonnet-4-5-20250929", settings.Model);
        Assert.True(settings.AllowInsert);
        Assert.False(settings.AllowDelete);
        Assert.True(settings.IsUsable());

        // Arrives plaintext from configuration; everything downstream expects the stored shape.
        Assert.NotEqual("sk-ant-from-the-host", settings.ApiKey);
        Assert.Equal("sk-ant-from-the-host", this.profiles.RevealApiKey(settings));
    }

    [Fact]
    public void IsAiProvided_IsTrueForSavedConnectionsToo()
    {
        // The instance default is a policy for the whole tool, not only for what the host handed over.
        Assert.True(this.profiles.IsAiProvided(this.providedProfileId));
        Assert.True(this.profiles.IsAiProvided(Guid.NewGuid().ToString("N")));
    }

    [Fact]
    public async Task Saving_IsRefused()
    {
        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => this.profiles.SaveAiSettings(
            new AiConnectionSettings
            {
                ProfileId = this.providedProfileId,
                Provider = AiProviderKind.OpenAI,
                Model = "gpt-4o",
                Enabled = true
            },
            "sk-mine",
            TestContext.Current.CancellationToken));

        Assert.Contains("host environment", ex.Message);
    }

    [Fact]
    public async Task Deleting_IsRefused()
        => await Assert.ThrowsAsync<InvalidOperationException>(
            () => this.profiles.DeleteAiSettings(this.providedProfileId, TestContext.Current.CancellationToken));

    [Fact]
    public async Task TheHostConfiguration_IsNotMergedWithAStoredOne()
    {
        // Nothing can be stored for this connection any more, but a store that predates the host
        // configuration still has rows - and those must not show through.
        var settings = await this.profiles.GetAiSettings("some-older-profile", TestContext.Current.CancellationToken);

        Assert.NotNull(settings);
        Assert.Equal(AiProviderKind.Anthropic, settings.Provider);
    }
}
