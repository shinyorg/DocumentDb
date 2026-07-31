using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging.Abstractions;
using ShinyDocDbMyAdmin.Models;
using ShinyDocDbMyAdmin.Providers;
using ShinyDocDbMyAdmin.Services;

namespace ShinyDocDbMyAdmin.Tests;

/// <summary>
/// Storage and construction of the assistant's per-connection configuration.
/// </summary>
public sealed class AiSettingsTests : IDisposable
{
    readonly string directory;
    readonly ProfileStore profiles;
    readonly AiClientFactory factory;
    readonly string providedProfileId;

    public AiSettingsTests()
    {
        this.directory = Path.Combine(Path.GetTempPath(), $"docdbaiset_{Guid.NewGuid():N}");
        Directory.CreateDirectory(this.directory);

        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["ShinyDocDbMyAdmin:DataDirectory"] = Path.Combine(this.directory, "admin"),
                ["ShinyDocDbMyAdmin:SecretKey"] = "test-key",
                ["ConnectionStrings:store"] = $"Data Source={Path.Combine(this.directory, "store.db")}",
                ["Shiny:DocumentDb:store:Provider"] = "Sqlite"
            })
            .Build();

        var paths = new AppPaths(configuration);
        var protector = new SecretProtector(configuration, paths, NullLogger<SecretProtector>.Instance);
        var provided = new ProvidedConnections(configuration, NullLogger<ProvidedConnections>.Instance);

        this.profiles = new ProfileStore(paths, protector, provided,
            // Demo mode off: these exercise the write paths a demo instance closes.
            new DemoMode(configuration, NullLogger<DemoMode>.Instance));
        this.factory = new AiClientFactory(this.profiles, NullLogger<AiClientFactory>.Instance);
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

    static AiConnectionSettings Openai(string profileId) => new()
    {
        ProfileId = profileId,
        Provider = AiProviderKind.OpenAI,
        Model = "gpt-4o-mini",
        Enabled = true
    };

    // ── Storage ─────────────────────────────────────────────────────────

    [Fact]
    public async Task Settings_RoundTrip()
    {
        var saved = Openai("p1");
        await this.profiles.SaveAiSettings(saved, "sk-secret", TestContext.Current.CancellationToken);

        var loaded = await this.profiles.GetAiSettings("p1", TestContext.Current.CancellationToken);

        Assert.NotNull(loaded);
        Assert.Equal(AiProviderKind.OpenAI, loaded.Provider);
        Assert.Equal("gpt-4o-mini", loaded.Model);
        Assert.True(loaded.Enabled);
        Assert.Equal("sk-secret", this.profiles.RevealApiKey(loaded));
    }

    [Fact]
    public async Task TheApiKey_IsStoredAsCiphertext()
    {
        await this.profiles.SaveAiSettings(Openai("p1"), "sk-secret", TestContext.Current.CancellationToken);

        var loaded = await this.profiles.GetAiSettings("p1", TestContext.Current.CancellationToken);

        // What sits in the store must not be the key. Reveal is the only way back to it.
        Assert.NotNull(loaded!.ApiKey);
        Assert.DoesNotContain("sk-secret", loaded.ApiKey);
    }

    [Fact]
    public async Task ClearingTheKey_Sticks()
    {
        var settings = Openai("p1");
        await this.profiles.SaveAiSettings(settings, "sk-secret", TestContext.Current.CancellationToken);

        // A merging Upsert would leave the old ciphertext behind - the same trap profile saves avoid.
        await this.profiles.SaveAiSettings(settings, null, TestContext.Current.CancellationToken);

        var loaded = await this.profiles.GetAiSettings("p1", TestContext.Current.CancellationToken);
        Assert.Null(loaded!.ApiKey);
    }

    [Fact]
    public async Task Settings_CanBeSavedForAHostProvidedConnection()
    {
        // The reason these live in their own collection: ProfileStore.Save refuses to write the
        // *profile* of a provided connection, so settings on ConnectionProfile could never be saved.
        var settings = Openai(this.providedProfileId);

        await this.profiles.SaveAiSettings(settings, "sk-secret", TestContext.Current.CancellationToken);

        var loaded = await this.profiles.GetAiSettings(this.providedProfileId, TestContext.Current.CancellationToken);
        Assert.NotNull(loaded);
    }

    [Fact]
    public async Task Settings_AreScopedToTheirConnection()
    {
        await this.profiles.SaveAiSettings(Openai("p1"), "sk-one", TestContext.Current.CancellationToken);

        Assert.Null(await this.profiles.GetAiSettings("p2", TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task Settings_CanBeRemoved()
    {
        await this.profiles.SaveAiSettings(Openai("p1"), "sk-secret", TestContext.Current.CancellationToken);
        await this.profiles.DeleteAiSettings("p1", TestContext.Current.CancellationToken);

        Assert.Null(await this.profiles.GetAiSettings("p1", TestContext.Current.CancellationToken));
    }

    // ── Usability ───────────────────────────────────────────────────────

    [Fact]
    public void Disabled_IsNotUsable()
    {
        var settings = Openai("p1");
        settings.ApiKey = "cipher";
        Assert.True(settings.IsUsable());

        settings.Enabled = false;
        Assert.False(settings.IsUsable());
    }

    [Fact]
    public void MissingModel_IsNotUsable()
    {
        var settings = Openai("p1");
        settings.Model = "";
        Assert.False(settings.IsUsable());
    }

    [Fact]
    public void MissingEndpoint_IsNotUsableWhereOneIsRequired()
    {
        var settings = new AiConnectionSettings
        {
            ProfileId = "p1",
            Provider = AiProviderKind.AzureOpenAI,
            Model = "my-deployment",
            ApiKey = "cipher",
            Enabled = true
        };

        Assert.False(settings.IsUsable());

        settings.Endpoint = "https://x.openai.azure.com";
        Assert.True(settings.IsUsable());
    }

    [Fact]
    public void ALocalEndpoint_NeedsNoKey()
    {
        // The configuration that sends nothing off the machine must not be the one we block.
        var settings = new AiConnectionSettings
        {
            ProfileId = "p1",
            Provider = AiProviderKind.OpenAiCompatible,
            Model = "llama3",
            Endpoint = "http://localhost:11434/v1",
            Enabled = true
        };

        Assert.True(settings.IsUsable());
    }

    // ── Client construction ─────────────────────────────────────────────

    [Theory]
    [InlineData(AiProviderKind.OpenAI, null)]
    [InlineData(AiProviderKind.AzureOpenAI, "https://x.openai.azure.com")]
    [InlineData(AiProviderKind.Anthropic, null)]
    [InlineData(AiProviderKind.OpenAiCompatible, "http://localhost:11434/v1")]
    public async Task EveryProvider_BuildsAClient(AiProviderKind kind, string? endpoint)
    {
        var settings = new AiConnectionSettings
        {
            ProfileId = "p1",
            Provider = kind,
            Model = "some-model",
            Endpoint = endpoint,
            Enabled = true
        };

        await this.profiles.SaveAiSettings(settings, "sk-test", TestContext.Current.CancellationToken);
        var stored = await this.profiles.GetAiSettings("p1", TestContext.Current.CancellationToken);

        using var client = this.factory.Create(stored!);
        Assert.NotNull(client);
    }

    [Fact]
    public void AMissingKey_SaysWhichProviderWantsOne()
    {
        var settings = new AiConnectionSettings
        {
            ProfileId = "p1",
            Provider = AiProviderKind.OpenAI,
            Model = "gpt-4o-mini",
            Enabled = true
        };

        var ex = Assert.Throws<InvalidOperationException>(() => this.factory.Create(settings));
        Assert.Contains("OpenAI", ex.Message);
        Assert.Contains("API key", ex.Message);
    }

    [Fact]
    public async Task AMissingEndpoint_SaysSo()
    {
        var settings = new AiConnectionSettings
        {
            ProfileId = "p1",
            Provider = AiProviderKind.AzureOpenAI,
            Model = "my-deployment",
            Enabled = true
        };

        await this.profiles.SaveAiSettings(settings, "sk-test", TestContext.Current.CancellationToken);
        var stored = await this.profiles.GetAiSettings("p1", TestContext.Current.CancellationToken);

        var ex = Assert.Throws<InvalidOperationException>(() => this.factory.Create(stored!));
        Assert.Contains("endpoint", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task AMissingModel_NamesTheFieldByItsProviderSpecificLabel()
    {
        var settings = new AiConnectionSettings
        {
            ProfileId = "p1",
            Provider = AiProviderKind.AzureOpenAI,
            Endpoint = "https://x.openai.azure.com",
            Enabled = true
        };

        await this.profiles.SaveAiSettings(settings, "sk-test", TestContext.Current.CancellationToken);
        var stored = await this.profiles.GetAiSettings("p1", TestContext.Current.CancellationToken);

        // "deployment name" on Azure, not "model" - the whole point of the per-provider label.
        var ex = Assert.Throws<InvalidOperationException>(() => this.factory.Create(stored!));
        Assert.Contains("deployment name", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    // ── Catalog ─────────────────────────────────────────────────────────

    [Fact]
    public void EveryProviderKind_HasACatalogEntry()
    {
        foreach (var kind in Enum.GetValues<AiProviderKind>())
        {
            var descriptor = AiProviderCatalog.Get(kind);
            Assert.False(string.IsNullOrWhiteSpace(descriptor.DisplayName));
            Assert.False(string.IsNullOrWhiteSpace(descriptor.Summary));

            if (descriptor.RequiresEndpoint)
                Assert.False(string.IsNullOrWhiteSpace(descriptor.EndpointPlaceholder));
        }
    }
}
