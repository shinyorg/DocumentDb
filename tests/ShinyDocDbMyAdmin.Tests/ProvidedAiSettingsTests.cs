using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging.Abstractions;
using ShinyDocDbMyAdmin.Providers;
using ShinyDocDbMyAdmin.Services;

namespace ShinyDocDbMyAdmin.Tests;

/// <summary>
/// Assistant configuration arriving from the host - an Aspire AppHost's <c>WithAi</c>, the terminal
/// tool's <c>--ai-*</c> flags, or plain environment variables. This is the contract
/// <c>Shiny.DocumentDb.Aspire.Hosting</c> writes, from the reading end.
/// </summary>
public class ProvidedAiSettingsTests
{
    static ProvidedAiSettings Build(params (string Key, string Value)[] settings)
    {
        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(settings.Select(x => new KeyValuePair<string, string?>(x.Key, x.Value)))
            .Build();

        return new ProvidedAiSettings(configuration, NullLogger<ProvidedAiSettings>.Instance);
    }

    [Fact]
    public void NoConfiguration_ProvidesNothing()
    {
        var ai = Build(("ConnectionStrings:orders", "Data Source=/tmp/orders.db"));

        Assert.False(ai.Any);
        Assert.False(ai.IsProvided("provided-orders"));
        Assert.Null(ai.Find("provided-orders"));
    }

    [Fact]
    public void TheInstanceDefault_AppliesToEveryConnection()
    {
        var ai = Build(
            ("ShinyDocDbMyAdmin:Ai:Provider", "Anthropic"),
            ("ShinyDocDbMyAdmin:Ai:Model", "claude-sonnet-4-5-20250929"),
            ("ShinyDocDbMyAdmin:Ai:ApiKey", "sk-ant-test"));

        // Including a saved (GUID) profile: configuring the assistant is an instance-level policy,
        // which is what makes `shinydocdb --ai-provider …` mean anything against your own connections.
        foreach (var profileId in new[] { "provided-orders", Guid.NewGuid().ToString("N") })
        {
            Assert.True(ai.IsProvided(profileId));

            var found = ai.Find(profileId)!;
            Assert.Equal(AiProviderKind.Anthropic, found.Settings.Provider);
            Assert.Equal("claude-sonnet-4-5-20250929", found.Settings.Model);
            Assert.Equal(profileId, found.Settings.ProfileId);
            Assert.Equal("sk-ant-test", found.ApiKey);
            Assert.True(found.Settings.Enabled);
        }
    }

    [Fact]
    public void APerStoreSection_LayersOverTheDefault()
    {
        var ai = Build(
            ("ShinyDocDbMyAdmin:Ai:Provider", "OpenAI"),
            ("ShinyDocDbMyAdmin:Ai:Model", "gpt-4o-mini"),
            ("ShinyDocDbMyAdmin:Ai:ApiKey", "sk-shared"),
            ("Shiny:DocumentDb:orders:Ai:Model", "gpt-4o"));

        var orders = ai.Find("provided-orders")!;
        Assert.Equal("gpt-4o", orders.Settings.Model);
        // Provider and key were not restated, so they come from the default - that is the whole point
        // of layering rather than replacing.
        Assert.Equal(AiProviderKind.OpenAI, orders.Settings.Provider);
        Assert.Equal("sk-shared", orders.ApiKey);

        Assert.Equal("gpt-4o-mini", ai.Find("provided-catalog")!.Settings.Model);
    }

    [Fact]
    public void APerStoreSection_WorksWithNoInstanceDefault()
    {
        var ai = Build(
            ("Shiny:DocumentDb:orders:Ai:Provider", "Anthropic"),
            ("Shiny:DocumentDb:orders:Ai:Model", "claude-haiku-4-5-20251001"));

        Assert.True(ai.IsProvided("provided-orders"));
        Assert.False(ai.IsProvided("provided-catalog"));
    }

    [Theory]
    [InlineData("azure", AiProviderKind.AzureOpenAI)]
    [InlineData("AzureOpenAI", AiProviderKind.AzureOpenAI)]
    [InlineData("openai", AiProviderKind.OpenAI)]
    [InlineData("anthropic", AiProviderKind.Anthropic)]
    [InlineData("compatible", AiProviderKind.OpenAiCompatible)]
    [InlineData("openai-compatible", AiProviderKind.OpenAiCompatible)]
    [InlineData("ollama", AiProviderKind.OpenAiCompatible)]
    public void ProviderNames_ParseIncludingTheCommandLineAliases(string name, AiProviderKind expected)
    {
        var ai = Build(
            ("ShinyDocDbMyAdmin:Ai:Provider", name),
            ("ShinyDocDbMyAdmin:Ai:Model", "some-model"));

        Assert.Equal(expected, ai.Find("provided-orders")!.Settings.Provider);
    }

    [Fact]
    public void AnUnknownProviderName_IsIgnoredRatherThanGuessed()
    {
        var ai = Build(
            ("ShinyDocDbMyAdmin:Ai:Provider", "gemini"),
            ("ShinyDocDbMyAdmin:Ai:Model", "gemini-2.0"));

        Assert.False(ai.Any);
    }

    [Fact]
    public void ProviderWithoutModel_IsIgnored()
    {
        // Half a configuration is a typo far more often than an intention, and a client built from it
        // would fail at first send with nothing to point at.
        Assert.False(Build(("ShinyDocDbMyAdmin:Ai:Provider", "OpenAI")).Any);
        Assert.False(Build(("ShinyDocDbMyAdmin:Ai:Model", "gpt-4o")).Any);
    }

    [Fact]
    public void DisableAi_WinsOverEverything()
    {
        // A host that removed the assistant has nothing to configure, and honouring the keys anyway
        // would hand an API key to a front end that refuses to render a chat.
        var ai = Build(
            ("ShinyDocDbMyAdmin:DisableAi", "true"),
            ("ShinyDocDbMyAdmin:Ai:Provider", "OpenAI"),
            ("ShinyDocDbMyAdmin:Ai:Model", "gpt-4o"),
            ("ShinyDocDbMyAdmin:Ai:ApiKey", "sk-should-not-be-read"));

        Assert.False(ai.Any);
        Assert.Null(ai.Find("provided-orders"));
    }

    [Fact]
    public void WriteToolsAreOffUnlessTheHostOptsIn()
    {
        var off = Build(
            ("ShinyDocDbMyAdmin:Ai:Provider", "OpenAI"),
            ("ShinyDocDbMyAdmin:Ai:Model", "gpt-4o")).Find("provided-orders")!.Settings;

        Assert.False(off.AllowInsert);
        Assert.False(off.AllowUpdate);
        Assert.False(off.AllowDelete);
        Assert.False(off.AllowsAnyWrite);

        var on = Build(
            ("ShinyDocDbMyAdmin:Ai:Provider", "OpenAI"),
            ("ShinyDocDbMyAdmin:Ai:Model", "gpt-4o"),
            ("ShinyDocDbMyAdmin:Ai:AllowInsert", "true"),
            ("ShinyDocDbMyAdmin:Ai:AllowUpdate", "true")).Find("provided-orders")!.Settings;

        Assert.True(on.AllowInsert);
        Assert.True(on.AllowUpdate);
        Assert.False(on.AllowDelete);
    }

    [Fact]
    public void EnabledFalse_StagesTheConfigurationWithoutTurningItOn()
    {
        var settings = Build(
            ("ShinyDocDbMyAdmin:Ai:Provider", "OpenAI"),
            ("ShinyDocDbMyAdmin:Ai:Model", "gpt-4o"),
            ("ShinyDocDbMyAdmin:Ai:ApiKey", "sk-test"),
            ("ShinyDocDbMyAdmin:Ai:Enabled", "false")).Find("provided-orders")!.Settings;

        Assert.False(settings.Enabled);
        Assert.False(settings.IsUsable());
    }

    [Fact]
    public void EachCallGetsItsOwnInstance()
    {
        // Callers hand these to UI screens that mutate them; a shared instance would leak one
        // connection's edits into the next.
        var ai = Build(
            ("ShinyDocDbMyAdmin:Ai:Provider", "OpenAI"),
            ("ShinyDocDbMyAdmin:Ai:Model", "gpt-4o"));

        var first = ai.Find("provided-orders")!.Settings;
        first.Model = "mutated";

        Assert.Equal("gpt-4o", ai.Find("provided-orders")!.Settings.Model);
    }
}
