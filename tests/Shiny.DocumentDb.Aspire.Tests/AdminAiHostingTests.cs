using Aspire.Hosting;
using Aspire.Hosting.ApplicationModel;
using Shiny.DocumentDb.Aspire.Hosting;

namespace Shiny.DocumentDb.Aspire.Tests;

/// <summary>
/// The AppHost half of host-supplied assistant configuration: which environment variables reach the
/// tool. The reading end is covered by ShinyDocDbMyAdmin's ProvidedAiSettings tests - these two
/// suites are the two sides of one contract, so the key spellings must match exactly.
/// </summary>
public class AdminAiHostingTests
{
    static IDistributedApplicationBuilder CreateBuilder() => DistributedApplication.CreateBuilder();

    static async Task<IReadOnlyDictionary<string, string>> EnvOf(IResource resource)
    {
#pragma warning disable CS0618 // Simplest model-only resolution for tests, as elsewhere in this suite.
        return await ((IResourceWithEnvironment)resource)
            .GetEnvironmentVariableValuesAsync(DistributedApplicationOperation.Publish);
#pragma warning restore CS0618
    }

    [Fact]
    public async Task WithAi_WritesTheInstanceDefault()
    {
        var builder = CreateBuilder();

        var admin = builder
            .AddDocumentDbAdmin()
            .WithAi(AdminAiProvider.Anthropic, "claude-sonnet-4-5-20250929", "sk-ant-test");

        var env = await EnvOf(admin.Resource);

        Assert.Equal("Anthropic", env["ShinyDocDbMyAdmin__Ai__Provider"]);
        Assert.Equal("claude-sonnet-4-5-20250929", env["ShinyDocDbMyAdmin__Ai__Model"]);
        Assert.Equal("sk-ant-test", env["ShinyDocDbMyAdmin__Ai__ApiKey"]);
        Assert.Equal("true", env["ShinyDocDbMyAdmin__Ai__Enabled"]);
    }

    [Fact]
    public async Task WithAi_TakesTheKeyAsAParameter()
    {
        var builder = CreateBuilder();
        var key = builder.AddParameter("anthropic-key", secret: true);

        var admin = builder
            .AddDocumentDbAdmin()
            .WithAi(AdminAiProvider.Anthropic, "claude-sonnet-4-5-20250929", key);

        var env = await EnvOf(admin.Resource);

        // In Publish mode the value is a manifest expression referencing the parameter - the key being
        // present, and naming the parameter, proves the secret was wired rather than inlined.
        Assert.Contains("anthropic-key", env["ShinyDocDbMyAdmin__Ai__ApiKey"]);
    }

    [Fact]
    public async Task WithAi_AppliesToTheTerminalResourceToo()
    {
        var builder = CreateBuilder();

#pragma warning disable ASPIRETERMINAL001
        var terminal = builder
            .AddDocumentDbAdminTerminal()
            .WithAi(AdminAiProvider.OpenAI, "gpt-4o");
#pragma warning restore ASPIRETERMINAL001

        var env = await EnvOf(terminal.Resource);

        // Both front ends are one Core over one IConfiguration, so they take the same keys.
        Assert.Equal("OpenAI", env["ShinyDocDbMyAdmin__Ai__Provider"]);
        Assert.Equal("gpt-4o", env["ShinyDocDbMyAdmin__Ai__Model"]);
    }

    [Fact]
    public async Task WithAiFor_WritesThePerStoreOverride()
    {
        var builder = CreateBuilder();
        var store = builder.AddSqliteDocumentStore("orders", "/tmp/orders.db");

        var admin = builder
            .AddDocumentDbAdmin()
            .WithReference(store)
            .WithAi(AdminAiProvider.OpenAI, "gpt-4o-mini", "sk-shared")
            .WithAiFor(store, AdminAiProvider.OpenAI, "gpt-4o");

        var env = await EnvOf(admin.Resource);

        Assert.Equal("gpt-4o-mini", env["ShinyDocDbMyAdmin__Ai__Model"]);
        Assert.Equal("gpt-4o", env["Shiny__DocumentDb__orders__Ai__Model"]);
        // The override restates no key: the tool layers it over the default key by key.
        Assert.False(env.ContainsKey("Shiny__DocumentDb__orders__Ai__ApiKey"));
    }

    [Fact]
    public async Task WithAiWrites_IsOffUnlessAsked()
    {
        var builder = CreateBuilder();

        var admin = builder
            .AddDocumentDbAdmin()
            .WithAi(AdminAiProvider.OpenAI, "gpt-4o", "sk-test");

        var env = await EnvOf(admin.Resource);

        // WithAi alone grants nothing - a write tool the AppHost enabled silently is exactly what
        // nobody wants to discover later.
        Assert.False(env.ContainsKey("ShinyDocDbMyAdmin__Ai__AllowInsert"));
    }

    [Fact]
    public async Task WithAiWrites_EmitsEachFlagExplicitly()
    {
        var builder = CreateBuilder();

        var admin = builder
            .AddDocumentDbAdmin()
            .WithAi(AdminAiProvider.OpenAI, "gpt-4o", "sk-test")
            .WithAiWrites(insert: true, update: true);

        var env = await EnvOf(admin.Resource);

        Assert.Equal("true", env["ShinyDocDbMyAdmin__Ai__AllowInsert"]);
        Assert.Equal("true", env["ShinyDocDbMyAdmin__Ai__AllowUpdate"]);
        // Written as false rather than omitted, so it overrides anything already in the environment.
        Assert.Equal("false", env["ShinyDocDbMyAdmin__Ai__AllowDelete"]);
    }

    [Fact]
    public async Task WithAiWrites_ScopesToOneStore()
    {
        var builder = CreateBuilder();
        var store = builder.AddSqliteDocumentStore("orders", "/tmp/orders.db");

        var admin = builder
            .AddDocumentDbAdmin()
            .WithReference(store)
            .WithAi(AdminAiProvider.OpenAI, "gpt-4o", "sk-test")
            .WithAiWrites(insert: true, store: store);

        var env = await EnvOf(admin.Resource);

        Assert.Equal("true", env["Shiny__DocumentDb__orders__Ai__AllowInsert"]);
        Assert.False(env.ContainsKey("ShinyDocDbMyAdmin__Ai__AllowInsert"));
    }

    [Theory]
    [InlineData(AdminAiProvider.AzureOpenAI)]
    [InlineData(AdminAiProvider.OpenAiCompatible)]
    public void AProviderThatNeedsAnEndpoint_ThrowsWithoutOne(AdminAiProvider provider)
    {
        var builder = CreateBuilder();

        // Caught in the AppHost, where the mistake was made. The tool can only log and carry on with
        // no assistant, by which point the run is already up.
        var ex = Assert.Throws<ArgumentException>(() => builder
            .AddDocumentDbAdmin()
            .WithAi(provider, "some-deployment"));

        Assert.Contains("endpoint", ex.Message);
    }

    [Fact]
    public async Task AnEndpoint_IsPassedThrough()
    {
        var builder = CreateBuilder();

        var admin = builder
            .AddDocumentDbAdmin()
            .WithAi(AdminAiProvider.OpenAiCompatible, "llama3", endpoint: "http://localhost:11434/v1");

        var env = await EnvOf(admin.Resource);

        Assert.Equal("http://localhost:11434/v1", env["ShinyDocDbMyAdmin__Ai__Endpoint"]);
        // A local runtime usually ignores the key, so "no key" has to stay a working setup.
        Assert.False(env.ContainsKey("ShinyDocDbMyAdmin__Ai__ApiKey"));
    }

    [Fact]
    public async Task EnabledFalse_StagesWithoutSwitchingOn()
    {
        var builder = CreateBuilder();

        var admin = builder
            .AddDocumentDbAdmin()
            .WithAi(AdminAiProvider.OpenAI, "gpt-4o", "sk-test", enabled: false);

        var env = await EnvOf(admin.Resource);

        Assert.Equal("false", env["ShinyDocDbMyAdmin__Ai__Enabled"]);
    }
}
