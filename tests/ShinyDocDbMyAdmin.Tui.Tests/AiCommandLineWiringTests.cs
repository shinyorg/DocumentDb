using Microsoft.Extensions.DependencyInjection;
using ShinyDocDbMyAdmin.Providers;
using ShinyDocDbMyAdmin.Services;
using ShinyDocDbMyAdmin.Tui.Cli;

namespace ShinyDocDbMyAdmin.Tui.Tests;

/// <summary>
/// The seam between the <c>--ai-*</c> flags and the configuration the tool actually reads. Parsing
/// the flags and reading the configuration are both covered elsewhere; this is the join, which is
/// where a renamed key would break silently.
/// </summary>
public sealed class AiCommandLineWiringTests : IDisposable
{
    readonly string directory;

    public AiCommandLineWiringTests()
    {
        this.directory = Path.Combine(Path.GetTempPath(), $"docdbaicli_{Guid.NewGuid():N}");
        Directory.CreateDirectory(this.directory);
        SQLitePCL.Batteries_V2.Init();
    }

    public void Dispose()
    {
        try { Directory.Delete(this.directory, recursive: true); }
        catch (IOException) { /* not worth failing a green test over */ }
    }

    ProvidedAiSettings Resolve(params string[] args)
    {
        var options = CommandLineOptions.Parse([.. args, "--data-dir", this.directory]);
        Assert.Null(options.Error);

        var services = AdminHost.Build(options);
        return services.GetRequiredService<ProvidedAiSettings>();
    }

    [Fact]
    public void The_flags_land_in_the_section_the_tool_reads()
    {
        var ai = Resolve(
            "--ai-provider", "anthropic",
            "--ai-model", "claude-sonnet-4-5-20250929",
            "--ai-writes", "insert,update");

        var settings = ai.Find("some-profile");

        Assert.NotNull(settings);
        Assert.Equal(AiProviderKind.Anthropic, settings.Settings.Provider);
        Assert.Equal("claude-sonnet-4-5-20250929", settings.Settings.Model);
        Assert.True(settings.Settings.AllowInsert);
        Assert.True(settings.Settings.AllowUpdate);
        Assert.False(settings.Settings.AllowDelete);
    }

    [Fact]
    public void An_endpoint_flag_reaches_the_settings()
    {
        var ai = Resolve(
            "--ai-provider", "compatible",
            "--ai-model", "llama3",
            "--ai-endpoint", "http://localhost:11434/v1");

        Assert.Equal("http://localhost:11434/v1", ai.Find("some-profile")!.Settings.Endpoint);
    }

    [Fact]
    public void Without_the_flags_nothing_is_host_provided()
    {
        // The settings screen stays editable, which is the behaviour every existing install has.
        Assert.False(Resolve().Any);
    }

    [Fact]
    public void No_ai_leaves_nothing_configured()
    {
        Assert.False(Resolve("--no-ai").Any);
    }
}
