using ShinyDocDbMyAdmin.Tui.Cli;

namespace ShinyDocDbMyAdmin.Tui.Tests;

/// <summary>
/// The command line. Small surface, but the one place a typo has to produce an error rather than a
/// silently different run - a mistyped <c>--profile</c> that opened the connection list instead would
/// look like the tool losing the argument.
/// </summary>
public sealed class CommandLineTests
{
    [Fact]
    public void No_arguments_opens_the_connection_list()
    {
        var options = CommandLineOptions.Parse([]);

        Assert.Equal(CliVerb.Browse, options.Verb);
        Assert.Null(options.Error);
        Assert.Null(options.Profile);
    }

    [Theory]
    [InlineData("--profile")]
    [InlineData("-p")]
    public void A_profile_can_be_named(string flag)
    {
        var options = CommandLineOptions.Parse([flag, "prod"]);

        Assert.Equal(CliVerb.Browse, options.Verb);
        Assert.Equal("prod", options.Profile);
    }

    [Fact]
    public void Export_takes_its_path_positionally()
    {
        var options = CommandLineOptions.Parse(["export", "bundle.json", "--secrets"]);

        Assert.Equal(CliVerb.Export, options.Verb);
        Assert.Equal("bundle.json", options.File);
        Assert.True(options.Secrets);
    }

    [Fact]
    public void Import_takes_its_path_positionally()
    {
        var options = CommandLineOptions.Parse(["import", "bundle.json"]);

        Assert.Equal(CliVerb.Import, options.Verb);
        Assert.Equal("bundle.json", options.File);
    }

    [Fact]
    public void A_transfer_verb_without_a_file_is_an_error()
    {
        Assert.NotNull(CommandLineOptions.Parse(["export"]).Error);
        Assert.NotNull(CommandLineOptions.Parse(["import"]).Error);
    }

    [Fact]
    public void Secrets_only_applies_to_export()
    {
        var options = CommandLineOptions.Parse(["import", "bundle.json", "--secrets"]);

        Assert.NotNull(options.Error);
        Assert.Contains("export", options.Error);
    }

    [Fact]
    public void An_unknown_argument_is_refused_rather_than_ignored()
    {
        var options = CommandLineOptions.Parse(["--porfile", "prod"]);

        Assert.NotNull(options.Error);
        Assert.Contains("--porfile", options.Error);
    }

    [Fact]
    public void A_flag_missing_its_value_is_refused()
    {
        Assert.NotNull(CommandLineOptions.Parse(["--profile"]).Error);
        Assert.NotNull(CommandLineOptions.Parse(["--data-dir"]).Error);
        Assert.NotNull(CommandLineOptions.Parse(["--theme"]).Error);
    }

    [Fact]
    public void Only_the_two_themes_are_accepted()
    {
        Assert.Equal("light", CommandLineOptions.Parse(["--theme", "LIGHT"]).Theme);
        Assert.Equal("dark", CommandLineOptions.Parse(["--theme", "dark"]).Theme);
        Assert.NotNull(CommandLineOptions.Parse(["--theme", "solarized"]).Error);
    }

    [Fact]
    public void The_splash_can_be_turned_off()
    {
        Assert.False(CommandLineOptions.Parse([]).NoSplash);
        Assert.True(CommandLineOptions.Parse(["--no-splash"]).NoSplash);
        Assert.Null(CommandLineOptions.Parse(["--no-splash"]).Error);
    }

    [Fact]
    public void Help_and_version_short_circuit_everything_else()
    {
        Assert.Equal(CliVerb.Help, CommandLineOptions.Parse(["--help"]).Verb);
        Assert.Equal(CliVerb.Help, CommandLineOptions.Parse(["export", "x.json", "--help"]).Verb);
        Assert.Equal(CliVerb.Version, CommandLineOptions.Parse(["--version"]).Verb);
    }

    // ── Assistant configuration ─────────────────────────────────────────

    [Fact]
    public void The_assistant_can_be_configured_from_the_command_line()
    {
        var options = CommandLineOptions.Parse(
            ["--ai-provider", "anthropic", "--ai-model", "claude-sonnet-4-5-20250929"]);

        Assert.Null(options.Error);
        Assert.Equal("anthropic", options.AiProvider);
        Assert.Equal("claude-sonnet-4-5-20250929", options.AiModel);
    }

    [Fact]
    public void There_is_no_key_flag()
    {
        // A command line lands in ps output, shell history and CI logs. The key comes from
        // ShinyDocDbMyAdmin__Ai__ApiKey or the settings file instead.
        var options = CommandLineOptions.Parse(["--ai-key", "sk-ant-oops"]);

        Assert.Equal("Unknown argument '--ai-key'.", options.Error);
    }

    [Theory]
    [InlineData("--ai-provider", "openai")]
    [InlineData("--ai-model", "gpt-4o")]
    public void Provider_and_model_go_together(string flag, string value)
    {
        var options = CommandLineOptions.Parse([flag, value]);

        Assert.Equal("--ai-provider and --ai-model go together; pass both or neither.", options.Error);
    }

    [Fact]
    public void Configuring_the_assistant_conflicts_with_removing_it()
    {
        var options = CommandLineOptions.Parse(
            ["--no-ai", "--ai-provider", "openai", "--ai-model", "gpt-4o"]);

        Assert.NotNull(options.Error);
        Assert.Contains("--no-ai", options.Error);
    }

    [Fact]
    public void Endpoint_and_writes_need_a_provider()
    {
        var options = CommandLineOptions.Parse(["--ai-writes", "insert"]);

        Assert.Equal("--ai-endpoint and --ai-writes need --ai-provider and --ai-model.", options.Error);
    }

    [Fact]
    public void Write_tools_parse_as_a_comma_separated_list()
    {
        var parsed = CommandLineOptions.ParseWrites("insert, delete");

        Assert.NotNull(parsed);
        Assert.True(parsed.Value.Insert);
        Assert.False(parsed.Value.Update);
        Assert.True(parsed.Value.Delete);
    }

    [Fact]
    public void None_is_an_explicit_way_to_grant_nothing()
    {
        var parsed = CommandLineOptions.ParseWrites("none");

        Assert.NotNull(parsed);
        Assert.False(parsed.Value.Insert);
        Assert.False(parsed.Value.Update);
        Assert.False(parsed.Value.Delete);
    }

    [Fact]
    public void An_unknown_write_tool_is_an_error_rather_than_a_silent_drop()
    {
        Assert.Null(CommandLineOptions.ParseWrites("insert,drop-table"));

        var options = CommandLineOptions.Parse(
            ["--ai-provider", "openai", "--ai-model", "gpt-4o", "--ai-writes", "everything"]);

        Assert.Contains("Unknown --ai-writes value", options.Error);
    }
}
