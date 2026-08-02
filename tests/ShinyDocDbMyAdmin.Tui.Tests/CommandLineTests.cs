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
}
