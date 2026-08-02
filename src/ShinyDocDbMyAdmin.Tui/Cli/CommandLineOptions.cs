namespace ShinyDocDbMyAdmin.Tui.Cli;

/// <summary>What the tool was asked to do, before any of it is done.</summary>
public enum CliVerb
{
    /// <summary>Open the terminal UI. The default.</summary>
    Browse,

    /// <summary>Write a connection bundle and exit, without drawing anything.</summary>
    Export,

    /// <summary>Read a connection bundle and exit.</summary>
    Import,

    /// <summary>Print usage and exit.</summary>
    Help,

    /// <summary>Print the version and exit.</summary>
    Version
}

/// <summary>
/// The command line, parsed.
/// </summary>
/// <remarks>
/// Hand-rolled rather than pulled from a parser package: the surface is four verbs and five options,
/// and a global tool that a person installs is a place to keep the dependency count honest. Anything
/// unrecognised is an error rather than a silent ignore - a mistyped <c>--profile</c> that opens the
/// connection list instead looks like the tool losing the argument.
/// </remarks>
public sealed record CommandLineOptions
{
    public CliVerb Verb { get; init; } = CliVerb.Browse;

    /// <summary>The bundle path for <see cref="CliVerb.Export"/> / <see cref="CliVerb.Import"/>.</summary>
    public string? File { get; init; }

    /// <summary>A profile name or id to open straight into.</summary>
    public string? Profile { get; init; }

    /// <summary>Overrides <c>ShinyDocDbMyAdmin:DataDirectory</c>.</summary>
    public string? DataDirectory { get; init; }

    /// <summary>Sets <c>ShinyDocDbMyAdmin:DisableAi</c>, removing the assistant from this run entirely.</summary>
    public bool NoAi { get; init; }

    /// <summary>Light or dark. Null follows whatever was last chosen in the UI.</summary>
    public string? Theme { get; init; }

    /// <summary>Carry secrets in an export, encrypted under a passphrase typed at the prompt.</summary>
    public bool Secrets { get; init; }

    /// <summary>Start straight into the UI, without drawing the mark first.</summary>
    public bool NoSplash { get; init; }

    /// <summary>Set when parsing failed; <see cref="Verb"/> is then meaningless.</summary>
    public string? Error { get; init; }

    public static CommandLineOptions Parse(string[] args)
    {
        var result = new CommandLineOptions();
        if (args.Length == 0)
            return result;

        var index = 0;
        switch (args[0])
        {
            case "export":
                result = result with { Verb = CliVerb.Export };
                index = 1;
                break;

            case "import":
                result = result with { Verb = CliVerb.Import };
                index = 1;
                break;

            case "help" or "--help" or "-h" or "-?":
                return result with { Verb = CliVerb.Help };

            case "version" or "--version":
                return result with { Verb = CliVerb.Version };
        }

        // The verbs take the bundle path positionally, which is how every other file-taking CLI reads.
        if (result.Verb is CliVerb.Export or CliVerb.Import && index < args.Length && !args[index].StartsWith('-'))
        {
            result = result with { File = args[index] };
            index++;
        }

        while (index < args.Length)
        {
            var arg = args[index];
            switch (arg)
            {
                case "--profile" or "-p":
                    if (++index >= args.Length)
                        return result with { Error = "--profile needs a connection name or id." };
                    result = result with { Profile = args[index] };
                    break;

                case "--data-dir":
                    if (++index >= args.Length)
                        return result with { Error = "--data-dir needs a path." };
                    result = result with { DataDirectory = args[index] };
                    break;

                case "--theme":
                    if (++index >= args.Length)
                        return result with { Error = "--theme needs 'light' or 'dark'." };
                    if (!args[index].Equals("light", StringComparison.OrdinalIgnoreCase) &&
                        !args[index].Equals("dark", StringComparison.OrdinalIgnoreCase))
                        return result with { Error = $"Unknown theme '{args[index]}'. Use 'light' or 'dark'." };
                    result = result with { Theme = args[index].ToLowerInvariant() };
                    break;

                case "--no-ai":
                    result = result with { NoAi = true };
                    break;

                case "--secrets":
                    result = result with { Secrets = true };
                    break;

                case "--no-splash":
                    result = result with { NoSplash = true };
                    break;

                case "--help" or "-h" or "-?":
                    return result with { Verb = CliVerb.Help };

                default:
                    return result with { Error = $"Unknown argument '{arg}'." };
            }
            index++;
        }

        if (result.Verb is CliVerb.Export or CliVerb.Import && result.File is null)
            return result with { Error = $"'{result.Verb.ToString().ToLowerInvariant()}' needs a file path." };

        if (result.Secrets && result.Verb != CliVerb.Export)
            return result with { Error = "--secrets only applies to 'export'." };

        return result;
    }

    public const string Usage = """
        shinydocdb - a terminal front end for Shiny.DocumentDb stores

        Usage:
          shinydocdb [options]                 open the connection list
          shinydocdb --profile <name|id>       open straight into a connection
          shinydocdb export <file> [--secrets] write the connection bundle and exit
          shinydocdb import <file>             read a connection bundle and exit
          shinydocdb help | version

        Options:
          -p, --profile <name|id>  connection to open (name match is case-insensitive)
              --data-dir <path>    where the tool keeps its own state
                                   (default ~/.shinydocdbmyadmin, same as the web front end)
              --theme <light|dark> override the saved theme for this run
              --no-ai              run without the assistant, and without registering it
              --no-splash          skip the Shiny mark on startup
              --secrets            include secrets in an export, encrypted under a passphrase
                                   you are prompted for. Left out by default: an export that
                                   carries connection strings is a credential file.
        """;
}
