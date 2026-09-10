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

    /// <summary>
    /// Configures the assistant for this run, as <c>ShinyDocDbMyAdmin:Ai:*</c>. Doing so makes the
    /// configuration <b>host-owned</b>: the settings screen shows it read-only for every connection.
    /// </summary>
    /// <remarks>
    /// There is deliberately no <c>--ai-key</c>. A command line lands in <c>ps</c> output, shell
    /// history and CI logs, so the key comes from <c>ShinyDocDbMyAdmin__Ai__ApiKey</c> or the settings
    /// file instead - the two places a secret can live without being broadcast.
    /// </remarks>
    public string? AiProvider { get; init; }

    /// <inheritdoc cref="AiProvider" />
    public string? AiModel { get; init; }

    /// <inheritdoc cref="AiProvider" />
    public string? AiEndpoint { get; init; }

    /// <summary>Comma-separated write tools to opt in: <c>insert</c>, <c>update</c>, <c>delete</c>, or <c>none</c>.</summary>
    public string? AiWrites { get; init; }

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

                case "--ai-provider":
                    if (++index >= args.Length)
                        return result with { Error = "--ai-provider needs a name (openai, azure, anthropic, compatible)." };
                    result = result with { AiProvider = args[index] };
                    break;

                case "--ai-model":
                    if (++index >= args.Length)
                        return result with { Error = "--ai-model needs a model id (a deployment name on Azure)." };
                    result = result with { AiModel = args[index] };
                    break;

                case "--ai-endpoint":
                    if (++index >= args.Length)
                        return result with { Error = "--ai-endpoint needs a URL." };
                    result = result with { AiEndpoint = args[index] };
                    break;

                case "--ai-writes":
                    if (++index >= args.Length)
                        return result with { Error = "--ai-writes needs insert, update, delete (comma-separated) or none." };
                    if (ParseWrites(args[index]) is null)
                        return result with { Error = $"Unknown --ai-writes value '{args[index]}'. Use insert, update, delete or none." };
                    result = result with { AiWrites = args[index] };
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

        // Half a configuration is a typo far more often than an intention, and silently ignoring it
        // would leave someone staring at a settings screen that does not show what they just passed.
        if (result.AiProvider is null != result.AiModel is null)
            return result with { Error = "--ai-provider and --ai-model go together; pass both or neither." };

        if (result.NoAi && result.AiProvider is not null)
            return result with { Error = "--no-ai removes the assistant, so it cannot be combined with --ai-provider." };

        if (result.AiProvider is null && (result.AiEndpoint is not null || result.AiWrites is not null))
            return result with { Error = "--ai-endpoint and --ai-writes need --ai-provider and --ai-model." };

        return result;
    }

    /// <summary>
    /// The three write flags, or null when the value names something else. Returned as a tuple rather
    /// than parsed twice so the validation above and the configuration below cannot drift.
    /// </summary>
    public static (bool Insert, bool Update, bool Delete)? ParseWrites(string value)
    {
        var result = (Insert: false, Update: false, Delete: false);
        if (value.Trim().Equals("none", StringComparison.OrdinalIgnoreCase))
            return result;

        foreach (var part in value.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
        {
            switch (part.ToLowerInvariant())
            {
                case "insert": result.Insert = true; break;
                case "update": result.Update = true; break;
                case "delete": result.Delete = true; break;
                default: return null;
            }
        }

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
              --ai-provider <name> configure the assistant for this run: openai, azure,
                                   anthropic or compatible. Needs --ai-model too, and makes
                                   the configuration read-only in the settings screen.
              --ai-model <id>      model id, or the deployment name on Azure
              --ai-endpoint <url>  base URL, for azure and compatible
              --ai-writes <list>   write tools to allow: insert, update, delete or none
                                   (default none). The API key is NOT a flag - set
                                   ShinyDocDbMyAdmin__Ai__ApiKey, so it stays out of ps
                                   output and shell history.
              --no-splash          skip the Shiny mark on startup
              --secrets            include secrets in an export, encrypted under a passphrase
                                   you are prompted for. Left out by default: an export that
                                   carries connection strings is a credential file.
        """;
}
