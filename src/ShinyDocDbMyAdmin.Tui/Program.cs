using Microsoft.Extensions.DependencyInjection;
using ShinyDocDbMyAdmin.Services;
using ShinyDocDbMyAdmin.Tui;
using ShinyDocDbMyAdmin.Tui.Cli;
using ShinyDocDbMyAdmin.Tui.Screens;
using ShinyDocDbMyAdmin.Tui.Shell;
using XenoAtom.Terminal;
using XenoAtom.Terminal.UI;

var options = CommandLineOptions.Parse(args);

if (options.Error is { } error)
{
    Console.Error.WriteLine(error);
    Console.Error.WriteLine();
    Console.Error.WriteLine(CommandLineOptions.Usage);
    return 2;
}

switch (options.Verb)
{
    case CliVerb.Help:
        Console.WriteLine(CommandLineOptions.Usage);
        return 0;

    case CliVerb.Version:
        Console.WriteLine(AppInfo.Version);
        return 0;
}

// Drawn before anything is built so the container and the profile store open underneath it - the mark
// is up for the time startup already takes rather than on top of it.
var splash = options.Verb is CliVerb.Browse ? Splash.Show(options) : null;

await using var services = AdminHost.Build(options);

// The transfer verbs never draw anything: they are the scriptable half of the connections screen,
// and a script that has to drive a full-screen UI is not scriptable.
if (options.Verb is CliVerb.Export or CliVerb.Import)
{
    var transfer = services.GetRequiredService<ConnectionTransferService>();
    return options.Verb == CliVerb.Export
        ? await HeadlessTransfer.Export(transfer, options.File!, options.Secrets, CancellationToken.None)
        : await HeadlessTransfer.Import(transfer, options.File!, CancellationToken.None);
}

var shell = new AdminShell(services, options);
var start = await ResolveStartScreen(shell, options);
var root = shell.Build(start);
var started = false;

await Splash.Settle(splash);

await Terminal.RunAsync(
    root,
    context =>
    {
        if (!started)
        {
            started = true;

            // The first tick is the earliest the app object exists, and loading starts here rather
            // than before the loop for exactly that reason: until the shell has a dispatcher to post
            // through, a background load's results are applied inline - on the pool thread - and the
            // binding layer refuses a write from anywhere but the render thread.
            shell.Attach(context.App);
            shell.Start();

            // The tree is where navigation starts, so that is where the cursor starts.
            shell.FocusExplorer();
        }

        return shell.ExitRequested.Value
            ? TerminalLoopResult.Stop
            : TerminalLoopResult.Continue;
    },
    new TerminalRunOptions
    {
        // Auto lets the framework block on input instead of spinning a clock: an admin tool is idle
        // almost all of the time, and a terminal that repaints for nothing is a laptop fan.
        LoopMode = TerminalLoopMode.Auto,
        UpdateWaitDuration = TimeSpan.FromMilliseconds(250)
    },
    CancellationToken.None
);

shell.Shutdown();
return 0;

static async Task<Screen> ResolveStartScreen(AdminShell shell, CommandLineOptions options)
{
    if (options.Profile is null)
        return new ConnectionsScreen(shell);

    // Name first, then id: --profile is something a person types, and nobody types a GUID by choice.
    var profiles = await shell.Profiles.List();
    var match = profiles.FirstOrDefault(p => p.Name.Equals(options.Profile, StringComparison.OrdinalIgnoreCase))
                ?? profiles.FirstOrDefault(p => p.Id == options.Profile);

    if (match is null)
    {
        Console.Error.WriteLine($"No connection called '{options.Profile}'. Run 'shinydocdb' to see the list.");
        return new ConnectionsScreen(shell);
    }

    return new DatabaseOverviewScreen(shell, match.Id);
}
