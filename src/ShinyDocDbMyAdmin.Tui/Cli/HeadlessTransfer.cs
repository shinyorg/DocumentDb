using ShinyDocDbMyAdmin.Services;

namespace ShinyDocDbMyAdmin.Tui.Cli;

/// <summary>
/// <c>shinydocdb export</c> and <c>shinydocdb import</c>, run without drawing a UI.
/// </summary>
/// <remarks>
/// The same <see cref="ConnectionTransferService"/> the web front end's transfer page calls, so a file
/// written here is the file that page reads and vice versa. Having it on the command line as well as
/// in the UI is the difference between "you can move your connections" and "you can script moving your
/// connections" - onto a new laptop, into a dotfiles repo, across a team.
/// </remarks>
public static class HeadlessTransfer
{
    public static async Task<int> Export(
        ConnectionTransferService transfer,
        string path,
        bool withSecrets,
        CancellationToken ct
    )
    {
        string? passphrase = null;
        if (withSecrets)
        {
            passphrase = ReadPassphrase("Passphrase to encrypt the secrets with: ");
            if (string.IsNullOrEmpty(passphrase))
            {
                Console.Error.WriteLine("No passphrase given - nothing was written.");
                return 1;
            }

            if (ReadPassphrase("Repeat it: ") != passphrase)
            {
                Console.Error.WriteLine("The two passphrases differ - nothing was written.");
                return 1;
            }
        }

        var bundle = await transfer.Export(passphrase, ct);
        await File.WriteAllTextAsync(path, ConnectionTransferService.Serialize(bundle), ct);

        Console.WriteLine($"Wrote {bundle.Connections.Count} connection(s) to {path}.");
        Console.WriteLine(withSecrets
            ? "Secrets are included, encrypted under the passphrase you typed. Treat the file as a credential."
            : "Secrets were left out. Whoever imports this supplies them - re-run with --secrets to carry them.");

        return 0;
    }

    public static async Task<int> Import(ConnectionTransferService transfer, string path, CancellationToken ct)
    {
        if (!File.Exists(path))
        {
            Console.Error.WriteLine($"No such file: {path}");
            return 1;
        }

        var json = await File.ReadAllTextAsync(path, ct);
        var preview = await transfer.Preview(json, null, ct);

        // Asked for only once the file has been read far enough to know it is encrypted at all, so a
        // plain bundle never prompts.
        string? passphrase = null;
        if (preview.NeedsPassphrase)
        {
            passphrase = ReadPassphrase("This file carries encrypted secrets. Passphrase: ");
            preview = await transfer.Preview(json, passphrase, ct);
        }

        if (preview.Problem is { } problem)
        {
            Console.Error.WriteLine(problem);
            return 1;
        }

        Console.WriteLine($"{preview.Candidates.Count} connection(s) in {path}:");
        foreach (var candidate in preview.Candidates)
        {
            var note = candidate.Conflicts
                ? $"already here as '{candidate.Existing!.Name}'"
                : "new";
            Console.WriteLine($"  {candidate.Name,-32} {candidate.Connection.Provider,-12} {note}");
        }

        // Non-interactive default, matching the review screen's: add what is new, leave what exists
        // alone. Overwriting someone's working connection because a file mentioned the same name is
        // not a decision to take without being asked, and the UI is where that asking happens.
        var decisions = preview.Candidates.ToDictionary(
            c => c.Connection.Id,
            c => c.Suggested
        );

        var outcome = await transfer.Import(preview.Bundle, decisions, passphrase, ct);

        Console.WriteLine($"Added {outcome.Added}, replaced {outcome.Replaced}, skipped {outcome.Skipped}.");
        foreach (var error in outcome.Errors)
            Console.Error.WriteLine(error);

        if (outcome.Skipped > 0 && outcome.Errors.Count == 0)
            Console.WriteLine("Skipped connections already exist here. Run 'shinydocdb' and use Connections → Transfer to replace them.");

        return outcome.Errors.Count == 0 ? 0 : 1;
    }

    /// <summary>
    /// Reads a secret without echoing it. Falls back to a plain read when the input is redirected,
    /// because a script piping a passphrase in is a legitimate use and <c>ReadKey</c> would throw.
    /// </summary>
    static string ReadPassphrase(string prompt)
    {
        if (Console.IsInputRedirected)
            return Console.ReadLine() ?? "";

        Console.Write(prompt);
        var typed = new System.Text.StringBuilder();
        while (true)
        {
            var key = Console.ReadKey(intercept: true);
            if (key.Key == ConsoleKey.Enter)
            {
                Console.WriteLine();
                return typed.ToString();
            }

            if (key.Key == ConsoleKey.Backspace)
            {
                if (typed.Length > 0)
                    typed.Length--;
            }
            else if (!char.IsControl(key.KeyChar))
            {
                typed.Append(key.KeyChar);
            }
        }
    }
}
