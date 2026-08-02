using System.Reflection;
using Microsoft.Extensions.DependencyInjection;
using ShinyDocDbMyAdmin.Services;
using ShinyDocDbMyAdmin.Tui.Shell;
using XenoAtom.Terminal.UI;
using XenoAtom.Terminal.UI.Controls;

namespace ShinyDocDbMyAdmin.Tui.Widgets;

/// <summary>The keyboard map. In a terminal this is documentation, not a nicety.</summary>
public static class HelpDialog
{
    static readonly (string Keys, string What)[] Bindings =
    [
        ("Ctrl+P", "Command palette - everything the tool can do, by name"),
        ("F5", "Refresh the current screen and the explorer"),
        ("Esc", "Back one screen, or close the dialog"),
        ("F2", "Jump to the explorer - the tree is where navigation starts"),
        ("F1", "This list"),
        ("Ctrl+Q", "Quit"),
        ("", ""),
        ("Tab / Shift+Tab", "Move between controls"),
        ("↑ ↓ ← →", "Move within a grid, tree or list"),
        ("Enter", "Open the selected row"),
        ("Ctrl+F", "Search inside a grid"),
        ("", ""),
        ("Tab to the tabs, ← →", "Move between workspace tabs (or Ctrl+P → Next tab)"),
        ("PgUp / PgDn", "Previous / next page of documents"),
        ("Ctrl+S", "Save the open document or query"),
        ("Delete", "Delete the selected row - always asks first")
    ];

    public static Dialog Create(AdminShell shell)
    {
        var rows = new VStack().Spacing(0);
        foreach (var (keys, what) in Bindings)
        {
            if (keys.Length == 0)
            {
                rows.Add(new TextBlock(" "));
            }
            else
            {
                rows.Add(new HStack(
                    new Markup($"[bold]{Ui.Escape(keys)}[/]").MinWidth(18),
                    new Markup($"[dim]{Ui.Escape(what)}[/]")
                ).Spacing(1));
            }
        }

        Dialog? dialog = null;
        var close = Ui.Action("Close", () => shell.CloseDialog(dialog!)).AutoFocus(true);
        dialog = Modal.Create(shell, "Keyboard", rows, close);
        return dialog;
    }
}

/// <summary>Version, data directory and where the profile store actually is.</summary>
/// <remarks>
/// The data directory is the useful part: it is what the web front end also reads, and it is the
/// first thing to check when a connection saved in one does not appear in the other.
/// </remarks>
public static class AboutDialog
{
    public static Dialog Create(AdminShell shell)
    {
        var paths = shell.Services.GetRequiredService<AppPaths>();
        var version = Assembly.GetExecutingAssembly()
            .GetCustomAttribute<AssemblyInformationalVersionAttribute>()?.InformationalVersion ?? "unknown";

        var content = new VStack(
            new Markup("[bold]ShinyDocDbMyAdmin[/] [dim]terminal front end[/]"),
            new TextBlock(" "),
            Ui.Facts(
                ("Version", version.Split('+')[0]),
                ("Data directory", paths.DataDirectory),
                ("Profile store", paths.ProfileDatabasePath),
                ("Assistant", shell.AiAvailability.IsEnabled ? "available" : "disabled for this run")
            ),
            new TextBlock(" "),
            new Markup("[dim]The web front end reads the same profile store. Connections saved here appear there,[/]"),
            new Markup("[dim]and the other way round - as long as both are pointed at this directory.[/]")
        ).Spacing(0);

        Dialog? dialog = null;
        var close = Ui.Action("Close", () => shell.CloseDialog(dialog!)).AutoFocus(true);
        dialog = Modal.Create(shell, "About", content, close);
        return dialog;
    }
}
