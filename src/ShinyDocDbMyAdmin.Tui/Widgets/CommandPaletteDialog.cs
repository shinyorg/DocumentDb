using ShinyDocDbMyAdmin.Tui.Shell;
using XenoAtom.Terminal;
using XenoAtom.Terminal.UI;
using XenoAtom.Terminal.UI.Commands;
using XenoAtom.Terminal.UI.Controls;
using XenoAtom.Terminal.UI.Input;

namespace ShinyDocDbMyAdmin.Tui.Widgets;

/// <summary>
/// Everything the tool can do, by name.
/// </summary>
/// <remarks>
/// <para>
/// The address bar of a terminal app. The web front end reaches any page by URL; here the equivalent
/// is typing part of what you want, because a command you cannot currently see has no other way in.
/// </para>
/// <para>
/// Built from <see cref="Dialog"/> and <see cref="ListBox{T}"/> rather than from the framework's own
/// <c>CommandPalette</c>: that control hosts its results in a popup layer that only exists under a
/// running fullscreen loop, so it cannot be laid out - or screenshotted, or tested - anywhere else.
/// This is the same interaction over primitives that behave the same in both places.
/// </para>
/// </remarks>
public static class CommandPaletteDialog
{
    public static void Open(AdminShell shell, IReadOnlyList<Command> commands)
    {
        Dialog? dialog = null;

        var query = new State<string>("");
        var matches = commands.ToList();

        var list = new ListBox<string>([.. matches.Select(Describe)]);
        var input = Ui.Input(query).AutoFocus(true).MinWidth(64);

        var count = new State<string>(Summary(matches.Count, commands.Count));

        // Re-filtered on every keystroke rather than on submit: the list is the feedback that tells
        // you whether what you are typing is going to find anything.
        input.KeyDown(Refilter);
        input.TextInput(Refilter);

        input.AddKeyBinding(new KeyGesture(TerminalKey.Enter), Run);
        input.AddKeyBinding(new KeyGesture(TerminalKey.Down), () => Move(1));
        input.AddKeyBinding(new KeyGesture(TerminalKey.Up), () => Move(-1));
        list.AddKeyBinding(new KeyGesture(TerminalKey.Enter), Run);

        var content = new VStack(
            input,
            new Markup(() => count.Value),
            new ScrollViewer(list).MinWidth(64).MinHeight(8).MaxHeight(14)
        ).Spacing(1);

        dialog = Modal.Create(shell, "Commands", content, Ui.Action("Close", () => shell.CloseDialog(dialog!)));
        shell.OpenDialog(dialog);
        return;

        void Refilter()
        {
            var needle = query.Value.Trim();
            matches = needle.Length == 0
                ? commands.ToList()
                : [.. commands.Where(c => Haystack(c).Contains(needle, StringComparison.OrdinalIgnoreCase))];

            list.Items.Clear();
            foreach (var match in matches)
                list.Items.Add(Describe(match));

            list.SelectedIndex = matches.Count == 0 ? -1 : 0;
            count.Value = Summary(matches.Count, commands.Count);
        }

        void Move(int delta)
        {
            if (matches.Count == 0)
                return;

            list.SelectedIndex = Math.Clamp(list.SelectedIndex + delta, 0, matches.Count - 1);
        }

        void Run()
        {
            var index = list.SelectedIndex;
            if (index < 0 || index >= matches.Count)
                return;

            var chosen = matches[index];

            // Closed first: several commands push a screen, and running one behind a modal would
            // leave the new screen under a dialog nobody asked to keep open.
            shell.CloseDialog(dialog!);
            chosen.Execute?.Invoke(null!);
        }
    }

    static string Summary(int shown, int total)
        => shown == 0
            ? "[dim]nothing matches[/]"
            : $"[dim]{shown} of {total} command(s)[/]";

    static string Haystack(Command command)
        => $"{command.Name} {command.SearchText} {command.Id}";

    static string Describe(Command command)
    {
        var gesture = command.Gesture is { } key ? $"   [{key}]" : "";
        return command.Name is { Length: > 0 } name ? name + gesture : command.Id + gesture;
    }
}
