using ShinyDocDbMyAdmin.Tui.Shell;
using XenoAtom.Terminal;
using XenoAtom.Terminal.UI;
using XenoAtom.Terminal.UI.Controls;
using XenoAtom.Terminal.UI.Geometry;
using XenoAtom.Terminal.UI.Input;

namespace ShinyDocDbMyAdmin.Tui.Widgets;

/// <summary>
/// Modal dialogs: confirmations, prompts and forms.
/// </summary>
/// <remarks>
/// Every destructive action in the web front end is behind a confirm, and the same has to be true
/// here - more so, because a keyboard interface makes it easier to trigger something by accident than
/// a mouse does. <see cref="Confirm"/> takes the same wording the web app uses so the two tools warn
/// about the same things in the same words.
/// </remarks>
public static class Modal
{
    /// <summary>A dialog with a title, arbitrary content, and a row of buttons along the bottom.</summary>
    public static Dialog Create(AdminShell shell, string title, Visual content, params Visual[] buttons)
    {
        Dialog? dialog = null;

        var body = new VStack(
            content,
            new Rule(),
            new HStack(buttons).Spacing(1).HorizontalAlignment(Align.End)
        ).Spacing(1);

        dialog = new Dialog(new Markup($"[bold]{Ui.Escape(title)}[/]"), body)
            .Padding(new Thickness(1))
            .IsModal(true)
            .IsDraggable(true);

        // Escape closes, which is the only exit a keyboard user will look for first.
        dialog.AddKeyBinding(new KeyGesture(TerminalKey.Escape), () => shell.CloseDialog(dialog!));
        return dialog;
    }

    /// <summary>
    /// Opens a dialog whose buttons need to close it.
    /// </summary>
    /// <remarks>
    /// The button row is built from the dialog rather than handed in, because almost every button in it
    /// wants to call <c>CloseDialog</c> on the thing it lives inside - and there is no way to write that
    /// as one expression without something to close over first.
    /// </remarks>
    public static Dialog Open(AdminShell shell, string title, Visual content, Func<Dialog, Visual[]> buttons)
    {
        var row = new HStack().Spacing(1).HorizontalAlignment(Align.End);
        var dialog = Create(shell, title, content, row);

        row.Add(buttons(dialog));

        shell.OpenDialog(dialog);
        return dialog;
    }

    /// <summary>
    /// A yes/no with the destructive option coloured and never focused first.
    /// </summary>
    public static void Confirm(AdminShell shell, string title, string message, string confirmLabel, Action onConfirm)
    {
        Dialog? dialog = null;

        var cancel = Ui.Action("Cancel", () => shell.CloseDialog(dialog!)).AutoFocus(true);
        var confirm = Ui.Danger(confirmLabel, () =>
        {
            shell.CloseDialog(dialog!);
            onConfirm();
        });

        dialog = Create(shell, title, new Markup(Ui.Escape(message)).Wrap(true).MaxWidth(70), cancel, confirm);
        shell.OpenDialog(dialog);
    }

    /// <summary>A single-line text prompt - a file path, a name, a passphrase.</summary>
    public static void Prompt(
        AdminShell shell,
        string title,
        string label,
        string initial,
        Action<string> onAccept,
        bool isPassword = false
    )
    {
        Dialog? dialog = null;
        var value = new State<string>(initial);

        var input = Ui.Input(value).IsPassword(isPassword).AutoFocus(true).MinWidth(56);

        var cancel = Ui.Action("Cancel", () => shell.CloseDialog(dialog!));
        var accept = Ui.Primary("OK", Accept);

        var content = new VStack(Ui.Label(label), input).Spacing(0);
        dialog = Create(shell, title, content, cancel, accept);

        // Enter from the field is the same as pressing OK; typing a path and reaching for the mouse
        // is not how anyone uses a terminal.
        input.AddKeyBinding(new KeyGesture(TerminalKey.Enter), Accept);

        shell.OpenDialog(dialog);
        return;

        void Accept()
        {
            shell.CloseDialog(dialog!);
            onAccept(value.Value);
        }
    }

    /// <summary>A scrollable read-only text dialog - query plans, compiled SQL, error detail.</summary>
    public static void Show(AdminShell shell, string title, string text)
    {
        Dialog? dialog = null;
        var close = Ui.Action("Close", () => shell.CloseDialog(dialog!)).AutoFocus(true);

        var body = new ScrollViewer(new TextBlock(text).IsSelectable(true))
            .MinWidth(72)
            .MinHeight(12)
            .MaxHeight(28);

        dialog = Create(shell, title, body, close);
        shell.OpenDialog(dialog);
    }
}
