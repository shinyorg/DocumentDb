using System.Text.Json;
using ShinyDocDbMyAdmin.Models;
using ShinyDocDbMyAdmin.Services;
using ShinyDocDbMyAdmin.Tui.Panels;
using XenoAtom.Terminal;
using XenoAtom.Terminal.UI;
using XenoAtom.Terminal.UI.Controls;
using XenoAtom.Terminal.UI.Input;

namespace ShinyDocDbMyAdmin.Tui.Widgets;

/// <summary>
/// The document editor - a JSON body in a code editor, saved through the same write path the web
/// front end uses.
/// </summary>
/// <remarks>
/// <para>
/// The web app's equivalent is <c>DocumentDialog</c>, and this keeps its two rules. The body is
/// validated as JSON before the save is attempted, because a store that accepts a malformed body will
/// hand it back malformed forever. And the sidecar outcome is reported rather than assumed: a write
/// here bypasses the library, so if the vector sidecar could not be updated the document was still
/// saved and the index is now stale, which is something you are told rather than something you find.
/// </para>
/// <para>
/// Read-only connections get the same editor with the save button gone - reading a document is not a
/// write, and refusing to show it would make read-only mean less than it does.
/// </para>
/// </remarks>
public static class JsonDocumentDialog
{
    public static void Open(WorkspaceContext context, DocumentRow? existing, Action onSaved)
    {
        var shell = context.Shell;
        var isNew = existing is null;

        var id = existing?.Id ?? Guid.NewGuid().ToString("N");
        var text = new State<string>(existing is null
            ? DocumentAdminService.NewDocumentTemplate(id)
            : DocumentAdminService.Prettify(existing.Json));

        var problem = new State<string>("");

        var editor = Ui.Code(text)
            .SyntaxHighlighter(JsonSyntaxHighlighter.Instance)
            .ShowLineNumbers(true)
            .IndentationSize(2)
            .AutoFocus(true)
            .MinWidth(80)
            .MinHeight(20);

        var header = new HStack(
            new Markup($"[dim]id[/] {Ui.Escape(id)}"),
            new Markup(() => context.ReadOnly.Value ? "[yellow]read-only connection[/]" : "")
        ).Spacing(2);

        var body = new VStack(
            header,
            editor.Stretch(),
            new Markup(() => problem.Value).Wrap(true)
        ).Spacing(1);

        Dialog? dialog = null;

        var buttons = new List<Visual>
        {
            Ui.Action("Format", () =>
            {
                try
                {
                    text.Value = DocumentAdminService.Prettify(text.Value);
                    problem.Value = "";
                }
                catch (JsonException ex)
                {
                    problem.Value = $"[red]{Ui.Escape(ex.Message)}[/]";
                }
            }),
            Ui.Action("Close", () => shell.CloseDialog(dialog!))
        };

        if (context.CanWrite)
            buttons.Insert(0, Ui.Primary("Save", Save));

        dialog = Modal.Create(shell, isNew ? "New document" : $"Edit {id}", body, [.. buttons]);

        if (context.CanWrite)
            editor.AddKeyBinding(new KeyGesture(TerminalChar.CtrlS, TerminalModifiers.Ctrl), Save);

        shell.OpenDialog(dialog);
        return;

        void Save()
        {
            // Checked here rather than left to the database: every backend rejects malformed JSON with
            // a different message, and none of them says which line.
            try
            {
                using var _ = JsonDocument.Parse(text.Value);
            }
            catch (JsonException ex)
            {
                problem.Value = $"[red]That is not valid JSON. {Ui.Escape(ex.Message)}[/]";
                return;
            }

            var json = text.Value;
            context.Run(async ct =>
            {
                var outcome = await context.Admin.SaveDocument(
                    context.ProfileId,
                    context.Table,
                    context.TypeName,
                    id,
                    json,
                    isNew,
                    ct
                );

                context.Post(() =>
                {
                    shell.CloseDialog(dialog!);
                    onSaved();

                    switch (outcome)
                    {
                        case VectorSyncOutcome.Unavailable:
                            shell.Warn("Saved, but the vector sidecar could not be updated - it still holds the previous embedding.");
                            break;

                        case VectorSyncOutcome.Ambiguous:
                            shell.Warn("Saved, but the body held no single embedding to sync, so the vector sidecar was left as it was.");
                            break;

                        default:
                            shell.Success(isNew ? "Document created." : "Document saved.");
                            break;
                    }
                });
            }, "Could not save the document");
        }
    }

    /// <summary>The read-only view of a body, for panels that show a document but never write one.</summary>
    public static void Show(Shell.AdminShell shell, string title, string json)
    {
        Dialog? dialog = null;

        var editor = new CodeEditor(DocumentAdminService.Prettify(json))
            .SyntaxHighlighter(JsonSyntaxHighlighter.Instance)
            .ShowLineNumbers(true)
            .AutoFocus(true)
            .MinWidth(80)
            .MinHeight(18);

        var close = Ui.Action("Close", () => shell.CloseDialog(dialog!));
        dialog = Modal.Create(shell, title, editor, close);
        shell.OpenDialog(dialog);
    }
}
