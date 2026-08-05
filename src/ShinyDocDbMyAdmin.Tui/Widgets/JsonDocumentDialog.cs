using System.Text.Json;
using System.Text.Json.Nodes;
using Microsoft.Extensions.DependencyInjection;
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

        // Read once the dialog opens rather than on save: a banner over the editor is the version of
        // this that prevents the mistake, and the save-time guard is the one that catches it.
        var encryptedPaths = new State<string>("");
        var canReveal = new State<bool>(false);
        var keys = shell.Services.GetRequiredService<EncryptionKeyRing>();

        context.Run(async ct =>
        {
            var paths = await context.Admin.EncryptedPathsFor(context.ProfileId, context.Table, context.TypeName, ct);
            var available = paths.Count > 0 && await keys.IsAvailable(context.ProfileId, ct);

            if (paths.Count > 0)
            {
                context.Post(() =>
                {
                    encryptedPaths.Value =
                        $"[yellow]encrypted: {Ui.Escape(string.Join(", ", paths))} - leave the enc:… text exactly as it is; " +
                        "this tool cannot re-encrypt what you overtype[/]";
                    canReveal.Value = available;
                });
            }
        });

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
            new Markup(() => encryptedPaths.Value).Wrap(true),
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

        // Only offered when the connection actually holds keys. A Reveal button that always answers
        // "no key" is worse than no button.
        buttons.Insert(buttons.Count - 1,
            Ui.Action("Reveal protected", () => Reveal(context, text.Value)).IsVisible(() => canReveal.Value));

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

            Write(text.Value, allowDowngrade: false);
        }

        void Write(string json, bool allowDowngrade)
        {
            context.Run(async ct =>
            {
                VectorSyncOutcome outcome;
                try
                {
                    outcome = await context.Admin.SaveDocument(
                        context.ProfileId,
                        context.Table,
                        context.TypeName,
                        id,
                        json,
                        isNew,
                        allowDowngrade,
                        ct
                    );
                }
                catch (EncryptedFieldDowngradeException ex)
                {
                    // A confirmation rather than a failure: the operator may well have meant to do this,
                    // and what they need is the consequence spelled out, not a refusal to explain itself.
                    context.Post(() => Modal.Confirm(
                        shell,
                        "Save in clear text?",
                        $"{string.Join(", ", ex.Paths)} would be saved in clear text. The application will read " +
                        (ex.Paths.Count == 1 ? "it" : "them") + " back as plaintext and the value is no longer " +
                        "protected - nothing will error, and nothing else will notice.",
                        "Save in clear text",
                        () => Write(json, allowDowngrade: true)));

                    return;
                }

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

    /// <summary>
    /// Decrypts the body's protected values into a modal, one line per path.
    /// </summary>
    /// <remarks>
    /// A separate, explicit action rather than something the editor renders inline. Reading a protected
    /// value is the deliberate act the field exists to require, and it stays out of the grid entirely.
    /// </remarks>
    static void Reveal(WorkspaceContext context, string json)
    {
        var shell = context.Shell;
        var keys = shell.Services.GetRequiredService<EncryptionKeyRing>();

        JsonNode? body;
        try
        {
            body = JsonNode.Parse(json);
        }
        catch (JsonException)
        {
            shell.Warn("The body is not valid JSON, so there is nothing to read out of it.");
            return;
        }

        var paths = EncryptedFields.PathsIn(body);
        if (paths.Count == 0)
        {
            shell.Info("Nothing in this body is encrypted.");
            return;
        }

        context.Run(async ct =>
        {
            var lines = new List<string>(paths.Count);
            foreach (var path in paths)
            {
                var stored = EncryptedFields.Read(body, path)?.GetValue<string>();
                var revealed = await keys.Reveal(context.ProfileId, stored, ct);
                lines.Add(revealed.Succeeded ? $"{path} = {revealed.Text}" : $"{path} : {revealed.Message}");
            }

            context.Post(() => Modal.Show(shell, "Protected values", string.Join(Environment.NewLine, lines)));
        }, "Could not read the protected values");
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
