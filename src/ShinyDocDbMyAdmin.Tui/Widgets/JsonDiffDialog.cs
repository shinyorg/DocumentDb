using ShinyDocDbMyAdmin.Models;
using ShinyDocDbMyAdmin.Tui.Shell;
using XenoAtom.Terminal.UI;
using XenoAtom.Terminal.UI.Controls;

namespace ShinyDocDbMyAdmin.Tui.Widgets;

/// <summary>
/// The field-level difference between two versions of a document.
/// </summary>
/// <remarks>
/// A flat list of dotted paths rather than a side-by-side text diff. Two versions of a document differ
/// in a handful of fields inside a body that may be hundreds of lines, and a line diff of pretty-printed
/// JSON spends most of its height agreeing with itself. This is the same shape the web front end's
/// history tab shows, from the same <c>JsonComparer</c>.
/// </remarks>
public static class JsonDiffDialog
{
    public static Dialog Create(AdminShell shell, VersionComparison comparison)
    {
        var rows = new VStack().Spacing(0);

        if (comparison.IsIdentical)
        {
            rows.Add(Ui.Muted("The two versions are identical."));
        }
        else
        {
            foreach (var change in comparison.Changes)
            {
                var (glyph, colour) = change.Kind switch
                {
                    JsonChangeKind.Added => ("+", "green"),
                    JsonChangeKind.Removed => ("-", "red"),
                    _ => ("~", "yellow")
                };

                rows.Add(new HStack(
                    new Markup($"[{colour}]{glyph}[/]").MinWidth(2),
                    new Markup($"[bold]{Ui.Escape(change.Path)}[/]").MinWidth(30)
                ).Spacing(1));

                if (change.Before is { } before)
                    rows.Add(new Markup($"    [dim]before[/] [red]{Ui.Escape(Ui.Cell(before, 100))}[/]").Wrap(true));

                if (change.After is { } after)
                    rows.Add(new Markup($"    [dim]after [/] [green]{Ui.Escape(Ui.Cell(after, 100))}[/]").Wrap(true));
            }
        }

        var header = new VStack(
            new Markup(
                $"[bold]{Ui.Escape(comparison.Left.DocumentId)}[/]  " +
                $"[dim]version {comparison.Left.Version} → {comparison.Right.Version}[/]"),
            new Markup(
                $"[dim]{Ui.Escape(Ui.Timestamp(comparison.Left.ValidFrom))} → {Ui.Escape(Ui.Timestamp(comparison.Right.ValidFrom))}  ·  " +
                $"[/][green]{comparison.Added} added[/] [dim]·[/] [yellow]{comparison.Modified} modified[/] [dim]·[/] [red]{comparison.Removed} removed[/]")
        ).Spacing(0);

        var body = new VStack(
            header,
            new Rule(),
            new ScrollViewer(rows).MinWidth(86).MinHeight(12).MaxHeight(26)
        ).Spacing(1);

        Dialog? dialog = null;
        var close = Ui.Action("Close", () => shell.CloseDialog(dialog!)).AutoFocus(true);
        dialog = Modal.Create(shell, "Version diff", body, close);
        return dialog;
    }
}
