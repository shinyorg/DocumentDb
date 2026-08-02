using XenoAtom.Terminal;
using XenoAtom.Terminal.UI;
using XenoAtom.Terminal.UI.Controls;
using XenoAtom.Terminal.UI.DataGrid;
using XenoAtom.Terminal.UI.Input;

namespace ShinyDocDbMyAdmin.Tui.Widgets;

/// <summary>How wide a column should be, in the small vocabulary the panels actually need.</summary>
public enum ColumnWidth
{
    /// <summary>As wide as its content.</summary>
    Fit,

    /// <summary>Takes one share of the leftover width.</summary>
    Fill,

    /// <summary>Takes two shares - for the one column that carries the most text.</summary>
    Wide
}

/// <summary>
/// A read-only table of <typeparamref name="T"/> with sorting, in-grid search and Enter-to-open.
/// </summary>
/// <remarks>
/// <para>
/// Eleven panels show a list of something, so this is the shape they share rather than eleven
/// near-identical <see cref="DataGridControl"/> setups. Every column is a string: the values come out
/// of a database as text or are formatted for display anyway, and letting each panel own its
/// formatting is simpler than teaching the grid about nine providers' idea of a number.
/// </para>
/// <para>
/// Selection is resolved through the <i>view</i>, not the document, so it stays correct after the
/// user sorts or searches - <c>SelectedRow</c> is a view index and the two stop agreeing the moment a
/// sort is applied.
/// </para>
/// </remarks>
public sealed class RowGrid<T> where T : class
{
    readonly DataGridListDocument<T> document = new();
    readonly DataGridDocumentView view;

    // The grid tracks its own document, but the empty-state swap is a decision about the grid rather
    // than inside it, so it needs a binding of its own to re-evaluate on.
    readonly State<int> revision = new(0);

    public RowGrid(params (BindingAccessor<string> Accessor, string Header, ColumnWidth Width)[] columns)
    {
        foreach (var (accessor, _, _) in columns)
            this.document.AddColumn(accessor);

        this.view = new DataGridDocumentView(this.document);

        this.Control = new DataGridControl
        {
            View = this.view,
            SelectionMode = DataGridSelectionMode.Row,
            ReadOnly = true,
            ShowRowAnchor = false
        };

        foreach (var (accessor, header, width) in columns)
        {
            this.Control.Columns.Add(new DataGridColumn<string>
            {
                Key = accessor.Name,
                TypedValueAccessor = accessor,
                Header = new Markup($"[bold]{Ui.Escape(header)}[/]"),
                Width = width switch
                {
                    ColumnWidth.Fill => GridLength.Star(1),
                    ColumnWidth.Wide => GridLength.Star(3),
                    _ => GridLength.Auto
                },
                Sortable = true,
                ReadOnly = true
            });
        }

        this.Control.AddKeyBinding(new KeyGesture(TerminalChar.CtrlF, TerminalModifiers.Ctrl), this.Control.OpenSearch);
    }

    public DataGridControl Control { get; }

    public int Count => this.document.Rows.Count;

    public IReadOnlyList<T> Rows => this.document.Rows;

    /// <summary>The model under the cursor, or null when the grid is empty.</summary>
    public T? Selected
    {
        get
        {
            var snapshot = this.view.CurrentSnapshot;
            var index = this.Control.SelectedRow;
            return index >= 0 && index < snapshot.RowCount ? snapshot.GetRowModel(index) as T : null;
        }
    }

    /// <summary>Swaps the whole contents in one update, so the grid repaints once rather than per row.</summary>
    public void Replace(IEnumerable<T> rows)
    {
        using (this.document.BeginUpdate())
        {
            var existing = this.document.Rows.Count;
            if (existing > 0)
                this.document.RemoveRows(0, existing);

            foreach (var row in rows)
                this.document.AddRow(row);
        }

        this.revision.Value++;
    }

    /// <summary>Enter on a row. The primary action of every list in the tool.</summary>
    public RowGrid<T> OnActivate(Action<T> handler)
    {
        this.Control.AddKeyBinding(new KeyGesture(TerminalKey.Enter), () =>
        {
            if (this.Selected is { } selected)
                handler(selected);
        });
        return this;
    }

    /// <summary>A second key for a secondary action - Delete on a document, say.</summary>
    public RowGrid<T> OnKey(KeyGesture gesture, Action<T> handler)
    {
        this.Control.AddKeyBinding(gesture, () =>
        {
            if (this.Selected is { } selected)
                handler(selected);
        });
        return this;
    }

    /// <summary>The grid, or an empty-state message when there is nothing in it.</summary>
    public Visual OrEmpty(string emptyMessage)
        => new ComputedVisual(() =>
        {
            _ = this.revision.Value;
            return this.Count == 0 ? Ui.Empty(emptyMessage) : this.Control;
        });
}
