using System.Collections.Concurrent;
using ShinyDocDbMyAdmin.Models;
using ShinyDocDbMyAdmin.Services;
using XenoAtom.Terminal;
using XenoAtom.Terminal.UI;
using XenoAtom.Terminal.UI.Controls;
using XenoAtom.Terminal.UI.DataGrid;
using XenoAtom.Terminal.UI.Input;

namespace ShinyDocDbMyAdmin.Tui.Widgets;

/// <summary>A browse row: the envelope, plus whichever JSON paths are currently shown as columns.</summary>
public sealed class DocumentGridRow(DocumentRow row)
{
    public DocumentRow Row { get; } = row;

    /// <summary>
    /// One column's text. Envelope columns come from the envelope; anything else is a dotted path into
    /// the body.
    /// </summary>
    /// <remarks>
    /// A path the document does not carry reads as <c>null</c>, the same as one carrying an explicit
    /// null - which is what the web front end's grid shows for both. The distinction is real in a
    /// schema-free store, but it is the Structure tab's job to report it (a field below 100% present
    /// is absent from some documents) rather than something to encode in a cell two ways.
    /// </remarks>
    public string Read(string path) => path switch
    {
        "Id" => this.Row.Id,
        "TypeName" => this.Row.TypeName,
        "CreatedAt" => Ui.Timestamp(this.Row.CreatedAt),
        "UpdatedAt" => Ui.Timestamp(this.Row.UpdatedAt),
        "TenantId" => this.Row.TenantId ?? "",
        _ => this.Row.Body is null ? "" : Ui.Cell(JsonDisplay.Cell(this.Row.Read(path)))
    };
}

/// <summary>
/// The browse grid, whose columns are inferred from the documents rather than declared in code.
/// </summary>
/// <remarks>
/// <see cref="RowGrid{T}"/> covers the lists whose shape is known at compile time. This one cannot use
/// it: a schema-free store has no fixed columns, so the column set is rebuilt from
/// <c>InferSchema</c> on every load and each column is a <see cref="BindingAccessor{T}"/> closed over
/// its JSON path. Accessors are cached by path because the framework compares them by identity when
/// deciding what changed.
/// </remarks>
public sealed class DocumentGrid
{
    static readonly ConcurrentDictionary<string, BindingAccessor<string>> Accessors = new();

    readonly DataGridListDocument<DocumentGridRow> document = new();
    readonly DataGridDocumentView view;
    readonly State<int> revision = new(0);

    public DocumentGrid()
    {
        this.view = new DataGridDocumentView(this.document);
        this.Control = new DataGridControl
        {
            View = this.view,
            SelectionMode = DataGridSelectionMode.Row,
            ReadOnly = true,
            FrozenColumns = 1,
            ShowRowAnchor = false
        };

        this.Control.AddKeyBinding(new KeyGesture(TerminalChar.CtrlF, TerminalModifiers.Ctrl), this.Control.OpenSearch);
    }

    public DataGridControl Control { get; }

    public IReadOnlyList<string> Columns { get; private set; } = [];

    public int Count => this.document.Rows.Count;

    public DocumentRow? Selected
    {
        get
        {
            var snapshot = this.view.CurrentSnapshot;
            var index = this.Control.SelectedRow;
            return index >= 0 && index < snapshot.RowCount
                ? (snapshot.GetRowModel(index) as DocumentGridRow)?.Row
                : null;
        }
    }

    static BindingAccessor<string> AccessorFor(string path)
        => Accessors.GetOrAdd(path, p => new BindingAccessor<string>(
            p,
            instance => ((DocumentGridRow)instance).Read(p),

            // Read-only: editing happens in the JSON editor, where the whole body is in view. Letting
            // a cell rewrite one path would silently produce a document nobody reviewed.
            (_, _) => { }
        ));

    /// <summary>Rebuilds the column set. Call whenever the inferred schema or the user's choice changes.</summary>
    public void SetColumns(IReadOnlyList<string> paths)
    {
        this.Columns = paths;

        this.document.SetColumns([.. paths.Select(p =>
            new DataGridColumnInfo(p, p, typeof(string), ReadOnly: true, AccessorFor(p)))]);

        this.Control.Columns.Clear();
        foreach (var path in paths)
        {
            this.Control.Columns.Add(new DataGridColumn<string>
            {
                Key = path,
                TypedValueAccessor = AccessorFor(path),
                Header = new Markup($"[bold]{Ui.Escape(path)}[/]"),

                // Id is what you scan for, so it gets a real share of the width; everything else
                // divides what is left.
                Width = path == "Id" ? GridLength.Star(2) : GridLength.Star(1),
                MinWidth = 8,
                Sortable = false,
                ReadOnly = true
            });
        }

        this.revision.Value++;
    }

    /// <summary>
    /// Replaces the page. Sorting is deliberately left to the database - the grid holds one page, and
    /// sorting a page rather than the query would reorder twenty-five rows out of thousands and look
    /// like it had done something.
    /// </summary>
    public void SetRows(IEnumerable<DocumentRow> rows)
    {
        using (this.document.BeginUpdate())
        {
            var existing = this.document.Rows.Count;
            if (existing > 0)
                this.document.RemoveRows(0, existing);

            foreach (var row in rows)
                this.document.AddRow(new DocumentGridRow(row));
        }

        this.revision.Value++;
    }

    public void OnActivate(Action<DocumentRow> handler)
        => this.Control.AddKeyBinding(new KeyGesture(TerminalKey.Enter), () =>
        {
            if (this.Selected is { } row)
                handler(row);
        });

    public void OnKey(KeyGesture gesture, Action<DocumentRow> handler)
        => this.Control.AddKeyBinding(gesture, () =>
        {
            if (this.Selected is { } row)
                handler(row);
        });

    public Visual OrEmpty(string emptyMessage)
        => new ComputedVisual(() =>
        {
            _ = this.revision.Value;
            return this.Count == 0 ? Ui.Empty(emptyMessage) : this.Control;
        });
}
