using ShinyDocDbMyAdmin.Models;
using ShinyDocDbMyAdmin.Services;
using ShinyDocDbMyAdmin.Tui.Widgets;
using XenoAtom.Terminal;
using XenoAtom.Terminal.UI;
using XenoAtom.Terminal.UI.Controls;
using XenoAtom.Terminal.UI.Input;

namespace ShinyDocDbMyAdmin.Tui.Panels;

/// <summary>
/// The document grid: columns inferred from the data, JSON-path filters, quick search, paging, and
/// the JSON editor behind Enter.
/// </summary>
/// <remarks>
/// Filtering, sorting and paging all happen in the database, not in the grid. The grid holds one page
/// of a type that may have millions of documents, so a client-side sort would reorder twenty-five rows
/// and look like it had sorted the type.
/// </remarks>
public sealed class BrowsePanel(WorkspaceContext context) : WorkspacePanel(context)
{
    readonly DocumentGrid grid = new();
    readonly List<BrowseFilter> filters = [];

    readonly State<string> search = new("");
    readonly State<string> pageLabel = new("");
    readonly State<string> filterLabel = new("");
    readonly State<string> error = new("");

    // Plain fields, not State: the fetch runs on the thread pool, and a State may only be read from
    // the render thread. What the boxes hold is copied into these when the user applies them.
    int pageNumber = 1;
    string searchText = "";

    InferredSchema? schema;
    List<string> columns = [];
    BrowseSort sort = new("UpdatedAt", true, true);
    DocumentPage? current;

    /// <summary>The flag declared for this type on the connection, or null. Never inferred - see
    /// <c>DocumentAdminService.SoftDelete</c>.</summary>
    SoftDeleteFlag? softDelete;

    DeletedFilter deleted = DeletedFilter.Live;

    protected override Visual Build()
    {
        this.grid.OnActivate(this.Edit);
        this.grid.OnKey(new KeyGesture(TerminalKey.Delete), this.Delete);

        var searchBox = Ui.Input(this.search).MinWidth(24);
        searchBox.AddKeyBinding(new KeyGesture(TerminalKey.Enter), this.ApplySearch);

        var toolbar = Ui.Toolbar(
            Ui.Action("Refresh", () => this.Reload(reinfer: true)),
            Ui.Primary("New", this.Insert).IsVisible(() => this.Context.CanWrite),
            Ui.Action("Edit", () => this.WithSelection(this.Edit)),
            Ui.Danger("Delete", () => this.WithSelection(this.Delete)).IsVisible(() => this.Context.CanWrite),
            // Both appear only for a declared type: with no declaration there is nothing to restore from
            // and nothing to partition on.
            Ui.Action("Restore", () => this.WithSelection(this.Restore))
                .IsVisible(() => this.Context.CanWrite && this.softDelete is not null),
            Ui.Action("Live / deleted / all", this.CycleDeleted)
                .IsVisible(() => this.softDelete is not null),
            Ui.Action("Filters", this.EditFilters),
            Ui.Action("Columns", this.EditColumns),
            Ui.Action("Sort", this.EditSort),
            Ui.Label("Search"),
            searchBox,
            Ui.Action("Go", this.ApplySearch)
        );

        var paging = new HStack(
            Ui.Action("« First", () => this.GoTo(1)),
            Ui.Action("‹ Prev", () => this.GoTo(this.pageNumber - 1)),
            new Markup(() => this.pageLabel.Value).MinWidth(38),
            Ui.Action("Next ›", () => this.GoTo(this.pageNumber + 1)),
            Ui.Action("Last »", () => this.GoTo(this.current?.PageCount ?? 1))
        ).Spacing(1);

        var body = this.grid.OrEmpty("No documents match.");
        body.AddKeyBinding(new KeyGesture(TerminalKey.PageUp), () => this.GoTo(this.pageNumber - 1));
        body.AddKeyBinding(new KeyGesture(TerminalKey.PageDown), () => this.GoTo(this.pageNumber + 1));

        return new VStack(
            toolbar,
            new ComputedVisual(() => this.filterLabel.Value.Length == 0
                ? Ui.Nothing()
                : new Markup(this.filterLabel.Value)),
            new ComputedVisual(() => this.error.Value.Length == 0
                ? Ui.Nothing()
                : Ui.Failure(this.error.Value)),
            body.Stretch(),
            paging
        ).Spacing(1);
    }

    public override Task Load(CancellationToken ct) => this.Fetch(reinfer: true, ct);

    void Reload(bool reinfer) => this.Context.Run(ct => this.Fetch(reinfer, ct), "Could not read the documents");

    /// <summary>Takes what is in the search box - on the render thread, where it may be read.</summary>
    void ApplySearch()
    {
        this.searchText = this.search.Value;
        this.GoTo(1);
    }

    void GoTo(int target)
    {
        this.pageNumber = Math.Max(1, target);
        this.Reload(reinfer: false);
    }

    async Task Fetch(bool reinfer, CancellationToken ct)
    {
        try
        {
            var flag = await this.Context.Admin.GetSoftDeleteFlag(this.Context.ProfileId, this.Context.TypeName, ct);
            this.Context.Post(() => this.softDelete = flag);

            if (reinfer || this.schema is null)
            {
                var inferred = await this.Context.Admin.InferSchema(this.Context.ProfileId, this.Context.Table, this.Context.TypeName, ct: ct);
                var defaults = DocumentAdminService.DefaultColumns(inferred);

                this.Context.Post(() =>
                {
                    this.schema = inferred;

                    // Only chosen once. Re-deriving them on every refresh would undo a column the
                    // user had just turned off.
                    if (this.columns.Count == 0)
                    {
                        // Id leads, as it does in the web grid: it is what you scan for, and it is the
                        // handle every other tab asks you for.
                        this.columns = ["Id", .. defaults.Where(d => d != "Id")];
                        this.grid.SetColumns(this.columns);
                    }
                });
            }

            var query = new BrowseQuery
            {
                Page = this.pageNumber,
                PageSize = this.Shell.Settings.PageSize,
                Sort = this.sort,
                Filters = [.. this.filters.Where(f => !string.IsNullOrWhiteSpace(f.Path))],
                Search = this.searchText,
                SearchPaths = this.schema is null ? ["Id"] : DocumentAdminService.SearchableColumns(this.schema),
                Deleted = this.softDelete is null ? DeletedFilter.All : this.deleted
            };

            var result = await this.Context.Admin.Browse(this.Context.ProfileId, this.Context.Table, this.Context.TypeName, query, ct);

            this.Context.Post(() =>
            {
                this.current = result;
                this.error.Value = "";
                this.grid.SetRows(result.Rows, flag);

                // The scope is part of the count: "12 documents" under a live-only filter is a different
                // claim from "12 documents", and only one of them is true.
                var scope = flag is null ? "" : $" · {this.deleted.ToString().ToLowerInvariant()}";
                this.pageLabel.Value = $"[dim]page {result.Page} of {result.PageCount} · {Ui.Number(result.TotalCount)} document(s){scope}[/]";
                this.Shell.Status.Value = $"sorted by {this.sort.Path} {(this.sort.Descending ? "desc" : "asc")}";
            });
        }
        catch (Exception ex)
        {
            this.Context.Post(() =>
            {
                this.error.Value = ex.Message;
                this.grid.SetRows([]);
                this.pageLabel.Value = "";
            });
        }
    }

    void WithSelection(Action<DocumentRow> action)
    {
        if (this.grid.Selected is { } row)
            action(row);
        else
            this.Shell.Info("Select a document first.");
    }

    void Insert() => JsonDocumentDialog.Open(this.Context, null, () => this.Reload(reinfer: false));

    void Edit(DocumentRow row) => JsonDocumentDialog.Open(this.Context, row, () => this.Reload(reinfer: false));

    void Delete(DocumentRow row)
    {
        if (!this.Context.CanWrite)
        {
            this.Shell.Warn("This connection is read-only.");
            return;
        }

        if (this.softDelete is { } flag)
        {
            // A declared type is only ever flagged by the application, so that is what delete means here.
            // Removing the row for real is the Purge command, which says so.
            Modal.Confirm(
                this.Shell,
                "Flag deleted",
                $"Flag '{row.Id}' deleted? '{this.Context.TypeName}' is declared for soft delete on '{flag.PropertyPath}', so the document stays in the table and can be restored.",
                "Flag deleted",
                () => this.Context.Run(async ct =>
                {
                    await this.Context.Admin.SoftDeleteDocuments(this.Context.ProfileId, this.Context.Table, this.Context.TypeName, [row.Id], ct);
                    await this.Fetch(reinfer: false, ct);
                    this.Context.Post(() => this.Shell.Success($"Flagged '{row.Id}' deleted."));
                }, "Could not flag the document")
            );
            return;
        }

        Modal.Confirm(
            this.Shell,
            "Delete document",
            $"Delete '{row.Id}'? Its blob, vector and spatial sidecar rows go with it, and its history gains a Removed version.",
            "Delete",
            () => this.Context.Run(async ct =>
            {
                await this.Context.Admin.DeleteDocuments(this.Context.ProfileId, this.Context.Table, this.Context.TypeName, [row.Id], permanent: true, ct);
                await this.Fetch(reinfer: false, ct);
                this.Context.Post(() => this.Shell.Success($"Deleted '{row.Id}'."));
            }, "Could not delete the document")
        );
    }

    /// <summary>Clears the declared flag - <c>false</c> for a boolean, <c>null</c> for a timestamp.</summary>
    void Restore(DocumentRow row)
    {
        if (this.softDelete is null)
        {
            this.Shell.Info($"'{this.Context.TypeName}' has no declared soft-delete flag, so there is nothing to restore.");
            return;
        }

        this.Context.Run(async ct =>
        {
            await this.Context.Admin.RestoreDocuments(this.Context.ProfileId, this.Context.Table, this.Context.TypeName, [row.Id], ct);
            await this.Fetch(reinfer: false, ct);
            this.Context.Post(() => this.Shell.Success($"Restored '{row.Id}'."));
        }, "Could not restore the document");
    }

    void CycleDeleted()
    {
        this.deleted = this.deleted switch
        {
            DeletedFilter.Live => DeletedFilter.Deleted,
            DeletedFilter.Deleted => DeletedFilter.All,
            _ => DeletedFilter.Live
        };

        this.GoTo(1);
    }

    // ── Filters, columns and sort ───────────────────────────────────────

    IReadOnlyList<string> AllPaths =>
    [
        .. DocumentAdminService.EnvelopeColumnNames,
        .. this.schema?.Fields.Select(f => f.Path) ?? []
    ];

    /// <summary>
    /// One row of the filter editor. Holds its own controls so the dialog can read the whole form
    /// back on Apply rather than tracking every keystroke into a shared list.
    /// </summary>
    sealed class FilterEditorRow
    {
        static readonly FilterOperator[] Operators = Enum.GetValues<FilterOperator>();

        readonly TextBox path;
        readonly Select<string> op;
        readonly TextBox value;

        public FilterEditorRow(BrowseFilter filter, Action onRemove)
        {
            this.path = new TextBox(filter.Path).MinWidth(26);
            this.op = new Select<string>([.. Operators.Select(Describe)], Array.IndexOf(Operators, filter.Operator));
            this.value = new TextBox(filter.Value).MinWidth(22);

            this.View = new HStack(this.path, this.op, this.value, Ui.Action("Remove", onRemove)).Spacing(1);
        }

        public Visual View { get; }

        public BrowseFilter Read()
            => new(this.path.Typed().Trim(), Operators[Math.Max(0, this.op.SelectedIndex)], this.value.Typed());
    }

    void EditFilters()
    {
        var working = this.filters.ToList();
        var editors = new List<FilterEditorRow>();
        var rows = new VStack().Spacing(0);

        void Render()
        {
            editors.Clear();
            rows.Children.Clear();

            if (working.Count == 0)
            {
                rows.Add(Ui.Muted("No filters. Every document of this type is shown."));
                return;
            }

            for (var i = 0; i < working.Count; i++)
            {
                var index = i;
                var editor = new FilterEditorRow(working[index], () =>
                {
                    // Read the others back first, or removing row 2 discards what was typed into row 1.
                    working = [.. editors.Select(e => e.Read())];
                    working.RemoveAt(index);
                    Render();
                });

                editors.Add(editor);
                rows.Add(editor.View);
            }
        }

        Render();

        var content = new VStack(
            Ui.Muted("Paths are dotted JSON paths into the body, or one of Id / TypeName / CreatedAt / UpdatedAt / TenantId."),
            rows,
            Ui.Action("+ Add filter", () =>
            {
                working = [.. editors.Select(e => e.Read()), new BrowseFilter("", FilterOperator.Equals, "")];
                Render();
            })
        ).Spacing(1).MinWidth(88);

        Modal.Open(this.Shell, "Filters", content, dialog =>
        [
            Ui.Action("Clear all", () =>
            {
                working = [];
                Render();
            }),
            Ui.Action("Cancel", () => this.Shell.CloseDialog(dialog)),
            Ui.Primary("Apply", () =>
            {
                this.Shell.CloseDialog(dialog);
                this.filters.Clear();

                // A row left with a blank path is a row the user was still filling in, not a filter.
                this.filters.AddRange(editors.Select(e => e.Read()).Where(f => !string.IsNullOrWhiteSpace(f.Path)));

                this.filterLabel.Value = this.filters.Count == 0
                    ? ""
                    : $"[dim]filters: {Ui.Escape(string.Join(" and ", this.filters.Select(f => $"{f.Path} {Describe(f.Operator)} {f.Value}")))}[/]";

                this.GoTo(1);
            })
        ]);
    }

    void EditColumns()
    {
        if (this.schema is null)
        {
            this.Shell.Info("The schema has not been sampled yet.");
            return;
        }

        var chosen = this.columns.ToHashSet(StringComparer.Ordinal);
        var list = new VStack().Spacing(0);

        foreach (var path in this.AllPaths)
        {
            var current = path;
            var box = new CheckBox(new TextBlock(current), chosen.Contains(current));
            box.ValueChanged(() =>
            {
                if (box.IsChecked)
                    chosen.Add(current);
                else
                    chosen.Remove(current);
            });
            list.Add(box);
        }

        Modal.Open(this.Shell, "Columns", new ScrollViewer(list).MinWidth(56).MaxHeight(22), dialog =>
        [
            Ui.Action("Cancel", () => this.Shell.CloseDialog(dialog)),
            Ui.Primary("Apply", () =>
            {
                this.Shell.CloseDialog(dialog);

                // Kept in the order the schema reports rather than the order they were ticked, so the
                // grid does not reshuffle its columns every time one is added.
                this.columns = [.. this.AllPaths.Where(chosen.Contains)];
                if (this.columns.Count == 0)
                    this.columns = ["Id"];

                this.grid.SetColumns(this.columns);
            })
        ]);
    }

    void EditSort()
    {
        var paths = this.AllPaths.ToList();
        var select = new Select<string>(paths, Math.Max(0, paths.IndexOf(this.sort.Path)));
        var descending = new CheckBox(new TextBlock("Descending"), this.sort.Descending);

        var content = new VStack(
            Ui.Label("Sort by"),
            select,
            descending,
            Ui.Muted("Sorting happens in the database, so it orders the whole type rather than this page.")
        ).Spacing(1).MinWidth(48);

        Modal.Open(this.Shell, "Sort", content, dialog =>
        [
            Ui.Action("Cancel", () => this.Shell.CloseDialog(dialog)),
            Ui.Primary("Apply", () =>
            {
                this.Shell.CloseDialog(dialog);
                var path = paths[Math.Max(0, select.SelectedIndex)];
                this.sort = new BrowseSort(path, descending.IsChecked, DocumentAdminService.IsEnvelopeColumn(path));
                this.GoTo(1);
            })
        ]);
    }

    static string Describe(FilterOperator op) => op switch
    {
        FilterOperator.Equals => "=",
        FilterOperator.NotEquals => "≠",
        FilterOperator.Contains => "contains",
        FilterOperator.StartsWith => "starts with",
        FilterOperator.EndsWith => "ends with",
        FilterOperator.GreaterThan => ">",
        FilterOperator.GreaterOrEqual => "≥",
        FilterOperator.LessThan => "<",
        FilterOperator.LessOrEqual => "≤",
        FilterOperator.IsNull => "is null",
        _ => "is not null"
    };
}
