using ShinyDocDbMyAdmin.Models;
using ShinyDocDbMyAdmin.Tui.Shell;
using ShinyDocDbMyAdmin.Tui.Widgets;
using XenoAtom.Terminal;
using XenoAtom.Terminal.UI;
using XenoAtom.Terminal.UI.Commands;
using XenoAtom.Terminal.UI.Controls;
using XenoAtom.Terminal.UI.Geometry;
using XenoAtom.Terminal.UI.Input;

namespace ShinyDocDbMyAdmin.Tui.Screens;

/// <summary>One distinct <c>TypeName</c> inside a documents table.</summary>
public sealed partial class TypeRow
{
    [Bindable] public partial string TypeName { get; set; }
    [Bindable] public partial string Documents { get; set; }
    [Bindable] public partial string Indexes { get; set; }

    public required DocumentTypeInfo Info { get; init; }
}

/// <summary>
/// The types inside one documents table - the equivalent of a database's table list, one level down.
/// </summary>
/// <remarks>
/// DocumentDb keeps every type in one table and tells them apart by the <c>TypeName</c> column, so
/// what phpMyAdmin would call a table is a <i>type</i> here. Both levels are shown because both are
/// real: the table is where the rows live, the type is what you actually browse.
/// </remarks>
public sealed class TableOverviewScreen(AdminShell shell, string profileId, string table) : Screen(shell)
{
    readonly RowGrid<TypeRow> grid = new(
        (TypeRow.Accessor.TypeName, "Type", ColumnWidth.Fill),
        (TypeRow.Accessor.Documents, "Documents", ColumnWidth.Fit),
        (TypeRow.Accessor.Indexes, "JSON indexes", ColumnWidth.Fit)
    );

    readonly State<string> facts = new("");
    readonly State<string> error = new("");

    public string ProfileId { get; } = profileId;
    public string Table { get; } = table;

    public override string Title => this.Table;

    protected override Visual Build()
    {
        this.grid
            .OnActivate(this.Open)
            .OnKey(new KeyGesture(TerminalKey.Delete), this.ClearType);

        var toolbar = Ui.Toolbar(
            Ui.Primary("Browse", () => this.WithSelection(this.Open)),
            Ui.Action("Query console", () => this.Shell.Push(new QueryConsoleScreen(this.Shell, this.ProfileId))),
            Ui.Danger("Empty type", () => this.WithSelection(this.ClearType)),
            Ui.Action("Refresh", () => this.Shell.Reload())
        );

        return new Padder(new VStack(
            new Markup($"[bold]{Ui.Escape(this.Table)}[/]  [dim]document types in this table[/]"),
            new Markup(() => this.facts.Value),
            toolbar,
            new ComputedVisual(() => this.error.Value.Length == 0 ? Ui.Nothing() : Ui.Failure(this.error.Value)),
            this.grid.OrEmpty("This table exists but holds no rows. A type appears here as soon as its first document is written.").Stretch()
        ).Spacing(1)).Padding(new Thickness(1));
    }

    void WithSelection(Action<TypeRow> action)
    {
        if (this.grid.Selected is { } row)
            action(row);
        else
            this.Shell.Info("Select a type first.");
    }

    void Open(TypeRow row)
        => this.Shell.Push(new TypeWorkspaceScreen(this.Shell, this.ProfileId, this.Table, row.Info.TypeName));

    public override async Task Load(CancellationToken ct)
    {
        try
        {
            var stats = await this.Shell.Admin.GetStats(this.ProfileId, this.Table, null, ct);
            var types = await this.Shell.Admin.ListTypes(this.ProfileId, this.Table, ct);

            // One index read for the whole table rather than one per type: the catalog query is the
            // expensive part and it returns everything anyway.
            var indexes = await this.Shell.Admin.ListIndexes(this.ProfileId, this.Table, ct);
            var byType = indexes.GroupBy(i => i.TypeName).ToDictionary(g => g.Key, g => g.Count());

            // Built inside the post: a [Bindable] property refuses a write from anywhere but the
            // render thread, so a row constructed out here would throw.
            this.Shell.Post(() =>
            {
                this.error.Value = "";
                this.grid.Replace(types.Select(type => new TypeRow
                {
                    TypeName = type.TypeName,
                    Documents = Ui.Number(type.Count),
                    Indexes = byType.TryGetValue(type.TypeName, out var count) ? Ui.Number(count) : "0",
                    Info = type
                }));
                this.facts.Value =
                    $"[dim]{Ui.Number(stats.DocumentCount)} documents · {Ui.Number(stats.TypeCount)} types · " +
                    $"{Ui.Escape(Ui.Bytes(stats.TotalJsonBytes))} of JSON · largest {Ui.Escape(Ui.Bytes(stats.LargestJsonBytes))}[/]";
            });
        }
        catch (Exception ex)
        {
            this.Shell.Post(() =>
            {
                this.error.Value = ex.Message;
                this.grid.Replace([]);
            });
        }
    }

    void ClearType(TypeRow row) => Modal.Confirm(
        this.Shell,
        "Empty type",
        $"Delete every '{row.Info.TypeName}' document in {this.Table}? {row.Documents} document(s) go, and so do their history, blob and vector sidecar rows. This cannot be undone from here.",
        "Delete them all",
        () => this.Shell.RunAsync(async ct =>
        {
            var deleted = await this.Shell.Admin.ClearType(this.ProfileId, this.Table, row.Info.TypeName, ct);
            await this.Load(ct);
            this.Shell.Post(() =>
            {
                this.Shell.Success($"Deleted {deleted} document(s).");
                this.Shell.ReloadExplorer();
            });
        }, "Could not empty the type")
    );

    public override IEnumerable<Command> Commands() =>
    [
        new Command
        {
            Id = "table.sql",
            Name = "Query console",
            LabelMarkup = "Query console",
            Execute = _ => this.Shell.Push(new QueryConsoleScreen(this.Shell, this.ProfileId))
        }
    ];
}
