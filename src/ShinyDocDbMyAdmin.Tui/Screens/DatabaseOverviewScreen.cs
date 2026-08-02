using ShinyDocDbMyAdmin.Models;
using ShinyDocDbMyAdmin.Providers;
using ShinyDocDbMyAdmin.Tui.Shell;
using ShinyDocDbMyAdmin.Tui.Widgets;
using XenoAtom.Terminal.UI;
using XenoAtom.Terminal.UI.Commands;
using XenoAtom.Terminal.UI.Controls;
using XenoAtom.Terminal.UI.Geometry;

namespace ShinyDocDbMyAdmin.Tui.Screens;

/// <summary>One table in the target database.</summary>
public sealed partial class TableRow
{
    [Bindable] public partial string Table { get; set; }
    [Bindable] public partial string Role { get; set; }
    [Bindable] public partial string Documents { get; set; }
    [Bindable] public partial string Types { get; set; }
    [Bindable] public partial string Size { get; set; }

    public required TableInfo Info { get; init; }
}

/// <summary>
/// What is in one connection: the browsable document tables, and the sidecars around them.
/// </summary>
/// <remarks>
/// Sidecars are listed rather than hidden. They are not browsable - the envelope is not in them - but
/// their presence is what tells you a type has history, blobs, geometry or vectors, and an admin tool
/// that silently omitted half the tables would be lying about the database.
/// </remarks>
public sealed class DatabaseOverviewScreen(AdminShell shell, string profileId) : Screen(shell)
{
    readonly RowGrid<TableRow> grid = new(
        (TableRow.Accessor.Table, "Table", ColumnWidth.Fill),
        (TableRow.Accessor.Role, "Role", ColumnWidth.Fit),
        (TableRow.Accessor.Documents, "Documents", ColumnWidth.Fit),
        (TableRow.Accessor.Types, "Types", ColumnWidth.Fit),
        (TableRow.Accessor.Size, "JSON size", ColumnWidth.Fit)
    );

    readonly State<string> heading = new("");
    readonly State<string> error = new("");

    ConnectionProfile? profile;

    public string ProfileId { get; } = profileId;

    public override string Title => this.profile?.Name ?? "Database";

    protected override Visual Build()
    {
        this.grid.OnActivate(row =>
        {
            if (row.Info.IsBrowsable)
                this.Shell.Push(new TableOverviewScreen(this.Shell, this.ProfileId, row.Info.Name));
            else
                this.Shell.Info($"'{row.Info.Name}' is a {row.Role} sidecar - it is maintained alongside a document table, not browsed on its own.");
        });

        var toolbar = Ui.Toolbar(
            Ui.Primary("Open", () => this.Activate()),
            Ui.Action("Query console", () => this.Shell.Push(new QueryConsoleScreen(this.Shell, this.ProfileId))),
            Ui.Action("Assistant", this.OpenAssistant).IsVisible(this.Shell.AiAvailability.IsEnabled),
            Ui.Action("Settings", () => this.Shell.Push(new ConnectionEditScreen(this.Shell, this.ProfileId))),
            Ui.Action("New table", this.CreateTable),
            Ui.Action("Refresh", () => this.Shell.Reload())
        );

        return new Padder(new VStack(
            new Markup(() => this.heading.Value),
            toolbar,
            new ComputedVisual(() => this.error.Value.Length == 0
                ? Ui.Nothing()
                : Ui.Failure(this.error.Value)),
            this.grid.OrEmpty(
                "No table here carries the DocumentDb envelope (Id / TypeName / Data / CreatedAt / UpdatedAt). " +
                "DocumentDb creates its tables on first write, so a store that has never run looks like this - " +
                "press New table and it will match what the library would have made."
            ).Stretch()
        ).Spacing(1)).Padding(new Thickness(1));
    }

    void Activate()
    {
        if (this.grid.Selected is { Info.IsBrowsable: true } row)
            this.Shell.Push(new TableOverviewScreen(this.Shell, this.ProfileId, row.Info.Name));
        else
            this.Shell.Info("Select a document table.");
    }

    public override async Task Load(CancellationToken ct)
    {
        var loaded = await this.Shell.Profiles.Get(this.ProfileId, ct);
        if (loaded is null)
        {
            this.Shell.Post(() =>
            {
                this.Shell.Warn("That connection no longer exists.");
                this.Shell.GoHome();
            });
            return;
        }

        var descriptor = ProviderCatalog.Get(loaded.Provider);
        var readOnly = loaded.ReadOnly ? "  [yellow]read-only[/]" : "";

        this.Shell.Post(() =>
        {
            this.profile = loaded;
            this.heading.Value = $"[bold]{Ui.Escape(loaded.Name)}[/]  [dim]{Ui.Escape(descriptor.DisplayName)}[/]{readOnly}";
        });

        List<(TableInfo Table, TableStats? Stats)> rows;
        try
        {
            var tables = await this.Shell.Admin.ListTables(this.ProfileId, refresh: true, ct);
            rows = [];

            foreach (var table in tables)
            {
                // Statistics are only meaningful for a documents table, and only cheap there - the
                // sidecars are listed for context, not measured.
                TableStats? stats = null;
                if (table.IsBrowsable)
                {
                    try
                    {
                        stats = await this.Shell.Admin.GetStats(this.ProfileId, table.Name, null, ct);
                    }
                    catch (Exception)
                    {
                        // One unreadable table should not blank the whole list.
                    }
                }

                rows.Add((table, stats));
            }

            // Built inside the post: a [Bindable] property refuses a write from anywhere but the
            // render thread, so a row constructed out here would throw.
            this.Shell.Post(() =>
            {
                this.error.Value = "";
                this.grid.Replace(rows.Select(entry => new TableRow
                {
                    Table = entry.Table.Name,
                    Role = entry.Table.Role == TableRole.Documents
                        ? (entry.Table.HasTenantColumn ? "documents · tenant" : "documents")
                        : entry.Table.Role.ToString().ToLowerInvariant(),
                    Documents = entry.Stats is null ? "" : Ui.Number(entry.Stats.DocumentCount),
                    Types = entry.Stats is null ? "" : Ui.Number(entry.Stats.TypeCount),
                    Size = entry.Stats is null ? "" : Ui.Bytes(entry.Stats.TotalJsonBytes),
                    Info = entry.Table
                }));

                this.Shell.Status.Value = $"{rows.Count(r => r.Table.IsBrowsable)} document table(s)";
            });
        }
        catch (Exception ex)
        {
            this.Shell.Post(() =>
            {
                this.error.Value = $"Could not read the schema. {ex.Message}";
                this.grid.Replace([]);
            });
        }
    }

    void CreateTable() => Modal.Prompt(
        this.Shell,
        "New documents table",
        "Table name - created with the provider's own DDL, so it matches what DocumentStore would have made.",
        "documents",
        table => this.Shell.RunAsync(async ct =>
        {
            await this.Shell.Admin.CreateDocumentsTable(this.ProfileId, table, ct);
            await this.Load(ct);
            this.Shell.Post(() =>
            {
                this.Shell.Success($"Created '{table}'.");
                this.Shell.ReloadExplorer();
            });
        }, "Could not create the table")
    );

    void OpenAssistant() => this.Shell.RunAsync(async ct =>
    {
        var settings = await this.Shell.Profiles.GetAiSettings(this.ProfileId, ct);
        this.Shell.Post(() => this.Shell.Push(settings is { Enabled: true }
            ? new AssistantScreen(this.Shell, this.ProfileId)
            : new AiSettingsScreen(this.Shell, this.ProfileId)));
    });

    public override IEnumerable<Command> Commands() =>
    [
        new Command
        {
            Id = "database.sql",
            Name = "Query console",
            LabelMarkup = "Query console",
            Execute = _ => this.Shell.Push(new QueryConsoleScreen(this.Shell, this.ProfileId))
        },
        new Command
        {
            Id = "database.newtable",
            Name = "New documents table",
            LabelMarkup = "New documents table",
            Execute = _ => this.CreateTable()
        }
    ];
}
