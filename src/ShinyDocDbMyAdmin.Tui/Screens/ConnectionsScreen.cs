using Microsoft.Extensions.DependencyInjection;
using ShinyDocDbMyAdmin.Models;
using ShinyDocDbMyAdmin.Providers;
using ShinyDocDbMyAdmin.Services;
using ShinyDocDbMyAdmin.Tui.Shell;
using ShinyDocDbMyAdmin.Tui.Widgets;
using XenoAtom.Terminal;
using XenoAtom.Terminal.UI;
using XenoAtom.Terminal.UI.Commands;
using XenoAtom.Terminal.UI.Controls;
using XenoAtom.Terminal.UI.Geometry;
using XenoAtom.Terminal.UI.Input;

namespace ShinyDocDbMyAdmin.Tui.Screens;

/// <summary>One row of the connection list.</summary>
public sealed partial class ConnectionRow
{
    [Bindable] public partial string Name { get; set; }
    [Bindable] public partial string Provider { get; set; }
    [Bindable] public partial string Target { get; set; }
    [Bindable] public partial string Flags { get; set; }
    [Bindable] public partial string LastOpened { get; set; }

    public required ConnectionProfile Profile { get; init; }
    public required bool IsProvided { get; init; }
}

/// <summary>
/// The connection list - the terminal equivalent of the web front end's home page.
/// </summary>
/// <remarks>
/// Reads the same <see cref="ProfileStore"/> the web app does, so this list and that one are the same
/// list. Host-provided connections lead it and cannot be edited or removed here, exactly as they
/// cannot there: they are declared by an Aspire AppHost or by configuration, and this is not where
/// they are declared.
/// </remarks>
public sealed class ConnectionsScreen(AdminShell shell) : Screen(shell)
{
    readonly RowGrid<ConnectionRow> grid = new(
        (ConnectionRow.Accessor.Name, "Connection", ColumnWidth.Fill),
        (ConnectionRow.Accessor.Provider, "Provider", ColumnWidth.Fit),
        (ConnectionRow.Accessor.Target, "Target", ColumnWidth.Wide),
        (ConnectionRow.Accessor.Flags, "", ColumnWidth.Fit),
        (ConnectionRow.Accessor.LastOpened, "Last opened", ColumnWidth.Fit)
    );

    readonly State<string> summary = new("");

    public override string Title => "Connections";

    protected override Visual Build()
    {
        this.grid
            .OnActivate(row => this.Open(row))
            .OnKey(new KeyGesture(TerminalKey.Delete), this.Delete);

        var toolbar = Ui.Toolbar(
            Ui.Primary("Open", () => this.WithSelection(this.Open)),
            Ui.Action("New", () => this.Shell.Push(new ConnectionEditScreen(this.Shell, null))),
            Ui.Action("Edit", () => this.WithSelection(this.Edit)),
            Ui.Action("Test", () => this.WithSelection(this.Test)),
            Ui.Danger("Delete", () => this.WithSelection(this.Delete)),
            Ui.Action("Import / export", () => this.Shell.Push(new ConnectionTransferScreen(this.Shell)))
        );

        return new Padder(new VStack(
            Ui.Title("Connections"),
            new Markup(() => this.summary.Value),
            toolbar,
            this.grid.OrEmpty("No connections yet. Press New, or import a bundle exported from the web front end.").Stretch()
        ).Spacing(1)).Padding(new Thickness(1));
    }

    public override async Task Load(CancellationToken ct)
    {
        var profiles = await this.Shell.Profiles.List(ct);

        var directory = this.Shell.Services.GetRequiredService<AppPaths>().DataDirectory;

        // The rows are built inside the post, not before it: a [Bindable] property refuses to be
        // written from anywhere but the render thread, so constructing one here would throw.
        this.Shell.Post(() =>
        {
            this.grid.Replace(profiles.Select(profile =>
            {
                var descriptor = ProviderCatalog.Get(profile.Provider);
                var provided = this.Shell.Profiles.IsProvided(profile.Id);

                var flags = string.Join(" ", new[]
                {
                    profile.ReadOnly ? "read-only" : null,
                    provided ? "from host" : null,
                    profile.UploadedFileName is not null ? "managed file" : null
                }.Where(x => x is not null));

                return new ConnectionRow
                {
                    // The connection string is a credential. Show what it points at - the host or the
                    // file - and nothing else; the full value never leaves the edit form.
                    Name = profile.Name,
                    Provider = descriptor.DisplayName,
                    Target = Describe(profile, descriptor, this.Shell.Profiles),
                    Flags = flags,
                    LastOpened = profile.LastOpenedAt is { } when ? Ui.Timestamp(when) : "never",
                    Profile = profile,
                    IsProvided = provided
                };
            }));

            this.summary.Value = profiles.Count == 0
                ? "[dim]Nothing configured yet.[/]"
                : $"[dim]{profiles.Count} connection(s) in {Ui.Escape(directory)}[/]";
        });
    }

    /// <summary>
    /// What the connection points at, with the credentials taken out. For a file that is the path;
    /// for a server it is host and database, recovered from the connection string by key rather than
    /// by position so it reads correctly across every dialect's spelling.
    /// </summary>
    static string Describe(ConnectionProfile profile, ProviderDescriptor descriptor, ProfileStore profiles)
    {
        try
        {
            var connectionString = profiles.RevealConnectionString(profile);
            if (descriptor.IsFileBased)
                return SqliteConnectionStrings.ExtractDataSource(connectionString);

            string? host = null;
            string? database = null;
            foreach (var part in connectionString.Split(';', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
            {
                var eq = part.IndexOf('=');
                if (eq <= 0)
                    continue;

                var key = part[..eq].Trim();
                var value = part[(eq + 1)..].Trim();

                if (key.Equals("Host", StringComparison.OrdinalIgnoreCase) ||
                    key.Equals("Server", StringComparison.OrdinalIgnoreCase) ||
                    key.Equals("Data Source", StringComparison.OrdinalIgnoreCase))
                {
                    host = value;
                }
                else if (key.Equals("Database", StringComparison.OrdinalIgnoreCase) ||
                         key.Equals("Initial Catalog", StringComparison.OrdinalIgnoreCase))
                {
                    database = value;
                }
            }

            return (host, database) switch
            {
                (null, null) => "—",
                (not null, null) => host,
                (null, not null) => database,
                _ => $"{host}/{database}"
            };
        }
        catch (Exception)
        {
            // A profile whose secret will not decrypt - a replaced key file, most likely. The list
            // still has to render, and the edit form is where that gets diagnosed.
            return "(unreadable)";
        }
    }

    void WithSelection(Action<ConnectionRow> action)
    {
        if (this.grid.Selected is { } row)
            action(row);
        else
            this.Shell.Info("Select a connection first.");
    }

    void Open(ConnectionRow row) => this.Shell.RunAsync(async ct =>
    {
        await this.Shell.Profiles.MarkOpened(row.Profile.Id, ct);
        this.Shell.Post(() => this.Shell.Push(new DatabaseOverviewScreen(this.Shell, row.Profile.Id)));
    }, "Could not open the connection");

    void Edit(ConnectionRow row)
    {
        if (row.IsProvided)
            this.Shell.Warn("This connection comes from the host environment. Change it where it is declared.");
        else
            this.Shell.Push(new ConnectionEditScreen(this.Shell, row.Profile.Id));
    }

    void Test(ConnectionRow row) => this.Shell.RunAsync(async ct =>
    {
        var result = await this.Shell.Admin.TestConnection(row.Profile.Id, ct);
        this.Shell.Post(() => this.Shell.Success(result));
    }, $"'{row.Name}' did not answer");

    void Delete(ConnectionRow row)
    {
        if (row.IsProvided)
        {
            this.Shell.Warn("This connection comes from the host environment and cannot be removed here.");
            return;
        }

        var note = row.Profile.UploadedFileName is null
            ? "The database itself is not touched - only this connection to it."
            : $"The managed database file ({row.Profile.UploadedFileName}) is deleted with it.";

        Modal.Confirm(
            this.Shell,
            "Delete connection",
            $"Delete '{row.Name}'? {note}",
            "Delete",
            () => this.Shell.RunAsync(async ct =>
            {
                await this.Shell.Profiles.Delete(row.Profile.Id, ct);

                // The cached provider instance was built from this profile, so it is now pointed at
                // something that no longer exists.
                this.Shell.Services.GetRequiredService<ConnectionManager>().Evict(row.Profile.Id);
                await this.Load(ct);

                this.Shell.Post(() =>
                {
                    this.Shell.Success($"Deleted '{row.Name}'.");
                    this.Shell.ReloadExplorer();
                });
            }, "Could not delete the connection")
        );
    }

    public override IEnumerable<Command> Commands() =>
    [
        new Command
        {
            Id = "connections.new",
            Name = "New connection",
            LabelMarkup = "New connection",
            Execute = _ => this.Shell.Push(new ConnectionEditScreen(this.Shell, null))
        },
        new Command
        {
            Id = "connections.test",
            Name = "Test connection",
            LabelMarkup = "Test the selected connection",
            Execute = _ => this.WithSelection(this.Test)
        }
    ];
}
