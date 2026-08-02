using ShinyDocDbMyAdmin.Models;
using ShinyDocDbMyAdmin.Services;
using ShinyDocDbMyAdmin.Tui.Shell;
using ShinyDocDbMyAdmin.Tui.Widgets;
using XenoAtom.Terminal.UI;
using XenoAtom.Terminal.UI.Commands;
using XenoAtom.Terminal.UI.Controls;
using XenoAtom.Terminal.UI.Geometry;

namespace ShinyDocDbMyAdmin.Tui.Screens;

/// <summary>One connection in a bundle being reviewed for import.</summary>
public sealed partial class ImportRow
{
    [Bindable] public partial string Name { get; set; }
    [Bindable] public partial string Provider { get; set; }
    [Bindable] public partial string Extras { get; set; }
    [Bindable] public partial string Existing { get; set; }
    [Bindable] public partial string Action { get; set; }

    public required ImportCandidate Candidate { get; init; }
    public ImportAction Decision { get; set; }
}

/// <summary>
/// Moving the connection list between instances - the same bundle format the web front end reads and
/// writes, because it is the same <see cref="ConnectionTransferService"/> producing it.
/// </summary>
/// <remarks>
/// <para>
/// The interesting decision, and the one this screen exists to make explicit, is what happens to
/// secrets. They are <b>left out by default</b>: a connection string is a credential, and an export
/// that carries them is a credential file - which is exactly what an export invites you to drop in a
/// ticket. Opting in re-encrypts them under a passphrase you type, not under this instance's key,
/// because the instance key would make the file useless anywhere but the machine that wrote it.
/// </para>
/// <para>
/// Import writes nothing until the review below has been confirmed, and defaults every conflict to
/// Skip. Overwriting a connection that already works, because a file mentioned the same name, is not
/// a decision to take on someone's behalf.
/// </para>
/// </remarks>
public sealed class ConnectionTransferScreen(AdminShell shell) : Screen(shell)
{
    readonly RowGrid<ImportRow> grid = new(
        (ImportRow.Accessor.Name, "Connection", ColumnWidth.Fill),
        (ImportRow.Accessor.Provider, "Provider", ColumnWidth.Fit),
        (ImportRow.Accessor.Extras, "Extras", ColumnWidth.Fit),
        (ImportRow.Accessor.Existing, "Already here?", ColumnWidth.Fill),
        (ImportRow.Accessor.Action, "Action", ColumnWidth.Fit)
    );

    readonly State<bool> includeSecrets = new(false);
    readonly State<string> exportPath = new("");
    readonly State<string> importPath = new("");
    readonly State<string> bundleFacts = new("");
    readonly State<string> problem = new("");

    string? importPassphrase;
    ImportPreview? preview;

    public override string Title => "Import / export";

    protected override Visual Build()
    {
        this.exportPath.Value = Path.Combine(Environment.CurrentDirectory, "shinydocdb-connections.json");
        this.grid.OnActivate(this.CycleDecision);

        var export = Ui.Panel("Export", new VStack(
            Ui.Muted(
                "By default the file lists which connections exist and how they are configured - not connection " +
                "strings, SQLCipher passwords or assistant API keys. Whoever imports it fills those in."
            ),
            new CheckBox(new TextBlock("Include secrets, encrypted under a passphrase you type"), this.includeSecrets),
            new ComputedVisual(() => this.includeSecrets.Value
                ? Ui.Warning(
                    "The file is then a credential file. The passphrase is stretched with PBKDF2-SHA256 and each " +
                    "secret encrypted with AES-GCM; there is no way to recover it, so a lost passphrase is a lost file.")
                : Ui.Nothing()),
            new VStack(Ui.Label("Write to"), Ui.Input(this.exportPath).MinWidth(64)).Spacing(0),
            Ui.Toolbar(Ui.Primary("Export", this.Export))
        ).Spacing(1));

        var import = Ui.Panel("Import", new VStack(
            new VStack(Ui.Label("Bundle file"), Ui.Input(this.importPath).MinWidth(64)).Spacing(0),
            Ui.Muted("Nothing is written until you review what it would do and confirm. Enter on a row cycles its action."),
            Ui.Toolbar(
                Ui.Action("Review", this.Review),
                Ui.Primary("Apply", this.Apply),
                Ui.Action("Add all new", () => this.SetAll(ImportAction.Add)),
                Ui.Action("Skip all", () => this.SetAll(ImportAction.Skip))
            ),
            new ComputedVisual(() => this.problem.Value.Length == 0 ? Ui.Nothing() : Ui.Failure(this.problem.Value)),
            new Markup(() => this.bundleFacts.Value),
            this.grid.OrEmpty("Point at a bundle and press Review.").Stretch()
        ).Spacing(1));

        return new Padder(new VStack(
            Ui.Title("Import / export connections"),
            Ui.Muted("Moves the connection list - with its saved queries and assistant settings - between instances. The web front end reads and writes the same file."),
            export,
            import.Stretch()
        ).Spacing(1)).Padding(new Thickness(1));
    }

    // ── Export ──────────────────────────────────────────────────────────

    void Export()
    {
        var path = this.exportPath.Value.Trim();
        if (path.Length == 0)
        {
            this.Shell.Warn("Give the export a path.");
            return;
        }

        if (!this.includeSecrets.Value)
        {
            this.WriteExport(path, null);
            return;
        }

        Modal.Prompt(this.Shell, "Export passphrase", "You will need this to import the file. It cannot be recovered.", "",
            passphrase =>
            {
                if (string.IsNullOrEmpty(passphrase))
                    this.Shell.Warn("No passphrase given - nothing was written.");
                else
                    this.WriteExport(path, passphrase);
            },
            isPassword: true);
    }

    void WriteExport(string path, string? passphrase) => this.Shell.RunAsync(async ct =>
    {
        var bundle = await this.Shell.Transfer.Export(passphrase, ct);
        await File.WriteAllTextAsync(path, ConnectionTransferService.Serialize(bundle), ct);

        this.Shell.Post(() => this.Shell.Success(
            $"Wrote {bundle.Connections.Count} connection(s) to {path}" +
            (passphrase is null ? ", without secrets." : ", with secrets encrypted under your passphrase.")));
    }, "Could not write the export");

    // ── Import ──────────────────────────────────────────────────────────

    void Review()
    {
        var path = this.importPath.Value.Trim();
        if (path.Length == 0)
        {
            this.Shell.Warn("Point at a bundle file first.");
            return;
        }

        if (!File.Exists(path))
        {
            this.problem.Value = $"No such file: {path}";
            return;
        }

        this.importPassphrase = null;
        this.Parse(path, null);
    }

    void Parse(string path, string? passphrase) => this.Shell.RunAsync(async ct =>
    {
        var json = await File.ReadAllTextAsync(path, ct);
        var result = await this.Shell.Transfer.Preview(json, passphrase, ct);

        this.Shell.Post(() =>
        {
            this.preview = result;
            this.importPassphrase = passphrase;
            this.problem.Value = result.Problem ?? "";
            this.Render(result);

            // Asked for only once the file has said it is encrypted, so a plain bundle never prompts.
            if (result.NeedsPassphrase)
            {
                Modal.Prompt(this.Shell, "Import passphrase", "This file carries encrypted secrets.", "",
                    typed => this.Parse(path, typed),
                    isPassword: true);
            }
        });
    }, "Could not read the bundle");

    /// <summary>
    /// Always called from inside a post - the rows are <c>[Bindable]</c>, and those refuse a write from
    /// anywhere but the render thread.
    /// </summary>
    void Render(ImportPreview result)
    {
        var rows = result.Candidates.Select(candidate => new ImportRow
        {
            Name = candidate.Name,
            Provider = candidate.Connection.Provider.ToString(),
            Extras = string.Join(" ", new[]
            {
                candidate.Connection.Ai is not null ? "assistant" : null,
                candidate.Connection.SavedQueries.Count > 0 ? $"{candidate.Connection.SavedQueries.Count} quer(y/ies)" : null,
                candidate.Connection.UploadedFileName is not null ? "was an uploaded file" : null
            }.Where(x => x is not null)),
            Existing = candidate.Existing is { } existing ? $"yes - '{existing.Name}'" : "no",
            Action = Describe(candidate.Suggested),
            Candidate = candidate,
            Decision = candidate.Suggested
        }).ToList();

        this.grid.Replace(rows);

        var secrets = result.Bundle.IncludesSecrets ? "with secrets" : "without secrets";
        var source = result.Bundle.Source is { Length: > 0 } from ? $" from {from}" : "";
        this.bundleFacts.Value = rows.Count == 0
            ? ""
            : $"[dim]{rows.Count} connection(s), exported {result.Bundle.ExportedAt.ToLocalTime():yyyy-MM-dd HH:mm}{Ui.Escape(source)}, {secrets}.[/]";
    }

    /// <summary>Enter cycles Skip → Add → Replace, so a decision is one key rather than a dropdown.</summary>
    void CycleDecision(ImportRow row)
    {
        row.Decision = row.Decision switch
        {
            ImportAction.Skip => ImportAction.Add,
            ImportAction.Add when row.Candidate.Conflicts => ImportAction.Replace,
            _ => ImportAction.Skip
        };

        row.Action = Describe(row.Decision);
    }

    void SetAll(ImportAction action)
    {
        foreach (var row in this.grid.Rows)
        {
            // "Add all new" means what it says: a conflict still needs a deliberate Replace.
            row.Decision = action == ImportAction.Add && row.Candidate.Conflicts ? ImportAction.Skip : action;
            row.Action = Describe(row.Decision);
        }
    }

    void Apply()
    {
        if (this.preview is not { } current || this.grid.Count == 0)
        {
            this.Shell.Warn("Review a bundle first.");
            return;
        }

        var decisions = this.grid.Rows.ToDictionary(r => r.Candidate.Connection.Id, r => r.Decision);
        var replacing = decisions.Count(d => d.Value == ImportAction.Replace);

        var message = replacing == 0
            ? $"Add {decisions.Count(d => d.Value == ImportAction.Add)} connection(s)?"
            : $"Add {decisions.Count(d => d.Value == ImportAction.Add)} and overwrite {replacing} existing connection(s)? Overwriting replaces the stored connection string and password.";

        Modal.Confirm(this.Shell, "Import connections", message, "Import", () => this.Shell.RunAsync(async ct =>
        {
            var outcome = await this.Shell.Transfer.Import(current.Bundle, decisions, this.importPassphrase, ct);

            this.Shell.Post(() =>
            {
                this.Shell.Success($"Added {outcome.Added}, replaced {outcome.Replaced}, skipped {outcome.Skipped}.");
                this.problem.Value = string.Join("  ", outcome.Errors);
                this.Shell.ReloadExplorer();
            });
        }, "Could not import the bundle"));
    }

    static string Describe(ImportAction action) => action switch
    {
        ImportAction.Add => "add",
        ImportAction.Replace => "replace",
        _ => "skip"
    };

    public override IEnumerable<Command> Commands() =>
    [
        new Command
        {
            Id = "transfer.export",
            Name = "Export connections",
            LabelMarkup = "Export connections",
            Execute = _ => this.Export()
        },
        new Command
        {
            Id = "transfer.review",
            Name = "Review a bundle",
            LabelMarkup = "Review a bundle",
            Execute = _ => this.Review()
        }
    ];
}
