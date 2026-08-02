using ShinyDocDbMyAdmin.Models;
using ShinyDocDbMyAdmin.Services;
using ShinyDocDbMyAdmin.Tui.Widgets;
using XenoAtom.Terminal.UI;
using XenoAtom.Terminal.UI.Controls;

namespace ShinyDocDbMyAdmin.Tui.Panels;

/// <summary>
/// Bulk movement of documents in and out of a type, and the generator that makes more of them.
/// </summary>
/// <remarks>
/// <para>
/// Export streams straight to the file rather than buffering, so exporting a million documents costs
/// the same memory as exporting ten. The envelope format is the one that round-trips: it carries ids
/// and timestamps, where the plain body formats carry only what is inside <c>Data</c>.
/// </para>
/// <para>
/// The generator learns from the documents already there - numbers from the observed range, dates from
/// the observed span, categorical values from the set actually in use - so what it writes looks like
/// what is already in the type rather than like sample data. It is seeded, so the preview is a promise:
/// committing replays the same seed and writes the documents you were shown.
/// </para>
/// </remarks>
public sealed class TransferPanel(WorkspaceContext context) : WorkspacePanel(context)
{
    static readonly ExportFormat[] Formats = Enum.GetValues<ExportFormat>();
    static readonly ImportMode[] Modes = Enum.GetValues<ImportMode>();

    readonly State<string> exportPath = new("");
    readonly State<string> importPath = new("");
    readonly State<string> shapeNote = new("");
    readonly State<string> previewText = new("");
    readonly State<string> error = new("");
    readonly State<int> generateCount = new(25);

    Select<string>? formatSelect;
    Select<string>? modeSelect;

    DocumentShape? shape;
    GenerationPreview? preview;

    protected override Visual Build()
    {
        this.exportPath.Value = Path.Combine(Environment.CurrentDirectory, $"{this.Context.TypeName}.ndjson");

        this.formatSelect = new Select<string>([.. Formats.Select(Describe)], Array.IndexOf(Formats, ExportFormat.Ndjson));
        this.modeSelect = new Select<string>([.. Modes.Select(Describe)], Array.IndexOf(Modes, ImportMode.SkipExisting));

        var export = Ui.Panel("Export", new VStack(
            Ui.Muted("Streamed to the file - the whole type never sits in memory. Only the envelope format round-trips ids and timestamps."),
            new HStack(Ui.Label("Format"), this.formatSelect).Spacing(1),
            new VStack(Ui.Label("Write to"), Ui.Input(this.exportPath).MinWidth(64)).Spacing(0),
            Ui.Toolbar(Ui.Primary("Export", this.Export))
        ).Spacing(1));

        var import = Ui.Panel("Import", new VStack(
            Ui.Muted("A JSON array or an NDJSON stream. Both a bare body and the envelope form are accepted, so an export reads back."),
            new VStack(Ui.Label("Read from"), Ui.Input(this.importPath).MinWidth(64)).Spacing(0),
            new HStack(Ui.Label("On duplicate id"), this.modeSelect).Spacing(1),
            Ui.Toolbar(Ui.Primary("Import", this.Import).IsVisible(() => this.Context.CanWrite))
        ).Spacing(1));

        var generate = Ui.Panel("Generate test data", new VStack(
            Ui.Muted("Learns the shape of the documents already here and writes more like them. Seeded, so the preview is exactly what commit will write."),
            new HStack(
                Ui.Label("How many"),
                new NumberBox<int>(this.generateCount).MinWidth(10),
                Ui.Action("Analyse", this.Analyse),
                Ui.Action("Preview", this.BuildPreview),
                Ui.Primary("Commit", this.Commit).IsVisible(() => this.Context.CanWrite)
            ).Spacing(1),
            new Markup(() => this.shapeNote.Value).Wrap(true),
            new ComputedVisual(() => this.previewText.Value.Length == 0
                ? Ui.Nothing()
                : new ScrollViewer(new TextBlock(this.previewText.Value)).MaxHeight(14))
        ).Spacing(1));

        return new VStack(
            new ComputedVisual(() => this.error.Value.Length == 0 ? Ui.Nothing() : Ui.Failure(this.error.Value)),
            export,
            import,
            generate.Stretch()
        ).Spacing(1);
    }

    public override Task Load(CancellationToken ct) => Task.CompletedTask;

    static string Describe(ExportFormat format) => format switch
    {
        ExportFormat.Json => "JSON array of bodies",
        ExportFormat.Ndjson => "NDJSON, one body per line",
        ExportFormat.Csv => "CSV of flattened scalars",
        _ => "JSON envelope (round-trips ids and timestamps)"
    };

    static string Describe(ImportMode mode) => mode switch
    {
        ImportMode.Insert => "fail",
        ImportMode.Replace => "overwrite",
        _ => "skip"
    };

    void Export()
    {
        var path = this.exportPath.Value.Trim();
        if (path.Length == 0)
        {
            this.error.Value = "Give the export a path.";
            return;
        }

        var format = Formats[Math.Clamp(this.formatSelect?.SelectedIndex ?? 0, 0, Formats.Length - 1)];

        this.Context.Run(async ct =>
        {
            await using (var file = File.Create(path))
            {
                await this.Shell.ImportExport.Export(
                    this.Context.ProfileId, this.Context.Table, this.Context.TypeName,
                    format, new BrowseQuery(), file, ct);
            }

            var size = new FileInfo(path).Length;
            this.Context.Post(() =>
            {
                this.error.Value = "";
                this.Shell.Success($"Wrote {Ui.Bytes(size)} to {path}.");
            });
        }, "Could not export");
    }

    void Import()
    {
        var path = this.importPath.Value.Trim();
        if (path.Length == 0)
        {
            this.error.Value = "Point at a file to import.";
            return;
        }

        if (!File.Exists(path))
        {
            this.error.Value = $"No such file: {path}";
            return;
        }

        var mode = Modes[Math.Clamp(this.modeSelect?.SelectedIndex ?? 0, 0, Modes.Length - 1)];

        Modal.Confirm(
            this.Shell,
            "Import documents",
            $"Read {path} into '{this.Context.TypeName}'? Duplicate ids will {Describe(mode)}.",
            "Import",
            () => this.Context.Run(async ct =>
            {
                ImportResult result;
                await using (var file = File.OpenRead(path))
                {
                    result = await this.Shell.ImportExport.Import(
                        this.Context.ProfileId, this.Context.Table, this.Context.TypeName, file, mode, ct);
                }

                this.Context.Post(() =>
                {
                    this.error.Value = string.Join("  ", result.Errors);
                    this.Shell.Success($"Read {result.Read}, wrote {result.Written}, skipped {result.Skipped}.");
                    this.Shell.ReloadExplorer();
                });
            }, "Could not import")
        );
    }

    void Analyse() => this.Context.Run(async ct =>
    {
        var analysed = await this.Context.Admin.AnalyzeShape(
            this.Context.ProfileId, this.Context.Table, this.Context.TypeName, ct: ct);

        this.Context.Post(() =>
        {
            this.shape = analysed;
            this.shapeNote.Value = analysed.Fields.Count == 0
                ? "[yellow]Nothing to learn from - this type has no documents yet.[/]"
                : $"[dim]Learned {analysed.Fields.Count} field(s) from {analysed.Samples} document(s).[/]";
        });
    }, "Could not analyse the documents");

    void BuildPreview()
    {
        if (this.shape is not { } learned)
        {
            this.Shell.Info("Analyse the existing documents first.");
            return;
        }

        var count = Math.Clamp(this.generateCount.Value, 1, 5000);
        var built = DocumentAdminService.Preview(learned, count);

        this.preview = built;
        this.previewText.Value = string.Join(
            Environment.NewLine + Environment.NewLine,
            built.Sample.Select(o => DocumentAdminService.Prettify(o.ToJsonString())));

        this.shapeNote.Value = $"[dim]Seed {built.Seed}. Committing replays it, so these are the documents that get written.[/]";
    }

    void Commit()
    {
        if (this.preview is not { } built)
        {
            this.Shell.Info("Preview first - committing writes exactly what the preview showed.");
            return;
        }

        Modal.Confirm(
            this.Shell,
            "Generate documents",
            $"Write {built.Count} generated document(s) into '{this.Context.TypeName}'?",
            "Write them",
            () => this.Context.Run(async ct =>
            {
                var result = await this.Context.Admin.Commit(
                    this.Context.ProfileId, this.Context.Table, this.Context.TypeName, built, ct);

                this.Context.Post(() =>
                {
                    this.Shell.Success($"Wrote {result.Written} document(s).");
                    this.Shell.ReloadExplorer();

                    if (result.StaleSidecars.Count > 0)
                    {
                        this.Shell.Warn(
                            $"Generated rows are absent from {string.Join(", ", result.StaleSidecars)} - " +
                            "bulk generation goes through the schema-free lane, which does not maintain sidecars.");
                    }
                });
            }, "Could not write the generated documents")
        );
    }
}
