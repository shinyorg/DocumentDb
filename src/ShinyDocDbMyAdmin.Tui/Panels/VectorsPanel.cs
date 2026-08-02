using ShinyDocDbMyAdmin.Models;
using ShinyDocDbMyAdmin.Services;
using ShinyDocDbMyAdmin.Tui.Widgets;
using Shiny.DocumentDb;
using XenoAtom.Terminal.UI;
using XenoAtom.Terminal.UI.Controls;

namespace ShinyDocDbMyAdmin.Tui.Panels;

/// <summary>One embedding, reduced to its statistics.</summary>
public sealed partial class VectorRow
{
    [Bindable] public partial string DocumentId { get; set; }
    [Bindable] public partial string Dimensions { get; set; }
    [Bindable] public partial string Norm { get; set; }
    [Bindable] public partial string Range { get; set; }
    [Bindable] public partial string Flags { get; set; }
    [Bindable] public partial string Preview { get; set; }

    public required VectorEntry Entry { get; init; }
}

/// <summary>One neighbour of a similarity search.</summary>
public sealed partial class NeighbourRow
{
    [Bindable] public partial string DocumentId { get; set; }
    [Bindable] public partial string Score { get; set; }
    [Bindable] public partial string Norm { get; set; }
    [Bindable] public partial string Summary { get; set; }

    public required VectorNeighbour Neighbour { get; init; }
}

/// <summary>
/// Embedding statistics, sidecar drift, and exact nearest-neighbour search.
/// </summary>
/// <remarks>
/// <para>
/// The document bodies are the source of truth; the <c>{table}_vec_{type}</c> sidecar is an index built
/// from them. So anything this panel reports as drift means a similarity search is currently returning
/// wrong answers, and Rebuild is how that is fixed.
/// </para>
/// <para>
/// The search here is exact rather than approximate: it scans the bodies and scores every one. That is
/// the opposite of what the sidecar is for, and deliberately so - an admin tool that used the index
/// could not tell you the index was wrong.
/// </para>
/// </remarks>
public sealed class VectorsPanel(WorkspaceContext context) : WorkspacePanel(context)
{
    readonly RowGrid<VectorRow> grid = new(
        (VectorRow.Accessor.DocumentId, "Document", ColumnWidth.Fill),
        (VectorRow.Accessor.Dimensions, "Dims", ColumnWidth.Fit),
        (VectorRow.Accessor.Norm, "Norm", ColumnWidth.Fit),
        (VectorRow.Accessor.Range, "Min / max", ColumnWidth.Fit),
        (VectorRow.Accessor.Flags, "", ColumnWidth.Fit),
        (VectorRow.Accessor.Preview, "First components", ColumnWidth.Wide)
    );

    readonly RowGrid<NeighbourRow> neighbours = new(
        (NeighbourRow.Accessor.DocumentId, "Document", ColumnWidth.Fill),
        (NeighbourRow.Accessor.Score, "Distance", ColumnWidth.Fit),
        (NeighbourRow.Accessor.Norm, "Norm", ColumnWidth.Fit),
        (NeighbourRow.Accessor.Summary, "First components", ColumnWidth.Wide)
    );

    readonly State<string> health = new("");
    readonly State<string> stats = new("");
    readonly State<string> searchNote = new("");
    readonly State<string> error = new("");

    Select<string>? pathSelect;
    Select<string>? metricSelect;

    List<VectorFieldInfo> fields = [];
    string? selectedPath;
    VectorSet? current;
    VectorHealth? lastHealth;

    protected override Visual Build()
    {
        this.grid.OnActivate(this.SearchFrom);
        this.neighbours.OnActivate(row => this.SearchFrom(row.Neighbour.DocumentId));

        var pathSelect = new Select<string>(["(loading)"]);
        pathSelect.SelectionChanged(() =>
        {
            if (this.fields.Count == 0)
                return;

            this.selectedPath = this.fields[Math.Clamp(pathSelect.SelectedIndex, 0, this.fields.Count - 1)].Path;
            this.Reload();
        });
        this.pathSelect = pathSelect;

        var metrics = Enum.GetValues<VectorDistance>().ToList();
        this.metricSelect = new Select<string>([.. metrics.Select(m => m.ToString())],
            Math.Max(0, metrics.IndexOf(VectorDistance.Cosine)));

        var toolbar = Ui.Toolbar(
            Ui.Action("Refresh", this.Reload),
            Ui.Label("Path"), pathSelect,
            Ui.Label("Metric"), this.metricSelect,
            Ui.Primary("Nearest to selection", () => this.WithSelection(this.SearchFrom)),
            Ui.Action("Clear search", this.ClearSearch),
            Ui.Danger("Rebuild sidecar", this.Resync).IsVisible(() => this.Context.CanWrite),
            Ui.Action("Clear embedding", () => this.WithSelection(this.ClearVector)).IsVisible(() => this.Context.CanWrite)
        );

        return new VStack(
            new Markup(() => this.health.Value).Wrap(true),
            toolbar,
            new ComputedVisual(() => this.error.Value.Length == 0 ? Ui.Nothing() : Ui.Failure(this.error.Value)),
            new Markup(() => this.stats.Value).Wrap(true),
            this.grid.OrEmpty("No embeddings at this path.").Stretch(),
            new ComputedVisual(() => this.searchNote.Value.Length == 0
                ? Ui.Nothing()
                : Ui.Panel("Nearest neighbours", new VStack(
                    new Markup(this.searchNote.Value),
                    this.neighbours.OrEmpty("No neighbours.")
                ).Spacing(1)))
        ).Spacing(1);
    }

    void Reload() => this.Context.Run(this.Load, "Could not read the embeddings");

    public override async Task Load(CancellationToken ct)
    {
        try
        {
            var found = await this.Context.Admin.FindVectorPaths(
                this.Context.ProfileId, this.Context.Table, this.Context.TypeName, ct);

            if (found.Count == 0)
            {
                this.Context.Post(() =>
                {
                    this.fields = [];
                    this.grid.Replace([]);
                    this.stats.Value = "[dim]No embedding-shaped values in the sampled documents.[/]";
                });
                return;
            }

            this.selectedPath ??= found.OrderByDescending(f => f.Occurrences).First().Path;
            var path = this.selectedPath;

            var set = await this.Context.Admin.LoadVectors(
                this.Context.ProfileId, this.Context.Table, this.Context.TypeName, path, ct: ct);

            var report = await this.Context.Admin.CheckVectorHealth(
                this.Context.ProfileId, this.Context.Table, this.Context.TypeName, set, ct);

            // Built inside the post: a [Bindable] property refuses a write from anywhere but the
            // render thread, so a row constructed out here would throw.
            this.Context.Post(() =>
            {
                this.error.Value = "";
                this.fields = [.. found];
                this.current = set;
                this.lastHealth = report;

                if (this.pathSelect is { } select)
                {
                    select.Items.Clear();
                    foreach (var field in found)
                    {
                        var ragged = field.Ragged ? " · ragged" : "";
                        select.Items.Add($"{field.Path} · float[{field.Dimensions}] · {field.PercentPresent}%{ragged}");
                    }

                    select.SelectedIndex = Math.Max(0, found.ToList().FindIndex(f => f.Path == path));
                }

                this.grid.Replace(set.Entries.Select(entry => new VectorRow
                {
                    DocumentId = entry.DocumentId,
                    Dimensions = entry.Dimensions.ToString(),
                    Norm = entry.Norm.ToString("0.####"),
                    Range = $"{entry.Min:0.###} / {entry.Max:0.###}",
                    Flags = string.Join(" ", new[]
                    {
                        entry.IsZero ? "zero" : null,
                        entry.HasNonFinite ? "non-finite" : null,
                        entry.IsNormalised ? "unit" : null
                    }.Where(x => x is not null)),
                    Preview = VectorMath.Format(entry.Preview),
                    Entry = entry
                }));

                this.health.Value = DescribeHealth(report);
                this.stats.Value = DescribeStats(set);
            });
        }
        catch (Exception ex)
        {
            this.Context.Post(() =>
            {
                this.error.Value = ex.Message;
                this.grid.Replace([]);
            });
        }
    }

    static string DescribeHealth(VectorHealth report)
    {
        if (!report.Sidecar.ProviderSupportsVectors)
            return "[dim]This backend has no vector sidecar - the embeddings live in the document bodies and nothing indexes them.[/]";

        if (!report.Sidecar.Present)
            return "[dim]No vector sidecar for this type. Map one with MapVectorProperty and the library will create it.[/]";

        if (report.Sidecar.Unavailable is { } why)
        {
            return $"[yellow]The sidecar exists but this tool cannot read or write it: {Ui.Escape(why)} " +
                   "Documents written from here will leave it holding the previous embedding.[/]";
        }

        if (report.Unknown)
            return "[yellow]The sidecar could not be counted, so its state is unknown.[/]";

        if (report.IsHealthy)
            return $"[green]Sidecar in step[/] [dim]· {Ui.Number(report.Sidecar.RowCount ?? 0)} row(s) for {report.DocumentsWithVector} document(s) with an embedding.[/]";

        var problems = new List<string>();
        if (report.MissingFromSidecar.Count > 0)
            problems.Add($"{report.MissingFromSidecar.Count} document(s) have an embedding the sidecar does not");

        if (report.OrphanedInSidecar.Count > 0)
            problems.Add($"{report.OrphanedInSidecar.Count} sidecar row(s) have no document behind them");

        if (report.Ragged)
            problems.Add($"mixed dimensions ({string.Join(", ", report.DimensionsPresent)}) - a search cannot compare these");

        return $"[yellow]Sidecar has drifted:[/] {Ui.Escape(string.Join("; ", problems))}. " +
               "[dim]A similarity search is returning wrong answers until it is rebuilt.[/]";
    }

    static string DescribeStats(VectorSet set)
    {
        if (set.IsEmpty)
            return "[dim]No embeddings found.[/]";

        var dimensions = string.Join(", ", set.DimensionGroups.Select(g => $"float[{g.Dimensions}] ×{g.Count}"));
        return
            $"[dim]{set.Entries.Count} embedding(s) over {set.DocumentsScanned} document(s)" +
            (set.DocumentsWithoutVector > 0 ? $", {set.DocumentsWithoutVector} without one" : "") +
            (set.Truncated ? " · truncated" : "") +
            $" · {Ui.Escape(dimensions)} · mean norm {set.AverageNorm:0.###} · " +
            $"{set.NormalisedCount} unit-length · {set.ZeroCount} all-zero · {set.NonFiniteCount} non-finite[/]";
    }

    void WithSelection(Action<VectorRow> action)
    {
        if (this.grid.Selected is { } row)
            action(row);
        else
            this.Shell.Info("Select an embedding first.");
    }

    void SearchFrom(VectorRow row) => this.SearchFrom(row.DocumentId);

    void SearchFrom(string documentId)
    {
        if (this.selectedPath is not { } path)
            return;

        var metrics = Enum.GetValues<VectorDistance>().ToList();
        var metric = metrics[Math.Clamp(this.metricSelect?.SelectedIndex ?? 0, 0, metrics.Count - 1)];

        this.Context.Run(async ct =>
        {
            var probe = await this.Context.Admin.ReadVector(
                this.Context.ProfileId, this.Context.Table, this.Context.TypeName, documentId, path, ct);

            if (probe is null)
            {
                this.Context.Post(() => this.Shell.Warn($"'{documentId}' has no embedding at {path}."));
                return;
            }

            var hits = await this.Context.Admin.FindNearest(
                this.Context.ProfileId, this.Context.Table, this.Context.TypeName, path, probe, k: 20, metric, ct: ct);

            this.Context.Post(() =>
            {
                this.neighbours.Replace(hits.Select(hit => new NeighbourRow
                {
                    DocumentId = hit.DocumentId,
                    Score = hit.Score.ToString("0.######"),
                    Norm = hit.Norm.ToString("0.###"),
                    Summary = hit.Summary,
                    Neighbour = hit
                }));

                this.searchNote.Value =
                    $"[dim]{metric} distance from '{Ui.Escape(documentId)}', scored against every document body - " +
                    "an exact scan, not the sidecar's approximate index.[/]";
            });
        }, "Could not run the similarity search");
    }

    void ClearSearch()
    {
        this.neighbours.Replace([]);
        this.searchNote.Value = "";
    }

    void Resync()
    {
        if (this.selectedPath is not { } path)
            return;

        if (this.lastHealth?.Sidecar is { Maintainable: false, Present: true } sidecar)
        {
            this.Shell.Warn($"The sidecar cannot be written from here: {sidecar.Unavailable}");
            return;
        }

        Modal.Confirm(
            this.Shell,
            "Rebuild vector sidecar",
            $"Rewrite every sidecar row for this type from the embeddings at {path}? The document bodies are the source of truth, so this makes the index agree with them.",
            "Rebuild",
            () => this.Context.Run(async ct =>
            {
                var written = await this.Context.Admin.ResyncVectorSidecar(
                    this.Context.ProfileId, this.Context.Table, this.Context.TypeName, path, ct: ct);

                await this.Load(ct);
                this.Context.Post(() => this.Shell.Success($"Rewrote {written} sidecar row(s)."));
            }, "Could not rebuild the sidecar")
        );
    }

    void ClearVector(VectorRow row)
    {
        if (this.selectedPath is not { } path)
            return;

        Modal.Confirm(
            this.Shell,
            "Clear embedding",
            $"Remove the sidecar row for '{row.DocumentId}'? The embedding stays in the document body - only the index entry goes.",
            "Clear",
            () => this.Context.Run(async ct =>
            {
                await this.Context.Admin.WriteVector(
                    this.Context.ProfileId, this.Context.Table, this.Context.TypeName, row.DocumentId, path, null, ct);

                await this.Load(ct);
                this.Context.Post(() => this.Shell.Success($"Cleared the sidecar row for '{row.DocumentId}'."));
            }, "Could not clear the embedding")
        );
    }
}
