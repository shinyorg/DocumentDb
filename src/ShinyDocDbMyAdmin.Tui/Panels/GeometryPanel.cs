using ShinyDocDbMyAdmin.Models;
using ShinyDocDbMyAdmin.Services;
using ShinyDocDbMyAdmin.Tui.Widgets;
using Shiny.DocumentDb;
using XenoAtom.Terminal.UI;
using XenoAtom.Terminal.UI.Controls;

namespace ShinyDocDbMyAdmin.Tui.Panels;

/// <summary>One geometry found in a document body.</summary>
public sealed partial class GeoRow
{
    [Bindable] public partial string DocumentId { get; set; }
    [Bindable] public partial string Kind { get; set; }
    [Bindable] public partial string Family { get; set; }
    [Bindable] public partial string Measure { get; set; }
    [Bindable] public partial string Centroid { get; set; }

    public required GeoFeature Feature { get; init; }
}

/// <summary>
/// GeoJSON in the documents, drawn offline and listed with the measurements the OGC model gives.
/// </summary>
/// <remarks>
/// Geometry is read from the document body, not from the spatial sidecar - the sidecar holds bounding
/// boxes for R*Tree pruning, and the value is the GeoJSON in <c>Data</c>. Parsing goes through the
/// library's own converter, so Centroid, Area, Length and IsValid come from the real OGC model rather
/// than from anything reimplemented here.
/// </remarks>
public sealed class GeometryPanel(WorkspaceContext context) : WorkspacePanel(context)
{
    readonly GeoCanvas canvas = new();

    readonly RowGrid<GeoRow> grid = new(
        (GeoRow.Accessor.DocumentId, "Document", ColumnWidth.Fill),
        (GeoRow.Accessor.Kind, "Kind", ColumnWidth.Fit),
        (GeoRow.Accessor.Family, "Family", ColumnWidth.Fit),
        (GeoRow.Accessor.Measure, "Area / length", ColumnWidth.Fit),
        (GeoRow.Accessor.Centroid, "Centroid", ColumnWidth.Fit)
    );

    readonly State<string> summary = new("");
    readonly State<string> legend = new("");
    readonly State<string> error = new("");

    Select<string>? pathSelect;
    List<GeometryPath> paths = [];
    string? selectedPath;
    GeometrySet? current;

    protected override Visual Build()
    {
        this.grid.OnActivate(this.ZoomTo);

        var pathSelect = new Select<string>(["(loading)"]);
        pathSelect.SelectionChanged(() =>
        {
            if (this.paths.Count == 0)
                return;

            this.selectedPath = this.paths[Math.Clamp(pathSelect.SelectedIndex, 0, this.paths.Count - 1)].Path;
            this.Reload();
        });

        this.pathSelect = pathSelect;

        var toolbar = Ui.Toolbar(
            Ui.Action("Refresh", this.Reload),
            Ui.Label("Path"), pathSelect,
            Ui.Primary("Zoom to selection", () => this.WithSelection(this.ZoomTo)),
            Ui.Action("Fit all", () => this.Redraw(null)),
            Ui.Action("View document", () => this.WithSelection(this.ShowDocument))
        );

        return new VStack(
            Ui.Muted("Drawn offline - there is no basemap, because a tile layer would send these coordinates to a third party on every pan."),
            toolbar,
            new ComputedVisual(() => this.error.Value.Length == 0 ? Ui.Nothing() : Ui.Failure(this.error.Value)),
            new Markup(() => this.summary.Value),
            new Markup(() => this.legend.Value),
            // The map takes the room that is going; the list keeps a fixed share underneath it, which
            // is enough to read the selected feature's measurements without pushing the map off screen.
            this.canvas.Stretch(),
            this.grid.OrEmpty("No geometry at this path.").MinHeight(10).MaxHeight(12)
        ).Spacing(1);
    }

    void Reload() => this.Context.Run(this.Load, "Could not read the geometry");

    public override async Task Load(CancellationToken ct)
    {
        try
        {
            var found = await this.Context.Admin.FindGeometryPaths(this.Context.ProfileId, this.Context.Table, this.Context.TypeName, ct);

            if (found.Count == 0)
            {
                this.Context.Post(() =>
                {
                    this.paths = [];
                    this.canvas.Clear();
                    this.grid.Replace([]);
                    this.summary.Value = "[dim]No GeoJSON found in the sampled documents.[/]";
                });
                return;
            }

            // Opens on the field with the most to say - a scatter of dots is the least a map can tell
            // you, and opening on it hides that the type also carries boundaries.
            this.selectedPath ??= found.OrderByDescending(p => p.Rank).ThenByDescending(p => p.Occurrences).First().Path;
            var path = this.selectedPath;

            var set = await this.Context.Admin.LoadGeometry(
                this.Context.ProfileId, this.Context.Table, this.Context.TypeName, path, new BrowseQuery(), ct: ct);

            // Built inside the post: a [Bindable] property refuses a write from anywhere but the
            // render thread, so a row constructed out here would throw.
            this.Context.Post(() =>
            {
                this.error.Value = "";
                this.paths = [.. found];
                this.current = set;

                if (this.pathSelect is { } select)
                {
                    select.Items.Clear();
                    foreach (var candidate in found)
                        select.Items.Add($"{candidate.Label} · {candidate.Occurrences}");

                    select.SelectedIndex = Math.Max(0, found.ToList().FindIndex(p => p.Path == path));
                }

                this.grid.Replace(set.Features.Select(feature => new GeoRow
                {
                    DocumentId = feature.DocumentId,
                    Kind = feature.Geometry.GeometryType.ToString(),
                    Family = GeoCanvas.FamilyLabel(feature.Family),
                    Measure = Measure(feature.Geometry),
                    Centroid = Describe(feature.Geometry.Centroid),
                    Feature = feature
                }));

                this.Redraw(null);

                this.summary.Value =
                    $"[dim]{set.Features.Count} geometr(y/ies) at {Ui.Escape(path)} over {set.DocumentsScanned} document(s)" +
                    (set.Truncated ? " · truncated" : "") + "[/]";

                this.legend.Value = set.IsEmpty
                    ? ""
                    : string.Join("  ", set.FamiliesPresent.Select(f =>
                        $"[{GeoCanvas.FamilyColour(f)}]⣿[/] {GeoCanvas.FamilyLabel(f)}"));
            });
        }
        catch (Exception ex)
        {
            this.Context.Post(() =>
            {
                this.error.Value = ex.Message;
                this.canvas.Clear();
                this.grid.Replace([]);
            });
        }
    }

    void Redraw(GeoBoundingBox? focus)
    {
        if (this.current is { } set)
            this.canvas.Draw(set, focus);
    }

    void WithSelection(Action<GeoRow> action)
    {
        if (this.grid.Selected is { } row)
            action(row);
        else
            this.Shell.Info("Select a geometry first.");
    }

    /// <summary>
    /// Redraws over one feature's envelope. A fit over two distant clusters honestly renders both as
    /// specks, so zooming is the only way to actually look at one of them.
    /// </summary>
    void ZoomTo(GeoRow row) => this.Redraw(row.Feature.Geometry.GetEnvelope());

    void ShowDocument(GeoRow row) => this.Context.Run(async ct =>
    {
        var document = await this.Context.Admin.GetDocument(
            this.Context.ProfileId, this.Context.Table, this.Context.TypeName, row.Feature.DocumentId, ct);

        this.Context.Post(() =>
        {
            if (document is null)
                this.Shell.Warn("That document is gone.");
            else
                JsonDocumentDialog.Show(this.Shell, document.Id, document.Json);
        });
    }, "Could not read the document");

    /// <summary>
    /// Area in m² and length in m, as the library reports them and as the web front end's table
    /// labels them - both are geodesic, so they are real measurements rather than degrees squared.
    /// </summary>
    static string Measure(Geometry geometry) => geometry switch
    {
        _ when geometry.Area > 0 => $"{geometry.Area:N0} m²",
        _ when geometry.Length > 0 => $"{geometry.Length:N0} m",
        _ => "—"
    };

    static string Describe(GeoPoint point) => $"{point.Latitude:0.####}, {point.Longitude:0.####}";
}
