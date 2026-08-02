using ShinyDocDbMyAdmin.Models;
using ShinyDocDbMyAdmin.Services;
using ShinyDocDbMyAdmin.Tui.Widgets;
using XenoAtom.Terminal.UI;
using XenoAtom.Terminal.UI.Controls;

namespace ShinyDocDbMyAdmin.Tui.Panels;

/// <summary>One field discovered by sampling the type's documents.</summary>
public sealed partial class FieldRow
{
    [Bindable] public partial string Path { get; set; }
    [Bindable] public partial string Types { get; set; }
    [Bindable] public partial string Present { get; set; }
    [Bindable] public partial string Example { get; set; }
    [Bindable] public partial string Index { get; set; }

    public required InferredField Field { get; init; }
    public required JsonIndexInfo? Existing { get; init; }

    /// <summary>Objects and arrays are not indexable as scalars, so there is nothing to offer for them.</summary>
    public bool CanIndex => !this.Field.Types.Contains("object") && !this.Field.Types.Contains("array");
}

/// <summary>Any index on the table, whether DocumentDb built it or someone else did.</summary>
public sealed partial class IndexRow
{
    [Bindable] public partial string Name { get; set; }
    [Bindable] public partial string Kind { get; set; }
    [Bindable] public partial string Definition { get; set; }
    [Bindable] public partial string Size { get; set; }
    [Bindable] public partial string Scans { get; set; }

    public required IndexInfo Info { get; init; }
}

/// <summary>
/// The inferred shape of a type, and the indexes on the table it lives in.
/// </summary>
/// <remarks>
/// The shape is a description of what the sampled documents contain, not a contract - the store is
/// schema-free and a field present in 40% of the sample is reported as exactly that. Every index on
/// the table is listed, not only DocumentDb's, because an index someone added by hand is often what
/// explains a query's speed; only DocumentDb's own can be dropped from here.
/// </remarks>
public sealed class StructurePanel(WorkspaceContext context) : WorkspacePanel(context)
{
    readonly RowGrid<FieldRow> fields = new(
        (FieldRow.Accessor.Path, "Path", ColumnWidth.Fill),
        (FieldRow.Accessor.Types, "Type", ColumnWidth.Fit),
        (FieldRow.Accessor.Present, "Present", ColumnWidth.Fit),
        (FieldRow.Accessor.Example, "Example", ColumnWidth.Wide),
        (FieldRow.Accessor.Index, "Index", ColumnWidth.Fit)
    );

    readonly RowGrid<IndexRow> indexes = new(
        (IndexRow.Accessor.Name, "Index", ColumnWidth.Fill),
        (IndexRow.Accessor.Kind, "Source", ColumnWidth.Fit),
        (IndexRow.Accessor.Definition, "Path / definition", ColumnWidth.Wide),
        (IndexRow.Accessor.Size, "Size", ColumnWidth.Fit),
        (IndexRow.Accessor.Scans, "Scans", ColumnWidth.Fit)
    );

    readonly State<string> facts = new("");
    readonly State<string> indexFacts = new("");
    readonly State<string> error = new("");

    protected override Visual Build()
    {
        this.fields.OnActivate(this.ToggleIndex);
        this.indexes.OnActivate(this.DropIndex);

        var toolbar = Ui.Toolbar(
            Ui.Action("Refresh", () => this.Context.Run(this.Load, "Could not read the structure")),
            Ui.Primary("Index field", () => this.WithField(this.ToggleIndex)).IsVisible(() => this.Context.CanWrite),
            Ui.Action("Composite index", this.CreateComposite).IsVisible(() => this.Context.CanWrite),
            Ui.Danger("Drop index", () => this.WithIndex(this.DropIndex)).IsVisible(() => this.Context.CanWrite)
        );

        return new VStack(
            toolbar,
            new Markup(() => this.facts.Value),
            new ComputedVisual(() => this.error.Value.Length == 0 ? Ui.Nothing() : Ui.Failure(this.error.Value)),

            Ui.Panel("Inferred shape", new VStack(
                Ui.Muted("What the sampled documents actually contain - not a contract. A field below 100% is simply absent from some of them."),
                this.fields.OrEmpty("No documents to sample.").Stretch()
            ).Spacing(1)).Stretch(),

            Ui.Panel("Indexes on the table", new VStack(
                new Markup(() => this.indexFacts.Value),
                this.indexes.OrEmpty("No indexes on this table.").Stretch()
            ).Spacing(1)).Stretch()
        ).Spacing(1);
    }

    public override async Task Load(CancellationToken ct)
    {
        try
        {
            var stats = await this.Context.Admin.GetStats(this.Context.ProfileId, this.Context.Table, this.Context.TypeName, ct);
            var schema = await this.Context.Admin.InferSchema(this.Context.ProfileId, this.Context.Table, this.Context.TypeName, ct: ct);
            var jsonIndexes = await this.Context.Admin.ListIndexes(this.Context.ProfileId, this.Context.Table, ct);
            var allIndexes = await this.Context.Admin.ListAllIndexes(this.Context.ProfileId, this.Context.Table, ct);

            // Built inside the post: a [Bindable] property refuses a write from anywhere but the
            // render thread, so a row constructed out here would throw.
            this.Context.Post(() =>
            {
                this.error.Value = "";

                this.fields.Replace(schema.Fields.Select(field =>
                {
                    var existing = jsonIndexes.FirstOrDefault(i => i.TypeName == this.Context.TypeName && i.Path == field.Path);
                    return new FieldRow
                    {
                        Path = field.Path,
                        Types = field.Types,
                        Present = $"{field.PercentPresent}%",
                        Example = Ui.Cell(field.Example, 60),
                        Index = existing is null ? "" : "indexed",
                        Field = field,
                        Existing = existing
                    };
                }));

                this.indexes.Replace(allIndexes.Select(index => new IndexRow
                {
                    Name = index.Name,
                    Kind = index.IsDocumentDb ? "DocumentDb" : "other",
                    Definition = Ui.Cell(index.Json?.Path ?? index.Definition, 80),
                    Size = index.SizeBytes is { } size ? Ui.Bytes(size) : "—",
                    Scans = index.Scans is { } scans ? Ui.Number(scans) : "—",
                    Info = index
                }));
                this.facts.Value =
                    $"[dim]{Ui.Number(stats.DocumentCount)} documents · {schema.Fields.Count} field(s) over a sample of " +
                    $"{schema.SampleSize} · {Ui.Escape(Ui.Bytes(stats.TotalJsonBytes))} of JSON · largest " +
                    $"{Ui.Escape(Ui.Bytes(stats.LargestJsonBytes))}[/]";
                this.indexFacts.Value =
                    $"[dim]{allIndexes.Count} total, {allIndexes.Count(i => i.IsDocumentDb)} from DocumentDb. " +
                    "An index someone added by hand is often what explains a query's speed - only DocumentDb's own can be dropped here.[/]";
            });
        }
        catch (Exception ex)
        {
            this.Context.Post(() =>
            {
                this.error.Value = ex.Message;
                this.fields.Replace([]);
                this.indexes.Replace([]);
            });
        }
    }

    void WithField(Action<FieldRow> action)
    {
        if (this.fields.Selected is { } row)
            action(row);
        else
            this.Shell.Info("Select a field first.");
    }

    void WithIndex(Action<IndexRow> action)
    {
        if (this.indexes.Selected is { } row)
            action(row);
        else
            this.Shell.Info("Select an index first.");
    }

    void ToggleIndex(FieldRow row)
    {
        if (!this.Context.CanWrite)
        {
            this.Shell.Warn("This connection is read-only.");
            return;
        }

        if (row.Existing is { } existing)
        {
            this.Drop(existing.Name);
            return;
        }

        if (!row.CanIndex)
        {
            this.Shell.Info($"'{row.Path}' holds {row.Types}, which is not indexable as a scalar.");
            return;
        }

        this.Context.Run(async ct =>
        {
            await this.Context.Admin.CreateIndex(this.Context.ProfileId, this.Context.Table, this.Context.TypeName, row.Path, ct);
            await this.Load(ct);
            this.Context.Post(() => this.Shell.Success($"Indexed '{row.Path}'."));
        }, "Could not create the index");
    }

    void DropIndex(IndexRow row)
    {
        if (!row.Info.IsDocumentDb)
        {
            this.Shell.Info($"'{row.Name}' was not created by DocumentDb, so it is not this tool's to drop.");
            return;
        }

        this.Drop(row.Name);
    }

    void Drop(string name)
    {
        if (!this.Context.CanWrite)
        {
            this.Shell.Warn("This connection is read-only.");
            return;
        }

        Modal.Confirm(
            this.Shell,
            "Drop index",
            $"Drop '{name}'? Queries that relied on it fall back to a scan until it is recreated.",
            "Drop",
            () => this.Context.Run(async ct =>
            {
                await this.Context.Admin.DropIndex(this.Context.ProfileId, this.Context.Table, name, ct);
                await this.Load(ct);
                this.Context.Post(() => this.Shell.Success($"Dropped '{name}'."));
            }, "Could not drop the index")
        );
    }

    void CreateComposite() => Modal.Prompt(
        this.Shell,
        "Composite index",
        "Comma-separated JSON paths, in the order a query would filter on them.",
        "",
        input =>
        {
            var paths = input
                .Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
                .ToList();

            if (paths.Count < 2)
            {
                this.Shell.Warn("A composite index needs at least two paths - use Index field for a single one.");
                return;
            }

            this.Context.Run(async ct =>
            {
                await this.Context.Admin.CreateIndex(this.Context.ProfileId, this.Context.Table, this.Context.TypeName, paths, ct);
                await this.Load(ct);
                this.Context.Post(() => this.Shell.Success($"Indexed {string.Join(", ", paths)}."));
            }, "Could not create the index");
        }
    );
}
