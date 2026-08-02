using ShinyDocDbMyAdmin.Models;
using ShinyDocDbMyAdmin.Tui.Widgets;
using Shiny.DocumentDb;
using XenoAtom.Terminal;
using XenoAtom.Terminal.UI;
using XenoAtom.Terminal.UI.Controls;
using XenoAtom.Terminal.UI.Input;

namespace ShinyDocDbMyAdmin.Tui.Panels;

/// <summary>One ranked full-text match.</summary>
public sealed partial class FullTextRow
{
    [Bindable] public partial string DocumentId { get; set; }
    [Bindable] public partial string Score { get; set; }
    [Bindable] public partial string Preview { get; set; }

    public required FullTextHit Hit { get; init; }
}

/// <summary>
/// Ranked search through the provider's own full-text engine.
/// </summary>
/// <remarks>
/// Unlike every other sidecar this tool touches, a full-text index is maintained by the engine - FTS5
/// triggers, generated or computed columns, an on-commit CONTEXT index - so a document written from
/// here updates it exactly as a write from the library would. DuckDB is the one backend where that is
/// not true: its index is a snapshot the library rebuilds before each query, and this panel says so
/// rather than pretending otherwise.
/// </remarks>
public sealed class FullTextPanel(WorkspaceContext context) : WorkspacePanel(context)
{
    readonly RowGrid<FullTextRow> grid = new(
        (FullTextRow.Accessor.DocumentId, "Document", ColumnWidth.Fill),
        (FullTextRow.Accessor.Score, "Score", ColumnWidth.Fit),
        (FullTextRow.Accessor.Preview, "Body", ColumnWidth.Wide)
    );

    readonly State<string> query = new("");
    readonly State<string> indexNote = new("");
    readonly State<string> summary = new("");
    readonly State<string> error = new("");

    string? lastSql;

    protected override Visual Build()
    {
        this.grid.OnActivate(row => JsonDocumentDialog.Show(this.Shell, row.DocumentId, row.Hit.Json));

        var languages = Enum.GetValues<FullTextLanguage>().ToList();
        var language = new Select<string>([.. languages.Select(l => l.ToString())],
            Math.Max(0, languages.IndexOf(FullTextLanguage.English)));

        this.language = () => languages[Math.Max(0, language.SelectedIndex)];

        var box = Ui.Input(this.query).MinWidth(40);
        box.AddKeyBinding(new KeyGesture(TerminalKey.Enter), this.Search);

        var toolbar = Ui.Toolbar(
            Ui.Label("Search"), box,
            Ui.Primary("Search", this.Search),
            Ui.Action("Clear", this.Clear),
            Ui.Label("Language"), language,
            Ui.Action("Show SQL", this.ShowSql),
            Ui.Action("View document", () => this.WithSelection(row => JsonDocumentDialog.Show(this.Shell, row.DocumentId, row.Hit.Json)))
        );

        return new VStack(
            new Markup(() => this.indexNote.Value).Wrap(true),
            toolbar,
            Ui.Muted("Only PostgreSQL reads the language - it picks the to_tsquery configuration, which has to match the one the index was built with."),
            new ComputedVisual(() => this.error.Value.Length == 0 ? Ui.Nothing() : Ui.Failure(this.error.Value)),
            new Markup(() => this.summary.Value),
            this.grid.OrEmpty("Nothing searched yet.").Stretch()
        ).Spacing(1);
    }

    Func<FullTextLanguage> language = () => FullTextLanguage.English;

    public override async Task Load(CancellationToken ct)
    {
        var index = await this.Context.Admin.DescribeFullTextIndex(
            this.Context.ProfileId, this.Context.Table, this.Context.TypeName, ct);

        this.Context.Post(() => this.indexNote.Value = Describe(index));
    }

    static string Describe(FullTextIndexInfo index)
    {
        if (!index.ProviderSupportsFullText)
            return "[yellow]This backend has no full-text engine.[/]";

        if (index.Unavailable is { } why)
            return $"[yellow]{Ui.Escape(why)}[/]";

        if (!index.Present)
            return "[yellow]No full-text index for this type. Map one with MapFullTextProperty and the library will create it.[/]";

        return index.RequiresRebuild
            ? "[yellow]Indexed, but this backend's index is a snapshot the library rebuilds before each query - writes made since the last rebuild are not searchable yet.[/]"
            : "[dim]Indexed and maintained by the engine, so a write from here updates it the same way a write from the library does.[/]";
    }

    /// <summary>Searches for a phrase, as the box would. Used by the command palette and the docs harness.</summary>
    public void Search(string text)
    {
        this.query.Value = text;
        this.Search();
    }

    void Search()
    {
        var text = this.query.Value.Trim();
        if (text.Length == 0)
        {
            this.error.Value = "Enter something to search for.";
            return;
        }

        var chosen = this.language();

        this.Context.Run(async ct =>
        {
            try
            {
                var results = await this.Context.Admin.SearchFullText(
                    this.Context.ProfileId, this.Context.Table, this.Context.TypeName, text, chosen, ct: ct);

                // Built inside the post: a [Bindable] property refuses a write from anywhere but
                // the render thread, so a row constructed out here would throw.
                this.Context.Post(() =>
                {
                    this.error.Value = "";
                    this.lastSql = results.Sql;
                    this.grid.Replace(results.Hits.Select(hit => new FullTextRow
                    {
                        DocumentId = hit.DocumentId,
                        Score = hit.Score.ToString("0.####"),
                        Preview = Ui.Cell(hit.Json, 120),
                        Hit = hit
                    }));

                    this.summary.Value = $"[dim]{results.Hits.Count} match(es) in {Ui.Escape(Ui.Duration(results.Elapsed))}[/]";
                });
            }
            catch (Exception ex)
            {
                this.Context.Post(() =>
                {
                    this.error.Value = ex.Message;
                    this.grid.Replace([]);
                    this.summary.Value = "";
                });
            }
        });
    }

    void Clear()
    {
        this.query.Value = "";
        this.error.Value = "";
        this.summary.Value = "";
        this.lastSql = null;
        this.grid.Replace([]);
    }

    void ShowSql()
    {
        if (this.lastSql is { } sql)
            Modal.Show(this.Shell, "Full-text SQL", sql);
        else
            this.Shell.Info("Search first - the SQL is what the provider built for it.");
    }

    void WithSelection(Action<FullTextRow> action)
    {
        if (this.grid.Selected is { } row)
            action(row);
        else
            this.Shell.Info("Select a match first.");
    }
}
