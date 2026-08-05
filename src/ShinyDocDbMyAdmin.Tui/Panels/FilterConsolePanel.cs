using System.Text.Json.Nodes;
using ShinyDocDbMyAdmin.Models;
using ShinyDocDbMyAdmin.Services;
using ShinyDocDbMyAdmin.Tui.Shell;
using ShinyDocDbMyAdmin.Tui.Widgets;
using XenoAtom.Terminal;
using XenoAtom.Terminal.UI;
using XenoAtom.Terminal.UI.Controls;
using XenoAtom.Terminal.UI.Input;

namespace ShinyDocDbMyAdmin.Tui.Panels;

/// <summary>
/// DocumentDb's string query grammar, and the SQL it compiles to.
/// </summary>
/// <remarks>
/// <para>
/// This console goes through the library's own parser and translator rather than reimplementing the
/// grammar, so what runs here is exactly what <c>store.Collection(name).Where("…")</c> would run in an
/// application - and the compiled SQL is shown, because that is the point of the console: the grammar
/// is not a second query language, it is shorthand for a statement you can read.
/// </para>
/// <para>
/// The type is typed in rather than fixed, because a collection's name <i>is</i> the <c>TypeName</c>
/// column the rest of the tool browses by - the same rows, reached the other way round.
/// </para>
/// </remarks>
public sealed class FilterConsolePanel
{
    readonly AdminShell shell;
    readonly string profileId;
    readonly Action<string> openInSql;

    readonly State<string> table = new("documents");
    readonly State<string> typeName = new("");
    readonly State<string> where = new("");
    readonly State<string> orderBy = new("");
    readonly State<string> project = new("");
    readonly State<string> summary = new("");
    readonly State<string> error = new("");
    readonly State<string> suggestion = new("");
    readonly State<string> encryptionWarning = new("");

    readonly ResultGrid results = new();

    FilterResult? last;

    public FilterConsolePanel(AdminShell shell, string profileId, Action<string> openInSql)
    {
        this.shell = shell;
        this.profileId = profileId;
        this.openInSql = openInSql;
        this.View = this.Build();
    }

    public Visual View { get; }

    Visual Build()
    {
        var whereBox = Ui.Input(this.where).MinWidth(60);
        whereBox.AddKeyBinding(new KeyGesture(TerminalKey.Enter), this.Run);

        var form = new VStack(
            new HStack(
                Ui.Label("Table"), Ui.Input(this.table).MinWidth(18),
                Ui.Label("Type"), Ui.Input(this.typeName).MinWidth(22)
            ).Spacing(1),
            new VStack(Ui.Label("Where"), whereBox).Spacing(0),
            new HStack(
                new VStack(Ui.Label("Order by"), Ui.Input(this.orderBy).MinWidth(30)).Spacing(0),
                new VStack(Ui.Label("Project"), Ui.Input(this.project).MinWidth(30)).Spacing(0)
            ).Spacing(2)
        ).Spacing(1);

        var toolbar = Ui.Toolbar(
            Ui.Primary("Run", this.Run),
            Ui.Action("Clear", this.Clear),
            Ui.Action("Show SQL", this.ShowSql),
            Ui.Action("Explain", this.Explain),
            Ui.Action("Open in SQL console", this.HandOff),
            Ui.Action("Create suggested index", this.CreateSuggested).IsVisible(() => this.suggestion.Value.Length > 0)
        );

        return new VStack(
            Ui.Muted("status == 'Shipped' and total:number > 100 · the same grammar the library's Where(\"…\") takes."),
            form,
            toolbar,
            new ComputedVisual(() => this.error.Value.Length == 0 ? Ui.Nothing() : Ui.Failure(this.error.Value)),
            new ComputedVisual(() => this.suggestion.Value.Length == 0 ? Ui.Nothing() : Ui.Warning(this.suggestion.Value)),
            new ComputedVisual(() => this.encryptionWarning.Value.Length == 0 ? Ui.Nothing() : Ui.Warning(this.encryptionWarning.Value)),
            new Markup(() => this.summary.Value),
            this.results.OrEmpty("Nothing run yet.").Stretch()
        ).Spacing(1);
    }

    FilterQuery BuildQuery() => new(
        Where: Blank(this.where.Value),
        OrderBy: Blank(this.orderBy.Value),
        Project: Blank(this.project.Value),
        Offset: 0,
        Take: 200
    );

    static string? Blank(string value) => string.IsNullOrWhiteSpace(value) ? null : value.Trim();

    void Run()
    {
        if (string.IsNullOrWhiteSpace(this.typeName.Value))
        {
            this.error.Value = "Name the type to query - it is the TypeName column, and the collection name the library would use.";
            return;
        }

        var table = this.table.Value.Trim();
        var type = this.typeName.Value.Trim();
        var query = this.BuildQuery();

        this.shell.RunAsync(async ct =>
        {
            try
            {
                var result = await this.shell.Admin.ExecuteFilter(this.profileId, table, type, query, ct);

                // Asked for only after a successful run: an unindexed path only matters once the query
                // is one that works.
                var paths = await this.shell.Admin.SuggestIndexPaths(this.profileId, table, type, query, ct);

                // A predicate over an encrypted path succeeds and matches nothing, because this console
                // compiles to SQL and never encrypts the constant the way the library's LINQ path does.
                var encryption = await this.shell.Admin.EncryptionWarnings(this.profileId, table, type, query, ct);

                this.shell.Post(() =>
                {
                    this.last = result;
                    this.error.Value = "";
                    this.results.ShowDocuments(result.Columns, result.Rows);
                    this.summary.Value =
                        $"[dim]{Ui.Number(result.TotalCount)} match(es) in {Ui.Escape(Ui.Duration(result.Elapsed))}" +
                        (result.PageFull ? $" · showing the first {result.Rows.Count}" : "") + "[/]";

                    this.encryptionWarning.Value = string.Join(Environment.NewLine, encryption);

                    this.suggestion.Value = paths.Count == 0
                        ? ""
                        : $"Unindexed: {string.Join(", ", paths)}. An index on {(paths.Count == 1 ? "it" : "them")} would usually remove the scan.";
                });
            }
            catch (Exception ex)
            {
                this.shell.Post(() =>
                {
                    this.error.Value = ex.Message;
                    this.results.Clear();
                    this.summary.Value = "";
                    this.suggestion.Value = "";
                    this.last = null;
                });
            }
        });
    }

    void Clear()
    {
        this.where.Value = "";
        this.orderBy.Value = "";
        this.project.Value = "";
        this.error.Value = "";
        this.summary.Value = "";
        this.suggestion.Value = "";
        this.results.Clear();
        this.last = null;
    }

    void ShowSql()
    {
        if (this.last is not { } result)
        {
            this.shell.Info("Run the query first - the SQL is what it compiled to.");
            return;
        }

        var parameters = result.Parameters.Count == 0
            ? ""
            : Environment.NewLine + Environment.NewLine + "Parameters:" + Environment.NewLine +
              string.Join(Environment.NewLine, result.Parameters.Select(p => $"  {p.Key} = {p.Value}"));

        Modal.Show(this.shell, "Compiled SQL", result.Sql + parameters);
    }

    void HandOff()
    {
        if (this.last is not { } result)
            this.shell.Info("Run the query first - there is no SQL until it has compiled.");
        else
            this.openInSql(result.Sql);
    }

    void Explain()
    {
        if (this.last is not { } result)
        {
            this.shell.Info("Run the query first - the plan is for the SQL it compiled to.");
            return;
        }

        this.shell.RunAsync(async ct =>
        {
            var plan = await this.shell.Admin.Explain(this.profileId, result.Sql, result.Parameters, ct);

            this.shell.Post(() => Modal.Show(
                this.shell,
                plan.Supported ? $"Query plan · {Ui.Duration(plan.Elapsed)}" : "Query plan",
                plan.Supported
                    ? string.Join(Environment.NewLine, plan.Lines) + (plan.LooksLikeScan
                        ? Environment.NewLine + Environment.NewLine +
                          "This plan mentions a full scan. It is a text match across nine dialects, so read the plan itself - " +
                          "but an index on the filtered path is usually what removes it."
                        : "")
                    : "This provider offers no query plan the tool can run."
            ));
        }, "Could not explain the query");
    }

    void CreateSuggested()
    {
        var table = this.table.Value.Trim();
        var type = this.typeName.Value.Trim();
        var query = this.BuildQuery();

        this.shell.RunAsync(async ct =>
        {
            var paths = await this.shell.Admin.SuggestIndexPaths(this.profileId, table, type, query, ct);
            if (paths.Count == 0)
            {
                this.shell.Post(() => this.shell.Info("Nothing left to index for this query."));
                return;
            }

            foreach (var path in paths)
                await this.shell.Admin.CreateIndex(this.profileId, table, type, path, ct);

            this.shell.Post(() =>
            {
                this.suggestion.Value = "";
                this.shell.Success($"Indexed {string.Join(", ", paths)}. Run the query again to see the new plan.");
            });
        }, "Could not create the index");
    }
}

/// <summary>
/// A grid whose columns come from a result set rather than from a type.
/// </summary>
/// <remarks>
/// Shared by both consoles: a filter result is a list of <see cref="JsonObject"/> and a SQL result is
/// a list of string arrays, but on screen they are the same thing - a header row and cells - so they
/// go through one control that takes columns and rows of text.
/// </remarks>
public sealed class ResultGrid
{
    readonly List<string> columns = [];
    readonly List<IReadOnlyList<string?>> rows = [];
    readonly State<int> revision = new(0);

    public Visual OrEmpty(string emptyMessage)
        => new ComputedVisual(() =>
        {
            _ = this.revision.Value;
            return this.rows.Count == 0 && this.columns.Count == 0
                ? Ui.Empty(emptyMessage)
                : this.BuildTable();
        });

    /// <summary>Documents, whose cells are read out of the projected JSON by column name.</summary>
    public void ShowDocuments(IReadOnlyList<string> resultColumns, IReadOnlyList<JsonObject> resultRows)
        => this.Show(resultColumns, resultRows
            .Select(row => (IReadOnlyList<string?>)[.. resultColumns.Select(c => JsonDisplay.Cell(row[c]))])
            .ToList());

    public void Clear() => this.Show([], []);

    public void Show(IReadOnlyList<string> resultColumns, IReadOnlyList<IReadOnlyList<string?>> resultRows)
    {
        this.columns.Clear();
        this.columns.AddRange(resultColumns);
        this.rows.Clear();
        this.rows.AddRange(resultRows);
        this.revision.Value++;
    }

    /// <summary>
    /// A plain <see cref="Table"/> rather than a data grid: the column set changes on every run, and
    /// rebuilding one control's columns per run costs more than rebuilding a table that never had any.
    /// </summary>
    Visual BuildTable()
    {
        var table = new Table();
        table.Headers([.. this.columns.Select(c => (Visual)new Markup($"[bold]{Ui.Escape(c)}[/]"))]);

        foreach (var row in this.rows)
            table.AddRow([.. row.Select(cell => (Visual)new TextBlock(Ui.Cell(cell, 80)))]);

        return new ScrollViewer(table);
    }
}
