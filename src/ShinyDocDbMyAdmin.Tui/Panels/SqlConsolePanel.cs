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
/// The SQL console: whatever you type is what runs.
/// </summary>
/// <remarks>
/// The phpMyAdmin tab, with the same posture as the web front end's. There is no statement parsing and
/// no allow-list; the only guard is the read-only profile flag, which refuses anything that is not a
/// SELECT. <c>@name</c> placeholders work on every backend because each provider's connection rewrites
/// them, so a query written against SQLite mostly moves to PostgreSQL unchanged.
/// </remarks>
public sealed class SqlConsolePanel
{
    readonly AdminShell shell;
    readonly string profileId;

    readonly State<string> text = new("");
    readonly State<string> parameters = new("");
    readonly State<string> summary = new("");
    readonly State<string> error = new("");
    readonly ResultGrid results = new();

    readonly RowGrid<SavedQueryRow> saved = new(
        (SavedQueryRow.Accessor.Name, "Saved query", ColumnWidth.Fill),
        (SavedQueryRow.Accessor.SavedAt, "Saved", ColumnWidth.Fit)
    );

    public SqlConsolePanel(AdminShell shell, string profileId)
    {
        this.shell = shell;
        this.profileId = profileId;
        this.View = this.Build();
    }

    public Visual View { get; }

    /// <summary>Used when the filter console hands over the SQL it compiled to.</summary>
    public void SetText(string statement) => this.text.Value = statement;

    Visual Build()
    {
        this.saved.OnActivate(row => this.text.Value = row.Query.Sql);

        var editor = Ui.Code(this.text)
            .SyntaxHighlighter(SqlSyntaxHighlighter.Instance)
            .ShowLineNumbers(true)
            .MinHeight(8);

        editor.AddKeyBinding(new KeyGesture(TerminalKey.Enter, TerminalModifiers.Ctrl), this.Run);
        editor.AddKeyBinding(new KeyGesture(TerminalChar.CtrlS, TerminalModifiers.Ctrl), this.Save);

        var toolbar = Ui.Toolbar(
            Ui.Primary("Run", this.Run),
            Ui.Action("Explain", this.Explain),
            Ui.Action("Save", this.Save),
            Ui.Danger("Delete saved", this.DeleteSaved),
            Ui.Label("Parameters"),
            Ui.Input(this.parameters).MinWidth(34)
        );

        var left = new VStack(
            Ui.Muted("Ctrl+Enter runs. Parameters are a JSON object, e.g. {\"id\": \"abc\"}, bound as @id."),
            editor,
            toolbar,
            new ComputedVisual(() => this.error.Value.Length == 0 ? Ui.Nothing() : Ui.Failure(this.error.Value)),
            new Markup(() => this.summary.Value),
            this.results.OrEmpty("Nothing run yet.").Stretch()
        ).Spacing(1);

        return new HSplitter()
            .First(left)
            .Second(Ui.Panel("Saved", this.saved.OrEmpty("Nothing saved for this connection yet.")))
            .Ratio(0.74)
            .MinSecond(24);
    }

    public async Task LoadSavedQueries(CancellationToken ct)
    {
        var queries = await this.shell.Profiles.ListQueries(this.profileId, ct);

        // Built inside the post: a [Bindable] property refuses a write from anywhere but the render
        // thread, so a row constructed out here would throw.
        this.shell.Post(() => this.saved.Replace(queries.Select(q => new SavedQueryRow
        {
            Name = q.Name,
            SavedAt = Ui.Timestamp(q.SavedAt),
            Query = q
        })));
    }

    void Run()
    {
        var sql = this.text.Value;
        if (string.IsNullOrWhiteSpace(sql))
        {
            this.error.Value = "Nothing to run.";
            return;
        }

        IReadOnlyDictionary<string, object?> bound;
        try
        {
            bound = DocumentAdminService.ParseParameters(this.parameters.Value);
        }
        catch (Exception ex)
        {
            this.error.Value = $"Parameters: {ex.Message}";
            return;
        }

        this.shell.RunAsync(async ct =>
        {
            try
            {
                var result = await this.shell.Admin.ExecuteSql(this.profileId, sql, bound, ct);

                this.shell.Post(() =>
                {
                    this.error.Value = "";
                    this.results.Show(result.Columns, result.Rows);
                    this.summary.Value = result.HasGrid
                        ? $"[dim]{Ui.Number(result.Rows.Count)} row(s) in {Ui.Escape(Ui.Duration(result.Elapsed))}" +
                          (result.Truncated ? " · truncated" : "") + "[/]"
                        : $"[dim]{result.RecordsAffected} row(s) affected in {Ui.Escape(Ui.Duration(result.Elapsed))}[/]";
                });
            }
            catch (Exception ex)
            {
                this.shell.Post(() =>
                {
                    this.error.Value = ex.Message;
                    this.results.Clear();
                    this.summary.Value = "";
                });
            }
        });
    }

    void Explain()
    {
        var sql = this.text.Value;
        if (string.IsNullOrWhiteSpace(sql))
        {
            this.error.Value = "Nothing to explain.";
            return;
        }

        IReadOnlyDictionary<string, object?> bound;
        try
        {
            bound = DocumentAdminService.ParseParameters(this.parameters.Value);
        }
        catch (Exception ex)
        {
            this.error.Value = $"Parameters: {ex.Message}";
            return;
        }

        this.shell.RunAsync(async ct =>
        {
            var plan = await this.shell.Admin.Explain(this.profileId, sql, bound, ct);
            this.shell.Post(() => Modal.Show(
                this.shell,
                plan.Supported ? $"Query plan · {Ui.Duration(plan.Elapsed)}" : "Query plan",
                plan.Supported
                    ? string.Join(Environment.NewLine, plan.Lines)
                    : "This provider offers no query plan the tool can run."
            ));
        }, "Could not explain the statement");
    }

    void Save()
    {
        if (string.IsNullOrWhiteSpace(this.text.Value))
        {
            this.error.Value = "Nothing to save.";
            return;
        }

        var sql = this.text.Value;
        Modal.Prompt(this.shell, "Save query", "Name", "", name =>
        {
            if (string.IsNullOrWhiteSpace(name))
                return;

            this.shell.RunAsync(async ct =>
            {
                await this.shell.Profiles.SaveQuery(new SavedQuery
                {
                    ProfileId = this.profileId,
                    Name = name.Trim(),
                    Sql = sql
                }, ct);

                await this.LoadSavedQueries(ct);
                this.shell.Post(() => this.shell.Success($"Saved '{name.Trim()}'."));
            }, "Could not save the query");
        });
    }

    void DeleteSaved()
    {
        if (this.saved.Selected is not { } row)
        {
            this.shell.Info("Select a saved query first.");
            return;
        }

        Modal.Confirm(this.shell, "Delete saved query", $"Delete '{row.Name}'?", "Delete",
            () => this.shell.RunAsync(async ct =>
            {
                await this.shell.Profiles.DeleteQuery(row.Query.Id, ct);
                await this.LoadSavedQueries(ct);
                this.shell.Post(() => this.shell.Success($"Deleted '{row.Name}'."));
            }, "Could not delete the saved query"));
    }
}

/// <summary>One entry in the saved-query list.</summary>
public sealed partial class SavedQueryRow
{
    [Bindable] public partial string Name { get; set; }
    [Bindable] public partial string SavedAt { get; set; }

    public required SavedQuery Query { get; init; }
}
