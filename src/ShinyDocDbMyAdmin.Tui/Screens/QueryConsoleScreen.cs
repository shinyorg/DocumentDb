using ShinyDocDbMyAdmin.Tui.Panels;
using ShinyDocDbMyAdmin.Tui.Shell;
using ShinyDocDbMyAdmin.Tui.Widgets;
using XenoAtom.Terminal.UI;
using XenoAtom.Terminal.UI.Commands;
using XenoAtom.Terminal.UI.Controls;
using XenoAtom.Terminal.UI.Geometry;

namespace ShinyDocDbMyAdmin.Tui.Screens;

/// <summary>
/// The two query consoles: DocumentDb's own string grammar, and the database's SQL.
/// </summary>
/// <remarks>
/// Both are here rather than in the type workspace because a query is a property of the connection,
/// not of one type - and because the interesting thing about the filter console is that it shows you
/// the SQL it compiled to, which only makes sense next to a console where you could have written that
/// SQL yourself.
/// </remarks>
public sealed class QueryConsoleScreen : Screen
{
    readonly FilterConsolePanel filter;
    readonly SqlConsolePanel sql;
    readonly State<int> selected = new(0);

    public QueryConsoleScreen(AdminShell shell, string profileId) : base(shell)
    {
        this.ProfileId = profileId;
        this.filter = new FilterConsolePanel(shell, profileId, this.OpenInSql);
        this.sql = new SqlConsolePanel(shell, profileId);
    }

    public string ProfileId { get; }

    public override string Title => "Query console";

    protected override Visual Build()
    {
        var tabs = new TabControl(
        [
            new TabPage(new TextBlock("Filter grammar"), this.filter.View),
            new TabPage(new TextBlock("SQL"), this.sql.View)
        ]);

        tabs.SelectedIndex(this.selected.Bind.Value);

        return new Padder(new VStack(
            Ui.Title("Query console"),
            tabs.Stretch()
        ).Spacing(1)).Padding(new Thickness(1));
    }

    public override Task Load(CancellationToken ct) => this.sql.LoadSavedQueries(ct);

    /// <summary>
    /// Hands the compiled SQL to the SQL console and switches to it - the move that makes "here is
    /// what your filter became" actionable rather than informational.
    /// </summary>
    void OpenInSql(string statement)
    {
        this.sql.SetText(statement);
        this.selected.Value = 1;
    }

    public override IEnumerable<Command> Commands() =>
    [
        new Command
        {
            Id = "console.filter",
            Name = "Filter grammar console",
            LabelMarkup = "Filter grammar console",
            Execute = _ => this.selected.Value = 0
        },
        new Command
        {
            Id = "console.sql",
            Name = "SQL console",
            LabelMarkup = "SQL console",
            Execute = _ => this.selected.Value = 1
        }
    ];
}
