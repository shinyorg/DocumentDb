using ShinyDocDbMyAdmin.Tui.Panels;
using ShinyDocDbMyAdmin.Tui.Shell;
using ShinyDocDbMyAdmin.Tui.Widgets;
using XenoAtom.Terminal;
using XenoAtom.Terminal.UI;
using XenoAtom.Terminal.UI.Commands;
using XenoAtom.Terminal.UI.Controls;
using XenoAtom.Terminal.UI.Geometry;
using XenoAtom.Terminal.UI.Input;

namespace ShinyDocDbMyAdmin.Tui.Screens;

/// <summary>
/// The workspace for one document type - browse, structure, history, geometry, full text, vectors,
/// blobs, transfer and the assistant, behind the same tabs the web front end uses.
/// </summary>
/// <remarks>
/// <para>
/// Tabs whose backing data does not exist are not shown, exactly as they are not there: a type with no
/// <c>{table}_history</c> has no history, and offering an empty History tab would suggest the feature
/// was configured and idle rather than absent.
/// </para>
/// <para>
/// Each panel loads on first sight rather than up front. Opening a type would otherwise mean a schema
/// sample, an index read, four sidecar probes and a vector health check before the first row appeared.
/// </para>
/// </remarks>
public sealed class TypeWorkspaceScreen : Screen
{
    readonly List<(string Title, WorkspacePanel Panel)> panels = [];
    readonly HashSet<int> loaded = [];
    readonly State<int> layout = new(0);
    readonly State<int> selectedTab = new(0);
    readonly WorkspaceContext context;

    public TypeWorkspaceScreen(AdminShell shell, string profileId, string table, string typeName) : base(shell)
    {
        this.context = new WorkspaceContext(shell, profileId, table, typeName);
    }

    public override string Title => this.context.TypeName;

    protected override Visual Build()
    {
        var header = new HStack(
            new Markup($"[bold]{Ui.Escape(this.context.TypeName)}[/]"),
            new Markup($"[dim]in {Ui.Escape(this.context.Table)}[/]"),
            new Markup(() => this.context.ReadOnly.Value ? "[yellow]read-only[/]" : "")
        ).Spacing(2);

        // A fresh TabControl per layout change: which tabs exist is decided by probing the database,
        // and the control's tab list is read-only once built. Rebuilding is cheap and happens once
        // per open, where removing tabs one by one would depend on close being permitted.
        var body = new ComputedVisual(() =>
        {
            _ = this.layout.Value;
            return this.BuildTabs();
        });

        return new Padder(new VStack(header, body.Stretch()).Spacing(1)).Padding(new Thickness(1));
    }

    TabControl BuildTabs()
    {
        var control = new TabControl();
        foreach (var (title, panel) in this.panels)
            control.AddTab(new TabPage(new TextBlock(title), panel.View));

        // Selection lives outside the control, so a rebuild lands on the tab that was open rather
        // than snapping back to Browse.
        control.SelectedIndex(this.selectedTab.Bind.Value);
        control.SelectionChanged(this.OnTabChanged);

        return control;
    }

    /// <summary>
    /// Opens the tab with this title, if it is one this type has. Used by the command palette and by
    /// the documentation harness, which needs to photograph a named tab rather than an index that
    /// shifts with whichever sidecars happen to exist.
    /// </summary>
    public bool SelectTab(string title)
    {
        var index = this.panels.FindIndex(p => p.Title.Equals(title, StringComparison.OrdinalIgnoreCase));
        if (index < 0)
            return false;

        this.selectedTab.Value = index;
        this.OnTabChanged();
        return true;
    }

    /// <summary>The tabs this type actually has, in order.</summary>
    public IReadOnlyList<string> TabTitles => [.. this.panels.Select(p => p.Title)];

    /// <summary>The panel behind a tab, or null when this type has no such tab.</summary>
    public WorkspacePanel? Panel(string title)
        => this.panels.FirstOrDefault(p => p.Title.Equals(title, StringComparison.OrdinalIgnoreCase)).Panel;

    void Step(int delta)
    {
        if (this.panels.Count == 0)
            return;

        this.selectedTab.Value = (this.selectedTab.Value + delta + this.panels.Count) % this.panels.Count;
    }

    public override async Task Load(CancellationToken ct)
    {
        var profile = await this.Shell.Profiles.Get(this.context.ProfileId, ct);
        var readOnly = profile?.ReadOnly ?? true;

        // Which optional tabs exist is a property of the database, so it is asked once here rather
        // than by each panel deciding whether to render itself.
        var hasHistory = await Probe(() => this.Shell.Admin.HasHistorySidecar(this.context.ProfileId, this.context.Table, ct));
        var hasBlobs = await Probe(() => this.Shell.Admin.HasBlobSidecar(this.context.ProfileId, this.context.Table, ct));

        var geometry = await ProbeList(() => this.Shell.Admin.FindGeometryPaths(this.context.ProfileId, this.context.Table, this.context.TypeName, ct));
        var vectors = await ProbeList(() => this.Shell.Admin.FindVectorPaths(this.context.ProfileId, this.context.Table, this.context.TypeName, ct));

        var fullText = false;
        try
        {
            var index = await this.Shell.Admin.DescribeFullTextIndex(this.context.ProfileId, this.context.Table, this.context.TypeName, ct);
            fullText = index.Present;
        }
        catch (Exception)
        {
            // A provider that cannot describe its full-text index has none to describe.
        }

        var assistant = this.Shell.AiAvailability.IsEnabled
                        && await this.Shell.Profiles.GetAiSettings(this.context.ProfileId, ct) is { Enabled: true };

        this.Shell.Post(() =>
        {
            this.context.ReadOnly.Value = readOnly;
            this.Rebuild(hasHistory, geometry, fullText, vectors, hasBlobs, assistant);
        });
    }

    static async Task<bool> Probe(Func<Task<bool>> probe)
    {
        try
        {
            return await probe();
        }
        catch (Exception)
        {
            return false;
        }
    }

    static async Task<bool> ProbeList<T>(Func<Task<IReadOnlyList<T>>> probe)
    {
        try
        {
            return (await probe()).Count > 0;
        }
        catch (Exception)
        {
            return false;
        }
    }

    void Rebuild(bool history, bool geometry, bool fullText, bool vectors, bool blobs, bool assistant)
    {
        var selected = this.selectedTab.Value >= 0 && this.selectedTab.Value < this.panels.Count
            ? this.panels[this.selectedTab.Value].Title
            : null;

        this.panels.Clear();
        this.loaded.Clear();

        this.Add("Browse", new BrowsePanel(this.context));
        this.Add("Structure", new StructurePanel(this.context));

        if (history)
            this.Add("History", new TemporalPanel(this.context));

        if (geometry)
            this.Add("Geometry", new GeometryPanel(this.context));

        if (fullText)
            this.Add("Full text", new FullTextPanel(this.context));

        if (vectors)
            this.Add("Vectors", new VectorsPanel(this.context));

        if (blobs)
            this.Add("Blobs", new BlobsPanel(this.context));

        this.Add("Import / export", new TransferPanel(this.context));

        if (assistant)
            this.Add("Assistant", new AssistantPanel(this.context));

        var restored = selected is null ? 0 : this.panels.FindIndex(p => p.Title == selected);
        this.selectedTab.Value = restored < 0 ? 0 : restored;
        this.layout.Value++;
        this.OnTabChanged();
    }

    void Add(string title, WorkspacePanel panel) => this.panels.Add((title, panel));

    void OnTabChanged()
    {
        var index = this.selectedTab.Value;
        if (index < 0 || index >= this.panels.Count || !this.loaded.Add(index))
            return;

        var panel = this.panels[index].Panel;

        // Built before the load starts. Panels keep controls they later fill - a path picker, a metric
        // picker - and a load that ran first would fill nothing and then be replaced by an empty one.
        _ = panel.View;

        this.Shell.RunAsync(panel.Load, $"Could not load {this.panels[index].Title}");
    }

    /// <summary>
    /// Tab switching lives here - in the palette - rather than on a gesture.
    /// </summary>
    /// <remarks>
    /// A command's gesture is routed from the focused element, and a screen-level one is unreachable
    /// whenever the focus is in the explorer, which is most of the time. The tab strip's own arrow keys
    /// still work once it has focus; this is the way that works from anywhere.
    /// </remarks>
    public override IEnumerable<Command> Commands()
    {
        yield return new Command
        {
            Id = "workspace.prevtab",
            Name = "Previous tab",
            LabelMarkup = "Previous tab",
            Execute = _ => this.Step(-1)
        };

        yield return new Command
        {
            Id = "workspace.nexttab",
            Name = "Next tab",
            LabelMarkup = "Next tab",
            Execute = _ => this.Step(1)
        };

        yield return new Command
        {
            Id = "workspace.sql",
            Name = "Query console",
            LabelMarkup = "Query console",
            Execute = _ => this.Shell.Push(new QueryConsoleScreen(this.Shell, this.context.ProfileId))
        };

        yield return new Command
        {
            Id = "workspace.reloadtab",
            Name = "Reload this tab",
            LabelMarkup = "Reload this tab",
            Execute = _ =>
            {
                this.loaded.Remove(this.selectedTab.Value);
                this.OnTabChanged();
            }
        };
    }
}
