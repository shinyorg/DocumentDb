using ShinyDocDbMyAdmin.Models;
using ShinyDocDbMyAdmin.Providers;
using ShinyDocDbMyAdmin.Tui.Screens;
using ShinyDocDbMyAdmin.Tui.Widgets;
using XenoAtom.Terminal;
using XenoAtom.Terminal.UI;
using XenoAtom.Terminal.UI.Controls;
using XenoAtom.Terminal.UI.Input;

namespace ShinyDocDbMyAdmin.Tui.Shell;

/// <summary>
/// The left-hand tree: connections, their document tables, and the types inside them.
/// </summary>
/// <remarks>
/// <para>
/// Same shape and same lazy loading as the web front end's <c>ExplorerTree</c> - a branch fetches its
/// children the first time it is opened, and a connection that will not open shows the reason on the
/// branch rather than swallowing it. Sidecar tables are left out here for the same reason they are
/// there: they are visible in the Structure tab, and the tree is for what you can browse.
/// </para>
/// <para>
/// <c>TreeNode</c> raises no expansion event, so expansion is picked up from the framework's own
/// binding change stream and matched against the nodes this pane created. That is one subscription for
/// the whole tree rather than a handler per node, and it is the only hook the control offers.
/// </para>
/// </remarks>
public sealed class ExplorerPane : IDisposable
{
    readonly AdminShell shell;
    readonly TreeView tree = new();
    readonly Dictionary<TreeNode, Action<TreeNode>> loaders = [];

    public ExplorerPane(AdminShell shell)
    {
        this.shell = shell;

        // Enter opens whatever is selected; the tree's own arrow keys handle expansion.
        this.tree.AddKeyBinding(new KeyGesture(TerminalKey.Enter), this.OpenSelected);

        this.View = new Group(
            new Markup("[bold]Connections[/]"),
            new VStack(
                new HStack(
                    Ui.Action("+ Add", () => this.shell.Push(new ConnectionEditScreen(this.shell, null))),
                    Ui.Action("Refresh", this.Reload)
                ).Spacing(1),
                new ScrollViewer(this.tree).Stretch()
            ).Spacing(1)
        );

        BindingManager.Current.ValueChanged += this.OnBindingChanged;
    }

    public Visual View { get; }

    /// <summary>The tree itself, so the shell can put the cursor in it.</summary>
    public Visual Tree => this.tree;

    public void Dispose() => BindingManager.Current.ValueChanged -= this.OnBindingChanged;

    void OnBindingChanged(Binding binding)
    {
        if (binding.Owner is TreeNode node && binding.Accessor.Name == nameof(TreeNode.IsExpanded) && node.IsExpanded)
            this.Fill(node);
    }

    /// <summary>Runs a branch's loader, once. A branch already filled - or filling - is left alone.</summary>
    void Fill(TreeNode node)
    {
        if (this.loaders.Remove(node, out var load))
            load(node);
    }

    public void Reload() => this.shell.RunAsync(async ct =>
    {
        var profiles = await this.shell.Profiles.List(ct);
        this.shell.Post(() => this.Rebuild(profiles));
    }, "Could not list connections");

    /// <summary>
    /// Expands every connection, which loads its tables through the ordinary expansion path rather
    /// than through a second one that could disagree with it.
    /// </summary>
    public void ExpandAll()
    {
        foreach (var node in this.tree.Roots)
        {
            node.IsExpanded = true;

            // Asked for directly as well as through the change stream: setting the property only
            // raises a change when something is tracking reads of it, which nothing is until the tree
            // has been rendered once.
            this.Fill(node);
        }
    }

    void Rebuild(IReadOnlyList<ConnectionProfile> profiles)
    {
        this.tree.Roots.Clear();
        this.loaders.Clear();

        foreach (var profile in profiles)
        {
            var badge = ProviderCatalog.Get(profile.Provider).Badge;
            var readOnly = profile.ReadOnly ? " [yellow]ro[/]" : "";
            var node = new TreeNode(new Markup($"{Ui.Escape(profile.Name)} [dim]{badge}[/]{readOnly}"))
            {
                Data = new ProfileTarget(profile.Id)
            };

            // A placeholder child is what makes the branch expandable before its contents are known.
            node.Children.Add(Placeholder("…"));
            this.loaders[node] = n => this.LoadTables(profile.Id, n);

            this.tree.Roots.Add(node);
        }

        if (profiles.Count == 0)
            this.tree.Roots.Add(Placeholder("No connections yet - press + Add"));
    }

    void LoadTables(string profileId, TreeNode node) => this.shell.RunAsync(async ct =>
    {
        try
        {
            var tables = await this.shell.Admin.ListTables(profileId, refresh: false, ct);
            var browsable = tables.Where(t => t.IsBrowsable).ToList();
            this.shell.Post(() => this.FillTables(profileId, node, browsable));
        }
        catch (Exception ex)
        {
            // Shown on the branch rather than as a toast: a connection that will not open is a
            // property of that connection, and the tree is where you are looking when you find out.
            this.shell.Post(() => Replace(node, Placeholder($"[red]{Ui.Escape(Describe(ex))}[/]")));
        }
    });

    void FillTables(string profileId, TreeNode node, IReadOnlyList<TableInfo> tables)
    {
        node.Children.Clear();

        if (tables.Count == 0)
        {
            node.Children.Add(Placeholder("No document tables"));
            return;
        }

        foreach (var table in tables)
        {
            var tenant = table.HasTenantColumn ? " [dim]tenant[/]" : "";
            var child = new TreeNode(new Markup($"{Ui.Escape(table.Name)}{tenant}"))
            {
                Data = new TableTarget(profileId, table.Name)
            };

            child.Children.Add(Placeholder("…"));
            this.loaders[child] = n => this.LoadTypes(profileId, table.Name, n);
            node.Children.Add(child);
        }
    }

    void LoadTypes(string profileId, string table, TreeNode node) => this.shell.RunAsync(async ct =>
    {
        try
        {
            var types = await this.shell.Admin.ListTypes(profileId, table, ct);
            this.shell.Post(() => FillTypes(profileId, table, node, types));
        }
        catch (Exception ex)
        {
            this.shell.Post(() => Replace(node, Placeholder($"[red]{Ui.Escape(Describe(ex))}[/]")));
        }
    });

    static void FillTypes(string profileId, string table, TreeNode node, IReadOnlyList<DocumentTypeInfo> types)
    {
        node.Children.Clear();

        if (types.Count == 0)
        {
            node.Children.Add(Placeholder("Empty table"));
            return;
        }

        foreach (var type in types)
        {
            node.Children.Add(new TreeNode(new Markup($"{Ui.Escape(type.TypeName)} [dim]{Ui.Number(type.Count)}[/]"))
            {
                Data = new TypeTarget(profileId, table, type.TypeName)
            });
        }
    }

    void OpenSelected()
    {
        switch (this.tree.SelectedNode?.Data)
        {
            case ProfileTarget profile:
                this.shell.Push(new DatabaseOverviewScreen(this.shell, profile.ProfileId));
                break;

            case TableTarget table:
                this.shell.Push(new TableOverviewScreen(this.shell, table.ProfileId, table.Table));
                break;

            case TypeTarget type:
                this.shell.Push(new TypeWorkspaceScreen(this.shell, type.ProfileId, type.Table, type.TypeName));
                break;
        }
    }

    static void Replace(TreeNode node, TreeNode child)
    {
        node.Children.Clear();
        node.Children.Add(child);
    }

    static TreeNode Placeholder(string markup) => new(new Markup($"[dim]{markup}[/]")) { Data = PlaceholderTarget.Instance };

    static string Describe(Exception ex) => ex.Message.Length > 120 ? ex.Message[..117] + "…" : ex.Message;

    sealed record ProfileTarget(string ProfileId);
    sealed record TableTarget(string ProfileId, string Table);
    sealed record TypeTarget(string ProfileId, string Table, string TypeName);

    sealed class PlaceholderTarget
    {
        public static readonly PlaceholderTarget Instance = new();
    }
}
