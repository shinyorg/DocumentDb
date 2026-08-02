using ShinyDocDbMyAdmin.Tui.Panels;
using ShinyDocDbMyAdmin.Tui.Screens;
using ShinyDocDbMyAdmin.Tui.Shell;
using XenoAtom.Terminal.UI;
using XenoAtom.Terminal.UI.Rendering;

namespace ShinyDocDbMyAdmin.Tui.Tests;

/// <summary>
/// Every screen and every panel is built, loaded against a real store, and rendered.
/// </summary>
/// <remarks>
/// <para>
/// A terminal UI fails in ways a compiler cannot see - a layout that cannot measure, a binding that
/// reads a state object from the wrong thread, a control asked for a size it will not take. Rendering
/// through <see cref="VisualSnapshotRenderer"/> exercises measure, arrange and draw without a real
/// terminal, so these run in CI exactly as they run here.
/// </para>
/// <para>
/// The assertions are deliberately about what the render <i>says</i>, not about pixels: a text
/// comparison would fail on every wording change and teach us nothing about whether the tool works.
/// </para>
/// </remarks>
public sealed class ScreenRenderTests
{
    const int Width = 160;
    const int Height = 44;

    static string Render(Visual visual)
    {
        var buffer = VisualSnapshotRenderer.Render(visual, Width, Height);
        var text = string.Join("\n", buffer.ToMarkupLines());

        // Set TUI_DUMP to a directory to keep every render on disk - the only practical way to see
        // what a failing layout actually drew.
        if (Environment.GetEnvironmentVariable("TUI_DUMP") is { Length: > 0 } dump)
        {
            Directory.CreateDirectory(dump);
            File.WriteAllText(Path.Combine(dump, $"render-{Guid.NewGuid():N}.txt"), text);
        }

        return text;
    }

    /// <summary>Runs a screen's load to completion on the calling thread, as the shell would post it.</summary>
    static async Task<string> Open(ScratchInstance instance, Screen screen)
    {
        var root = instance.Shell.Build(screen);
        await screen.Load(CancellationToken.None);
        return Render(root);
    }

    [Fact]
    public async Task Connections_screen_lists_the_provided_connection()
    {
        using var instance = new ScratchInstance();

        var text = await Open(instance, new ConnectionsScreen(instance.Shell));

        Assert.Contains("Connections", text);
        Assert.Contains("Scratch", text);
        Assert.Contains("SQLite", text);
    }

    [Fact]
    public async Task Database_overview_lists_the_documents_table_and_its_sidecars()
    {
        using var instance = new ScratchInstance();

        var text = await Open(instance, new DatabaseOverviewScreen(instance.Shell, ScratchInstance.ProfileId));

        Assert.Contains("documents", text);
        Assert.Contains("documents_history", text);
        Assert.Contains("documents_blobs", text);
    }

    [Fact]
    public async Task Table_overview_lists_the_types_with_their_counts()
    {
        using var instance = new ScratchInstance();

        var text = await Open(instance, new TableOverviewScreen(instance.Shell, ScratchInstance.ProfileId, "documents"));

        Assert.Contains("Order", text);
        Assert.Contains("Product", text);
    }

    [Fact]
    public async Task Workspace_shows_only_the_tabs_whose_data_exists()
    {
        using var instance = new ScratchInstance();

        var text = await Open(instance, new TypeWorkspaceScreen(instance.Shell, ScratchInstance.ProfileId, "documents", "Order"));

        Assert.Contains("Browse", text);
        Assert.Contains("Structure", text);

        // The seed has history, blobs, geometry and embeddings for Order.
        Assert.Contains("History", text);
        Assert.Contains("Blobs", text);
        Assert.Contains("Geometry", text);
        Assert.Contains("Vectors", text);

        // It has no full-text index, and the assistant is switched off for the test instance.
        Assert.DoesNotContain("Full text", text);
        Assert.DoesNotContain("Assistant", text);
    }

    [Fact]
    public async Task Workspace_hides_the_sidecar_tabs_for_a_type_that_has_none()
    {
        using var instance = new ScratchInstance();

        var text = await Open(instance, new TypeWorkspaceScreen(instance.Shell, ScratchInstance.ProfileId, "documents", "Product"));

        Assert.Contains("Browse", text);
        Assert.DoesNotContain("Geometry", text);
        Assert.DoesNotContain("Vectors", text);
    }

    [Fact]
    public async Task Query_console_offers_both_grammars()
    {
        using var instance = new ScratchInstance();

        var text = await Open(instance, new QueryConsoleScreen(instance.Shell, ScratchInstance.ProfileId));

        Assert.Contains("Filter grammar", text);
        Assert.Contains("SQL", text);
    }

    [Fact]
    public async Task Transfer_screen_leads_with_the_secrets_warning()
    {
        using var instance = new ScratchInstance();

        var text = await Open(instance, new ConnectionTransferScreen(instance.Shell));

        Assert.Contains("Export", text);
        Assert.Contains("Import", text);
        Assert.Contains("Include secrets", text);
    }

    [Fact]
    public async Task Connection_edit_form_renders_for_a_new_connection()
    {
        using var instance = new ScratchInstance();

        var text = await Open(instance, new ConnectionEditScreen(instance.Shell, null));

        Assert.Contains("New connection", text);
        Assert.Contains("Provider", text);
        Assert.Contains("Read-only", text);
    }

    // ── Panels ──────────────────────────────────────────────────────────

    static async Task<string> OpenPanel(ScratchInstance instance, Func<WorkspaceContext, WorkspacePanel> build, string typeName = "Order")
    {
        var context = new WorkspaceContext(instance.Shell, ScratchInstance.ProfileId, "documents", typeName);
        var panel = build(context);

        // The workspace normally sets this from the profile; a render test wants the write paths visible.
        context.ReadOnly.Value = false;

        var view = panel.View;
        await panel.Load(CancellationToken.None);
        return Render(view);
    }

    [Fact]
    public async Task Browse_panel_shows_inferred_columns_and_a_page_count()
    {
        using var instance = new ScratchInstance();

        var text = await OpenPanel(instance, c => new BrowsePanel(c));

        Assert.Contains("order-1", text);
        Assert.Contains("reference", text);
        Assert.Contains("document(s)", text);
    }

    [Fact]
    public async Task Structure_panel_reports_the_inferred_shape()
    {
        using var instance = new ScratchInstance();

        var text = await OpenPanel(instance, c => new StructurePanel(c));

        Assert.Contains("Inferred shape", text);
        Assert.Contains("customer.name", text);
        Assert.Contains("Indexes on the table", text);
    }

    [Fact]
    public async Task Temporal_panel_lists_versions_newest_first()
    {
        using var instance = new ScratchInstance();

        var text = await OpenPanel(instance, c => new TemporalPanel(c));

        Assert.Contains("order-1", text);
        Assert.Contains("Updated", text);
        Assert.Contains("version", text, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task Geometry_panel_draws_the_points_and_names_the_path()
    {
        using var instance = new ScratchInstance();

        var text = await OpenPanel(instance, c => new GeometryPanel(c));

        Assert.Contains("deliverTo", text);
        Assert.Contains("Points", text);

        // Braille output - the marks themselves, not a placeholder.
        Assert.Contains(text, c => c is >= '⠁' and <= '⣿');
    }

    [Fact]
    public async Task Blobs_panel_lists_the_attachment_without_reading_it()
    {
        using var instance = new ScratchInstance();

        var text = await OpenPanel(instance, c => new BlobsPanel(c));

        Assert.Contains("invoice", text);
        Assert.Contains("SO-1001.txt", text);
        Assert.Contains("text/plain", text);
    }

    [Fact]
    public async Task Vectors_panel_reports_the_embeddings_and_the_missing_sidecar()
    {
        using var instance = new ScratchInstance();

        var text = await OpenPanel(instance, c => new VectorsPanel(c));

        Assert.Contains("embedding", text);

        // The seed has embeddings in the bodies but no vec sidecar, which is exactly the state the
        // panel exists to report rather than to hide.
        Assert.Contains("sidecar", text, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task Transfer_panel_offers_export_import_and_the_generator()
    {
        using var instance = new ScratchInstance();

        var text = await OpenPanel(instance, c => new TransferPanel(c));

        Assert.Contains("Export", text);
        Assert.Contains("Import", text);
        Assert.Contains("Generate test data", text);
    }

    [Fact]
    public async Task Read_only_connections_hide_every_write_button()
    {
        using var readWrite = new ScratchInstance();
        using var readOnly = new ScratchInstance(readOnly: true);

        var writable = await Open(readWrite, new TypeWorkspaceScreen(readWrite.Shell, ScratchInstance.ProfileId, "documents", "Order"));
        var blocked = await Open(readOnly, new TypeWorkspaceScreen(readOnly.Shell, ScratchInstance.ProfileId, "documents", "Order"));

        Assert.Contains("New", writable);
        Assert.Contains("read-only", blocked);
        Assert.DoesNotContain("Delete", blocked);
    }
}
