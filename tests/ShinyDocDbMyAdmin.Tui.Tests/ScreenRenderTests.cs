using ShinyDocDbMyAdmin.Models;
using ShinyDocDbMyAdmin.Providers;
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

    /// <summary>
    /// The chrome the web front end has too: a brand corner above the menu, and a credit bar under the
    /// whole shell with the site centred and the version hard right.
    /// </summary>
    [Fact]
    public async Task The_frame_carries_the_brand_corner_and_the_version_footer()
    {
        using var instance = new ScratchInstance();

        var text = await Open(instance, new ConnectionsScreen(instance.Shell));
        var lines = text.Split('\n');

        // Top-left: the mark's half blocks, then the wordmark, before anything the menu contributes.
        Assert.Contains("▀", lines[0]);
        Assert.Contains("ShinyDocDbMyAdmin", lines[0]);
        Assert.Contains("File", lines[1]);

        // Bottom: the credit bar, under the status line rather than replacing it.
        var footer = lines[^1];
        Assert.Contains(ShinyDocDbMyAdmin.Services.AppInfo.Site, footer);
        Assert.Contains($"v{ShinyDocDbMyAdmin.Services.AppInfo.Version}", footer);
        Assert.Contains("Ctrl+P commands", lines[^2]);

        // Centred against the bar and not merely left of the version: the credit's midpoint lands within a
        // couple of cells of the window's, which is the whole reason the footer is a 1*/auto/1* grid.
        var plain = System.Text.RegularExpressions.Regex.Replace(footer, @"\[[^\]]*\]", "");
        var credit = plain.IndexOf("Built with Shiny", StringComparison.Ordinal);
        var site = plain.IndexOf(ShinyDocDbMyAdmin.Services.AppInfo.Site, StringComparison.Ordinal);
        var middle = (credit + site + ShinyDocDbMyAdmin.Services.AppInfo.Site.Length) / 2.0;

        Assert.InRange(middle, Width / 2.0 - 2, Width / 2.0 + 2);
        Assert.EndsWith($"v{ShinyDocDbMyAdmin.Services.AppInfo.Version}", plain.TrimEnd());
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

    [Fact]
    public async Task Connection_edit_provider_select_shows_the_display_name()
    {
        using var instance = new ScratchInstance();

        var text = await Open(instance, new ConnectionEditScreen(instance.Shell, null));

        // The select is built over display names, not the descriptors themselves - a Select<ProviderDescriptor>
        // renders the record's ToString() into the closed dropdown.
        Assert.Contains("SQLite", text);
        Assert.DoesNotContain("ProviderDescriptor", text);
        Assert.DoesNotContain("ConnectionStringTemplate", text);
    }

    [Fact]
    public async Task Connection_edit_provider_select_opens_on_the_saved_provider()
    {
        using var instance = new ScratchInstance();

        var profile = new ConnectionProfile { Name = "Reporting", Provider = ProviderKind.PostgreSql };
        await instance.Shell.Profiles.Save(profile, "Host=localhost;Database=reporting", null);

        var text = await Open(instance, new ConnectionEditScreen(instance.Shell, profile.Id));

        // The saved provider has to select itself by index into the same catalog order the labels came from.
        Assert.Contains("PostgreSQL", text);
        Assert.DoesNotContain("ProviderDescriptor", text);
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
    public async Task Outbox_screen_shows_the_health_strip_and_the_messages()
    {
        using var instance = new ScratchInstance();

        var text = await Open(instance, new OutboxScreen(instance.Shell, ScratchInstance.ProfileId));

        Assert.Contains("Outbox", text);
        Assert.Contains("pending", text);
        Assert.Contains("dead-lettered", text);
        Assert.Contains("Shop.OrderPlaced", text);
        Assert.Contains("DeadLettered", text);

        // The single most likely wrong assumption about this screen, so it is stated on it.
        Assert.Contains("cannot deliver", text);
    }

    [Fact]
    public async Task Outbox_screen_says_so_when_there_is_no_outbox()
    {
        // Every other suite runs against SampleData, which has an outbox; this one deliberately does not.
        using var instance = new ScratchInstance(seedSql: """
            CREATE TABLE IF NOT EXISTS "documents" (
                Id TEXT NOT NULL,
                TypeName TEXT NOT NULL,
                Data TEXT NOT NULL,
                CreatedAt TEXT NOT NULL,
                UpdatedAt TEXT NOT NULL,
                PRIMARY KEY (Id, TypeName)
            );
            INSERT INTO "documents" (Id, TypeName, Data, CreatedAt, UpdatedAt)
            VALUES ('order-1', 'Order', '{"id":"order-1"}',
                    '2026-01-04 09:15:00.000000+00:00', '2026-01-04 09:15:00.000000+00:00');
            """);

        var text = await Open(instance, new OutboxScreen(instance.Shell, ScratchInstance.ProfileId));

        Assert.Contains("No outbox in this database", text);
    }

    [Fact]
    public async Task Outbox_screen_hides_its_write_actions_on_a_read_only_connection()
    {
        using var readWrite = new ScratchInstance();
        using var readOnly = new ScratchInstance(readOnly: true);

        var writable = await Open(readWrite, new OutboxScreen(readWrite.Shell, ScratchInstance.ProfileId));
        var blocked = await Open(readOnly, new OutboxScreen(readOnly.Shell, ScratchInstance.ProfileId));

        Assert.Contains("Requeue", writable);
        Assert.Contains("Purge", writable);
        Assert.DoesNotContain("Requeue", blocked);
        Assert.DoesNotContain("Purge", blocked);
    }

    [Fact]
    public async Task Outbox_commands_drop_the_write_entries_on_a_read_only_connection()
    {
        using var readWrite = new ScratchInstance();
        using var readOnly = new ScratchInstance(readOnly: true);

        var writable = new OutboxScreen(readWrite.Shell, ScratchInstance.ProfileId);
        var blocked = new OutboxScreen(readOnly.Shell, ScratchInstance.ProfileId);
        readWrite.Shell.Build(writable);
        readOnly.Shell.Build(blocked);
        await writable.Load(CancellationToken.None);
        await blocked.Load(CancellationToken.None);

        // In a terminal the palette is the address bar, so a command listed there has to be usable —
        // omitting it is a better answer than failing on invoke.
        Assert.Contains(writable.Commands(), c => c.Id == "outbox.requeue");
        Assert.DoesNotContain(blocked.Commands(), c => c.Id == "outbox.requeue");
        Assert.Contains(blocked.Commands(), c => c.Id == "outbox.filter.pending");
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
