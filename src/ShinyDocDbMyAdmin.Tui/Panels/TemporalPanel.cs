using ShinyDocDbMyAdmin.Models;
using ShinyDocDbMyAdmin.Tui.Widgets;
using XenoAtom.Terminal;
using XenoAtom.Terminal.UI;
using XenoAtom.Terminal.UI.Controls;
using XenoAtom.Terminal.UI.Input;

namespace ShinyDocDbMyAdmin.Tui.Panels;

/// <summary>One row of the version list.</summary>
public sealed partial class VersionRow
{
    [Bindable] public partial string DocumentId { get; set; }
    [Bindable] public partial string Version { get; set; }
    [Bindable] public partial string Operation { get; set; }
    [Bindable] public partial string ValidFrom { get; set; }
    [Bindable] public partial string Duration { get; set; }
    [Bindable] public partial string Actor { get; set; }

    public required DocumentVersion Value { get; init; }
}

/// <summary>
/// Temporal history: every version of a document, the differences between any two, and restore.
/// </summary>
/// <remarks>
/// Restoring writes the old body back as a new version rather than rewinding the sidecar. History
/// moves forward - that is what makes it an audit trail rather than a working copy, and it is what the
/// library itself does.
/// </remarks>
public sealed class TemporalPanel(WorkspaceContext context) : WorkspacePanel(context)
{
    readonly RowGrid<VersionRow> grid = new(
        (VersionRow.Accessor.DocumentId, "Document", ColumnWidth.Fill),
        (VersionRow.Accessor.Version, "Version", ColumnWidth.Fit),
        (VersionRow.Accessor.Operation, "Operation", ColumnWidth.Fit),
        (VersionRow.Accessor.ValidFrom, "Valid from", ColumnWidth.Fit),
        (VersionRow.Accessor.Duration, "Stood for", ColumnWidth.Fit),
        (VersionRow.Accessor.Actor, "Actor", ColumnWidth.Fit)
    );

    readonly State<string> documentFilter = new("");
    readonly State<string> actorFilter = new("");
    readonly State<string> summary = new("");
    readonly State<string> error = new("");

    // Plain fields, not State: the load runs on the thread pool, and a State may only be read from the
    // render thread. What the boxes hold is copied into these when the user applies them.
    string documentId = "";
    string actor = "";

    VersionRow? diffAnchor;

    protected override Visual Build()
    {
        this.grid.OnActivate(this.ShowBody);

        var documentBox = Ui.Input(this.documentFilter).MinWidth(28);
        documentBox.AddKeyBinding(new KeyGesture(TerminalKey.Enter), this.Apply);

        var toolbar = Ui.Toolbar(
            Ui.Action("Refresh", this.Reload),
            Ui.Label("Document"), documentBox,
            Ui.Label("Actor"), Ui.Input(this.actorFilter).MinWidth(18),
            Ui.Action("Apply", this.Apply),
            Ui.Primary("View body", () => this.WithSelection(this.ShowBody)),
            Ui.Action("Mark for diff", () => this.WithSelection(this.MarkForDiff)),
            Ui.Action("Diff against mark", () => this.WithSelection(this.Diff)),
            Ui.Danger("Restore", () => this.WithSelection(this.Restore)).IsVisible(() => this.Context.CanWrite)
        );

        return new VStack(
            Ui.Muted(
                "Leave the document blank for the newest versions across the whole type. " +
                "Restoring appends a new version rather than rewriting history."),
            toolbar,
            new ComputedVisual(() => this.error.Value.Length == 0 ? Ui.Nothing() : Ui.Failure(this.error.Value)),
            new Markup(() => this.summary.Value),
            this.grid.OrEmpty("No versions recorded.").Stretch()
        ).Spacing(1);
    }

    /// <summary>Takes what is in the boxes - on the render thread, where they may be read.</summary>
    void Apply()
    {
        this.documentId = this.documentFilter.Value.Trim();
        this.actor = this.actorFilter.Value.Trim();
        this.Reload();
    }

    void Reload() => this.Context.Run(this.Load, "Could not read the history");

    public override async Task Load(CancellationToken ct)
    {
        try
        {
            var documentId = this.documentId;
            var actor = this.actor;

            var versions = documentId.Length > 0
                ? await this.Context.Admin.ListVersions(this.Context.ProfileId, this.Context.Table, this.Context.TypeName, documentId, ct)
                : await this.Context.Admin.ListRecentVersions(this.Context.ProfileId, this.Context.Table, this.Context.TypeName,
                    actor.Length == 0 ? null : actor, ct: ct);

            var ordered = versions
                .OrderByDescending(v => v.ValidFrom)
                .ThenByDescending(v => v.Version)
                .ToList();

            // Built inside the post: a [Bindable] property refuses a write from anywhere but the
            // render thread, so a row constructed out here would throw.
            this.Context.Post(() =>
            {
                this.error.Value = "";
                this.grid.Replace(ordered.Select(v => new VersionRow
                {
                    DocumentId = v.DocumentId,
                    Version = v.Version.ToString(),
                    Operation = v.IsCurrent ? $"{v.Operation} · current" : v.Operation,
                    ValidFrom = Ui.Timestamp(v.ValidFrom),
                    Duration = v.Duration is { } span ? Describe(span) : (v.ValidTo is null ? "still current" : "—"),
                    Actor = v.Actor ?? "—",
                    Value = v
                }));

                this.summary.Value = documentId.Length > 0
                    ? $"[dim]{ordered.Count} version(s) of '{Ui.Escape(documentId)}'[/]"
                    : $"[dim]{ordered.Count} most recent version(s) across the type[/]";
            });
        }
        catch (Exception ex)
        {
            this.Context.Post(() =>
            {
                this.error.Value = ex.Message;
                this.grid.Replace([]);
            });
        }
    }

    static string Describe(TimeSpan span)
        => span.TotalDays >= 1 ? $"{span.TotalDays:0.#} d"
            : span.TotalHours >= 1 ? $"{span.TotalHours:0.#} h"
            : span.TotalMinutes >= 1 ? $"{span.TotalMinutes:0.#} m"
            : $"{span.TotalSeconds:0.#} s";

    void WithSelection(Action<VersionRow> action)
    {
        if (this.grid.Selected is { } row)
            action(row);
        else
            this.Shell.Info("Select a version first.");
    }

    void ShowBody(VersionRow row)
    {
        if (row.Value.Json is not { } json)
            this.Shell.Info("This is a delete tombstone - there is no body to show.");
        else
            JsonDocumentDialog.Show(this.Shell, $"{row.DocumentId} · version {row.Version}", json);
    }

    void MarkForDiff(VersionRow row)
    {
        this.diffAnchor = row;
        this.Shell.Info($"Marked {row.DocumentId} version {row.Version}. Select another and press Diff against mark.");
    }

    void Diff(VersionRow row)
    {
        if (this.diffAnchor is not { } anchor)
        {
            this.Shell.Info("Mark a version first.");
            return;
        }

        if (anchor.Value.DocumentId != row.Value.DocumentId)
        {
            this.Shell.Warn("Both versions have to be of the same document.");
            return;
        }

        if (anchor.Value.Version == row.Value.Version)
        {
            this.Shell.Info("That is the version you marked.");
            return;
        }

        this.Context.Run(async ct =>
        {
            var comparison = await this.Context.Admin.CompareVersions(
                this.Context.ProfileId,
                this.Context.Table,
                this.Context.TypeName,
                row.Value.DocumentId,
                anchor.Value.Version,
                row.Value.Version,
                ct
            );

            this.Context.Post(() => this.Shell.OpenDialog(JsonDiffDialog.Create(this.Shell, comparison)));
        }, "Could not compare the versions");
    }

    void Restore(VersionRow row)
    {
        if (!this.Context.CanWrite)
        {
            this.Shell.Warn("This connection is read-only.");
            return;
        }

        if (row.Value.IsTombstone)
        {
            this.Shell.Info("A delete tombstone has no body to restore.");
            return;
        }

        Modal.Confirm(
            this.Shell,
            "Restore version",
            $"Write version {row.Version} of '{row.DocumentId}' back as the current document? " +
            "This appends a new version - the history in between is kept.",
            "Restore",
            () => this.Context.Run(async ct =>
            {
                await this.Context.Admin.RestoreVersion(
                    this.Context.ProfileId, this.Context.Table, this.Context.TypeName,
                    row.Value.DocumentId, row.Value.Version, ct);

                await this.Load(ct);
                this.Context.Post(() => this.Shell.Success($"Restored version {row.Version}."));
            }, "Could not restore the version")
        );
    }
}
