using System.Text;
using ShinyDocDbMyAdmin.Models;
using ShinyDocDbMyAdmin.Tui.Widgets;
using XenoAtom.Terminal;
using XenoAtom.Terminal.UI;
using XenoAtom.Terminal.UI.Controls;
using XenoAtom.Terminal.UI.Input;

namespace ShinyDocDbMyAdmin.Tui.Panels;

/// <summary>One row of a <c>{table}_blobs</c> sidecar.</summary>
public sealed partial class BlobRow
{
    [Bindable] public partial string DocumentId { get; set; }
    [Bindable] public partial string Key { get; set; }
    [Bindable] public partial string FileName { get; set; }
    [Bindable] public partial string ContentType { get; set; }
    [Bindable] public partial string Size { get; set; }
    [Bindable] public partial string Updated { get; set; }

    public required BlobInfo Info { get; init; }
}

/// <summary>
/// Attached payloads, listed without ever selecting the bytes.
/// </summary>
/// <remarks>
/// <para>
/// The listing does not read the payload column - a list of a hundred attachments should not move a
/// hundred megabytes to render five columns of metadata. Bytes are read only when you ask for one, and
/// then they go to a file you name rather than anywhere else.
/// </para>
/// <para>
/// Text payloads can be previewed inline; anything else is save-only. The web front end draws that line
/// to avoid serving user-controlled bytes from its own origin. Here the reason is plainer - a terminal
/// has nothing to do with a JPEG, and printing one would fill the screen with control characters.
/// </para>
/// </remarks>
public sealed class BlobsPanel(WorkspaceContext context) : WorkspacePanel(context)
{
    readonly RowGrid<BlobRow> grid = new(
        (BlobRow.Accessor.DocumentId, "Document", ColumnWidth.Fill),
        (BlobRow.Accessor.Key, "Key", ColumnWidth.Fit),
        (BlobRow.Accessor.FileName, "File name", ColumnWidth.Fill),
        (BlobRow.Accessor.ContentType, "Content type", ColumnWidth.Fit),
        (BlobRow.Accessor.Size, "Size", ColumnWidth.Fit),
        (BlobRow.Accessor.Updated, "Updated", ColumnWidth.Fit)
    );

    readonly State<string> documentFilter = new("");
    readonly State<string> summary = new("");
    readonly State<string> error = new("");

    // Plain fields, not State: the load runs on the thread pool, and a State may only be read from the
    // render thread. What the boxes hold is copied into these when the user applies them.
    string documentId = "";

    protected override Visual Build()
    {
        this.grid
            .OnActivate(this.Preview)
            .OnKey(new KeyGesture(TerminalKey.Delete), this.Delete);

        var filterBox = Ui.Input(this.documentFilter).MinWidth(28);
        filterBox.AddKeyBinding(new KeyGesture(TerminalKey.Enter), this.Apply);

        var toolbar = Ui.Toolbar(
            Ui.Action("Refresh", this.Reload),
            Ui.Label("Document"), filterBox,
            Ui.Action("Apply", this.Apply),
            Ui.Primary("Preview", () => this.WithSelection(this.Preview)),
            Ui.Action("Save to file", () => this.WithSelection(this.Save)),
            Ui.Danger("Delete", () => this.WithSelection(this.Delete)).IsVisible(() => this.Context.CanWrite)
        );

        return new VStack(
            Ui.Muted("Payload bytes are read only when you preview or save one - the listing never selects them."),
            toolbar,
            new ComputedVisual(() => this.error.Value.Length == 0 ? Ui.Nothing() : Ui.Failure(this.error.Value)),
            new Markup(() => this.summary.Value),
            this.grid.OrEmpty("No blobs for this type.").Stretch()
        ).Spacing(1);
    }

    /// <summary>Takes what is in the box - on the render thread, where it may be read.</summary>
    void Apply()
    {
        this.documentId = this.documentFilter.Value.Trim();
        this.Reload();
    }

    void Reload() => this.Context.Run(this.Load, "Could not list the blobs");

    public override async Task Load(CancellationToken ct)
    {
        try
        {
            var documentId = this.documentId;
            var blobs = await this.Context.Admin.ListBlobs(
                this.Context.ProfileId, this.Context.Table, this.Context.TypeName,
                documentId.Length == 0 ? null : documentId, ct);

            // Built inside the post: a [Bindable] property refuses a write from anywhere but the
            // render thread, so a row constructed out here would throw.
            this.Context.Post(() =>
            {
                this.error.Value = "";
                this.grid.Replace(blobs.Select(blob => new BlobRow
                {
                    DocumentId = blob.DocumentId,
                    Key = blob.BlobKey,
                    FileName = blob.FileName ?? "—",
                    ContentType = blob.ContentType ?? "—",
                    Size = Ui.Bytes(blob.Length),
                    Updated = Ui.Timestamp(blob.UpdatedAt ?? blob.CreatedAt),
                    Info = blob
                }));

                this.summary.Value = $"[dim]{blobs.Count} blob(s) · {Ui.Escape(Ui.Bytes(blobs.Sum(b => b.Length)))} in total[/]";
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

    void WithSelection(Action<BlobRow> action)
    {
        if (this.grid.Selected is { } row)
            action(row);
        else
            this.Shell.Info("Select a blob first.");
    }

    void Preview(BlobRow row)
    {
        if (row.Info.Kind != BlobKind.Text)
        {
            this.Shell.Info($"'{row.Key}' is {row.ContentType} - save it to a file and open it with something that understands it.");
            return;
        }

        this.Context.Run(async ct =>
        {
            var payload = await this.Context.Admin.ReadBlob(
                this.Context.ProfileId, this.Context.Table, this.Context.TypeName, row.DocumentId, row.Key, ct);

            this.Context.Post(() =>
            {
                if (payload is null)
                {
                    this.Shell.Warn("That blob is gone.");
                    return;
                }

                // Decoded strictly enough that a mislabelled binary shows as replacement characters
                // rather than as terminal escape sequences it would be unwise to print.
                var text = new UTF8Encoding(false).GetString(payload.Data);
                var clean = new string([.. text.Select(c => char.IsControl(c) && c is not ('\n' or '\r' or '\t') ? '�' : c)]);

                Modal.Show(this.Shell, $"{row.DocumentId} · {row.Key}", clean);
            });
        }, "Could not read the blob");
    }

    void Save(BlobRow row)
    {
        var suggested = Path.Combine(
            Environment.CurrentDirectory,
            row.Info.FileName is { Length: > 0 } name ? name : $"{row.DocumentId}-{row.Key}.bin");

        Modal.Prompt(this.Shell, "Save blob", "Write to", suggested, path => this.Context.Run(async ct =>
        {
            var payload = await this.Context.Admin.ReadBlob(
                this.Context.ProfileId, this.Context.Table, this.Context.TypeName, row.DocumentId, row.Key, ct);

            if (payload is null)
            {
                this.Context.Post(() => this.Shell.Warn("That blob is gone."));
                return;
            }

            await File.WriteAllBytesAsync(path, payload.Data, ct);
            this.Context.Post(() => this.Shell.Success($"Wrote {Ui.Bytes(payload.Data.LongLength)} to {path}."));
        }, "Could not save the blob"));
    }

    void Delete(BlobRow row)
    {
        if (!this.Context.CanWrite)
        {
            this.Shell.Warn("This connection is read-only.");
            return;
        }

        Modal.Confirm(
            this.Shell,
            "Delete blob",
            $"Delete '{row.Key}' from '{row.DocumentId}'? The document keeps its metadata reference, which will then point at nothing.",
            "Delete",
            () => this.Context.Run(async ct =>
            {
                var deleted = await this.Context.Admin.DeleteBlob(
                    this.Context.ProfileId, this.Context.Table, this.Context.TypeName, row.DocumentId, row.Key, ct);

                await this.Load(ct);
                this.Context.Post(() =>
                {
                    if (deleted)
                        this.Shell.Success($"Deleted '{row.Key}'.");
                    else
                        this.Shell.Info("That blob was already gone.");
                });
            }, "Could not delete the blob")
        );
    }
}
