using ShinyDocDbMyAdmin.Models;
using ShinyDocDbMyAdmin.Services;
using ShinyDocDbMyAdmin.Tui.Shell;
using ShinyDocDbMyAdmin.Tui.Widgets;
using XenoAtom.Terminal.UI;
using XenoAtom.Terminal.UI.Controls;
using XenoAtom.Terminal.UI.Geometry;

namespace ShinyDocDbMyAdmin.Tui.Screens;

/// <summary>One stream queue, flattened for the grid.</summary>
public sealed partial class StreamQueueGridRow
{
    [Bindable] public partial string Queue { get; set; }
    [Bindable] public partial string Undelivered { get; set; }
    [Bindable] public partial string Retained { get; set; }
    [Bindable] public partial string Oldest { get; set; }

    public required StreamQueueRow Row { get; init; }
}

/// <summary>One stuck stream, flattened for the grid.</summary>
public sealed partial class StuckStreamGridRow
{
    [Bindable] public partial string Stream { get; set; }
    [Bindable] public partial string Namespace { get; set; }
    [Bindable] public partial string Queue { get; set; }
    [Bindable] public partial string Undelivered { get; set; }
    [Bindable] public partial string Oldest { get; set; }
}

/// <summary>
/// Orleans stream queues, in the terminal. Answers the same questions the web screen does: how deep is each
/// queue, is it draining, and which streams are stuck.
/// </summary>
/// <remarks>
/// Read-only by design, not by omission. A stream event is a position in a gap-free sequence that every
/// subscriber holds a cursor into — deleting one tears a hole in that sequence, re-dating one reorders
/// delivery. There is no safe operator action on a stream event, so this screen offers none.
/// </remarks>
public sealed class StreamsScreen(AdminShell shell, string profileId) : Screen(shell)
{
    readonly RowGrid<StreamQueueGridRow> queues = new(
        (StreamQueueGridRow.Accessor.Queue, "Queue", ColumnWidth.Fill),
        (StreamQueueGridRow.Accessor.Undelivered, "Undelivered", ColumnWidth.Fit),
        (StreamQueueGridRow.Accessor.Retained, "Retained", ColumnWidth.Fit),
        (StreamQueueGridRow.Accessor.Oldest, "Oldest", ColumnWidth.Fit)
    );

    readonly RowGrid<StuckStreamGridRow> stuck = new(
        (StuckStreamGridRow.Accessor.Stream, "Stream", ColumnWidth.Fill),
        (StuckStreamGridRow.Accessor.Namespace, "Namespace", ColumnWidth.Fit),
        (StuckStreamGridRow.Accessor.Queue, "Queue", ColumnWidth.Fit),
        (StuckStreamGridRow.Accessor.Undelivered, "Undelivered", ColumnWidth.Fit),
        (StuckStreamGridRow.Accessor.Oldest, "Oldest", ColumnWidth.Fit)
    );

    readonly State<string> health = new("");
    readonly State<string> stalled = new("");
    readonly State<string> truncated = new("");
    readonly State<string> error = new("");
    readonly State<string> window = new("5 minutes");

    static readonly (string Label, int Minutes)[] Windows =
    [
        ("1 minute", 1),
        ("5 minutes", 5),
        ("1 hour", 60),
        ("1 day", 1440)
    ];

    int stuckMinutes = 5;

    public string ProfileId { get; } = profileId;

    public override string Title => "Streams";

    protected override Visual Build()
    {
        var chips = new WrapHStack().Spacing(1);
        foreach (var (label, minutes) in Windows)
            chips.Add(Ui.Action(label, () => this.SetWindow(minutes)));

        return new Padder(new VStack(
            new Markup("[bold]Streams[/]  [dim]Orleans stream queues, and what is not draining[/]"),
            new Markup(() => this.health.Value),
            new ComputedVisual(() => this.stalled.Value.Length == 0 ? Ui.Nothing() : Ui.Warning(this.stalled.Value)),
            new ComputedVisual(() => this.error.Value.Length == 0 ? Ui.Nothing() : Ui.Failure(this.error.Value)),
            Ui.Toolbar(Ui.Action("Refresh", () => this.Shell.Reload())),
            new Markup("[dim]Queues[/]"),
            this.queues.OrEmpty("No queues found.").Stretch(),
            new Markup(() => $"[dim]Stuck streams — undelivered for more than[/] {Ui.Escape(this.window.Value)}"),
            chips,
            new ComputedVisual(() => this.truncated.Value.Length == 0 ? Ui.Nothing() : Ui.Muted(this.truncated.Value)),
            this.stuck.OrEmpty("Nothing has been waiting that long.").Stretch()
        ).Spacing(1)).Padding(new Thickness(1));
    }

    void SetWindow(int minutes)
    {
        this.stuckMinutes = minutes;
        this.Shell.Reload();
    }

    public override async Task Load(CancellationToken ct)
    {
        try
        {
            var providers = await this.Shell.Admin.FindStreamProviders(this.ProfileId, ct);
            var located = providers.FirstOrDefault();

            if (located is null)
            {
                this.Shell.Post(() =>
                {
                    this.health.Value = "[dim]No Orleans streams in this database.[/]";
                    this.stalled.Value = "";
                    this.truncated.Value = "";
                    this.error.Value = "";
                    this.queues.Replace([]);
                    this.stuck.Replace([]);
                });
                return;
            }

            if (!located.Addressable)
            {
                this.Shell.Post(() =>
                {
                    this.queues.Replace([]);
                    this.stuck.Replace([]);
                    this.error.Value =
                        $"This provider stores its type name as '{located.TypeName}' (TypeNameResolution.FullName). " +
                        "A dotted name cannot be addressed through the collection lane — use the query console.";
                });
                return;
            }

            var summary = await this.Shell.Admin.GetStreamHealth(this.ProfileId, located, ct);
            var queueRows = await this.Shell.Admin.ListStreamQueues(this.ProfileId, located, ct);
            var found = await this.Shell.Admin.FindStuckStreams(
                this.ProfileId, located, TimeSpan.FromMinutes(this.stuckMinutes), ct: ct);

            var now = DateTimeOffset.UtcNow;
            var oldest = summary.OldestUndeliveredAt is { } at ? now - at : (TimeSpan?)null;

            // Built inside the post: a [Bindable] property refuses a write from anywhere but the render
            // thread, so a row constructed out here would throw.
            this.Shell.Post(() =>
            {
                this.error.Value = "";
                this.window.Value = Windows.First(w => w.Minutes == this.stuckMinutes).Label;

                this.health.Value =
                    $"[dim]{Ui.Escape(located.Table)} ·[/] undelivered [{(summary.Depth > 0 ? "yellow" : "dim")}]{Ui.Number(summary.Depth)}[/] · " +
                    $"queues [bold]{Ui.Number(summary.QueueCount)}[/] · " +
                    $"retained [bold]{Ui.Number(summary.Retained)}[/] · " +
                    $"oldest undelivered [bold]{Ui.Escape(oldest is { } value ? Age(value) : "—")}[/]";

                // The one sentence this screen exists for: depth alone cannot separate "busy" from "dead",
                // the age of the oldest undelivered event can.
                this.stalled.Value = oldest > TimeSpan.FromMinutes(1)
                    ? "Nothing has drained recently — are the silos running, and is the stream provider registered on them?"
                    : "";

                // Never let a bounded scan read as a total.
                this.truncated.Value = found.Truncated
                    ? $"Only the oldest {Ui.Number(DocumentAdminService.StreamStuckScanLimit)} undelivered events were scanned; this list is partial and the counts are lower bounds."
                    : "";

                this.queues.Replace(queueRows.Select(q => new StreamQueueGridRow
                {
                    Queue = q.QueueId,
                    Undelivered = Ui.Number(q.Depth),
                    Retained = Ui.Number(q.Retained),
                    Oldest = q.OldestUndeliveredAt is { } enqueued ? Age(now - enqueued) : "—",
                    Row = q
                }));

                this.stuck.Replace(found.Streams.Select(s => new StuckStreamGridRow
                {
                    Stream = s.StreamId,
                    Namespace = s.StreamNamespace ?? "",
                    Queue = s.QueueId,
                    Undelivered = Ui.Number(s.UndeliveredCount),
                    Oldest = Age(now - s.OldestUndeliveredAt)
                }));

                this.Shell.Status.Value = $"{Ui.Number(summary.Depth)} undelivered stream event(s)";
            });
        }
        catch (Exception ex)
        {
            this.Shell.Post(() =>
            {
                this.error.Value = ex.Message;
                this.queues.Replace([]);
                this.stuck.Replace([]);
            });
        }
    }

    static string Age(TimeSpan age) => age switch
    {
        { TotalSeconds: < 60 } => $"{(int)age.TotalSeconds}s",
        { TotalMinutes: < 60 } => $"{(int)age.TotalMinutes}m",
        { TotalHours: < 24 } => $"{(int)age.TotalHours}h",
        _ => $"{(int)age.TotalDays}d"
    };
}
