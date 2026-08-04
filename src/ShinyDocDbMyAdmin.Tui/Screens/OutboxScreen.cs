using ShinyDocDbMyAdmin.Models;
using ShinyDocDbMyAdmin.Tui.Shell;
using ShinyDocDbMyAdmin.Tui.Widgets;
using XenoAtom.Terminal;
using XenoAtom.Terminal.UI;
using XenoAtom.Terminal.UI.Commands;
using XenoAtom.Terminal.UI.Controls;
using XenoAtom.Terminal.UI.Geometry;
using XenoAtom.Terminal.UI.Input;

namespace ShinyDocDbMyAdmin.Tui.Screens;

/// <summary>One outbox message, flattened for the grid.</summary>
public sealed partial class OutboxRow
{
    [Bindable] public partial string Id { get; set; }
    [Bindable] public partial string MessageType { get; set; }
    [Bindable] public partial string Partition { get; set; }
    [Bindable] public partial string Created { get; set; }
    [Bindable] public partial string Attempts { get; set; }
    [Bindable] public partial string State { get; set; }
    [Bindable] public partial string Error { get; set; }

    public required OutboxEntry Entry { get; init; }
}

/// <summary>
/// The outbox queue, in the terminal. Answers the same four questions the web screen does — how deep is
/// the queue, is it draining, what is dead-lettered, and why — and offers the same single action.
/// </summary>
/// <remarks>
/// <b>This tool cannot dispatch.</b> Delivery is <c>IOutboxDispatcher</c>, which lives in the application
/// and talks to a transport this process cannot reach. Requeue only makes a message eligible again.
/// </remarks>
public sealed class OutboxScreen(AdminShell shell, string profileId) : Screen(shell)
{
    readonly RowGrid<OutboxRow> grid = new(
        (OutboxRow.Accessor.Id, "Id", ColumnWidth.Fit),
        (OutboxRow.Accessor.MessageType, "Message type", ColumnWidth.Fill),
        (OutboxRow.Accessor.Partition, "Partition", ColumnWidth.Fit),
        (OutboxRow.Accessor.Created, "Created", ColumnWidth.Fit),
        (OutboxRow.Accessor.Attempts, "Attempts", ColumnWidth.Fit),
        (OutboxRow.Accessor.State, "State", ColumnWidth.Fit),
        (OutboxRow.Accessor.Error, "Error", ColumnWidth.Fill)
    );

    readonly State<string> health = new("");
    readonly State<string> stalled = new("");
    readonly State<string> failures = new("");
    readonly State<string> error = new("");
    readonly State<string> filter = new("All");

    // In a terminal the palette is the address bar, so every state chip is also a named command.
    static readonly (string Label, OutboxState? State)[] Filters =
    [
        ("All", null),
        ("Pending", OutboxState.Pending),
        ("Scheduled", OutboxState.Scheduled),
        ("Dead-lettered", OutboxState.DeadLettered),
        ("Processed", OutboxState.Processed)
    ];

    readonly State<bool> writable = new(false);

    OutboxLocation? outbox;
    OutboxHealth? counts;
    OutboxState? state;
    bool readOnly;

    public string ProfileId { get; } = profileId;

    public override string Title => "Outbox";

    protected override Visual Build()
    {
        this.grid
            .OnActivate(this.ShowPayload)
            .OnKey(new KeyGesture('r', TerminalModifiers.None), this.Requeue)
            .OnKey(new KeyGesture('p', TerminalModifiers.None), _ => this.Purge());

        var chips = new WrapHStack().Spacing(1);
        foreach (var (label, value) in Filters)
            chips.Add(Ui.Action(label, () => this.SetFilter(value)));

        var toolbar = Ui.Toolbar(
            Ui.Primary("Payload", () => this.WithSelection(this.ShowPayload)),
            // Read-only profiles omit the write actions entirely rather than failing on press. Bound to
            // a state rather than to the bool: Build runs before the first Load, so a value read here
            // would always be the default.
            Ui.Danger("Requeue", () => this.WithSelection(this.Requeue)).IsVisible(this.writable.Bind.Value),
            Ui.Danger("Requeue all dead", this.RequeueAll).IsVisible(this.writable.Bind.Value),
            Ui.Danger("Purge processed", this.Purge).IsVisible(this.writable.Bind.Value),
            Ui.Action("Refresh", () => this.Shell.Reload())
        );

        return new Padder(new VStack(
            new Markup("[bold]Outbox[/]  [dim]domain events waiting to be delivered[/]"),
            new Markup(() => this.health.Value),
            new ComputedVisual(() => this.stalled.Value.Length == 0 ? Ui.Nothing() : Ui.Warning(this.stalled.Value)),
            new ComputedVisual(() => this.failures.Value.Length == 0 ? Ui.Nothing() : new Markup(this.failures.Value).Wrap(true)),
            new Markup(() => $"[dim]Filter:[/] {Ui.Escape(this.filter.Value)}"),
            chips,
            toolbar,
            // Explains the buttons above it, so it goes when they do.
            Ui.Muted("This tool cannot deliver messages. Requeue makes them eligible again; your application's outbox processor does the delivery.")
                .IsVisible(this.writable.Bind.Value),
            new ComputedVisual(() => this.error.Value.Length == 0 ? Ui.Nothing() : Ui.Failure(this.error.Value)),
            this.grid.OrEmpty("No message matches this filter.").Stretch()
        ).Spacing(1)).Padding(new Thickness(1));
    }

    void WithSelection(Action<OutboxRow> action)
    {
        if (this.grid.Selected is { } row)
            action(row);
        else
            this.Shell.Info("Select a message first.");
    }

    void SetFilter(OutboxState? next)
    {
        this.state = next;
        this.Shell.Reload();
    }

    void ShowPayload(OutboxRow row)
    {
        var entry = row.Entry;
        var payload = entry.Payload?.ToJsonString() ?? "(no payload)";
        var trace = entry.TraceParent is { } traceParent ? $"\n\ntraceparent: {traceParent}" : "";
        var failure = entry.Error is { Length: > 0 } text ? $"\n\nerror: {text}" : "";
        JsonDocumentDialog.Show(this.Shell, $"{entry.MessageType} · {entry.Id}", payload + trace + failure);
    }

    public override async Task Load(CancellationToken ct)
    {
        try
        {
            var profile = await this.Shell.Profiles.Get(this.ProfileId, ct);
            var outboxes = await this.Shell.Admin.FindOutboxes(this.ProfileId, ct);
            var located = outboxes.FirstOrDefault();

            if (located is null)
            {
                this.Shell.Post(() =>
                {
                    this.SetWritable(profile);
                    this.outbox = null;
                    this.health.Value = "[dim]No outbox in this database.[/]";
                    this.stalled.Value = "";
                    this.failures.Value = "";
                    this.error.Value = "";
                    this.grid.Replace([]);
                });
                return;
            }

            if (!located.Addressable)
            {
                this.Shell.Post(() =>
                {
                    this.SetWritable(profile);
                    this.outbox = located;
                    this.grid.Replace([]);
                    this.error.Value =
                        $"This outbox stores its type name as '{located.TypeName}' (TypeNameResolution.FullName). " +
                        "A dotted name cannot be addressed through the collection lane — use the query console.";
                });
                return;
            }

            var summary = await this.Shell.Admin.GetOutboxHealth(this.ProfileId, located, ct);
            var rows = await this.Shell.Admin.ListOutbox(this.ProfileId, located, new OutboxFilter(this.state, Take: 200), ct);
            var grouped = summary.DeadLettered > 0
                ? await this.Shell.Admin.GroupOutboxFailures(this.ProfileId, located, ct)
                : null;

            var age = summary.OldestPendingAge(DateTimeOffset.UtcNow);

            // Built inside the post: a [Bindable] property refuses a write from anywhere but the render
            // thread, so a row constructed out here would throw.
            this.Shell.Post(() =>
            {
                this.SetWritable(profile);
                this.outbox = located;
                this.counts = summary;
                this.error.Value = "";

                this.health.Value =
                    $"[dim]{Ui.Escape(located.Table)} ·[/] pending [bold]{Ui.Number(summary.Pending)}[/] · " +
                    $"scheduled [bold]{Ui.Number(summary.Scheduled)}[/] · " +
                    $"dead-lettered [{(summary.DeadLettered > 0 ? "red" : "dim")}]{Ui.Number(summary.DeadLettered)}[/] · " +
                    $"processed [bold]{Ui.Number(summary.Processed)}[/] · " +
                    $"oldest pending [bold]{Ui.Escape(age is { } value ? Age(value) : "—")}[/]";

                // The one sentence this screen exists for. Depth alone cannot separate "busy" from
                // "dead"; the age of the oldest pending message can.
                this.stalled.Value = age > TimeSpan.FromMinutes(1)
                    ? "Nothing has drained recently — is the outbox processor running?"
                    : "";

                this.failures.Value = grouped is { Groups.Count: > 0 }
                    ? "[dim]Failures:[/] " + string.Join("  ", grouped.Groups.Take(3)
                        .Select(g => $"[bold]{Ui.Number(g.Count)}[/]× {Ui.Escape(g.MessageType)} [dim]{Ui.Escape(g.ErrorSummary)}[/]"))
                    : "";

                this.filter.Value = Filters.First(f => f.State == this.state).Label;

                this.grid.Replace(rows.Select(entry => new OutboxRow
                {
                    Id = entry.Id,
                    MessageType = entry.MessageType,
                    Partition = entry.PartitionKey ?? "",
                    Created = entry.CreatedAt?.ToLocalTime().ToString("yyyy-MM-dd HH:mm") ?? "",
                    Attempts = Ui.Number(entry.Attempts),
                    State = entry.State.ToString(),
                    Error = entry.ErrorSummary ?? "",
                    Entry = entry
                }));

                this.Shell.Status.Value = $"{Ui.Number(summary.Total)} outbox message(s)";
            });
        }
        catch (Exception ex)
        {
            this.Shell.Post(() =>
            {
                this.error.Value = ex.Message;
                this.grid.Replace([]);
            });
        }
    }

    void Requeue(OutboxRow row)
    {
        if (this.readOnly || this.outbox is not { } located)
            return;

        if (row.Entry.State != OutboxState.DeadLettered)
        {
            this.Shell.Info("Only a dead-lettered message can be requeued. Requeueing a scheduled one would reset its backoff, and a processed one would redeliver an event that already happened.");
            return;
        }

        // Requeueing re-inserts behind messages already delivered for the key, so ordering for that
        // partition is already broken by the dead-letter and will not be restored. Say it; do not
        // silently reorder.
        var ordering = row.Entry.PartitionKey is { } key
            ? $" This message is in partition '{key}': ordering for that partition was already broken by the dead-letter and requeueing does not restore it."
            : "";

        Modal.Confirm(
            this.Shell,
            "Requeue message",
            $"Make {row.Entry.MessageType} eligible for delivery again? Your application's processor delivers it — this tool cannot.{ordering}",
            "Requeue it",
            () => this.Shell.RunAsync(async ct =>
            {
                var affected = await this.Shell.Admin.RequeueOutbox(this.ProfileId, located, [row.Entry.Id], ct);
                await this.Load(ct);
                this.Shell.Post(() => this.Shell.Success($"Requeued {affected} message(s)."));
            }, "Could not requeue the message"));
    }

    void RequeueAll()
    {
        if (this.readOnly || this.outbox is not { } located || this.counts is not { DeadLettered: > 0 } summary)
        {
            this.Shell.Info("Nothing is dead-lettered.");
            return;
        }

        Modal.Confirm(
            this.Shell,
            "Requeue all dead letters",
            $"Make all {summary.DeadLettered} dead-lettered message(s) eligible again? Consumers must be idempotent — delivery is at-least-once by design.",
            "Requeue them all",
            () => this.Shell.RunAsync(async ct =>
            {
                var affected = await this.Shell.Admin.RequeueAllDeadLettered(this.ProfileId, located, null, ct);
                await this.Load(ct);
                this.Shell.Post(() => this.Shell.Success($"Requeued {affected} message(s)."));
            }, "Could not requeue the dead letters"));
    }

    void Purge()
    {
        if (this.readOnly || this.outbox is not { } located || this.counts is not { Processed: > 0 } summary)
        {
            this.Shell.Info("There is nothing processed to purge.");
            return;
        }

        Modal.Confirm(
            this.Shell,
            "Purge processed messages",
            $"Delete processed messages older than 7 days? Up to {summary.Processed} row(s) are candidates and they are gone for good. Pending and dead-lettered messages are never touched.",
            "Purge them",
            () => this.Shell.RunAsync(async ct =>
            {
                var affected = await this.Shell.Admin.PurgeProcessedOutbox(
                    this.ProfileId, located, DateTimeOffset.UtcNow.AddDays(-7), ct);
                await this.Load(ct);
                this.Shell.Post(() => this.Shell.Success($"Purged {affected} message(s)."));
            }, "Could not purge the processed messages"));
    }

    void SetWritable(ConnectionProfile? profile)
    {
        this.readOnly = profile?.ReadOnly ?? false;
        this.writable.Value = !this.readOnly;
    }

    static string Age(TimeSpan age) => age switch
    {
        { TotalSeconds: < 60 } => $"{(int)age.TotalSeconds}s",
        { TotalMinutes: < 60 } => $"{(int)age.TotalMinutes}m",
        { TotalHours: < 24 } => $"{(int)age.TotalHours}h",
        _ => $"{(int)age.TotalDays}d"
    };

    public override IEnumerable<Command> Commands()
    {
        foreach (var (label, value) in Filters)
        {
            var captured = value;
            yield return new Command
            {
                Id = $"outbox.filter.{label.ToLowerInvariant()}",
                Name = $"Outbox: {label}",
                LabelMarkup = $"Outbox: {label}",
                Execute = _ => this.SetFilter(captured)
            };
        }

        // Write commands are omitted entirely on a read-only profile rather than failing on invoke —
        // the palette is how a terminal user discovers what the tool can do, so listing something
        // unusable there is a worse lie than a hidden button.
        if (this.readOnly)
            yield break;

        yield return new Command
        {
            Id = "outbox.requeue",
            Name = "Outbox: requeue selected",
            LabelMarkup = "Outbox: requeue selected",
            Execute = _ => this.WithSelection(this.Requeue)
        };
        yield return new Command
        {
            Id = "outbox.requeueall",
            Name = "Outbox: requeue all dead letters",
            LabelMarkup = "Outbox: requeue all dead letters",
            Execute = _ => this.RequeueAll()
        };
        yield return new Command
        {
            Id = "outbox.purge",
            Name = "Outbox: purge processed",
            LabelMarkup = "Outbox: purge processed",
            Execute = _ => this.Purge()
        };
    }
}
