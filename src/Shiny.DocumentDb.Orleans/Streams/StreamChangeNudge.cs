using Microsoft.Extensions.Logging;

namespace Shiny.DocumentDb.Orleans;

/// <summary>
/// One native change-feed subscription per silo, fanned out to every receiver — so an enqueue on any silo
/// cuts the poll backoff short cluster-wide.
/// </summary>
/// <remarks>
/// <para>
/// Shared rather than per-receiver on purpose. A native change feed is not a cheap object: on PostgreSQL it
/// is a dedicated <c>LISTEN</c> connection with server-side trigger provisioning behind it, so one
/// subscription per queue would mean eight long-lived connections per silo to watch a single table.
/// </para>
/// <para>
/// The fan-out is deliberately undiscriminating: every receiver is woken by every enqueue, including the
/// seven-in-eight that turn out to be for another queue. Those wake-ups cost one indexed read that returns
/// nothing, against the alternative of every queue polling on a timer forever — and filtering by queue would
/// mean parsing the payload of every change to route it, which costs more than the read it saves.
/// </para>
/// <para>
/// Best-effort throughout. The poll interval is the correctness floor, so a backend with no change feed
/// (CockroachDB, MySQL, MariaDB, Oracle) behaves identically, just at the backoff's latency.
/// </para>
/// </remarks>
sealed class StreamChangeNudge : IAsyncDisposable
{
    readonly List<Action> listeners = [];
    readonly Lock gate = new();
    IAsyncDisposable? subscription;

    StreamChangeNudge()
    {
    }

    public static async Task<StreamChangeNudge?> Attach(IDocumentStore store, ILogger logger)
    {
        if (store is not IChangeFeedDocumentStore feed)
            return null;

        var nudge = new StreamChangeNudge();
        try
        {
            nudge.subscription = await feed.SubscribeChanges<StreamEventDocument>((_, _) =>
            {
                nudge.Signal();
                return Task.CompletedTask;
            }).ConfigureAwait(false);
            return nudge;
        }
        catch (Exception ex)
        {
            logger.LogInformation(ex, "Native change feed unavailable for streams; receivers will poll instead");
            return null;
        }
    }

    public void Subscribe(Action onChange)
    {
        lock (this.gate)
            this.listeners.Add(onChange);
    }

    public void Unsubscribe(Action onChange)
    {
        lock (this.gate)
            this.listeners.Remove(onChange);
    }

    void Signal()
    {
        Action[] snapshot;
        lock (this.gate)
            snapshot = [.. this.listeners];

        foreach (var listener in snapshot)
        {
            try
            {
                listener();
            }
            catch
            {
                // A nudge is an optimization; losing one costs latency, never correctness.
            }
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (this.subscription != null)
        {
            await this.subscription.DisposeAsync().ConfigureAwait(false);
            this.subscription = null;
        }
        lock (this.gate)
            this.listeners.Clear();
    }
}
