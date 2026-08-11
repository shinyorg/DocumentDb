namespace Shiny.DocumentDb.Orleans;

/// <summary>
/// <see cref="IStreamAdmin"/> over the stream provider's own store. Registered by
/// <c>AddDocumentDbStreams</c> as a keyed singleton under the provider name.
/// </summary>
public sealed class StreamAdmin : IStreamAdmin
{
    readonly IDocumentStore store;
    readonly string prefix;

    public StreamAdmin(IDocumentStore store, string providerName, string serviceId)
    {
        ArgumentNullException.ThrowIfNull(store);
        this.store = store;
        this.prefix = $"{serviceId}|{providerName}|";
    }

    public async Task<IReadOnlyList<StreamQueueStatus>> Queues(CancellationToken cancellationToken = default)
    {
        // Counter rows are the queue list: they are created at startup for every queue, so a queue with no
        // traffic still shows up (as depth zero) rather than silently vanishing from the view.
        var counters = await this.store.Query<StreamSequenceDocument>()
            .ToList(cancellationToken)
            .ConfigureAwait(false);

        var checkpoints = (await this.store.Query<StreamCheckpointDocument>()
                .ToList(cancellationToken)
                .ConfigureAwait(false))
            .ToDictionary(c => c.Id, StringComparer.Ordinal);

        var results = new List<StreamQueueStatus>(counters.Count);
        foreach (var counter in counters.Where(c => c.Id.StartsWith(this.prefix, StringComparison.Ordinal)))
        {
            var queueId = counter.Id[this.prefix.Length..];
            var undelivered = this.store.Query<StreamEventDocument>()
                .Where(e => e.QueueId == queueId && e.DeliveredAt == null);

            var depth = await undelivered.Count(cancellationToken).ConfigureAwait(false);
            var oldest = await undelivered
                .OrderBy(e => e.Seq)
                .FirstOrDefault(cancellationToken)
                .ConfigureAwait(false);
            var retained = await this.store.Query<StreamEventDocument>()
                .Where(e => e.QueueId == queueId && e.DeliveredAt != null)
                .Count(cancellationToken)
                .ConfigureAwait(false);

            results.Add(new StreamQueueStatus(
                queueId,
                (int)depth,
                checkpoints.TryGetValue(counter.Id, out var cp) ? cp.Cursor : 0,
                // Next is the value about to be handed out, so the last one issued is one below it.
                counter.Next - 1,
                oldest?.EnqueuedAt,
                (int)retained));
        }

        return [.. results.OrderBy(r => r.QueueId, StringComparer.Ordinal)];
    }

    public async Task<int> TotalDepth(CancellationToken cancellationToken = default)
        => (int)await this.store.Query<StreamEventDocument>()
            .Where(e => e.DeliveredAt == null)
            .Count(cancellationToken)
            .ConfigureAwait(false);

    public async Task<IReadOnlyList<StuckStream>> StuckStreams(TimeSpan? olderThan = null, int take = 50, CancellationToken cancellationToken = default)
    {
        var cutoff = DateTimeOffset.UtcNow - (olderThan ?? TimeSpan.FromMinutes(5));

        // Grouped client-side rather than with GroupBy: the rows are already bounded by the "older than"
        // predicate, and a stuck-stream view that works identically on every supported backend is worth more
        // than pushing an aggregate down. If a deployment is stuck badly enough for this to be slow, `take`
        // is the wrong tool anyway — look at Queues() first.
        var rows = await this.store.Query<StreamEventDocument>()
            .Where(e => e.DeliveredAt == null && e.EnqueuedAt < cutoff)
            .OrderBy(e => e.Seq)
            .Paginate(0, take * 20)
            .ToList(cancellationToken)
            .ConfigureAwait(false);

        return [.. rows
            .GroupBy(e => e.StreamId, StringComparer.Ordinal)
            .Select(g => new StuckStream(
                g.Key,
                g.First().StreamNamespace,
                g.First().QueueId,
                g.Count(),
                g.Min(e => e.EnqueuedAt)))
            .OrderBy(s => s.OldestUndeliveredAt)
            .Take(take)];
    }

    public async Task<IReadOnlyList<StreamEventDocument>> Backlog(string queueId, int take = 100, CancellationToken cancellationToken = default)
        => await this.store.Query<StreamEventDocument>()
            .Where(e => e.QueueId == queueId && e.DeliveredAt == null)
            .OrderBy(e => e.Seq)
            .Paginate(0, take)
            .ToList(cancellationToken)
            .ConfigureAwait(false);
}
