namespace Shiny.DocumentDb;

/// <summary>
/// <see cref="IOutboxAdmin"/> over the ordinary query surface — outbox messages are ordinary documents, so
/// there is no provider code here and nothing to keep in step with a backend.
/// </summary>
sealed class OutboxAdmin(IDocumentStore store, OutboxOptions options, TimeProvider clock) : IOutboxAdmin
{
    IDocumentQuery<OutboxMessage> Query() => store.Query(options.MessageTypeInfo);

    IDocumentQuery<OutboxMessage> Pending()
    {
        var now = clock.GetUtcNow();
        return this.Query().Where(m => m.ProcessedAt == null && m.DeadLetteredAt == null && m.AvailableAt <= now);
    }

    IDocumentQuery<OutboxMessage> DeadLettered() => this.Query().Where(m => m.DeadLetteredAt != null);

    public async Task<int> PendingCount(CancellationToken cancellationToken = default)
        => (int)await this.Pending().Count(cancellationToken).ConfigureAwait(false);

    public async Task<int> ScheduledCount(CancellationToken cancellationToken = default)
    {
        var now = clock.GetUtcNow();
        var count = await this.Query()
            .Where(m => m.ProcessedAt == null && m.DeadLetteredAt == null && m.AvailableAt > now)
            .Count(cancellationToken)
            .ConfigureAwait(false);
        return (int)count;
    }

    public async Task<int> DeadLetterCount(CancellationToken cancellationToken = default)
        => (int)await this.DeadLettered().Count(cancellationToken).ConfigureAwait(false);

    public async Task<DateTimeOffset?> OldestPendingAt(CancellationToken cancellationToken = default)
    {
        var oldest = await this.Pending()
            .OrderBy(m => m.CreatedAt)
            .Paginate(0, 1)
            .FirstOrDefault(cancellationToken)
            .ConfigureAwait(false);
        return oldest?.CreatedAt;
    }

    public Task<IReadOnlyList<OutboxMessage>> DeadLetters(int take = 100, CancellationToken cancellationToken = default)
        => this.DeadLettered()
            .OrderByDescending(m => m.DeadLetteredAt!)
            .Paginate(0, Math.Max(1, take))
            .ToList(cancellationToken);

    public Task<int> Requeue(IEnumerable<string> ids, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(ids);
        var list = ids as IReadOnlyList<string> ?? [.. ids];
        if (list.Count == 0)
            return Task.FromResult(0);

        // The dead-letter guard stays in the predicate even though the caller named ids explicitly: requeueing
        // a merely *scheduled* message would reset its backoff, and requeueing a *processed* one would
        // redeliver a business event that already happened. Neither is the UI's job to prevent.
        return this.RequeueCore(this.DeadLettered().Where(m => list.Contains(m.Id)), cancellationToken);
    }

    public Task<int> RequeueAllDeadLettered(string? messageType = null, CancellationToken cancellationToken = default)
    {
        var query = this.DeadLettered();
        if (!string.IsNullOrWhiteSpace(messageType))
            query = query.Where(m => m.MessageType == messageType);
        return this.RequeueCore(query, cancellationToken);
    }

    Task<int> RequeueCore(IDocumentQuery<OutboxMessage> query, CancellationToken cancellationToken)
    {
        var now = clock.GetUtcNow();
        return query.ExecuteUpdate(
            b => b
                .Set(m => m.DeadLetteredAt, null)
                .Set(m => m.Error, null)
                .Set(m => m.Attempts, 0)
                .Set(m => m.AvailableAt, now),
            cancellationToken);
    }

    public Task<int> PurgeProcessed(DateTimeOffset olderThan, CancellationToken cancellationToken = default)
        // Structurally incapable of touching a pending or dead-lettered row: only an acknowledged message has
        // a ProcessedAt at all.
        => this.Query()
            .Where(m => m.ProcessedAt != null && m.ProcessedAt < olderThan)
            .ExecuteDelete(cancellationToken);
}
