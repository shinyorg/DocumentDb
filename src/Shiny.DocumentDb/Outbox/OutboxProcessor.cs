using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Shiny.DocumentDb.Diagnostics;

namespace Shiny.DocumentDb;

/// <summary>
/// Drains the outbox for the life of the host. Registered by <c>AddDocumentOutbox</c>; the work itself lives
/// in <see cref="OutboxRunner"/>, which you can drive directly where there is no generic host.
/// </summary>
sealed class OutboxProcessor(IServiceProvider services, OutboxOptions options) : BackgroundService
{
    readonly TimeProvider clock = services.GetService<TimeProvider>() ?? TimeProvider.System;

    // Resolved rather than injected: logging is genuinely optional here (a MAUI or test host often registers
    // none), and a required ILogger<T> constructor parameter would make the whole registration unresolvable.
    readonly ILogger logger = (ILogger?)services.GetService<ILoggerFactory>()?.CreateLogger<OutboxProcessor>()
        ?? Microsoft.Extensions.Logging.Abstractions.NullLogger.Instance;

    /// <summary>
    /// Fails the host start when the store cannot promise atomic multi-document writes. Checked here rather
    /// than at registration because that is the first moment the store exists — and checked at all because the
    /// alternative is an "outbox" that is a dual write with extra steps.
    /// </summary>
    public override async Task StartAsync(CancellationToken cancellationToken)
    {
        await using (var scope = services.CreateAsyncScope())
        {
            var store = scope.ServiceProvider.GetRequiredService<IDocumentStore>();
            if (!store.SupportsTransactions)
                throw new NotSupportedException(
                    $"The transactional outbox requires a store whose unit of work is a real transaction, and '{store.GetType().Name}' does not provide one — its writes would be a dual write, which is the exact failure an outbox exists to prevent. " +
                    "Use a relational provider (SQLite, SQL Server, PostgreSQL, MySQL, MariaDB, CockroachDB, Oracle, DuckDB) or LiteDB, or drive events from IChangeFeedDocumentStore instead.");
        }
        await base.StartAsync(cancellationToken).ConfigureAwait(false);
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var lastSweep = this.clock.GetUtcNow();
        var stopping = false;

        while (!stoppingToken.IsCancellationRequested && !stopping)
        {
            var dispatched = 0;
            try
            {
                // The store is resolved per pass so a scoped registration (tenant-per-database) resolves
                // normally rather than being captured once from the root.
                await using var scope = services.CreateAsyncScope();
                var store = scope.ServiceProvider.GetRequiredService<IDocumentStore>();
                var runner = new OutboxRunner(
                    store,
                    scope.ServiceProvider.GetRequiredService<IServiceScopeFactory>(),
                    options,
                    this.clock,
                    this.logger);

                dispatched = await runner.DrainOnce(stoppingToken).ConfigureAwait(false);
                DocumentStoreMetrics.RecordOutboxPending(await runner.PendingDepth(stoppingToken).ConfigureAwait(false));

                if (options.Retention != null && this.clock.GetUtcNow() - lastSweep >= options.RetentionInterval)
                {
                    lastSweep = this.clock.GetUtcNow();
                    var purged = await runner.PurgeExpired(stoppingToken).ConfigureAwait(false);
                    if (purged > 0)
                        this.logger.LogDebug("Outbox retention sweep removed {Count} processed message(s)", purged);
                }
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                stopping = true;
            }
            catch (Exception ex)
            {
                // A poll that throws — the database went away, a claim raced badly — must not kill the loop.
                this.logger.LogError(ex, "Outbox poll failed; retrying after {PollInterval}", options.PollInterval);
            }

            // A full batch means there is probably more waiting, so skip the wait and go straight round again.
            if (!stopping && dispatched < options.BatchSize)
            {
                try
                {
                    await Task.Delay(options.PollInterval, this.clock, stoppingToken).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    stopping = true;
                }
            }
        }
    }
}
