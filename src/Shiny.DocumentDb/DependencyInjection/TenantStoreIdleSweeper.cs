using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace Shiny.DocumentDb;

/// <summary>
/// Evicts tenant stores that have gone idle. Registered only when
/// <see cref="TenantStoreOptions.IdleTimeout"/> is set, so an opted-out app has no background timer at all.
/// </summary>
sealed class TenantStoreIdleSweeper : BackgroundService
{
    readonly TenantDocumentStoreCache cache;
    readonly TimeProvider clock;

    public TenantStoreIdleSweeper(TenantDocumentStoreCache cache, IServiceProvider services)
    {
        this.cache = cache;
        this.clock = services.GetService<TimeProvider>() ?? TimeProvider.System;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        using var timer = new PeriodicTimer(this.cache.Options.SweepInterval, this.clock);
        try
        {
            while (await timer.WaitForNextTickAsync(stoppingToken).ConfigureAwait(false))
                this.cache.SweepIdle();
        }
        catch (OperationCanceledException)
        {
            // Host shutdown.
        }
    }
}
