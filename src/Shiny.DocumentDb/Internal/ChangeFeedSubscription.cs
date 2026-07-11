namespace Shiny.DocumentDb.Internal;

/// <summary>
/// A reusable <see cref="IAsyncDisposable"/> for native change-feed subscriptions. It runs a worker
/// loop on a token linked to the caller's, and on disposal cancels the loop and awaits its
/// completion. The worker delegate is responsible for opening and disposing its own resources
/// (connections, iterators) — typically in a <c>finally</c> block.
/// </summary>
public sealed class ChangeFeedSubscription : IAsyncDisposable
{
    readonly CancellationTokenSource cts;
    readonly Task worker;

    public ChangeFeedSubscription(CancellationToken external, Func<CancellationToken, Task> run)
    {
        this.cts = CancellationTokenSource.CreateLinkedTokenSource(external);
        this.worker = Task.Run(() => run(this.cts.Token));
    }

    public async ValueTask DisposeAsync()
    {
        if (!this.cts.IsCancellationRequested)
            await this.cts.CancelAsync().ConfigureAwait(false);
        try
        {
            await this.worker.ConfigureAwait(false);
        }
        catch
        {
            // The worker was cancelled (expected) or faulted (e.g. a change handler threw) — either way we're
            // disposing, so observe the exception rather than letting it escape DisposeAsync. Disposing the CTS
            // must still happen, so it lives in the finally (a faulted worker used to leak the linked CTS).
        }
        finally
        {
            this.cts.Dispose();
        }
    }
}
