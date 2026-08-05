namespace Shiny.DocumentDb;

/// <summary>
/// Hands a store's lifetime to the tenant store cache instead of the DI scope that resolved it.
/// <para>
/// A tenant store is shared by every request for that tenant, but it is handed out from a <b>scoped</b>
/// <see cref="IDocumentStore"/> registration — and <c>Microsoft.Extensions.DependencyInjection</c> disposes any
/// <see cref="IDisposable"/> a scoped factory returns when the scope ends. Left alone, the first request to
/// finish would close the store every other request is still using. So the cache marks the store as
/// externally owned on creation (the store's own <c>Dispose</c> then no-ops) and calls <see cref="Dispose"/>
/// here when the last lease is gone.
/// </para>
/// </summary>
static class TenantStoreOwnership
{
    /// <summary>Marks the store as owned by the cache, so a DI scope disposing it is a no-op.</summary>
    public static void Take(IDocumentStore store)
    {
        switch (store)
        {
            case DocumentStore relational:
                relational.DisposeSuppressed = true;
                break;

            case DocumentProviderBase provider:
                provider.DisposeSuppressed = true;
                break;

            // Anything else is a store type this library did not build. If it is disposable, the resolving
            // scope will close it — documented on AddMultiTenantDocumentStore.
        }
    }

    /// <summary>Releases ownership and actually disposes the store. Called once, after the last lease.</summary>
    public static void Dispose(IDocumentStore store)
    {
        switch (store)
        {
            case DocumentStore relational:
                relational.DisposeSuppressed = false;
                break;

            case DocumentProviderBase provider:
                provider.DisposeSuppressed = false;
                break;
        }

        if (store is IDisposable disposable)
            disposable.Dispose();
        else if (store is IAsyncDisposable asyncDisposable)
            asyncDisposable.DisposeAsync().AsTask().GetAwaiter().GetResult();
    }
}
