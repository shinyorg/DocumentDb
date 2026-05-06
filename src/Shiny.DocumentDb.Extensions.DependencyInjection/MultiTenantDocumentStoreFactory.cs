using System.Collections.Concurrent;

namespace Shiny.DocumentDb;

internal sealed class MultiTenantDocumentStoreFactory(
    Func<string, DocumentStoreOptions> optionsFactory) : IDisposable
{
    readonly ConcurrentDictionary<string, IDocumentStore> stores = new();

    public IDocumentStore GetStore(string tenantId)
        => stores.GetOrAdd(tenantId, id => new DocumentStore(optionsFactory(id)));

    public void Dispose()
    {
        foreach (var store in stores.Values)
            (store as IDisposable)?.Dispose();
        stores.Clear();
    }
}
