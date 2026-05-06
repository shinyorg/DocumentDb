using Microsoft.Extensions.DependencyInjection;

namespace Shiny.DocumentDb;

internal sealed class DocumentStoreProvider(IServiceProvider serviceProvider) : IDocumentStoreProvider
{
    public IDocumentStore GetStore(string name)
        => serviceProvider.GetRequiredKeyedService<IDocumentStore>(name);
}
