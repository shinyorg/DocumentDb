using Microsoft.Extensions.DependencyInjection;

namespace Shiny.DocumentDb;

/// <summary>
/// Default <see cref="IDocumentSessionFactory"/> — mints sessions that own a private child DI scope. Registered
/// as a singleton by <c>AddDocumentStore</c>. See design §4b.
/// </summary>
public sealed class DocumentSessionFactory : IDocumentSessionFactory
{
    readonly IServiceProvider root;
    readonly IServiceScopeFactory scopeFactory;

    public DocumentSessionFactory(IServiceProvider root, IServiceScopeFactory scopeFactory)
    {
        this.root = root;
        this.scopeFactory = scopeFactory;
    }

    public IDocumentSession OpenSession()
    {
        var scope = this.scopeFactory.CreateScope();
        var store = scope.ServiceProvider.GetRequiredService<IDocumentStore>();
        return new DocumentSession(store, scope.ServiceProvider, scope);   // session owns and disposes the scope
    }

    public IDocumentSession OpenSession(IServiceProvider scope)
    {
        var store = scope.GetRequiredService<IDocumentStore>();
        return new DocumentSession(store, scope, ownedScope: null);        // caller owns the scope
    }

    public IDocumentSession OpenSession(string storeName)
    {
        var scope = this.scopeFactory.CreateScope();
        var store = scope.ServiceProvider.GetRequiredKeyedService<IDocumentStore>(storeName);
        return new DocumentSession(store, scope.ServiceProvider, scope);
    }

    public IDocumentSession OpenSession(string storeName, IServiceProvider scope)
    {
        var store = scope.GetRequiredKeyedService<IDocumentStore>(storeName);
        return new DocumentSession(store, scope, ownedScope: null);
    }

    public IDocumentStore GetStore(string storeName = "default")
        => storeName == "default"
            ? this.root.GetRequiredService<IDocumentStore>()
            : this.root.GetRequiredKeyedService<IDocumentStore>(storeName);
}
