using Microsoft.Extensions.DependencyInjection;

namespace Shiny.DocumentDb;

/// <summary>
/// Default <see cref="IDocumentSessionFactory"/> — mints sessions that own a private child DI scope. Registered
/// as a singleton by <c>AddDocumentStore</c>. See design §4b.
/// </summary>
/// <remarks>SPIKE: the default store is supported; named/multi-store overloads throw until §4c.</remarks>
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
        var store = (DocumentStore)scope.ServiceProvider.GetRequiredService<IDocumentStore>();
        return new DocumentSession(store, scope.ServiceProvider, scope);   // session owns and disposes the scope
    }

    public IDocumentSession OpenSession(IServiceProvider scope)
    {
        var store = (DocumentStore)scope.GetRequiredService<IDocumentStore>();
        return new DocumentSession(store, scope, ownedScope: null);        // caller owns the scope
    }

    public IDocumentSession OpenSession(string storeName)
        => throw new NotSupportedException("SPIKE: named/multi-store sessions land with §4c.");

    public IDocumentSession OpenSession(string storeName, IServiceProvider scope)
        => throw new NotSupportedException("SPIKE: named/multi-store sessions land with §4c.");

    public IDocumentStore GetStore(string storeName = "default")
        => this.root.GetRequiredService<IDocumentStore>();
}
