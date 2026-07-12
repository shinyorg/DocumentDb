using Microsoft.Extensions.DependencyInjection;

namespace Shiny.DocumentDb;

/// <summary>
/// Default <see cref="IDocumentContextFactory{TContext}"/> — creates a fresh context per <see cref="Create"/>,
/// each owning a private child DI scope + session (disposed when the context is disposed). The no-ambient-scope
/// registration (MAUI/desktop/background); mirrors EF Core's <c>IDbContextFactory</c>. See design §4b/§4d.
/// </summary>
/// <typeparam name="TContext">The generated context type.</typeparam>
public sealed class DocumentContextFactory<TContext> : IDocumentContextFactory<TContext>
    where TContext : DocumentContext
{
    readonly IDocumentStore store;
    readonly IServiceScopeFactory scopeFactory;
    readonly Func<IDocumentSession, TContext> activator;

    /// <summary>Creates the factory over <paramref name="store"/>, using <paramref name="activator"/> to construct each context.</summary>
    public DocumentContextFactory(IDocumentStore store, IServiceScopeFactory scopeFactory, Func<IDocumentSession, TContext> activator)
    {
        ArgumentNullException.ThrowIfNull(store);
        ArgumentNullException.ThrowIfNull(scopeFactory);
        ArgumentNullException.ThrowIfNull(activator);
        this.store = store;
        this.scopeFactory = scopeFactory;
        this.activator = activator;
    }

    /// <inheritdoc />
    public TContext Create()
    {
        var scope = this.scopeFactory.CreateScope();
        // The session owns the child scope; the context owns the session and disposes both on DisposeAsync.
        var session = new DocumentSession(this.store, scope.ServiceProvider, scope);
        return this.activator(session);
    }
}
