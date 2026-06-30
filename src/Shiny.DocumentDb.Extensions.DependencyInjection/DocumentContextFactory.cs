namespace Shiny.DocumentDb;

/// <summary>
/// Default <see cref="IDocumentContextFactory{TContext}"/> — holds the per-context store and news up a fresh
/// context per <see cref="Create"/>. The store is a singleton owned by the container (it is registered
/// separately and disposed with it), so the factory itself owns nothing disposable.
/// </summary>
/// <typeparam name="TContext">The generated context type.</typeparam>
public sealed class DocumentContextFactory<TContext> : IDocumentContextFactory<TContext>
    where TContext : DocumentContext
{
    readonly IDocumentStore store;
    readonly Func<IDocumentStore, TContext> activator;

    /// <summary>Creates the factory over <paramref name="store"/>, using <paramref name="activator"/> to construct each context.</summary>
    public DocumentContextFactory(IDocumentStore store, Func<IDocumentStore, TContext> activator)
    {
        ArgumentNullException.ThrowIfNull(store);
        ArgumentNullException.ThrowIfNull(activator);
        this.store = store;
        this.activator = activator;
    }

    /// <inheritdoc />
    public TContext Create() => this.activator(this.store);
}
