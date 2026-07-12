namespace Shiny.DocumentDb;

/// <summary>
/// Opens <see cref="IDocumentSession"/> units of work where no ambient DI scope is available (MAUI/desktop,
/// background workers, Orleans grains, seeders). Its real job is <b>scope ownership</b>: a factory session mints
/// and owns a private child scope, disposed with the session. Registered as a singleton. See design §4b/§4c.
/// </summary>
public interface IDocumentSessionFactory
{
    /// <summary>Opens a session on a fresh, session-owned child scope (disposed with the session).</summary>
    IDocumentSession OpenSession();

    /// <summary>Opens a session on a named store (§4c).</summary>
    IDocumentSession OpenSession(string storeName);

    /// <summary>Opens a session bound to a caller-supplied scope. The session does NOT dispose the scope.</summary>
    IDocumentSession OpenSession(IServiceProvider scope);

    /// <summary>Opens a session on a named store, bound to a caller-supplied scope.</summary>
    IDocumentSession OpenSession(string storeName, IServiceProvider scope);

    /// <summary>The root store by name — the root-only surface (change feed, maintenance, backup). Absorbs IDocumentStoreProvider.</summary>
    IDocumentStore GetStore(string storeName = "default");
}
