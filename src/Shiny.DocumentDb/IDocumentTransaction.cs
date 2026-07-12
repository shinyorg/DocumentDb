namespace Shiny.DocumentDb;

/// <summary>
/// An explicit, caller-controlled transaction opened on an <see cref="IDocumentSession"/> via
/// <see cref="IDocumentSession.BeginTransaction"/>. Only one may be active on a session at a time. While it is
/// open, every session operation (buffered-write flush, locking reads, set-based ExecuteUpdate/ExecuteDelete)
/// runs on its pinned connection. Disposing without <see cref="Commit"/> rolls back. See design §4f.
/// </summary>
public interface IDocumentTransaction : IAsyncDisposable
{
    /// <summary>True until the transaction is committed, rolled back, or disposed.</summary>
    bool IsActive { get; }

    /// <summary>Commits the transaction, releases the connection, and clears the session's current transaction.</summary>
    Task Commit(CancellationToken cancellationToken = default);

    /// <summary>Rolls the transaction back, releases the connection, and clears the session's current transaction.</summary>
    Task Rollback(CancellationToken cancellationToken = default);
}
