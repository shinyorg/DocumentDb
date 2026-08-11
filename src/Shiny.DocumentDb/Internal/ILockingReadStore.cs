using System.Text.Json.Serialization.Metadata;

namespace Shiny.DocumentDb.Internal;

/// <summary>
/// Implemented by the transaction-bound store so a session's locking read can reach real row-locking SQL
/// (<c>FOR UPDATE</c> / <c>UPDLOCK, HOLDLOCK</c>). It is deliberately not on <see cref="IDocumentStore"/>:
/// a locking read outside a transaction is meaningless, so the capability belongs to the transaction-bound
/// store rather than the root one.
/// </summary>
/// <remarks>
/// A store that does not implement this still honours <see cref="LockMode"/> — the session falls back to an
/// ordinary read, which is correct where the transaction boundary is itself the lock (SQLite and DuckDB take
/// a whole-database write lock on <c>BEGIN IMMEDIATE</c>).
/// </remarks>
interface ILockingReadStore
{
    Task<T?> GetWithLock<T>(
        object id,
        LockMode lockMode,
        JsonTypeInfo<T>? jsonTypeInfo,
        CancellationToken cancellationToken) where T : class;
}
