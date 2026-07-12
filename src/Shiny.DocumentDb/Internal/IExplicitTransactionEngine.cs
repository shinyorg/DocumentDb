namespace Shiny.DocumentDb.Internal;

/// <summary>
/// Implemented by stores that support caller-controlled explicit transactions (the relational
/// <see cref="DocumentStore"/>). <see cref="DocumentSession"/> gates <see cref="IDocumentSession.BeginTransaction"/>
/// on this capability; a store that does not implement it throws <see cref="NotSupportedException"/> (design §4f).
/// </summary>
interface IExplicitTransactionEngine
{
    /// <summary>Acquires a connection, begins a transaction (at <paramref name="isolationLevel"/> when not
    /// <see cref="System.Data.IsolationLevel.Unspecified"/>), and returns a caller-controlled unit whose
    /// <see cref="ExplicitUnit.Store"/> runs every op on that pinned connection + transaction.</summary>
    Task<ExplicitUnit> BeginExplicitUnitAsync(System.Data.IsolationLevel isolationLevel, CancellationToken cancellationToken);
}
