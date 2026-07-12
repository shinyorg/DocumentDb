namespace Shiny.DocumentDb;

/// <summary>
/// Pessimistic locking hint for reads performed inside an explicit <see cref="IDocumentSession"/> transaction.
/// Relational only; document-native / key-partitioned providers throw <see cref="System.NotSupportedException"/>
/// for anything but <see cref="None"/>. See the store-as-connection design (§4f).
/// </summary>
/// <remarks>
/// Today the transaction boundary is what enforces the lock — on SQLite an explicit write transaction already
/// takes a whole-database lock, so a locking read inside one is safe. Emitting provider-specific
/// <c>FOR UPDATE</c> / <c>UPDLOCK,HOLDLOCK</c> SQL for finer row-level locking is a per-provider enhancement.
/// </remarks>
public enum LockMode
{
    /// <summary>No lock hint (default).</summary>
    None = 0,

    /// <summary>Exclusive lock — <c>FOR UPDATE</c> / <c>UPDLOCK,HOLDLOCK</c> / SQLite <c>BEGIN IMMEDIATE</c>.</summary>
    Update,

    /// <summary>Shared lock — <c>FOR SHARE</c> (SQLite: same as <see cref="Update"/>, whole-database).</summary>
    Share
}
