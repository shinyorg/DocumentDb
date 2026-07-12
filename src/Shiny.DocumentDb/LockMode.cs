namespace Shiny.DocumentDb;

/// <summary>
/// Pessimistic locking hint for reads performed inside an explicit <see cref="IDocumentSession"/> transaction.
/// Relational only; document-native / key-partitioned providers throw <see cref="System.NotSupportedException"/>
/// for anything but <see cref="None"/>. See the store-as-connection design (§4f).
/// </summary>
/// <remarks>
/// SPIKE: the enum is plumbed end-to-end, but only the transaction boundary is enforced today — on SQLite an
/// explicit write transaction already takes a whole-database lock, so a locking read inside one is safe. Emitting
/// provider-specific <c>FOR UPDATE</c> / <c>UPDLOCK,HOLDLOCK</c> SQL is provider-migration work.
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
