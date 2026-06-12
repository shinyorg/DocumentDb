using System.Linq.Expressions;

namespace Shiny.DocumentDb.Tests.Fixtures;

public interface IDocumentStoreFixture
{
    IDocumentStore CreateStore(string tableName);

    /// <summary>
    /// Create a store with a single unnamed global query filter for <typeparamref name="T"/>.
    /// </summary>
    IDocumentStore CreateStoreWithFilter<T>(string tableName, Expression<Func<T, bool>> filter) where T : class;

    /// <summary>
    /// Create a store with a single named global query filter for <typeparamref name="T"/>.
    /// </summary>
    IDocumentStore CreateStoreWithNamedFilter<T>(string tableName, string filterName, Expression<Func<T, bool>> filter) where T : class;

    /// <summary>
    /// Create a store with an optimistic-concurrency version property mapped on <typeparamref name="T"/>.
    /// </summary>
    IDocumentStore CreateStoreWithVersion<T>(string tableName, Expression<Func<T, int>> versionProperty) where T : class;

}

/// <summary>
/// Implemented only by fixtures whose provider supports shared-table multi-tenancy
/// (SQL providers — Sqlite, DuckDb, PostgreSql, MySql, SqlServer). Mongo/Cosmos/LiteDb
/// do not currently expose a <c>TenantIdAccessor</c>.
/// </summary>
public interface ITenantDocumentStoreFixture
{
    IDocumentStore CreateStoreWithTenant(string tableName, Func<string> tenantIdAccessor);
}

/// <summary>
/// Implemented by fixtures whose provider supports temporal history (the relational
/// <see cref="DocumentStore"/> providers — Sqlite, DuckDb, PostgreSql, MySql, SqlServer, Oracle).
/// Maps both <c>VersionedUser</c> (with the supplied options) and <c>MergeDoc</c> as temporal and
/// returns the concrete store so the History/AsOf/Restore/GetDiffBetween methods are reachable.
/// </summary>
public interface ITemporalDocumentStoreFixture
{
    DocumentStore CreateTemporalStore(string tableName, Action<TemporalOptions>? configure = null, Func<string>? actor = null);
}
