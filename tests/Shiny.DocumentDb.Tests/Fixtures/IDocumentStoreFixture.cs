using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;

namespace Shiny.DocumentDb.Tests.Fixtures;

public interface IDocumentStoreFixture
{
    IDocumentStore CreateStore(string tableName);

    /// <summary>
    /// Create a store configured through the provider-agnostic <see cref="IDocumentStoreOptions"/>. This is the
    /// one hook a cross-provider conformance suite needs — anything expressible on that interface (query filters,
    /// interceptors, soft delete, JSON schema) no longer needs its own fixture method per provider.
    /// </summary>
    IDocumentStore CreateStore(string tableName, Action<IDocumentStoreOptions> configure);

    /// <summary>
    /// Create a store with a single unnamed global query filter for <typeparamref name="T"/>.
    /// </summary>
    IDocumentStore CreateStoreWithFilter<T>(string tableName, Expression<Func<T, bool>> filter) where T : class
        => this.CreateStore(tableName, o => o.AddQueryFilter(null, filter));

    /// <summary>
    /// Create a store with a single named global query filter for <typeparamref name="T"/>.
    /// </summary>
    IDocumentStore CreateStoreWithNamedFilter<T>(string tableName, string filterName, Expression<Func<T, bool>> filter) where T : class
        => this.CreateStore(tableName, o => o.AddQueryFilter(filterName, filter));

    /// <summary>
    /// Create a store with an optimistic-concurrency version property mapped on <typeparamref name="T"/>.
    /// </summary>
    IDocumentStore CreateStoreWithVersion<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)] T>(string tableName, Expression<Func<T, int>> versionProperty) where T : class;

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
/// Implemented by fixtures whose provider supports spatial queries — the relational two-pass providers
/// (Sqlite, DuckDb, PostgreSql, MySql, SqlServer, Oracle) and the document stores (MongoDb, CosmosDb).
/// Maps <see cref="GeoZone"/>'s <c>Area</c> as a full geometry.
/// </summary>
public interface ISpatialDocumentStoreFixture
{
    IDocumentStore CreateSpatialStore(string tableName);
}

/// <summary>
/// Implemented by every fixture whose provider supports temporal history — the relational
/// <see cref="DocumentStore"/> providers (Sqlite, DuckDb, PostgreSql, MySql, SqlServer, Oracle) and the
/// document stores (LiteDb, MongoDb, CosmosDb, IndexedDb). Maps both <c>VersionedUser</c> (with the
/// supplied options) and <c>MergeDoc</c> as temporal and returns the store as
/// <see cref="ITemporalDocumentStore"/> so the History/AsOf/Restore/GetDiffBetween methods are reachable.
/// </summary>
public interface ITemporalDocumentStoreFixture
{
    ITemporalDocumentStore CreateTemporalStore(string tableName, Action<TemporalOptions>? configure = null, Func<string>? actor = null);
}
