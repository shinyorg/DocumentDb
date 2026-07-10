using System.Data.Common;
using System.Linq.Expressions;
using System.Text.Json;

namespace Shiny.DocumentDb.Internal;

internal interface IQueryExecutor
{
    /// <summary>
    /// Bridge from <see cref="DocumentQuery{T}"/> fluent <c>NearestVectors</c> to the owning store's
    /// implementation. <c>null</c> when the underlying executor is not a vector-capable store.
    /// </summary>
    Task<IReadOnlyList<VectorResult<T>>> NearestVectorsAsync<T>(
        ReadOnlyMemory<float> query,
        int k,
        Expression<Func<T, bool>>? filter,
        CancellationToken ct) where T : class;

    /// <summary>
    /// Bridge from <see cref="DocumentQuery{T}"/> fluent <c>FullTextMatch</c> to the owning store's
    /// implementation.
    /// </summary>
    Task<IReadOnlyList<FullTextResult<T>>> FullTextSearchAsync<T>(
        string searchText,
        int maxResults,
        Expression<Func<T, bool>>? filter,
        CancellationToken ct) where T : class;

    /// <summary>
    /// Runs <paramref name="operation"/> inside a bound session. For pooled providers the session
    /// owns a fresh connection that is disposed when the lambda returns; for SQLite-style stores
    /// it carries the long-lived shared connection. Callers must only use the session inside the
    /// lambda — the connection may be closed after it returns.
    /// </summary>
    Task<TResult> ExecuteAsync<TResult>(string tableName, Func<DocumentStoreSession, Task<TResult>> operation, CancellationToken ct);

    IAsyncEnumerable<T> ReadStreamAsync<T>(string tableName, Action<DbCommand> configure, Func<string, T> deserialize, CancellationToken ct = default);
    string ResolveTypeName<T>();
    string ResolveTableName<T>();

    /// <summary>
    /// Returns the cached <see cref="IdAccessor{T}"/> for <typeparamref name="T"/> — used by cursor
    /// pagination to read a materialized row's Id for the keyset tiebreaker.
    /// </summary>
    IdAccessor<T> GetIdAccessor<T>(System.Text.Json.Serialization.Metadata.JsonTypeInfo<T>? typeInfo) where T : class;
    JsonSerializerOptions JsonOptions { get; }
    Action<string>? Logging { get; }
    IDatabaseProvider Provider { get; }

    /// <summary>
    /// Returns " AND TenantId = @tenantId" when multi-tenancy is enabled, null otherwise.
    /// </summary>
    string? TenantFilter { get; }

    /// <summary>
    /// Adds the @tenantId parameter to the command when multi-tenancy is enabled.
    /// </summary>
    void AddTenantParameter(DbCommand cmd);

    /// <summary>
    /// Adds the @tenantId parameter name/value to <paramref name="parameters"/> when multi-tenancy is
    /// enabled. Mirrors <see cref="AddTenantParameter(DbCommand)"/> for query-string building.
    /// </summary>
    void CollectTenantParameter(IDictionary<string, object?> parameters);

    /// <summary>
    /// In-process change broadcaster. <c>null</c> when the underlying store does not support
    /// change observation (e.g. transactional sub-store paths that do not own one).
    /// </summary>
    ChangeBroadcaster? Broadcaster { get; }

    /// <summary>
    /// The owning store's options. Exposed so query implementations can resolve global query
    /// filters (and any future cross-cutting configuration).
    /// </summary>
    DocumentStoreOptions Options { get; }
}
