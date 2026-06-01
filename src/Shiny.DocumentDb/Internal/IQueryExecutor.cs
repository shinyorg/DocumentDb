using System.Data.Common;
using System.Text.Json;

namespace Shiny.DocumentDb.Internal;

internal interface IQueryExecutor
{
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
