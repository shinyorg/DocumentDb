using System.Data.Common;
using System.Text.Json;

namespace Shiny.DocumentDb.Internal;

internal interface IQueryExecutor
{
    Task<TResult> ExecuteAsync<TResult>(Func<Task<TResult>> operation, CancellationToken ct);
    IAsyncEnumerable<T> ReadStreamAsync<T>(Action<DbCommand> configure, Func<string, T> deserialize, CancellationToken ct = default);
    DbCommand CreateCommand();
    string ResolveTypeName<T>();
    string ResolveTableName<T>();
    JsonSerializerOptions JsonOptions { get; }
    Action<string>? Logging { get; }
    IDatabaseProvider Provider { get; }
}
