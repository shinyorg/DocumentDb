using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;

namespace Shiny.DocumentDb.Internal;

internal sealed class DocumentQuery<T> : IDocumentQuery<T> where T : class
{
    readonly IQueryExecutor executor;
    readonly JsonTypeInfo<T>? jsonTypeInfo;
    readonly JsonSerializerOptions jsonOptions;
    readonly List<Expression<Func<T, bool>>> wheres = [];
    readonly List<(Expression<Func<T, object>> Selector, bool IsDescending)> orderBys = [];
    Expression<Func<T, object>>? groupBy;
    int? paginateOffset;
    int? paginateTake;

    internal DocumentQuery(IQueryExecutor executor, JsonTypeInfo<T>? jsonTypeInfo)
    {
        this.executor = executor;
        this.jsonTypeInfo = jsonTypeInfo;
        this.jsonOptions = executor.JsonOptions;
    }

    public IDocumentQuery<T> Where(Expression<Func<T, bool>> predicate)
    {
        this.wheres.Add(predicate);
        return this;
    }

    public IDocumentQuery<T> OrderBy(Expression<Func<T, object>> selector)
    {
        this.orderBys.Add((selector, false));
        return this;
    }

    public IDocumentQuery<T> OrderByDescending(Expression<Func<T, object>> selector)
    {
        this.orderBys.Add((selector, true));
        return this;
    }

    public IDocumentQuery<T> GroupBy(Expression<Func<T, object>> selector)
    {
        this.groupBy = selector;
        return this;
    }

    public IDocumentQuery<T> Paginate(int offset, int take)
    {
        this.paginateOffset = offset;
        this.paginateTake = take;
        return this;
    }

    public IDocumentQuery<TResult> Select<TResult>(
        Expression<Func<T, TResult>> selector,
        JsonTypeInfo<TResult>? resultTypeInfo = null) where TResult : class
    {
        return new ProjectedDocumentQuery<T, TResult>(
            this.executor,
            this.jsonTypeInfo,
            this.wheres,
            this.orderBys,
            this.groupBy,
            selector,
            resultTypeInfo,
            this.paginateOffset,
            this.paginateTake);
    }

    public Task<IReadOnlyList<T>> ToList(CancellationToken ct = default)
    {
        var (whereClause, whereParams) = BuildWhereClause();
        var orderByClause = BuildOrderByClause();
        var paginationClause = BuildPaginationClause();
        var typeName = this.executor.ResolveTypeName<T>();
        var tableName = this.executor.ResolveTableName<T>();

        return this.executor.ExecuteAsync(async () =>
        {
            await using var cmd = this.executor.CreateCommand();
            var sql = $"SELECT Data FROM {tableName} WHERE TypeName = @typeName";
            if (whereClause != null)
                sql += $" AND ({whereClause})";
            sql += orderByClause + paginationClause + ";";
            cmd.CommandText = sql;
            AddParameter(cmd, "@typeName", typeName);
            if (whereParams != null)
                BindDictionaryParameters(cmd, whereParams);

            this.executor.Logging?.Invoke(cmd.CommandText);
            return await ReadListAsync(cmd, this.Deserialize, ct).ConfigureAwait(false);
        }, ct);
    }

    public IAsyncEnumerable<T> ToAsyncEnumerable(CancellationToken ct = default)
    {
        var (whereClause, whereParams) = BuildWhereClause();
        var orderByClause = BuildOrderByClause();
        var paginationClause = BuildPaginationClause();
        var typeName = this.executor.ResolveTypeName<T>();
        var tableName = this.executor.ResolveTableName<T>();

        return this.executor.ReadStreamAsync<T>(
            cmd =>
            {
                var sql = $"SELECT Data FROM {tableName} WHERE TypeName = @typeName";
                if (whereClause != null)
                    sql += $" AND ({whereClause})";
                sql += orderByClause + paginationClause + ";";
                cmd.CommandText = sql;
                AddParameter(cmd, "@typeName", typeName);
                if (whereParams != null)
                    BindDictionaryParameters(cmd, whereParams);
            },
            this.Deserialize,
            ct);
    }

    public Task<long> Count(CancellationToken ct = default)
    {
        var (whereClause, whereParams) = BuildWhereClause();
        var typeName = this.executor.ResolveTypeName<T>();
        var tableName = this.executor.ResolveTableName<T>();

        return this.executor.ExecuteAsync(async () =>
        {
            await using var cmd = this.executor.CreateCommand();
            var sql = $"SELECT COUNT(*) FROM {tableName} WHERE TypeName = @typeName";
            if (whereClause != null)
                sql += $" AND ({whereClause})";
            cmd.CommandText = sql + ";";
            AddParameter(cmd, "@typeName", typeName);
            if (whereParams != null)
                BindDictionaryParameters(cmd, whereParams);

            this.executor.Logging?.Invoke(cmd.CommandText);
            var result = await cmd.ExecuteScalarAsync(ct).ConfigureAwait(false);
            return Convert.ToInt64(result);
        }, ct);
    }

    public Task<bool> Any(CancellationToken ct = default)
    {
        var (whereClause, whereParams) = BuildWhereClause();
        var typeName = this.executor.ResolveTypeName<T>();
        var tableName = this.executor.ResolveTableName<T>();

        return this.executor.ExecuteAsync(async () =>
        {
            await using var cmd = this.executor.CreateCommand();
            var sql = $"SELECT EXISTS(SELECT 1 FROM {tableName} WHERE TypeName = @typeName";
            if (whereClause != null)
                sql += $" AND ({whereClause})";
            cmd.CommandText = sql + ");";
            AddParameter(cmd, "@typeName", typeName);
            if (whereParams != null)
                BindDictionaryParameters(cmd, whereParams);

            this.executor.Logging?.Invoke(cmd.CommandText);
            var result = await cmd.ExecuteScalarAsync(ct).ConfigureAwait(false);
            return Convert.ToInt64(result) == 1;
        }, ct);
    }

    public Task<int> ExecuteDelete(CancellationToken ct = default)
    {
        var (whereClause, whereParams) = BuildWhereClause();
        var typeName = this.executor.ResolveTypeName<T>();
        var tableName = this.executor.ResolveTableName<T>();

        return this.executor.ExecuteAsync(async () =>
        {
            await using var cmd = this.executor.CreateCommand();
            var sql = $"DELETE FROM {tableName} WHERE TypeName = @typeName";
            if (whereClause != null)
                sql += $" AND ({whereClause})";
            cmd.CommandText = sql + ";";
            AddParameter(cmd, "@typeName", typeName);
            if (whereParams != null)
                BindDictionaryParameters(cmd, whereParams);

            this.executor.Logging?.Invoke(cmd.CommandText);
            return await cmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
        }, ct);
    }

    public Task<int> ExecuteUpdate(Expression<Func<T, object>> property, object? value, CancellationToken ct = default)
    {
        var typeInfo = this.RequireTypeInfo();
        var jsonPath = IndexExpressionHelper.ResolveJsonPath(property, this.jsonOptions, typeInfo);
        var (whereClause, whereParams) = BuildWhereClause();
        var typeName = this.executor.ResolveTypeName<T>();
        var tableName = this.executor.ResolveTableName<T>();

        return this.executor.ExecuteAsync(async () =>
        {
            await using var cmd = this.executor.CreateCommand();
            var sql = $"UPDATE {tableName} SET Data = json_set(Data, @path, json(@value)), UpdatedAt = @now WHERE TypeName = @typeName";
            if (whereClause != null)
                sql += $" AND ({whereClause})";
            cmd.CommandText = sql + ";";
            AddParameter(cmd, "@path", "$." + jsonPath);
            AddParameter(cmd, "@value", DocumentStore.ToJsonLiteral(value));
            AddParameter(cmd, "@now", DateTimeOffset.UtcNow.ToString("O"));
            AddParameter(cmd, "@typeName", typeName);
            if (whereParams != null)
                BindDictionaryParameters(cmd, whereParams);

            this.executor.Logging?.Invoke(cmd.CommandText);
            return await cmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
        }, ct);
    }

    public Task<TValue> Max<TValue>(Expression<Func<T, TValue>> selector, CancellationToken ct = default)
        => ScalarAggregate("MAX", selector, ct);

    public Task<TValue> Min<TValue>(Expression<Func<T, TValue>> selector, CancellationToken ct = default)
        => ScalarAggregate("MIN", selector, ct);

    public Task<TValue> Sum<TValue>(Expression<Func<T, TValue>> selector, CancellationToken ct = default)
        => ScalarAggregate("SUM", selector, ct);

    public Task<double> Average(Expression<Func<T, object>> selector, CancellationToken ct = default)
    {
        var typeInfo = this.RequireTypeInfo();
        var jsonPath = AggregateTranslator.ResolveJsonPathFromSelector(selector, this.jsonOptions, typeInfo);
        var (whereClause, whereParams) = BuildWhereClause();
        var typeName = this.executor.ResolveTypeName<T>();
        var tableName = this.executor.ResolveTableName<T>();
        var provider = this.executor.Provider;

        return this.executor.ExecuteAsync(async () =>
        {
            await using var cmd = this.executor.CreateCommand();
            var sql = $"SELECT AVG({provider.JsonExtract("Data", jsonPath)}) FROM {tableName} WHERE TypeName = @typeName";
            if (whereClause != null)
                sql += $" AND ({whereClause})";
            cmd.CommandText = sql + ";";
            AddParameter(cmd, "@typeName", typeName);
            if (whereParams != null)
                BindDictionaryParameters(cmd, whereParams);

            this.executor.Logging?.Invoke(cmd.CommandText);
            var result = await cmd.ExecuteScalarAsync(ct).ConfigureAwait(false);
            if (result is null or DBNull)
                return 0d;
            return Convert.ToDouble(result);
        }, ct);
    }

    Task<TValue> ScalarAggregate<TValue>(string sqlFunc, Expression<Func<T, TValue>> selector, CancellationToken ct)
    {
        var typeInfo = this.RequireTypeInfo();
        var jsonPath = AggregateTranslator.ResolveJsonPathFromSelector(selector, this.jsonOptions, typeInfo);
        var (whereClause, whereParams) = BuildWhereClause();
        var typeName = this.executor.ResolveTypeName<T>();
        var tableName = this.executor.ResolveTableName<T>();
        var provider = this.executor.Provider;

        return this.executor.ExecuteAsync(async () =>
        {
            await using var cmd = this.executor.CreateCommand();
            var sql = $"SELECT {sqlFunc}({provider.JsonExtract("Data", jsonPath)}) FROM {tableName} WHERE TypeName = @typeName";
            if (whereClause != null)
                sql += $" AND ({whereClause})";
            cmd.CommandText = sql + ";";
            AddParameter(cmd, "@typeName", typeName);
            if (whereParams != null)
                BindDictionaryParameters(cmd, whereParams);

            this.executor.Logging?.Invoke(cmd.CommandText);
            var result = await cmd.ExecuteScalarAsync(ct).ConfigureAwait(false);
            if (result is null or DBNull)
                return default!;
            return (TValue)Convert.ChangeType(result, Nullable.GetUnderlyingType(typeof(TValue)) ?? typeof(TValue));
        }, ct);
    }

    JsonTypeInfo<T> RequireTypeInfo()
    {
        return this.jsonTypeInfo ?? throw new InvalidOperationException(
            $"This operation requires a JsonTypeInfo<{typeof(T).Name}>. Use the Query<T>(JsonTypeInfo<T>) overload.");
    }

    (string? WhereClause, Dictionary<string, object?>? Parameters) BuildWhereClause()
    {
        if (this.wheres.Count == 0)
            return (null, null);

        var typeInfo = RequireTypeInfo();
        var combined = CombinePredicates(this.wheres);
        var (clause, parms) = JsonExpressionVisitor.Translate(combined, typeInfo, this.executor.Provider);
        return (clause, parms);
    }

    string BuildPaginationClause()
    {
        if (this.paginateTake == null)
            return "";

        return " " + this.executor.Provider.BuildPaginationClause(this.paginateOffset!.Value, this.paginateTake.Value);
    }

    string BuildOrderByClause()
    {
        if (this.orderBys.Count == 0)
            return "";

        var typeInfo = RequireTypeInfo();
        var provider = this.executor.Provider;
        var parts = new List<string>(this.orderBys.Count);
        foreach (var (selector, isDescending) in this.orderBys)
        {
            var jsonPath = IndexExpressionHelper.ResolveJsonPath(selector, this.jsonOptions, typeInfo);
            var direction = isDescending ? "DESC" : "ASC";
            parts.Add($"{provider.JsonExtract("Data", jsonPath)} {direction}");
        }
        return " ORDER BY " + string.Join(", ", parts);
    }

    [UnconditionalSuppressMessage("Trimming", "IL2026", Justification = "Reflection path is only reached when jsonTypeInfo is null (reflection fallback).")]
    [UnconditionalSuppressMessage("AOT", "IL3050", Justification = "Reflection path is only reached when jsonTypeInfo is null (reflection fallback).")]
    T Deserialize(string json)
    {
        return this.jsonTypeInfo != null
            ? JsonSerializer.Deserialize(json, this.jsonTypeInfo)!
            : JsonSerializer.Deserialize<T>(json, this.jsonOptions)!;
    }

    internal static Expression<Func<TItem, bool>> CombinePredicates<TItem>(List<Expression<Func<TItem, bool>>> predicates)
    {
        if (predicates.Count == 1)
            return predicates[0];

        var parameter = predicates[0].Parameters[0];
        Expression body = predicates[0].Body;

        for (var i = 1; i < predicates.Count; i++)
        {
            var nextBody = new ParameterReplacer(predicates[i].Parameters[0], parameter).Visit(predicates[i].Body);
            body = Expression.AndAlso(body, nextBody);
        }

        return Expression.Lambda<Func<TItem, bool>>(body, parameter);
    }

    internal static void BindDictionaryParameters(DbCommand cmd, Dictionary<string, object?> parameters)
    {
        foreach (var kvp in parameters)
        {
            var paramName = kvp.Key.StartsWith('@') ? kvp.Key : "@" + kvp.Key;
            AddParameter(cmd, paramName, kvp.Value ?? DBNull.Value);
        }
    }

    internal static void AddParameter(DbCommand cmd, string name, object? value)
    {
        var p = cmd.CreateParameter();
        p.ParameterName = name;
        p.Value = value ?? DBNull.Value;
        cmd.Parameters.Add(p);
    }

    internal static async Task<IReadOnlyList<TItem>> ReadListAsync<TItem>(DbCommand cmd, Func<string, TItem> deserialize, CancellationToken ct)
    {
        var list = new List<TItem>();
        await using var reader = await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false);
        while (await reader.ReadAsync(ct).ConfigureAwait(false))
        {
            var json = reader.GetString(0);
            list.Add(deserialize(json));
        }
        return list;
    }

    sealed class ParameterReplacer : ExpressionVisitor
    {
        readonly ParameterExpression from;
        readonly ParameterExpression to;

        public ParameterReplacer(ParameterExpression from, ParameterExpression to)
        {
            this.from = from;
            this.to = to;
        }

        protected override Expression VisitParameter(ParameterExpression node)
            => node == this.from ? this.to : node;
    }
}
