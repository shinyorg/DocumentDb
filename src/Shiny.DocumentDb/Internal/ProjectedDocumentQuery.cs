using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;

namespace Shiny.DocumentDb.Internal;

internal sealed class ProjectedDocumentQuery<TSource, TResult> : IDocumentQuery<TResult>
    where TSource : class
    where TResult : class
{
    readonly IQueryExecutor executor;
    readonly JsonTypeInfo<TSource>? sourceTypeInfo;
    readonly List<Expression<Func<TSource, bool>>> wheres;
    readonly List<(Expression<Func<TSource, object>> Selector, bool IsDescending)> orderBys;
    readonly Expression<Func<TSource, object>>? groupBy;
    readonly Expression<Func<TSource, TResult>> selector;
    readonly JsonTypeInfo<TResult>? resultTypeInfo;
    readonly int? paginateOffset;
    readonly int? paginateTake;

    internal ProjectedDocumentQuery(
        IQueryExecutor executor,
        JsonTypeInfo<TSource>? sourceTypeInfo,
        List<Expression<Func<TSource, bool>>> wheres,
        List<(Expression<Func<TSource, object>> Selector, bool IsDescending)> orderBys,
        Expression<Func<TSource, object>>? groupBy,
        Expression<Func<TSource, TResult>> selector,
        JsonTypeInfo<TResult>? resultTypeInfo,
        int? paginateOffset,
        int? paginateTake)
    {
        this.executor = executor;
        this.sourceTypeInfo = sourceTypeInfo;
        this.wheres = new List<Expression<Func<TSource, bool>>>(wheres);
        this.orderBys = new List<(Expression<Func<TSource, object>>, bool)>(orderBys);
        this.groupBy = groupBy;
        this.selector = selector;
        this.resultTypeInfo = resultTypeInfo;
        this.paginateOffset = paginateOffset;
        this.paginateTake = paginateTake;
    }

    public IDocumentQuery<TResult> Where(Expression<Func<TResult, bool>> predicate)
        => throw new InvalidOperationException("Cannot modify query after Select.");

    public IDocumentQuery<TResult> OrderBy(Expression<Func<TResult, object>> selector)
        => throw new InvalidOperationException("Cannot modify query after Select.");

    public IDocumentQuery<TResult> OrderByDescending(Expression<Func<TResult, object>> selector)
        => throw new InvalidOperationException("Cannot modify query after Select.");

    public IDocumentQuery<TResult> GroupBy(Expression<Func<TResult, object>> selector)
        => throw new InvalidOperationException("Cannot modify query after Select.");

    public IDocumentQuery<TResult> Paginate(int offset, int take)
        => throw new InvalidOperationException("Cannot modify query after Select.");

    public IDocumentQuery<TNewResult> Select<TNewResult>(
        Expression<Func<TResult, TNewResult>> selector,
        JsonTypeInfo<TNewResult>? resultTypeInfo = null) where TNewResult : class
        => throw new InvalidOperationException("Cannot apply Select twice.");

    public Task<IReadOnlyList<TResult>> ToList(CancellationToken ct = default)
    {
        var srcTypeInfo = RequireSourceTypeInfo();
        var (whereClause, whereParams) = BuildWhereClause(srcTypeInfo);
        var orderByClause = BuildOrderByClause(srcTypeInfo);
        var paginationClause = BuildPaginationClause();
        var typeName = this.executor.ResolveTypeName<TSource>();
        var tableName = this.executor.ResolveTableName<TSource>();
        var useAggregate = ContainsSqlAggregates(this.selector.Body) || this.groupBy != null;
        var provider = this.executor.Provider;
        var qt = provider.QuoteTable(tableName);

        return this.executor.ExecuteAsync(async () =>
        {
            await using var cmd = this.executor.CreateCommand();
            string sql;
            Dictionary<string, object?> projParams;

            if (useAggregate)
            {
                var (selectClause, groupByClause, aggParams) = AggregateTranslator.Translate(this.selector, srcTypeInfo, RequireResultTypeInfo(), provider);
                projParams = aggParams;
                sql = $"SELECT {selectClause} FROM {qt} WHERE TypeName = @typeName";
                sql += this.executor.TenantFilter ?? "";
                if (whereClause != null)
                    sql += $" AND ({whereClause})";
                if (groupByClause != null)
                    sql += $" GROUP BY {groupByClause}";
            }
            else
            {
                var (projection, parms) = ProjectionTranslator.Translate(this.selector, srcTypeInfo, RequireResultTypeInfo(), provider);
                projParams = parms;
                sql = $"SELECT {projection} FROM {qt} WHERE TypeName = @typeName";
                sql += this.executor.TenantFilter ?? "";
                if (whereClause != null)
                    sql += $" AND ({whereClause})";
            }

            sql += orderByClause + paginationClause + ";";
            cmd.CommandText = sql;
            DocumentQuery<TSource>.AddParameter(cmd, "@typeName", typeName);
            this.executor.AddTenantParameter(cmd);
            if (whereParams != null)
                DocumentQuery<TSource>.BindDictionaryParameters(cmd, whereParams);
            DocumentQuery<TSource>.BindDictionaryParameters(cmd, projParams);

            this.executor.Logging?.Invoke(cmd.CommandText);
            return await DocumentQuery<TSource>.ReadListAsync(cmd, this.DeserializeResult, ct).ConfigureAwait(false);
        }, ct);
    }

    public IAsyncEnumerable<TResult> ToAsyncEnumerable(CancellationToken ct = default)
    {
        var srcTypeInfo = RequireSourceTypeInfo();
        var (whereClause, whereParams) = BuildWhereClause(srcTypeInfo);
        var orderByClause = BuildOrderByClause(srcTypeInfo);
        var paginationClause = BuildPaginationClause();
        var typeName = this.executor.ResolveTypeName<TSource>();
        var tableName = this.executor.ResolveTableName<TSource>();
        var useAggregate = ContainsSqlAggregates(this.selector.Body) || this.groupBy != null;
        var provider = this.executor.Provider;
        var qt = provider.QuoteTable(tableName);

        string selectSql;
        Dictionary<string, object?> projParams;
        string? groupByStr = null;

        if (useAggregate)
        {
            var (selectClause, groupByClause, aggParams) = AggregateTranslator.Translate(this.selector, srcTypeInfo, RequireResultTypeInfo(), provider);
            selectSql = selectClause;
            projParams = aggParams;
            groupByStr = groupByClause;
        }
        else
        {
            var (projection, parms) = ProjectionTranslator.Translate(this.selector, srcTypeInfo, RequireResultTypeInfo(), provider);
            selectSql = projection;
            projParams = parms;
        }

        return this.executor.ReadStreamAsync<TResult>(
            cmd =>
            {
                var sql = $"SELECT {selectSql} FROM {qt} WHERE TypeName = @typeName";
                sql += this.executor.TenantFilter ?? "";
                if (whereClause != null)
                    sql += $" AND ({whereClause})";
                if (groupByStr != null)
                    sql += $" GROUP BY {groupByStr}";
                sql += orderByClause + paginationClause + ";";
                cmd.CommandText = sql;
                DocumentQuery<TSource>.AddParameter(cmd, "@typeName", typeName);
                this.executor.AddTenantParameter(cmd);
                if (whereParams != null)
                    DocumentQuery<TSource>.BindDictionaryParameters(cmd, whereParams);
                DocumentQuery<TSource>.BindDictionaryParameters(cmd, projParams);
            },
            this.DeserializeResult,
            ct);
    }

    public Task<long> Count(CancellationToken ct = default)
    {
        var srcTypeInfo = RequireSourceTypeInfo();
        var (whereClause, whereParams) = BuildWhereClause(srcTypeInfo);
        var typeName = this.executor.ResolveTypeName<TSource>();
        var tableName = this.executor.ResolveTableName<TSource>();
        var qt = this.executor.Provider.QuoteTable(tableName);

        return this.executor.ExecuteAsync(async () =>
        {
            await using var cmd = this.executor.CreateCommand();
            var sql = $"SELECT COUNT(*) FROM {qt} WHERE TypeName = @typeName";
            sql += this.executor.TenantFilter ?? "";
            if (whereClause != null)
                sql += $" AND ({whereClause})";
            cmd.CommandText = sql + ";";
            DocumentQuery<TSource>.AddParameter(cmd, "@typeName", typeName);
            this.executor.AddTenantParameter(cmd);
            if (whereParams != null)
                DocumentQuery<TSource>.BindDictionaryParameters(cmd, whereParams);

            this.executor.Logging?.Invoke(cmd.CommandText);
            var result = await cmd.ExecuteScalarAsync(ct).ConfigureAwait(false);
            return Convert.ToInt64(result);
        }, ct);
    }

    public Task<int> ExecuteDelete(CancellationToken ct = default)
        => throw new InvalidOperationException("Cannot execute delete after Select.");

    public Task<int> ExecuteUpdate(Expression<Func<TResult, object>> property, object? value, CancellationToken ct = default)
        => throw new InvalidOperationException("Cannot execute update after Select.");

    public Task<bool> Any(CancellationToken ct = default)
    {
        var srcTypeInfo = RequireSourceTypeInfo();
        var (whereClause, whereParams) = BuildWhereClause(srcTypeInfo);
        var typeName = this.executor.ResolveTypeName<TSource>();
        var tableName = this.executor.ResolveTableName<TSource>();
        var qt = this.executor.Provider.QuoteTable(tableName);

        return this.executor.ExecuteAsync(async () =>
        {
            await using var cmd = this.executor.CreateCommand();
            var sql = $"SELECT CASE WHEN EXISTS(SELECT 1 FROM {qt} WHERE TypeName = @typeName";
            sql += this.executor.TenantFilter ?? "";
            if (whereClause != null)
                sql += $" AND ({whereClause})";
            cmd.CommandText = sql + ") THEN 1 ELSE 0 END;";
            DocumentQuery<TSource>.AddParameter(cmd, "@typeName", typeName);
            this.executor.AddTenantParameter(cmd);
            if (whereParams != null)
                DocumentQuery<TSource>.BindDictionaryParameters(cmd, whereParams);

            this.executor.Logging?.Invoke(cmd.CommandText);
            var result = await cmd.ExecuteScalarAsync(ct).ConfigureAwait(false);
            return Convert.ToInt64(result) == 1;
        }, ct);
    }

    public Task<TValue> Max<TValue>(Expression<Func<TResult, TValue>> selector, CancellationToken ct = default)
        => throw new InvalidOperationException("Aggregate terminals are not supported after Select.");

    public Task<TValue> Min<TValue>(Expression<Func<TResult, TValue>> selector, CancellationToken ct = default)
        => throw new InvalidOperationException("Aggregate terminals are not supported after Select.");

    public Task<TValue> Sum<TValue>(Expression<Func<TResult, TValue>> selector, CancellationToken ct = default)
        => throw new InvalidOperationException("Aggregate terminals are not supported after Select.");

    public Task<double> Average(Expression<Func<TResult, object>> selector, CancellationToken ct = default)
        => throw new InvalidOperationException("Aggregate terminals are not supported after Select.");

    [UnconditionalSuppressMessage("Trimming", "IL2026", Justification = "Reflection path only used when resultTypeInfo is null (reflection fallback).")]
    [UnconditionalSuppressMessage("AOT", "IL3050", Justification = "Reflection path only used when resultTypeInfo is null (reflection fallback).")]
    TResult DeserializeResult(string json)
    {
        return this.resultTypeInfo != null
            ? JsonSerializer.Deserialize(json, this.resultTypeInfo)!
            : JsonSerializer.Deserialize<TResult>(json, this.executor.JsonOptions)!;
    }

    JsonTypeInfo<TSource> RequireSourceTypeInfo()
    {
        return this.sourceTypeInfo ?? throw new InvalidOperationException(
            $"This operation requires a JsonTypeInfo<{typeof(TSource).Name}>. Use the Query<T>(JsonTypeInfo<T>) overload.");
    }

    JsonTypeInfo<TResult> RequireResultTypeInfo()
    {
        return this.resultTypeInfo ?? throw new InvalidOperationException(
            $"This operation requires a JsonTypeInfo<{typeof(TResult).Name}>. Pass it to the Select() call.");
    }

    (string? WhereClause, Dictionary<string, object?>? Parameters) BuildWhereClause(JsonTypeInfo<TSource> typeInfo)
    {
        if (this.wheres.Count == 0)
            return (null, null);

        var combined = DocumentQuery<TSource>.CombinePredicates(this.wheres);
        var (clause, parms) = JsonExpressionVisitor.Translate(combined, typeInfo, this.executor.Provider);
        return (clause, parms);
    }

    string BuildPaginationClause()
    {
        if (this.paginateTake == null)
            return "";

        return " " + this.executor.Provider.BuildPaginationClause(this.paginateOffset!.Value, this.paginateTake.Value);
    }

    string BuildOrderByClause(JsonTypeInfo<TSource> typeInfo)
    {
        if (this.orderBys.Count == 0)
            return "";

        var provider = this.executor.Provider;
        var parts = new List<string>(this.orderBys.Count);
        foreach (var (selector, isDescending) in this.orderBys)
        {
            var jsonPath = IndexExpressionHelper.ResolveJsonPath(selector, this.executor.JsonOptions, typeInfo);
            var direction = isDescending ? "DESC" : "ASC";
            parts.Add($"{provider.JsonExtract("Data", jsonPath)} {direction}");
        }
        return " ORDER BY " + string.Join(", ", parts);
    }

    static bool ContainsSqlAggregates(Expression body)
    {
        if (body is not MemberInitExpression memberInit)
            return false;

        foreach (var binding in memberInit.Bindings)
        {
            if (binding is MemberAssignment assignment && HasSqlMethodCall(assignment.Expression))
                return true;
        }
        return false;
    }

    static bool HasSqlMethodCall(Expression expr)
    {
        if (expr is MethodCallExpression mc && mc.Method.DeclaringType == typeof(Sql))
            return true;
        return false;
    }
}
