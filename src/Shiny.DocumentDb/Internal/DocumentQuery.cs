using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;
using System.Runtime.CompilerServices;
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
    bool ignoreAllFilters;
    HashSet<string>? ignoredFilterNames;

    internal DocumentQuery(IQueryExecutor executor, JsonTypeInfo<T>? jsonTypeInfo)
    {
        this.executor = executor;
        this.jsonTypeInfo = jsonTypeInfo;
        this.jsonOptions = executor.JsonOptions;
    }

    string Qt(string tableName) => this.executor.Provider.QuoteTable(tableName);

    public JsonTypeInfo<T>? QueryTypeInfo => this.jsonTypeInfo;

    public IDocumentQuery<T> Where(Expression<Func<T, bool>> predicate)
    {
        this.wheres.Add(predicate);
        return this;
    }

    public IDocumentQuery<T> IgnoreQueryFilters()
    {
        this.ignoreAllFilters = true;
        return this;
    }

    public IDocumentQuery<T> IgnoreQueryFilters(params string[] filterNames)
    {
        ArgumentNullException.ThrowIfNull(filterNames);
        this.ignoredFilterNames ??= new HashSet<string>(StringComparer.Ordinal);
        foreach (var n in filterNames)
            if (!string.IsNullOrWhiteSpace(n))
                this.ignoredFilterNames.Add(n);
        return this;
    }

    /// <summary>
    /// Returns the predicates effective for this query: any registered global filters that are
    /// not ignored, followed by the user's <see cref="Where"/> predicates.
    /// </summary>
    internal List<Expression<Func<T, bool>>> GetEffectivePredicates()
    {
        var effective = new List<Expression<Func<T, bool>>>();
        if (!this.ignoreAllFilters)
        {
            foreach (var f in this.executor.Options.ResolveQueryFilters(typeof(T)))
            {
                if (f.Name != null && this.ignoredFilterNames?.Contains(f.Name) == true)
                    continue;
                effective.Add((Expression<Func<T, bool>>)f.Predicate);
            }
        }
        effective.AddRange(this.wheres);
        return effective;
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
            this.paginateTake,
            this.ignoreAllFilters,
            this.ignoredFilterNames);
    }

    public IDocumentQuery<System.Text.Json.Nodes.JsonObject> Project(string fields, JsonTypeInfo<T>? jsonTypeInfo = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(fields);
        var typeInfo = jsonTypeInfo ?? this.jsonTypeInfo
            ?? throw new InvalidOperationException(
                $"No JsonTypeInfo<{typeof(T).Name}> could be resolved for the projection. " +
                "Pass one explicitly or register a JsonSerializerContext on the store.");

        var provider = this.executor.Provider;
        var pairs = new List<string>();
        var seen = new HashSet<string>(StringComparer.Ordinal);

        foreach (var raw in fields.Split(','))
        {
            var path = raw.Trim();
            if (path.Length == 0)
                continue;

            var (jsonPath, leafJsonName) = DocumentQueryExtensions.ResolveJsonPath(path, typeInfo);
            if (!seen.Add(leafJsonName))
                throw new ArgumentException(
                    $"Field '{path}' resolves to duplicate output key '{leafJsonName}'. Projected fields must have unique leaf names.",
                    nameof(fields));

            pairs.Add($"'{leafJsonName}'");
            pairs.Add(provider.JsonExtract("Data", jsonPath));
        }

        if (pairs.Count == 0)
            throw new ArgumentException("At least one field must be specified.", nameof(fields));

        return new JsonProjectionDocumentQuery<T>(
            this.executor,
            typeInfo,
            this.wheres,
            this.orderBys,
            provider.JsonObject(pairs),
            this.paginateOffset,
            this.paginateTake,
            this.ignoreAllFilters,
            this.ignoredFilterNames);
    }

    public Task<IReadOnlyList<T>> ToList(CancellationToken ct = default)
    {
        var (whereClause, whereParams) = BuildWhereClause();
        var orderByClause = BuildOrderByClause();
        var paginationClause = BuildPaginationClause();
        if (orderByClause == "" && paginationClause != "")
            orderByClause = " ORDER BY (SELECT NULL)";
        var typeName = this.executor.ResolveTypeName<T>();
        var tableName = this.executor.ResolveTableName<T>();

        return this.executor.ExecuteAsync(tableName, async session =>
        {
            await using var cmd = session.CreateCommand();
            var sql = $"SELECT Data FROM {Qt(tableName)} WHERE TypeName = @typeName";
            sql += this.executor.TenantFilter ?? "";
            if (whereClause != null)
                sql += $" AND ({whereClause})";
            sql += orderByClause + paginationClause + ";";
            cmd.CommandText = sql;
            AddParameter(cmd, "@typeName", typeName);
            this.executor.AddTenantParameter(cmd);
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
        if (orderByClause == "" && paginationClause != "")
            orderByClause = " ORDER BY (SELECT NULL)";
        var typeName = this.executor.ResolveTypeName<T>();
        var tableName = this.executor.ResolveTableName<T>();

        return this.executor.ReadStreamAsync<T>(
            tableName,
            cmd =>
            {
                var sql = $"SELECT Data FROM {Qt(tableName)} WHERE TypeName = @typeName";
                sql += this.executor.TenantFilter ?? "";
                if (whereClause != null)
                    sql += $" AND ({whereClause})";
                sql += orderByClause + paginationClause + ";";
                cmd.CommandText = sql;
                AddParameter(cmd, "@typeName", typeName);
                this.executor.AddTenantParameter(cmd);
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

        return this.executor.ExecuteAsync(tableName, async session =>
        {
            await using var cmd = session.CreateCommand();
            var sql = $"SELECT COUNT(*) FROM {Qt(tableName)} WHERE TypeName = @typeName";
            sql += this.executor.TenantFilter ?? "";
            if (whereClause != null)
                sql += $" AND ({whereClause})";
            cmd.CommandText = sql + ";";
            AddParameter(cmd, "@typeName", typeName);
            this.executor.AddTenantParameter(cmd);
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

        return this.executor.ExecuteAsync(tableName, async session =>
        {
            await using var cmd = session.CreateCommand();
            var sql = $"SELECT CASE WHEN EXISTS(SELECT 1 FROM {Qt(tableName)} WHERE TypeName = @typeName";
            sql += this.executor.TenantFilter ?? "";
            if (whereClause != null)
                sql += $" AND ({whereClause})";
            cmd.CommandText = sql + ") THEN 1 ELSE 0 END;";
            AddParameter(cmd, "@typeName", typeName);
            this.executor.AddTenantParameter(cmd);
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

        return this.executor.ExecuteAsync(tableName, async session =>
        {
            await using var cmd = session.CreateCommand();
            var sql = $"DELETE FROM {Qt(tableName)} WHERE TypeName = @typeName";
            sql += this.executor.TenantFilter ?? "";
            if (whereClause != null)
                sql += $" AND ({whereClause})";
            cmd.CommandText = sql + ";";
            AddParameter(cmd, "@typeName", typeName);
            this.executor.AddTenantParameter(cmd);
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
        var provider = this.executor.Provider;

        return this.executor.ExecuteAsync(tableName, async session =>
        {
            await using var cmd = session.CreateCommand();
            var jsonSetExpr = provider.BuildJsonSetExpression();
            var sql = $"UPDATE {Qt(tableName)} SET Data = {jsonSetExpr}, UpdatedAt = @now WHERE TypeName = @typeName";
            sql += this.executor.TenantFilter ?? "";
            if (whereClause != null)
                sql += $" AND ({whereClause})";
            cmd.CommandText = sql + ";";
            AddParameter(cmd, "@path", "$." + jsonPath);
            AddParameter(cmd, "@value", provider.FormatPropertyValue(value));
            AddParameter(cmd, "@now", DateTimeOffset.UtcNow);
            AddParameter(cmd, "@typeName", typeName);
            this.executor.AddTenantParameter(cmd);
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

        return this.executor.ExecuteAsync(tableName, async session =>
        {
            await using var cmd = session.CreateCommand();
            var sql = $"SELECT AVG({provider.JsonExtractNumeric("Data", jsonPath)}) FROM {Qt(tableName)} WHERE TypeName = @typeName";
            sql += this.executor.TenantFilter ?? "";
            if (whereClause != null)
                sql += $" AND ({whereClause})";
            cmd.CommandText = sql + ";";
            AddParameter(cmd, "@typeName", typeName);
            this.executor.AddTenantParameter(cmd);
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
        var extract = provider.JsonExtractNumeric("Data", jsonPath);

        return this.executor.ExecuteAsync(tableName, async session =>
        {
            await using var cmd = session.CreateCommand();
            var sql = $"SELECT {sqlFunc}({extract}) FROM {Qt(tableName)} WHERE TypeName = @typeName";
            sql += this.executor.TenantFilter ?? "";
            if (whereClause != null)
                sql += $" AND ({whereClause})";
            cmd.CommandText = sql + ";";
            AddParameter(cmd, "@typeName", typeName);
            this.executor.AddTenantParameter(cmd);
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
        var effective = this.GetEffectivePredicates();
        if (effective.Count == 0)
            return (null, null);

        var typeInfo = RequireTypeInfo();
        var combined = CombinePredicates(effective);
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

    public Task<IReadOnlyList<VectorResult<T>>> NearestVectors(ReadOnlyMemory<float> query, int k, CancellationToken ct = default)
    {
        var effective = this.GetEffectivePredicates();
        Expression<Func<T, bool>>? filter = effective.Count == 0
            ? null
            : CombinePredicates(effective);
        return this.executor.NearestVectorsAsync<T>(query, k, filter, ct);
    }

    public IAsyncEnumerable<DocumentChange<T>> NotifyOnChange(CancellationToken ct = default)
    {
        var broadcaster = this.executor.Broadcaster
            ?? throw new NotSupportedException(
                "This document store does not support change observation (IObservableDocumentStore).");

        var effective = this.GetEffectivePredicates();
        Func<T, bool>? predicate = effective.Count == 0
            ? null
            : CombinePredicates(effective).Compile();

        return Filter(broadcaster.Observe<T>(ct), predicate, ct);
    }

    static async IAsyncEnumerable<DocumentChange<T>> Filter(
        IAsyncEnumerable<DocumentChange<T>> source,
        Func<T, bool>? predicate,
        [EnumeratorCancellation] CancellationToken ct)
    {
        await foreach (var change in source.WithCancellation(ct).ConfigureAwait(false))
        {
            // Property-level / removal / clear paths don't carry the document — pass through so
            // the consumer can re-query if it needs to test membership.
            if (predicate == null || change.Document is null)
            {
                yield return change;
                continue;
            }

            if (predicate(change.Document))
                yield return change;
        }
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
