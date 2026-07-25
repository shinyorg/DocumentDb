using System.Linq.Expressions;
using System.Text.Json.Nodes;
using System.Text.Json.Serialization.Metadata;

namespace Shiny.DocumentDb.Internal;

/// <summary>
/// Executes a runtime string projection (<see cref="DocumentQueryExtensions"/>-style sparse fieldset)
/// against the source query, returning each row as a <see cref="JsonObject"/>. The projection SQL is
/// precomputed by <see cref="DocumentQuery{T}.Project"/>; this type only carries the source filters,
/// ordering, and pagination and deserializes rows with <see cref="JsonNode.Parse(string, JsonNodeOptions?, System.Text.Json.JsonDocumentOptions)"/>
/// (reflection-free, AOT-safe).
/// </summary>
internal sealed class JsonProjectionDocumentQuery<TSource> : IDocumentQuery<JsonObject>
    where TSource : class
{
    readonly IQueryExecutor executor;
    readonly JsonTypeInfo<TSource> sourceTypeInfo;
    readonly List<Expression<Func<TSource, bool>>> wheres;
    readonly List<(Expression<Func<TSource, object>> Selector, bool IsDescending)> orderBys;
    readonly string projection;
    readonly Dictionary<string, object?>? projectionParams;
    readonly bool ignoreAllFilters;
    readonly HashSet<string>? ignoredFilterNames;
    int? paginateOffset;
    int? paginateTake;

    internal JsonProjectionDocumentQuery(
        IQueryExecutor executor,
        JsonTypeInfo<TSource> sourceTypeInfo,
        List<Expression<Func<TSource, bool>>> wheres,
        List<(Expression<Func<TSource, object>> Selector, bool IsDescending)> orderBys,
        string projection,
        Dictionary<string, object?>? projectionParams,
        int? paginateOffset,
        int? paginateTake,
        bool ignoreAllFilters,
        HashSet<string>? ignoredFilterNames)
    {
        this.executor = executor;
        this.sourceTypeInfo = sourceTypeInfo;
        this.wheres = wheres;
        this.orderBys = orderBys;
        this.projection = projection;
        this.projectionParams = projectionParams;
        this.paginateOffset = paginateOffset;
        this.paginateTake = paginateTake;
        this.ignoreAllFilters = ignoreAllFilters;
        this.ignoredFilterNames = ignoredFilterNames;
    }

    void BindProjectionParams(System.Data.Common.DbCommand cmd)
    {
        if (this.projectionParams != null)
            DocumentQuery<TSource>.BindDictionaryParameters(cmd, this.projectionParams);
    }

    // ── Operations not valid after Project ──────────────────────────

    public IDocumentQuery<JsonObject> Where(Expression<Func<JsonObject, bool>> predicate)
        => throw new NotSupportedException("Cannot modify query after Project.");

    public IDocumentQuery<JsonObject> IgnoreQueryFilters()
        => throw new NotSupportedException("Cannot call IgnoreQueryFilters after Project. Call it on the source query before projecting.");

    public IDocumentQuery<JsonObject> IgnoreQueryFilters(params string[] filterNames)
        => throw new NotSupportedException("Cannot call IgnoreQueryFilters after Project. Call it on the source query before projecting.");

    public IDocumentQuery<JsonObject> OrderBy(Expression<Func<JsonObject, object>> selector)
        => throw new NotSupportedException("Cannot modify query after Project.");

    public IDocumentQuery<JsonObject> OrderByDescending(Expression<Func<JsonObject, object>> selector)
        => throw new NotSupportedException("Cannot modify query after Project.");

    public IGroupedDocumentQuery<JsonObject, TKey> GroupBy<TKey>(Expression<Func<JsonObject, TKey>> keySelector)
        => throw new NotSupportedException("Cannot modify query after Project.");

    public IDocumentQuery<TResult> Select<TResult>(
        Expression<Func<JsonObject, TResult>> selector,
        JsonTypeInfo<TResult>? resultTypeInfo = null) where TResult : class
        => throw new NotSupportedException("Cannot apply Select after Project.");

    public Task<int> ExecuteDelete(CancellationToken ct = default)
        => throw new NotSupportedException("Cannot execute delete after Project.");

    public Task<int> ExecuteUpdate(Expression<Func<JsonObject, object>> property, object? value, CancellationToken ct = default)
        => throw new NotSupportedException("Cannot execute update after Project.");

    public Task<TValue> Max<TValue>(Expression<Func<JsonObject, TValue>> selector, CancellationToken ct = default)
        => throw new NotSupportedException("Cannot aggregate after Project.");

    public Task<TValue> Min<TValue>(Expression<Func<JsonObject, TValue>> selector, CancellationToken ct = default)
        => throw new NotSupportedException("Cannot aggregate after Project.");

    public Task<TValue> Sum<TValue>(Expression<Func<JsonObject, TValue>> selector, CancellationToken ct = default)
        => throw new NotSupportedException("Cannot aggregate after Project.");

    public Task<double> Average(Expression<Func<JsonObject, object>> selector, CancellationToken ct = default)
        => throw new NotSupportedException("Cannot aggregate after Project.");

    public IAsyncEnumerable<DocumentChange<JsonObject>> NotifyOnChange(CancellationToken ct = default)
        => throw new NotSupportedException("Cannot observe changes after Project.");

    // ── Supported operations ────────────────────────────────────────

    public IDocumentQuery<JsonObject> Paginate(int offset, int take)
    {
        this.paginateOffset = offset;
        this.paginateTake = take;
        return this;
    }

    public DocumentQueryString ToQueryString()
    {
        var (whereClause, whereParams) = this.BuildWhereClause();
        var sql = this.BuildSelectSql(whereClause);
        var typeName = this.executor.ResolveTypeName<TSource>();

        var parameters = new Dictionary<string, object?>(StringComparer.Ordinal) { ["@typeName"] = typeName };
        this.executor.CollectTenantParameter(parameters);
        if (whereParams != null)
            foreach (var kv in whereParams)
                parameters[kv.Key] = kv.Value;
        if (this.projectionParams != null)
            foreach (var kv in this.projectionParams)
                parameters[kv.Key] = kv.Value;

        return new DocumentQueryString(sql, parameters);
    }

    public Task<IReadOnlyList<JsonObject>> ToList(CancellationToken ct = default)
    {
        var (whereClause, whereParams) = this.BuildWhereClause();
        var sql = this.BuildSelectSql(whereClause);
        var typeName = this.executor.ResolveTypeName<TSource>();
        var tableName = this.executor.ResolveTableName<TSource>();

        return this.executor.ExecuteAsync(tableName, async session =>
        {
            await using var cmd = session.CreateCommand();
            cmd.CommandText = sql;
            DocumentQuery<TSource>.AddParameter(cmd, "@typeName", typeName);
            this.executor.AddTenantParameter(cmd);
            if (whereParams != null)
                DocumentQuery<TSource>.BindDictionaryParameters(cmd, whereParams);
            this.BindProjectionParams(cmd);

            this.executor.Logging?.Invoke(cmd.CommandText);
            return await DocumentQuery<TSource>.ReadListAsync(cmd, Deserialize, ct).ConfigureAwait(false);
        }, ct);
    }

    public IAsyncEnumerable<JsonObject> ToAsyncEnumerable(CancellationToken ct = default)
    {
        var (whereClause, whereParams) = this.BuildWhereClause();
        var sql = this.BuildSelectSql(whereClause);
        var typeName = this.executor.ResolveTypeName<TSource>();
        var tableName = this.executor.ResolveTableName<TSource>();

        return this.executor.ReadStreamAsync<JsonObject>(
            tableName,
            cmd =>
            {
                cmd.CommandText = sql;
                DocumentQuery<TSource>.AddParameter(cmd, "@typeName", typeName);
                this.executor.AddTenantParameter(cmd);
                if (whereParams != null)
                    DocumentQuery<TSource>.BindDictionaryParameters(cmd, whereParams);
                this.BindProjectionParams(cmd);
            },
            Deserialize,
            ct);
    }

    public Task<long> Count(CancellationToken ct = default)
    {
        var (whereClause, whereParams) = this.BuildWhereClause();
        var typeName = this.executor.ResolveTypeName<TSource>();
        var tableName = this.executor.ResolveTableName<TSource>();
        var qt = this.executor.Provider.QuoteTable(tableName);

        return this.executor.ExecuteAsync(tableName, async session =>
        {
            await using var cmd = session.CreateCommand();
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

    public Task<bool> Any(CancellationToken ct = default)
    {
        var (whereClause, whereParams) = this.BuildWhereClause();
        var typeName = this.executor.ResolveTypeName<TSource>();
        var tableName = this.executor.ResolveTableName<TSource>();
        var qt = this.executor.Provider.QuoteTable(tableName);

        return this.executor.ExecuteAsync(tableName, async session =>
        {
            await using var cmd = session.CreateCommand();
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

    // ── Helpers ─────────────────────────────────────────────────────

    static JsonObject Deserialize(string json)
        => JsonNode.Parse(json)?.AsObject() ?? new JsonObject();

    string BuildSelectSql(string? whereClause)
    {
        var tableName = this.executor.ResolveTableName<TSource>();
        var qt = this.executor.Provider.QuoteTable(tableName);

        var orderByClause = this.BuildOrderByClause();
        var paginationClause = this.paginateTake == null
            ? ""
            : " " + this.executor.Provider.BuildPaginationClause(this.paginateOffset!.Value, this.paginateTake.Value);

        // Some providers (e.g. SQL Server) require an ORDER BY for OFFSET/FETCH pagination.
        if (orderByClause == "" && paginationClause != "")
            orderByClause = " ORDER BY (SELECT NULL)";

        var sql = $"SELECT {this.projection} FROM {qt} WHERE TypeName = @typeName";
        sql += this.executor.TenantFilter ?? "";
        if (whereClause != null)
            sql += $" AND ({whereClause})";
        sql += orderByClause + paginationClause + ";";
        return sql;
    }

    string BuildOrderByClause()
    {
        if (this.orderBys.Count == 0)
            return "";

        var provider = this.executor.Provider;
        var parts = new List<string>(this.orderBys.Count);
        foreach (var (selector, isDescending) in this.orderBys)
        {
            var jsonPath = IndexExpressionHelper.ResolveJsonPath(selector, this.executor.JsonOptions, this.sourceTypeInfo);
            var direction = isDescending ? "DESC" : "ASC";
            parts.Add($"{provider.JsonExtract("Data", jsonPath)} {direction}");
        }
        return " ORDER BY " + string.Join(", ", parts);
    }

    (string? WhereClause, Dictionary<string, object?>? Parameters) BuildWhereClause()
    {
        var effective = this.GetEffectivePredicates();
        if (effective.Count == 0)
            return (null, null);

        var combined = DocumentQuery<TSource>.CombinePredicates(effective);
        var (clause, parms) = JsonExpressionVisitor.Translate(combined, this.sourceTypeInfo, this.executor.Provider);
        return (clause, parms);
    }

    List<Expression<Func<TSource, bool>>> GetEffectivePredicates()
    {
        var effective = new List<Expression<Func<TSource, bool>>>();
        if (!this.ignoreAllFilters)
        {
            foreach (var f in this.executor.Options.ResolveQueryFilters(typeof(TSource)))
            {
                if (f.Name != null && this.ignoredFilterNames?.Contains(f.Name) == true)
                    continue;
                effective.Add((Expression<Func<TSource, bool>>)f.Predicate);
            }
        }
        effective.AddRange(this.wheres);
        return effective;
    }
}
