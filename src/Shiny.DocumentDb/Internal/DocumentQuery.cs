using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;
using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;
using Shiny.DocumentDb.Internal.Query;

namespace Shiny.DocumentDb.Internal;

internal sealed class DocumentQuery<T> : IDocumentQuery<T>, IComputedAwareQuery where T : class
{
    readonly IQueryExecutor executor;
    readonly JsonTypeInfo<T>? jsonTypeInfo;
    readonly JsonSerializerOptions jsonOptions;
    readonly IReadOnlyDictionary<string, ComputedMapping>? computed;
    readonly IReadOnlyList<ComputedMapping> computedList;
    readonly List<Expression<Func<T, bool>>> wheres = [];
    readonly List<(Expression<Func<T, object>> Selector, bool IsDescending)> orderBys = [];
    int? paginateOffset;
    int? paginateTake;
    bool ignoreAllFilters;
    HashSet<string>? ignoredFilterNames;

    internal DocumentQuery(IQueryExecutor executor, JsonTypeInfo<T>? jsonTypeInfo)
    {
        this.executor = executor;
        this.jsonTypeInfo = jsonTypeInfo;
        this.jsonOptions = executor.JsonOptions;
        this.computed = executor.Options.ResolveComputedLookup(typeof(T));
        this.computedList = executor.Options.ResolveComputedMappings(typeof(T));
    }

    string Qt(string tableName) => this.executor.Provider.QuoteTable(tableName);

    public JsonTypeInfo<T>? QueryTypeInfo => this.jsonTypeInfo;

    public IReadOnlyDictionary<string, ComputedMapping>? ComputedLookup => this.computed;

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

    public IGroupedDocumentQuery<T, TKey> GroupBy<TKey>(Expression<Func<T, TKey>> keySelector)
        => new GroupedDocumentQuery<T, TKey>(
            this.executor,
            this.jsonTypeInfo,
            this.wheres,
            keySelector,
            this.computed,
            this.ignoreAllFilters,
            this.ignoredFilterNames);

    public IGroupedDocumentQuery<T, object> GroupBy(string keyField)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(keyField);
        return new StringGroupedDocumentQuery<T>(
            this.executor,
            this.RequireTypeInfo(),
            this.wheres,
            keyField,
            this.computed,
            this.ignoreAllFilters,
            this.ignoredFilterNames);
    }

    public IDocumentQuery<T> Paginate(int offset, int take)
    {
        this.paginateOffset = offset;
        this.paginateTake = take;
        return this;
    }

    // ── Cursor / keyset pagination ──────────────────────────────────────────
    const int MaxCursorTake = 10_000;

    public async Task<CursorPage<T>> ToCursorPage(string? cursor, int take, CancellationToken ct = default)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(take);
        if (take > MaxCursorTake)
            throw new ArgumentOutOfRangeException(nameof(take), take,
                $"Cursor page size must not exceed {MaxCursorTake}.");

        var typeInfo = RequireTypeInfo();
        var provider = this.executor.Provider;
        var keys = this.BuildCursorKeys(typeInfo);

        // The ordering (and its column SQL) is the cursor's identity — a cursor is only valid for the exact
        // shape that produced it. Emit each key's column SQL once and reuse it for both ORDER BY and the seek.
        var orderByParts = new List<string>(keys.Count);
        var specParts = new List<string>(keys.Count);
        Dictionary<string, object?>? orderParams = null;
        var idx = 0;
        foreach (var key in keys)
        {
            var (colSql, ps) = SqlPredicateEmitter.EmitValue(key.Column, provider, $"@co{idx}x");
            orderByParts.Add($"{colSql} {(key.Descending ? "DESC" : "ASC")}");
            specParts.Add($"{colSql}:{(key.Descending ? "d" : "a")}");
            if (ps.Count > 0)
            {
                orderParams ??= new Dictionary<string, object?>(StringComparer.Ordinal);
                foreach (var kv in ps)
                    orderParams[kv.Key] = kv.Value;
            }
            idx++;
        }
        var shapeHash = CursorCodec.ComputeShapeHash(this.executor.ResolveTypeName<T>() + "|" + string.Join("|", specParts));

        var (whereClause, whereParams) = BuildWhereClause();

        string? keysetClause = null;
        Dictionary<string, object?>? keysetParams = null;
        if (cursor != null)
        {
            var values = CursorCodec.DecodeKeyset(cursor, shapeHash);
            if (values.Count != keys.Count)
                throw new InvalidOperationException("The cursor is malformed or corrupt.");
            var predicate = BuildKeysetPredicate(keys, values);
            (keysetClause, keysetParams) = SqlPredicateEmitter.EmitPredicate(predicate, provider, "@ks");
        }

        var typeName = this.executor.ResolveTypeName<T>();
        var tableName = this.executor.ResolveTableName<T>();
        var limitClause = provider.BuildLimitClause(take + 1);
        var orderBy = " ORDER BY " + string.Join(", ", orderByParts);

        var rows = await this.executor.ExecuteAsync(tableName, async session =>
        {
            await using var cmd = session.CreateCommand();
            var sql = $"SELECT Data FROM {Qt(tableName)} WHERE TypeName = @typeName";
            sql += this.executor.TenantFilter ?? "";
            if (whereClause != null)
                sql += $" AND ({whereClause})";
            if (keysetClause != null)
                sql += $" AND ({keysetClause})";
            sql += orderBy + " " + limitClause + ";";
            cmd.CommandText = sql;
            AddParameter(cmd, "@typeName", typeName);
            this.executor.AddTenantParameter(cmd);
            if (whereParams != null)
                BindDictionaryParameters(cmd, whereParams);
            if (orderParams != null)
                BindDictionaryParameters(cmd, orderParams);
            if (keysetParams != null)
                BindDictionaryParameters(cmd, keysetParams);

            this.executor.Logging?.Invoke(cmd.CommandText);
            return await ReadListAsync(cmd, this.Deserialize, ct).ConfigureAwait(false);
        }, ct).ConfigureAwait(false);

        if (rows.Count <= take)
            return new CursorPage<T>(rows, null);

        // One extra row was fetched to detect "more" without a count; drop it and encode the cursor from the
        // last kept row's key values.
        var items = new List<T>(take);
        for (var i = 0; i < take; i++)
            items.Add(rows[i]);

        var last = items[take - 1];
        var cursorValues = new object?[keys.Count];
        for (var i = 0; i < keys.Count; i++)
            cursorValues[i] = keys[i].GetValue(last);

        return new CursorPage<T>(items, CursorCodec.EncodeKeyset(shapeHash, cursorValues));
    }

    readonly record struct CursorKey(ValueNode Column, bool Descending, Func<T, object?> GetValue);

    List<CursorKey> BuildCursorKeys(JsonTypeInfo<T> typeInfo)
    {
        var keys = new List<CursorKey>(this.orderBys.Count + 1);
        foreach (var (selector, isDescending) in this.orderBys)
        {
            var body = selector.Body;
            if (body is UnaryExpression { NodeType: ExpressionType.Convert } u)
                body = u.Operand;

            if (body is not MemberExpression)
                throw new NotSupportedException(
                    "Cursor pagination requires ordering by document properties (or computed properties). " +
                    "Ordering by a function such as Distance or a full-text score is not supported for cursors.");

            var column = ExpressionLowerer.LowerValue(body, this.jsonOptions, typeInfo, this.computed);
            var getter = ExpressionInterpreter.Interpret(selector);
            keys.Add(new CursorKey(column, isDescending, getter));
        }

        // Mandatory Id tiebreaker (ascending) so the total order is deterministic even when the sort key is
        // non-unique. Id is a real column (stored as text); reference it directly.
        var idAccessor = this.executor.GetIdAccessor(this.jsonTypeInfo);
        keys.Add(new CursorKey(
            new ComputedColumnNode("Id", typeof(string)),
            false,
            doc => idAccessor.GetIdAsString(doc)));
        return keys;
    }

    static PredicateNode BuildKeysetPredicate(IReadOnlyList<CursorKey> keys, IReadOnlyList<object?> values)
    {
        // OR-chain (portable, direction-correct — native tuple compare isn't available everywhere and is
        // wrong for mixed directions):
        //   (k0 OP0 v0) OR (k0=v0 AND k1 OP1 v1) OR (k0=v0 AND k1=v1 AND k2 OP2 v2) ...
        PredicateNode? result = null;
        for (var j = 0; j < keys.Count; j++)
        {
            var op = keys[j].Descending ? CompareOp.LessThan : CompareOp.GreaterThan;
            PredicateNode term = new CompareNode(op, keys[j].Column, new ConstantNode(values[j]));
            for (var i = 0; i < j; i++)
                term = new LogicalNode(LogicalOp.And,
                    new CompareNode(CompareOp.Equal, keys[i].Column, new ConstantNode(values[i])),
                    term);

            result = result == null ? term : new LogicalNode(LogicalOp.Or, result, term);
        }
        return result!;
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
        Dictionary<string, object?>? projectionParams = null;
        var fnIndex = 0;

        foreach (var item in FilterExpressionParser.ParseProjection(fields, typeInfo, this.computed).Items)
        {
            string alias;
            string valueSql;

            if (item.FieldPath != null && this.computed != null && this.computed.TryGetValue(item.FieldPath, out var computedMapping))
            {
                alias = item.Alias ?? computedMapping.JsonName;
                if (computedMapping.MaterializedColumn != null)
                {
                    // Materialized → project the real column.
                    valueSql = computedMapping.MaterializedColumn;
                }
                else
                {
                    // Alias → inline its definition as the projected value.
                    var node = ExpressionLowerer.LowerValue(computedMapping.Definition.Body, typeInfo.Options, typeInfo, this.computed);
                    var (sql, ps) = SqlPredicateEmitter.EmitValue(node, provider, $"@j{fnIndex++}x");
                    valueSql = sql;
                    if (ps.Count > 0)
                    {
                        projectionParams ??= new Dictionary<string, object?>();
                        foreach (var kv in ps)
                            projectionParams[kv.Key] = kv.Value;
                    }
                }
            }
            else if (item.FieldPath != null)
            {
                // Plain document path → json_extract; output key defaults to the leaf JSON name.
                var (jsonPath, leafJsonName) = DocumentQueryExtensions.ResolveJsonPath(item.FieldPath, typeInfo);
                alias = item.Alias ?? leafJsonName;
                valueSql = provider.JsonExtract("Data", jsonPath);
            }
            else
            {
                // Scalar function → lower to the shared value IR and emit via the dialect (with a distinct
                // parameter prefix so its @jNx params don't collide with the WHERE clause's @p params).
                alias = item.Alias!;
                var node = ExpressionLowerer.LowerValue(item.ValueExpr!, typeInfo.Options, typeInfo);
                var (sql, ps) = SqlPredicateEmitter.EmitValue(node, provider, $"@j{fnIndex++}x");
                valueSql = sql;
                if (ps.Count > 0)
                {
                    projectionParams ??= new Dictionary<string, object?>();
                    foreach (var kv in ps)
                        projectionParams[kv.Key] = kv.Value;
                }
            }

            if (!seen.Add(alias))
                throw new ArgumentException(
                    $"Projection resolves to duplicate output key '{alias}'. Projected fields must have unique names.",
                    nameof(fields));

            pairs.Add($"'{alias}'");
            pairs.Add(valueSql);
        }

        if (pairs.Count == 0)
            throw new ArgumentException("At least one field must be specified.", nameof(fields));

        return new JsonProjectionDocumentQuery<T>(
            this.executor,
            typeInfo,
            this.wheres,
            this.orderBys,
            provider.JsonObject(pairs),
            projectionParams,
            this.paginateOffset,
            this.paginateTake,
            this.ignoreAllFilters,
            this.ignoredFilterNames);
    }

    public DocumentQueryString ToQueryString()
    {
        var (whereClause, whereParams) = BuildWhereClause();
        var (orderByClause, orderByParams) = BuildOrderByClause();
        var paginationClause = BuildPaginationClause();
        if (orderByClause == "" && paginationClause != "")
            orderByClause = " ORDER BY (SELECT NULL)";
        var typeName = this.executor.ResolveTypeName<T>();
        var tableName = this.executor.ResolveTableName<T>();

        var sql = $"SELECT Data FROM {Qt(tableName)} WHERE TypeName = @typeName";
        sql += this.executor.TenantFilter ?? "";
        if (whereClause != null)
            sql += $" AND ({whereClause})";
        sql += orderByClause + paginationClause + ";";

        var parameters = new Dictionary<string, object?>(StringComparer.Ordinal) { ["@typeName"] = typeName };
        this.executor.CollectTenantParameter(parameters);
        if (whereParams != null)
            foreach (var kv in whereParams)
                parameters[kv.Key] = kv.Value;
        if (orderByParams != null)
            foreach (var kv in orderByParams)
                parameters[kv.Key] = kv.Value;

        return new DocumentQueryString(sql, parameters);
    }

    public Task<IReadOnlyList<T>> ToList(CancellationToken ct = default)
    {
        var (whereClause, whereParams) = BuildWhereClause();
        var (orderByClause, orderByParams) = BuildOrderByClause();
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
            if (orderByParams != null)
                BindDictionaryParameters(cmd, orderByParams);

            this.executor.Logging?.Invoke(cmd.CommandText);
            return await ReadListAsync(cmd, this.Deserialize, ct).ConfigureAwait(false);
        }, ct);
    }

    public IAsyncEnumerable<T> ToAsyncEnumerable(CancellationToken ct = default)
    {
        var (whereClause, whereParams) = BuildWhereClause();
        var (orderByClause, orderByParams) = BuildOrderByClause();
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
                if (orderByParams != null)
                    BindDictionaryParameters(cmd, orderByParams);
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

    public async Task<int> ExecuteDelete(CancellationToken ct = default)
    {
        var (whereClause, whereParams) = BuildWhereClause();
        var typeName = this.executor.ResolveTypeName<T>();
        var tableName = this.executor.ResolveTableName<T>();

        var interceptors = this.executor.Options.Interceptors;
        var bulkCtx = interceptors.NewBulk<T>(DocumentOperation.Delete, typeName, whereClause);
        await interceptors.BeforeBulk(bulkCtx, ct).ConfigureAwait(false);

        var affected = await this.executor.ExecuteAsync(tableName, async session =>
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
        }, ct).ConfigureAwait(false);

        await interceptors.AfterBulk(bulkCtx, affected, ct).ConfigureAwait(false);
        return affected;
    }

    public async Task<int> ExecuteUpdate(Expression<Func<T, object>> property, object? value, CancellationToken ct = default)
    {
        var typeInfo = this.RequireTypeInfo();
        var jsonPath = IndexExpressionHelper.ResolveJsonPath(property, this.jsonOptions, typeInfo);
        var (whereClause, whereParams) = BuildWhereClause();
        var typeName = this.executor.ResolveTypeName<T>();
        var tableName = this.executor.ResolveTableName<T>();
        var provider = this.executor.Provider;

        var interceptors = this.executor.Options.Interceptors;
        var bulkCtx = interceptors.NewBulk<T>(DocumentOperation.Update, typeName, whereClause, (jsonPath, value));
        await interceptors.BeforeBulk(bulkCtx, ct).ConfigureAwait(false);

        var affected = await this.executor.ExecuteAsync(tableName, async session =>
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
        }, ct).ConfigureAwait(false);

        await interceptors.AfterBulk(bulkCtx, affected, ct).ConfigureAwait(false);
        return affected;
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
        var (ftType, ftMap) = this.ResolveFullText();
        var (clause, parms) = JsonExpressionVisitor.Translate(combined, typeInfo, this.executor.Provider, this.executor.Options.FunctionRegistry, this.computed, this.executor.ResolveTableName<T>(), ftType, ftMap);
        return (clause, parms);
    }

    // Full-text (LuceneMatch/LuceneScore) needs the type's mapping + resolved type name to build the native
    // index predicate; null when the type isn't full-text mapped (the emitter then throws a clear error).
    (string? TypeName, FullTextMapping? Mapping) ResolveFullText()
    {
        var mapping = this.executor.Options.ResolveFullTextMapping(typeof(T));
        return mapping is null ? (null, null) : (this.executor.ResolveTypeName<T>(), mapping);
    }

    string BuildPaginationClause()
    {
        if (this.paginateTake == null)
            return "";

        return " " + this.executor.Provider.BuildPaginationClause(this.paginateOffset!.Value, this.paginateTake.Value);
    }

    (string Clause, Dictionary<string, object?>? Parameters) BuildOrderByClause()
    {
        if (this.orderBys.Count == 0)
            return ("", null);

        var typeInfo = RequireTypeInfo();
        var provider = this.executor.Provider;
        var parts = new List<string>(this.orderBys.Count);
        Dictionary<string, object?>? parameters = null;
        var idx = 0;
        foreach (var (selector, isDescending) in this.orderBys)
        {
            var direction = isDescending ? "DESC" : "ASC";
            if (this.computed != null && TryGetComputedSelector(selector, out var mapping))
            {
                if (mapping.MaterializedColumn != null)
                {
                    // Materialized → sort by the real column (index-served when indexed).
                    parts.Add($"{mapping.MaterializedColumn} {direction}");
                }
                else
                {
                    // Alias → inline its definition as the sort key (with a distinct @oNx parameter prefix
                    // so any literals don't collide with the WHERE clause's @p params).
                    var node = ExpressionLowerer.LowerValue(mapping.Definition.Body, this.jsonOptions, typeInfo, this.computed);
                    var (sql, ps) = SqlPredicateEmitter.EmitValue(node, provider, $"@o{idx}x");
                    parts.Add($"{sql} {direction}");
                    if (ps.Count > 0)
                    {
                        parameters ??= new Dictionary<string, object?>(StringComparer.Ordinal);
                        foreach (var kv in ps)
                            parameters[kv.Key] = kv.Value;
                    }
                }
            }
            else
            {
                var body = selector.Body;
                if (body is UnaryExpression { NodeType: ExpressionType.Convert } u)
                    body = u.Operand;

                // A method-call selector (e.g. DocumentFunctions.Distance(...)) lowers to a value expression.
                if (body is MethodCallExpression)
                {
                    var node = ExpressionLowerer.LowerValue(body, this.jsonOptions, typeInfo, this.computed);
                    var (ftType, ftMap) = this.ResolveFullText();
                    var (sql, ps) = SqlPredicateEmitter.EmitValue(node, provider, $"@o{idx}x", this.executor.ResolveTableName<T>(), ftType, ftMap);
                    parts.Add($"{sql} {direction}");
                    if (ps.Count > 0)
                    {
                        parameters ??= new Dictionary<string, object?>(StringComparer.Ordinal);
                        foreach (var kv in ps)
                            parameters[kv.Key] = kv.Value;
                    }
                }
                else
                {
                    var jsonPath = IndexExpressionHelper.ResolveJsonPath(selector, this.jsonOptions, typeInfo);
                    parts.Add($"{provider.JsonExtract("Data", jsonPath)} {direction}");
                }
            }
            idx++;
        }
        return (" ORDER BY " + string.Join(", ", parts), parameters);
    }

    bool TryGetComputedSelector(Expression<Func<T, object>> selector, out ComputedMapping mapping)
    {
        mapping = null!;
        var body = selector.Body;
        if (body is UnaryExpression { NodeType: ExpressionType.Convert } unary)
            body = unary.Operand;

        if (body is MemberExpression { Expression: ParameterExpression } member
            && this.computed!.TryGetValue(member.Member.Name, out var found))
        {
            mapping = found;
            return true;
        }
        return false;
    }

    [UnconditionalSuppressMessage("Trimming", "IL2026", Justification = "Reflection path is only reached when jsonTypeInfo is null (reflection fallback).")]
    [UnconditionalSuppressMessage("AOT", "IL3050", Justification = "Reflection path is only reached when jsonTypeInfo is null (reflection fallback).")]
    T Deserialize(string json)
    {
        var document = this.jsonTypeInfo != null
            ? JsonSerializer.Deserialize(json, this.jsonTypeInfo)!
            : JsonSerializer.Deserialize<T>(json, this.jsonOptions)!;

        // Populate computed (JsonIgnore'd) properties on read so the round-tripped object is complete.
        for (var i = 0; i < this.computedList.Count; i++)
        {
            var mapping = this.computedList[i];
            mapping.SetValue(document, mapping.Compute(document));
        }
        return document;
    }

    public Task<IReadOnlyList<VectorResult<T>>> NearestVectors(ReadOnlyMemory<float> query, int k, CancellationToken ct = default)
    {
        var effective = this.GetEffectivePredicates();
        Expression<Func<T, bool>>? filter = effective.Count == 0
            ? null
            : CombinePredicates(effective);
        return this.executor.NearestVectorsAsync<T>(query, k, filter, ct);
    }

    public Task<IReadOnlyList<FullTextResult<T>>> FullTextMatch(string searchText, int maxResults = 50, CancellationToken ct = default)
    {
        var effective = this.GetEffectivePredicates();
        Expression<Func<T, bool>>? filter = effective.Count == 0
            ? null
            : CombinePredicates(effective);
        return this.executor.FullTextSearchAsync<T>(searchText, maxResults, filter, ct);
    }

    public IAsyncEnumerable<DocumentChange<T>> NotifyOnChange(CancellationToken ct = default)
    {
        var broadcaster = this.executor.Broadcaster
            ?? throw new NotSupportedException(
                "This document store does not support change observation (IObservableDocumentStore).");

        var effective = this.GetEffectivePredicates();
        Func<T, bool>? predicate = effective.Count == 0
            ? null
            : ExpressionInterpreter.Interpret(CombinePredicates(effective));

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
