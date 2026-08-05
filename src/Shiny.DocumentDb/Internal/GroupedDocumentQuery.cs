using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.Json.Serialization.Metadata;

namespace Shiny.DocumentDb.Internal;

/// <summary>
/// The typed builder returned by <see cref="DocumentQuery{T}.GroupBy{TKey}"/>. Captures the group key
/// and any <see cref="Having"/> predicates, then produces an executable query when <see cref="Select"/>
/// is called.
/// </summary>
internal sealed class GroupedDocumentQuery<T, TKey> : IGroupedDocumentQuery<T, TKey>
    where T : class
{
    readonly IQueryExecutor executor;
    readonly JsonTypeInfo<T>? sourceTypeInfo;
    readonly List<Expression<Func<T, bool>>> wheres;
    readonly Expression<Func<T, TKey>> keySelector;
    readonly IReadOnlyDictionary<string, ComputedMapping>? computed;
    readonly bool ignoreAllFilters;
    readonly HashSet<string>? ignoredFilterNames;
    readonly List<Expression<Func<IDocumentGroup<TKey, T>, bool>>> havings = [];

    internal GroupedDocumentQuery(
        IQueryExecutor executor,
        JsonTypeInfo<T>? sourceTypeInfo,
        List<Expression<Func<T, bool>>> wheres,
        Expression<Func<T, TKey>> keySelector,
        IReadOnlyDictionary<string, ComputedMapping>? computed,
        bool ignoreAllFilters,
        HashSet<string>? ignoredFilterNames)
    {
        this.executor = executor;
        this.sourceTypeInfo = sourceTypeInfo;
        this.wheres = wheres;
        this.keySelector = keySelector;
        this.computed = computed;
        this.ignoreAllFilters = ignoreAllFilters;
        this.ignoredFilterNames = ignoredFilterNames;
    }

    public IGroupedDocumentQuery<T, TKey> Having(Expression<Func<IDocumentGroup<TKey, T>, bool>> predicate)
    {
        this.havings.Add(predicate);
        return this;
    }

    public IDocumentQuery<TResult> Select<TResult>(
        Expression<Func<IDocumentGroup<TKey, T>, TResult>> selector,
        JsonTypeInfo<TResult>? resultTypeInfo = null) where TResult : class
    {
        var srcTypeInfo = this.RequireSourceTypeInfo();
        return new GroupedExecutable<T, TResult>(
            this.executor,
            srcTypeInfo,
            this.wheres,
            this.computed,
            this.ignoreAllFilters,
            this.ignoredFilterNames,
            resultTypeInfo,
            provider => GroupedAggregateTranslator.Translate(
                selector, this.keySelector, this.havings, srcTypeInfo,
                resultTypeInfo ?? throw ResultTypeInfoRequired<TResult>(), provider, this.computed),
            clr => resultTypeInfo != null
                ? JsonPropertyNameResolver.ResolveProperty(resultTypeInfo, clr).JsonName
                : clr);
    }

    // The string projection surface belongs to the string GroupBy("field") query.
    public IDocumentQuery<JsonObject> Project(string fields, JsonTypeInfo<T>? jsonTypeInfo = null)
        => throw new NotSupportedException(
            "String Project is only available on a string GroupBy(\"field\") query. Use the typed Select " +
            "with this typed GroupBy, or start from store.Query<T>().GroupBy(\"field\").");

    JsonTypeInfo<T> RequireSourceTypeInfo()
        => this.sourceTypeInfo ?? throw new InvalidOperationException(
            $"GroupBy requires a JsonTypeInfo<{typeof(T).Name}>. Use the Query<T>(JsonTypeInfo<T>) overload.");

    internal static InvalidOperationException ResultTypeInfoRequired<TResult>()
        => new($"This operation requires a JsonTypeInfo<{typeof(TResult).Name}>. Pass it to the Select() call.");
}

/// <summary>
/// The string-grammar builder returned by <see cref="DocumentQuery{T}.GroupBy(string)"/>. Groups by a
/// named JSON field and exposes the string projection surface (<see cref="Project"/> / <see cref="Having"/>).
/// </summary>
internal sealed class StringGroupedDocumentQuery<T> : IGroupedDocumentQuery<T, object>
    where T : class
{
    readonly IQueryExecutor executor;
    readonly JsonTypeInfo<T>? typeInfo;
    readonly List<Expression<Func<T, bool>>> wheres;
    readonly string keyField;
    // Set when grouping a JSON collection: names come from the collection rather than from T, and the field
    // metadata (which may describe a different type than T, or be absent entirely) comes in separately.
    readonly string? boundTypeName;
    readonly string? boundTableName;
    readonly JsonTypeInfo? boundFieldTypeInfo;
    readonly IReadOnlyDictionary<string, ComputedMapping>? computed;
    readonly bool ignoreAllFilters;
    readonly HashSet<string>? ignoredFilterNames;
    readonly List<string> havingStrings = [];

    internal StringGroupedDocumentQuery(
        IQueryExecutor executor,
        JsonTypeInfo<T>? typeInfo,
        List<Expression<Func<T, bool>>> wheres,
        string keyField,
        IReadOnlyDictionary<string, ComputedMapping>? computed,
        bool ignoreAllFilters,
        HashSet<string>? ignoredFilterNames,
        string? boundTypeName = null,
        string? boundTableName = null,
        JsonTypeInfo? boundFieldTypeInfo = null)
    {
        this.executor = executor;
        this.typeInfo = typeInfo;
        this.boundTypeName = boundTypeName;
        this.boundTableName = boundTableName;
        this.boundFieldTypeInfo = boundFieldTypeInfo;
        this.wheres = wheres;
        this.keyField = keyField;
        this.computed = computed;
        this.ignoreAllFilters = ignoreAllFilters;
        this.ignoredFilterNames = ignoredFilterNames;
    }

    public IGroupedDocumentQuery<T, object> Having(string predicate)
    {
        this.havingStrings.Add(predicate);
        return this;
    }

    public IGroupedDocumentQuery<T, object> Having(Expression<Func<IDocumentGroup<object, T>, bool>> predicate)
        => throw new NotSupportedException("Use the string Having(\"…\") overload with a string GroupBy(\"field\") query.");

    public IDocumentQuery<TResult> Select<TResult>(
        Expression<Func<IDocumentGroup<object, T>, TResult>> selector,
        JsonTypeInfo<TResult>? resultTypeInfo = null) where TResult : class
        => throw new NotSupportedException(
            "Use the string Project(\"…\") overload with a string GroupBy(\"field\") query, or start from " +
            "the typed GroupBy(keySelector) for a typed Select.");

    public IDocumentQuery<JsonObject> Project(string fields, JsonTypeInfo<T>? jsonTypeInfo = null)
    {
        var info = jsonTypeInfo ?? this.typeInfo;
        // Three cases, one executable: a typed query resolves paths through JsonTypeInfo<T>; a type-keyed
        // JSON collection through the document's (non-generic) metadata; a schema-free one takes them literally.
        var fieldInfo = this.boundFieldTypeInfo;
        return new GroupedExecutable<T, JsonObject>(
            this.executor,
            info,
            this.wheres,
            this.computed,
            this.ignoreAllFilters,
            this.ignoredFilterNames,
            null,
            provider => info != null
                ? GroupStringTranslator.Translate(fields, this.havingStrings, this.keyField, info, provider, this.computed)
                : fieldInfo != null
                    ? GroupStringTranslator.TranslateJson(fields, this.havingStrings, this.keyField, fieldInfo, provider)
                    : GroupStringTranslator.TranslateDynamic(fields, this.havingStrings, this.keyField, provider),
            clr => clr,
            this.boundTypeName,
            this.boundTableName,
            fieldInfo);
    }
}

/// <summary>
/// The executable form of a grouped query, shared by the typed and string surfaces. Emits
/// <c>SELECT &lt;json&gt; … GROUP BY … [HAVING …]</c> from a <paramref name="planFactory"/> and materializes
/// one row per group; post-projection <c>OrderBy</c> sorts by an output column and <c>Paginate</c> pages
/// the group rows.
/// </summary>
internal sealed class GroupedExecutable<TSource, TResult> : IDocumentQuery<TResult>
    where TSource : class
    where TResult : class
{
    readonly IQueryExecutor executor;
    readonly JsonTypeInfo<TSource>? sourceTypeInfo;
    readonly List<Expression<Func<TSource, bool>>> wheres;
    readonly string? boundTypeName;
    readonly string? boundTableName;
    readonly JsonTypeInfo? boundFieldTypeInfo;
    readonly IReadOnlyDictionary<string, ComputedMapping>? computed;
    readonly bool ignoreAllFilters;
    readonly HashSet<string>? ignoredFilterNames;
    readonly JsonTypeInfo<TResult>? resultTypeInfo;
    readonly Func<IDatabaseProvider, GroupedAggregateTranslator.Result> planFactory;
    readonly Func<string, string> resolveOrderKey;
    readonly List<(string OutputKey, bool IsDescending)> orderBys = [];
    int? paginateOffset;
    int? paginateTake;

    internal GroupedExecutable(
        IQueryExecutor executor,
        JsonTypeInfo<TSource>? sourceTypeInfo,
        List<Expression<Func<TSource, bool>>> wheres,
        IReadOnlyDictionary<string, ComputedMapping>? computed,
        bool ignoreAllFilters,
        HashSet<string>? ignoredFilterNames,
        JsonTypeInfo<TResult>? resultTypeInfo,
        Func<IDatabaseProvider, GroupedAggregateTranslator.Result> planFactory,
        Func<string, string> resolveOrderKey,
        string? boundTypeName = null,
        string? boundTableName = null,
        JsonTypeInfo? boundFieldTypeInfo = null)
    {
        this.executor = executor;
        this.sourceTypeInfo = sourceTypeInfo;
        this.boundTypeName = boundTypeName;
        this.boundTableName = boundTableName;
        this.boundFieldTypeInfo = boundFieldTypeInfo;
        this.wheres = wheres;
        this.computed = computed;
        this.ignoreAllFilters = ignoreAllFilters;
        this.ignoredFilterNames = ignoredFilterNames;
        this.resultTypeInfo = resultTypeInfo;
        this.planFactory = planFactory;
        this.resolveOrderKey = resolveOrderKey;
    }

    string TypeName => this.boundTypeName ?? this.executor.ResolveTypeName<TSource>();
    string TableName => this.boundTableName ?? this.executor.ResolveTableName<TSource>();

    public IDocumentQuery<TResult> OrderBy(Expression<Func<TResult, object>> selector)
        => this.AddOrderBy(selector, isDescending: false);

    public IDocumentQuery<TResult> OrderByDescending(Expression<Func<TResult, object>> selector)
        => this.AddOrderBy(selector, isDescending: true);

    IDocumentQuery<TResult> AddOrderBy(Expression<Func<TResult, object>> selector, bool isDescending)
    {
        this.orderBys.Add((this.resolveOrderKey(ResolveMemberName(selector.Body)), isDescending));
        return this;
    }

    public IDocumentQuery<TResult> Paginate(int offset, int take)
    {
        this.paginateOffset = offset;
        this.paginateTake = take;
        return this;
    }

    public Task<IReadOnlyList<TResult>> ToList(CancellationToken ct = default)
    {
        var (sql, parameters) = this.BuildSql();
        var tableName = this.TableName;
        return this.executor.ExecuteAsync(tableName, async session =>
        {
            await using var cmd = session.CreateCommand();
            cmd.CommandText = sql + ";";
            DocumentQuery<TSource>.BindDictionaryParameters(cmd, parameters);
            this.executor.Logging?.Invoke(cmd.CommandText);
            return await DocumentQuery<TSource>.ReadListAsync(cmd, this.DeserializeResult, ct).ConfigureAwait(false);
        }, ct);
    }

    public IAsyncEnumerable<TResult> ToAsyncEnumerable(CancellationToken ct = default)
    {
        var (sql, parameters) = this.BuildSql();
        var tableName = this.TableName;
        return this.executor.ReadStreamAsync<TResult>(
            tableName,
            cmd =>
            {
                cmd.CommandText = sql + ";";
                DocumentQuery<TSource>.BindDictionaryParameters(cmd, parameters);
            },
            this.DeserializeResult,
            ct);
    }

    public DocumentQueryString ToQueryString()
    {
        var (sql, parameters) = this.BuildSql();
        return new DocumentQueryString(sql + ";", parameters);
    }

    public Task<long> Count(CancellationToken ct = default)
    {
        // Number of groups: a derived table of one named column per group, counted.
        var (sql, parameters) = this.BuildCountSql();
        var tableName = this.TableName;
        return this.executor.ExecuteAsync(tableName, async session =>
        {
            await using var cmd = session.CreateCommand();
            cmd.CommandText = sql;
            DocumentQuery<TSource>.BindDictionaryParameters(cmd, parameters);
            this.executor.Logging?.Invoke(cmd.CommandText);
            var result = await cmd.ExecuteScalarAsync(ct).ConfigureAwait(false);
            return Convert.ToInt64(result);
        }, ct);
    }

    public async Task<bool> Any(CancellationToken ct = default) => await this.Count(ct).ConfigureAwait(false) > 0;

    // Groups as a derived table with one named column, wrapped in COUNT(*). Uses a bare column list so the
    // derived table is valid across dialects (SQL Server requires named columns; Oracle rejects "AS" on the alias).
    (string Sql, Dictionary<string, object?> Parameters) BuildCountSql()
    {
        var provider = this.executor.Provider;
        var plan = this.planFactory(provider);
        var (whereClause, whereParams) = this.BuildWhereClause();
        var typeName = this.TypeName;
        var qt = provider.QuoteTable(this.TableName);

        var inner = $"SELECT NULL AS c FROM {qt} WHERE TypeName = @typeName";
        inner += this.executor.TenantFilter ?? "";
        if (whereClause != null)
            inner += $" AND ({whereClause})";
        inner += $" GROUP BY {plan.GroupByClause}";
        if (plan.HavingClause != null)
            inner += $" HAVING {plan.HavingClause}";

        var parameters = new Dictionary<string, object?>(StringComparer.Ordinal) { ["@typeName"] = typeName };
        this.executor.CollectTenantParameter(parameters);
        if (whereParams != null)
            foreach (var kv in whereParams)
                parameters[kv.Key] = kv.Value;
        foreach (var kv in plan.Parameters)
            parameters[kv.Key] = kv.Value;

        return ($"SELECT COUNT(*) FROM ({inner}) g;", parameters);
    }

    (string Sql, Dictionary<string, object?> Parameters) BuildSql()
    {
        var provider = this.executor.Provider;
        var plan = this.planFactory(provider);

        var (whereClause, whereParams) = this.BuildWhereClause();
        var typeName = this.TypeName;
        var tableName = this.TableName;
        var qt = provider.QuoteTable(tableName);

        var sql = $"SELECT {plan.SelectClause} FROM {qt} WHERE TypeName = @typeName";
        sql += this.executor.TenantFilter ?? "";
        if (whereClause != null)
            sql += $" AND ({whereClause})";
        sql += $" GROUP BY {plan.GroupByClause}";
        if (plan.HavingClause != null)
            sql += $" HAVING {plan.HavingClause}";

        var orderByClause = this.BuildOrderByClause(plan.OutputColumns);
        var paginationClause = this.paginateTake == null
            ? ""
            : " " + provider.BuildPaginationClause(this.paginateOffset!.Value, this.paginateTake.Value);
        if (orderByClause == "" && paginationClause != "")
            orderByClause = " ORDER BY (SELECT NULL)";
        sql += orderByClause + paginationClause;

        var parameters = new Dictionary<string, object?>(StringComparer.Ordinal) { ["@typeName"] = typeName };
        this.executor.CollectTenantParameter(parameters);
        if (whereParams != null)
            foreach (var kv in whereParams)
                parameters[kv.Key] = kv.Value;
        foreach (var kv in plan.Parameters)
            parameters[kv.Key] = kv.Value;

        return (sql, parameters);
    }

    string BuildOrderByClause(IReadOnlyDictionary<string, string> outputColumns)
    {
        if (this.orderBys.Count == 0)
            return "";

        var parts = new List<string>(this.orderBys.Count);
        foreach (var (outputKey, isDescending) in this.orderBys)
        {
            if (!outputColumns.TryGetValue(outputKey, out var columnSql))
                throw new InvalidOperationException(
                    $"Cannot order a grouped query by '{outputKey}' — it is not one of the projected output columns.");
            parts.Add($"{columnSql} {(isDescending ? "DESC" : "ASC")}");
        }
        return " ORDER BY " + string.Join(", ", parts);
    }

    (string? WhereClause, Dictionary<string, object?>? Parameters) BuildWhereClause()
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
        if (effective.Count == 0)
            return (null, null);

        var combined = DocumentQuery<TSource>.CombinePredicates(effective);
        return JsonExpressionVisitor.Translate(
            combined, this.executor.JsonOptions, (JsonTypeInfo?)this.sourceTypeInfo ?? this.boundFieldTypeInfo,
            this.executor.Provider, this.executor.Options.FunctionRegistry, this.computed, this.TableName,
            spatialPaths: this.sourceTypeInfo == null ? null : this.executor.Options.Mappings.SpatialJsonPathsFor(typeof(TSource)));
    }

    static string ResolveMemberName(Expression body)
    {
        if (body is UnaryExpression { NodeType: ExpressionType.Convert } u)
            body = u.Operand;
        if (body is MemberExpression { Expression: ParameterExpression } m)
            return m.Member.Name;
        throw new ArgumentException("Order-by selector must be a simple output property, e.g. r => r.Revenue.");
    }

    [UnconditionalSuppressMessage("Trimming", "IL2026", Justification = "Reflection path only used when resultTypeInfo is null (reflection fallback).")]
    [UnconditionalSuppressMessage("AOT", "IL3050", Justification = "Reflection path only used when resultTypeInfo is null (reflection fallback).")]
    TResult DeserializeResult(string json)
    {
        if (typeof(TResult) == typeof(JsonObject))
            return (TResult)(object)(JsonNode.Parse(json)!.AsObject());
        return this.resultTypeInfo != null
            ? JsonSerializer.Deserialize(json, this.resultTypeInfo)!
            : JsonSerializer.Deserialize<TResult>(json, this.executor.JsonOptions)!;
    }

    // ── Not valid after a grouped projection ──
    const string AfterGroup = "Cannot modify a grouped query after its projection.";
    public IDocumentQuery<TResult> Where(Expression<Func<TResult, bool>> predicate) => throw new InvalidOperationException(AfterGroup);
    public IDocumentQuery<TResult> IgnoreQueryFilters() => throw new InvalidOperationException(AfterGroup);
    public IDocumentQuery<TResult> IgnoreQueryFilters(params string[] filterNames) => throw new InvalidOperationException(AfterGroup);
    public IGroupedDocumentQuery<TResult, TNewKey> GroupBy<TNewKey>(Expression<Func<TResult, TNewKey>> keySelector) => throw new InvalidOperationException(AfterGroup);
    public IDocumentQuery<TNewResult> Select<TNewResult>(Expression<Func<TResult, TNewResult>> selector, JsonTypeInfo<TNewResult>? resultTypeInfo = null) where TNewResult : class => throw new InvalidOperationException(AfterGroup);
    public IDocumentQuery<JsonObject> Project(string fields, JsonTypeInfo<TResult>? jsonTypeInfo = null) => throw new InvalidOperationException(AfterGroup);
    public Task<int> ExecuteDelete(CancellationToken ct = default) => throw new InvalidOperationException(AfterGroup);
    public Task<int> ExecuteUpdate(Expression<Func<TResult, object>> property, object? value, CancellationToken ct = default) => throw new InvalidOperationException(AfterGroup);
    public Task<TValue> Max<TValue>(Expression<Func<TResult, TValue>> selector, CancellationToken ct = default) => throw new InvalidOperationException("Aggregate terminals are not supported after a grouped projection.");
    public Task<TValue> Min<TValue>(Expression<Func<TResult, TValue>> selector, CancellationToken ct = default) => throw new InvalidOperationException("Aggregate terminals are not supported after a grouped projection.");
    public Task<TValue> Sum<TValue>(Expression<Func<TResult, TValue>> selector, CancellationToken ct = default) => throw new InvalidOperationException("Aggregate terminals are not supported after a grouped projection.");
    public Task<double> Average(Expression<Func<TResult, object>> selector, CancellationToken ct = default) => throw new InvalidOperationException("Aggregate terminals are not supported after a grouped projection.");
    public IAsyncEnumerable<DocumentChange<TResult>> NotifyOnChange(CancellationToken ct = default) => throw new InvalidOperationException("NotifyOnChange is not supported after a grouped projection.");
}
