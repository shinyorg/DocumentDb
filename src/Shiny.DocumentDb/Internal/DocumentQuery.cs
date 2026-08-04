using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;
using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Text.Json.Nodes;
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

    // A JSON collection binds the query to an explicit collection instead of letting it resolve one from T,
    // and supplies its own materializer (raw JsonObject rather than a deserialize to T). For a name-keyed
    // collection fieldTypeInfo is null, which is what makes the whole pipeline schema-free.
    readonly string? boundTypeName;
    readonly string? boundTableName;
    readonly JsonTypeInfo? boundFieldTypeInfo;
    readonly Func<string, T>? materializer;
    readonly Func<T, string?>? boundIdGetter;

    internal DocumentQuery(IQueryExecutor executor, JsonTypeInfo<T>? jsonTypeInfo)
    {
        this.executor = executor;
        this.jsonTypeInfo = jsonTypeInfo;
        this.jsonOptions = executor.JsonOptions;
        this.computed = executor.Options.ResolveComputedLookup(typeof(T));
        this.computedList = executor.Options.ResolveComputedMappings(typeof(T));
    }

    internal DocumentQuery(
        IQueryExecutor executor,
        JsonTypeInfo<T>? jsonTypeInfo,
        string typeName,
        string tableName,
        JsonTypeInfo? fieldTypeInfo,
        Func<string, T> materializer,
        Func<T, string?> idGetter)
        : this(executor, jsonTypeInfo)
    {
        this.boundTypeName = typeName;
        this.boundTableName = tableName;
        this.boundFieldTypeInfo = fieldTypeInfo;
        this.materializer = materializer;
        this.boundIdGetter = idGetter;
    }

    /// <summary>The collection this query reads — the bound one, or the one <typeparamref name="T"/> resolves to.</summary>
    string TypeName => this.boundTypeName ?? this.executor.ResolveTypeName<T>();
    string TableName => this.boundTableName ?? this.executor.ResolveTableName<T>();

    /// <summary>
    /// The metadata field paths resolve through — <c>null</c> on a schema-free collection, where paths are
    /// already resolved into <c>DocumentFieldExpression</c> by the parser.
    /// </summary>
    JsonTypeInfo? FieldTypeInfo => this.boundFieldTypeInfo ?? this.jsonTypeInfo;

    /// <summary>True when this query has no document metadata at all and must stay on the string grammar.</summary>
    internal bool IsSchemaFree => this.boundTypeName != null && this.boundFieldTypeInfo == null;

    // Every builder call returns a copy, so a query is immutable once handed out — the same contract every
    // document provider has always had, and what LINQ/EF Core callers expect.
    DocumentQuery(DocumentQuery<T> source)
    {
        this.executor = source.executor;
        this.jsonTypeInfo = source.jsonTypeInfo;
        this.jsonOptions = source.jsonOptions;
        this.computed = source.computed;
        this.computedList = source.computedList;
        this.wheres.AddRange(source.wheres);
        this.orderBys.AddRange(source.orderBys);
        this.paginateOffset = source.paginateOffset;
        this.paginateTake = source.paginateTake;
        this.ignoreAllFilters = source.ignoreAllFilters;
        this.ignoredFilterNames = source.ignoredFilterNames is null
            ? null
            : new HashSet<string>(source.ignoredFilterNames, StringComparer.Ordinal);
        this.boundTypeName = source.boundTypeName;
        this.boundTableName = source.boundTableName;
        this.boundFieldTypeInfo = source.boundFieldTypeInfo;
        this.materializer = source.materializer;
        this.boundIdGetter = source.boundIdGetter;
    }

    string Qt(string tableName) => this.executor.Provider.QuoteTable(tableName);

    // Embedded telemetry for fluent-query terminals — built lazily from the executor's provider + store name.
    Diagnostics.OperationTracker? tracker;
    Diagnostics.OperationTracker Tracker => this.tracker ??=
        new(Diagnostics.OperationTracker.SystemName(this.executor.Provider), this.executor.Options.StoreName);

    public JsonTypeInfo<T>? QueryTypeInfo => this.jsonTypeInfo;

    public IReadOnlyDictionary<string, ComputedMapping>? ComputedLookup => this.computed;

    public IDocumentQuery<T> Where(Expression<Func<T, bool>> predicate)
    {
        var clone = new DocumentQuery<T>(this);
        clone.wheres.Add(predicate);
        return clone;
    }

    public IDocumentQuery<T> IgnoreQueryFilters()
    {
        var clone = new DocumentQuery<T>(this);
        clone.ignoreAllFilters = true;
        return clone;
    }

    public IDocumentQuery<T> IgnoreQueryFilters(params string[] filterNames)
    {
        ArgumentNullException.ThrowIfNull(filterNames);
        var clone = new DocumentQuery<T>(this);
        clone.ignoredFilterNames ??= new HashSet<string>(StringComparer.Ordinal);
        foreach (var n in filterNames)
            if (!string.IsNullOrWhiteSpace(n))
                clone.ignoredFilterNames.Add(n);
        return clone;
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

        // Cross-cutting features that must transform the caller's expression (field-level encryption rewrites a
        // comparison constant into its ciphertext) get their pass here, before anything is emitted as SQL.
        DocumentPredicateRewriters.ApplyAll(effective);
        return effective;
    }

    public IDocumentQuery<T> OrderBy(Expression<Func<T, object>> selector)
    {
        var clone = new DocumentQuery<T>(this);
        clone.orderBys.Add((selector, false));
        return clone;
    }

    public IDocumentQuery<T> OrderByDescending(Expression<Func<T, object>> selector)
    {
        var clone = new DocumentQuery<T>(this);
        clone.orderBys.Add((selector, true));
        return clone;
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
            this.boundTypeName != null ? null : this.RequireTypeInfo(),
            this.wheres,
            keyField,
            this.computed,
            this.ignoreAllFilters,
            this.ignoredFilterNames,
            this.boundTypeName,
            this.boundTableName,
            this.boundFieldTypeInfo);
    }

    public IDocumentQuery<T> Paginate(int offset, int take)
    {
        var clone = new DocumentQuery<T>(this);
        clone.paginateOffset = offset;
        clone.paginateTake = take;
        return clone;
    }

    // ── Cursor / keyset pagination ──────────────────────────────────────────
    const int MaxCursorTake = 10_000;

    public Task<CursorPage<T>> ToCursorPage(string? cursor, int take, CancellationToken ct = default)
        => this.Tracker.Track("query.paginate", typeof(T).Name, () => this.ToCursorPageImpl(cursor, take, ct), r => r.Items.Count);

    public Task<CursorPage<JsonObject>> ToJsonCursorPage(string? cursor, int take, CancellationToken ct = default)
    {
        RawJsonGuard.Ensure(typeof(T));
        return this.Tracker.Track("query.paginate", typeof(T).Name, () => this.ToJsonCursorPageImpl(cursor, take, ct), r => r.Items.Count);
    }

    async Task<CursorPage<T>> ToCursorPageImpl(string? cursor, int take, CancellationToken ct)
    {
        var (rows, keys, shapeHash) = await this.CursorPageRowsAsync(cursor, take, ct).ConfigureAwait(false);

        if (rows.Count <= take)
            return new CursorPage<T>(rows.Select(this.Deserialize).ToList(), null);

        // One extra row was fetched to detect "more" without a count; drop it and encode the cursor from the
        // last kept row's key values.
        var items = new List<T>(take);
        for (var i = 0; i < take; i++)
            items.Add(this.Deserialize(rows[i]));

        return new CursorPage<T>(items, EncodeCursor(keys, shapeHash, items[take - 1]));
    }

    async Task<CursorPage<JsonObject>> ToJsonCursorPageImpl(string? cursor, int take, CancellationToken ct)
    {
        var (rows, keys, shapeHash) = await this.CursorPageRowsAsync(cursor, take, ct).ConfigureAwait(false);

        if (rows.Count <= take)
            return new CursorPage<JsonObject>(rows.Select(r => RawJson.ParseObject(r, typeof(T))).ToList(), null);

        var items = new List<JsonObject>(take);
        for (var i = 0; i < take; i++)
            items.Add(RawJson.ParseObject(rows[i], typeof(T)));

        // Only the boundary row has to become a T — the cursor is encoded from its key values, and the other
        // rows never leave the raw lane.
        return new CursorPage<JsonObject>(items, EncodeCursor(keys, shapeHash, this.Deserialize(rows[take - 1])));
    }

    string EncodeCursor(List<CursorKey> keys, string shapeHash, T last)
    {
        var cursorValues = new object?[keys.Count];
        for (var i = 0; i < keys.Count; i++)
            cursorValues[i] = keys[i].GetValue(last);

        return CursorCodec.EncodeKeyset(shapeHash, cursorValues);
    }

    /// <summary>
    /// Runs the keyset page and hands back the raw bodies (<paramref name="take"/> + 1 of them, so the caller
    /// can detect "more" without a count) plus what the cursor is encoded from. Both cursor terminals share it,
    /// so the SQL, the NULL ordering and the shape hash cannot drift apart between the typed and JSON lanes.
    /// </summary>
    async Task<(IReadOnlyList<string> Rows, List<CursorKey> Keys, string ShapeHash)> CursorPageRowsAsync(string? cursor, int take, CancellationToken ct)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(take);
        if (take > MaxCursorTake)
            throw new ArgumentOutOfRangeException(nameof(take), take,
                $"Cursor page size must not exceed {MaxCursorTake}.");

        var provider = this.executor.Provider;
        var keys = this.BuildCursorKeys(this.RequireFieldTypeInfo());

        // The ordering (and its column SQL) is the cursor's identity — a cursor is only valid for the exact
        // shape that produced it. Emit each key's column SQL once and reuse it for both ORDER BY and the seek.
        var orderByParts = new List<string>(keys.Count);
        var specParts = new List<string>(keys.Count);
        Dictionary<string, object?>? orderParams = null;
        var idx = 0;
        foreach (var key in keys)
        {
            var (colSql, ps) = SqlPredicateEmitter.EmitValue(key.Column, provider, $"@co{idx}x");
            // Force a deterministic NULL placement (NULLs last, both directions) via a leading flag sort key.
            // Providers disagree on default NULL ordering (SQLite/MySQL NULLs-first on ASC vs PostgreSQL/Oracle
            // NULLs-last), so we pin it here and build the keyset seek (BuildKeysetPredicate) to match — otherwise
            // a NULL in an ordered column silently drops every row after the boundary.
            orderByParts.Add($"(CASE WHEN {colSql} IS NULL THEN 1 ELSE 0 END), {colSql} {(key.Descending ? "DESC" : "ASC")}");
            specParts.Add($"{colSql}:{(key.Descending ? "d" : "a")}");
            if (ps.Count > 0)
            {
                orderParams ??= new Dictionary<string, object?>(StringComparer.Ordinal);
                foreach (var kv in ps)
                    orderParams[kv.Key] = kv.Value;
            }
            idx++;
        }
        var shapeHash = CursorCodec.ComputeShapeHash(this.TypeName + "|" + string.Join("|", specParts));

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

        var typeName = this.TypeName;
        var tableName = this.TableName;
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
            return await ReadListAsync(cmd, static json => json, ct).ConfigureAwait(false);
        }, ct).ConfigureAwait(false);

        return (rows, keys, shapeHash);
    }

    readonly record struct CursorKey(ValueNode Column, bool Descending, Func<T, object?> GetValue);

    List<CursorKey> BuildCursorKeys(JsonTypeInfo? typeInfo)
    {
        var keys = new List<CursorKey>(this.orderBys.Count + 1);
        foreach (var (selector, isDescending) in this.orderBys)
        {
            var body = selector.Body;
            if (body is UnaryExpression { NodeType: ExpressionType.Convert } u)
                body = u.Operand;

            if (body is not (MemberExpression or DocumentFieldExpression))
                throw new NotSupportedException(
                    "Cursor pagination requires ordering by document properties (or computed properties). " +
                    "Ordering by a function such as Distance or a full-text score is not supported for cursors.");

            var column = ExpressionLowerer.LowerValue(body, this.jsonOptions, typeInfo, this.computed);
            var getter = ExpressionInterpreter.Interpret(selector);
            keys.Add(new CursorKey(column, isDescending, getter));
        }

        // Mandatory Id tiebreaker (ascending) so the total order is deterministic even when the sort key is
        // non-unique. Id is a real column (stored as text); reference it directly.
        var idGetter = this.boundIdGetter;
        if (idGetter == null)
        {
            var idAccessor = this.executor.GetIdAccessor(this.jsonTypeInfo);
            idGetter = doc => idAccessor.GetIdAsString(doc);
        }
        keys.Add(new CursorKey(new ComputedColumnNode("Id", typeof(string)), false, idGetter));
        return keys;
    }

    static PredicateNode BuildKeysetPredicate(IReadOnlyList<CursorKey> keys, IReadOnlyList<object?> values)
    {
        // OR-chain (portable, direction-correct — native tuple compare isn't available everywhere and is
        // wrong for mixed directions):
        //   (k0 OP0 v0) OR (k0=v0 AND k1 OP1 v1) OR (k0=v0 AND k1=v1 AND k2 OP2 v2) ...
        //
        // NULL handling matches the NULLs-last ordering pinned in ToCursorPage's ORDER BY. In SQL,
        // `col > NULL` and `col = NULL` are UNKNOWN, so a naive predicate would match zero rows once the
        // boundary lands on a NULL and silently drop the rest of the result set. Instead:
        //   * equality tie on key i:  values[i] IS NULL  ->  (col IS NULL)          else (col = v)
        //   * strict advance on key j (values[j] non-null): (col OP v) OR (col IS NULL)   -- NULLs sort after v
        //   * strict advance on key j (values[j] IS NULL):  no row sorts after a trailing NULL on this key,
        //                                                    so the branch is dead and is skipped entirely.
        PredicateNode? result = null;
        for (var j = 0; j < keys.Count; j++)
        {
            if (values[j] is null)
                continue; // dead branch under NULLs-last (nothing is strictly after a NULL on this key)

            var op = keys[j].Descending ? CompareOp.LessThan : CompareOp.GreaterThan;
            PredicateNode term = new LogicalNode(LogicalOp.Or,
                new CompareNode(op, keys[j].Column, new ConstantNode(values[j])),
                new NullCheckExprNode(keys[j].Column, true));
            for (var i = 0; i < j; i++)
                term = new LogicalNode(LogicalOp.And, EqualityTie(keys[i].Column, values[i]), term);

            result = result == null ? term : new LogicalNode(LogicalOp.Or, result, term);
        }
        // The mandatory Id tiebreaker (last key) is never NULL, so it always contributes a branch — result is
        // non-null here.
        return result!;
    }

    static PredicateNode EqualityTie(ValueNode column, object? value)
        => value is null
            ? new NullCheckExprNode(column, true)
            : new CompareNode(CompareOp.Equal, column, new ConstantNode(value));

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
        // A JSON collection projects over the raw body, so paths resolve to JSON paths (through the
        // document's metadata when it has one) rather than to a member chain on T.
        if (this.boundTypeName != null)
            return this.ProjectJson(fields, this.boundFieldTypeInfo);

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
            this.ignoredFilterNames,
            this.boundTypeName,
            this.boundTableName);
    }

    /// <summary>
    /// JSON-collection projection. With metadata, paths resolve through it and keep their leaf types; without,
    /// they are taken literally and an optional <c>:type</c> hint decides the extraction cast. Function
    /// projections lower through the same value IR as everything else either way.
    /// </summary>
    IDocumentQuery<System.Text.Json.Nodes.JsonObject> ProjectJson(string fields, JsonTypeInfo? fieldTypeInfo)
    {
        var provider = this.executor.Provider;
        var pairs = new List<string>();
        var seen = new HashSet<string>(StringComparer.Ordinal);
        Dictionary<string, object?>? projectionParams = null;
        var fnIndex = 0;

        foreach (var item in FilterExpressionParser.ParseJsonProjection(fields, fieldTypeInfo).Items)
        {
            string alias;
            string valueSql;

            if (item.FieldPath != null)
            {
                var (jsonPath, leafType) = fieldTypeInfo != null
                    ? JsonPathFieldBinder.ResolvePath(fieldTypeInfo, item.FieldPath)
                    : DynamicPathResolver.ResolveJsonPathWithType(item.FieldPath);
                var dot = jsonPath.LastIndexOf('.');
                alias = item.Alias ?? (dot < 0 ? jsonPath : jsonPath[(dot + 1)..]);
                // An unhinted schema-free path extracts as text — byte-identical to the plain extract.
                valueSql = provider.JsonExtractTyped("Data", jsonPath, leafType);
            }
            else
            {
                alias = item.Alias!;
                var node = ExpressionLowerer.LowerValue(item.ValueExpr!, this.jsonOptions, fieldTypeInfo);
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
            sourceTypeInfo: null,
            this.wheres,
            this.orderBys,
            provider.JsonObject(pairs),
            projectionParams,
            this.paginateOffset,
            this.paginateTake,
            this.ignoreAllFilters,
            this.ignoredFilterNames,
            this.boundTypeName,
            this.boundTableName);
    }

    public DocumentQueryString ToQueryString()
    {
        var (whereClause, whereParams) = BuildWhereClause();
        var (orderByClause, orderByParams) = BuildOrderByClause();
        var paginationClause = BuildPaginationClause();
        if (orderByClause == "" && paginationClause != "")
            orderByClause = " ORDER BY (SELECT NULL)";
        var typeName = this.TypeName;
        var tableName = this.TableName;

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
        => this.Tracker.Track("query.to_list", typeof(T).Name, () => this.ToListImpl(ct), r => r.Count);

    Task<IReadOnlyList<T>> ToListImpl(CancellationToken ct)
    {
        var (whereClause, whereParams) = BuildWhereClause();
        var (orderByClause, orderByParams) = BuildOrderByClause();
        var paginationClause = BuildPaginationClause();
        if (orderByClause == "" && paginationClause != "")
            orderByClause = " ORDER BY (SELECT NULL)";
        var typeName = this.TypeName;
        var tableName = this.TableName;

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
        => this.executor.ReadStreamAsync(this.TableName, this.BuildSelectCommand(null), this.Deserialize, ct);

    /// <inheritdoc />
    public bool SupportsRawJson => RawJsonGuard.IsSupported(typeof(T));

    /// <inheritdoc />
    public IAsyncEnumerable<string> RawJsonRows(int? maxRows, CancellationToken ct = default)
    {
        RawJsonGuard.Ensure(typeof(T));

        // The whole point of the raw lane: the Data column goes straight from the reader to the caller. Same
        // SQL as ToAsyncEnumerable, identity in place of Deserialize.
        return this.executor.ReadStreamAsync(this.TableName, this.BuildSelectCommand(maxRows), static json => json, ct);
    }

    /// <summary>
    /// Renders the row-returning SELECT once, so the typed and raw lanes cannot drift apart. The clauses are
    /// built eagerly (before the command exists) because binding happens per-execution.
    /// </summary>
    Action<DbCommand> BuildSelectCommand(int? maxRows)
    {
        var (whereClause, whereParams) = BuildWhereClause();
        var (orderByClause, orderByParams) = BuildOrderByClause();
        var paginationClause = BuildPaginationClause(maxRows);
        if (orderByClause == "" && paginationClause != "")
            orderByClause = " ORDER BY (SELECT NULL)";
        var typeName = this.TypeName;
        var tableName = this.TableName;

        return cmd =>
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
        };
    }

    public Task<long> Count(CancellationToken ct = default)
        => this.Tracker.Track("query.count", typeof(T).Name, () => this.CountImpl(ct), r => r);

    Task<long> CountImpl(CancellationToken ct)
    {
        var (whereClause, whereParams) = BuildWhereClause();
        var typeName = this.TypeName;
        var tableName = this.TableName;

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
        => this.Tracker.Track("query.any", typeof(T).Name, () => this.AnyImpl(ct), r => r ? 1 : 0);

    Task<bool> AnyImpl(CancellationToken ct)
    {
        var (whereClause, whereParams) = BuildWhereClause();
        var typeName = this.TypeName;
        var tableName = this.TableName;

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

    // The single-row terminals run the same SELECT with the row limit narrowed, so the engine returns one (or
    // two) rows instead of the whole match set. An existing, smaller Paginate window wins and the skip is kept,
    // which is what makes Paginate(20, 10).First() the 21st document.
    DocumentQuery<T> WithRowLimit(int take)
    {
        var clone = new DocumentQuery<T>(this)
        {
            paginateOffset = this.paginateOffset ?? 0,
            paginateTake = this.paginateTake is { } existing && existing < take ? existing : take
        };
        return clone;
    }

    public Task<T?> FirstOrDefault(CancellationToken ct = default)
        => this.Tracker.Track("query.first", typeof(T).Name, async () =>
        {
            var rows = await this.WithRowLimit(1).ToListImpl(ct).ConfigureAwait(false);
            return rows.Count == 0 ? null : rows[0];
        }, r => r == null ? 0 : 1);

    public async Task<T> First(CancellationToken ct = default)
        => await this.FirstOrDefault(ct).ConfigureAwait(false)
            ?? throw new InvalidOperationException($"No '{typeof(T).Name}' matched the query.");

    public Task<T?> SingleOrDefault(CancellationToken ct = default)
        => this.Tracker.Track("query.single", typeof(T).Name, async () =>
        {
            var rows = await this.WithRowLimit(2).ToListImpl(ct).ConfigureAwait(false);
            if (rows.Count > 1)
                throw new InvalidOperationException($"More than one '{typeof(T).Name}' matched the query.");
            return rows.Count == 0 ? null : rows[0];
        }, r => r == null ? 0 : 1);

    public async Task<T> Single(CancellationToken ct = default)
        => await this.SingleOrDefault(ct).ConfigureAwait(false)
            ?? throw new InvalidOperationException($"No '{typeof(T).Name}' matched the query.");

    public Task<int> ExecuteDelete(CancellationToken ct = default)
        => this.Tracker.Track("query.execute_delete", typeof(T).Name, () => this.ExecuteDeleteImpl(ct), r => r);

    async Task<int> ExecuteDeleteImpl(CancellationToken ct)
    {
        var (whereClause, whereParams) = BuildWhereClause();
        var typeName = this.TypeName;
        var tableName = this.TableName;

        // Bulk interceptors are registered per CLR document type, so a schema-free collection has none to run.
        var interceptors = this.executor.Options.Interceptors;
        var bulkCtx = this.IsSchemaFree
            ? null
            : interceptors.NewBulk(DocumentOperation.Delete, typeName, whereClause, null, (IDocumentQuery<T>)this);
        if (bulkCtx != null && !await interceptors.BeforeBulk(bulkCtx, ct).ConfigureAwait(false))
            return bulkCtx.CancelAffected;

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

        if (bulkCtx != null)
            await interceptors.AfterBulk(bulkCtx, affected, ct).ConfigureAwait(false);
        return affected;
    }

    public Task<int> ExecuteUpdate(Expression<Func<T, object>> property, object? value, CancellationToken ct = default)
        => this.Tracker.Track("query.execute_update", typeof(T).Name, () =>
        {
            ArgumentNullException.ThrowIfNull(property);
            var jsonPath = IndexExpressionHelper.ResolveJsonPath(property, this.jsonOptions, this.RequireTypeInfo());
            return this.ExecuteAssignmentsImpl([(jsonPath, value)], ct);
        }, r => r);

    public Task<int> ExecuteUpdate(Action<IDocumentUpdateBuilder<T>> build, CancellationToken ct = default)
    {
        var assignments = DocumentUpdateBuilder<T>.Collect(build);
        var typeInfo = this.RequireTypeInfo();

        var resolved = new (string JsonPath, object? Value)[assignments.Count];
        for (var i = 0; i < assignments.Count; i++)
            resolved[i] = (IndexExpressionHelper.ResolveJsonPath(assignments[i].Property, this.jsonOptions, typeInfo), assignments[i].Value);
        DocumentUpdateBuilder<T>.RejectDuplicatePaths(resolved);

        return this.Tracker.Track("query.execute_update", typeof(T).Name, () => this.ExecuteAssignmentsImpl(resolved, ct), r => r);
    }

    /// <summary>
    /// <c>ExecuteUpdate</c> addressed by JSON path rather than by a member selector — what
    /// <see cref="IJsonDocumentQuery.ExecuteUpdate(string, object?, CancellationToken)"/> lowers to. Paths are
    /// validated by the caller.
    /// </summary>
    internal Task<int> ExecuteUpdatePaths(IReadOnlyList<(string JsonPath, object? Value)> assignments, CancellationToken ct = default)
        => this.Tracker.Track("query.execute_update", this.TypeName, () => this.ExecuteAssignmentsImpl(assignments, ct), r => r);

    // One statement for N assignments: each dialect's JSON-set expression nests around the previous one, so the
    // predicate is evaluated once and the whole update is atomic without a surrounding transaction.
    async Task<int> ExecuteAssignmentsImpl(IReadOnlyList<(string JsonPath, object? Value)> assignments, CancellationToken ct)
    {
        var (whereClause, whereParams) = BuildWhereClause();
        var typeName = this.TypeName;
        var tableName = this.TableName;
        var provider = this.executor.Provider;

        // Bulk interceptors are registered per CLR document type, so a schema-free collection has none to run.
        var interceptors = this.executor.Options.Interceptors;
        var bulkCtx = this.IsSchemaFree
            ? null
            : interceptors.NewBulk(DocumentOperation.Update, typeName, whereClause, assignments, (IDocumentQuery<T>)this);
        if (bulkCtx != null && !await interceptors.BeforeBulk(bulkCtx, ct).ConfigureAwait(false))
            return bulkCtx.CancelAffected;

        var affected = await this.executor.ExecuteAsync(tableName, async session =>
        {
            await using var cmd = session.CreateCommand();
            var setExpression = "Data";
            for (var i = 0; i < assignments.Count; i++)
                setExpression = provider.BuildJsonSetExpression(setExpression, $"@path{i}", $"@value{i}");

            var sql = $"UPDATE {Qt(tableName)} SET Data = {setExpression}, UpdatedAt = @now WHERE TypeName = @typeName";
            sql += this.executor.TenantFilter ?? "";
            if (whereClause != null)
                sql += $" AND ({whereClause})";
            cmd.CommandText = sql + ";";
            for (var i = 0; i < assignments.Count; i++)
            {
                AddParameter(cmd, $"@path{i}", "$." + assignments[i].JsonPath);
                AddParameter(cmd, $"@value{i}", provider.FormatPropertyValue(assignments[i].Value));
            }
            AddParameter(cmd, "@now", DateTimeOffset.UtcNow);
            AddParameter(cmd, "@typeName", typeName);
            this.executor.AddTenantParameter(cmd);
            if (whereParams != null)
                BindDictionaryParameters(cmd, whereParams);

            this.executor.Logging?.Invoke(cmd.CommandText);
            return await cmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
        }, ct).ConfigureAwait(false);

        if (bulkCtx != null)
            await interceptors.AfterBulk(bulkCtx, affected, ct).ConfigureAwait(false);
        return affected;
    }

    public Task<TValue> Max<TValue>(Expression<Func<T, TValue>> selector, CancellationToken ct = default)
        => this.Tracker.Track("query.max", typeof(T).Name, () => this.ScalarAggregate("MAX", selector, ct));

    public Task<TValue> Min<TValue>(Expression<Func<T, TValue>> selector, CancellationToken ct = default)
        => this.Tracker.Track("query.min", typeof(T).Name, () => this.ScalarAggregate("MIN", selector, ct));

    public Task<TValue> Sum<TValue>(Expression<Func<T, TValue>> selector, CancellationToken ct = default)
        => this.Tracker.Track("query.sum", typeof(T).Name, () => this.ScalarAggregate("SUM", selector, ct));

    public Task<double> Average(Expression<Func<T, object>> selector, CancellationToken ct = default)
        => this.Tracker.Track("query.average", typeof(T).Name, () => this.AverageImpl(selector, ct));

    Task<double> AverageImpl(Expression<Func<T, object>> selector, CancellationToken ct)
    {
        var typeInfo = this.RequireTypeInfo();
        var jsonPath = AggregateTranslator.ResolveJsonPathFromSelector(selector, this.jsonOptions, typeInfo);
        var (whereClause, whereParams) = BuildWhereClause();
        var typeName = this.TypeName;
        var tableName = this.TableName;
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
        var typeName = this.TypeName;
        var tableName = this.TableName;
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

    /// <summary>
    /// Scalar aggregate over an already-resolved JSON path — the JSON lane's entry point, where the field
    /// arrives as a path string rather than as a member selector. Extraction is numeric on every provider,
    /// and the result is <c>null</c> when no row matched (rather than the typed surface's <c>default</c>,
    /// which cannot tell "no rows" from "summed to zero").
    /// </summary>
    internal Task<double?> ScalarAggregateByPath(string sqlFunc, string jsonPath, CancellationToken ct = default)
        => this.Tracker.Track(
            $"query.{sqlFunc.ToLowerInvariant()}",
            this.TypeName,
            () => this.ScalarAggregateByPathImpl(sqlFunc, jsonPath, ct));

    Task<double?> ScalarAggregateByPathImpl(string sqlFunc, string jsonPath, CancellationToken ct)
    {
        var (whereClause, whereParams) = BuildWhereClause();
        var typeName = this.TypeName;
        var tableName = this.TableName;
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
                return (double?)null;
            return Convert.ToDouble(result);
        }, ct);
    }

    /// <summary>
    /// The metadata for lowering field paths. Unlike <see cref="RequireTypeInfo"/> this legitimately returns
    /// <c>null</c> for a schema-free collection — the lowerer only needs it to walk CLR member chains, and
    /// there are none.
    /// </summary>
    JsonTypeInfo? RequireFieldTypeInfo()
        => this.IsSchemaFree ? null : this.FieldTypeInfo ?? RequireTypeInfo();

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

        var combined = CombinePredicates(effective);
        var (ftType, ftMap) = this.ResolveFullText();
        var (clause, parms) = JsonExpressionVisitor.Translate(combined, this.jsonOptions, this.RequireFieldTypeInfo(), this.executor.Provider, this.executor.Options.FunctionRegistry, this.computed, this.TableName, ftType, ftMap);
        return (clause, parms);
    }

    // Full-text (LuceneMatch/LuceneScore) needs the type's mapping + resolved type name to build the native
    // index predicate; null when the type isn't full-text mapped (the emitter then throws a clear error).
    (string? TypeName, FullTextMapping? Mapping) ResolveFullText()
    {
        if (this.IsSchemaFree)
            return (null, null);
        var mapping = this.executor.Options.ResolveFullTextMapping(typeof(T));
        return mapping is null ? (null, null) : (this.TypeName, mapping);
    }

    /// <param name="maxRows">
    /// A row cap from a single-row terminal. An existing, smaller <c>Paginate</c> window wins and the skip is
    /// always preserved — the same rule as <c>DocumentQueryBase.BuildPlan(int)</c>.
    /// </param>
    string BuildPaginationClause(int? maxRows = null)
    {
        var take = this.paginateTake;
        if (maxRows is { } max)
            take = take is { } window && window < max ? window : max;

        if (take == null)
            return "";

        return " " + this.executor.Provider.BuildPaginationClause(this.paginateOffset ?? 0, take.Value);
    }

    (string Clause, Dictionary<string, object?>? Parameters) BuildOrderByClause()
    {
        if (this.orderBys.Count == 0)
            return ("", null);

        var typeInfo = this.RequireFieldTypeInfo();
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
                    var (sql, ps) = SqlPredicateEmitter.EmitValue(node, provider, $"@o{idx}x", this.TableName, ftType, ftMap);
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
                    // Lower the member access to a typed extraction (JsonExtractTyped) so a numeric column sorts
                    // numerically, not lexicographically ("100" before "9"), on providers whose plain JSON
                    // extract returns text (PostgreSQL/MySQL/SQL Server). Dates/strings still sort as text.
                    var node = ExpressionLowerer.LowerValue(body, this.jsonOptions, typeInfo, this.computed);
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
        // A JSON collection hands back the raw body; there is no T to populate computed properties on or to
        // stamp blob loaders onto.
        if (this.materializer != null)
            return this.materializer(json);

        var document = this.jsonTypeInfo != null
            ? JsonSerializer.Deserialize(json, this.jsonTypeInfo)!
            : JsonSerializer.Deserialize<T>(json, this.jsonOptions)!;

        // Populate computed (JsonIgnore'd) properties on read so the round-tripped object is complete.
        for (var i = 0; i < this.computedList.Count; i++)
        {
            var mapping = this.computedList[i];
            mapping.SetValue(document, mapping.Compute(document));
        }

        // Stamp blob loaders so metadata-only blobs on the result can self-load (no-op for non-blob types).
        this.executor.AttachBlobLoaders(document);
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
