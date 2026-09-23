using System.Data.Common;
using System.Linq.Expressions;
using System.Runtime.CompilerServices;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;
using Shiny.DocumentDb.Internal.Query;

namespace Shiny.DocumentDb.Internal;

/// <summary>
/// A join on the relational providers. Both documents' conditions, filters, ordering and paging compile to one
/// statement over two aliased table references; both bodies come back and are shaped after deserialization.
/// </summary>
/// <remarks>
/// The aliases are fixed (<c>j0</c>, <c>j1</c>) rather than the caller's, so no user-chosen name can collide with a
/// reserved word. The right side's type, tenant and query-filter predicates are part of the <c>ON</c> clause: in
/// <c>WHERE</c> they would turn a left join's unmatched row — or a match whose right document is soft-deleted — into
/// no row at all.
/// </remarks>
sealed class JoinDocumentQuery<TLeft, TRight> : JoinQueryBase<TLeft, TRight> where TLeft : class where TRight : class
{
    const string LeftAlias = "j0";
    const string RightAlias = "j1";

    readonly IQueryExecutor executor;
    Diagnostics.OperationTracker? tracker;

    internal JoinDocumentQuery(IQueryExecutor executor, JoinDefinition<TLeft, TRight> definition) : base(definition)
        => this.executor = executor;

    JoinDocumentQuery(JoinDocumentQuery<TLeft, TRight> source) : base(source)
        => this.executor = source.executor;

    protected override JoinQueryBase<TLeft, TRight> Clone() => new JoinDocumentQuery<TLeft, TRight>(this);

    Diagnostics.OperationTracker Tracker => this.tracker ??=
        new(Diagnostics.OperationTracker.SystemName(this.executor.Provider), this.executor.Options.StoreName);

    string TrackedName => $"{typeof(TLeft).Name}+{typeof(TRight).Name}";

    /// <summary>The statement's reusable part: <c>FROM … JOIN … ON … WHERE …</c>, its ordering and its parameters.</summary>
    sealed record JoinSql(string LeftTable, string RightTable, string FromWhere, string OrderBy, Dictionary<string, object?> Parameters);

    JoinSql Build()
    {
        var d = this.Definition;
        var provider = this.executor.Provider;
        var prepared = this.Prepare();
        var registry = this.executor.Options.FunctionRegistry;
        var sides = new Dictionary<ParameterExpression, JoinSide>
        {
            [d.Left] = new(LeftAlias, d.LeftSource.TypeInfo, d.LeftSource.Computed),
            [d.Right] = new(RightAlias, d.RightSource.TypeInfo, d.RightSource.Computed)
        };

        var parameters = new Dictionary<string, object?>(StringComparer.Ordinal)
        {
            ["@jt0"] = this.executor.ResolveTypeName<TLeft>(),
            ["@jt1"] = this.executor.ResolveTypeName<TRight>()
        };
        var tenanted = this.executor.TenantFilter != null;
        if (tenanted)
            this.executor.CollectTenantParameter(parameters);

        var on = new StringBuilder($"{RightAlias}.TypeName = @jt1");
        if (tenanted)
            on.Append($" AND {RightAlias}.TenantId = @tenantId");
        this.AppendConditions(on, [.. prepared.RightScope, .. prepared.On], "@jn", sides, parameters);

        var where = new StringBuilder($"{LeftAlias}.TypeName = @jt0");
        if (tenanted)
            where.Append($" AND {LeftAlias}.TenantId = @tenantId");
        this.AppendConditions(where, [.. prepared.LeftScope, .. prepared.Where], "@jw", sides, parameters);

        var orderBy = new StringBuilder();
        for (var i = 0; i < prepared.OrderBy.Count; i++)
        {
            var (body, descending) = prepared.OrderBy[i];
            var node = ExpressionLowerer.LowerJoinValue(body, this.executor.JsonOptions, sides, registry);
            var (sql, ps) = SqlPredicateEmitter.EmitValue(node, provider, $"@js{i}x");
            orderBy.Append(i == 0 ? " ORDER BY " : ", ").Append(sql).Append(descending ? " DESC" : " ASC");
            foreach (var kv in ps)
                parameters[kv.Key] = kv.Value;
        }

        var leftTable = this.executor.ResolveTableName<TLeft>();
        var rightTable = this.executor.ResolveTableName<TRight>();
        var joinKeyword = d.Kind == JoinKind.Left ? "LEFT JOIN" : "INNER JOIN";
        var fromWhere =
            $"FROM {provider.QuoteTable(leftTable)} {LeftAlias} {joinKeyword} {provider.QuoteTable(rightTable)} {RightAlias} ON {on} WHERE {where}";

        return new JoinSql(leftTable, rightTable, fromWhere, orderBy.ToString(), parameters);
    }

    void AppendConditions(
        StringBuilder sql,
        List<Expression> conditions,
        string parameterPrefix,
        IReadOnlyDictionary<ParameterExpression, JoinSide> sides,
        Dictionary<string, object?> parameters)
    {
        if (conditions.Count == 0)
            return;

        var node = ExpressionLowerer.LowerJoin(JoinExpressions.Combine(conditions), this.executor.JsonOptions, sides, this.executor.Options.FunctionRegistry);
        var (predicate, ps) = SqlPredicateEmitter.EmitPredicate(node, this.executor.Provider, parameterPrefix);
        sql.Append(" AND (").Append(predicate).Append(')');
        foreach (var kv in ps)
            parameters[kv.Key] = kv.Value;
    }

    string SelectStatement(JoinSql sql, int? maxRows)
    {
        var (skip, take) = this.Window(maxRows);
        var pagination = take is { } limit ? " " + this.executor.Provider.BuildPaginationClause(skip ?? 0, limit) : "";
        var orderBy = sql.OrderBy == "" && pagination != "" ? " ORDER BY (SELECT NULL)" : sql.OrderBy;
        return $"SELECT {LeftAlias}.Data, {RightAlias}.Data{this.TimestampColumns} {sql.FromWhere}{orderBy}{pagination};";
    }

    DocumentMetadataAccessor? LeftMetadata => MetadataSupport.For(typeof(TLeft), this.Definition.LeftSource.TypeInfo, this.executor.JsonOptions);
    DocumentMetadataAccessor? RightMetadata => MetadataSupport.For(typeof(TRight), this.Definition.RightSource.TypeInfo, this.executor.JsonOptions);

    // Either side declaring DocumentMetadata widens the select list to both sides' envelope timestamps (columns 2–5),
    // so ReadPair can stamp each document from its own row.
    string TimestampColumns => this.LeftMetadata == null && this.RightMetadata == null
        ? ""
        : $", {LeftAlias}.CreatedAt, {LeftAlias}.UpdatedAt, {RightAlias}.CreatedAt, {RightAlias}.UpdatedAt";

    // Tables are created lazily, per operation, for the one table that operation names. A join reads two, so the
    // right one is touched first when it differs.
    Task EnsureRightTableAsync(JoinSql sql, CancellationToken ct)
        => String.Equals(sql.LeftTable, sql.RightTable, StringComparison.Ordinal)
            ? Task.CompletedTask
            : this.executor.ExecuteAsync(sql.RightTable, static _ => Task.FromResult(true), ct);

    // ── Terminals ───────────────────────────────────────────────────────

    internal override Task<IReadOnlyList<JoinPair<TLeft, TRight>>> FetchAsync(int? maxRows, CancellationToken ct)
        => this.Tracker.Track("query.join.to_list", this.TrackedName, () => this.FetchImpl(maxRows, ct), r => r.Count);

    async Task<IReadOnlyList<JoinPair<TLeft, TRight>>> FetchImpl(int? maxRows, CancellationToken ct)
    {
        var sql = this.Build();
        await this.EnsureRightTableAsync(sql, ct).ConfigureAwait(false);

        return await this.executor.ExecuteAsync<IReadOnlyList<JoinPair<TLeft, TRight>>>(sql.LeftTable, async session =>
        {
            await using var cmd = session.CreateCommand();
            cmd.CommandText = this.SelectStatement(sql, maxRows);
            DocumentQuery<TLeft>.BindDictionaryParameters(cmd, sql.Parameters);
            this.executor.Logging?.Invoke(cmd.CommandText);

            var rows = new List<JoinPair<TLeft, TRight>>();
            await using var reader = await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false);
            while (await reader.ReadAsync(ct).ConfigureAwait(false))
                rows.Add(this.ReadPair(reader));
            return rows;
        }, ct).ConfigureAwait(false);
    }

    internal override IAsyncEnumerable<JoinPair<TLeft, TRight>> StreamAsync(CancellationToken ct)
        => this.Tracker.TrackStream("query.join.stream", this.TrackedName, this.StreamImpl(ct), ct);

    async IAsyncEnumerable<JoinPair<TLeft, TRight>> StreamImpl([EnumeratorCancellation] CancellationToken ct)
    {
        var sql = this.Build();
        await this.EnsureRightTableAsync(sql, ct).ConfigureAwait(false);

        var rows = this.executor.ReadRowsAsync(
            sql.LeftTable,
            cmd =>
            {
                cmd.CommandText = this.SelectStatement(sql, null);
                DocumentQuery<TLeft>.BindDictionaryParameters(cmd, sql.Parameters);
            },
            this.ReadPair,
            ct);

        await foreach (var pair in rows.WithCancellation(ct).ConfigureAwait(false))
            yield return pair;
    }

    internal override Task<long> CountAsync(CancellationToken ct)
        => this.Tracker.Track("query.join.count", this.TrackedName, () => this.ScalarAsync(sql => $"SELECT COUNT(*) {sql.FromWhere};", ct), r => r);

    internal override Task<bool> AnyAsync(CancellationToken ct)
        => this.Tracker.Track(
            "query.join.any",
            this.TrackedName,
            async () => await this.ScalarAsync(sql => $"SELECT CASE WHEN EXISTS(SELECT 1 {sql.FromWhere}) THEN 1 ELSE 0 END;", ct).ConfigureAwait(false) == 1,
            r => r ? 1 : 0);

    async Task<long> ScalarAsync(Func<JoinSql, string> statement, CancellationToken ct)
    {
        var sql = this.Build();
        await this.EnsureRightTableAsync(sql, ct).ConfigureAwait(false);

        return await this.executor.ExecuteAsync(sql.LeftTable, async session =>
        {
            await using var cmd = session.CreateCommand();
            cmd.CommandText = statement(sql);
            DocumentQuery<TLeft>.BindDictionaryParameters(cmd, sql.Parameters);
            this.executor.Logging?.Invoke(cmd.CommandText);
            return Convert.ToInt64(await cmd.ExecuteScalarAsync(ct).ConfigureAwait(false));
        }, ct).ConfigureAwait(false);
    }

    internal override DocumentQueryString RenderQuery()
    {
        var sql = this.Build();
        return new DocumentQueryString(this.SelectStatement(sql, null), sql.Parameters);
    }

    // ── Materialization ─────────────────────────────────────────────────

    JoinPair<TLeft, TRight> ReadPair(DbDataReader reader)
    {
        var d = this.Definition;
        var left = this.Materialize(reader.GetString(0), d.LeftSource.TypeInfo);
        var right = reader.IsDBNull(1) ? null : this.Materialize(reader.GetString(1), d.RightSource.TypeInfo);
        var tenantId = this.executor.CurrentTenantId;
        MetadataSupport.StampFromReader(this.LeftMetadata, left, reader, 2, tenantId);
        MetadataSupport.StampFromReader(this.RightMetadata, right, reader, 4, tenantId);
        return new JoinPair<TLeft, TRight>(left, right);
    }

    T Materialize<T>(string json, JsonTypeInfo<T> typeInfo) where T : class
    {
        var document = JsonSerializer.Deserialize(json, typeInfo)!;
        ComputedReadBack.Apply([document], this.executor.Options.ResolveComputedMappings(typeof(T)));
        this.executor.AttachBlobLoaders(document);
        return document;
    }
}
