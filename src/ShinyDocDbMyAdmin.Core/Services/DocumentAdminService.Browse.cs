using System.Data.Common;
using System.Globalization;
using System.Text;
using ShinyDocDbMyAdmin.Models;
using Shiny.DocumentDb;

namespace ShinyDocDbMyAdmin.Services;

public sealed partial class DocumentAdminService
{
    /// <summary>
    /// Columns that live on the envelope rather than inside the JSON body. A filter or sort naming one
    /// of these targets the real column; anything else is treated as a dotted path into <c>Data</c>.
    /// </summary>
    public static readonly IReadOnlySet<string> EnvelopeColumnNames =
        new HashSet<string>(StringComparer.OrdinalIgnoreCase) { "Id", "TypeName", "CreatedAt", "UpdatedAt", "TenantId" };

    public static bool IsEnvelopeColumn(string path) => EnvelopeColumnNames.Contains(path);

    public async Task<DocumentPage> Browse(
        string profileId,
        string table,
        string typeName,
        BrowseQuery query,
        CancellationToken ct = default)
    {
        var connection = await this.Connect(profileId, ct);
        var provider = connection.Provider;
        var quoted = provider.QuoteTable(Ado.SafeIdentifier(table));

        var where = new WhereBuilder(provider);
        where.Add("TypeName = @typeName", ("@typeName", typeName));
        ApplyFilters(where, provider, query);

        var page = Math.Max(1, query.Page);
        var pageSize = Math.Clamp(query.PageSize, 1, 500);
        var offset = (page - 1) * pageSize;

        var sort = query.Sort ?? new BrowseSort("UpdatedAt", true, true);
        var sortExpr = ColumnExpression(provider, sort.Path);
        // Id as the tiebreaker keeps paging stable when the sort column has duplicates.
        var orderBy = $"ORDER BY {sortExpr} {(sort.Descending ? "DESC" : "ASC")}, Id ASC";

        var columns = query.IncludeTenant ? $"{EnvelopeColumns}, TenantId" : EnvelopeColumns;

        return await connection.Execute(async (db, token) =>
        {
            long total;
            await using (var countCmd = Ado.Command(db, $"SELECT COUNT(*) FROM {quoted} {where.Sql}"))
            {
                where.Bind(countCmd);
                total = Ado.Count(await countCmd.ExecuteScalarAsync(token));
            }

            var sql = $"SELECT {columns} FROM {quoted} {where.Sql} {orderBy} {provider.BuildPaginationClause(offset, pageSize)}";
            await using var cmd = Ado.Command(db, sql);
            where.Bind(cmd);

            var rows = new List<DocumentRow>();
            await using (var reader = await cmd.ExecuteReaderAsync(token))
            {
                while (await reader.ReadAsync(token))
                    rows.Add(ReadRow(reader, query.IncludeTenant));
            }

            return new DocumentPage(rows, total, page, pageSize);
        }, ct);
    }

    /// <summary>Streams every document matching a browse query - the export path, so it never buffers a page.</summary>
    public async IAsyncEnumerable<DocumentRow> StreamAll(
        string profileId,
        string table,
        string typeName,
        BrowseQuery query,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct = default)
    {
        // Keyset paging over (sort, Id) would be nicer, but offset paging is the only form every one of
        // the nine dialects agrees on, and an export is not a hot path.
        var page = 1;
        while (!ct.IsCancellationRequested)
        {
            var slice = await this.Browse(profileId, table, typeName, query with { Page = page, PageSize = 500 }, ct);
            if (slice.Rows.Count == 0)
                yield break;

            foreach (var row in slice.Rows)
                yield return row;

            if (page >= slice.PageCount)
                yield break;

            page++;
        }
    }

    static DocumentRow ReadRow(DbDataReader reader, bool includeTenant) => new(
        Ado.Text(reader, 0),
        Ado.Text(reader, 1),
        Ado.Text(reader, 2),
        Ado.Timestamp(reader, 3),
        Ado.Timestamp(reader, 4),
        includeTenant && reader.FieldCount > 5 ? Ado.NullableText(reader, 5) : null
    );

    static void ApplyFilters(WhereBuilder where, IDatabaseProvider provider, BrowseQuery query)
    {
        foreach (var filter in query.Filters)
        {
            if (string.IsNullOrWhiteSpace(filter.Path))
                continue;

            AppendFilter(where, provider, filter);
        }

        if (!string.IsNullOrWhiteSpace(query.Search) && query.SearchPaths.Count > 0)
        {
            var pattern = $"%{provider.EscapeLikePattern(query.Search.ToLowerInvariant())}%";
            var alternatives = query.SearchPaths
                .Select(path => $"LOWER({ColumnExpression(provider, path)}) LIKE {where.Reserve(pattern)}{provider.LikeEscapeClause}")
                .ToList();

            where.Add($"({string.Join(" OR ", alternatives)})");
        }

        if (!string.IsNullOrWhiteSpace(query.TenantId))
            where.Add($"TenantId = {where.Reserve(query.TenantId)}");
    }

    static void AppendFilter(WhereBuilder where, IDatabaseProvider provider, BrowseFilter filter)
    {
        var isEnvelope = IsEnvelopeColumn(filter.Path);
        var text = ColumnExpression(provider, filter.Path);

        switch (filter.Operator)
        {
            case FilterOperator.IsNull:
                where.Add(isEnvelope
                    ? $"{text} IS NULL"
                    : provider.JsonNullCheck("Data", filter.Path, true));
                return;

            case FilterOperator.IsNotNull:
                where.Add(isEnvelope
                    ? $"{text} IS NOT NULL"
                    : provider.JsonNullCheck("Data", filter.Path, false));
                return;

            case FilterOperator.Equals:
                where.Add($"{text} = {where.Reserve(filter.Value)}");
                return;

            case FilterOperator.NotEquals:
                where.Add($"{text} <> {where.Reserve(filter.Value)}");
                return;

            case FilterOperator.Contains:
            case FilterOperator.StartsWith:
            case FilterOperator.EndsWith:
                var escaped = provider.EscapeLikePattern(filter.Value.ToLowerInvariant());
                var pattern = filter.Operator switch
                {
                    FilterOperator.StartsWith => $"{escaped}%",
                    FilterOperator.EndsWith => $"%{escaped}",
                    _ => $"%{escaped}%"
                };
                // Both sides lowered: SQLite's LIKE is case-insensitive but PostgreSQL's is not, and a
                // filter that quietly means something different per backend is worse than a slower one.
                where.Add($"LOWER({text}) LIKE {where.Reserve(pattern)}{provider.LikeEscapeClause}");
                return;

            default:
                AppendComparison(where, provider, filter, isEnvelope, text);
                return;
        }
    }

    static void AppendComparison(WhereBuilder where, IDatabaseProvider provider, BrowseFilter filter, bool isEnvelope, string textExpr)
    {
        var op = filter.Operator switch
        {
            FilterOperator.GreaterThan => ">",
            FilterOperator.GreaterOrEqual => ">=",
            FilterOperator.LessThan => "<",
            FilterOperator.LessOrEqual => "<=",
            _ => "="
        };

        // A numeric literal has to be compared numerically or JSON text ordering makes "9" > "10".
        if (!isEnvelope && double.TryParse(filter.Value, NumberStyles.Any, CultureInfo.InvariantCulture, out var number))
        {
            where.Add($"{provider.JsonExtractNumeric("Data", filter.Path)} {op} {where.Reserve(number)}");
            return;
        }

        where.Add($"{textExpr} {op} {where.Reserve(filter.Value)}");
    }

    /// <summary>Resolves a filter/sort path to SQL: an envelope column, or a JSON extract from <c>Data</c>.</summary>
    public static string ColumnExpression(IDatabaseProvider provider, string path)
        => IsEnvelopeColumn(path) ? path : provider.JsonExtract("Data", path);

    /// <summary>Accumulates AND-ed predicates and their parameters for one statement.</summary>
    sealed class WhereBuilder(IDatabaseProvider provider)
    {
        readonly List<string> clauses = [];
        readonly List<(string Name, object? Value)> parameters = [];

        public void Add(string clause, params (string Name, object? Value)[] bound)
        {
            this.clauses.Add(clause);
            this.parameters.AddRange(bound);
        }

        /// <summary>Registers a value and returns the placeholder to drop into the SQL.</summary>
        public string Reserve(object? value)
        {
            var name = $"@f{this.parameters.Count}";
            this.parameters.Add((name, provider.NormalizeParameterValue(value)));
            return name;
        }

        public string Sql => this.clauses.Count == 0
            ? ""
            : "WHERE " + string.Join(" AND ", this.clauses.Select(c => $"({c})"));

        public void Bind(DbCommand cmd)
        {
            foreach (var (name, value) in this.parameters)
                Ado.Bind(cmd, name, value);
        }
    }
}

/// <summary>Everything the Browse tab can ask for, in one value so it round-trips through the URL.</summary>
public sealed record BrowseQuery
{
    public int Page { get; init; } = 1;
    public int PageSize { get; init; } = 25;
    public BrowseSort? Sort { get; init; }
    public IReadOnlyList<BrowseFilter> Filters { get; init; } = [];

    /// <summary>Free-text needle applied across <see cref="SearchPaths"/>.</summary>
    public string? Search { get; init; }

    /// <summary>Which JSON paths the quick search covers - normally the inferred string fields plus Id.</summary>
    public IReadOnlyList<string> SearchPaths { get; init; } = [];

    public string? TenantId { get; init; }
    public bool IncludeTenant { get; init; }
}
