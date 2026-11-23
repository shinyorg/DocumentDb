using System.Text.Json;
using System.Text.Json.Serialization;
using System.Text.RegularExpressions;
using Microsoft.Data.Sqlite;
using Shiny.DocumentDb.Sqlite;
using Xunit;

namespace Shiny.DocumentDb.Tests.Sqlite;

public class Film
{
    public string Id { get; set; } = "";
    public string CategoryId { get; set; } = "";
    public long AddedUtc { get; set; }
    public long? Rating { get; set; }
}

[JsonSerializable(typeof(Film))]
public partial class FilmJsonContext : JsonSerializerContext;

/// <summary>
/// Cursor pages ordered by keys that can't be NULL (a non-nullable value-type property, and the Id tiebreaker) emit a
/// plain ORDER BY and an index-seekable bound, so a composite index on (filter, sort) serves the page.
/// </summary>
public partial class SqliteCursorIndexTests : IDisposable
{
    readonly string dir = Path.Combine(Path.GetTempPath(), "docdb-cursor", Guid.NewGuid().ToString("N"));
    readonly string dbPath;
    readonly List<string> log = [];

    public SqliteCursorIndexTests()
    {
        Directory.CreateDirectory(this.dir);
        this.dbPath = Path.Combine(this.dir, "test.db");
    }

    public void Dispose()
    {
        SqliteConnection.ClearAllPools();
        Directory.Delete(this.dir, true);
    }

    (DocumentStore Store, FilmJsonContext Ctx) Open(JsonIgnoreCondition ignore = JsonIgnoreCondition.WhenWritingNull)
    {
        var ctx = new FilmJsonContext(new JsonSerializerOptions { PropertyNamingPolicy = JsonNamingPolicy.CamelCase, DefaultIgnoreCondition = ignore });
        var store = new DocumentStore(new DocumentStoreOptions
        {
            DatabaseProvider = new SqliteDatabaseProvider($"Data Source={this.dbPath}"),
            JsonSerializerOptions = ctx.Options,
            TableName = "docs",
            Logging = sql => { lock (this.log) this.log.Add(sql); }
        });
        return (store, ctx);
    }

    static async Task SeedAsync(DocumentStore store, FilmJsonContext ctx)
    {
        // repeated AddedUtc values so pages break inside a tie, two categories so the filter matters
        var films = Enumerable.Range(0, 300).Select(i => new Film
        {
            Id = $"f{i:D3}",
            CategoryId = i % 3 == 0 ? "other" : "c1",
            AddedUtc = 1000 + i / 4,
            Rating = i % 5 == 0 ? null : i
        });
        await store.BatchInsert(films, ctx.Film);
    }

    string LastPageSql() => this.log.Last(x => x.Contains("ORDER BY", StringComparison.Ordinal) && x.Contains("LIMIT", StringComparison.Ordinal));

    [Fact]
    public async Task Pages_over_a_non_null_key_match_the_full_ordering()
    {
        var (store, ctx) = this.Open();
        using var _ = store;
        await SeedAsync(store, ctx);

        var expected = (await store.Query(ctx.Film).Where(x => x.CategoryId == "c1").ToList())
            .OrderByDescending(x => x.AddedUtc).ThenBy(x => x.Id, StringComparer.Ordinal)
            .Select(x => x.Id).ToList();

        var paged = new List<string>();
        string? cursor = null;
        do
        {
            var page = await store.Query(ctx.Film).Where(x => x.CategoryId == "c1").OrderByDescending(x => x.AddedUtc).ToCursorPage(cursor, 7);
            paged.AddRange(page.Items.Select(x => x.Id));
            cursor = page.NextCursor;
        }
        while (cursor != null);

        Assert.Equal(expected, paged);
    }

    [Fact]
    public async Task A_non_null_key_drops_the_null_ordering_and_adds_a_seek_bound()
    {
        var (store, ctx) = this.Open();
        using var _ = store;
        await SeedAsync(store, ctx);

        var first = await store.Query(ctx.Film).Where(x => x.CategoryId == "c1").OrderByDescending(x => x.AddedUtc).ToCursorPage(null, 10);
        await store.Query(ctx.Film).Where(x => x.CategoryId == "c1").OrderByDescending(x => x.AddedUtc).ToCursorPage(first.NextCursor, 10);
        var sql = this.LastPageSql();

        Assert.DoesNotContain("CASE WHEN", sql);
        Assert.Contains("ORDER BY json_extract(Data, '$.addedUtc') DESC, Id ASC", sql);
        Assert.Contains("<=", sql);
        Assert.DoesNotContain("IS NULL", sql);
    }

    [Fact]
    public async Task A_nullable_key_keeps_nulls_last()
    {
        var (store, ctx) = this.Open();
        using var _ = store;
        await SeedAsync(store, ctx);

        var paged = new List<Film>();
        string? cursor = null;
        do
        {
            var page = await store.Query(ctx.Film).OrderBy(x => x.Rating).ToCursorPage(cursor, 9);
            paged.AddRange(page.Items);
            cursor = page.NextCursor;
        }
        while (cursor != null);

        Assert.Equal(300, paged.Select(x => x.Id).Distinct().Count());
        Assert.All(paged.SkipWhile(x => x.Rating != null), x => Assert.Null(x.Rating));
        Assert.Contains("CASE WHEN", this.LastPageSql());
    }

    [Fact]
    public async Task A_serializer_that_omits_defaults_keeps_the_null_handling()
    {
        var (store, ctx) = this.Open(JsonIgnoreCondition.WhenWritingDefault);
        using var _ = store;
        await SeedAsync(store, ctx);

        await store.Query(ctx.Film).OrderByDescending(x => x.AddedUtc).ToCursorPage(null, 10);
        Assert.Contains("CASE WHEN", this.LastPageSql());
    }

    [Fact]
    public async Task A_composite_index_serves_the_page_without_a_sort()
    {
        var (store, ctx) = this.Open();
        using var _ = store;
        await SeedAsync(store, ctx);
        await store.CreateIndexAsync(ctx.Film, x => x.CategoryId, x => x.AddedUtc);

        var first = await store.Query(ctx.Film).Where(x => x.CategoryId == "c1").OrderByDescending(x => x.AddedUtc).ToCursorPage(null, 10);
        await store.Query(ctx.Film).Where(x => x.CategoryId == "c1").OrderByDescending(x => x.AddedUtc).ToCursorPage(first.NextCursor, 10);
        var plan = this.Explain(this.LastPageSql());

        // SEARCH ... USING INDEX (<expr>=? AND <expr><?): the category equality plus the AddedUtc range seek
        Assert.Contains(plan, p => p.Contains("USING INDEX", StringComparison.Ordinal) && p.Contains("<expr>=? AND <expr><?", StringComparison.Ordinal));
        // a partial sort of Id ties within one AddedUtc is fine; sorting the whole category is not
        Assert.DoesNotContain(plan, p => p.Contains("USE TEMP B-TREE FOR ORDER BY", StringComparison.Ordinal));
    }

    List<string> Explain(string sql)
    {
        using var conn = new SqliteConnection($"Data Source={this.dbPath}");
        conn.Open();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "EXPLAIN QUERY PLAN " + sql.TrimEnd(';');
        foreach (var name in ParameterName().Matches(sql).Select(m => m.Value).Distinct())
        {
            object value = name switch
            {
                "@typeName" => "Film",
                "@p0" => "c1",
                _ when name.StartsWith("@ks", StringComparison.Ordinal) => 1050L,
                _ => "x"
            };
            cmd.Parameters.AddWithValue(name, value);
        }

        var plan = new List<string>();
        using var reader = cmd.ExecuteReader();
        while (reader.Read())
            plan.Add(reader.GetString(3));
        return plan;
    }

    [GeneratedRegex(@"@\w+")]
    private static partial Regex ParameterName();
}
