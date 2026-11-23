using System.Text.Json;
using System.Text.Json.Serialization;
using Microsoft.Data.Sqlite;
using Shiny.DocumentDb.Sqlite;
using Shiny.DocumentDb.Tests.Fixtures;
using Xunit;

namespace Shiny.DocumentDb.Tests;

/// <summary>A document whose key is DocKey, with an unrelated property that happens to be called Id.</summary>
public class CustomKeyModel
{
    public string DocKey { get; set; } = "";
    public string Id { get; set; } = "";
    public string Name { get; set; } = "";
}

[JsonSerializable(typeof(CustomKeyModel))]
[JsonSerializable(typeof(StringIdModel))]
[JsonSerializable(typeof(IntIdModel))]
public partial class IdColumnJsonContext : JsonSerializerContext;

/// <summary>
/// Equality and IN tests on a string id use the envelope Id column (the primary key) rather than
/// json_extract over the body, which SQLite can only answer by reading every row.
/// </summary>
public class IdColumnPredicateTests : IDisposable
{
    static readonly IdColumnJsonContext ctx = new(new JsonSerializerOptions { PropertyNamingPolicy = JsonNamingPolicy.CamelCase });
    readonly string dir = Path.Combine(Path.GetTempPath(), "docdb-idcol", Guid.NewGuid().ToString("N"));
    readonly string dbPath;
    readonly DocumentStore store;

    public IdColumnPredicateTests()
    {
        Directory.CreateDirectory(this.dir);
        this.dbPath = Path.Combine(this.dir, "test.db");
        var options = new DocumentStoreOptions
        {
            DatabaseProvider = new SqliteDatabaseProvider($"Data Source={this.dbPath}"),
            JsonSerializerOptions = ctx.Options,
            TableName = "docs"
        };
        options.ConfigureDocument<CustomKeyModel>(cfg => cfg.MapIdProperty(x => x.DocKey));
        this.store = new DocumentStore(options);
    }

    public void Dispose()
    {
        this.store.Dispose();
        SqliteConnection.ClearAllPools();
        Directory.Delete(this.dir, true);
    }

    async Task SeedAsync()
    {
        foreach (var id in new[] { "a", "b", "c", "d" })
            await this.store.Insert(new StringIdModel { Id = id, Name = $"n{id}" }, ctx.StringIdModel);
    }

    [Fact]
    public void Equality_and_in_on_a_string_id_use_the_id_column()
    {
        var eq = this.store.Query(ctx.StringIdModel).Where(x => x.Id == "a").ToQueryString();
        var reversed = this.store.Query(ctx.StringIdModel).Where(x => "a" == x.Id).ToQueryString();
        var notEq = this.store.Query(ctx.StringIdModel).Where(x => x.Id != "a").ToQueryString();
        var @in = this.store.Query(ctx.StringIdModel).WhereIn(x => x.Id, ["a", "b"]).ToQueryString();
        var notIn = this.store.Query(ctx.StringIdModel).WhereNotIn(x => x.Id, ["a", "b"]).ToQueryString();

        Assert.Contains("(Id = @p0)", eq.Sql);
        Assert.Contains("(@p0 = Id)", reversed.Sql);
        Assert.Contains("(Id <> @p0)", notEq.Sql.Replace("!=", "<>"));
        Assert.Contains("(Id IN (@p0, @p1))", @in.Sql);
        Assert.Contains("NOT ((Id IN (@p0, @p1)))", notIn.Sql);
        // != keeps its null branch on the body (x.Id != v also matches a null id), which is harmless: it can't seek anyway
        foreach (var sql in new[] { eq.Sql, reversed.Sql, @in.Sql, notIn.Sql })
            Assert.False(sql.Contains("'$.id'"), sql);
    }

    [Fact]
    public void Pattern_matches_on_the_id_stay_on_the_body()
    {
        var like = this.store.Query(ctx.StringIdModel).Where(x => x.Id.StartsWith("a")).ToQueryString();

        Assert.Contains("'$.id'", like.Sql);
    }

    [Fact]
    public void A_numeric_id_stays_on_the_body()
    {
        var qs = this.store.Query(ctx.IntIdModel).WhereIn(x => x.Id, [1, 2]).ToQueryString();
        Assert.Contains("'$.id'", qs.Sql);
        Assert.DoesNotContain("(Id IN", qs.Sql);
    }

    [Fact]
    public void A_mapped_id_property_uses_the_column_and_a_plain_id_property_does_not()
    {
        var byKey = this.store.Query(ctx.CustomKeyModel).Where(x => x.DocKey == "k1").ToQueryString();
        var byPlainId = this.store.Query(ctx.CustomKeyModel).Where(x => x.Id == "k1").ToQueryString();

        Assert.Contains("(Id = @p0)", byKey.Sql);
        Assert.Contains("'$.id'", byPlainId.Sql);
    }

    [Fact]
    public async Task Results_match_the_body_comparison()
    {
        await this.SeedAsync();

        var eq = await this.store.Query(ctx.StringIdModel).Where(x => x.Id == "b").ToList();
        var @in = await this.store.Query(ctx.StringIdModel).WhereIn(x => x.Id, ["d", "a", "missing"]).ToList();
        var notIn = await this.store.Query(ctx.StringIdModel).WhereNotIn(x => x.Id, ["a", "b"]).ToList();
        var combined = await this.store.Query(ctx.StringIdModel).Where(x => x.Id == "c" || x.Name == "na").ToList();
        var count = await this.store.Query(ctx.StringIdModel).WhereIn(x => x.Id, ["a", "b"]).Count();
        var deleted = await this.store.Query(ctx.StringIdModel).Where(x => x.Id == "d").ExecuteDelete();

        Assert.Equal(["b"], eq.Select(x => x.Id));
        Assert.Equal(["a", "d"], @in.Select(x => x.Id).OrderBy(x => x));
        Assert.Equal(["c", "d"], notIn.Select(x => x.Id).OrderBy(x => x));
        Assert.Equal(["a", "c"], combined.Select(x => x.Id).OrderBy(x => x));
        Assert.Equal(2, count);
        Assert.Equal(1, deleted);
        Assert.Null(await this.store.Get<StringIdModel>("d", ctx.StringIdModel));
    }

    [Fact]
    public async Task Mapped_id_property_results_are_by_key()
    {
        await this.store.Insert(new CustomKeyModel { DocKey = "k1", Id = "other", Name = "one" }, ctx.CustomKeyModel);
        await this.store.Insert(new CustomKeyModel { DocKey = "k2", Id = "k1", Name = "two" }, ctx.CustomKeyModel);

        var byKey = await this.store.Query(ctx.CustomKeyModel).WhereIn(x => x.DocKey, ["k1"]).ToList();
        var byPlainId = await this.store.Query(ctx.CustomKeyModel).Where(x => x.Id == "k1").ToList();

        Assert.Equal(["one"], byKey.Select(x => x.Name));
        Assert.Equal(["two"], byPlainId.Select(x => x.Name));
    }

    [Fact]
    public async Task In_on_the_id_is_answered_by_the_primary_key()
    {
        await this.SeedAsync();
        var qs = this.store.Query(ctx.StringIdModel).WhereIn(x => x.Id, ["a", "b"]).ToQueryString();

        using var conn = new SqliteConnection($"Data Source={this.dbPath}");
        conn.Open();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "EXPLAIN QUERY PLAN " + qs.Sql;
        foreach (var (name, value) in qs.Parameters)
            cmd.Parameters.AddWithValue(name, value ?? DBNull.Value);

        var plan = new List<string>();
        using var reader = cmd.ExecuteReader();
        while (reader.Read())
            plan.Add(reader.GetString(3));

        Assert.Contains(plan, p => p.Contains("PRIMARY KEY", StringComparison.OrdinalIgnoreCase) || p.Contains("sqlite_autoindex", StringComparison.OrdinalIgnoreCase));
    }
}
