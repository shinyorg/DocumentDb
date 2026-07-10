using Shiny.DocumentDb.DuckDb;
using Shiny.DocumentDb.Internal;
using Shiny.DocumentDb.Internal.FullText;
using Shiny.DocumentDb.MySql;
using Shiny.DocumentDb.Oracle;
using Shiny.DocumentDb.PostgreSql;
using Shiny.DocumentDb.SqlServer;
using Shiny.DocumentDb.Tests.Fixtures;
using Xunit;

namespace Shiny.DocumentDb.Tests;

/// <summary>
/// DB-free coverage of the relational full-text translators: exercises each provider's
/// BuildFullTextMatchSql / BuildFullTextScoreSql directly (no container needed) so the generated native
/// full-text syntax is validated even where the engine can't be spun up locally.
/// </summary>
public class LuceneProviderSqlTests
{
    static FullTextMapping Mapping()
    {
        var m = FullTextMappingFactory.FromExpressions<FtArticle>([a => a.Body], FullTextLanguage.English);
        m.JsonPaths = new[] { "body" };
        return m;
    }

    static FtQuery Q(string s) => LuceneQueryParser.Parse(s);

    // Captures the translated native query string handed to the parameter binder.
    static (string Sql, string Query) MatchOf(IDatabaseProvider p, string lucene)
    {
        string captured = "";
        var sql = p.BuildFullTextMatchSql("Docs", "FtArticle", Mapping(), Q(lucene), v => { captured = v; return "@q"; })!;
        return (sql, captured);
    }

    [Fact]
    public void Postgres_Match_And_Score()
    {
        var p = new PostgreSqlDatabaseProvider("Host=localhost");
        var (sql, q) = MatchOf(p, "orleans AND grain");
        Assert.Contains("@@ to_tsquery('english', @q)", sql);
        Assert.Contains("'orleans'", q);
        Assert.Contains("'grain'", q);
        Assert.Contains("&", q);

        var (_, orQ) = MatchOf(p, "orleans grain");
        Assert.Contains("|", orQ);

        Assert.Contains("ts_rank", p.BuildFullTextScoreSql("Docs", "FtArticle", Mapping(), Q("orleans"), v => "@q")!);
    }

    [Fact]
    public void Postgres_Prefix_And_Not()
    {
        var p = new PostgreSqlDatabaseProvider("Host=localhost");
        Assert.Contains("'distrib':*", MatchOf(p, "distrib*").Query);
        Assert.Contains("!", MatchOf(p, "orleans NOT grain").Query);
    }

    [Fact]
    public void MySql_BooleanMode()
    {
        var p = new MySqlDatabaseProvider("Server=localhost");
        var (sql, q) = MatchOf(p, "orleans AND grain");
        Assert.Contains("IN BOOLEAN MODE", sql);
        Assert.Contains("+\"orleans\"", q);
        Assert.Contains("+\"grain\"", q);

        Assert.Contains("-\"grain\"", MatchOf(p, "orleans NOT grain").Query);
        Assert.Contains("distrib*", MatchOf(p, "distrib*").Query);
    }

    [Fact]
    public void SqlServer_Contains_And_ContainsTable()
    {
        var p = new SqlServerDatabaseProvider("Server=localhost");
        var (sql, q) = MatchOf(p, "orleans AND grain");
        Assert.StartsWith("CONTAINS(", sql);
        Assert.Contains("\"orleans\"", q);
        Assert.Contains("AND", q);
        Assert.Contains("\"grain\"", q);

        Assert.Contains("\"distrib*\"", MatchOf(p, "distrib*").Query);
        Assert.Contains("AND NOT", MatchOf(p, "orleans NOT grain").Query);

        var score = p.BuildFullTextScoreSql("Docs", "FtArticle", Mapping(), Q("orleans"), v => "@q")!;
        Assert.Contains("CONTAINSTABLE", score);
        Assert.Contains("ct.[KEY] = FtKey", score);
    }

    [Fact]
    public void Oracle_Contains_MatchOnly()
    {
        IDatabaseProvider p = new OracleDatabaseProvider("User Id=x;Password=y;Data Source=z");
        var (sql, q) = MatchOf(p, "orleans AND grain");
        Assert.Contains("CONTAINS(", sql);
        Assert.Contains("{orleans}", q);
        Assert.Contains("&", q);
        Assert.Contains("{grain}", q);
        Assert.Contains("?orleans", MatchOf(p, "orleans~").Query);   // fuzzy supported on Oracle Text

        // SCORE(n) requires a co-located CONTAINS label, so standalone score is unsupported.
        Assert.Null(p.BuildFullTextScoreSql("Docs", "FtArticle", Mapping(), Q("orleans"), v => "@q"));
    }

    [Fact]
    public void DuckDb_Composable_NotSupported()
    {
        IDatabaseProvider p = new DuckDbDatabaseProvider("DataSource=:memory:");
        Assert.Null(p.BuildFullTextMatchSql("Docs", "FtArticle", Mapping(), Q("orleans"), v => "@q"));
        Assert.Equal(FtCapabilities.None, p.FullTextQueryCapabilities);
    }
}
