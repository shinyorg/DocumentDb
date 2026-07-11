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

    // Regression (#6): a multi-Should base with a NOT must parenthesize the OR group before the AND-NOT is
    // folded in — otherwise AND-NOT (which binds tighter than OR in every dialect) lowers `a OR b NOT c` to
    // `a OR (b AND NOT c)`, wrongly returning docs that contain `a` and `c`.
    [Fact]
    public void Postgres_OrBase_WithNot_GroupsBeforeAndNot()
    {
        var p = new PostgreSqlDatabaseProvider("Host=localhost");
        var q = MatchOf(p, "orleans grain NOT saga").Query;
        Assert.Equal("(('orleans') | ('grain')) & !('saga')", q);
    }

    [Fact]
    public void SqlServer_OrBase_WithNot_GroupsBeforeAndNot()
    {
        var p = new SqlServerDatabaseProvider("Server=localhost");
        var q = MatchOf(p, "orleans grain NOT saga").Query;
        Assert.StartsWith("((", q);                       // the OR base is wrapped as a unit
        Assert.Contains(") AND NOT ", q);                  // ...before the AND NOT
    }

    [Fact]
    public void Oracle_OrBase_WithNot_GroupsBeforeAndNot()
    {
        IDatabaseProvider p = new OracleDatabaseProvider("User Id=x;Password=y;Data Source=z");
        var q = MatchOf(p, "orleans grain NOT saga").Query;
        Assert.StartsWith("((", q);
        Assert.Contains(") ~ ", q);                        // Oracle Text MINUS binds tighter than |, so group first
    }

    // Regression (#7): a nested OR group under a required (+) clause must be parenthesized as a whole, else
    // `(orleans OR grain) AND fox` collapses to `+"orleans" "grain" +"fox"` — silently making `grain` optional.
    [Fact]
    public void MySql_GroupedOr_UnderRequired_Parenthesizes()
    {
        var p = new MySqlDatabaseProvider("Server=localhost");
        var q = MatchOf(p, "(orleans OR grain) AND fox").Query;
        Assert.Contains("+(\"orleans\" \"grain\")", q);
        Assert.Contains("+\"fox\"", q);
    }

    [Fact]
    public void DuckDb_Composable_NotSupported()
    {
        IDatabaseProvider p = new DuckDbDatabaseProvider("DataSource=:memory:");
        Assert.Null(p.BuildFullTextMatchSql("Docs", "FtArticle", Mapping(), Q("orleans"), v => "@q"));
        Assert.Equal(FtCapabilities.None, p.FullTextQueryCapabilities);
    }
}
