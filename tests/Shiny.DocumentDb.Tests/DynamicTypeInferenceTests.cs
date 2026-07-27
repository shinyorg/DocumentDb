using Shiny.DocumentDb.CockroachDb;
using Shiny.DocumentDb.DuckDb;
using Shiny.DocumentDb.MariaDb;
using Shiny.DocumentDb.MySql;
using Shiny.DocumentDb.Oracle;
using Shiny.DocumentDb.PostgreSql;
using Shiny.DocumentDb.Sqlite;
using Shiny.DocumentDb.SqlServer;
using Xunit;

namespace Shiny.DocumentDb.Tests;

/// <summary>
/// Type inference asserted on the <em>emitted SQL</em>, per dialect, without touching a database — the
/// cheapest possible guard on the whole design.
/// </summary>
/// <remarks>
/// The failure this catches is silent and provider-specific: an unhinted numeric comparison on a
/// schema-free field extracts as text, and on every provider whose plain JSON extract returns text that
/// either compares lexicographically (<c>"100" &lt; "9"</c>) or, for PostgreSQL, fails outright with
/// <c>42883 operator does not exist</c>. SQLite happens to be right either way, so a SQLite-only test would
/// prove nothing.
/// </remarks>
public class DynamicTypeInferenceTests
{
    static DocumentStore Store(IDatabaseProvider provider)
        => new(new DocumentStoreOptions { DatabaseProvider = provider, TableName = "docs" });

    // ToQueryString() renders SQL without executing, so these need no live server.
    public static TheoryData<string, IDatabaseProvider, string> NumericCasts => new()
    {
        { "PostgreSQL", new PostgreSqlDatabaseProvider("Host=localhost"), "::BIGINT" },
        { "CockroachDB", new CockroachDbDatabaseProvider("Host=localhost"), "::BIGINT" },
        { "MySQL", new MySqlDatabaseProvider("Server=localhost"), "AS SIGNED" },
        { "MariaDB", new MariaDbDatabaseProvider("Server=localhost"), "AS SIGNED" },
        { "SQL Server", new SqlServerDatabaseProvider("Server=localhost"), "AS BIGINT" },
        { "DuckDB", new DuckDbDatabaseProvider("Data Source=:memory:"), "AS BIGINT" },
        { "Oracle", new OracleDatabaseProvider("Data Source=localhost"), "RETURNING NUMBER" }
    };

    [Theory, MemberData(nameof(NumericCasts))]
    public void InferredFromTheOtherOperand_EmitsANumericExtraction(string dialect, IDatabaseProvider provider, string expectedCast)
    {
        using var store = Store(provider);

        var sql = store.Collection("orders").Query().Where("total > 100").ToQueryString().Sql;

        Assert.True(sql.Contains(expectedCast, StringComparison.Ordinal),
            $"{dialect}: expected a numeric extraction containing '{expectedCast}' but got: {sql}");
    }

    [Theory, MemberData(nameof(NumericCasts))]
    public void ExplicitHint_ChangesTheExtraction_WhereNothingCanBeInferred(string dialect, IDatabaseProvider provider, string expectedCast)
    {
        using var store = Store(provider);

        // An OrderBy has no other operand to infer from, which is exactly where the hint is required. Each
        // dialect spells its numeric cast differently (::DOUBLE PRECISION, AS FLOAT, + 0, RETURNING NUMBER),
        // so assert the property that matters rather than the spelling: the hint must change the SQL.
        var hinted = store.Collection("orders").Query().OrderBy("total:number").ToQueryString().Sql;
        var unhinted = store.Collection("orders").Query().OrderBy("total").ToQueryString().Sql;

        Assert.False(string.Equals(hinted, unhinted, StringComparison.Ordinal),
            $"{dialect}: ':number' did not change the sort key extraction: {hinted}");
    }

    [Theory, MemberData(nameof(NumericCasts))]
    public void UnhintedOrderBy_StaysText(string dialect, IDatabaseProvider provider, string expectedCast)
    {
        using var store = Store(provider);

        var sql = store.Collection("orders").Query().OrderBy("total").ToQueryString().Sql;

        // The documented divergence: with nothing to infer from, an unhinted sort key extracts as text and
        // therefore sorts lexicographically on these providers. Asserted so the behaviour cannot drift
        // silently in either direction.
        Assert.False(sql.Contains(expectedCast, StringComparison.Ordinal),
            $"{dialect}: an unhinted OrderBy unexpectedly emitted a numeric cast: {sql}");
    }

    [Fact]
    public void Sqlite_IsNumericallyCorrectWithoutACast()
    {
        using var store = Store(new SqliteDatabaseProvider("Data Source=:memory:"));

        var sql = store.Collection("orders").Query().Where("total > 100").ToQueryString().Sql;

        // SQLite's json_extract preserves the JSON number, so the typed extraction is the plain one — the
        // emitted SQL must stay byte-identical to the untyped form rather than growing a redundant CAST.
        Assert.Contains("json_extract(Data, '$.total')", sql);
        Assert.DoesNotContain("CAST", sql);
    }

    [Fact]
    public void StringFloor_EmitsThePlainExtract()
    {
        using var store = Store(new PostgreSqlDatabaseProvider("Host=localhost"));

        var hinted = store.Collection("orders").Query().Where("name:string == 'bob'").ToQueryString().Sql;
        var inferred = store.Collection("orders").Query().Where("name == 'bob'").ToQueryString().Sql;

        // The floor is string, not object: the emitted SQL is identical to an untyped extract, but the CLR
        // expression is well-typed so the comparison constructs at all.
        Assert.Equal(hinted, inferred);
        Assert.DoesNotContain("::BIGINT", inferred);
    }

    [Fact]
    public void FunctionArgument_InfersFromTheFunction()
    {
        using var store = Store(new PostgreSqlDatabaseProvider("Host=localhost"));

        var sql = store.Collection("orders").Query().Where("abs(total) > 5").ToQueryString().Sql;

        // abs() requires a number, so the field is retyped from the function's argument rather than from a
        // literal — rule 2 of the inference contract.
        Assert.Contains("DOUBLE PRECISION", sql);
    }

    [Fact]
    public void InterpolatedPlaceholder_InfersFromTheRuntimeType()
    {
        using var store = Store(new PostgreSqlDatabaseProvider("Host=localhost"));

        var min = 100;
        var sql = store.Collection("orders").Query().Where($"total > {min}").ToQueryString().Sql;

        Assert.Contains("::BIGINT", sql);
    }

    [Fact]
    public void UnknownHint_IsRejected()
    {
        using var store = Store(new SqliteDatabaseProvider("Data Source=:memory:"));

        Assert.ThrowsAny<ArgumentException>(
            () => store.Collection("orders").Query().Where("total:money > 1").ToQueryString());
    }
}
