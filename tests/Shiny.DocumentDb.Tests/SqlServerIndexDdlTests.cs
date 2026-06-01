using Shiny.DocumentDb.SqlServer;
using Xunit;

namespace Shiny.DocumentDb.Tests;

// Pure DDL-string tests for the SqlServerDatabaseProvider — no live DB connection required.
public class SqlServerIndexDdlTests
{
    readonly SqlServerDatabaseProvider provider = new("Server=fake;");

    [Fact]
    public void CreateJsonIndex_AddsComputedColumnOverJsonPath()
    {
        var sql = provider.BuildCreateJsonIndexSql(
            indexName: "idx_json_User_name",
            tableName: "documents",
            jsonPath: "name",
            typeName: "User");

        // Must materialize a computed column extracting the JSON path, then index that column.
        Assert.Contains("ALTER TABLE [documents] ADD [cc_idx_json_User_name] AS CAST(JSON_VALUE(Data, '$.name')", sql);
        Assert.Contains("CREATE INDEX idx_json_User_name ON [documents] ([cc_idx_json_User_name]) WHERE TypeName = 'User'", sql);
    }

    [Fact]
    public void DropIndex_RemovesIndexAndBackingComputedColumns()
    {
        var sql = provider.BuildDropIndexSql("idx_json_User_name", "documents");

        // The drop must (1) discover the index's backing computed columns from sys.index_columns,
        // (2) drop the index, (3) drop those columns. Filter to the "cc_" convention so we never
        // accidentally drop user-defined computed columns that happen to be part of the index.
        Assert.Contains("FROM sys.index_columns", sql);
        Assert.Contains("c.is_computed = 1", sql);
        Assert.Contains("c.name LIKE 'cc[_]%'", sql);
        Assert.Contains("DROP INDEX [idx_json_User_name] ON [documents]", sql);
        Assert.Contains("ALTER TABLE [documents] DROP COLUMN [' + name + N']", sql);
        // Idempotent fallback: legacy single-column convention still drops cleanly when the index
        // is already gone but the computed column is orphaned.
        Assert.Contains("'cc_idx_json_User_name'", sql);
    }

    [Fact]
    public void CreateCompositeJsonIndex_AddsOneComputedColumnPerPath_AndIndexesAllOfThem()
    {
        var sql = provider.BuildCreateJsonIndexSql(
            indexName: "idx_json_User__country__age",
            tableName: "documents",
            jsonPaths: new[] { "country", "age" },
            typeName: "User");

        // One computed column per indexed path, suffixed by ordinal.
        Assert.Contains("ALTER TABLE [documents] ADD [cc_idx_json_User__country__age_0] AS CAST(JSON_VALUE(Data, '$.country')", sql);
        Assert.Contains("ALTER TABLE [documents] ADD [cc_idx_json_User__country__age_1] AS CAST(JSON_VALUE(Data, '$.age')", sql);
        // Single composite index over all of them, with the filtered TypeName predicate.
        Assert.Contains("CREATE INDEX idx_json_User__country__age ON [documents] ([cc_idx_json_User__country__age_0], [cc_idx_json_User__country__age_1]) WHERE TypeName = 'User'", sql);
    }

    [Fact]
    public void CreateCompositeJsonIndex_WithSinglePath_DelegatesToLegacyShape()
    {
        var sql = provider.BuildCreateJsonIndexSql(
            indexName: "idx_json_User_name",
            tableName: "documents",
            jsonPaths: new[] { "name" },
            typeName: "User");

        // Single-path overload preserves the pre-composite naming so existing schemas don't drift.
        Assert.Contains("ALTER TABLE [documents] ADD [cc_idx_json_User_name] AS CAST(JSON_VALUE(Data, '$.name')", sql);
        Assert.Contains("CREATE INDEX idx_json_User_name ON [documents] ([cc_idx_json_User_name])", sql);
    }

    [Fact]
    public void SupportsJsonMergePatch_IsFalse_SoFallbackPathIsUsed()
    {
        Assert.False(provider.SupportsJsonMergePatch);
    }

    [Fact]
    public void SelectDataForUpdate_UsesUpdLockHoldLock()
    {
        var sql = provider.BuildSelectDataForUpdateSql("documents");
        Assert.Contains("WITH (UPDLOCK, HOLDLOCK)", sql);
    }

    [Fact]
    public void BuildUpsertMergeSql_Throws_BecauseFallbackPathOwnsTheBehavior()
    {
        Assert.Throws<NotSupportedException>(() => provider.BuildUpsertMergeSql("documents"));
    }
}
