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
    public void DropIndex_RemovesIndexAndBackingComputedColumn()
    {
        var sql = provider.BuildDropIndexSql("idx_json_User_name", "documents");

        // Both the index AND the convention-named computed column must be dropped — the old code
        // produced `DROP INDEX IF EXISTS idx_…;` which is invalid SQL Server syntax (missing `ON`)
        // and leaked the computed column.
        Assert.Contains("DROP INDEX [idx_json_User_name] ON [documents]", sql);
        Assert.Contains("ALTER TABLE [documents] DROP COLUMN [cc_idx_json_User_name]", sql);
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
