using System.Text.Json.Nodes;
using Microsoft.Data.Sqlite;
using Shiny.DocumentDb.Sqlite;
using Xunit;

namespace Shiny.DocumentDb.Tests;

/// <summary>
/// <see cref="DocumentStoreOptions.SkipTableInitialization"/> — pointing a store at a database it does
/// not own.
/// </summary>
/// <remarks>
/// The lazy <c>CREATE TABLE IF NOT EXISTS</c> runs on the first operation against a table whatever that
/// operation is, reads included, which is wrong in three situations that all look the same from here: a
/// read replica, a database account with no DDL rights, and a tool that has told the user it will not
/// change anything. A file-backed database is used rather than <c>:memory:</c> so a second connection
/// can check what is actually there.
/// </remarks>
public class SkipTableInitializationTests : IDisposable
{
    readonly string dbPath = Path.Combine(Path.GetTempPath(), $"skipinit_{Guid.NewGuid():N}.db");

    public void Dispose()
    {
        if (File.Exists(this.dbPath))
            File.Delete(this.dbPath);
    }

    DocumentStore Store(bool skip) => new(new DocumentStoreOptions
    {
        DatabaseProvider = new SqliteDatabaseProvider($"Data Source={this.dbPath}"),
        TableName = "documents",
        SkipTableInitialization = skip
    });

    async Task<bool> TableExists()
    {
        await using var connection = new SqliteConnection($"Data Source={this.dbPath}");
        await connection.OpenAsync();
        await using var cmd = connection.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'documents';";
        return Convert.ToInt64(await cmd.ExecuteScalarAsync()) > 0;
    }

    [Fact]
    public async Task ByDefault_AReadCreatesTheTable()
    {
        // The behaviour the flag exists to switch off: even a query is enough to run the DDL.
        using var store = this.Store(skip: false);
        await store.Collection("Order").Query().ToList();

        Assert.True(await this.TableExists());
    }

    [Fact]
    public async Task WhenSkipped_AReadAgainstAMissingTable_FailsRatherThanCreatingIt()
    {
        using var store = this.Store(skip: true);

        await Assert.ThrowsAsync<SqliteException>(() => store.Collection("Order").Query().ToList());
        Assert.False(await this.TableExists());
    }

    [Fact]
    public async Task WhenSkipped_AnExistingTable_IsQueriedNormally()
    {
        // Created by a store that is allowed to, then read by one that is not - the admin-tool shape.
        using (var owner = this.Store(skip: false))
        {
            await owner.Collection("Order").Insert(new JsonObject { ["id"] = "a", ["total"] = 120 });
            await owner.Collection("Order").Insert(new JsonObject { ["id"] = "b", ["total"] = 90 });
        }

        using var reader = this.Store(skip: true);
        var rows = await reader.Collection("Order").Query().Where("total:number > 100").ToList();

        Assert.Equal("a", Assert.Single(rows)["id"]!.GetValue<string>());
    }

    [Fact]
    public async Task WhenSkipped_NoWriteIsAttemptedAgainstTheSchema()
    {
        using (var owner = this.Store(skip: false))
            await owner.Collection("Order").Insert(new JsonObject { ["id"] = "a" });

        // A second table name the store has never touched must stay absent even after it is queried.
        using var reader = new DocumentStore(new DocumentStoreOptions
        {
            DatabaseProvider = new SqliteDatabaseProvider($"Data Source={this.dbPath}"),
            TableName = "audit",
            SkipTableInitialization = true
        });

        await Assert.ThrowsAsync<SqliteException>(() => reader.Collection("Entry").Query().Count());

        await using var connection = new SqliteConnection($"Data Source={this.dbPath}");
        await connection.OpenAsync();
        await using var cmd = connection.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM sqlite_master WHERE name = 'audit';";
        Assert.Equal(0L, Convert.ToInt64(await cmd.ExecuteScalarAsync()));
    }
}
