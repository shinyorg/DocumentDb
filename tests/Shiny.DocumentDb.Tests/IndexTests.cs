using System.Text.Json;
using Microsoft.Data.Sqlite;
using Shiny.DocumentDb.Tests.Fixtures;
using Shiny.DocumentDb.Sqlite;
using Xunit;

namespace Shiny.DocumentDb.Tests;

public class IndexTests : IDisposable
{
    readonly SqliteDocumentStore store;
    readonly string connectionString;
    static readonly TestJsonContext ctx = new(new JsonSerializerOptions { PropertyNamingPolicy = JsonNamingPolicy.CamelCase });

    public IndexTests()
    {
        var dbName = $"IndexTest_{Guid.NewGuid():N}";
        this.connectionString = $"Data Source={dbName};Mode=Memory;Cache=Shared";
        this.store = new SqliteDocumentStore(new DocumentStoreOptions
        {
            DatabaseProvider = new SqliteDatabaseProvider(this.connectionString),
            JsonSerializerOptions = ctx.Options
        });
    }

    public void Dispose() => this.store.Dispose();

    [Fact]
    public async Task CreateIndex_SimpleProperty_AppearsInSqliteMaster()
    {
        await this.store.CreateIndexAsync<User>(u => u.Name, ctx.User);

        var indexes = await this.GetIndexNamesAsync();
        Assert.Contains("idx_json_User_name", indexes);
    }

    [Fact]
    public async Task CreateIndex_NestedProperty_AppearsInSqliteMaster()
    {
        await this.store.CreateIndexAsync<Order>(o => o.ShippingAddress.City, ctx.Order);

        var indexes = await this.GetIndexNamesAsync();
        Assert.Contains("idx_json_Order_shippingAddress_city", indexes);
    }

    [Fact]
    public async Task CreateIndex_IsIdempotent()
    {
        await this.store.CreateIndexAsync<User>(u => u.Name, ctx.User);
        await this.store.CreateIndexAsync<User>(u => u.Name, ctx.User);

        var indexes = await this.GetIndexNamesAsync();
        Assert.Single(indexes, i => i == "idx_json_User_name");
    }

    [Fact]
    public async Task DropIndex_RemovesFromSqliteMaster()
    {
        await this.store.CreateIndexAsync<User>(u => u.Name, ctx.User);
        await this.store.DropIndexAsync<User>(u => u.Name, ctx.User);

        var indexes = await this.GetIndexNamesAsync();
        Assert.DoesNotContain("idx_json_User_name", indexes);
    }

    [Fact]
    public async Task DropIndex_NonExistent_DoesNotThrow()
    {
        // Force initialization so the table exists
        await this.store.Count<User>();

        await this.store.DropIndexAsync<User>(u => u.Name, ctx.User);
    }

    [Fact]
    public async Task DropAllIndexes_RemovesOnlyJsonIndexesForType()
    {
        await this.store.CreateIndexAsync<User>(u => u.Name, ctx.User);
        await this.store.CreateIndexAsync<User>(u => u.Age, ctx.User);
        await this.store.CreateIndexAsync<Order>(o => o.ShippingAddress.City, ctx.Order);

        await this.store.DropAllIndexesAsync<User>();

        var indexes = await this.GetIndexNamesAsync();
        Assert.DoesNotContain("idx_json_User_name", indexes);
        Assert.DoesNotContain("idx_json_User_age", indexes);
        Assert.Contains("idx_json_Order_shippingAddress_city", indexes);
        Assert.Contains("idx_documents_typename", indexes);
    }

    [Fact]
    public async Task QueriesReturnCorrectResults_AfterIndexCreation()
    {
        await this.store.Insert(new User { Id = "u1", Name = "Alice", Age = 25 }, ctx.User);
        await this.store.Insert(new User { Id = "u2", Name = "Bob", Age = 35 }, ctx.User);

        await this.store.CreateIndexAsync<User>(u => u.Name, ctx.User);

        var results = await this.store.Query(ctx.User).Where(u => u.Name == "Alice").ToList();
        Assert.Single(results);
        Assert.Equal("Alice", results[0].Name);
    }

    async Task<List<string>> GetIndexNamesAsync()
    {
        await using var conn = new SqliteConnection(this.connectionString);
        await conn.OpenAsync();

        await using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT name FROM sqlite_master WHERE type = 'index' AND tbl_name = 'documents';";

        var names = new List<string>();
        await using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
            names.Add(reader.GetString(0));

        return names;
    }
}
