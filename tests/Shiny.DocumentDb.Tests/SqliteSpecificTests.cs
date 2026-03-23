using System.Text.Json;
using Shiny.DocumentDb.Tests.Fixtures;
using Shiny.DocumentDb.Sqlite;
using Xunit;

namespace Shiny.DocumentDb.Tests;

/// <summary>
/// Tests that use raw SQLite-specific SQL (json_extract, json_each, etc.)
/// or SQLite internals (sqlite_master). These cannot run on other providers.
/// </summary>
public class LoggingTests : IDisposable
{
    readonly List<string> loggedSql = [];
    readonly SqliteDocumentStore store;

    public LoggingTests()
    {
        this.store = new SqliteDocumentStore(new DocumentStoreOptions
        {
            DatabaseProvider = new SqliteDatabaseProvider("Data Source=:memory:"),
            Logging = sql => this.loggedSql.Add(sql)
        });
    }

    public void Dispose() => this.store.Dispose();

    [Fact]
    public async Task Logging_CapturesSqlStatements()
    {
        var user = new User { Id = "log-1", Name = "Allan", Age = 30 };
        await this.store.Insert(user);
        await this.store.Get<User>(user.Id);
        await this.store.Count<User>();
        await this.store.Remove<User>(user.Id);

        Assert.Contains(this.loggedSql, s => s.Contains("CREATE TABLE IF NOT EXISTS"));
        Assert.Contains(this.loggedSql, s => s.Contains("INSERT INTO documents"));
        Assert.Contains(this.loggedSql, s => s.Contains("SELECT Data FROM documents"));
        Assert.Contains(this.loggedSql, s => s.Contains("SELECT COUNT(*)"));
        Assert.Contains(this.loggedSql, s => s.Contains("DELETE FROM documents"));
    }
}

public class SqliteResolverRawSqlTests : IDisposable
{
    readonly SqliteDocumentStore store;

    public SqliteResolverRawSqlTests()
    {
        var ctx = new TestJsonContext(new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase
        });
        this.store = new SqliteDocumentStore(new DocumentStoreOptions
        {
            DatabaseProvider = new SqliteDatabaseProvider("Data Source=:memory:"),
            JsonSerializerOptions = ctx.Options
        });
    }

    public void Dispose() => this.store.Dispose();

    [Fact]
    public async Task Query_WithResolver_UsesTypeInfo_RawSql()
    {
        await this.store.Insert(new User { Id = "u1", Name = "Alice", Age = 25 });
        await this.store.Insert(new User { Id = "u2", Name = "Bob", Age = 35 });

        var results = await this.store.Query<User>(
            "json_extract(Data, '$.age') > @minAge",
            parameters: new { minAge = 30 });

        Assert.Single(results);
        Assert.Equal("Bob", results[0].Name);
    }
}

public class SqliteStreamRawSqlTests : IDisposable
{
    readonly SqliteDocumentStore store;
    static readonly TestJsonContext ctx = new(new JsonSerializerOptions { PropertyNamingPolicy = JsonNamingPolicy.CamelCase });

    public SqliteStreamRawSqlTests()
    {
        this.store = new SqliteDocumentStore(new DocumentStoreOptions
        {
            DatabaseProvider = new SqliteDatabaseProvider("Data Source=:memory:"),
            JsonSerializerOptions = ctx.Options
        });
    }

    public void Dispose() => this.store.Dispose();

    [Fact]
    public async Task Stream_RawSql()
    {
        await this.store.Insert(new User { Id = "u1", Name = "Alice", Age = 25, Email = "alice@test.com" }, ctx.User);
        await this.store.Insert(new User { Id = "u2", Name = "Bob", Age = 35 }, ctx.User);
        await this.store.Insert(new User { Id = "u3", Name = "Charlie", Age = 25 }, ctx.User);

        var results = new List<User>();
        await foreach (var user in this.store.QueryStream<User>(
            "json_extract(Data, '$.name') = @name",
            ctx.User,
            new { name = "Alice" }))
        {
            results.Add(user);
        }

        Assert.Single(results);
        Assert.Equal("Alice", results[0].Name);
    }
}
