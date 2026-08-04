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

        // Sqlite provider quotes the table name, so logged SQL is `INSERT INTO "documents" ...`.
        Assert.Contains(this.loggedSql, s => s.Contains("CREATE TABLE IF NOT EXISTS"));
        Assert.Contains(this.loggedSql, s => s.Contains("INSERT INTO \"documents\""));
        Assert.Contains(this.loggedSql, s => s.Contains("SELECT Data FROM \"documents\""));
        Assert.Contains(this.loggedSql, s => s.Contains("SELECT COUNT(*)"));
        Assert.Contains(this.loggedSql, s => s.Contains("DELETE FROM \"documents\""));
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

/// <summary>
/// Regression tests for 5.1: SQLite reserved-word table identifiers must be quoted
/// in all generated DDL/DML. Mapping a type whose name collides with a reserved
/// keyword (Order, Group, User, ...) used to throw at CREATE TABLE / INSERT time.
/// </summary>
public class SqliteReservedTableQuotingTests
{
    // Mini reserved-word models so we don't pollute global TestModels with collisions.
    public class Order
    {
        public string Id { get; set; } = "";
        public string Customer { get; set; } = "";
        public decimal Total { get; set; }
    }

    public class Group
    {
        public string Id { get; set; } = "";
        public string Name { get; set; } = "";
    }

    static SqliteDocumentStore NewStore(Action<DocumentStoreOptions> configure)
    {
        var opts = new DocumentStoreOptions
        {
            DatabaseProvider = new SqliteDatabaseProvider("Data Source=:memory:")
        };
        configure(opts);
        return new SqliteDocumentStore(opts);
    }

    [Fact]
    public void QuoteTable_WrapsInDoubleQuotes()
    {
        var provider = new SqliteDatabaseProvider("Data Source=:memory:");
        Assert.Equal("\"Order\"", provider.QuoteTable("Order"));
    }

    [Fact]
    public void QuoteTable_EscapesEmbeddedDoubleQuotes()
    {
        var provider = new SqliteDatabaseProvider("Data Source=:memory:");
        Assert.Equal("\"weird\"\"name\"", provider.QuoteTable("weird\"name"));
    }

    [Fact]
    public async Task MapTypeToTable_ReservedWord_Order_Insert_DoesNotThrow()
    {
        using var store = NewStore(o => o.ConfigureDocument<Order>(cfg => cfg.Table = cfg.TypeName));
        await store.Insert(new Order { Id = "o1", Customer = "Alice", Total = 99.99m });

        var fetched = await store.Get<Order>("o1");
        Assert.NotNull(fetched);
        Assert.Equal("Alice", fetched.Customer);
    }

    [Fact]
    public async Task MapTypeToTable_ReservedWord_Order_FullCrud()
    {
        using var store = NewStore(o => o.ConfigureDocument<Order>(cfg => cfg.Table = cfg.TypeName));

        await store.Insert(new Order { Id = "o1", Customer = "Alice", Total = 10m });
        await store.Upsert(new Order { Id = "o1", Customer = "Alice", Total = 20m });
        await store.Update(new Order { Id = "o1", Customer = "Alice Updated", Total = 30m });

        var fetched = await store.Get<Order>("o1");
        Assert.Equal("Alice Updated", fetched!.Customer);
        // Round-trip via query path as well — exercises quoted SELECT.
        Assert.Equal(1, await store.Query<Order>().Count());
        Assert.True(await store.Query<Order>().Any());
        var list = await store.Query<Order>().ToList();
        Assert.Single(list);

        Assert.True(await store.Remove<Order>("o1"));
        Assert.Equal(0, await store.Query<Order>().Count());
    }

    [Fact]
    public async Task MapTypeToTable_ReservedWord_Order_Clear()
    {
        // Clear<T> uses DELETE on the (reserved) table name — verifies quoted DELETE.
        using var store = NewStore(o => o.ConfigureDocument<Order>(cfg => cfg.Table = cfg.TypeName));
        await store.Insert(new Order { Id = "o1", Customer = "Alice", Total = 10m });
        await store.Insert(new Order { Id = "o2", Customer = "Bob", Total = 20m });
        Assert.Equal(2, await store.Query<Order>().Count());

        await store.Clear<Order>();
        Assert.Equal(0, await store.Query<Order>().Count());
    }

    [Fact]
    public async Task MapTypeToTable_ReservedWord_Group_Works()
    {
        // Validates the fix isn't a one-off — another reserved word also works.
        using var store = NewStore(o => o.ConfigureDocument<Group>(cfg => cfg.Table = cfg.TypeName));
        await store.Insert(new Group { Id = "g1", Name = "Admins" });

        var fetched = await store.Get<Group>("g1");
        Assert.NotNull(fetched);
        Assert.Equal("Admins", fetched.Name);
    }

    [Fact]
    public async Task MapTypeToTable_ExplicitReservedTableName_Works()
    {
        // The explicit-name overload should also route through QuoteTable.
        using var store = NewStore(o => o.ConfigureDocument<Order>(cfg => cfg.Table = "Select"));
        await store.Insert(new Order { Id = "o1", Customer = "Alice", Total = 10m });
        Assert.Equal(1, await store.Query<Order>().Count());
    }

    [Fact]
    public async Task MapTypeToTable_ReservedWord_Order_BatchInsert()
    {
        using var store = NewStore(o => o.ConfigureDocument<Order>(cfg => cfg.Table = cfg.TypeName));
        var orders = Enumerable.Range(1, 25)
            .Select(i => new Order { Id = $"o{i}", Customer = $"C{i}", Total = i })
            .ToList();

        var count = await store.BatchInsert(orders);
        Assert.Equal(25, count);
        Assert.Equal(25, await store.Query<Order>().Count());
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

/// <summary>
/// Regression tests for binding <see cref="decimal"/> filter constants on SQLite. Microsoft.Data.Sqlite
/// binds a CLR decimal as TEXT, and SQLite type affinity ranks TEXT above REAL — so before the
/// provider normalized decimals to REAL, a numeric JSON value never compared against a decimal
/// parameter and every decimal predicate returned nothing.
/// </summary>
public class SqliteDecimalFilterTests : IDisposable
{
    static readonly TestJsonContext ctx = new(new JsonSerializerOptions { PropertyNamingPolicy = JsonNamingPolicy.CamelCase });
    readonly SqliteDocumentStore store;

    public SqliteDecimalFilterTests()
    {
        this.store = new SqliteDocumentStore(new DocumentStoreOptions
        {
            DatabaseProvider = new SqliteDatabaseProvider("Data Source=:memory:"),
            JsonSerializerOptions = ctx.Options,
            UseReflectionFallback = false
        });
    }

    public void Dispose() => this.store.Dispose();

    async Task SeedAsync()
    {
        await this.store.Insert(new Product { Id = "p1", Title = "Cheap", Price = 9.99m }, ctx.Product);
        await this.store.Insert(new Product { Id = "p2", Title = "Mid", Price = 49.00m }, ctx.Product);
        await this.store.Insert(new Product { Id = "p3", Title = "Pricey", Price = 129.95m }, ctx.Product);
    }

    async Task<List<string>> Ids(System.Linq.Expressions.Expression<Func<Product, bool>> p)
        => (await this.store.Query(ctx.Product).Where(p).ToList()).Select(x => x.Id).OrderBy(x => x).ToList();

    [Fact]
    public async Task GreaterThan()
    {
        await this.SeedAsync();
        Assert.Equal(["p2", "p3"], await this.Ids(x => x.Price > 10m));
        Assert.Equal(["p3"], await this.Ids(x => x.Price > 100m));
    }

    [Fact]
    public async Task LessThanOrEqualAndEqual()
    {
        await this.SeedAsync();
        Assert.Equal(["p1"], await this.Ids(x => x.Price <= 20m));
        Assert.Equal(["p2"], await this.Ids(x => x.Price == 49.00m));
    }
}
