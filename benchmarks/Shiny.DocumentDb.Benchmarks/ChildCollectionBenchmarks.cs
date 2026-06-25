using BenchmarkDotNet.Attributes;
using Dapper;
using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;
using Shiny.DocumentDb;
using Shiny.DocumentDb.Sqlite;
using SQLite;

namespace Shiny.DocumentDb.Benchmarks;

/// <summary>
/// Benchmarks that highlight the document store advantage: nested objects and child
/// collections are stored/retrieved as a single JSON blob vs. 3 normalized tables
/// with manual joins in sqlite-net, or EF Core entities with Include() joins.
/// </summary>
[MemoryDiagnoser]
public class ChildCollectionInsertBenchmarks
{
    SqliteDocumentStore store = null!;
    SQLiteAsyncConnection db = null!;
    BenchDbContext efContext = null!;
    SqliteConnection dapper = null!;
    string storePath = null!;
    string sqlitePath = null!;
    string efPath = null!;
    string dapperPath = null!;

    [Params(10, 100, 1000)]
    public int Count { get; set; }

    [GlobalSetup]
    public async Task GlobalSetup()
    {
        storePath = Path.Combine(Path.GetTempPath(), $"bench_store_{Guid.NewGuid():N}.db");
        sqlitePath = Path.Combine(Path.GetTempPath(), $"bench_sqlite_{Guid.NewGuid():N}.db");
        efPath = Path.Combine(Path.GetTempPath(), $"bench_ef_{Guid.NewGuid():N}.db");
        dapperPath = Path.Combine(Path.GetTempPath(), $"bench_dapper_{Guid.NewGuid():N}.db");

        store = new SqliteDocumentStore(new DocumentStoreOptions
        {
            DatabaseProvider = new SqliteDatabaseProvider($"Data Source={storePath}")
        });

        db = new SQLiteAsyncConnection(sqlitePath);
        await db.CreateTableAsync<SqliteOrder>();
        await db.CreateTableAsync<SqliteOrderLine>();
        await db.CreateTableAsync<SqliteOrderTag>();

        efContext = BenchDbContext.Create(efPath);

        dapper = DapperSetup.CreateOrders(dapperPath);

        // Force DocumentStore to initialize its table
        await store.Clear<BenchmarkOrder>();
    }

    [IterationSetup]
    public void IterationSetup()
    {
        using var conn = new SqliteConnection($"Data Source={storePath}");
        conn.Open();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "DELETE FROM documents;";
        cmd.ExecuteNonQuery();

        var sqliteConn = db.GetConnection();
        sqliteConn.DeleteAll<SqliteOrderTag>();
        sqliteConn.DeleteAll<SqliteOrderLine>();
        sqliteConn.DeleteAll<SqliteOrder>();

        efContext.Set<EfOrderTag>().ExecuteDelete();
        efContext.Set<EfOrderLine>().ExecuteDelete();
        efContext.Orders.ExecuteDelete();
        efContext.ChangeTracker.Clear();

        dapper.Execute("DELETE FROM OrderTags; DELETE FROM OrderLines; DELETE FROM Orders;");
    }

    [Benchmark(Description = "DocumentStore Insert (nested)")]
    public async Task DocumentStore_Insert()
    {
        var ctx = BenchmarkJsonContext.Default;
        for (var i = 0; i < Count; i++)
        {
            await store.Insert(CreateOrder(i), ctx.BenchmarkOrder);
        }
    }

    [Benchmark(Description = "sqlite-net Insert (3 tables)")]
    public async Task SqliteNet_Insert()
    {
        for (var i = 0; i < Count; i++)
        {
            var order = new SqliteOrder
            {
                DocId = Guid.NewGuid().ToString("N"),
                CustomerName = $"Customer_{i}",
                Status = i % 2 == 0 ? "Shipped" : "Pending",
                Street = $"{i} Main St",
                City = "Springfield",
                State = "IL",
                Zip = "62704"
            };
            await db.InsertAsync(order);

            for (var j = 0; j < 3; j++)
            {
                await db.InsertAsync(new SqliteOrderLine
                {
                    OrderId = order.Id,
                    ProductName = $"Product_{j}",
                    Quantity = j + 1,
                    UnitPrice = 9.99m + j
                });
            }

            await db.InsertAsync(new SqliteOrderTag { OrderId = order.Id, Tag = "priority" });
            await db.InsertAsync(new SqliteOrderTag { OrderId = order.Id, Tag = $"region-{i % 5}" });
        }
    }

    [Benchmark(Description = "EF Core Insert (3 tables)")]
    public async Task EfCore_Insert()
    {
        for (var i = 0; i < Count; i++)
        {
            efContext.Orders.Add(EfFactory.CreateOrder(i));
            await efContext.SaveChangesAsync();
        }
    }

    [Benchmark(Description = "Dapper Insert (3 tables)")]
    public async Task Dapper_Insert()
    {
        for (var i = 0; i < Count; i++)
            await DapperSetup.InsertOrderAsync(dapper, i);
    }

    [GlobalCleanup]
    public async Task GlobalCleanup()
    {
        store.Dispose();
        db.GetConnection().Close();
        await efContext.DisposeAsync();
        dapper.Dispose();
        File.Delete(storePath);
        File.Delete(sqlitePath);
        File.Delete(efPath);
        File.Delete(dapperPath);
    }

    static BenchmarkOrder CreateOrder(int i) => new()
    {
        CustomerName = $"Customer_{i}",
        Status = i % 2 == 0 ? "Shipped" : "Pending",
        ShippingAddress = new BenchmarkAddress
        {
            Street = $"{i} Main St",
            City = "Springfield",
            State = "IL",
            Zip = "62704"
        },
        Lines =
        [
            new() { ProductName = "Product_0", Quantity = 1, UnitPrice = 9.99m },
            new() { ProductName = "Product_1", Quantity = 2, UnitPrice = 10.99m },
            new() { ProductName = "Product_2", Quantity = 3, UnitPrice = 11.99m }
        ],
        Tags = ["priority", $"region-{i % 5}"]
    };
}

[MemoryDiagnoser]
public class ChildCollectionReadBenchmarks
{
    SqliteDocumentStore store = null!;
    SQLiteAsyncConnection db = null!;
    BenchDbContext efContext = null!;
    SqliteConnection dapper = null!;
    string storePath = null!;
    string sqlitePath = null!;
    string efPath = null!;
    string dapperPath = null!;
    string knownDocId = null!;
    int knownSqliteOrderId;
    int knownEfOrderId;
    long knownDapperOrderId;

    [GlobalSetup]
    public async Task GlobalSetup()
    {
        storePath = Path.Combine(Path.GetTempPath(), $"bench_store_{Guid.NewGuid():N}.db");
        sqlitePath = Path.Combine(Path.GetTempPath(), $"bench_sqlite_{Guid.NewGuid():N}.db");
        efPath = Path.Combine(Path.GetTempPath(), $"bench_ef_{Guid.NewGuid():N}.db");
        dapperPath = Path.Combine(Path.GetTempPath(), $"bench_dapper_{Guid.NewGuid():N}.db");

        store = new SqliteDocumentStore(new DocumentStoreOptions
        {
            DatabaseProvider = new SqliteDatabaseProvider($"Data Source={storePath}")
        });

        db = new SQLiteAsyncConnection(sqlitePath);
        await db.CreateTableAsync<SqliteOrder>();
        await db.CreateTableAsync<SqliteOrderLine>();
        await db.CreateTableAsync<SqliteOrderTag>();

        efContext = BenchDbContext.Create(efPath);

        dapper = DapperSetup.CreateOrders(dapperPath);

        var ctx = BenchmarkJsonContext.Default;
        for (var i = 0; i < 1000; i++)
        {
            var docOrder = CreateOrder(i);
            await store.Insert(docOrder, ctx.BenchmarkOrder);
            if (i == 500) knownDocId = docOrder.Id;

            var sqliteOrder = new SqliteOrder
            {
                DocId = Guid.NewGuid().ToString("N"),
                CustomerName = $"Customer_{i}",
                Status = i % 2 == 0 ? "Shipped" : "Pending",
                Street = $"{i} Main St", City = "Springfield", State = "IL", Zip = "62704"
            };
            await db.InsertAsync(sqliteOrder);
            if (i == 500) knownSqliteOrderId = sqliteOrder.Id;

            for (var j = 0; j < 3; j++)
            {
                await db.InsertAsync(new SqliteOrderLine
                {
                    OrderId = sqliteOrder.Id, ProductName = $"Product_{j}",
                    Quantity = j + 1, UnitPrice = 9.99m + j
                });
            }
            await db.InsertAsync(new SqliteOrderTag { OrderId = sqliteOrder.Id, Tag = "priority" });
            await db.InsertAsync(new SqliteOrderTag { OrderId = sqliteOrder.Id, Tag = $"region-{i % 5}" });

            var efOrder = EfFactory.CreateOrder(i);
            efContext.Orders.Add(efOrder);
            await efContext.SaveChangesAsync();
            if (i == 500) knownEfOrderId = efOrder.Id;

            var dapperOrderId = await DapperSetup.InsertOrderAsync(dapper, i);
            if (i == 500) knownDapperOrderId = dapperOrderId;
        }
        efContext.ChangeTracker.Clear();
    }

    [Benchmark(Description = "DocumentStore GetById (nested)")]
    public async Task<BenchmarkOrder?> DocumentStore_GetById()
    {
        return await store.Get<BenchmarkOrder>(knownDocId, BenchmarkJsonContext.Default.BenchmarkOrder);
    }

    [Benchmark(Description = "sqlite-net GetById (3 queries)")]
    public async Task<SqliteOrder?> SqliteNet_GetById()
    {
        var order = await db.GetAsync<SqliteOrder>(knownSqliteOrderId);
        // Must also load children — this is the overhead the document store avoids
        var _ = await db.Table<SqliteOrderLine>().Where(l => l.OrderId == knownSqliteOrderId).ToListAsync();
        var __ = await db.Table<SqliteOrderTag>().Where(t => t.OrderId == knownSqliteOrderId).ToListAsync();
        return order;
    }

    [Benchmark(Description = "EF Core GetById (Include, compiled)")]
    public async Task<EfOrder?> EfCore_GetById()
    {
        return await BenchDbContext.GetOrderById(efContext, knownEfOrderId);
    }

    [Benchmark(Description = "Dapper GetById (3 queries)")]
    public async Task<DapperOrder?> Dapper_GetById()
    {
        var order = await dapper.QuerySingleOrDefaultAsync<DapperOrder>(
            "SELECT Id, CustomerName, Status, Street, City, State, Zip FROM Orders WHERE Id = @id;",
            new { id = knownDapperOrderId });
        // Must also load children — the overhead the document store avoids
        var _ = (await dapper.QueryAsync<DapperOrderLine>(
            "SELECT Id, OrderId, ProductName, Quantity, UnitPrice FROM OrderLines WHERE OrderId = @id;",
            new { id = knownDapperOrderId })).AsList();
        var __ = (await dapper.QueryAsync<DapperOrderTag>(
            "SELECT Id, OrderId, Tag FROM OrderTags WHERE OrderId = @id;",
            new { id = knownDapperOrderId })).AsList();
        return order;
    }

    [GlobalCleanup]
    public async Task GlobalCleanup()
    {
        store.Dispose();
        db.GetConnection().Close();
        await efContext.DisposeAsync();
        dapper.Dispose();
        File.Delete(storePath);
        File.Delete(sqlitePath);
        File.Delete(efPath);
        File.Delete(dapperPath);
    }

    static BenchmarkOrder CreateOrder(int i) => new()
    {
        CustomerName = $"Customer_{i}",
        Status = i % 2 == 0 ? "Shipped" : "Pending",
        ShippingAddress = new BenchmarkAddress
        {
            Street = $"{i} Main St", City = "Springfield", State = "IL", Zip = "62704"
        },
        Lines =
        [
            new() { ProductName = "Product_0", Quantity = 1, UnitPrice = 9.99m },
            new() { ProductName = "Product_1", Quantity = 2, UnitPrice = 10.99m },
            new() { ProductName = "Product_2", Quantity = 3, UnitPrice = 11.99m }
        ],
        Tags = ["priority", $"region-{i % 5}"]
    };
}

[MemoryDiagnoser]
public class ChildCollectionGetAllBenchmarks
{
    SqliteDocumentStore store = null!;
    SQLiteAsyncConnection db = null!;
    BenchDbContext efContext = null!;
    SqliteConnection dapper = null!;
    string storePath = null!;
    string sqlitePath = null!;
    string efPath = null!;
    string dapperPath = null!;

    [Params(100, 1000)]
    public int Count { get; set; }

    [GlobalSetup]
    public async Task GlobalSetup()
    {
        storePath = Path.Combine(Path.GetTempPath(), $"bench_store_{Guid.NewGuid():N}.db");
        sqlitePath = Path.Combine(Path.GetTempPath(), $"bench_sqlite_{Guid.NewGuid():N}.db");
        efPath = Path.Combine(Path.GetTempPath(), $"bench_ef_{Guid.NewGuid():N}.db");
        dapperPath = Path.Combine(Path.GetTempPath(), $"bench_dapper_{Guid.NewGuid():N}.db");

        store = new SqliteDocumentStore(new DocumentStoreOptions
        {
            DatabaseProvider = new SqliteDatabaseProvider($"Data Source={storePath}")
        });

        db = new SQLiteAsyncConnection(sqlitePath);
        await db.CreateTableAsync<SqliteOrder>();
        await db.CreateTableAsync<SqliteOrderLine>();
        await db.CreateTableAsync<SqliteOrderTag>();

        efContext = BenchDbContext.Create(efPath);

        dapper = DapperSetup.CreateOrders(dapperPath);

        var ctx = BenchmarkJsonContext.Default;
        for (var i = 0; i < Count; i++)
        {
            await store.Insert(CreateOrder(i), ctx.BenchmarkOrder);

            var order = new SqliteOrder
            {
                DocId = Guid.NewGuid().ToString("N"),
                CustomerName = $"Customer_{i}",
                Status = i % 2 == 0 ? "Shipped" : "Pending",
                Street = $"{i} Main St", City = "Springfield", State = "IL", Zip = "62704"
            };
            await db.InsertAsync(order);

            for (var j = 0; j < 3; j++)
            {
                await db.InsertAsync(new SqliteOrderLine
                {
                    OrderId = order.Id, ProductName = $"Product_{j}",
                    Quantity = j + 1, UnitPrice = 9.99m + j
                });
            }
            await db.InsertAsync(new SqliteOrderTag { OrderId = order.Id, Tag = "priority" });
            await db.InsertAsync(new SqliteOrderTag { OrderId = order.Id, Tag = $"region-{i % 5}" });

            efContext.Orders.Add(EfFactory.CreateOrder(i));

            await DapperSetup.InsertOrderAsync(dapper, i);
        }
        await efContext.SaveChangesAsync();
        efContext.ChangeTracker.Clear();
    }

    [Benchmark(Description = "DocumentStore GetAll (nested)")]
    public async Task<IReadOnlyList<BenchmarkOrder>> DocumentStore_GetAll()
    {
        return await store.Query(BenchmarkJsonContext.Default.BenchmarkOrder).ToList();
    }

    [Benchmark(Description = "sqlite-net GetAll (3 tables + rehydrate)")]
    public async Task<List<SqliteOrder>> SqliteNet_GetAll()
    {
        var orders = await db.Table<SqliteOrder>().ToListAsync();
        // Must also load all children and match them to parents
        var lines = await db.Table<SqliteOrderLine>().ToListAsync();
        var tags = await db.Table<SqliteOrderTag>().ToListAsync();

        var linesByOrder = lines.ToLookup(l => l.OrderId);
        var tagsByOrder = tags.ToLookup(t => t.OrderId);

        // Simulates rehydration — the work an app must do with normalized tables
        foreach (var order in orders)
        {
            _ = linesByOrder[order.Id].ToList();
            _ = tagsByOrder[order.Id].ToList();
        }

        return orders;
    }

    [Benchmark(Description = "EF Core GetAll (Include, compiled)")]
    public async Task<List<EfOrder>> EfCore_GetAll()
    {
        return await BenchDbContext.GetAllOrders(efContext).ToListAsync();
    }

    [Benchmark(Description = "Dapper GetAll (3 tables + rehydrate)")]
    public async Task<List<DapperOrder>> Dapper_GetAll()
    {
        var orders = (await dapper.QueryAsync<DapperOrder>(
            "SELECT Id, CustomerName, Status, Street, City, State, Zip FROM Orders;")).AsList();
        // Must also load all children and match them to parents
        var lines = await dapper.QueryAsync<DapperOrderLine>(
            "SELECT Id, OrderId, ProductName, Quantity, UnitPrice FROM OrderLines;");
        var tags = await dapper.QueryAsync<DapperOrderTag>(
            "SELECT Id, OrderId, Tag FROM OrderTags;");

        var linesByOrder = lines.ToLookup(l => l.OrderId);
        var tagsByOrder = tags.ToLookup(t => t.OrderId);

        // Simulates rehydration — the work an app must do with normalized tables
        foreach (var order in orders)
        {
            _ = linesByOrder[order.Id].ToList();
            _ = tagsByOrder[order.Id].ToList();
        }

        return orders;
    }

    [GlobalCleanup]
    public async Task GlobalCleanup()
    {
        store.Dispose();
        db.GetConnection().Close();
        await efContext.DisposeAsync();
        dapper.Dispose();
        File.Delete(storePath);
        File.Delete(sqlitePath);
        File.Delete(efPath);
        File.Delete(dapperPath);
    }

    static BenchmarkOrder CreateOrder(int i) => new()
    {
        CustomerName = $"Customer_{i}",
        Status = i % 2 == 0 ? "Shipped" : "Pending",
        ShippingAddress = new BenchmarkAddress
        {
            Street = $"{i} Main St", City = "Springfield", State = "IL", Zip = "62704"
        },
        Lines =
        [
            new() { ProductName = "Product_0", Quantity = 1, UnitPrice = 9.99m },
            new() { ProductName = "Product_1", Quantity = 2, UnitPrice = 10.99m },
            new() { ProductName = "Product_2", Quantity = 3, UnitPrice = 11.99m }
        ],
        Tags = ["priority", $"region-{i % 5}"]
    };
}

[MemoryDiagnoser]
public class ChildCollectionQueryBenchmarks
{
    SqliteDocumentStore store = null!;
    SQLiteAsyncConnection db = null!;
    BenchDbContext efContext = null!;
    SqliteConnection dapper = null!;
    string storePath = null!;
    string sqlitePath = null!;
    string efPath = null!;
    string dapperPath = null!;

    [GlobalSetup]
    public async Task GlobalSetup()
    {
        storePath = Path.Combine(Path.GetTempPath(), $"bench_store_{Guid.NewGuid():N}.db");
        sqlitePath = Path.Combine(Path.GetTempPath(), $"bench_sqlite_{Guid.NewGuid():N}.db");
        efPath = Path.Combine(Path.GetTempPath(), $"bench_ef_{Guid.NewGuid():N}.db");
        dapperPath = Path.Combine(Path.GetTempPath(), $"bench_dapper_{Guid.NewGuid():N}.db");

        store = new SqliteDocumentStore(new DocumentStoreOptions
        {
            DatabaseProvider = new SqliteDatabaseProvider($"Data Source={storePath}")
        });

        db = new SQLiteAsyncConnection(sqlitePath);
        await db.CreateTableAsync<SqliteOrder>();
        await db.CreateTableAsync<SqliteOrderLine>();
        await db.CreateTableAsync<SqliteOrderTag>();

        efContext = BenchDbContext.Create(efPath);

        dapper = DapperSetup.CreateOrders(dapperPath);

        var ctx = BenchmarkJsonContext.Default;
        for (var i = 0; i < 1000; i++)
        {
            await store.Insert(CreateOrder(i), ctx.BenchmarkOrder);

            var order = new SqliteOrder
            {
                DocId = Guid.NewGuid().ToString("N"),
                CustomerName = $"Customer_{i}",
                Status = i % 2 == 0 ? "Shipped" : "Pending",
                Street = $"{i} Main St", City = "Springfield", State = "IL", Zip = "62704"
            };
            await db.InsertAsync(order);

            for (var j = 0; j < 3; j++)
            {
                await db.InsertAsync(new SqliteOrderLine
                {
                    OrderId = order.Id, ProductName = $"Product_{j}",
                    Quantity = j + 1, UnitPrice = 9.99m + j
                });
            }
            await db.InsertAsync(new SqliteOrderTag { OrderId = order.Id, Tag = "priority" });
            await db.InsertAsync(new SqliteOrderTag { OrderId = order.Id, Tag = $"region-{i % 5}" });

            efContext.Orders.Add(EfFactory.CreateOrder(i));

            await DapperSetup.InsertOrderAsync(dapper, i);
        }
        await efContext.SaveChangesAsync();
        efContext.ChangeTracker.Clear();
    }

    [Benchmark(Description = "DocumentStore Query (nested, by status)")]
    public async Task<IReadOnlyList<BenchmarkOrder>> DocumentStore_Query()
    {
        return await store.Query(BenchmarkJsonContext.Default.BenchmarkOrder)
            .Where(o => o.Status == "Shipped")
            .ToList();
    }

    [Benchmark(Description = "sqlite-net Query (3 tables + rehydrate)")]
    public async Task<List<SqliteOrder>> SqliteNet_Query()
    {
        var orders = await db.Table<SqliteOrder>().Where(o => o.Status == "Shipped").ToListAsync();
        var orderIds = orders.Select(o => o.Id).ToHashSet();

        // Must still load children for the matched orders
        var lines = await db.Table<SqliteOrderLine>().ToListAsync();
        var tags = await db.Table<SqliteOrderTag>().ToListAsync();

        var linesByOrder = lines.Where(l => orderIds.Contains(l.OrderId)).ToLookup(l => l.OrderId);
        var tagsByOrder = tags.Where(t => orderIds.Contains(t.OrderId)).ToLookup(t => t.OrderId);

        foreach (var order in orders)
        {
            _ = linesByOrder[order.Id].ToList();
            _ = tagsByOrder[order.Id].ToList();
        }

        return orders;
    }

    [Benchmark(Description = "EF Core Query (Include, compiled)")]
    public async Task<List<EfOrder>> EfCore_Query()
    {
        return await BenchDbContext.QueryOrdersByStatus(efContext, "Shipped").ToListAsync();
    }

    [Benchmark(Description = "Dapper Query (3 tables + rehydrate)")]
    public async Task<List<DapperOrder>> Dapper_Query()
    {
        var orders = (await dapper.QueryAsync<DapperOrder>(
            "SELECT Id, CustomerName, Status, Street, City, State, Zip FROM Orders WHERE Status = @status;",
            new { status = "Shipped" })).AsList();
        var orderIds = orders.Select(o => o.Id).ToHashSet();

        // Must still load children for the matched orders
        var lines = await dapper.QueryAsync<DapperOrderLine>(
            "SELECT Id, OrderId, ProductName, Quantity, UnitPrice FROM OrderLines;");
        var tags = await dapper.QueryAsync<DapperOrderTag>(
            "SELECT Id, OrderId, Tag FROM OrderTags;");

        var linesByOrder = lines.Where(l => orderIds.Contains(l.OrderId)).ToLookup(l => l.OrderId);
        var tagsByOrder = tags.Where(t => orderIds.Contains(t.OrderId)).ToLookup(t => t.OrderId);

        foreach (var order in orders)
        {
            _ = linesByOrder[order.Id].ToList();
            _ = tagsByOrder[order.Id].ToList();
        }

        return orders;
    }

    [GlobalCleanup]
    public async Task GlobalCleanup()
    {
        store.Dispose();
        db.GetConnection().Close();
        await efContext.DisposeAsync();
        dapper.Dispose();
        File.Delete(storePath);
        File.Delete(sqlitePath);
        File.Delete(efPath);
        File.Delete(dapperPath);
    }

    static BenchmarkOrder CreateOrder(int i) => new()
    {
        CustomerName = $"Customer_{i}",
        Status = i % 2 == 0 ? "Shipped" : "Pending",
        ShippingAddress = new BenchmarkAddress
        {
            Street = $"{i} Main St", City = "Springfield", State = "IL", Zip = "62704"
        },
        Lines =
        [
            new() { ProductName = "Product_0", Quantity = 1, UnitPrice = 9.99m },
            new() { ProductName = "Product_1", Quantity = 2, UnitPrice = 10.99m },
            new() { ProductName = "Product_2", Quantity = 3, UnitPrice = 11.99m }
        ],
        Tags = ["priority", $"region-{i % 5}"]
    };
}
