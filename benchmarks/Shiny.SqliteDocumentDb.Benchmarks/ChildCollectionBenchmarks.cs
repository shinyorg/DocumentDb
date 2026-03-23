using BenchmarkDotNet.Attributes;
using Microsoft.Data.Sqlite;
using Shiny.DocumentDb;
using Shiny.DocumentDb.Sqlite;
using SQLite;

namespace Shiny.SqliteDocumentDb.Benchmarks;

/// <summary>
/// Benchmarks that highlight the document store advantage: nested objects and child
/// collections are stored/retrieved as a single JSON blob vs. 3 normalized tables
/// with manual joins in sqlite-net.
/// </summary>
[MemoryDiagnoser]
public class ChildCollectionInsertBenchmarks
{
    SqliteDocumentStore store = null!;
    SQLiteAsyncConnection db = null!;
    string storePath = null!;
    string sqlitePath = null!;

    [Params(10, 100, 1000)]
    public int Count { get; set; }

    [GlobalSetup]
    public async Task GlobalSetup()
    {
        storePath = Path.Combine(Path.GetTempPath(), $"bench_store_{Guid.NewGuid():N}.db");
        sqlitePath = Path.Combine(Path.GetTempPath(), $"bench_sqlite_{Guid.NewGuid():N}.db");

        store = new SqliteDocumentStore(new DocumentStoreOptions
        {
            DatabaseProvider = new SqliteDatabaseProvider($"Data Source={storePath}")
        });

        db = new SQLiteAsyncConnection(sqlitePath);
        await db.CreateTableAsync<SqliteOrder>();
        await db.CreateTableAsync<SqliteOrderLine>();
        await db.CreateTableAsync<SqliteOrderTag>();

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

    [GlobalCleanup]
    public void GlobalCleanup()
    {
        store.Dispose();
        db.GetConnection().Close();
        File.Delete(storePath);
        File.Delete(sqlitePath);
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
    string storePath = null!;
    string sqlitePath = null!;
    string knownDocId = null!;
    int knownSqliteOrderId;

    [GlobalSetup]
    public async Task GlobalSetup()
    {
        storePath = Path.Combine(Path.GetTempPath(), $"bench_store_{Guid.NewGuid():N}.db");
        sqlitePath = Path.Combine(Path.GetTempPath(), $"bench_sqlite_{Guid.NewGuid():N}.db");

        store = new SqliteDocumentStore(new DocumentStoreOptions
        {
            DatabaseProvider = new SqliteDatabaseProvider($"Data Source={storePath}")
        });

        db = new SQLiteAsyncConnection(sqlitePath);
        await db.CreateTableAsync<SqliteOrder>();
        await db.CreateTableAsync<SqliteOrderLine>();
        await db.CreateTableAsync<SqliteOrderTag>();

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
        }
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

    [GlobalCleanup]
    public void GlobalCleanup()
    {
        store.Dispose();
        db.GetConnection().Close();
        File.Delete(storePath);
        File.Delete(sqlitePath);
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
    string storePath = null!;
    string sqlitePath = null!;

    [Params(100, 1000)]
    public int Count { get; set; }

    [GlobalSetup]
    public async Task GlobalSetup()
    {
        storePath = Path.Combine(Path.GetTempPath(), $"bench_store_{Guid.NewGuid():N}.db");
        sqlitePath = Path.Combine(Path.GetTempPath(), $"bench_sqlite_{Guid.NewGuid():N}.db");

        store = new SqliteDocumentStore(new DocumentStoreOptions
        {
            DatabaseProvider = new SqliteDatabaseProvider($"Data Source={storePath}")
        });

        db = new SQLiteAsyncConnection(sqlitePath);
        await db.CreateTableAsync<SqliteOrder>();
        await db.CreateTableAsync<SqliteOrderLine>();
        await db.CreateTableAsync<SqliteOrderTag>();

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
        }
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

    [GlobalCleanup]
    public void GlobalCleanup()
    {
        store.Dispose();
        db.GetConnection().Close();
        File.Delete(storePath);
        File.Delete(sqlitePath);
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
    string storePath = null!;
    string sqlitePath = null!;

    [GlobalSetup]
    public async Task GlobalSetup()
    {
        storePath = Path.Combine(Path.GetTempPath(), $"bench_store_{Guid.NewGuid():N}.db");
        sqlitePath = Path.Combine(Path.GetTempPath(), $"bench_sqlite_{Guid.NewGuid():N}.db");

        store = new SqliteDocumentStore(new DocumentStoreOptions
        {
            DatabaseProvider = new SqliteDatabaseProvider($"Data Source={storePath}")
        });

        db = new SQLiteAsyncConnection(sqlitePath);
        await db.CreateTableAsync<SqliteOrder>();
        await db.CreateTableAsync<SqliteOrderLine>();
        await db.CreateTableAsync<SqliteOrderTag>();

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
        }
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

    [GlobalCleanup]
    public void GlobalCleanup()
    {
        store.Dispose();
        db.GetConnection().Close();
        File.Delete(storePath);
        File.Delete(sqlitePath);
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
