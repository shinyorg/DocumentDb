using BenchmarkDotNet.Attributes;
using Shiny.DocumentDb;
using Shiny.DocumentDb.Sqlite;

namespace Shiny.DocumentDb.Benchmarks;

/// <summary>
/// Compares buffered Query (returns IReadOnlyList) vs streaming ToAsyncEnumerable
/// (returns IAsyncEnumerable) for flat POCOs.
/// </summary>
[MemoryDiagnoser]
public class StreamingGetAllBenchmarks
{
    SqliteDocumentStore store = null!;
    string storePath = null!;

    [Params(100, 1000)]
    public int Count { get; set; }

    [GlobalSetup]
    public async Task GlobalSetup()
    {
        storePath = Path.Combine(Path.GetTempPath(), $"bench_stream_{Guid.NewGuid():N}.db");

        store = new SqliteDocumentStore(new DocumentStoreOptions
        {
            DatabaseProvider = new SqliteDatabaseProvider($"Data Source={storePath}")
        });

        var ctx = BenchmarkJsonContext.Default;
        for (var i = 0; i < Count; i++)
        {
            var user = new BenchmarkUser { Name = $"User_{i}", Age = 20 + (i % 50), Email = $"user{i}@test.com" };
            await store.Insert(user, ctx.BenchmarkUser);
        }
    }

    [Benchmark(Description = "Query ToListAsync (buffered)")]
    public async Task<IReadOnlyList<BenchmarkUser>> GetAll_Buffered()
    {
        return await store.Query(BenchmarkJsonContext.Default.BenchmarkUser).ToList();
    }

    [Benchmark(Description = "Query ToAsyncEnumerable (streaming)")]
    public async Task<int> GetAll_Stream()
    {
        var count = 0;
        await foreach (var _ in store.Query(BenchmarkJsonContext.Default.BenchmarkUser).ToAsyncEnumerable().ConfigureAwait(false))
            count++;
        return count;
    }

    [GlobalCleanup]
    public void GlobalCleanup()
    {
        store.Dispose();
        File.Delete(storePath);
    }
}

/// <summary>
/// Compares buffered Query vs streaming ToAsyncEnumerable for nested objects
/// (orders with child collections).
/// </summary>
[MemoryDiagnoser]
public class StreamingGetAllNestedBenchmarks
{
    SqliteDocumentStore store = null!;
    string storePath = null!;

    [Params(100, 1000)]
    public int Count { get; set; }

    [GlobalSetup]
    public async Task GlobalSetup()
    {
        storePath = Path.Combine(Path.GetTempPath(), $"bench_stream_nested_{Guid.NewGuid():N}.db");

        store = new SqliteDocumentStore(new DocumentStoreOptions
        {
            DatabaseProvider = new SqliteDatabaseProvider($"Data Source={storePath}")
        });

        var ctx = BenchmarkJsonContext.Default;
        for (var i = 0; i < Count; i++)
        {
            await store.Insert(CreateOrder(i), ctx.BenchmarkOrder);
        }
    }

    [Benchmark(Description = "Query nested ToListAsync (buffered)")]
    public async Task<IReadOnlyList<BenchmarkOrder>> GetAll_Buffered()
    {
        return await store.Query(BenchmarkJsonContext.Default.BenchmarkOrder).ToList();
    }

    [Benchmark(Description = "Query nested ToAsyncEnumerable (streaming)")]
    public async Task<int> GetAll_Stream()
    {
        var count = 0;
        await foreach (var _ in store.Query(BenchmarkJsonContext.Default.BenchmarkOrder).ToAsyncEnumerable().ConfigureAwait(false))
            count++;
        return count;
    }

    [GlobalCleanup]
    public void GlobalCleanup()
    {
        store.Dispose();
        File.Delete(storePath);
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

/// <summary>
/// Compares buffered Query vs streaming QueryStream for expression-based queries.
/// Queries ~500 matching records from 1000 total.
/// </summary>
[MemoryDiagnoser]
public class StreamingQueryBenchmarks
{
    SqliteDocumentStore store = null!;
    string storePath = null!;

    [GlobalSetup]
    public async Task GlobalSetup()
    {
        storePath = Path.Combine(Path.GetTempPath(), $"bench_stream_query_{Guid.NewGuid():N}.db");

        store = new SqliteDocumentStore(new DocumentStoreOptions
        {
            DatabaseProvider = new SqliteDatabaseProvider($"Data Source={storePath}")
        });

        var ctx = BenchmarkJsonContext.Default;
        for (var i = 0; i < 1000; i++)
        {
            await store.Insert(CreateOrder(i), ctx.BenchmarkOrder);
        }
    }

    [Benchmark(Description = "Query Where ToListAsync (buffered)")]
    public async Task<IReadOnlyList<BenchmarkOrder>> Query_Buffered()
    {
        return await store.Query(BenchmarkJsonContext.Default.BenchmarkOrder)
            .Where(o => o.Status == "Shipped")
            .ToList();
    }

    [Benchmark(Description = "Query Where ToAsyncEnumerable (streaming)")]
    public async Task<int> Query_Stream()
    {
        var count = 0;
        await foreach (var _ in store.Query(BenchmarkJsonContext.Default.BenchmarkOrder)
            .Where(o => o.Status == "Shipped")
            .ToAsyncEnumerable()
            .ConfigureAwait(false))
        {
            count++;
        }
        return count;
    }

    [GlobalCleanup]
    public void GlobalCleanup()
    {
        store.Dispose();
        File.Delete(storePath);
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
