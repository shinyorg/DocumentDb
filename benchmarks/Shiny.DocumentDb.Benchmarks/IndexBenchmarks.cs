using BenchmarkDotNet.Attributes;
using Shiny.DocumentDb;
using Shiny.DocumentDb.Sqlite;

namespace Shiny.DocumentDb.Benchmarks;

/// <summary>
/// Compares query performance with and without a json_extract index on the Name property.
/// Both benchmarks query the same 1000-record dataset for a single matching document.
/// </summary>
[MemoryDiagnoser]
public class IndexQueryBenchmarks
{
    SqliteDocumentStore storeNoIndex = null!;
    SqliteDocumentStore storeWithIndex = null!;
    string noIndexPath = null!;
    string indexPath = null!;

    [GlobalSetup]
    public async Task GlobalSetup()
    {
        noIndexPath = Path.Combine(Path.GetTempPath(), $"bench_noindex_{Guid.NewGuid():N}.db");
        indexPath = Path.Combine(Path.GetTempPath(), $"bench_index_{Guid.NewGuid():N}.db");

        storeNoIndex = new SqliteDocumentStore(new DocumentStoreOptions
        {
            DatabaseProvider = new SqliteDatabaseProvider($"Data Source={noIndexPath}")
        });
        storeWithIndex = new SqliteDocumentStore(new DocumentStoreOptions
        {
            DatabaseProvider = new SqliteDatabaseProvider($"Data Source={indexPath}")
        });

        var ctx = BenchmarkJsonContext.Default;
        for (var i = 0; i < 1000; i++)
        {
            var user = new BenchmarkUser { Name = $"User_{i}", Age = 20 + (i % 50), Email = $"user{i}@test.com" };
            await storeNoIndex.Insert(user, ctx.BenchmarkUser);
            await storeWithIndex.Insert(user, ctx.BenchmarkUser);
        }

        await storeWithIndex.CreateIndexAsync<BenchmarkUser>(u => u.Name, ctx.BenchmarkUser);
    }

    [Benchmark(Description = "Query without index")]
    public async Task<IReadOnlyList<BenchmarkUser>> Query_NoIndex()
    {
        return await storeNoIndex.Query(BenchmarkJsonContext.Default.BenchmarkUser)
            .Where(u => u.Name == "User_500")
            .ToList();
    }

    [Benchmark(Description = "Query with index")]
    public async Task<IReadOnlyList<BenchmarkUser>> Query_WithIndex()
    {
        return await storeWithIndex.Query(BenchmarkJsonContext.Default.BenchmarkUser)
            .Where(u => u.Name == "User_500")
            .ToList();
    }

    [GlobalCleanup]
    public void GlobalCleanup()
    {
        storeNoIndex.Dispose();
        storeWithIndex.Dispose();
        File.Delete(noIndexPath);
        File.Delete(indexPath);
    }
}

/// <summary>
/// Compares nested object query performance with and without a json_extract index
/// on a nested property (ShippingAddress.City).
/// </summary>
[MemoryDiagnoser]
public class IndexNestedQueryBenchmarks
{
    SqliteDocumentStore storeNoIndex = null!;
    SqliteDocumentStore storeWithIndex = null!;
    string noIndexPath = null!;
    string indexPath = null!;

    [GlobalSetup]
    public async Task GlobalSetup()
    {
        noIndexPath = Path.Combine(Path.GetTempPath(), $"bench_noindex_nested_{Guid.NewGuid():N}.db");
        indexPath = Path.Combine(Path.GetTempPath(), $"bench_index_nested_{Guid.NewGuid():N}.db");

        var ctx = BenchmarkJsonContext.Default;
        storeNoIndex = new SqliteDocumentStore(new DocumentStoreOptions
        {
            DatabaseProvider = new SqliteDatabaseProvider($"Data Source={noIndexPath}"),
            JsonSerializerOptions = ctx.Options
        });
        storeWithIndex = new SqliteDocumentStore(new DocumentStoreOptions
        {
            DatabaseProvider = new SqliteDatabaseProvider($"Data Source={indexPath}"),
            JsonSerializerOptions = ctx.Options
        });
        var cities = new[] { "Portland", "Seattle", "Denver", "Austin", "Boston" };
        for (var i = 0; i < 1000; i++)
        {
            var order = CreateOrder(i, cities[i % cities.Length]);
            await storeNoIndex.Insert(order, ctx.BenchmarkOrder);
            await storeWithIndex.Insert(order, ctx.BenchmarkOrder);
        }

        await storeWithIndex.CreateIndexAsync<BenchmarkOrder>(o => o.ShippingAddress.City, ctx.BenchmarkOrder);
    }

    [Benchmark(Description = "Nested query without index")]
    public async Task<IReadOnlyList<BenchmarkOrder>> Query_NoIndex()
    {
        return await storeNoIndex.Query(BenchmarkJsonContext.Default.BenchmarkOrder)
            .Where(o => o.ShippingAddress.City == "Portland")
            .ToList();
    }

    [Benchmark(Description = "Nested query with index")]
    public async Task<IReadOnlyList<BenchmarkOrder>> Query_WithIndex()
    {
        return await storeWithIndex.Query(BenchmarkJsonContext.Default.BenchmarkOrder)
            .Where(o => o.ShippingAddress.City == "Portland")
            .ToList();
    }

    [GlobalCleanup]
    public void GlobalCleanup()
    {
        storeNoIndex.Dispose();
        storeWithIndex.Dispose();
        File.Delete(noIndexPath);
        File.Delete(indexPath);
    }

    static BenchmarkOrder CreateOrder(int i, string city) => new()
    {
        CustomerName = $"Customer_{i}",
        Status = i % 2 == 0 ? "Shipped" : "Pending",
        ShippingAddress = new BenchmarkAddress
        {
            Street = $"{i} Main St", City = city, State = "XX", Zip = "00000"
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
