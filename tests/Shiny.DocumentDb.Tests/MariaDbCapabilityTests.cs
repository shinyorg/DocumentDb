using System.Text.Json;
using Shiny.DocumentDb.MariaDb;
using Shiny.DocumentDb.Tests.Fixtures;
using Xunit;

namespace Shiny.DocumentDb.Tests;

/// <summary>
/// DB-free assertions of MariaDB's deliberate capability divergences from MySQL. MariaDB has no JSON_TABLE
/// (MDEV-16620) and no LATERAL, so the outer-correlated array unnest MySQL uses cannot be expressed — the
/// provider fails loud instead of emitting SQL that errors on the server.
/// </summary>
public class MariaDbCapabilityTests
{
    static readonly TestJsonContext ctx = new(new JsonSerializerOptions { PropertyNamingPolicy = JsonNamingPolicy.CamelCase });

    static IDocumentStore Store() => new DocumentStore(new DocumentStoreOptions
    {
        DatabaseProvider = new MariaDbDatabaseProvider("Server=localhost;Database=x"),
        JsonSerializerOptions = ctx.Options,
        TableName = "t"
    });

    [Fact]
    public void JsonEachFrom_ThrowsClearly()
    {
        var ex = Assert.Throws<NotSupportedException>(
            () => new MariaDbDatabaseProvider("Server=x").JsonEachFrom("Data", "lines"));
        Assert.Contains("JSON_TABLE", ex.Message);
    }

    [Fact]
    public void AnyOverCollection_InWhere_ThrowsAtBuild()
    {
        var store = Store();
        // ToQueryString builds the SQL through the provider without opening a connection, so the unnest is
        // rejected here rather than at execution time.
        Assert.Throws<NotSupportedException>(
            () => store.Query(ctx.Order).Where(o => o.Lines.Any(l => l.ProductName == "Gadget")).ToQueryString());
    }

    [Fact]
    public void CollectionAggregateProjection_ThrowsAtBuild()
    {
        var store = Store();
        Assert.Throws<NotSupportedException>(
            () => store.Query(ctx.Order)
                .Select(o => new OrderLineAggregates { Customer = o.CustomerName, TotalQty = o.Lines.Sum(l => l.Quantity) }, ctx.OrderLineAggregates)
                .ToQueryString());
    }
}
