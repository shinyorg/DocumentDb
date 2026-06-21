using System.Text.Json;
using System.Text.Json.Nodes;
using Shiny.DocumentDb.MongoDb;
using Shiny.DocumentDb.Tests.Fixtures;
using Xunit;

namespace Shiny.DocumentDb.Tests.MongoDb;

// ToQueryString renders the translated BSON without touching the database — the MongoClient is
// created lazily and never connects, so these run without a live MongoDB instance.
public class MongoToQueryStringTests
{
    static readonly TestJsonContext ctx = new(new JsonSerializerOptions { PropertyNamingPolicy = JsonNamingPolicy.CamelCase });

    static IDocumentStore CreateStore() => new MongoDbDocumentStore(new MongoDbDocumentStoreOptions
    {
        ConnectionString = "mongodb://localhost:27017",
        DatabaseName = "test",
        JsonSerializerOptions = ctx.Options
    });

    [Fact]
    public void Where_RendersBsonFilter()
    {
        using var store = (IDisposable)CreateStore();
        var qs = ((IDocumentStore)store).Query(ctx.User)
            .Where(u => u.Age > 28)
            .ToQueryString();

        // No params — MongoDB inlines values into the BSON.
        Assert.Empty(qs.Parameters);

        // The rendered filter is valid JSON referencing the field and the $gt operator.
        var node = JsonNode.Parse(qs.Sql);
        Assert.NotNull(node);
        Assert.Contains("$gt", qs.Sql);
        Assert.Contains("data.age", qs.Sql);
        Assert.Contains("28", qs.Sql);
    }

    [Fact]
    public void OrderByAndPaginate_RendersFindCommand()
    {
        using var store = (IDisposable)CreateStore();
        var qs = ((IDocumentStore)store).Query(ctx.User)
            .Where(u => u.Age > 28)
            .OrderByDescending(u => u.Age)
            .Paginate(10, 5)
            .ToQueryString();

        var node = JsonNode.Parse(qs.Sql)!.AsObject();
        Assert.True(node.ContainsKey("filter"));
        Assert.True(node.ContainsKey("sort"));
        Assert.True(node.ContainsKey("skip"));
        Assert.True(node.ContainsKey("limit"));
    }
}
