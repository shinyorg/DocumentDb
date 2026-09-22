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
    [Fact]
    public void DocumentMetadata_TranslatesToTheEnvelopeFields()
    {
        using var store = (IDisposable)CreateStore();
        var cutoff = new DateTimeOffset(2026, 1, 2, 8, 0, 0, TimeSpan.FromHours(5));
        var qs = ((IDocumentStore)store).Query(ctx.StampedNote)
            .Where(x => x.Metadata!.UpdatedAt > cutoff)
            .Where($"Metadata.CreatedAt <= {cutoff}")
            .OrderByDescending(x => x.Metadata!.CreatedAt)
            .ToQueryString();

        var node = JsonNode.Parse(qs.Sql)!.AsObject();
        // Top-level envelope dates, bound by instant (+05:00 → 03:00Z) — never a body path.
        Assert.DoesNotContain("data.metadata", qs.Sql);
        Assert.Contains("\"updatedAt\"", node["filter"]!.ToJsonString());
        Assert.Contains("\"createdAt\"", node["filter"]!.ToJsonString());
        Assert.Contains("2026-01-02T03:00:00Z", qs.Sql);
        Assert.Equal(-1, (int)node["sort"]!["createdAt"]!);
    }

    [Fact]
    public void DocumentMetadata_IsPersisted_IsNotQueryable()
    {
        using var store = (IDisposable)CreateStore();
        Assert.Throws<NotSupportedException>(() => ((IDocumentStore)store).Query(ctx.StampedNote)
            .Where(x => x.Metadata!.IsPersisted)
            .ToQueryString());
    }
}
