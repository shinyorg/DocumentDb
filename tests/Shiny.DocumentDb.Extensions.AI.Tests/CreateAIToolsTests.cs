using System.Text.Json;
using Microsoft.Extensions.AI;
using Shiny.DocumentDb.Extensions.AI.Tests.Fixtures;
using Shiny.DocumentDb.Sqlite;
using Xunit;

namespace Shiny.DocumentDb.Extensions.AI.Tests;

// Verifies AI tools can be built directly off a hand-constructed IDocumentStore — no DI container.
public class CreateAIToolsTests
{
    static (DocumentStore store, TestJsonContext ctx) NewStore()
    {
        var ctx = new TestJsonContext(new JsonSerializerOptions { PropertyNamingPolicy = JsonNamingPolicy.CamelCase });
        var opts = new DocumentStoreOptions
        {
            DatabaseProvider = new SqliteDatabaseProvider("Data Source=:memory:"),
            JsonSerializerOptions = ctx.Options,
            TableName = $"t{Guid.NewGuid():N}"
        };
        return (new DocumentStore(opts), ctx);
    }

    [Fact]
    public void CreateAITools_NoDI_BuildsToolsOverStore()
    {
        var (store, ctx) = NewStore();
        using (store)
        {
            var ai = store.CreateAITools(b =>
                b.AddType(ctx.Customer, capabilities: DocumentAICapabilities.ReadOnly));

            var names = ai.Tools.OfType<AIFunction>().Select(f => f.Name).ToList();
            Assert.Equal(4, names.Count);
            Assert.Contains("customer_get_by_id", names);
        }
    }

    [Fact]
    public void CreateAITools_EmptyRegistration_Throws()
    {
        var (store, _) = NewStore();
        using (store)
            Assert.Throws<InvalidOperationException>(() => store.CreateAITools(_ => { }));
    }
}
