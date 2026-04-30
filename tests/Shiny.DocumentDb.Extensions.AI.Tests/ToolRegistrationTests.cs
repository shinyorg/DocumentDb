using System.Text.Json;
using Microsoft.Extensions.AI;
using Microsoft.Extensions.DependencyInjection;
using Shiny.DocumentDb.Extensions.AI.Tests.Fixtures;
using Shiny.DocumentDb.Sqlite;
using Xunit;

namespace Shiny.DocumentDb.Extensions.AI.Tests;

public class ToolRegistrationTests
{
    TestJsonContext CreateContext() => new(new JsonSerializerOptions
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase
    });

    [Fact]
    public void All_Capabilities_Creates_Seven_Tools_Per_Type()
    {
        using var fixture = new AIToolsFixture();
        var customerTools = fixture.AITools.Tools
            .OfType<AIFunction>()
            .Where(f => f.Name.StartsWith("customer_"))
            .ToList();
        Assert.Equal(7, customerTools.Count);
    }

    [Fact]
    public void ReadOnly_Capabilities_Creates_Four_Tools()
    {
        using var fixture = new AIToolsFixture(
            customerCaps: DocumentAICapabilities.ReadOnly,
            productCaps: DocumentAICapabilities.ReadOnly);

        var customerTools = fixture.AITools.Tools
            .OfType<AIFunction>()
            .Where(f => f.Name.StartsWith("customer_"))
            .ToList();

        Assert.Equal(4, customerTools.Count);
        Assert.Contains(customerTools, f => f.Name == "customer_get_by_id");
        Assert.Contains(customerTools, f => f.Name == "customer_query");
        Assert.Contains(customerTools, f => f.Name == "customer_count");
        Assert.Contains(customerTools, f => f.Name == "customer_aggregate");
    }

    [Fact]
    public void Single_Capability_Creates_One_Tool()
    {
        using var fixture = new AIToolsFixture(
            customerCaps: DocumentAICapabilities.Get,
            productCaps: DocumentAICapabilities.Get);

        var customerTools = fixture.AITools.Tools
            .OfType<AIFunction>()
            .Where(f => f.Name.StartsWith("customer_"))
            .ToList();

        Assert.Single(customerTools);
        Assert.Equal("customer_get_by_id", customerTools[0].Name);
    }

    [Fact]
    public void Tool_Names_Use_Slugified_Type_Names()
    {
        using var fixture = new AIToolsFixture();
        var names = fixture.AITools.Tools
            .OfType<AIFunction>()
            .Select(f => f.Name)
            .ToList();

        Assert.Contains("customer_get_by_id", names);
        Assert.Contains("product_query", names);
    }

    [Fact]
    public void Custom_Name_Overrides_Slug()
    {
        var ctx = CreateContext();
        var services = new ServiceCollection();
        services.AddDocumentStore(opts =>
        {
            opts.DatabaseProvider = new SqliteDatabaseProvider("Data Source=:memory:");
            opts.JsonSerializerOptions = ctx.Options;
        });
        services.AddDocumentStoreAITools(b =>
        {
            b.AddType(ctx.Customer, name: "clients", capabilities: DocumentAICapabilities.Get);
        });
        var sp = services.BuildServiceProvider();
        var tools = sp.GetRequiredService<DocumentStoreAITools>();
        var fn = tools.Tools.OfType<AIFunction>().Single();
        Assert.Equal("clients_get_by_id", fn.Name);
        (sp.GetRequiredService<IDocumentStore>() as IDisposable)?.Dispose();
    }

    [Fact]
    public void Duplicate_Slug_Throws()
    {
        var ctx = CreateContext();
        var services = new ServiceCollection();
        services.AddDocumentStore(opts =>
        {
            opts.DatabaseProvider = new SqliteDatabaseProvider("Data Source=:memory:");
            opts.JsonSerializerOptions = ctx.Options;
        });
        Assert.Throws<InvalidOperationException>(() =>
        {
            services.AddDocumentStoreAITools(b =>
            {
                b.AddType(ctx.Customer, name: "same", capabilities: DocumentAICapabilities.Get);
                b.AddType(ctx.Product, name: "same", capabilities: DocumentAICapabilities.Get);
            });
        });
    }

    [Fact]
    public void Empty_Registration_Throws()
    {
        var services = new ServiceCollection();
        services.AddDocumentStore(opts =>
        {
            opts.DatabaseProvider = new SqliteDatabaseProvider("Data Source=:memory:");
        });
        Assert.Throws<InvalidOperationException>(() =>
        {
            services.AddDocumentStoreAITools(_ => { });
        });
    }

    [Fact]
    public void None_Capabilities_Throws()
    {
        var ctx = CreateContext();
        var services = new ServiceCollection();
        services.AddDocumentStore(opts =>
        {
            opts.DatabaseProvider = new SqliteDatabaseProvider("Data Source=:memory:");
            opts.JsonSerializerOptions = ctx.Options;
        });
        Assert.Throws<ArgumentException>(() =>
        {
            services.AddDocumentStoreAITools(b =>
            {
                b.AddType(ctx.Customer, capabilities: DocumentAICapabilities.None);
            });
        });
    }

    [Fact]
    public void Tools_Have_Descriptions()
    {
        using var fixture = new AIToolsFixture();
        foreach (var tool in fixture.AITools.Tools.OfType<AIFunction>())
        {
            Assert.False(string.IsNullOrWhiteSpace(tool.Description), $"Tool {tool.Name} has no description");
        }
    }

    [Fact]
    public void Tools_Have_JsonSchema()
    {
        using var fixture = new AIToolsFixture();
        foreach (var tool in fixture.AITools.Tools.OfType<AIFunction>())
        {
            Assert.NotEqual(default, tool.JsonSchema);
        }
    }
}
