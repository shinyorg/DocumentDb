using System.Text.Json;
using Shiny.DocumentDb.Extensions.AI.Tests.Fixtures;
using Xunit;

namespace Shiny.DocumentDb.Extensions.AI.Tests;

public class QueryToolTests
{
    [Fact]
    public async Task Returns_All_Documents_Without_Filter()
    {
        using var fixture = new AIToolsFixture();
        await fixture.SeedCustomers();

        var result = await fixture.InvokeTool("customer_query", new());

        var json = JsonSerializer.Serialize(result);
        using var doc = JsonDocument.Parse(json);
        Assert.Equal(3, doc.RootElement.GetProperty("count").GetInt32());
        Assert.Equal(3, doc.RootElement.GetProperty("documents").GetArrayLength());
    }

    [Fact]
    public async Task Filters_By_Equality()
    {
        using var fixture = new AIToolsFixture();
        await fixture.SeedCustomers();

        var filter = JsonSerializer.SerializeToElement(new
        {
            field = "name",
            op = "eq",
            value = "Alice"
        });

        var result = await fixture.InvokeTool("customer_query", new()
        {
            ["filter"] = filter
        });

        var json = JsonSerializer.Serialize(result);
        using var doc = JsonDocument.Parse(json);
        Assert.Equal(1, doc.RootElement.GetProperty("count").GetInt32());
        Assert.Equal("Alice", doc.RootElement.GetProperty("documents")[0].GetProperty("name").GetString());
    }

    [Fact]
    public async Task Filters_With_GreaterThan()
    {
        using var fixture = new AIToolsFixture();
        await fixture.SeedCustomers();

        var filter = JsonSerializer.SerializeToElement(new
        {
            field = "age",
            op = "gt",
            value = 28
        });

        var result = await fixture.InvokeTool("customer_query", new()
        {
            ["filter"] = filter
        });

        var json = JsonSerializer.Serialize(result);
        using var doc = JsonDocument.Parse(json);
        Assert.Equal(2, doc.RootElement.GetProperty("count").GetInt32());
    }

    [Fact]
    public async Task Filters_With_And_Combinator()
    {
        using var fixture = new AIToolsFixture();
        await fixture.SeedCustomers();

        var filter = JsonSerializer.SerializeToElement(new
        {
            and = new object[]
            {
                new { field = "age", op = "gte", value = 25 },
                new { field = "age", op = "lte", value = 30 }
            }
        });

        var result = await fixture.InvokeTool("customer_query", new()
        {
            ["filter"] = filter
        });

        var json = JsonSerializer.Serialize(result);
        using var doc = JsonDocument.Parse(json);
        Assert.Equal(2, doc.RootElement.GetProperty("count").GetInt32());
    }

    [Fact]
    public async Task Filters_With_Or_Combinator()
    {
        using var fixture = new AIToolsFixture();
        await fixture.SeedCustomers();

        var filter = JsonSerializer.SerializeToElement(new
        {
            or = new object[]
            {
                new { field = "name", op = "eq", value = "Alice" },
                new { field = "name", op = "eq", value = "Carol" }
            }
        });

        var result = await fixture.InvokeTool("customer_query", new()
        {
            ["filter"] = filter
        });

        var json = JsonSerializer.Serialize(result);
        using var doc = JsonDocument.Parse(json);
        Assert.Equal(2, doc.RootElement.GetProperty("count").GetInt32());
    }

    [Fact]
    public async Task Filters_With_Not_Combinator()
    {
        using var fixture = new AIToolsFixture();
        await fixture.SeedCustomers();

        var filter = JsonSerializer.SerializeToElement(new
        {
            not = new { field = "name", op = "eq", value = "Alice" }
        });

        var result = await fixture.InvokeTool("customer_query", new()
        {
            ["filter"] = filter
        });

        var json = JsonSerializer.Serialize(result);
        using var doc = JsonDocument.Parse(json);
        Assert.Equal(2, doc.RootElement.GetProperty("count").GetInt32());
    }

    [Fact]
    public async Task Filters_With_Contains()
    {
        using var fixture = new AIToolsFixture();
        await fixture.SeedCustomers();

        var filter = JsonSerializer.SerializeToElement(new
        {
            field = "name",
            op = "contains",
            value = "li"
        });

        var result = await fixture.InvokeTool("customer_query", new()
        {
            ["filter"] = filter
        });

        var json = JsonSerializer.Serialize(result);
        using var doc = JsonDocument.Parse(json);
        Assert.Equal(1, doc.RootElement.GetProperty("count").GetInt32());
        Assert.Equal("Alice", doc.RootElement.GetProperty("documents")[0].GetProperty("name").GetString());
    }

    [Fact]
    public async Task Filters_With_StartsWith()
    {
        using var fixture = new AIToolsFixture();
        await fixture.SeedCustomers();

        var filter = JsonSerializer.SerializeToElement(new
        {
            field = "name",
            op = "startsWith",
            value = "Bo"
        });

        var result = await fixture.InvokeTool("customer_query", new()
        {
            ["filter"] = filter
        });

        var json = JsonSerializer.Serialize(result);
        using var doc = JsonDocument.Parse(json);
        Assert.Equal(1, doc.RootElement.GetProperty("count").GetInt32());
    }

    [Fact]
    public async Task Filters_With_In_Operator()
    {
        using var fixture = new AIToolsFixture();
        await fixture.SeedCustomers();

        var filter = JsonSerializer.SerializeToElement(new
        {
            field = "name",
            op = "in",
            value = new[] { "Alice", "Carol" }
        });

        var result = await fixture.InvokeTool("customer_query", new()
        {
            ["filter"] = filter
        });

        var json = JsonSerializer.Serialize(result);
        using var doc = JsonDocument.Parse(json);
        Assert.Equal(2, doc.RootElement.GetProperty("count").GetInt32());
    }

    [Fact]
    public async Task Supports_OrderBy_Ascending()
    {
        using var fixture = new AIToolsFixture();
        await fixture.SeedCustomers();

        var result = await fixture.InvokeTool("customer_query", new()
        {
            ["orderBy"] = "age",
            ["orderDirection"] = "asc"
        });

        var json = JsonSerializer.Serialize(result);
        using var doc = JsonDocument.Parse(json);
        var docs = doc.RootElement.GetProperty("documents");
        Assert.Equal("Bob", docs[0].GetProperty("name").GetString());
        Assert.Equal("Carol", docs[2].GetProperty("name").GetString());
    }

    [Fact]
    public async Task Supports_OrderBy_Descending()
    {
        using var fixture = new AIToolsFixture();
        await fixture.SeedCustomers();

        var result = await fixture.InvokeTool("customer_query", new()
        {
            ["orderBy"] = "age",
            ["orderDirection"] = "desc"
        });

        var json = JsonSerializer.Serialize(result);
        using var doc = JsonDocument.Parse(json);
        var docs = doc.RootElement.GetProperty("documents");
        Assert.Equal("Carol", docs[0].GetProperty("name").GetString());
    }

    [Fact]
    public async Task Supports_Pagination()
    {
        using var fixture = new AIToolsFixture();
        await fixture.SeedCustomers();

        var result = await fixture.InvokeTool("customer_query", new()
        {
            ["limit"] = JsonSerializer.SerializeToElement(2),
            ["offset"] = JsonSerializer.SerializeToElement(0)
        });

        var json = JsonSerializer.Serialize(result);
        using var doc = JsonDocument.Parse(json);
        Assert.Equal(2, doc.RootElement.GetProperty("count").GetInt32());
        Assert.Equal(2, doc.RootElement.GetProperty("limit").GetInt32());
        Assert.Equal(0, doc.RootElement.GetProperty("offset").GetInt32());
    }

    [Fact]
    public async Task Clamps_Limit_To_MaxPageSize()
    {
        using var fixture = new AIToolsFixture(
            configureCustomer: b => b.MaxPageSize(2));
        await fixture.SeedCustomers();

        var result = await fixture.InvokeTool("customer_query", new()
        {
            ["limit"] = JsonSerializer.SerializeToElement(999)
        });

        var json = JsonSerializer.Serialize(result);
        using var doc = JsonDocument.Parse(json);
        Assert.Equal(2, doc.RootElement.GetProperty("limit").GetInt32());
        Assert.Equal(2, doc.RootElement.GetProperty("count").GetInt32());
    }
}
