using System.Text.Json;
using Shiny.DocumentDb.Extensions.AI.Tests.Fixtures;
using Xunit;

namespace Shiny.DocumentDb.Extensions.AI.Tests;

public class AggregateToolTests
{
    [Fact]
    public async Task Count_Aggregate()
    {
        using var fixture = new AIToolsFixture();
        await fixture.SeedCustomers();

        var result = await fixture.InvokeTool("customer_aggregate", new()
        {
            ["function"] = "count"
        });

        var json = JsonSerializer.Serialize(result);
        using var doc = JsonDocument.Parse(json);
        Assert.Equal("count", doc.RootElement.GetProperty("function").GetString());
        Assert.Equal(3, doc.RootElement.GetProperty("value").GetInt32());
    }

    [Fact]
    public async Task Sum_Aggregate()
    {
        using var fixture = new AIToolsFixture();
        await fixture.SeedCustomers();

        var result = await fixture.InvokeTool("customer_aggregate", new()
        {
            ["function"] = "sum",
            ["field"] = "age"
        });

        var json = JsonSerializer.Serialize(result);
        using var doc = JsonDocument.Parse(json);
        Assert.Equal("sum", doc.RootElement.GetProperty("function").GetString());
        Assert.Equal("age", doc.RootElement.GetProperty("field").GetString());
        Assert.Equal(90, doc.RootElement.GetProperty("value").GetInt32());
    }

    [Fact]
    public async Task Min_Aggregate()
    {
        using var fixture = new AIToolsFixture();
        await fixture.SeedCustomers();

        var result = await fixture.InvokeTool("customer_aggregate", new()
        {
            ["function"] = "min",
            ["field"] = "age"
        });

        var json = JsonSerializer.Serialize(result);
        using var doc = JsonDocument.Parse(json);
        Assert.Equal(25, doc.RootElement.GetProperty("value").GetInt32());
    }

    [Fact]
    public async Task Max_Aggregate()
    {
        using var fixture = new AIToolsFixture();
        await fixture.SeedCustomers();

        var result = await fixture.InvokeTool("customer_aggregate", new()
        {
            ["function"] = "max",
            ["field"] = "age"
        });

        var json = JsonSerializer.Serialize(result);
        using var doc = JsonDocument.Parse(json);
        Assert.Equal(35, doc.RootElement.GetProperty("value").GetInt32());
    }

    [Fact]
    public async Task Avg_Aggregate()
    {
        using var fixture = new AIToolsFixture();
        await fixture.SeedCustomers();

        var result = await fixture.InvokeTool("customer_aggregate", new()
        {
            ["function"] = "avg",
            ["field"] = "age"
        });

        var json = JsonSerializer.Serialize(result);
        using var doc = JsonDocument.Parse(json);
        Assert.Equal(30.0, doc.RootElement.GetProperty("value").GetDouble());
    }

    [Fact]
    public async Task Aggregate_With_Filter()
    {
        using var fixture = new AIToolsFixture();
        await fixture.SeedCustomers();

        var filter = JsonSerializer.SerializeToElement(new
        {
            field = "age",
            op = "gte",
            value = 30
        });

        var result = await fixture.InvokeTool("customer_aggregate", new()
        {
            ["function"] = "count",
            ["filter"] = filter
        });

        var json = JsonSerializer.Serialize(result);
        using var doc = JsonDocument.Parse(json);
        Assert.Equal(2, doc.RootElement.GetProperty("value").GetInt32());
    }

    [Fact]
    public async Task Sum_On_Decimal_Field()
    {
        using var fixture = new AIToolsFixture();
        await fixture.SeedProducts();

        var result = await fixture.InvokeTool("product_aggregate", new()
        {
            ["function"] = "sum",
            ["field"] = "price"
        });

        var json = JsonSerializer.Serialize(result);
        using var doc = JsonDocument.Parse(json);
        Assert.Equal(39.97m, doc.RootElement.GetProperty("value").GetDecimal());
    }

    [Fact]
    public async Task Throws_When_Function_Missing()
    {
        using var fixture = new AIToolsFixture();

        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            fixture.InvokeTool("customer_aggregate", new()));
    }

    [Fact]
    public async Task Throws_When_Field_Missing_For_Sum()
    {
        using var fixture = new AIToolsFixture();

        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            fixture.InvokeTool("customer_aggregate", new()
            {
                ["function"] = "sum"
            }));
    }

    [Fact]
    public async Task Throws_For_Non_Numeric_Field()
    {
        using var fixture = new AIToolsFixture();

        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            fixture.InvokeTool("customer_aggregate", new()
            {
                ["function"] = "sum",
                ["field"] = "name"
            }));
    }

    [Fact]
    public async Task Throws_For_Unknown_Function()
    {
        using var fixture = new AIToolsFixture();

        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            fixture.InvokeTool("customer_aggregate", new()
            {
                ["function"] = "median",
                ["field"] = "age"
            }));
    }
}
