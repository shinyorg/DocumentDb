using System.Text.Json;
using Shiny.DocumentDb.Extensions.AI.Tests.Fixtures;
using Xunit;

namespace Shiny.DocumentDb.Extensions.AI.Tests;

public class CountToolTests
{
    [Fact]
    public async Task Counts_All_Documents()
    {
        using var fixture = new AIToolsFixture();
        await fixture.SeedCustomers();

        var result = await fixture.InvokeTool("customer_count", new());

        var json = JsonSerializer.Serialize(result);
        using var doc = JsonDocument.Parse(json);
        Assert.Equal(3, doc.RootElement.GetProperty("count").GetInt32());
    }

    [Fact]
    public async Task Counts_With_Filter()
    {
        using var fixture = new AIToolsFixture();
        await fixture.SeedCustomers();

        var filter = JsonSerializer.SerializeToElement(new
        {
            field = "age",
            op = "gte",
            value = 30
        });

        var result = await fixture.InvokeTool("customer_count", new()
        {
            ["filter"] = filter
        });

        var json = JsonSerializer.Serialize(result);
        using var doc = JsonDocument.Parse(json);
        Assert.Equal(2, doc.RootElement.GetProperty("count").GetInt32());
    }

    [Fact]
    public async Task Returns_Zero_When_No_Matches()
    {
        using var fixture = new AIToolsFixture();
        await fixture.SeedCustomers();

        var filter = JsonSerializer.SerializeToElement(new
        {
            field = "age",
            op = "gt",
            value = 100
        });

        var result = await fixture.InvokeTool("customer_count", new()
        {
            ["filter"] = filter
        });

        var json = JsonSerializer.Serialize(result);
        using var doc = JsonDocument.Parse(json);
        Assert.Equal(0, doc.RootElement.GetProperty("count").GetInt32());
    }

    [Fact]
    public async Task Returns_Zero_When_Store_Empty()
    {
        using var fixture = new AIToolsFixture();

        var result = await fixture.InvokeTool("customer_count", new());

        var json = JsonSerializer.Serialize(result);
        using var doc = JsonDocument.Parse(json);
        Assert.Equal(0, doc.RootElement.GetProperty("count").GetInt32());
    }
}
