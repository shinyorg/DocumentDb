using System.Text.Json;
using Shiny.DocumentDb.Extensions.AI.Tests.Fixtures;
using Xunit;

namespace Shiny.DocumentDb.Extensions.AI.Tests;

public class GetByIdToolTests
{
    [Fact]
    public async Task Returns_Document_When_Found()
    {
        using var fixture = new AIToolsFixture();
        await fixture.SeedCustomers();

        var result = await fixture.InvokeTool("customer_get_by_id", new()
        {
            ["id"] = "cust-1"
        });

        var json = JsonSerializer.Serialize(result);
        using var doc = JsonDocument.Parse(json);
        Assert.True(doc.RootElement.GetProperty("found").GetBoolean());
        Assert.Equal("Alice", doc.RootElement.GetProperty("document").GetProperty("name").GetString());
    }

    [Fact]
    public async Task Returns_NotFound_For_Missing_Id()
    {
        using var fixture = new AIToolsFixture();

        var result = await fixture.InvokeTool("customer_get_by_id", new()
        {
            ["id"] = "nonexistent"
        });

        var json = JsonSerializer.Serialize(result);
        using var doc = JsonDocument.Parse(json);
        Assert.False(doc.RootElement.GetProperty("found").GetBoolean());
    }

    [Fact]
    public async Task Throws_When_Id_Missing()
    {
        using var fixture = new AIToolsFixture();

        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            fixture.InvokeTool("customer_get_by_id", new()));
    }
}
