using System.Text.Json;
using Shiny.DocumentDb.Extensions.AI.Tests.Fixtures;
using Xunit;

namespace Shiny.DocumentDb.Extensions.AI.Tests;

public class DeleteToolTests
{
    [Fact]
    public async Task Deletes_Existing_Document()
    {
        using var fixture = new AIToolsFixture();
        await fixture.SeedCustomers();

        var result = await fixture.InvokeTool("customer_delete", new()
        {
            ["id"] = "cust-1"
        });

        var json = JsonSerializer.Serialize(result);
        using var doc = JsonDocument.Parse(json);
        Assert.True(doc.RootElement.GetProperty("deleted").GetBoolean());

        var stored = await fixture.Store.Get<Customer>("cust-1", fixture.JsonContext.Customer);
        Assert.Null(stored);
    }

    [Fact]
    public async Task Returns_False_For_Missing_Document()
    {
        using var fixture = new AIToolsFixture();

        var result = await fixture.InvokeTool("customer_delete", new()
        {
            ["id"] = "nonexistent"
        });

        var json = JsonSerializer.Serialize(result);
        using var doc = JsonDocument.Parse(json);
        Assert.False(doc.RootElement.GetProperty("deleted").GetBoolean());
    }

    [Fact]
    public async Task Throws_When_Id_Missing()
    {
        using var fixture = new AIToolsFixture();

        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            fixture.InvokeTool("customer_delete", new()));
    }
}
