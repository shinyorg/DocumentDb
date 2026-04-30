using System.Text.Json;
using Shiny.DocumentDb.Extensions.AI.Tests.Fixtures;
using Xunit;

namespace Shiny.DocumentDb.Extensions.AI.Tests;

public class UpdateToolTests
{
    [Fact]
    public async Task Updates_Existing_Document()
    {
        using var fixture = new AIToolsFixture();
        await fixture.SeedCustomers();

        var document = JsonSerializer.SerializeToElement(new
        {
            id = "cust-1",
            name = "Alice Updated",
            age = 31,
            email = "alice-new@test.com"
        });

        var result = await fixture.InvokeTool("customer_update", new()
        {
            ["document"] = document
        });

        var json = JsonSerializer.Serialize(result);
        using var doc = JsonDocument.Parse(json);
        Assert.True(doc.RootElement.GetProperty("updated").GetBoolean());

        var stored = await fixture.Store.Get<Customer>("cust-1", fixture.JsonContext.Customer);
        Assert.NotNull(stored);
        Assert.Equal("Alice Updated", stored.Name);
        Assert.Equal(31, stored.Age);
    }

    [Fact]
    public async Task Throws_When_Document_Missing()
    {
        using var fixture = new AIToolsFixture();

        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            fixture.InvokeTool("customer_update", new()));
    }
}
