using System.Text.Json;
using Shiny.DocumentDb.Extensions.AI.Tests.Fixtures;
using Xunit;

namespace Shiny.DocumentDb.Extensions.AI.Tests;

public class InsertToolTests
{
    [Fact]
    public async Task Inserts_Document_And_Returns_It()
    {
        using var fixture = new AIToolsFixture();

        var document = JsonSerializer.SerializeToElement(new
        {
            id = "new-1",
            name = "Dave",
            age = 40,
            email = "dave@test.com"
        });

        var result = await fixture.InvokeTool("customer_insert", new()
        {
            ["document"] = document
        });

        var json = JsonSerializer.Serialize(result);
        using var doc = JsonDocument.Parse(json);
        Assert.True(doc.RootElement.GetProperty("inserted").GetBoolean());

        var stored = await fixture.Store.Get<Customer>("new-1", fixture.JsonContext.Customer);
        Assert.NotNull(stored);
        Assert.Equal("Dave", stored.Name);
        Assert.Equal(40, stored.Age);
    }

    [Fact]
    public async Task Throws_When_Document_Missing()
    {
        using var fixture = new AIToolsFixture();

        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            fixture.InvokeTool("customer_insert", new()));
    }
}
