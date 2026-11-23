using System.Text.Json;
using System.Text.Json.Nodes;
using Shiny.DocumentDb.Extensions.AI.Tests.Fixtures;
using Xunit;

namespace Shiny.DocumentDb.Extensions.AI.Tests;

/// <summary>
/// A property hidden with IgnoreProperties / AllowProperties is hidden from the tool results and from the
/// writes the model makes, not only from the schema.
/// </summary>
public class HiddenPropertyTests
{
    static JsonDocument Parse(object? result) => JsonDocument.Parse(JsonSerializer.Serialize(result));

    [Fact]
    public async Task Query_Omits_Ignored_Property()
    {
        using var fixture = new AIToolsFixture(configureCustomer: b => b.IgnoreProperties(c => c.Email));
        await fixture.SeedCustomers();

        using var doc = Parse(await fixture.InvokeTool("customer_query", new()));
        var documents = doc.RootElement.GetProperty("documents").EnumerateArray().ToList();

        Assert.Equal(3, documents.Count);
        Assert.All(documents, d =>
        {
            Assert.False(d.TryGetProperty("email", out _));
            Assert.True(d.TryGetProperty("name", out _));
        });
    }

    [Fact]
    public async Task Query_Keeps_Only_Allowed_Properties()
    {
        using var fixture = new AIToolsFixture(configureCustomer: b => b.AllowProperties(c => c.Id, c => c.Name));
        await fixture.SeedCustomers();

        using var doc = Parse(await fixture.InvokeTool("customer_query", new()));
        var names = doc.RootElement.GetProperty("documents")[0].EnumerateObject().Select(p => p.Name).OrderBy(n => n);

        Assert.Equal(new[] { "id", "name" }, names);
    }

    [Fact]
    public async Task GetById_Omits_Ignored_Property()
    {
        using var fixture = new AIToolsFixture(configureCustomer: b => b.IgnoreProperties(c => c.Email));
        await fixture.SeedCustomers();

        using var doc = Parse(await fixture.InvokeTool("customer_get_by_id", new() { ["id"] = "cust-1" }));
        var document = doc.RootElement.GetProperty("document");

        Assert.Equal("Alice", document.GetProperty("name").GetString());
        Assert.False(document.TryGetProperty("email", out _));
    }

    [Fact]
    public async Task Insert_Ignores_Hidden_Property_And_Omits_It_From_The_Echo()
    {
        using var fixture = new AIToolsFixture(configureCustomer: b => b.IgnoreProperties(c => c.Email));
        var body = JsonDocument.Parse("""{"id":"cust-9","name":"Zed","age":40,"email":"zed@test.com"}""").RootElement;

        using var doc = Parse(await fixture.InvokeTool("customer_insert", new() { ["document"] = body }));
        Assert.False(doc.RootElement.GetProperty("document").TryGetProperty("email", out _));

        var stored = await fixture.Store.Get("cust-9", fixture.JsonContext.Customer);
        Assert.NotNull(stored);
        Assert.Equal("Zed", stored.Name);
        Assert.Null(stored.Email);
    }

    [Fact]
    public async Task Update_Preserves_Hidden_Property()
    {
        using var fixture = new AIToolsFixture(configureCustomer: b => b.IgnoreProperties(c => c.Email));
        await fixture.SeedCustomers();
        var body = JsonDocument.Parse("""{"id":"cust-1","name":"Alicia","age":31,"email":"evil@test.com"}""").RootElement;

        using var doc = Parse(await fixture.InvokeTool("customer_update", new() { ["document"] = body }));
        Assert.True(doc.RootElement.GetProperty("updated").GetBoolean());

        var stored = await fixture.Store.Get("cust-1", fixture.JsonContext.Customer);
        Assert.NotNull(stored);
        Assert.Equal("Alicia", stored.Name);
        Assert.Equal("alice@test.com", stored.Email);
    }

    [Fact]
    public async Task Update_Of_Missing_Document_With_Hidden_Properties_Reports_Not_Updated()
    {
        using var fixture = new AIToolsFixture(configureCustomer: b => b.IgnoreProperties(c => c.Email));
        var body = JsonDocument.Parse("""{"id":"nobody","name":"X","age":1}""").RootElement;

        using var doc = Parse(await fixture.InvokeTool("customer_update", new() { ["document"] = body }));
        Assert.False(doc.RootElement.GetProperty("updated").GetBoolean());
    }

    [Fact]
    public async Task Nothing_Hidden_Returns_Every_Property()
    {
        using var fixture = new AIToolsFixture();
        await fixture.SeedCustomers();

        using var doc = Parse(await fixture.InvokeTool("customer_get_by_id", new() { ["id"] = "cust-1" }));
        Assert.Equal("alice@test.com", doc.RootElement.GetProperty("document").GetProperty("email").GetString());
    }
}
