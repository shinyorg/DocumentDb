using System.Text.Json;
using Shiny.DocumentDb.Extensions.AI.Tests.Fixtures;
using Xunit;

namespace Shiny.DocumentDb.Extensions.AI.Tests;

// Verifies the non-removable per-type Where filter (IDocumentAITypeBuilder<T>.Where) is enforced across
// every tool and cannot be bypassed by the LLM-supplied filter, id, or document.
public class WhereFilterTests
{
    // Seeded ages: Alice=30, Bob=25, Carol=35. Filter keeps only 30+ (Alice, Carol) — Bob is invisible.
    static AIToolsFixture ScopedFixture() =>
        new(configureCustomer: b => b.Where(c => c.Age >= 30));

    static JsonDocument Parse(object? result) =>
        JsonDocument.Parse(JsonSerializer.Serialize(result));

    [Fact]
    public async Task Query_Only_Returns_In_Scope()
    {
        using var fixture = ScopedFixture();
        await fixture.SeedCustomers();

        var result = await fixture.InvokeTool("customer_query", new());

        using var doc = Parse(result);
        Assert.Equal(2, doc.RootElement.GetProperty("count").GetInt32());
        var names = doc.RootElement.GetProperty("documents").EnumerateArray()
            .Select(d => d.GetProperty("name").GetString())
            .ToList();
        Assert.DoesNotContain("Bob", names);
    }

    [Fact]
    public async Task Query_Cannot_Be_Widened_By_Model_Filter()
    {
        using var fixture = ScopedFixture();
        await fixture.SeedCustomers();

        // The model tries to select the out-of-scope record directly.
        var filter = JsonSerializer.SerializeToElement(new { field = "name", op = "eq", value = "Bob" });
        var result = await fixture.InvokeTool("customer_query", new() { ["filter"] = filter });

        using var doc = Parse(result);
        Assert.Equal(0, doc.RootElement.GetProperty("count").GetInt32());
    }

    [Fact]
    public async Task Count_Respects_Filter()
    {
        using var fixture = ScopedFixture();
        await fixture.SeedCustomers();

        var result = await fixture.InvokeTool("customer_count", new());

        using var doc = Parse(result);
        Assert.Equal(2, doc.RootElement.GetProperty("count").GetInt32());
    }

    [Fact]
    public async Task Aggregate_Respects_Filter()
    {
        using var fixture = ScopedFixture();
        await fixture.SeedCustomers();

        var result = await fixture.InvokeTool("customer_aggregate", new()
        {
            ["function"] = "count"
        });

        using var doc = Parse(result);
        Assert.Equal(2, doc.RootElement.GetProperty("value").GetInt32());
    }

    [Fact]
    public async Task GetById_Hides_Out_Of_Scope()
    {
        using var fixture = ScopedFixture();
        await fixture.SeedCustomers();

        var outOfScope = await fixture.InvokeTool("customer_get_by_id", new() { ["id"] = "cust-2" });
        using (var doc = Parse(outOfScope))
            Assert.False(doc.RootElement.GetProperty("found").GetBoolean());

        var inScope = await fixture.InvokeTool("customer_get_by_id", new() { ["id"] = "cust-1" });
        using (var doc = Parse(inScope))
            Assert.True(doc.RootElement.GetProperty("found").GetBoolean());
    }

    [Fact]
    public async Task Delete_Refuses_Out_Of_Scope()
    {
        using var fixture = ScopedFixture();
        await fixture.SeedCustomers();

        var result = await fixture.InvokeTool("customer_delete", new() { ["id"] = "cust-2" });
        using (var doc = Parse(result))
            Assert.False(doc.RootElement.GetProperty("deleted").GetBoolean());

        // Bob is still there — the filter blocked the delete, not just the response.
        var stored = await fixture.Store.Get<Customer>("cust-2", fixture.JsonContext.Customer);
        Assert.NotNull(stored);
    }

    [Fact]
    public async Task Delete_Allows_In_Scope()
    {
        using var fixture = ScopedFixture();
        await fixture.SeedCustomers();

        var result = await fixture.InvokeTool("customer_delete", new() { ["id"] = "cust-1" });
        using var doc = Parse(result);
        Assert.True(doc.RootElement.GetProperty("deleted").GetBoolean());

        var stored = await fixture.Store.Get<Customer>("cust-1", fixture.JsonContext.Customer);
        Assert.Null(stored);
    }

    [Fact]
    public async Task Insert_Rejects_Out_Of_Scope_Document()
    {
        using var fixture = ScopedFixture();

        var document = JsonSerializer.SerializeToElement(new
        {
            id = "cust-new",
            name = "Dave",
            age = 20,
            email = "dave@test.com"
        });

        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            fixture.InvokeTool("customer_insert", new() { ["document"] = document }));

        var stored = await fixture.Store.Get<Customer>("cust-new", fixture.JsonContext.Customer);
        Assert.Null(stored);
    }

    [Fact]
    public async Task Insert_Allows_In_Scope_Document()
    {
        using var fixture = ScopedFixture();

        var document = JsonSerializer.SerializeToElement(new
        {
            id = "cust-new",
            name = "Dave",
            age = 40,
            email = "dave@test.com"
        });

        var result = await fixture.InvokeTool("customer_insert", new() { ["document"] = document });
        using var doc = Parse(result);
        Assert.True(doc.RootElement.GetProperty("inserted").GetBoolean());
    }

    [Fact]
    public async Task Update_Rejects_Document_That_Would_Leave_Scope()
    {
        using var fixture = ScopedFixture();
        await fixture.SeedCustomers();

        // In-scope record, but the incoming replacement drops below the filter threshold.
        var document = JsonSerializer.SerializeToElement(new
        {
            id = "cust-1",
            name = "Alice",
            age = 10,
            email = "alice@test.com"
        });

        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            fixture.InvokeTool("customer_update", new() { ["document"] = document }));

        var stored = await fixture.Store.Get<Customer>("cust-1", fixture.JsonContext.Customer);
        Assert.Equal(30, stored!.Age);
    }

    [Fact]
    public async Task Update_Refuses_To_Overwrite_Out_Of_Scope_Record()
    {
        using var fixture = ScopedFixture();
        await fixture.SeedCustomers();

        // Incoming doc is in scope (age 40) but the stored cust-2 (Bob, 25) is not — must not be hijacked.
        var document = JsonSerializer.SerializeToElement(new
        {
            id = "cust-2",
            name = "Hijack",
            age = 40,
            email = "x@test.com"
        });

        var result = await fixture.InvokeTool("customer_update", new() { ["document"] = document });
        using var doc = Parse(result);
        Assert.False(doc.RootElement.GetProperty("updated").GetBoolean());

        var stored = await fixture.Store.Get<Customer>("cust-2", fixture.JsonContext.Customer);
        Assert.Equal("Bob", stored!.Name);
    }

    [Fact]
    public async Task Update_Allows_In_Scope_On_Both_Sides()
    {
        using var fixture = ScopedFixture();
        await fixture.SeedCustomers();

        var document = JsonSerializer.SerializeToElement(new
        {
            id = "cust-1",
            name = "Alice",
            age = 33,
            email = "alice@test.com"
        });

        var result = await fixture.InvokeTool("customer_update", new() { ["document"] = document });
        using var doc = Parse(result);
        Assert.True(doc.RootElement.GetProperty("updated").GetBoolean());

        var stored = await fixture.Store.Get<Customer>("cust-1", fixture.JsonContext.Customer);
        Assert.Equal(33, stored!.Age);
    }

    [Fact]
    public async Task Multiple_Where_Calls_Are_And_Combined()
    {
        using var fixture = new AIToolsFixture(
            configureCustomer: b => b
                .Where(c => c.Age >= 18)
                .Where(c => c.Name != "Bob"));
        await fixture.SeedCustomers();

        var result = await fixture.InvokeTool("customer_query", new());

        using var doc = Parse(result);
        var names = doc.RootElement.GetProperty("documents").EnumerateArray()
            .Select(d => d.GetProperty("name").GetString())
            .ToList();
        Assert.Equal(2, names.Count);            // Alice + Carol (Bob excluded by the second predicate)
        Assert.DoesNotContain("Bob", names);
    }
}
