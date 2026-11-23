using System.Text.Json;
using Microsoft.Extensions.AI;
using Shiny.DocumentDb.Extensions.AI.Tests.Fixtures;
using Xunit;

namespace Shiny.DocumentDb.Extensions.AI.Tests;

public class TypeBuilderTests
{
    [Fact]
    public void IgnoreProperties_Hides_Fields_From_Schema()
    {
        using var fixture = new AIToolsFixture(
            configureCustomer: b => b.IgnoreProperties(c => c.Email));

        var schema = fixture.GetTool("customer_query").JsonSchema.GetRawText();
        Assert.DoesNotContain("email", schema);
    }

    [Fact]
    public void Description_Override_Appears_In_Tool()
    {
        using var fixture = new AIToolsFixture(
            configureCustomer: b => b.Description("VIP customer list"));

        var tool = fixture.GetTool("customer_get_by_id");
        Assert.Contains("VIP customer list", tool.Description);
    }

    [Fact]
    public void MaxPageSize_Reflected_In_Schema()
    {
        using var fixture = new AIToolsFixture(
            configureCustomer: b => b.MaxPageSize(10));

        var tool = fixture.GetTool("customer_query");
        var schema = tool.JsonSchema.GetRawText();
        Assert.Contains("10", schema);
    }

    [Fact]
    public void AllowProperties_Restricts_To_Listed_Fields()
    {
        using var fixture = new AIToolsFixture(
            configureCustomer: b => b.AllowProperties(
                c => c.Id,
                c => c.Name));

        var tool = fixture.GetTool("customer_query");
        var schema = tool.JsonSchema.GetRawText();
        Assert.Contains("name", schema);
        Assert.DoesNotContain("age", schema);
        Assert.DoesNotContain("email", schema);
    }

    [Fact]
    public void Property_Description_Override()
    {
        using var fixture = new AIToolsFixture(
            configureCustomer: b => b.Property(c => c.Age, "Customer approximate age"));

        // The insert tool schema includes per-field property descriptions
        var insertTool = fixture.GetTool("customer_insert");
        var schema = insertTool.JsonSchema.GetRawText();
        Assert.Contains("Customer approximate age", schema);
    }
}
