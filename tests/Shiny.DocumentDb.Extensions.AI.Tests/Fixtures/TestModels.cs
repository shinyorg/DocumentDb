using System.ComponentModel;

namespace Shiny.DocumentDb.Extensions.AI.Tests.Fixtures;

[Description("Customer record with contact information")]
public class Customer
{
    public string Id { get; set; } = "";

    [Description("Full name")]
    public string Name { get; set; } = "";

    [Description("Age in years")]
    public int Age { get; set; }

    [Description("Email address")]
    public string? Email { get; set; }
}

[Description("Product available for purchase")]
public class Product
{
    public string Id { get; set; } = "";
    public string Title { get; set; } = "";
    public decimal Price { get; set; }
    public string? Category { get; set; }
}
