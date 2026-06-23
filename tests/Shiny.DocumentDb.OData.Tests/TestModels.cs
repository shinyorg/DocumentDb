using System.Text.Json.Serialization;

namespace Shiny.DocumentDb.OData.Tests;

public class Customer
{
    public string Id { get; set; } = "";
    public string Name { get; set; } = "";
    public string Country { get; set; } = "";
    public int Age { get; set; }
    public DateTime Created { get; set; }
    public string? Email { get; set; }
    public Address? Address { get; set; }
}

public class Address
{
    public string City { get; set; } = "";
    public string State { get; set; } = "";
}

[JsonSerializable(typeof(Customer))]
[JsonSerializable(typeof(Address))]
public partial class TestJsonContext : JsonSerializerContext;
