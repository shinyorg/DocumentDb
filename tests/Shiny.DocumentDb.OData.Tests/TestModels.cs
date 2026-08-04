using System.Text.Json.Serialization;

namespace Shiny.DocumentDb.OData.Tests;

public class Customer
{
    public string Id { get; set; } = "";
    public string Name { get; set; } = "";
    public string Country { get; set; } = "";
    public int Age { get; set; }
    public decimal Balance { get; set; }
    public DateTime Created { get; set; }
    public string? Email { get; set; }
    public Address? Address { get; set; }
}

public class Address
{
    public string City { get; set; } = "";
    public string State { get; set; } = "";
}

// Encryption mappings are process-wide per document type, so the encrypted-lane test needs a type no other
// test in this assembly reads.
public class Patient
{
    public string Id { get; set; } = "";
    public string Name { get; set; } = "";
    public string? Ssn { get; set; }
}

[JsonSerializable(typeof(Customer))]
[JsonSerializable(typeof(Address))]
[JsonSerializable(typeof(Patient))]
public partial class TestJsonContext : JsonSerializerContext;
