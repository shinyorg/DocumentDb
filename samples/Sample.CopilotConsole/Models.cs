using System.ComponentModel;
using System.Text.Json.Serialization;

namespace Sample.CopilotConsole;

[Description("Customer record with contact information")]
public class Customer
{
    public string Id { get; set; } = "";

    [Description("Full name of the customer")]
    public string Name { get; set; } = "";

    [Description("Customer age in years")]
    public int Age { get; set; }

    [Description("Email address")]
    public string? Email { get; set; }
}

public class Address
{
    public string Street { get; set; } = "";
    public string City { get; set; } = "";
    public string State { get; set; } = "";
}

public class OrderLine
{
    public string ProductName { get; set; } = "";
    public int Quantity { get; set; }
    public decimal UnitPrice { get; set; }
}

[Description("Customer order with shipping and line items")]
public class Order
{
    public string Id { get; set; } = "";

    [Description("Name of the customer who placed the order")]
    public string CustomerName { get; set; } = "";

    [Description("Order status: Pending, Shipped, Delivered, or Cancelled")]
    public string Status { get; set; } = "";

    public Address ShippingAddress { get; set; } = new();
    public List<OrderLine> Lines { get; set; } = [];
    public List<string> Tags { get; set; } = [];
}

[JsonSerializable(typeof(Customer))]
[JsonSerializable(typeof(Order))]
[JsonSerializable(typeof(Address))]
[JsonSerializable(typeof(OrderLine))]
public partial class SampleJsonContext : JsonSerializerContext;
