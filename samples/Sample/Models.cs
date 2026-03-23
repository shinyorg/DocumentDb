namespace Sample;

public class Customer
{
    public string Id { get; set; } = "";
    public string Name { get; set; } = "";
    public int Age { get; set; }
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

public class Order
{
    public string Id { get; set; } = "";
    public string CustomerName { get; set; } = "";
    public string Status { get; set; } = "";
    public Address ShippingAddress { get; set; } = new();
    public List<OrderLine> Lines { get; set; } = [];
    public List<string> Tags { get; set; } = [];
}

public class OrderSummary
{
    public string Customer { get; set; } = "";
    public string City { get; set; } = "";
    public int LineCount { get; set; }
}

public class OrderStats
{
    public string Status { get; set; } = "";
    public int OrderCount { get; set; }
}

// Model with a custom Id property — no "Id" property at all
public class Sensor
{
    public Guid DeviceKey { get; set; }
    public string Location { get; set; } = "";
    public double Temperature { get; set; }
    public DateTimeOffset ReadingAt { get; set; }
}
