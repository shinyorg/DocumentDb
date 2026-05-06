namespace Sample.Blazor;

public class Customer
{
    public string Id { get; set; } = "";
    public string Name { get; set; } = "";
    public int Age { get; set; }
    public string? Email { get; set; }
    public string City { get; set; } = "";
}

public class Order
{
    public string Id { get; set; } = "";
    public string CustomerId { get; set; } = "";
    public string CustomerName { get; set; } = "";
    public string Status { get; set; } = "";
    public decimal Total { get; set; }
    public DateTime CreatedAt { get; set; }
    public List<OrderLine> Lines { get; set; } = [];
}

public class OrderLine
{
    public string ProductName { get; set; } = "";
    public int Quantity { get; set; }
    public decimal UnitPrice { get; set; }
}
