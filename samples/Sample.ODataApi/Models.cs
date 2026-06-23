namespace Sample.ODataApi;

public class Customer
{
    public string Id { get; set; } = "";
    public string Name { get; set; } = "";
    public string Email { get; set; } = "";
    public string Country { get; set; } = "";
    public int Age { get; set; }
    public bool IsActive { get; set; }
    public DateTimeOffset Created { get; set; }
}

public class Order
{
    public string Id { get; set; } = "";
    public string CustomerId { get; set; } = "";
    public string CustomerName { get; set; } = "";
    public string Status { get; set; } = "";
    public decimal Total { get; set; }
    public string Country { get; set; } = "";
    public DateTimeOffset Placed { get; set; }
}
