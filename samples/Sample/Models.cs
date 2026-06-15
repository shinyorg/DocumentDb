namespace Sample;

[Flags]
public enum Access
{
    None = 0,
    Read = 1,
    Write = 2,
    Delete = 4,
    Admin = Read | Write | Delete
}

public class Customer
{
    public string Id { get; set; } = "";
    public string Name { get; set; } = "";
    public int Age { get; set; }
    public string? Email { get; set; }
    public Access Access { get; set; }
    public DateTimeOffset CreatedAt { get; set; }
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

// Vector-search demo: small note documents with a 16-dim embedding.
// The sample uses a deterministic hash-based embedding so it runs without
// the sqlite-vec native binary or any cloud embedding service. In a real
// app you would replace this with Microsoft.Extensions.AI.IEmbeddingGenerator.
public class Memo
{
    public string Id { get; set; } = "";
    public string Title { get; set; } = "";
    public string Content { get; set; } = "";
    public ReadOnlyMemory<float> Embedding { get; set; }
}
