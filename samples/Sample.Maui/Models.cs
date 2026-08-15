using Shiny.DocumentDb;

namespace Sample.Maui;

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

// Geofencing demo - a polygon region, monitored by containment.
public class GeofenceZone
{
    public string Id { get; set; } = "";
    public string Name { get; set; } = "";
    public string City { get; set; } = "";
    public bool Active { get; set; } = true;
    public Geometry Boundary { get; set; } = null!;
}

// Geofencing demo - a point region. Point regions can only be monitored by proximity
// (withinMeters), since a GPS reading never falls "inside" a stored point.
public class Landmark
{
    public string Id { get; set; } = "";
    public string Name { get; set; } = "";
    public string City { get; set; } = "";
    public GeoPoint Location { get; set; }
}

// Vector-search demo. The 4 embedding dimensions are toy "topic" axes
// (animals, technology, food, travel) so nearest-neighbour results are easy to eyeball.
public class VectorNote
{
    public string Id { get; set; } = "";
    public string Title { get; set; } = "";
    public string Category { get; set; } = "";
    public ReadOnlyMemory<float> Embedding { get; set; }
}
