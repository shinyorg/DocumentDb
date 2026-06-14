namespace Shiny.DocumentDb.Tests.Fixtures;

public class User
{
    public string Id { get; set; } = "";
    public string Name { get; set; } = "";
    public int Age { get; set; }
    public string? Email { get; set; }
}

public class Product
{
    public string Id { get; set; } = "";
    public string Title { get; set; } = "";
    public decimal Price { get; set; }
}

public class Address
{
    public string Street { get; set; } = "";
    public string City { get; set; } = "";
    public string State { get; set; } = "";
    public string Zip { get; set; } = "";
}

public class Event
{
    public string Id { get; set; } = "";
    public string Title { get; set; } = "";
    public DateTime StartDate { get; set; }
    public DateTimeOffset CreatedAt { get; set; }
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

public class UserSummary
{
    public string Name { get; set; } = "";
    public string? Email { get; set; }
}

public class OrderSummary
{
    public string Customer { get; set; } = "";
    public string City { get; set; } = "";
}

public class OrderDetail
{
    public string Customer { get; set; } = "";
    public int LineCount { get; set; }
    public bool HasPriority { get; set; }
}

public class OrderLineAggregates
{
    public string Customer { get; set; } = "";
    public int TotalQty { get; set; }
    public decimal MaxPrice { get; set; }
    public decimal MinPrice { get; set; }
    public double AvgPrice { get; set; }
}

public class OrderStats
{
    public string Status { get; set; } = "";
    public int OrderCount { get; set; }
    public int MaxLineCount { get; set; }
}

public class PriceSummary
{
    public int TotalCount { get; set; }
    public decimal MaxPrice { get; set; }
    public decimal MinPrice { get; set; }
    public decimal SumPrice { get; set; }
    public double AvgPrice { get; set; }
}

// ── Test-only models for Id type coverage ───────────────────────────

public class GuidIdModel
{
    public Guid Id { get; set; }
    public string Name { get; set; } = "";
}

public class IntIdModel
{
    public int Id { get; set; }
    public string Name { get; set; } = "";
}

public class LongIdModel
{
    public long Id { get; set; }
    public string Name { get; set; } = "";
}

public class StringIdModel
{
    public string Id { get; set; } = "";
    public string Name { get; set; } = "";
}

public class NoIdModel
{
    public string Name { get; set; } = "";
}

public class BadIdTypeModel
{
    public decimal Id { get; set; }
    public string Name { get; set; } = "";
}

public class CustomIdModel
{
    public string UserId { get; set; } = "";
    public string Name { get; set; } = "";
    public int Age { get; set; }
}

public class GuidCustomIdModel
{
    public Guid Key { get; set; }
    public string Label { get; set; } = "";
}

public enum Priority { Low, Normal, High }

// Exercises WhereIn across non-string value types: long, Guid, and an enum (default numeric JSON).
public class TypedFields
{
    public string Id { get; set; } = "";
    public long Serial { get; set; }
    public Guid Ref { get; set; }
    public Priority Level { get; set; }
}

// Strongly-typed Id wrapper for MapIdType coverage. The JsonConverter keeps the in-document
// JSON representation ("N" Guid) aligned with the storage-string form.
[System.Text.Json.Serialization.JsonConverter(typeof(OrderIdJsonConverter))]
public readonly record struct OrderId(Guid Value)
{
    public static OrderId New() => new(Guid.NewGuid());
}

public sealed class OrderIdJsonConverter : System.Text.Json.Serialization.JsonConverter<OrderId>
{
    public override OrderId Read(ref System.Text.Json.Utf8JsonReader reader, Type typeToConvert, System.Text.Json.JsonSerializerOptions options)
        => new(Guid.ParseExact(reader.GetString()!, "N"));

    public override void Write(System.Text.Json.Utf8JsonWriter writer, OrderId value, System.Text.Json.JsonSerializerOptions options)
        => writer.WriteStringValue(value.Value.ToString("N"));
}

public class TypedIdModel
{
    public OrderId Id { get; set; }
    public string Name { get; set; } = "";
    public int Age { get; set; }
}

// Nested-object merge test model: nullable inner fields so partial patches
// only carry the keys we explicitly set (StripNullProperties drops the rest).
public class MergeNested
{
    public string? Street { get; set; }
    public string? City { get; set; }
    public string? State { get; set; }
}

public class MergeDoc
{
    public string Id { get; set; } = "";
    public string? Name { get; set; }
    public MergeNested? Address { get; set; }
}

public class VersionedUser
{
    public string Id { get; set; } = "";
    public string Name { get; set; } = "";
    public int Age { get; set; }
    public int Version { get; set; }
}
