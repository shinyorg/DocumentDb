using System.Text.Json.Serialization;
using SQLite;

namespace Shiny.DocumentDb.Benchmarks;

public class BenchmarkUser
{
    // DocumentStore does not auto-generate string Ids — assign one per instance.
    public string Id { get; set; } = Guid.NewGuid().ToString("N");
    public string Name { get; set; } = "";
    public int Age { get; set; }
    public string? Email { get; set; }
}

[Table("SqliteUsers")]
public class SqliteUser
{
    [PrimaryKey, AutoIncrement]
    public int Id { get; set; }

    public string DocId { get; set; } = "";
    public string Name { get; set; } = "";
    public int Age { get; set; }
    public string? Email { get; set; }
}

// --- Child-collection models for DocumentStore ---

public class BenchmarkAddress
{
    public string Street { get; set; } = "";
    public string City { get; set; } = "";
    public string State { get; set; } = "";
    public string Zip { get; set; } = "";
}

public class BenchmarkOrderLine
{
    public string ProductName { get; set; } = "";
    public int Quantity { get; set; }
    public decimal UnitPrice { get; set; }
}

public class BenchmarkOrder
{
    public string Id { get; set; } = Guid.NewGuid().ToString("N");
    public string CustomerName { get; set; } = "";
    public string Status { get; set; } = "";
    public BenchmarkAddress ShippingAddress { get; set; } = new();
    public List<BenchmarkOrderLine> Lines { get; set; } = [];
    public List<string> Tags { get; set; } = [];
}

// --- Normalized sqlite-net models (3 tables + manual joins) ---

[Table("SqliteOrders")]
public class SqliteOrder
{
    [PrimaryKey, AutoIncrement]
    public int Id { get; set; }

    public string DocId { get; set; } = "";
    public string CustomerName { get; set; } = "";
    public string Status { get; set; } = "";
    public string Street { get; set; } = "";
    public string City { get; set; } = "";
    public string State { get; set; } = "";
    public string Zip { get; set; } = "";
}

[Table("SqliteOrderLines")]
public class SqliteOrderLine
{
    [PrimaryKey, AutoIncrement]
    public int Id { get; set; }

    [Indexed]
    public int OrderId { get; set; }

    public string ProductName { get; set; } = "";
    public int Quantity { get; set; }
    public decimal UnitPrice { get; set; }
}

[Table("SqliteOrderTags")]
public class SqliteOrderTag
{
    [PrimaryKey, AutoIncrement]
    public int Id { get; set; }

    [Indexed]
    public int OrderId { get; set; }

    public string Tag { get; set; } = "";
}

[JsonSerializable(typeof(BenchmarkUser))]
[JsonSerializable(typeof(BenchmarkOrder))]
[JsonSerializable(typeof(BenchmarkAddress))]
[JsonSerializable(typeof(BenchmarkOrderLine))]
public partial class BenchmarkJsonContext : JsonSerializerContext;

// --- EF Core models ---
// Flat entity mirrors the sqlite-net SqliteUser (single table).
public class EfUser
{
    public int Id { get; set; }
    public string Name { get; set; } = "";
    public int Age { get; set; }
    public string? Email { get; set; }
}

// Normalized order graph: EfOrder owns its address columns and has two child
// collections (Lines, Tags) in separate tables — the relational equivalent of the
// nested document. Reads use Include(), matching what an EF Core app actually writes.
public class EfOrder
{
    public int Id { get; set; }
    public string CustomerName { get; set; } = "";
    public string Status { get; set; } = "";
    public EfAddress ShippingAddress { get; set; } = new();
    public List<EfOrderLine> Lines { get; set; } = [];
    public List<EfOrderTag> Tags { get; set; } = [];
}

public class EfAddress
{
    public string Street { get; set; } = "";
    public string City { get; set; } = "";
    public string State { get; set; } = "";
    public string Zip { get; set; } = "";
}

public class EfOrderLine
{
    public int Id { get; set; }
    public int OrderId { get; set; }
    public string ProductName { get; set; } = "";
    public int Quantity { get; set; }
    public decimal UnitPrice { get; set; }
}

public class EfOrderTag
{
    public int Id { get; set; }
    public int OrderId { get; set; }
    public string Tag { get; set; } = "";
}

// --- Dapper models ---
// Plain POCOs that Dapper maps to by column name. The flat user mirrors SqliteUser/EfUser
// (single table); the order graph mirrors the normalized sqlite-net layout (3 tables +
// manual joins) — the relational equivalent of the nested document, hand-written SQL.

public class DapperUser
{
    public int Id { get; set; }
    public string Name { get; set; } = "";
    public int Age { get; set; }
    public string? Email { get; set; }
}

public class DapperOrder
{
    public int Id { get; set; }
    public string CustomerName { get; set; } = "";
    public string Status { get; set; } = "";
    public string Street { get; set; } = "";
    public string City { get; set; } = "";
    public string State { get; set; } = "";
    public string Zip { get; set; } = "";
}

public class DapperOrderLine
{
    public int Id { get; set; }
    public int OrderId { get; set; }
    public string ProductName { get; set; } = "";
    public int Quantity { get; set; }
    public decimal UnitPrice { get; set; }
}

public class DapperOrderTag
{
    public int Id { get; set; }
    public int OrderId { get; set; }
    public string Tag { get; set; } = "";
}

public static class EfFactory
{
    public static EfOrder CreateOrder(int i) => new()
    {
        CustomerName = $"Customer_{i}",
        Status = i % 2 == 0 ? "Shipped" : "Pending",
        ShippingAddress = new EfAddress { Street = $"{i} Main St", City = "Springfield", State = "IL", Zip = "62704" },
        Lines =
        [
            new() { ProductName = "Product_0", Quantity = 1, UnitPrice = 9.99m },
            new() { ProductName = "Product_1", Quantity = 2, UnitPrice = 10.99m },
            new() { ProductName = "Product_2", Quantity = 3, UnitPrice = 11.99m }
        ],
        Tags = [new() { Tag = "priority" }, new() { Tag = $"region-{i % 5}" }]
    };
}
