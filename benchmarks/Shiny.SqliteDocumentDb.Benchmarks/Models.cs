using System.Text.Json.Serialization;
using SQLite;

namespace Shiny.SqliteDocumentDb.Benchmarks;

public class BenchmarkUser
{
    public string Id { get; set; } = "";
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
    public string Id { get; set; } = "";
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
