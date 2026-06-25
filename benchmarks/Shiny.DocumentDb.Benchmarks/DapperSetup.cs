using Dapper;
using Microsoft.Data.Sqlite;

namespace Shiny.DocumentDb.Benchmarks;

/// <summary>
/// Dapper is the micro-ORM contender: a thin mapper over a raw ADO.NET connection with
/// hand-written SQL — effectively the "floor" for how fast a relational round-trip can go.
/// This helper owns the connection and the schema so the benchmark files stay focused on
/// the operations being measured.
/// </summary>
public static class DapperSetup
{
    public static SqliteConnection CreateFlat(string path)
    {
        var conn = new SqliteConnection($"Data Source={path}");
        conn.Open();
        conn.Execute(
            """
            CREATE TABLE Users (
                Id INTEGER PRIMARY KEY AUTOINCREMENT,
                Name TEXT NOT NULL,
                Age INTEGER NOT NULL,
                Email TEXT
            );
            """);
        return conn;
    }

    public static SqliteConnection CreateOrders(string path)
    {
        var conn = new SqliteConnection($"Data Source={path}");
        conn.Open();
        conn.Execute(
            """
            CREATE TABLE Orders (
                Id INTEGER PRIMARY KEY AUTOINCREMENT,
                CustomerName TEXT NOT NULL,
                Status TEXT NOT NULL,
                Street TEXT NOT NULL,
                City TEXT NOT NULL,
                State TEXT NOT NULL,
                Zip TEXT NOT NULL
            );
            CREATE TABLE OrderLines (
                Id INTEGER PRIMARY KEY AUTOINCREMENT,
                OrderId INTEGER NOT NULL,
                ProductName TEXT NOT NULL,
                Quantity INTEGER NOT NULL,
                UnitPrice TEXT NOT NULL
            );
            CREATE TABLE OrderTags (
                Id INTEGER PRIMARY KEY AUTOINCREMENT,
                OrderId INTEGER NOT NULL,
                Tag TEXT NOT NULL
            );
            CREATE INDEX ix_orderlines_orderid ON OrderLines(OrderId);
            CREATE INDEX ix_ordertags_orderid ON OrderTags(OrderId);
            """);
        return conn;
    }

    /// <summary>
    /// Inserts one order graph (parent + 3 lines + 2 tags) with individual statements —
    /// the relational write the document store collapses into a single JSON blob. Mirrors
    /// the per-row inserts the sqlite-net contender does.
    /// </summary>
    public static async Task<long> InsertOrderAsync(SqliteConnection conn, int i)
    {
        var orderId = await conn.ExecuteScalarAsync<long>(
            """
            INSERT INTO Orders (CustomerName, Status, Street, City, State, Zip)
            VALUES (@CustomerName, @Status, @Street, @City, @State, @Zip);
            SELECT last_insert_rowid();
            """,
            new
            {
                CustomerName = $"Customer_{i}",
                Status = i % 2 == 0 ? "Shipped" : "Pending",
                Street = $"{i} Main St",
                City = "Springfield",
                State = "IL",
                Zip = "62704"
            });

        for (var j = 0; j < 3; j++)
        {
            await conn.ExecuteAsync(
                "INSERT INTO OrderLines (OrderId, ProductName, Quantity, UnitPrice) VALUES (@OrderId, @ProductName, @Quantity, @UnitPrice);",
                new { OrderId = orderId, ProductName = $"Product_{j}", Quantity = j + 1, UnitPrice = 9.99m + j });
        }

        await conn.ExecuteAsync(
            "INSERT INTO OrderTags (OrderId, Tag) VALUES (@OrderId, @Tag);",
            new { OrderId = orderId, Tag = "priority" });
        await conn.ExecuteAsync(
            "INSERT INTO OrderTags (OrderId, Tag) VALUES (@OrderId, @Tag);",
            new { OrderId = orderId, Tag = $"region-{i % 5}" });

        return orderId;
    }
}
