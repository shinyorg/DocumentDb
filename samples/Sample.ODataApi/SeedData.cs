using Shiny.DocumentDb;

namespace Sample.ODataApi;

/// <summary>
/// Idempotent seed data run once at startup via <c>AddDocumentSeeder</c> (a run-once marker keyed on the
/// seeder name is stored in the database, so re-launching the app does not re-seed). Bump the version
/// passed to <c>AddDocumentSeeder</c> to force a re-run after changing this data.
/// </summary>
public static class SeedData
{
    public static async Task SeedAsync(IDocumentStore store, CancellationToken ct)
    {
        foreach (var customer in Customers)
            await store.Upsert(customer, cancellationToken: ct);

        foreach (var order in Orders)
            await store.Upsert(order, cancellationToken: ct);
    }

    static readonly Customer[] Customers =
    [
        new() { Id = "c1", Name = "Alice Johnson",  Email = "alice@example.com",  Country = "CA", Age = 34, IsActive = true,  Created = new(2023, 1, 15, 0, 0, 0, TimeSpan.Zero) },
        new() { Id = "c2", Name = "Bob Smith",      Email = "bob@example.com",    Country = "US", Age = 29, IsActive = true,  Created = new(2023, 4, 2, 0, 0, 0, TimeSpan.Zero) },
        new() { Id = "c3", Name = "Carol White",    Email = "carol@example.com",  Country = "US", Age = 41, IsActive = false, Created = new(2022, 11, 20, 0, 0, 0, TimeSpan.Zero) },
        new() { Id = "c4", Name = "David Brown",    Email = "david@example.com",  Country = "GB", Age = 52, IsActive = true,  Created = new(2024, 2, 9, 0, 0, 0, TimeSpan.Zero) },
        new() { Id = "c5", Name = "Eve Davis",      Email = "eve@example.com",    Country = "CA", Age = 23, IsActive = true,  Created = new(2024, 6, 1, 0, 0, 0, TimeSpan.Zero) },
        new() { Id = "c6", Name = "Frank Miller",   Email = "frank@example.com",  Country = "GB", Age = 38, IsActive = false, Created = new(2023, 8, 17, 0, 0, 0, TimeSpan.Zero) }
    ];

    static readonly Order[] Orders =
    [
        new() { Id = "o1", CustomerId = "c1", CustomerName = "Alice Johnson", Status = "Shipped",   Total = 129.95m, Country = "CA", Placed = new(2024, 3, 1, 0, 0, 0, TimeSpan.Zero) },
        new() { Id = "o2", CustomerId = "c1", CustomerName = "Alice Johnson", Status = "Pending",   Total = 49.00m,  Country = "CA", Placed = new(2024, 6, 12, 0, 0, 0, TimeSpan.Zero) },
        new() { Id = "o3", CustomerId = "c2", CustomerName = "Bob Smith",     Status = "Shipped",   Total = 299.99m, Country = "US", Placed = new(2024, 5, 20, 0, 0, 0, TimeSpan.Zero) },
        new() { Id = "o4", CustomerId = "c3", CustomerName = "Carol White",   Status = "Cancelled", Total = 19.99m,  Country = "US", Placed = new(2024, 1, 8, 0, 0, 0, TimeSpan.Zero) },
        new() { Id = "o5", CustomerId = "c4", CustomerName = "David Brown",   Status = "Shipped",   Total = 540.50m, Country = "GB", Placed = new(2024, 4, 30, 0, 0, 0, TimeSpan.Zero) },
        new() { Id = "o6", CustomerId = "c5", CustomerName = "Eve Davis",     Status = "Pending",   Total = 75.25m,  Country = "CA", Placed = new(2024, 6, 18, 0, 0, 0, TimeSpan.Zero) },
        new() { Id = "o7", CustomerId = "c2", CustomerName = "Bob Smith",     Status = "Shipped",   Total = 12.00m,  Country = "US", Placed = new(2024, 2, 14, 0, 0, 0, TimeSpan.Zero) }
    ];
}
