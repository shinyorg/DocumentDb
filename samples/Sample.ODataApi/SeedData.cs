using Bogus;
using Shiny.DocumentDb;

namespace Sample.ODataApi;

/// <summary>
/// Seeds the store once at startup via <c>AddDocumentSeeder</c> (a run-once marker keyed on the seeder
/// name lives in the database, so re-launching does not re-seed; bump the version passed to
/// <c>AddDocumentSeeder</c> to force a re-run).
/// <para>
/// Data is generated with <see href="https://github.com/bchristie/Bogus">Bogus</see> using a <b>fixed
/// random seed</b>, so every run produces the exact same ~250 customers and ~1,000 orders. A handful of
/// deterministic <b>anchor</b> records (Alice Johnson, Bob Smith, …) are always present so the named
/// sample queries on the explorer page (<c>CustomerName eq 'Alice Johnson'</c>,
/// <c>contains(Name,'Smith')</c>) reliably return rows.
/// </para>
/// </summary>
public static class SeedData
{
    // Fixed value sets — the explorer's categorical sample queries (Country eq 'CA', Status eq 'Shipped')
    // assume these. Keep them in sync with wwwroot/index.html if you change them.
    static readonly string[] Countries = ["CA", "US", "GB", "AU", "DE", "FR"];
    static readonly string[] Statuses = ["Pending", "Shipped", "Delivered", "Cancelled", "Refunded"];

    const int CustomerCount = 250;

    public static async Task SeedAsync(IDocumentStore store, CancellationToken ct)
    {
        // Deterministic output across runs.
        Randomizer.Seed = new Random(8675309);

        var customers = new List<Customer>(Anchors);

        var customerFaker = new Faker<Customer>()
            .RuleFor(c => c.Id, f => $"cust-{f.IndexFaker:0000}")
            .RuleFor(c => c.Name, f => f.Name.FullName())
            .RuleFor(c => c.Email, (f, c) => f.Internet.Email(c.Name.Split(' ')[0]))
            .RuleFor(c => c.Country, f => f.PickRandom(Countries))
            .RuleFor(c => c.Age, f => f.Random.Int(18, 75))
            .RuleFor(c => c.IsActive, f => f.Random.Bool(0.8f))
            .RuleFor(c => c.Created, f => f.Date.PastOffset(4));
        customers.AddRange(customerFaker.Generate(CustomerCount));

        // One Faker for order fields; customer-derived fields (CustomerId/CustomerName/Country) and the
        // running order id are assigned after generation so each order ties back to a real customer.
        var orderFaker = new Faker<Order>()
            .RuleFor(o => o.Status, f => f.PickRandom(Statuses))
            .RuleFor(o => o.Total, f => Math.Round(f.Random.Decimal(5, 2000), 2))
            .RuleFor(o => o.Placed, f => f.Date.PastOffset(2));

        var orders = new List<Order>(AnchorOrders);
        var counts = new Randomizer();
        var nextOrder = AnchorOrders.Length + 1;
        foreach (var c in customers.Skip(Anchors.Length)) // anchors already have hand-written orders
        {
            foreach (var o in orderFaker.Generate(counts.Number(0, 8)))
            {
                o.Id = $"ord-{nextOrder++:00000}";
                o.CustomerId = c.Id;
                o.CustomerName = c.Name;
                o.Country = c.Country;
                orders.Add(o);
            }
        }

        // BatchUpsert is idempotent on the anchor ids and fast for the bulk. Pass the source-generated
        // JsonTypeInfo so the write path stays reflection-free.
        var sw = System.Diagnostics.Stopwatch.StartNew();
        await store.BatchUpsert(customers, SampleJsonContext.Default.Customer, ct);
        await store.BatchUpsert(orders, SampleJsonContext.Default.Order, ct);
        sw.Stop();

        Console.WriteLine(
            $"[seed] Wrote {customers.Count} customers + {orders.Count} orders in {sw.ElapsedMilliseconds} ms " +
            $"(source-gen JSON, no reflection).");
    }

    // ── Deterministic anchor records the explorer's named queries depend on ──────
    static readonly Customer[] Anchors =
    [
        new() { Id = "c1", Name = "Alice Johnson",  Email = "alice@example.com",  Country = "CA", Age = 34, IsActive = true,  Created = new(2023, 1, 15, 0, 0, 0, TimeSpan.Zero) },
        new() { Id = "c2", Name = "Bob Smith",      Email = "bob@example.com",    Country = "US", Age = 29, IsActive = true,  Created = new(2023, 4, 2, 0, 0, 0, TimeSpan.Zero) },
        new() { Id = "c3", Name = "Carol White",    Email = "carol@example.com",  Country = "US", Age = 41, IsActive = false, Created = new(2022, 11, 20, 0, 0, 0, TimeSpan.Zero) },
        new() { Id = "c4", Name = "David Brown",    Email = "david@example.com",  Country = "GB", Age = 52, IsActive = true,  Created = new(2024, 2, 9, 0, 0, 0, TimeSpan.Zero) },
        new() { Id = "c5", Name = "Eve Davis",      Email = "eve@example.com",    Country = "CA", Age = 23, IsActive = true,  Created = new(2024, 6, 1, 0, 0, 0, TimeSpan.Zero) },
        new() { Id = "c6", Name = "Frank Smithers", Email = "frank@example.com",  Country = "GB", Age = 38, IsActive = false, Created = new(2023, 8, 17, 0, 0, 0, TimeSpan.Zero) }
    ];

    static readonly Order[] AnchorOrders =
    [
        new() { Id = "ord-00001", CustomerId = "c1", CustomerName = "Alice Johnson", Status = "Shipped",   Total = 129.95m, Country = "CA", Placed = new(2024, 3, 1, 0, 0, 0, TimeSpan.Zero) },
        new() { Id = "ord-00002", CustomerId = "c1", CustomerName = "Alice Johnson", Status = "Delivered", Total = 749.00m, Country = "CA", Placed = new(2024, 6, 12, 0, 0, 0, TimeSpan.Zero) },
        new() { Id = "ord-00003", CustomerId = "c2", CustomerName = "Bob Smith",     Status = "Shipped",   Total = 299.99m, Country = "US", Placed = new(2024, 5, 20, 0, 0, 0, TimeSpan.Zero) },
        new() { Id = "ord-00004", CustomerId = "c3", CustomerName = "Carol White",   Status = "Cancelled", Total = 19.99m,  Country = "US", Placed = new(2024, 1, 8, 0, 0, 0, TimeSpan.Zero) },
        new() { Id = "ord-00005", CustomerId = "c4", CustomerName = "David Brown",   Status = "Shipped",   Total = 1540.50m, Country = "GB", Placed = new(2024, 4, 30, 0, 0, 0, TimeSpan.Zero) }
    ];
}
