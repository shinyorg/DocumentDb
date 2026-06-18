using Shiny.DocumentDb;

namespace Sample.Maui;

public static class SeedData
{
    static readonly string[] Cities = ["Portland", "Seattle", "San Francisco", "Denver", "Austin", "Chicago", "New York", "Boston"];
    static readonly string[] Statuses = ["Pending", "Shipped", "Delivered", "Cancelled"];
    static readonly string[] Products = ["Widget", "Gadget", "Doohickey", "Thingamajig", "Gizmo", "Contraption"];

    public static async Task SeedAsync(IDocumentStore store)
    {
        var existing = await store.Query<Customer>().Count();
        if (existing > 0)
            return;

        var random = new Random(42);
        var customers = Enumerable.Range(1, 50).Select(i => new Customer
        {
            Id = $"cust-{i}",
            Name = $"Customer {i}",
            Age = random.Next(18, 70),
            Email = $"customer{i}@example.com",
            City = Cities[random.Next(Cities.Length)]
        }).ToList();

        await store.BatchInsert(customers);

        var orders = new List<Order>();
        foreach (var customer in customers)
        {
            var orderCount = random.Next(1, 5);
            for (var j = 0; j < orderCount; j++)
            {
                var lines = Enumerable.Range(1, random.Next(1, 4)).Select(_ => new OrderLine
                {
                    ProductName = Products[random.Next(Products.Length)],
                    Quantity = random.Next(1, 10),
                    UnitPrice = Math.Round((decimal)(random.NextDouble() * 100), 2)
                }).ToList();

                orders.Add(new Order
                {
                    Id = Guid.NewGuid().ToString("N"),
                    CustomerId = customer.Id,
                    CustomerName = customer.Name,
                    Status = Statuses[random.Next(Statuses.Length)],
                    Total = lines.Sum(l => l.Quantity * l.UnitPrice),
                    CreatedAt = DateTime.UtcNow.AddDays(-random.Next(1, 365)),
                    Lines = lines
                });
            }
        }

        await store.BatchInsert(orders);
    }

    public static async Task SeedVectorsAsync(IDocumentStore store)
    {
        if (!store.SupportsVector)
            return;

        var existing = await store.Query<VectorNote>().Count();
        if (existing > 0)
            return;

        // 4 toy embedding axes: [animals, technology, food, travel].
        var notes = new List<VectorNote>
        {
            Note("Golden Retriever",   "Animals",    0.95f, 0.05f, 0.03f, 0.02f),
            Note("Siamese Cat",        "Animals",    0.90f, 0.08f, 0.05f, 0.03f),
            Note("African Elephant",   "Animals",    0.88f, 0.04f, 0.02f, 0.10f),
            Note("Quantum Computer",   "Technology", 0.05f, 0.94f, 0.03f, 0.02f),
            Note("Neural Network",     "Technology", 0.08f, 0.90f, 0.04f, 0.02f),
            Note("Smartphone Chip",    "Technology", 0.04f, 0.88f, 0.02f, 0.06f),
            Note("Margherita Pizza",   "Food",       0.03f, 0.04f, 0.93f, 0.03f),
            Note("Sushi Platter",      "Food",       0.05f, 0.06f, 0.88f, 0.05f),
            Note("Backpacking Europe", "Travel",     0.03f, 0.05f, 0.04f, 0.92f),
            Note("Beach Resort",       "Travel",     0.02f, 0.08f, 0.10f, 0.86f),
        };
        await store.BatchInsert(notes);
    }

    static VectorNote Note(string title, string category, float a, float b, float c, float d)
        => new()
        {
            Id = title.Replace(' ', '-').ToLowerInvariant(),
            Title = title,
            Category = category,
            Embedding = new[] { a, b, c, d }
        };
}
