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
}
