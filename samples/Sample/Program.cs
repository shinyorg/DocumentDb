using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.Json.Serialization.Metadata;
using Shiny.DocumentDb;
using Shiny.DocumentDb.Sqlite;
using Sample;

// ═══════════════════════════════════════════════════════════════════════
// Shiny.DocumentDb — Interactive Query Explorer
//
// Seeds sample Customers and Orders, then lets you run the string-based
// Where / OrderBy / Project clauses interactively. Leave any clause blank:
//   • blank Where    → no filter (all documents)
//   • blank OrderBy  → no ordering
//   • blank Project  → the whole document
// ═══════════════════════════════════════════════════════════════════════

var dbPath = Path.Combine(Path.GetTempPath(), "sample-query-explorer.db");
if (File.Exists(dbPath))
    File.Delete(dbPath);

var ctx = new SampleJsonContext(new JsonSerializerOptions { PropertyNamingPolicy = JsonNamingPolicy.CamelCase });

using var store = new SqliteDocumentStore(new DocumentStoreOptions
{
    DatabaseProvider = new SqliteDatabaseProvider($"Data Source={dbPath}"),
    JsonSerializerOptions = ctx.Options,
    UseReflectionFallback = false
}
.MapTypeToTable<Order>("orders"));

await SeedAsync(store);

Console.WriteLine("╔══════════════════════════════════════════════════════════════════╗");
Console.WriteLine("║          Shiny.DocumentDb — Interactive Query Explorer            ║");
Console.WriteLine("╚══════════════════════════════════════════════════════════════════╝");
Console.WriteLine($"Seeded {await store.Count<Customer>()} customers and {await store.Count<Order>()} orders.");
Console.WriteLine("Type 'help' for commands, 'samples' for example queries, 'quit' to exit.");
Console.WriteLine();

// ── Interactive loop ───────────────────────────────────────────────────
while (true)
{
    Console.Write("collection [customers] / orders / samples / help / quit > ");
    var line = Console.ReadLine();
    if (line is null)
        break; // EOF (piped input / non-interactive)

    line = line.Trim();
    switch (line.ToLowerInvariant())
    {
        case "quit" or "exit" or "q":
            goto done;
        case "help" or "?":
            PrintHelp();
            continue;
        case "samples":
            PrintSamples();
            continue;
    }

    var collection = line.ToLowerInvariant() switch
    {
        "orders" or "order" or "o" => "orders",
        "" or "customers" or "customer" or "c" => "customers",
        _ => null
    };
    if (collection is null)
    {
        Console.WriteLine($"  Unknown collection '{line}'. Use 'customers' or 'orders'.");
        continue;
    }

    var where = Prompt("  where   (blank = all)                       > ");
    var orderBy = Prompt("  orderby (blank = none, e.g. 'age desc')     > ");
    var project = Prompt("  project (blank = whole doc, e.g. 'name, lower(email) as email') > ");

    try
    {
        if (collection == "customers")
            await RunQuery(store.Query(ctx.Customer), ctx.Customer, where, orderBy, project);
        else
            await RunQuery(store.Query(ctx.Order), ctx.Order, where, orderBy, project);
    }
    catch (Exception ex)
    {
        Console.WriteLine($"  ✗ {ex.Message}");
    }
    Console.WriteLine();
}

done:
File.Delete(dbPath);
Console.WriteLine("Done.");

// ═══════════════════════════════════════════════════════════════════════
// Helpers
// ═══════════════════════════════════════════════════════════════════════

static string? Prompt(string label)
{
    Console.Write(label);
    var v = Console.ReadLine()?.Trim();
    return string.IsNullOrWhiteSpace(v) ? null : v;
}

static async Task RunQuery<T>(IDocumentQuery<T> query, JsonTypeInfo<T> info, string? where, string? orderBy, string? project) where T : class
{
    // Where — blank means no filter.
    if (where != null)
        query = query.Where(where, info);

    // OrderBy — blank means no ordering. Format: "field [asc|desc]".
    if (orderBy != null)
    {
        var parts = orderBy.Split(' ', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        var field = parts[0];
        var direction = parts.Length > 1 ? parts[1] : null;
        query = query.OrderBy(field, direction, info);
    }

    // Project — blank means the whole document.
    if (project != null)
    {
        var rows = await query.Project(project, info).ToList();
        Console.WriteLine($"  → {rows.Count} row(s):");
        foreach (var row in rows)
            Console.WriteLine($"      {row.ToJsonString()}");
    }
    else
    {
        var rows = await query.ToList();
        Console.WriteLine($"  → {rows.Count} document(s):");
        foreach (var doc in rows)
            Console.WriteLine($"      {JsonSerializer.Serialize(doc, info)}");
    }
}

static void PrintHelp()
{
    Console.WriteLine("""

      Commands
        customers | orders   Choose the collection to query (then you'll be prompted for clauses)
        samples              Show example queries you can paste in
        help                 This screen
        quit                 Exit

      At the clause prompts, leave any line BLANK for its default:
        where    blank → no filter (returns everything)
        orderby  blank → no ordering          (format: 'field' or 'field desc')
        project  blank → the whole document   (format: 'a, b, func(c) as alias')

      Field names are case-insensitive and support dotted paths (shippingAddress.city).
    """);
}

static void PrintSamples()
{
    Console.WriteLine("""

      ── Example WHERE clauses ──────────────────────────────────────────
      customers:
        age >= 30
        age >= 25 and age < 35
        name == 'Alice'
        name in ('Alice', 'Bob', 'Carol')
        email is not null
        contains(email, 'example')          startswith(name, 'A')
        lower(name) == 'alice'              length(name) > 4
        substring(name, 0, 2) == 'Al'
        year(createdAt) == 2024             month(createdAt) >= 6
        hasflag(access, 'Write')            (access & 2) == 2
        soundex(name) == soundex('Smith')   ← phonetic match (Smith/Smyth)
      orders:
        status == 'Shipped'
        shippingAddress.city == 'Portland'
        not (status == 'Cancelled')

      ── Example ORDERBY clauses ────────────────────────────────────────
        name            age desc            createdAt desc
        shippingAddress.city

      ── Example PROJECT clauses ────────────────────────────────────────
        name, age
        name, lower(email) as email, length(name) as nameLength
        name, year(createdAt) as joinedYear, soundex(name) as code
      orders:
        id, customerName, shippingAddress.city as city
    """);
}

static async Task SeedAsync(IDocumentStore store)
{
    Access RW = Access.Read | Access.Write;
    await store.Insert(new Customer { Id = "alice", Name = "Alice", Age = 30, Email = "alice@example.com", Access = RW, CreatedAt = new DateTimeOffset(2024, 3, 1, 0, 0, 0, TimeSpan.Zero) });
    await store.Insert(new Customer { Id = "bob", Name = "Bob", Age = 25, Email = "bob@example.com", Access = Access.Read, CreatedAt = new DateTimeOffset(2023, 6, 15, 0, 0, 0, TimeSpan.Zero) });
    await store.Insert(new Customer { Id = "carol", Name = "Carol", Age = 35, Email = "carol@example.com", Access = Access.Admin, CreatedAt = new DateTimeOffset(2024, 11, 20, 0, 0, 0, TimeSpan.Zero) });
    await store.Insert(new Customer { Id = "smith", Name = "Smith", Age = 41, Email = "smith@example.com", Access = Access.Read, CreatedAt = new DateTimeOffset(2022, 1, 10, 0, 0, 0, TimeSpan.Zero) });
    await store.Insert(new Customer { Id = "smyth", Name = "Smyth", Age = 28, Email = null, Access = Access.None, CreatedAt = new DateTimeOffset(2025, 2, 2, 0, 0, 0, TimeSpan.Zero) });
    await store.Insert(new Customer { Id = "dave", Name = "Dave", Age = 19, Email = "dave@example.com", Access = RW, CreatedAt = new DateTimeOffset(2024, 7, 7, 0, 0, 0, TimeSpan.Zero) });

    await store.BatchInsert(new[]
    {
        new Order { Id = "ord-1", CustomerName = "Alice", Status = "Shipped", ShippingAddress = new() { Street = "123 Main St", City = "Portland", State = "OR" }, Lines = [new() { ProductName = "Widget", Quantity = 2, UnitPrice = 9.99m }], Tags = ["priority"] },
        new Order { Id = "ord-2", CustomerName = "Bob", Status = "Pending", ShippingAddress = new() { Street = "456 Oak Ave", City = "Seattle", State = "WA" }, Lines = [new() { ProductName = "Gadget", Quantity = 1, UnitPrice = 24.99m }], Tags = ["bulk"] },
        new Order { Id = "ord-3", CustomerName = "Carol", Status = "Cancelled", ShippingAddress = new() { Street = "789 Pine Rd", City = "Portland", State = "OR" }, Lines = [new() { ProductName = "Thingamajig", Quantity = 3, UnitPrice = 4.50m }], Tags = [] }
    });
}
