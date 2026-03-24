using System.Text.Json;
using Shiny.DocumentDb;
using Shiny.DocumentDb.Sqlite;
using Sample;

var dbPath = Path.Combine(Path.GetTempPath(), "sample-docdb.db");
if (File.Exists(dbPath))
    File.Delete(dbPath);

// ═══════════════════════════════════════════════════════════════════
// 1. Setup — AOT-safe context, table-per-type, custom Id property
// ═══════════════════════════════════════════════════════════════════
Console.WriteLine("═══ Setup ═══");

var jsonContext = new SampleJsonContext(new JsonSerializerOptions
{
    PropertyNamingPolicy = JsonNamingPolicy.CamelCase
});

using var store = new SqliteDocumentStore(new DocumentStoreOptions
{
    DatabaseProvider = new SqliteDatabaseProvider($"Data Source={dbPath}"),
    JsonSerializerOptions = jsonContext.Options,
    UseReflectionFallback = false, // AOT: throw if type not in context
    // Logging = sql => Console.WriteLine($"  SQL: {sql}") // uncomment to see generated SQL
}
.MapTypeToTable<Order>("orders")                     // explicit table name
.MapTypeToTable<Sensor>("sensors", s => s.DeviceKey) // custom Id property
// Customer stays in the default "documents" table
);

Console.WriteLine($"Database: {dbPath}");
Console.WriteLine();

// ═══════════════════════════════════════════════════════════════════
// 2. Insert
// ═══════════════════════════════════════════════════════════════════
Console.WriteLine("═══ Insert ═══");

var alice = new Customer { Id = "alice", Name = "Alice", Age = 30, Email = "alice@example.com" };
var bob = new Customer { Id = "bob", Name = "Bob", Age = 25, Email = "bob@example.com" };
var carol = new Customer { Id = "carol", Name = "Carol", Age = 35 };
await store.Insert(alice);
await store.Insert(bob);
await store.Insert(carol);
Console.WriteLine($"Inserted {await store.Count<Customer>()} customers");

var orders = new[]
{
    new Order
    {
        Id = "ord-1", CustomerName = "Alice", Status = "Shipped",
        ShippingAddress = new() { Street = "123 Main St", City = "Portland", State = "OR" },
        Lines = [new() { ProductName = "Widget", Quantity = 2, UnitPrice = 9.99m },
                 new() { ProductName = "Gadget", Quantity = 1, UnitPrice = 24.99m }],
        Tags = ["priority", "free-shipping"]
    },
    new Order
    {
        Id = "ord-2", CustomerName = "Bob", Status = "Pending",
        ShippingAddress = new() { Street = "456 Oak Ave", City = "Seattle", State = "WA" },
        Lines = [new() { ProductName = "Widget", Quantity = 5, UnitPrice = 9.99m }],
        Tags = ["bulk"]
    },
    new Order
    {
        Id = "ord-3", CustomerName = "Alice", Status = "Shipped",
        ShippingAddress = new() { Street = "789 Pine Rd", City = "Portland", State = "OR" },
        Lines = [new() { ProductName = "Gadget", Quantity = 3, UnitPrice = 24.99m },
                 new() { ProductName = "Thingamajig", Quantity = 1, UnitPrice = 4.50m }],
        Tags = ["priority"]
    },
    new Order
    {
        Id = "ord-4", CustomerName = "Carol", Status = "Cancelled",
        ShippingAddress = new() { Street = "321 Elm Blvd", City = "Eugene", State = "OR" },
        Lines = [new() { ProductName = "Widget", Quantity = 1, UnitPrice = 9.99m }],
        Tags = []
    }
};
var batchCount = await store.BatchInsert(orders);
Console.WriteLine($"Batch-inserted {batchCount} orders (in 'orders' table)");

// Sensor — uses custom Id property (DeviceKey) with Guid auto-generation
var sensor = new Sensor { Location = "Warehouse A", Temperature = 22.5, ReadingAt = DateTimeOffset.UtcNow };
await store.Insert(sensor);
Console.WriteLine($"Inserted sensor — auto-generated DeviceKey: {sensor.DeviceKey}");
Console.WriteLine();

// ═══════════════════════════════════════════════════════════════════
// 3. Get by Id
// ═══════════════════════════════════════════════════════════════════
Console.WriteLine("═══ Get by Id ═══");
var fetched = await store.Get<Customer>("alice");
Console.WriteLine($"Get<Customer>(\"alice\") → {fetched!.Name}, Age={fetched.Age}, Email={fetched.Email}");

var fetchedSensor = await store.Get<Sensor>(sensor.DeviceKey);
Console.WriteLine($"Get<Sensor>({sensor.DeviceKey}) → {fetchedSensor!.Location}, Temp={fetchedSensor.Temperature}°C");
Console.WriteLine();

// ═══════════════════════════════════════════════════════════════════
// 4. Update (full replacement)
// ═══════════════════════════════════════════════════════════════════
Console.WriteLine("═══ Update ═══");
fetched.Age = 31;
fetched.Email = "alice.new@example.com";
await store.Update(fetched);
var updated = await store.Get<Customer>("alice");
Console.WriteLine($"After Update: Age={updated!.Age}, Email={updated.Email}");
Console.WriteLine();

// ═══════════════════════════════════════════════════════════════════
// 4b. GetDiff (diff against stored document)
// ═══════════════════════════════════════════════════════════════════
Console.WriteLine("═══ Patch Document (Diff) ═══");

// Diff an Order — has nested Address, collection of OrderLines, and Tags
var proposedOrder = new Order
{
    Id = "ord-1", CustomerName = "Alice", Status = "Delivered",
    ShippingAddress = new() { Street = "123 Main St", City = "Seattle", State = "WA" },
    Lines = [new() { ProductName = "Widget", Quantity = 10, UnitPrice = 8.99m },
             new() { ProductName = "Gadget", Quantity = 1, UnitPrice = 24.99m },
             new() { ProductName = "Doohickey", Quantity = 2, UnitPrice = 14.50m }],
    Tags = ["priority", "expedited"]
};
var orderPatch = await store.GetDiff("ord-1", proposedOrder);
Console.WriteLine($"Patch operations vs stored ord-1 ({orderPatch!.Operations.Count} changes):");
foreach (var op in orderPatch.Operations)
    Console.WriteLine($"  {op.Op} {op.Path} → {op.Value}");

// Apply the patch to a fresh copy
var freshOrder = await store.Get<Order>("ord-1");
var patchedOrder = orderPatch.ApplyTo(freshOrder!);
Console.WriteLine($"After applying: Status={patchedOrder.Status}, City={patchedOrder.ShippingAddress.City}, Lines={patchedOrder.Lines.Count}, Tags=[{string.Join(",", patchedOrder.Tags)}]");
Console.WriteLine();

// ═══════════════════════════════════════════════════════════════════
// 5. Upsert (JSON Merge Patch)
// ═══════════════════════════════════════════════════════════════════
Console.WriteLine("═══ Upsert (Merge Patch) ═══");
await store.Upsert(new Customer { Id = "bob", Name = "Bobby" });
var bobAfter = await store.Get<Customer>("bob");
Console.WriteLine($"Upsert patched Name → {bobAfter!.Name}, Email preserved → {bobAfter.Email}");
Console.WriteLine();

// ═══════════════════════════════════════════════════════════════════
// 6. SetProperty / RemoveProperty (surgical field updates)
// ═══════════════════════════════════════════════════════════════════
Console.WriteLine("═══ SetProperty / RemoveProperty ═══");
await store.SetProperty<Customer>("carol", c => c.Email, "carol@example.com");
var carolAfter = await store.Get<Customer>("carol");
Console.WriteLine($"SetProperty Email → {carolAfter!.Email}");

await store.RemoveProperty<Customer>("carol", c => c.Email);
carolAfter = await store.Get<Customer>("carol");
Console.WriteLine($"RemoveProperty Email → {carolAfter!.Email ?? "(null)"}");

// Nested property on an order
await store.SetProperty<Order>("ord-2", o => o.ShippingAddress.City, "Tacoma");
var ord2 = await store.Get<Order>("ord-2");
Console.WriteLine($"SetProperty nested ShippingAddress.City → {ord2!.ShippingAddress.City}");
Console.WriteLine();

// ═══════════════════════════════════════════════════════════════════
// 7. Fluent query builder — Where, OrderBy, Paginate
// ═══════════════════════════════════════════════════════════════════
Console.WriteLine("═══ Fluent Queries ═══");

var shippedOrders = await store.Query<Order>()
    .Where(o => o.Status == "Shipped")
    .OrderBy(o => o.CustomerName)
    .ToList();
Console.WriteLine($"Shipped orders: {shippedOrders.Count}");
foreach (var o in shippedOrders)
    Console.WriteLine($"  {o.Id} — {o.CustomerName} → {o.ShippingAddress.City}");

// Nested property query
var portlandOrders = await store.Query<Order>()
    .Where(o => o.ShippingAddress.City == "Portland")
    .ToList();
Console.WriteLine($"Portland orders: {portlandOrders.Count}");

// Collection Any() with predicate
var hasWidget = await store.Query<Order>()
    .Where(o => o.Lines.Any(l => l.ProductName == "Widget"))
    .ToList();
Console.WriteLine($"Orders with Widget: {hasWidget.Count}");

// Collection Count() comparison
var multiLineOrders = await store.Query<Order>()
    .Where(o => o.Lines.Count() > 1)
    .ToList();
Console.WriteLine($"Orders with >1 line: {multiLineOrders.Count}");

// String methods
var startsWithA = await store.Query<Customer>()
    .Where(c => c.Name.StartsWith("A"))
    .ToList();
Console.WriteLine($"Customers starting with 'A': {startsWithA.Count}");

// Pagination
var page1 = await store.Query<Customer>()
    .OrderBy(c => c.Name)
    .Paginate(0, 2)
    .ToList();
Console.WriteLine($"Page 1 (2 per page): {string.Join(", ", page1.Select(c => c.Name))}");
Console.WriteLine();

// ═══════════════════════════════════════════════════════════════════
// 8. Count / Any
// ═══════════════════════════════════════════════════════════════════
Console.WriteLine("═══ Count / Any ═══");
var count = await store.Query<Order>().Where(o => o.Status == "Shipped").Count();
var any = await store.Query<Order>().Where(o => o.Status == "Delivered").Any();
Console.WriteLine($"Shipped count: {count}");
Console.WriteLine($"Any delivered: {any}");
Console.WriteLine();

// ═══════════════════════════════════════════════════════════════════
// 9. Projections (SQL-level json_object)
// ═══════════════════════════════════════════════════════════════════
Console.WriteLine("═══ Projections ═══");
var summaries = await store.Query<Order>()
    .Where(o => o.Status == "Shipped")
    .Select(o => new OrderSummary
    {
        Customer = o.CustomerName,
        City = o.ShippingAddress.City,
        LineCount = o.Lines.Count()
    }, jsonContext.OrderSummary)
    .ToList();
foreach (var s in summaries)
    Console.WriteLine($"  {s.Customer} in {s.City}, {s.LineCount} line(s)");
Console.WriteLine();

// ═══════════════════════════════════════════════════════════════════
// 10. Aggregate projections (GROUP BY)
// ═══════════════════════════════════════════════════════════════════
Console.WriteLine("═══ Aggregate Projections (GROUP BY) ═══");
var stats = await store.Query<Order>()
    .Select(o => new OrderStats
    {
        Status = o.Status,
        OrderCount = Sql.Count()
    }, jsonContext.OrderStats)
    .ToList();
foreach (var s in stats)
    Console.WriteLine($"  {s.Status}: {s.OrderCount} order(s)");
Console.WriteLine();

// ═══════════════════════════════════════════════════════════════════
// 11. Scalar aggregates (Max, Min, Sum, Average)
// ═══════════════════════════════════════════════════════════════════
Console.WriteLine("═══ Scalar Aggregates ═══");
var maxAge = await store.Query<Customer>().Max(c => c.Age);
var minAge = await store.Query<Customer>().Min(c => c.Age);
var sumAge = await store.Query<Customer>().Sum(c => c.Age);
var avgAge = await store.Query<Customer>().Average(c => c.Age);
Console.WriteLine($"Customer ages — Max={maxAge}, Min={minAge}, Sum={sumAge}, Avg={avgAge:F1}");
Console.WriteLine();

// ═══════════════════════════════════════════════════════════════════
// 12. Streaming (IAsyncEnumerable)
// ═══════════════════════════════════════════════════════════════════
Console.WriteLine("═══ Streaming ═══");
Console.Write("Streaming all customers: ");
await foreach (var c in store.Query<Customer>().OrderBy(c => c.Name).ToAsyncEnumerable())
    Console.Write($"{c.Name} ");
Console.WriteLine();
Console.WriteLine();

// ═══════════════════════════════════════════════════════════════════
// 13. ExecuteUpdate (bulk field update)
// ═══════════════════════════════════════════════════════════════════
Console.WriteLine("═══ ExecuteUpdate ═══");
var updatedRows = await store.Query<Order>()
    .Where(o => o.Status == "Cancelled")
    .ExecuteUpdate(o => o.Status, "Archived");
Console.WriteLine($"Bulk-updated {updatedRows} cancelled order(s) to 'Archived'");
var archived = await store.Get<Order>("ord-4");
Console.WriteLine($"  ord-4 status → {archived!.Status}");
Console.WriteLine();

// ═══════════════════════════════════════════════════════════════════
// 14. ExecuteDelete (bulk delete)
// ═══════════════════════════════════════════════════════════════════
Console.WriteLine("═══ ExecuteDelete ═══");
var deleted = await store.Query<Order>()
    .Where(o => o.Status == "Archived")
    .ExecuteDelete();
Console.WriteLine($"Deleted {deleted} archived order(s). Remaining: {await store.Count<Order>()}");
Console.WriteLine();

// ═══════════════════════════════════════════════════════════════════
// 15. Raw SQL query
// ═══════════════════════════════════════════════════════════════════
Console.WriteLine("═══ Raw SQL Query ═══");
var rawResults = await store.Query<Customer>(
    "json_extract(Data, '$.age') >= @minAge",
    parameters: new { minAge = 30 });
Console.WriteLine($"Customers with age >= 30: {string.Join(", ", rawResults.Select(c => $"{c.Name}({c.Age})"))}");
Console.WriteLine();

// ═══════════════════════════════════════════════════════════════════
// 16. Dynamic query building
// ═══════════════════════════════════════════════════════════════════
Console.WriteLine("═══ Dynamic Query Building ═══");

// Simulate search parameters (e.g. from user input / API request)
string? nameFilter = "A";
int? minAgeFilter = null;
string? emailFilter = "alice";
string sortBy = "name";
int page = 0, pageSize = 10;

var dynamicQuery = store.Query<Customer>();

if (!string.IsNullOrEmpty(nameFilter))
    dynamicQuery = dynamicQuery.Where(c => c.Name.StartsWith(nameFilter));

if (minAgeFilter.HasValue)
    dynamicQuery = dynamicQuery.Where(c => c.Age >= minAgeFilter.Value);

if (!string.IsNullOrEmpty(emailFilter))
    dynamicQuery = dynamicQuery.Where(c => c.Email != null && c.Email.Contains(emailFilter));

dynamicQuery = sortBy switch
{
    "name" => dynamicQuery.OrderBy(c => c.Name),
    "age"  => dynamicQuery.OrderByDescending(c => c.Age),
    _ => dynamicQuery
};

var dynamicResults = await dynamicQuery.Paginate(page * pageSize, pageSize).ToList();
var totalMatches = await dynamicQuery.Count();
Console.WriteLine($"Dynamic query: {dynamicResults.Count} result(s) on page, {totalMatches} total match(es)");
foreach (var c in dynamicResults)
    Console.WriteLine($"  {c.Name} (age {c.Age})");
Console.WriteLine();

// ═══════════════════════════════════════════════════════════════════
// 17. Transactions
// ═══════════════════════════════════════════════════════════════════
Console.WriteLine("═══ Transactions ═══");
try
{
    await store.RunInTransaction(async tx =>
    {
        await tx.Insert(new Customer { Id = "dave", Name = "Dave", Age = 28 });
        await tx.Insert(new Customer { Id = "eve", Name = "Eve", Age = 22 });
        throw new Exception("Simulated failure");
    });
}
catch (Exception ex)
{
    Console.WriteLine($"Transaction rolled back: {ex.Message}");
}
Console.WriteLine($"Customers after rollback: {await store.Count<Customer>()} (Dave and Eve not persisted)");

await store.RunInTransaction(async tx =>
{
    await tx.Insert(new Customer { Id = "dave", Name = "Dave", Age = 28 });
    await tx.Insert(new Customer { Id = "eve", Name = "Eve", Age = 22 });
});
Console.WriteLine($"Customers after successful tx: {await store.Count<Customer>()}");
Console.WriteLine();

// ═══════════════════════════════════════════════════════════════════
// 18. Index management
// ═══════════════════════════════════════════════════════════════════
Console.WriteLine("═══ Index Management ═══");
await store.CreateIndexAsync<Customer>(c => c.Name, jsonContext.Customer);
Console.WriteLine("Created index on Customer.Name");

await store.CreateIndexAsync<Order>(o => o.ShippingAddress.City, jsonContext.Order);
Console.WriteLine("Created index on Order.ShippingAddress.City (nested property)");

await store.DropIndexAsync<Customer>(c => c.Name, jsonContext.Customer);
Console.WriteLine("Dropped index on Customer.Name");

await store.DropAllIndexesAsync<Order>();
Console.WriteLine("Dropped all indexes on Order");
Console.WriteLine();

// ═══════════════════════════════════════════════════════════════════
// 19. Remove / Clear
// ═══════════════════════════════════════════════════════════════════
Console.WriteLine("═══ Remove / Clear ═══");
var wasRemoved = await store.Remove<Customer>("eve");
Console.WriteLine($"Removed Eve: {wasRemoved}");

var cleared = await store.Clear<Sensor>();
Console.WriteLine($"Cleared {cleared} sensor(s)");
Console.WriteLine();

// ═══════════════════════════════════════════════════════════════════
// 20. Backup
// ═══════════════════════════════════════════════════════════════════
Console.WriteLine("═══ Backup ═══");
var backupPath = Path.Combine(Path.GetTempPath(), "sample-docdb-backup.db");
await store.Backup(backupPath);
Console.WriteLine($"Hot backup created at: {backupPath}");
Console.WriteLine();

// ═══════════════════════════════════════════════════════════════════
// Final state
// ═══════════════════════════════════════════════════════════════════
Console.WriteLine("═══ Final State ═══");
Console.WriteLine($"Customers: {await store.Count<Customer>()}");
Console.WriteLine($"Orders: {await store.Count<Order>()}");
Console.WriteLine($"Sensors: {await store.Count<Sensor>()}");

// Cleanup
File.Delete(dbPath);
File.Delete(backupPath);
Console.WriteLine("\nDone! Temp files cleaned up.");
