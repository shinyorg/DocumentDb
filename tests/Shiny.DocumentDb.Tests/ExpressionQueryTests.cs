using System.Text.Json;
using Shiny.DocumentDb.Tests.Fixtures;
using Xunit;

namespace Shiny.DocumentDb.Tests;

public abstract class ExpressionQueryTestsBase : IDisposable
{
    protected readonly IDatabaseFixture Fixture;
    protected readonly IDocumentStore store;
    static readonly TestJsonContext ctx = new(new JsonSerializerOptions { PropertyNamingPolicy = JsonNamingPolicy.CamelCase });

    protected ExpressionQueryTestsBase(IDatabaseFixture fixture)
    {
        this.Fixture = fixture;
        this.store = new DocumentStore(new DocumentStoreOptions
        {
            DatabaseProvider = fixture.CreateProvider(),
            JsonSerializerOptions = ctx.Options,
            TableName = $"t{Guid.NewGuid():N}"
        });
    }

    public void Dispose() => (this.store as IDisposable)?.Dispose();

    async Task SeedUsersAsync()
    {
        await this.store.Insert(new User { Id = "u1", Name = "Alice", Age = 25, Email = "alice@test.com" }, ctx.User);
        await this.store.Insert(new User { Id = "u2", Name = "Bob", Age = 35 }, ctx.User);
        await this.store.Insert(new User { Id = "u3", Name = "Charlie", Age = 25 }, ctx.User);
    }

    async Task SeedOrdersAsync()
    {
        await this.store.Insert(new Order
        {
            Id = "o1",
            CustomerName = "Alice",
            Status = "Shipped",
            ShippingAddress = new Address { Street = "123 Main St", City = "Portland", State = "OR", Zip = "97201" },
            Lines =
            [
                new OrderLine { ProductName = "Widget", Quantity = 2, UnitPrice = 9.99m },
                new OrderLine { ProductName = "Gadget", Quantity = 1, UnitPrice = 24.99m }
            ],
            Tags = ["priority", "wholesale"]
        }, ctx.Order);

        await this.store.Insert(new Order
        {
            Id = "o2",
            CustomerName = "Bob",
            Status = "Pending",
            ShippingAddress = new Address { Street = "456 Oak Ave", City = "Seattle", State = "WA", Zip = "98101" },
            Lines =
            [
                new OrderLine { ProductName = "Widget", Quantity = 5, UnitPrice = 9.99m }
            ],
            Tags = ["retail"]
        }, ctx.Order);

        await this.store.Insert(new Order
        {
            Id = "o3",
            CustomerName = "Charlie",
            Status = "Shipped",
            ShippingAddress = new Address { Street = "789 Elm Blvd", City = "Portland", State = "OR", Zip = "97205" },
            Lines =
            [
                new OrderLine { ProductName = "Doohickey", Quantity = 3, UnitPrice = 14.99m },
                new OrderLine { ProductName = "Gadget", Quantity = 2, UnitPrice = 24.99m }
            ],
            Tags = ["priority", "retail"]
        }, ctx.Order);
    }

    // ── Equality ─────────────────────────────────────────────────────

    [Fact]
    public async Task Query_Equality()
    {
        await this.SeedUsersAsync();

        var results = await this.store.Query(ctx.User).Where(u => u.Name == "Alice").ToList();

        Assert.Single(results);
        Assert.Equal("Alice", results[0].Name);
    }

    [Fact]
    public async Task Query_Equality_MultipleResults()
    {
        await this.SeedUsersAsync();

        var results = await this.store.Query(ctx.User).Where(u => u.Age == 25).ToList();

        Assert.Equal(2, results.Count);
    }

    [Fact]
    public async Task Query_NoMatches_ReturnsEmpty()
    {
        await this.SeedUsersAsync();

        var results = await this.store.Query(ctx.User).Where(u => u.Name == "Nobody").ToList();

        Assert.Empty(results);
    }

    // ── Numeric comparisons ──────────────────────────────────────────

    [Fact]
    public async Task Query_GreaterThan()
    {
        await this.SeedUsersAsync();

        var results = await this.store.Query(ctx.User).Where(u => u.Age > 30).ToList();

        Assert.Single(results);
        Assert.Equal("Bob", results[0].Name);
    }

    [Fact]
    public async Task Query_LessThanOrEqual()
    {
        await this.SeedUsersAsync();

        var results = await this.store.Query(ctx.User).Where(u => u.Age <= 25).ToList();

        Assert.Equal(2, results.Count);
    }

    // ── Logical operators ────────────────────────────────────────────

    [Fact]
    public async Task Query_AndAlso()
    {
        await this.SeedUsersAsync();

        var results = await this.store.Query(ctx.User).Where(u => u.Age == 25 && u.Name == "Alice").ToList();

        Assert.Single(results);
        Assert.Equal("Alice", results[0].Name);
    }

    [Fact]
    public async Task Query_OrElse()
    {
        await this.SeedUsersAsync();

        var results = await this.store.Query(ctx.User).Where(u => u.Name == "Alice" || u.Name == "Bob").ToList();

        Assert.Equal(2, results.Count);
    }

    [Fact]
    public async Task Query_Not()
    {
        await this.SeedUsersAsync();

        var results = await this.store.Query(ctx.User).Where(u => !(u.Name == "Alice")).ToList();

        Assert.Equal(2, results.Count);
        Assert.DoesNotContain(results, u => u.Name == "Alice");
    }

    // ── Null checks ──────────────────────────────────────────────────

    [Fact]
    public async Task Query_IsNull()
    {
        await this.SeedUsersAsync();

        var results = await this.store.Query(ctx.User).Where(u => u.Email == null).ToList();

        Assert.Equal(2, results.Count);
    }

    [Fact]
    public async Task Query_IsNotNull()
    {
        await this.SeedUsersAsync();

        var results = await this.store.Query(ctx.User).Where(u => u.Email != null).ToList();

        Assert.Single(results);
        Assert.Equal("Alice", results[0].Name);
    }

    // ── String methods ───────────────────────────────────────────────

    [Fact]
    public async Task Query_StringContains()
    {
        await this.SeedUsersAsync();

        var results = await this.store.Query(ctx.User).Where(u => u.Name.Contains("li")).ToList();

        Assert.Equal(2, results.Count); // Alice, Charlie
    }

    [Fact]
    public async Task Query_StringStartsWith()
    {
        await this.SeedUsersAsync();

        var results = await this.store.Query(ctx.User).Where(u => u.Name.StartsWith("Al")).ToList();

        Assert.Single(results);
        Assert.Equal("Alice", results[0].Name);
    }

    [Fact]
    public async Task Query_StringEndsWith()
    {
        await this.SeedUsersAsync();

        var results = await this.store.Query(ctx.User).Where(u => u.Name.EndsWith("ob")).ToList();

        Assert.Single(results);
        Assert.Equal("Bob", results[0].Name);
    }

    // ── Nested objects ───────────────────────────────────────────────

    [Fact]
    public async Task Query_NestedProperty()
    {
        await this.SeedOrdersAsync();

        var results = await this.store.Query(ctx.Order).Where(o => o.ShippingAddress.City == "Portland").ToList();

        Assert.Equal(2, results.Count);
        Assert.All(results, o => Assert.Equal("Portland", o.ShippingAddress.City));
    }

    // ── Any() on object collections ──────────────────────────────────

    [Fact]
    public async Task Query_Any_ObjectCollection()
    {
        await this.SeedOrdersAsync();

        var results = await this.store.Query(ctx.Order).Where(o => o.Lines.Any(l => l.ProductName == "Gadget")).ToList();

        Assert.Equal(2, results.Count);
        Assert.Contains(results, o => o.CustomerName == "Alice");
        Assert.Contains(results, o => o.CustomerName == "Charlie");
    }

    // ── Any() on primitive collections ───────────────────────────────

    [Fact]
    public async Task Query_Any_PrimitiveCollection()
    {
        await this.SeedOrdersAsync();

        var results = await this.store.Query(ctx.Order).Where(o => o.Tags.Any(t => t == "priority")).ToList();

        Assert.Equal(2, results.Count);
        Assert.Contains(results, o => o.CustomerName == "Alice");
        Assert.Contains(results, o => o.CustomerName == "Charlie");
    }

    // ── Any() without predicate ────────────────────────────────────

    [Fact]
    public async Task Query_Any_NoPredicate_HasElements()
    {
        await this.SeedOrdersAsync();

        var results = await this.store.Query(ctx.Order).Where(o => o.Tags.Any()).ToList();

        Assert.Equal(3, results.Count); // all orders have tags
    }

    [Fact]
    public async Task Query_Any_NoPredicate_EmptyCollection()
    {
        await this.store.Insert(new Order
        {
            Id = "o_empty",
            CustomerName = "Empty",
            Status = "Draft",
            ShippingAddress = new Address { Street = "", City = "", State = "", Zip = "" },
            Lines = [],
            Tags = []
        }, ctx.Order);

        await this.store.Insert(new Order
        {
            Id = "o_full",
            CustomerName = "Full",
            Status = "Active",
            ShippingAddress = new Address { Street = "1 St", City = "X", State = "Y", Zip = "0" },
            Lines = [new OrderLine { ProductName = "A", Quantity = 1, UnitPrice = 1m }],
            Tags = ["test"]
        }, ctx.Order);

        var results = await this.store.Query(ctx.Order).Where(o => o.Lines.Any()).ToList();

        Assert.Single(results);
        Assert.Equal("Full", results[0].CustomerName);
    }

    // ── Count() on collections ───────────────────────────────────────

    [Fact]
    public async Task Query_Count_NoPredicate()
    {
        await this.SeedOrdersAsync();

        // Orders with more than 1 line item
        var results = await this.store.Query(ctx.Order).Where(o => o.Lines.Count() > 1).ToList();

        Assert.Equal(2, results.Count);
        Assert.Contains(results, o => o.CustomerName == "Alice");
        Assert.Contains(results, o => o.CustomerName == "Charlie");
    }

    [Fact]
    public async Task Query_Count_WithPredicate()
    {
        await this.SeedOrdersAsync();

        // Orders that have at least 1 line with quantity >= 3
        var results = await this.store.Query(ctx.Order).Where(o => o.Lines.Count(l => l.Quantity >= 3) >= 1).ToList();

        Assert.Equal(2, results.Count);
        Assert.Contains(results, o => o.CustomerName == "Bob");     // Widget qty 5
        Assert.Contains(results, o => o.CustomerName == "Charlie"); // Doohickey qty 3
    }

    [Fact]
    public async Task Query_Count_PrimitiveCollection_WithPredicate()
    {
        await this.SeedOrdersAsync();

        // Orders with more than 1 tag matching a pattern — using count of tags == "priority"
        var results = await this.store.Query(ctx.Order).Where(o => o.Tags.Count(t => t == "priority") > 0).ToList();

        Assert.Equal(2, results.Count);
        Assert.Contains(results, o => o.CustomerName == "Alice");
        Assert.Contains(results, o => o.CustomerName == "Charlie");
    }

    [Fact]
    public async Task Query_CountProperty_GreaterThan()
    {
        await this.SeedOrdersAsync();

        // Property form: o.Lines.Count (not the .Count() method)
        var results = await this.store.Query(ctx.Order).Where(o => o.Lines.Count > 1).ToList();

        Assert.Equal(2, results.Count);
        Assert.Contains(results, o => o.CustomerName == "Alice");
        Assert.Contains(results, o => o.CustomerName == "Charlie");
    }

    [Fact]
    public async Task Query_CountProperty_Equals()
    {
        await this.SeedOrdersAsync();

        // Bob has exactly one line item
        var results = await this.store.Query(ctx.Order).Where(o => o.Lines.Count == 1).ToList();

        Assert.Single(results);
        Assert.Equal("Bob", results[0].CustomerName);
    }

    [Fact]
    public async Task Query_CountProperty_PrimitiveCollection()
    {
        await this.SeedOrdersAsync();

        // Tags.Count property on a primitive collection — only Bob has a single tag
        var results = await this.store.Query(ctx.Order).Where(o => o.Tags.Count == 1).ToList();

        Assert.Single(results);
        Assert.Equal("Bob", results[0].CustomerName);
    }

    // ── Captured variables ───────────────────────────────────────────

    [Fact]
    public async Task Query_CapturedVariable()
    {
        await this.SeedUsersAsync();

        var targetName = "Alice";
        var results = await this.store.Query(ctx.User).Where(u => u.Name == targetName).ToList();

        Assert.Single(results);
        Assert.Equal("Alice", results[0].Name);
    }

    [Fact]
    public async Task Query_CapturedVariable_Numeric()
    {
        await this.SeedUsersAsync();

        var minAge = 30;
        var results = await this.store.Query(ctx.User).Where(u => u.Age > minAge).ToList();

        Assert.Single(results);
        Assert.Equal("Bob", results[0].Name);
    }

    // ── Count with expressions ───────────────────────────────────────

    [Fact]
    public async Task Count_WithExpression()
    {
        await this.SeedUsersAsync();

        var count = await this.store.Query(ctx.User).Where(u => u.Age == 25).Count();

        Assert.Equal(2, count);
    }

    [Fact]
    public async Task Count_WithExpression_NoMatches()
    {
        await this.SeedUsersAsync();

        var count = await this.store.Query(ctx.User).Where(u => u.Name == "Nobody").Count();

        Assert.Equal(0, count);
    }

    // ── Count with Any/Count collection predicates ───────────────────

    [Fact]
    public async Task Count_WithAnyPredicate()
    {
        await this.SeedOrdersAsync();

        // Count orders that contain a "Gadget" line item
        var count = await this.store.Query(ctx.Order).Where(o => o.Lines.Any(l => l.ProductName == "Gadget")).Count();

        Assert.Equal(2, count); // Alice and Charlie
    }

    [Fact]
    public async Task Count_WithAnyNoPredicate()
    {
        await this.SeedOrdersAsync();

        // Count orders that have any tags at all
        var count = await this.store.Query(ctx.Order).Where(o => o.Tags.Any()).Count();

        Assert.Equal(3, count); // all three orders have tags
    }

    [Fact]
    public async Task Count_WithCollectionCount()
    {
        await this.SeedOrdersAsync();

        // Count orders with more than 1 line item
        var count = await this.store.Query(ctx.Order).Where(o => o.Lines.Count() > 1).Count();

        Assert.Equal(2, count); // Alice (2 lines) and Charlie (2 lines)
    }

    [Fact]
    public async Task Count_WithCollectionCountPredicate()
    {
        await this.SeedOrdersAsync();

        // Count orders that have at least 1 line with quantity >= 3
        var count = await this.store.Query(ctx.Order).Where(o => o.Lines.Count(l => l.Quantity >= 3) >= 1).Count();

        Assert.Equal(2, count); // Bob (qty 5) and Charlie (qty 3)
    }

    // ── DateTime / DateTimeOffset queries ────────────────────────────

    async Task SeedEventsAsync()
    {
        await this.store.Insert(new Event
        {
            Id = "e1",
            Title = "Past",
            StartDate = new DateTime(2024, 1, 15, 10, 0, 0, DateTimeKind.Utc),
            CreatedAt = new DateTimeOffset(2024, 1, 10, 8, 0, 0, TimeSpan.Zero)
        }, ctx.Event);

        await this.store.Insert(new Event
        {
            Id = "e2",
            Title = "Present",
            StartDate = new DateTime(2025, 6, 1, 12, 0, 0, DateTimeKind.Utc),
            CreatedAt = new DateTimeOffset(2025, 5, 20, 9, 0, 0, TimeSpan.Zero)
        }, ctx.Event);

        await this.store.Insert(new Event
        {
            Id = "e3",
            Title = "Future",
            StartDate = new DateTime(2026, 12, 25, 18, 0, 0, DateTimeKind.Utc),
            CreatedAt = new DateTimeOffset(2026, 11, 1, 14, 0, 0, TimeSpan.Zero)
        }, ctx.Event);
    }

    [Fact]
    public async Task Query_DateTime_Equality()
    {
        await this.SeedEventsAsync();

        var target = new DateTime(2025, 6, 1, 12, 0, 0, DateTimeKind.Utc);
        var results = await this.store.Query(ctx.Event).Where(e => e.StartDate == target).ToList();

        Assert.Single(results);
        Assert.Equal("Present", results[0].Title);
    }

    [Fact]
    public async Task Query_DateTime_GreaterThan()
    {
        await this.SeedEventsAsync();

        var cutoff = new DateTime(2025, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        var results = await this.store.Query(ctx.Event).Where(e => e.StartDate > cutoff).ToList();

        Assert.Equal(2, results.Count);
        Assert.Contains(results, e => e.Title == "Present");
        Assert.Contains(results, e => e.Title == "Future");
    }

    [Fact]
    public async Task Query_DateTime_LessThan()
    {
        await this.SeedEventsAsync();

        var cutoff = new DateTime(2025, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        var results = await this.store.Query(ctx.Event).Where(e => e.StartDate < cutoff).ToList();

        Assert.Single(results);
        Assert.Equal("Past", results[0].Title);
    }

    [Fact]
    public async Task Query_DateTimeOffset_GreaterThan()
    {
        await this.SeedEventsAsync();

        var cutoff = new DateTimeOffset(2025, 1, 1, 0, 0, 0, TimeSpan.Zero);
        var results = await this.store.Query(ctx.Event).Where(e => e.CreatedAt > cutoff).ToList();

        Assert.Equal(2, results.Count);
        Assert.Contains(results, e => e.Title == "Present");
        Assert.Contains(results, e => e.Title == "Future");
    }

    [Fact]
    public async Task Query_DateTimeOffset_Range()
    {
        await this.SeedEventsAsync();

        var start = new DateTimeOffset(2025, 1, 1, 0, 0, 0, TimeSpan.Zero);
        var end = new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);
        var results = await this.store.Query(ctx.Event)
            .Where(e => e.CreatedAt >= start && e.CreatedAt < end)
            .ToList();

        Assert.Single(results);
        Assert.Equal("Present", results[0].Title);
    }

    [Fact]
    public async Task Count_DateTime()
    {
        await this.SeedEventsAsync();

        var cutoff = new DateTime(2025, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        var count = await this.store.Query(ctx.Event).Where(e => e.StartDate >= cutoff).Count();

        Assert.Equal(2, count);
    }

    // ── Remove with expressions ─────────────────────────────────────

    [Fact]
    public async Task Remove_SimplePredicate()
    {
        await this.SeedUsersAsync();

        var deleted = await this.store.Query(ctx.User).Where(u => u.Name == "Alice").ExecuteDelete();

        Assert.Equal(1, deleted);
        var remaining = await this.store.Query(ctx.User).ToList();
        Assert.Equal(2, remaining.Count);
        Assert.DoesNotContain(remaining, u => u.Name == "Alice");
    }

    [Fact]
    public async Task Remove_ComplexPredicate_AndOr()
    {
        await this.SeedUsersAsync();

        var deleted = await this.store.Query(ctx.User).Where(u => u.Name == "Alice" || u.Age > 30).ExecuteDelete();

        Assert.Equal(2, deleted);
        var remaining = await this.store.Query(ctx.User).ToList();
        Assert.Single(remaining);
        Assert.Equal("Charlie", remaining[0].Name);
    }

    [Fact]
    public async Task Remove_NestedProperty()
    {
        await this.SeedOrdersAsync();

        var deleted = await this.store.Query(ctx.Order).Where(o => o.ShippingAddress.City == "Portland").ExecuteDelete();

        Assert.Equal(2, deleted);
        var remaining = await this.store.Query(ctx.Order).ToList();
        Assert.Single(remaining);
        Assert.Equal("Bob", remaining[0].CustomerName);
    }

    [Fact]
    public async Task Remove_CapturedVariable()
    {
        await this.SeedUsersAsync();

        var minAge = 30;
        var deleted = await this.store.Query(ctx.User).Where(u => u.Age > minAge).ExecuteDelete();

        Assert.Equal(1, deleted);
        var remaining = await this.store.Query(ctx.User).ToList();
        Assert.Equal(2, remaining.Count);
        Assert.DoesNotContain(remaining, u => u.Name == "Bob");
    }

    [Fact]
    public async Task Remove_ReturnsCorrectCount()
    {
        await this.SeedUsersAsync();

        var deleted = await this.store.Query(ctx.User).Where(u => u.Age == 25).ExecuteDelete();

        Assert.Equal(2, deleted);
    }

    [Fact]
    public async Task Remove_NoMatches_ReturnsZero()
    {
        await this.SeedUsersAsync();

        var deleted = await this.store.Query(ctx.User).Where(u => u.Name == "Nobody").ExecuteDelete();

        Assert.Equal(0, deleted);
        var remaining = await this.store.Query(ctx.User).ToList();
        Assert.Equal(3, remaining.Count);
    }

    // ── ExecuteUpdate with expressions ─────────────────────────────

    [Fact]
    public async Task ExecuteUpdate_UpdatesMatchingDocuments()
    {
        await this.SeedUsersAsync();

        var updated = await this.store.Query(ctx.User).Where(u => u.Age == 25).ExecuteUpdate(u => u.Age, 30);

        Assert.Equal(2, updated);
        var alice = (await this.store.Query(ctx.User).Where(u => u.Name == "Alice").ToList())[0];
        Assert.Equal(30, alice.Age);
        var charlie = (await this.store.Query(ctx.User).Where(u => u.Name == "Charlie").ToList())[0];
        Assert.Equal(30, charlie.Age);
        // Bob unchanged
        var bob = (await this.store.Query(ctx.User).Where(u => u.Name == "Bob").ToList())[0];
        Assert.Equal(35, bob.Age);
    }

    [Fact]
    public async Task ExecuteUpdate_WithNoMatch_ReturnsZero()
    {
        await this.SeedUsersAsync();

        var updated = await this.store.Query(ctx.User).Where(u => u.Name == "Nobody").ExecuteUpdate(u => u.Age, 99);

        Assert.Equal(0, updated);
    }

    [Fact]
    public async Task ExecuteUpdate_WithoutWhere_UpdatesAllOfType()
    {
        await this.SeedUsersAsync();

        var updated = await this.store.Query(ctx.User).ExecuteUpdate(u => u.Age, 50);

        Assert.Equal(3, updated);
        var all = await this.store.Query(ctx.User).ToList();
        Assert.All(all, u => Assert.Equal(50, u.Age));
    }

    [Fact]
    public async Task ExecuteUpdate_NestedProperty()
    {
        await this.SeedOrdersAsync();

        var updated = await this.store.Query(ctx.Order)
            .Where(o => o.ShippingAddress.City == "Portland")
            .ExecuteUpdate(o => o.ShippingAddress.City, "Eugene");

        Assert.Equal(2, updated);
        var orders = await this.store.Query(ctx.Order).Where(o => o.ShippingAddress.City == "Eugene").ToList();
        Assert.Equal(2, orders.Count);
        // Bob's Seattle unchanged
        var bob = (await this.store.Query(ctx.Order).Where(o => o.CustomerName == "Bob").ToList())[0];
        Assert.Equal("Seattle", bob.ShippingAddress.City);
    }

    [Fact]
    public async Task ExecuteUpdate_NullValue()
    {
        await this.SeedUsersAsync();

        var updated = await this.store.Query(ctx.User).Where(u => u.Name == "Alice").ExecuteUpdate(u => u.Email, null);

        Assert.Equal(1, updated);
        var alice = (await this.store.Query(ctx.User).Where(u => u.Name == "Alice").ToList())[0];
        Assert.Null(alice.Email);
    }

    // ── WhereIn / WhereNotIn ──────────────────────────────────────────

    [Fact]
    public async Task WhereIn_Strings()
    {
        await this.SeedUsersAsync();

        var results = await this.store.Query(ctx.User)
            .WhereIn(u => u.Name, ["Alice", "Charlie"])
            .ToList();

        Assert.Equal(2, results.Count);
        Assert.Contains(results, u => u.Name == "Alice");
        Assert.Contains(results, u => u.Name == "Charlie");
    }

    [Fact]
    public async Task WhereIn_Ints()
    {
        await this.SeedUsersAsync();

        var results = await this.store.Query(ctx.User)
            .WhereIn(u => u.Age, [25])
            .ToList();

        Assert.Equal(2, results.Count); // Alice, Charlie
        Assert.All(results, u => Assert.Equal(25, u.Age));
    }

    [Fact]
    public async Task WhereNotIn_Strings()
    {
        await this.SeedUsersAsync();

        var results = await this.store.Query(ctx.User)
            .WhereNotIn(u => u.Name, ["Bob"])
            .ToList();

        Assert.Equal(2, results.Count);
        Assert.DoesNotContain(results, u => u.Name == "Bob");
    }

    [Fact]
    public async Task WhereIn_EmptySet_MatchesNothing()
    {
        await this.SeedUsersAsync();

        var results = await this.store.Query(ctx.User)
            .WhereIn(u => u.Name, Array.Empty<string>())
            .ToList();

        Assert.Empty(results);
    }

    [Fact]
    public async Task WhereNotIn_EmptySet_MatchesEverything()
    {
        await this.SeedUsersAsync();

        var results = await this.store.Query(ctx.User)
            .WhereNotIn(u => u.Name, Array.Empty<string>())
            .ToList();

        Assert.Equal(3, results.Count);
    }

    [Fact]
    public async Task WhereIn_NullHandling_Ignore_SkipsNulls()
    {
        await this.SeedUsersAsync();

        // Only Alice has an email; null in the set is dropped under Ignore.
        var results = await this.store.Query(ctx.User)
            .WhereIn(u => u.Email, ["alice@test.com", null], NullHandling.Ignore)
            .ToList();

        Assert.Single(results);
        Assert.Equal("Alice", results[0].Name);
    }

    [Fact]
    public async Task WhereIn_NullHandling_Match_IncludesNullFields()
    {
        await this.SeedUsersAsync();

        // Under Match, the null in the set means "also match rows whose Email is null" (Bob, Charlie).
        var results = await this.store.Query(ctx.User)
            .WhereIn(u => u.Email, ["alice@test.com", null], NullHandling.Match)
            .ToList();

        Assert.Equal(3, results.Count);
    }

    [Fact]
    public async Task WhereNotIn_NullHandling_Match_ExcludesNullFields()
    {
        await this.SeedUsersAsync();

        // NOT IN with Match excludes null Email rows → only Alice remains.
        var results = await this.store.Query(ctx.User)
            .WhereNotIn(u => u.Email, [null], NullHandling.Match)
            .ToList();

        Assert.Single(results);
        Assert.Equal("Alice", results[0].Name);
    }

    [Fact]
    public async Task WhereIn_StringPath_Parity()
    {
        await this.SeedUsersAsync();

        var results = await this.store.Query(ctx.User)
            .WhereIn("Name", new[] { "Alice", "Charlie" })
            .ToList();

        Assert.Equal(2, results.Count);
    }

    [Fact]
    public async Task WhereIn_TypedValues_LongGuidEnum()
    {
        var g1 = Guid.NewGuid();
        var g2 = Guid.NewGuid();
        var g3 = Guid.NewGuid();
        await this.store.Insert(new TypedFields { Id = "a", Serial = 100, Ref = g1, Level = Priority.Low }, ctx.TypedFields);
        await this.store.Insert(new TypedFields { Id = "b", Serial = 200, Ref = g2, Level = Priority.High }, ctx.TypedFields);
        await this.store.Insert(new TypedFields { Id = "c", Serial = 300, Ref = g3, Level = Priority.High }, ctx.TypedFields);

        // long maps cleanly to a numeric JSON value on every provider.
        var byLong = await this.store.Query(ctx.TypedFields).WhereIn(x => x.Serial, [100L, 300L]).ToList();
        Assert.Equal(2, byLong.Count);
        Assert.DoesNotContain(byLong, x => x.Id == "b");

        // enum (default numeric JSON) — bound to its underlying value, so IN matches the stored number.
        var byEnum = await this.store.Query(ctx.TypedFields).WhereIn(x => x.Level, [Priority.High]).ToList();
        Assert.Equal(2, byEnum.Count);
        Assert.All(byEnum, x => Assert.Equal(Priority.High, x.Level));

        // Guid — bound as the STJ string form, so the field comparison is text-vs-text like any string.
        var byGuid = await this.store.Query(ctx.TypedFields).WhereIn(x => x.Ref, [g1, g3]).ToList();
        Assert.Equal(2, byGuid.Count);
        Assert.DoesNotContain(byGuid, x => x.Id == "b");

        // IN stays consistent with the equivalent == predicate for these scalar types.
        var eqEnum = await this.store.Query(ctx.TypedFields).Where(x => x.Level == Priority.High).ToList();
        Assert.Equal(eqEnum.Count, byEnum.Count);
    }
}
