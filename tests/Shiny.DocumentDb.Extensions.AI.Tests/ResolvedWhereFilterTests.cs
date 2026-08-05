using System.Diagnostics;
using System.Linq.Expressions;
using System.Text.Json;
using Microsoft.Extensions.AI;
using Microsoft.Extensions.DependencyInjection;
using Shiny.DocumentDb.Extensions.AI.Tests.Fixtures;
using Shiny.DocumentDb.Sqlite;
using Xunit;

namespace Shiny.DocumentDb.Extensions.AI.Tests;

/// <summary>
/// The request-resolved (per-call) form of the non-removable <c>Where</c> scope: it is resolved from
/// <see cref="AIFunctionArguments.Services"/> on every invocation, AND-ed with the static form, enforced on
/// every capability, and — the part that matters — it fails <b>closed</b>. Each fail-closed test asserts at
/// the store (via the operation spans the store emits) that no unscoped query ran, not merely that the
/// response looked empty.
/// </summary>
public class ResolvedWhereFilterTests
{
    /// <summary>The per-request service a real app would resolve its scope from.</summary>
    public interface ICustomerScope
    {
        int MinAge { get; set; }
    }

    sealed class CustomerScope : ICustomerScope
    {
        public int MinAge { get; set; }
    }

    /// <summary>Counts the document operations the store actually performed during a call.</summary>
    sealed class StoreSpy : IDisposable
    {
        readonly ActivityListener listener;
        int operations;

        public int Operations => Volatile.Read(ref this.operations);

        public StoreSpy(string storeName)
        {
            this.listener = new ActivityListener
            {
                ShouldListenTo = src => src.Name == "Shiny.DocumentDb",   // DocumentStoreMetrics.Name (internal to core)
                Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
                ActivityStopped = a =>
                {
                    if (a.GetTagItem("db.namespace") as string == storeName)
                        Interlocked.Increment(ref this.operations);
                }
            };
            ActivitySource.AddActivityListener(this.listener);
        }

        public void Dispose() => this.listener.Dispose();
    }

    sealed class Fixture : IDisposable
    {
        public Fixture(Action<IDocumentAITypeBuilder<Customer>> configure, bool registerScopeService = true)
        {
            this.JsonContext = new TestJsonContext(new JsonSerializerOptions
            {
                PropertyNamingPolicy = JsonNamingPolicy.CamelCase
            });

            var services = new ServiceCollection();
            if (registerScopeService)
                services.AddScoped<ICustomerScope, CustomerScope>();

            services.AddDocumentStore(this.StoreName, o =>
            {
                o.DatabaseProvider = new SqliteDatabaseProvider("Data Source=:memory:");
                o.JsonSerializerOptions = this.JsonContext.Options;
            });
            services.AddDocumentStoreAITools(this.StoreName, b =>
                b.AddType(this.JsonContext.Customer, capabilities: DocumentAICapabilities.All, configure: configure));

            this.Services = services.BuildServiceProvider();
            this.Store = this.Services.GetRequiredKeyedService<IDocumentStore>(this.StoreName);
            this.Tools = this.Services.GetRequiredService<DocumentStoreAITools>();
        }

        public string StoreName { get; } = $"s{Guid.NewGuid():N}";
        public TestJsonContext JsonContext { get; }
        public ServiceProvider Services { get; }
        public IDocumentStore Store { get; }
        public DocumentStoreAITools Tools { get; }

        public AIFunction Tool(string name) => (AIFunction)this.Tools.Tools.First(t => ((AIFunction)t).Name == name);

        public Task SeedCustomers() => this.Store.BatchInsert(new[]
        {
            new Customer { Id = "cust-1", Name = "Alice", Age = 30, Email = "alice@test.com" },
            new Customer { Id = "cust-2", Name = "Bob", Age = 25, Email = "bob@test.com" },
            new Customer { Id = "cust-3", Name = "Carol", Age = 35, Email = "carol@test.com" }
        }, this.JsonContext.Customer);

        /// <summary>Invokes a tool inside a fresh DI scope, with the scope's services on the arguments.</summary>
        public async Task<object?> Invoke(string tool, Dictionary<string, object?> args, int minAge = 0)
        {
            using var scope = this.Services.CreateScope();
            scope.ServiceProvider.GetRequiredService<ICustomerScope>().MinAge = minAge;
            return await this.Tool(tool).InvokeAsync(new AIFunctionArguments(args)
            {
                Services = scope.ServiceProvider
            });
        }

        /// <summary>Invokes a tool with no services attached — the "null Services" fail-closed case.</summary>
        public Task<object?> InvokeWithoutServices(string tool, Dictionary<string, object?> args)
            => this.Tool(tool).InvokeAsync(new AIFunctionArguments(args)).AsTask();

        public void Dispose() => this.Services.Dispose();
    }

    static JsonDocument Parse(object? result) => JsonDocument.Parse(JsonSerializer.Serialize(result));

    static Fixture Resolved() => new(b => b
        .Where<ICustomerScope>((scope, _) => c => c.Age >= scope.MinAge));

    // ── The scope is resolved per call, from the call's scope ────────────

    [Fact]
    public async Task SameToolInstance_ReturnsDifferentRows_PerCallerScope()
    {
        using var fixture = Resolved();
        await fixture.SeedCustomers();

        using (var doc = Parse(await fixture.Invoke("customer_query", new(), minAge: 0)))
            Assert.Equal(3, doc.RootElement.GetProperty("count").GetInt32());

        using (var doc = Parse(await fixture.Invoke("customer_query", new(), minAge: 30)))
        {
            Assert.Equal(2, doc.RootElement.GetProperty("count").GetInt32());
            var names = doc.RootElement.GetProperty("documents").EnumerateArray()
                .Select(d => d.GetProperty("name").GetString())
                .ToList();
            Assert.DoesNotContain("Bob", names);
        }

        using (var doc = Parse(await fixture.Invoke("customer_query", new(), minAge: 35)))
            Assert.Equal(1, doc.RootElement.GetProperty("count").GetInt32());
    }

    [Fact]
    public async Task ResolvedScope_AndsWithTheStaticOne()
    {
        using var fixture = new Fixture(b => b
            .Where(c => c.Name != "Carol")                                    // static
            .Where<ICustomerScope>((scope, _) => c => c.Age >= scope.MinAge)); // resolved
        await fixture.SeedCustomers();

        // minAge 30 alone would keep Alice + Carol; the static filter removes Carol.
        using var doc = Parse(await fixture.Invoke("customer_query", new(), minAge: 30));
        Assert.Equal(1, doc.RootElement.GetProperty("count").GetInt32());
        Assert.Equal("Alice", doc.RootElement.GetProperty("documents")[0].GetProperty("name").GetString());
    }

    [Fact]
    public async Task AsyncOverload_IsAwaited()
    {
        using var fixture = new Fixture(b => b.Where<ICustomerScope>(async (scope, ctx) =>
        {
            await Task.Delay(5, ctx.CancellationToken);
            var min = scope.MinAge;
            return c => c.Age >= min;
        }));
        await fixture.SeedCustomers();

        using var doc = Parse(await fixture.Invoke("customer_query", new(), minAge: 35));
        Assert.Equal(1, doc.RootElement.GetProperty("count").GetInt32());
    }

    [Fact]
    public async Task RawContextOverload_ResolvesFromTheCallScope()
    {
        using var fixture = new Fixture(b => b.Where(ctx =>
        {
            var min = ctx.GetRequiredService<ICustomerScope>().MinAge;
            return c => c.Age >= min;
        }));
        await fixture.SeedCustomers();

        using var doc = Parse(await fixture.Invoke("customer_count", new(), minAge: 30));
        Assert.Equal(2, doc.RootElement.GetProperty("count").GetInt32());
    }

    // ── Enforced on every capability ────────────────────────────────────

    [Fact]
    public async Task HoldsOnCountAndAggregate()
    {
        using var fixture = Resolved();
        await fixture.SeedCustomers();

        using (var doc = Parse(await fixture.Invoke("customer_count", new(), minAge: 30)))
            Assert.Equal(2, doc.RootElement.GetProperty("count").GetInt32());

        using (var doc = Parse(await fixture.Invoke("customer_aggregate", new() { ["function"] = "count" }, minAge: 30)))
            Assert.Equal(2, doc.RootElement.GetProperty("value").GetInt32());
    }

    [Fact]
    public async Task GetById_OutOfScope_IsNotFound_NotForbidden()
    {
        using var fixture = Resolved();
        await fixture.SeedCustomers();

        using (var doc = Parse(await fixture.Invoke("customer_get_by_id", new() { ["id"] = "cust-2" }, minAge: 30)))
            Assert.False(doc.RootElement.GetProperty("found").GetBoolean());

        using (var doc = Parse(await fixture.Invoke("customer_get_by_id", new() { ["id"] = "cust-1" }, minAge: 30)))
            Assert.True(doc.RootElement.GetProperty("found").GetBoolean());
    }

    [Fact]
    public async Task Delete_OutOfScope_IsRefused()
    {
        using var fixture = Resolved();
        await fixture.SeedCustomers();

        using (var doc = Parse(await fixture.Invoke("customer_delete", new() { ["id"] = "cust-2" }, minAge: 30)))
            Assert.False(doc.RootElement.GetProperty("deleted").GetBoolean());

        Assert.NotNull(await fixture.Store.Get<Customer>("cust-2", fixture.JsonContext.Customer));
    }

    [Fact]
    public async Task Update_OutOfScope_IsRefused()
    {
        using var fixture = Resolved();
        await fixture.SeedCustomers();

        var document = JsonSerializer.SerializeToElement(
            new Customer { Id = "cust-2", Name = "Hijacked", Age = 99, Email = "x@test.com" },
            fixture.JsonContext.Customer);

        // Age 99 satisfies the scope, but the STORED record (age 25) does not — the model may not overwrite
        // a document it cannot see.
        using var doc = Parse(await fixture.Invoke("customer_update", new() { ["document"] = document }, minAge: 30));
        Assert.False(doc.RootElement.GetProperty("updated").GetBoolean());

        var stored = await fixture.Store.Get<Customer>("cust-2", fixture.JsonContext.Customer);
        Assert.Equal("Bob", stored!.Name);
    }

    [Fact]
    public async Task Insert_OutOfScope_IsRejected()
    {
        using var fixture = Resolved();

        var document = JsonSerializer.SerializeToElement(
            new Customer { Id = "cust-9", Name = "Too young", Age = 10 },
            fixture.JsonContext.Customer);

        await Assert.ThrowsAnyAsync<Exception>(
            () => fixture.Invoke("customer_insert", new() { ["document"] = document }, minAge: 30));

        Assert.Null(await fixture.Store.Get<Customer>("cust-9", fixture.JsonContext.Customer));
    }

    // ── Fail closed ─────────────────────────────────────────────────────

    [Fact]
    public async Task AThrowingFilter_FailsTheCall_AndNoQueryReachesTheStore()
    {
        using var fixture = new Fixture(b => b.Where<ICustomerScope>(
            // A throw-only lambda has no return type for the compiler to pick an overload with, so the
            // delegate type is named explicitly. Real filters return a predicate and bind on their own.
            (Func<ICustomerScope, DocumentAIFilterContext, Expression<Func<Customer, bool>>>)(
                (_, _) => throw new InvalidOperationException("permission service down"))));
        await fixture.SeedCustomers();

        using var spy = new StoreSpy(fixture.StoreName);
        await Assert.ThrowsAnyAsync<Exception>(() => fixture.Invoke("customer_query", new()));
        Assert.Equal(0, spy.Operations);
    }

    [Fact]
    public async Task AnUnresolvableService_FailsTheCall_AndNoQueryReachesTheStore()
    {
        // Registration validation is skipped here (registerScopeService: false uses the raw context form so
        // the builder records no required service) — this is the runtime half of fail-closed.
        using var fixture = new Fixture(
            b => b.Where(ctx =>
            {
                var min = ctx.GetRequiredService<ICustomerScope>().MinAge;
                return c => c.Age >= min;
            }),
            registerScopeService: false);
        await fixture.SeedCustomers();

        using var spy = new StoreSpy(fixture.StoreName);
        await Assert.ThrowsAnyAsync<Exception>(() => fixture.Invoke("customer_query", new()));
        Assert.Equal(0, spy.Operations);
    }

    [Fact]
    public async Task NullServices_FailsTheCall_AndNoQueryReachesTheStore()
    {
        using var fixture = Resolved();
        await fixture.SeedCustomers();

        using var spy = new StoreSpy(fixture.StoreName);
        await Assert.ThrowsAnyAsync<Exception>(() => fixture.InvokeWithoutServices("customer_query", new()));
        Assert.Equal(0, spy.Operations);
    }

    [Fact]
    public async Task ANullPredicate_FailsTheCall()
    {
        using var fixture = new Fixture(b => b.Where(_ => null!));
        await fixture.SeedCustomers();

        using var spy = new StoreSpy(fixture.StoreName);
        await Assert.ThrowsAnyAsync<Exception>(() => fixture.Invoke("customer_query", new()));
        Assert.Equal(0, spy.Operations);
    }

    [Fact]
    public async Task DenyAll_ReturnsNothing_RatherThanEverything()
    {
        using var fixture = new Fixture(b => b.Where(_ => (c => false)));
        await fixture.SeedCustomers();

        using var doc = Parse(await fixture.Invoke("customer_query", new()));
        Assert.Equal(0, doc.RootElement.GetProperty("count").GetInt32());
    }

    // ── Still invisible to the model ────────────────────────────────────

    [Fact]
    public async Task TheResolvedScope_LeaksIntoNoSchema_Description_OrError()
    {
        using var fixture = new Fixture(b => b
            .Where<ICustomerScope>((Func<ICustomerScope, DocumentAIFilterContext, Expression<Func<Customer, bool>>>)(
                (_, _) => throw new InvalidOperationException("scope service exploded"))));

        var tool = fixture.Tool("customer_query");
        var schema = tool.JsonSchema.GetRawText();
        Assert.DoesNotContain("ICustomerScope", schema, StringComparison.Ordinal);
        Assert.DoesNotContain("MinAge", schema, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("ICustomerScope", tool.Description, StringComparison.Ordinal);
        Assert.DoesNotContain("Age >=", tool.Description, StringComparison.Ordinal);

        // The failure is a failure — it does not describe the scope back to the model. (Messages only: the
        // stack trace names this test's own parameters, which the model never sees.)
        var ex = await Record.ExceptionAsync(() => fixture.Invoke("customer_query", new()));
        Assert.NotNull(ex);
        for (var e = ex; e != null; e = e.InnerException)
        {
            Assert.DoesNotContain("MinAge", e.Message, StringComparison.OrdinalIgnoreCase);
            Assert.DoesNotContain("Age >=", e.Message, StringComparison.Ordinal);
        }
    }

    // ── Cost when unused ────────────────────────────────────────────────

    [Fact]
    public async Task WithoutResolvedFilters_ServicesIsNeverTouched()
    {
        var probe = new ThrowingServiceProvider();
        using var fixture = new Fixture(b => b.Where(c => c.Age >= 30));
        await fixture.SeedCustomers();

        // Services is present but poisoned: a static-only registration must never dereference it.
        var result = await fixture.Tool("customer_query").InvokeAsync(new AIFunctionArguments
        {
            Services = probe
        });

        using var doc = Parse(result);
        Assert.Equal(2, doc.RootElement.GetProperty("count").GetInt32());
        Assert.False(probe.WasTouched);
    }

    sealed class ThrowingServiceProvider : IServiceProvider
    {
        public bool WasTouched { get; private set; }

        public object? GetService(Type serviceType)
        {
            this.WasTouched = true;
            throw new InvalidOperationException("A static-only registration must not resolve services.");
        }
    }

    // ── Startup validation ──────────────────────────────────────────────

    [Fact]
    public void AnUnregisteredScopeService_FailsAtStartup()
    {
        var jsonContext = new TestJsonContext(new JsonSerializerOptions { PropertyNamingPolicy = JsonNamingPolicy.CamelCase });
        var services = new ServiceCollection();
        services.AddDocumentStore(o => o.DatabaseProvider = new SqliteDatabaseProvider("Data Source=:memory:"));

        var ex = Assert.Throws<InvalidOperationException>(() =>
            services.AddDocumentStoreAITools(b => b.AddType(
                jsonContext.Customer,
                configure: t => t.Where<ICustomerScope>((scope, _) => c => c.Age >= scope.MinAge))));

        Assert.Contains(nameof(ICustomerScope), ex.Message, StringComparison.Ordinal);
    }
}
