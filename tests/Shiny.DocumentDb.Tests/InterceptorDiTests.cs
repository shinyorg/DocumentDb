using Microsoft.Extensions.DependencyInjection;
using Shiny.DocumentDb.Sqlite;
using Shiny.DocumentDb.Tests.Fixtures;
using Xunit;

namespace Shiny.DocumentDb.Tests;

public class InterceptorDiTests
{
    static ServiceProvider BuildProvider(Action<IServiceCollection> configure)
    {
        var services = new ServiceCollection();
        services.AddDocumentStore(o =>
        {
            o.DatabaseProvider = new SqliteDatabaseProvider("Data Source=:memory:");
            o.TableName = $"t{Guid.NewGuid():N}";
        });
        configure(services);
        return services.BuildServiceProvider();
    }

    sealed class Recorder : IDocumentInterceptor
    {
        readonly List<string> sink;
        readonly string tag;
        public Recorder(List<string> sink, string tag) { this.sink = sink; this.tag = tag; }

        public Task BeforeWrite(DocumentWriteContext ctx, CancellationToken ct)
        {
            this.sink.Add($"{this.tag}:before:{ctx.Operation}");
            return Task.CompletedTask;
        }

        public Task AfterWrite(DocumentWriteContext ctx, CancellationToken ct)
        {
            this.sink.Add($"{this.tag}:after:{ctx.Operation}");
            return Task.CompletedTask;
        }
    }

    // A DI-registered interceptor that takes a constructor-injected dependency — proving DI resolution works.
    sealed class DependencyInterceptor : IDocumentInterceptor
    {
        readonly EventLog log;
        public DependencyInterceptor(EventLog log) => this.log = log;

        public Task BeforeWrite(DocumentWriteContext ctx, CancellationToken ct)
        {
            this.log.Events.Add($"di:before:{ctx.Operation}");
            return Task.CompletedTask;
        }

        public Task AfterWrite(DocumentWriteContext ctx, CancellationToken ct) => Task.CompletedTask;
    }

    sealed class EventLog { public readonly List<string> Events = new(); }

    sealed class BulkRecorder : IDocumentBulkInterceptor
    {
        readonly List<string> sink;
        public BulkRecorder(List<string> sink) => this.sink = sink;
        public Task BeforeBulkWrite(DocumentBulkContext ctx, CancellationToken ct) { this.sink.Add($"before:{ctx.Operation}"); return Task.CompletedTask; }
        public Task AfterBulkWrite(DocumentBulkContext ctx, CancellationToken ct) { this.sink.Add($"after:{ctx.Operation}:{ctx.AffectedCount}"); return Task.CompletedTask; }
    }

    [Fact]
    public async Task DiRegisteredInterceptor_FiresWithInjectedDependency()
    {
        var log = new EventLog();
        var sp = BuildProvider(s =>
        {
            s.AddSingleton(log);
            s.AddSingleton<IDocumentInterceptor, DependencyInterceptor>();
        });

        var store = sp.GetRequiredService<IDocumentStore>();
        await store.Insert(new User { Id = "u1", Name = "Alice" });

        Assert.Contains("di:before:Insert", log.Events);
    }

    [Fact]
    public async Task DiRegisteredBulkInterceptor_Fires()
    {
        var events = new List<string>();
        var sp = BuildProvider(s => s.AddSingleton<IDocumentBulkInterceptor>(new BulkRecorder(events)));

        var store = sp.GetRequiredService<IDocumentStore>();
        await store.Insert(new User { Id = "u1", Name = "A", Age = 30 });
        var deleted = await store.Query<User>().Where(u => u.Age == 30).ExecuteDelete();

        Assert.Equal(1, deleted);
        Assert.Contains("before:Delete", events);
        Assert.Contains("after:Delete:1", events);
    }

    [Fact]
    public async Task OptionsRegistered_RunBeforeDiRegistered()
    {
        var order = new List<string>();
        var services = new ServiceCollection();
        services.AddDocumentStore(o =>
        {
            o.DatabaseProvider = new SqliteDatabaseProvider("Data Source=:memory:");
            o.TableName = $"t{Guid.NewGuid():N}";
            o.AddInterceptor(new Recorder(order, "options"));
        });
        services.AddSingleton<IDocumentInterceptor>(new Recorder(order, "di"));
        using var sp = services.BuildServiceProvider();

        var store = sp.GetRequiredService<IDocumentStore>();
        await store.Insert(new User { Id = "u1", Name = "Alice" });

        Assert.Equal(
            new[] { "options:before:Insert", "di:before:Insert", "options:after:Insert", "di:after:Insert" },
            order);
    }

    [Fact]
    public async Task NoInterceptorsRegistered_WritesStillWork()
    {
        using var sp = BuildProvider(_ => { });
        var store = sp.GetRequiredService<IDocumentStore>();

        await store.Insert(new User { Id = "u1", Name = "Alice" });

        Assert.Equal("Alice", (await store.Get<User>("u1"))!.Name);
    }

    // DEFERRED-BUGS #5: IDocumentStore is a singleton and captures its interceptors once from the root provider,
    // so a Scoped interceptor is a captive-dependency bug. Resolving the store fails fast with a clear error.
    static IServiceProvider ProviderWithStore(Action<IServiceCollection> configure)
    {
        var services = new ServiceCollection();
        configure(services);
        services.AddDocumentStore(o =>
        {
            o.DatabaseProvider = new SqliteDatabaseProvider("Data Source=:memory:");
            o.TableName = $"t{Guid.NewGuid():N}";
        });
        return services.BuildServiceProvider();
    }

    [Fact]
    public void ScopedInterceptor_RegisteredBefore_Throws()
    {
        var log = new List<string>();
        using var sp = (ServiceProvider)ProviderWithStore(s => s.AddScoped<IDocumentInterceptor>(_ => new Recorder(log, "scoped")));

        var ex = Assert.Throws<InvalidOperationException>(() => sp.GetRequiredService<IDocumentStore>());
        Assert.Contains("Scoped", ex.Message);
    }

    // The order-independent case: the scoped interceptor is registered AFTER AddDocumentStore. The guard runs in
    // the store factory (at first resolve), so it still catches it.
    [Fact]
    public void ScopedInterceptor_RegisteredAfter_Throws()
    {
        var services = new ServiceCollection();
        var log = new List<string>();
        services.AddDocumentStore(o =>
        {
            o.DatabaseProvider = new SqliteDatabaseProvider("Data Source=:memory:");
            o.TableName = $"t{Guid.NewGuid():N}";
        });
        services.AddScoped<IDocumentInterceptor>(_ => new Recorder(log, "scoped"));

        using var sp = services.BuildServiceProvider();
        var ex = Assert.Throws<InvalidOperationException>(() => sp.GetRequiredService<IDocumentStore>());
        Assert.Contains("Scoped", ex.Message);
    }

    [Fact]
    public void SingletonInterceptorRegistration_Allowed()
    {
        var log = new List<string>();
        using var sp = (ServiceProvider)ProviderWithStore(s => s.AddSingleton<IDocumentInterceptor>(_ => new Recorder(log, "singleton")));

        // Resolving the store must not throw.
        var store = sp.GetRequiredService<IDocumentStore>();
        Assert.NotNull(store);
    }
}
