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
}
