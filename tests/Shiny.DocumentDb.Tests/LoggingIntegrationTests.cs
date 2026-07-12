using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Shiny.DocumentDb.LiteDb;
using Shiny.DocumentDb.Sqlite;
using Shiny.DocumentDb.Tests.Fixtures;
using Xunit;

namespace Shiny.DocumentDb.Tests;

// The store resolves an ILogger from the container (SP ctor) and fans SQL logging out to it, structured, under
// the "Shiny.DocumentDb" category — in addition to the raw options.Logging callback. Built with logging
// abstractions only (no concrete Microsoft.Extensions.Logging package needed).
public class LoggingIntegrationTests
{
    sealed record Entry(string Category, LogLevel Level, string Message);

    sealed class CapturingLoggerFactory : ILoggerFactory
    {
        public readonly List<Entry> Entries = new();
        public ILogger CreateLogger(string categoryName) => new CapturingLogger(categoryName, this.Entries);
        public void AddProvider(ILoggerProvider provider) { }
        public void Dispose() { }

        sealed class CapturingLogger(string category, List<Entry> sink) : ILogger
        {
            public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
            public bool IsEnabled(LogLevel logLevel) => true;
            public void Log<TState>(LogLevel level, EventId eventId, TState state, Exception? ex, Func<TState, Exception?, string> formatter)
            {
                lock (sink)
                    sink.Add(new Entry(category, level, formatter(state, ex)));
            }
        }
    }

    [Fact]
    public async Task Store_LogsSqlToILogger_UnderShinyCategory_AndPreservesCallback()
    {
        var factory = new CapturingLoggerFactory();
        var callbackSql = new List<string>();
        var services = new ServiceCollection();
        services.AddSingleton<ILoggerFactory>(factory);
        services.AddDocumentStore(o =>
        {
            o.DatabaseProvider = new SqliteDatabaseProvider("Data Source=:memory:");
            o.TableName = $"t{Guid.NewGuid():N}";
            o.Logging = callbackSql.Add;   // raw callback must still fire (composed, not replaced)
        });
        await using var sp = services.BuildServiceProvider();
        var store = sp.GetRequiredService<IDocumentStore>();

        await store.Insert(new User { Id = "u1", Name = "A" });

        List<Entry> entries;
        lock (factory.Entries) entries = factory.Entries.ToList();

        Assert.All(entries, e => Assert.Equal("Shiny.DocumentDb", e.Category));
        Assert.Contains(entries, e => e.Message.StartsWith("DocumentDb SQL:") && e.Level == LogLevel.Debug);
        Assert.Contains(entries, e => e.Message.Contains("store initialized"));   // lifecycle signal
        Assert.NotEmpty(callbackSql);                                             // callback preserved
    }

    // Minimal IServiceProvider that only supplies the ILoggerFactory — enough for a provider's SP constructor.
    sealed class LoggerOnlyServiceProvider(ILoggerFactory factory) : IServiceProvider
    {
        public object? GetService(Type serviceType) => serviceType == typeof(ILoggerFactory) ? factory : null;
    }

    [Fact]
    public async Task Provider_RoutesLogToILogger_UnderShinyCategory()
    {
        // LiteDB is embedded (in-memory-ish); the same DocumentProviderBase wiring covers Mongo/Cosmos/etc.
        var factory = new CapturingLoggerFactory();
        var path = Path.GetTempFileName();
        try
        {
            var store = new LiteDbDocumentStore(
                new LiteDbDocumentStoreOptions { ConnectionString = $"Filename={path};Connection=direct" },
                new LoggerOnlyServiceProvider(factory));

            await store.Insert(new User { Id = "u1", Name = "A" });

            List<Entry> entries;
            lock (factory.Entries) entries = factory.Entries.ToList();
            Assert.Contains(entries, e => e.Category == "Shiny.DocumentDb" && e.Message.Contains("LiteDB INSERT") && e.Level == LogLevel.Debug);
        }
        finally
        {
            try { File.Delete(path); } catch { /* best effort */ }
        }
    }

    [Fact]
    public async Task Store_WithoutLoggerFactory_UsesCallbackOnly_NoThrow()
    {
        // The container-free-shaped path: no ILoggerFactory registered → callback-only, unchanged behavior.
        var callbackSql = new List<string>();
        var services = new ServiceCollection();
        services.AddDocumentStore(o =>
        {
            o.DatabaseProvider = new SqliteDatabaseProvider("Data Source=:memory:");
            o.TableName = $"t{Guid.NewGuid():N}";
            o.Logging = callbackSql.Add;
        });
        await using var sp = services.BuildServiceProvider();
        var store = sp.GetRequiredService<IDocumentStore>();

        await store.Insert(new User { Id = "u1", Name = "A" });

        Assert.NotEmpty(callbackSql);
    }
}
