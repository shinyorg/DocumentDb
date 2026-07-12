using Microsoft.Extensions.DependencyInjection;
using Shiny.DocumentDb.LiteDb;
using Shiny.DocumentDb.Tests.Fixtures;
using Xunit;

namespace Shiny.DocumentDb.Tests;

// Phase 3 of the DI-scoped-interceptors feature: the non-relational providers gained an IServiceProvider
// constructor path (AttachServiceProvider), so DI-registered interceptors now fire on them — previously they
// had no container entry point and silently never ran. LiteDB is embedded (in-memory), so it is the runnable
// proof; Mongo/Cosmos/DynamoDB/AzureTable/IndexedDB share the identical DocumentProviderBase wiring.
public class ProviderInterceptorParityTests : IDisposable
{
    readonly string path = Path.GetTempFileName();

    public void Dispose()
    {
        try { File.Delete(this.path); } catch { /* best effort */ }
    }

    sealed class DiRecorder : IDocumentInterceptor
    {
        public readonly List<string> Events = new();
        public Task BeforeWrite(DocumentWriteContext ctx, CancellationToken ct) { this.Events.Add($"before:{ctx.Operation}"); return Task.CompletedTask; }
        public Task AfterWrite(DocumentWriteContext ctx, CancellationToken ct) { this.Events.Add($"after:{ctx.Operation}"); return Task.CompletedTask; }
    }

    [Fact]
    public async Task LiteDb_SpConstructor_FiresDiRegisteredInterceptor()
    {
        var recorder = new DiRecorder();
        var services = new ServiceCollection();
        services.AddSingleton<IDocumentInterceptor>(recorder);
        services.AddSingleton<IDocumentStore>(sp => new LiteDbDocumentStore(
            new LiteDbDocumentStoreOptions { ConnectionString = $"Filename={this.path};Connection=direct" }, sp));

        await using var sp = services.BuildServiceProvider();
        var store = sp.GetRequiredService<IDocumentStore>();

        await store.Insert(new User { Id = "u1", Name = "A" });

        // Before this feature the DI-registered interceptor never fired on a non-relational provider.
        Assert.Contains("before:Insert", recorder.Events);
        Assert.Contains("after:Insert", recorder.Events);
    }

    [Fact]
    public void LiteDb_OptionsOnlyConstructor_StillWorks_NoContainer()
    {
        // The container-free path is unchanged: no SP, no DI interceptors, still constructs fine.
        using var store = new LiteDbDocumentStore(
            new LiteDbDocumentStoreOptions { ConnectionString = $"Filename={this.path};Connection=direct" });
        Assert.NotNull(store);
    }
}
