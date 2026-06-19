using Shiny.DocumentDb.LiteDb;
using Shiny.DocumentDb.Tests.Fixtures;
using Xunit;

namespace Shiny.DocumentDb.Tests;

// Proves the interceptor pipeline is wired in a document provider (LiteDB is embedded — no container
// needed). Mongo/Cosmos/IndexedDB share the identical wiring pattern.
public class LiteDbInterceptorTests : IDisposable
{
    readonly string path = Path.GetTempFileName();

    LiteDbDocumentStore CreateStore(Action<LiteDbDocumentStoreOptions> configure)
    {
        var opts = new LiteDbDocumentStoreOptions { ConnectionString = $"Filename={this.path};Connection=direct" };
        configure(opts);
        return new LiteDbDocumentStore(opts);
    }

    public void Dispose()
    {
        try { File.Delete(this.path); } catch { /* best effort */ }
    }

    sealed class Recorder : IDocumentInterceptor
    {
        public readonly List<string> Events = new();
        public Action<DocumentWriteContext>? OnBefore;
        public Task BeforeWrite(DocumentWriteContext ctx, CancellationToken ct)
        {
            this.Events.Add($"before:{ctx.Operation}");
            this.OnBefore?.Invoke(ctx);
            return Task.CompletedTask;
        }
        public Task AfterWrite(DocumentWriteContext ctx, CancellationToken ct)
        {
            this.Events.Add($"after:{ctx.Operation}");
            return Task.CompletedTask;
        }
    }

    sealed class BulkRecorder : IDocumentBulkInterceptor
    {
        public readonly List<string> Events = new();
        public Task BeforeBulkWrite(DocumentBulkContext ctx, CancellationToken ct) { this.Events.Add($"before:{ctx.Operation}"); return Task.CompletedTask; }
        public Task AfterBulkWrite(DocumentBulkContext ctx, CancellationToken ct) { this.Events.Add($"after:{ctx.Operation}:{ctx.AffectedCount}"); return Task.CompletedTask; }
    }

    [Fact]
    public async Task PerDoc_FiresForInsertUpdateRemove()
    {
        var rec = new Recorder();
        using var store = CreateStore(o => o.AddInterceptor(rec));

        await store.Insert(new User { Id = "u1", Name = "Alice", Age = 30 });
        await store.Update(new User { Id = "u1", Name = "Alice2", Age = 31 });
        await store.Remove<User>("u1");

        Assert.Equal(
            new[] { "before:Insert", "after:Insert", "before:Update", "after:Update", "before:Delete", "after:Delete" },
            rec.Events);
    }

    [Fact]
    public async Task BeforeWrite_MutationIsPersisted()
    {
        var rec = new Recorder { OnBefore = ctx => { if (ctx.Document is User u) u.Name = "Mutated"; } };
        using var store = CreateStore(o => o.AddInterceptor(rec));

        await store.Insert(new User { Id = "u1", Name = "Original", Age = 30 });

        Assert.Equal("Mutated", (await store.Get<User>("u1"))!.Name);
    }

    [Fact]
    public async Task UnitOfWork_FiresPerDocInterceptors()
    {
        var rec = new Recorder();
        using var store = CreateStore(o => o.AddInterceptor(rec));

        await store.CreateUnitOfWork()
            .Add(new User { Id = "u1", Name = "A" })
            .Add(new User { Id = "u2", Name = "B" })
            .Update(new User { Id = "u1", Name = "A2" })
            .SaveChanges();

        Assert.Equal(2, rec.Events.Count(e => e == "before:Insert"));
        Assert.Equal(2, rec.Events.Count(e => e == "after:Insert"));
        Assert.Contains("before:Update", rec.Events);
        Assert.Contains("after:Update", rec.Events);
    }

    [Fact]
    public async Task Bulk_FiresOncePerCall()
    {
        var bulk = new BulkRecorder();
        using var store = CreateStore(o => o.AddBulkInterceptor(bulk));

        await store.BatchInsert(new[] { new User { Id = "u1", Name = "A", Age = 30 }, new User { Id = "u2", Name = "B", Age = 40 } });
        var updated = await store.Query<User>().Where(u => u.Age > 20).ExecuteUpdate(u => u.Age, 99);
        var deleted = await store.Query<User>().Where(u => u.Age == 99).ExecuteDelete();
        await store.Clear<User>();

        Assert.Equal(2, updated);
        Assert.Equal(2, deleted);
        Assert.Contains("after:Update:2", bulk.Events);
        Assert.Contains("after:Delete:2", bulk.Events);
        Assert.Contains(bulk.Events, e => e.StartsWith("before:Clear"));
    }
}
