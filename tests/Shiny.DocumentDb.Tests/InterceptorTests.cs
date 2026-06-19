using Shiny.DocumentDb.Sqlite;
using Shiny.DocumentDb.Tests.Fixtures;
using Xunit;

namespace Shiny.DocumentDb.Tests;

public class InterceptorTests
{
    static DocumentStore CreateStore(Action<DocumentStoreOptions> configure)
    {
        var opts = new DocumentStoreOptions
        {
            DatabaseProvider = new SqliteDatabaseProvider("Data Source=:memory:"),
            TableName = $"t{Guid.NewGuid():N}"
        };
        configure(opts);
        return new DocumentStore(opts);
    }

    sealed class Recorder : IDocumentInterceptor
    {
        public readonly List<string> Events = new();
        public readonly List<DocumentWriteContext> Before = new();
        public readonly List<DocumentWriteContext> After = new();
        public Action<DocumentWriteContext>? OnBefore;

        public Task BeforeWrite(DocumentWriteContext ctx, CancellationToken ct)
        {
            this.Events.Add($"before:{ctx.Operation}");
            this.Before.Add(ctx);
            this.OnBefore?.Invoke(ctx);
            return Task.CompletedTask;
        }

        public Task AfterWrite(DocumentWriteContext ctx, CancellationToken ct)
        {
            this.Events.Add($"after:{ctx.Operation}");
            this.After.Add(ctx);
            return Task.CompletedTask;
        }
    }

    sealed class BulkRecorder : IDocumentBulkInterceptor
    {
        public readonly List<string> Events = new();
        public readonly List<DocumentBulkContext> After = new();

        public Task BeforeBulkWrite(DocumentBulkContext ctx, CancellationToken ct)
        {
            this.Events.Add($"before:{ctx.Operation}");
            return Task.CompletedTask;
        }

        public Task AfterBulkWrite(DocumentBulkContext ctx, CancellationToken ct)
        {
            this.Events.Add($"after:{ctx.Operation}:{ctx.AffectedCount}");
            this.After.Add(ctx);
            return Task.CompletedTask;
        }
    }

    [Fact]
    public async Task PerDoc_FiresForInsertUpdateUpsertRemove()
    {
        var rec = new Recorder();
        using var store = CreateStore(o => o.AddInterceptor(rec));

        await store.Insert(new User { Id = "u1", Name = "Alice", Age = 30 });
        await store.Update(new User { Id = "u1", Name = "Alice2", Age = 31 });
        await store.Upsert(new User { Id = "u1", Age = 32 });
        await store.Remove<User>("u1");

        Assert.Equal(
            new[] { "before:Insert", "after:Insert", "before:Update", "after:Update", "before:Upsert", "after:Upsert", "before:Delete", "after:Delete" },
            rec.Events);
    }

    [Fact]
    public async Task BeforeWrite_MutationIsPersisted()
    {
        var rec = new Recorder { OnBefore = ctx => { if (ctx.Document is User u) u.Name = "Mutated"; } };
        using var store = CreateStore(o => o.AddInterceptor(rec));

        await store.Insert(new User { Id = "u1", Name = "Original", Age = 30 });

        var stored = await store.Get<User>("u1");
        Assert.Equal("Mutated", stored!.Name);
    }

    [Fact]
    public async Task BeforeWrite_Throw_AbortsWrite()
    {
        var store = CreateStore(o => o.OnBeforeWrite<User>((ctx, ct) => throw new InvalidOperationException("nope")));

        await Assert.ThrowsAsync<InvalidOperationException>(() => store.Insert(new User { Id = "u1", Name = "Alice" }));
        Assert.Equal(0, await store.Count<User>());
    }

    [Fact]
    public async Task AfterWrite_SeesGeneratedId()
    {
        var rec = new Recorder();
        using var store = CreateStore(o => o.AddInterceptor(rec));

        var model = new GuidIdModel { Name = "Allan" };
        await store.Insert(model);

        var after = rec.After.Single();
        Assert.NotEqual(Guid.Empty, model.Id);
        // ctx.Id carries the store's resolved (string) id form of the generated id.
        Assert.Equal(model.Id.ToString("N"), after.Id);
    }

    [Fact]
    public async Task AfterWrite_DoesNotFireWhenWriteThrows()
    {
        var rec = new Recorder();
        using var store = CreateStore(o => o.AddInterceptor(rec));

        await store.Insert(new User { Id = "dup", Name = "First" });
        rec.Events.Clear();

        await Assert.ThrowsAsync<InvalidOperationException>(() => store.Insert(new User { Id = "dup", Name = "Second" }));

        Assert.Contains("before:Insert", rec.Events);
        Assert.DoesNotContain("after:Insert", rec.Events);
    }

    [Fact]
    public async Task BatchInsert_FiresPerItem()
    {
        var rec = new Recorder();
        using var store = CreateStore(o => o.AddInterceptor(rec));

        await store.BatchInsert(new[]
        {
            new User { Id = "u1", Name = "A" },
            new User { Id = "u2", Name = "B" },
            new User { Id = "u3", Name = "C" }
        });

        Assert.Equal(3, rec.Before.Count(c => c.Operation == DocumentOperation.Insert));
        Assert.Equal(3, rec.After.Count(c => c.Operation == DocumentOperation.Insert));
    }

    [Fact]
    public async Task MultipleInterceptors_RunInRegistrationOrder()
    {
        var order = new List<string>();
        var a = new Recorder { OnBefore = _ => order.Add("a") };
        var b = new Recorder { OnBefore = _ => order.Add("b") };
        using var store = CreateStore(o => o.AddInterceptor(a).AddInterceptor(b));

        await store.Insert(new User { Id = "u1", Name = "Alice" });

        Assert.Equal(new[] { "a", "b" }, order);
    }

    [Fact]
    public async Task Bulk_FiresOncePerCall_AndPerDocDoesNot()
    {
        var perDoc = new Recorder();
        var bulk = new BulkRecorder();
        using var store = CreateStore(o => o.AddInterceptor(perDoc).AddBulkInterceptor(bulk));

        await store.BatchInsert(new[] { new User { Id = "u1", Name = "A", Age = 30 }, new User { Id = "u2", Name = "B", Age = 40 } });
        perDoc.Events.Clear();

        var updated = await store.Query<User>().Where(u => u.Age > 20).ExecuteUpdate(u => u.Age, 99);
        var deleted = await store.Query<User>().Where(u => u.Age == 99).ExecuteDelete();
        await store.Clear<User>();

        Assert.Equal(2, updated);
        Assert.Equal(2, deleted);
        Assert.Contains("before:Update", bulk.Events);
        Assert.Contains("after:Update:2", bulk.Events);
        Assert.Contains("before:Delete", bulk.Events);
        Assert.Contains("after:Delete:2", bulk.Events);
        Assert.Contains(bulk.Events, e => e.StartsWith("before:Clear"));
        Assert.Empty(perDoc.Events); // per-doc interceptors never fire for set-based ops
    }

    [Fact]
    public async Task ExecuteUpdate_PopulatesAssignment()
    {
        var bulk = new BulkRecorder();
        using var store = CreateStore(o => o.AddBulkInterceptor(bulk));

        await store.Insert(new User { Id = "u1", Name = "A", Age = 30 });
        await store.Query<User>().Where(u => u.Age == 30).ExecuteUpdate(u => u.Age, 31);

        var ctx = bulk.After.Single(c => c.Operation == DocumentOperation.Update);
        Assert.NotNull(ctx.Assignment);
        Assert.Equal(31, ctx.Assignment!.Value.Value);
        Assert.NotNull(ctx.WhereClause);
    }

    [Fact]
    public async Task UnitOfWork_FiresPerDocInterceptors()
    {
        var rec = new Recorder();
        using var store = CreateStore(o => o.AddInterceptor(rec));

        await store.CreateUnitOfWork()
            .Add(new User { Id = "u1", Name = "A" })
            .Update(new User { Id = "u1", Name = "B" })
            .SaveChanges();

        Assert.Contains("before:Insert", rec.Events);
        Assert.Contains("after:Insert", rec.Events);
        Assert.Contains("before:Update", rec.Events);
        Assert.Contains("after:Update", rec.Events);
    }

    [Fact]
    public async Task DirectWrite_ReportsSourceDirect()
    {
        var rec = new Recorder();
        using var store = CreateStore(o => o.AddInterceptor(rec));

        await store.Insert(new User { Id = "u1", Name = "Alice" });

        Assert.All(rec.Before, c => Assert.Equal(DocumentOperationSource.Direct, c.Source));
    }

    [Fact]
    public async Task Restore_ReportsSourceTemporal()
    {
        var rec = new Recorder();
        using var store = CreateStore(o =>
        {
            o.MapTemporal<VersionedUser>();
            o.AddInterceptor(rec);
        });

        await store.Insert(new VersionedUser { Id = "u1", Name = "V1", Age = 30 });
        await store.Update(new VersionedUser { Id = "u1", Name = "V2", Age = 31 });
        rec.Events.Clear();
        rec.Before.Clear();

        await store.Restore<VersionedUser>("u1", 1);

        Assert.NotEmpty(rec.Before);
        Assert.All(rec.Before, c => Assert.Equal(DocumentOperationSource.Temporal, c.Source));
    }
}
