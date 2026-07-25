using Shiny.DocumentDb.Tests.Fixtures;
using Xunit;

namespace Shiny.DocumentDb.Tests;

/// <summary>
/// Soft delete and interceptor cancellation, asserted on every provider — both ride the shared interceptor
/// plumbing, so this is where a provider that silently skips the cancel guard gets caught.
/// </summary>
public abstract class SoftDeleteConformanceTestsBase(IDocumentStoreFixture fixture)
{
    protected readonly IDocumentStoreFixture Fixture = fixture;

    // A per-suite document type: the soft-delete mapping registry is keyed by document type process-wide, so
    // every provider suite must map the same property (they do — they all use this one type).
    public class SoftDoc
    {
        public string Id { get; set; } = "";
        public string Name { get; set; } = "";
        public int Age { get; set; }
        public bool IsDeleted { get; set; }
    }

    IDocumentStore CreateStore()
        => this.Fixture.CreateStore($"t{Guid.NewGuid():N}", o => o.AddSoftDelete<SoftDoc>(x => x.IsDeleted));

    async Task<IDocumentStore> Seeded()
    {
        var store = this.CreateStore();
        await store.Insert(new SoftDoc { Id = "s1", Name = "Alice", Age = 30 });
        await store.Insert(new SoftDoc { Id = "s2", Name = "Bob", Age = 40 });
        await store.Insert(new SoftDoc { Id = "s3", Name = "Carol", Age = 50 });
        return store;
    }

    [Fact]
    public async Task Remove_SetsTheFlag_AndHidesTheDocument()
    {
        var store = await this.Seeded();
        using var _ = (IDisposable)store;

        Assert.True(await store.Remove<SoftDoc>("s1"));

        Assert.Null(await store.Get<SoftDoc>("s1"));
        Assert.Equal(2, await store.Query<SoftDoc>().Count());

        var all = await store.Query<SoftDoc>().IncludeDeleted().ToList();
        Assert.Equal(3, all.Count);
        var flagged = all.Single(x => x.Id == "s1");
        Assert.True(flagged.IsDeleted);
        Assert.Equal("Alice", flagged.Name);   // body untouched
    }

    [Fact]
    public async Task Remove_Twice_ReportsFalseTheSecondTime()
    {
        var store = await this.Seeded();
        using var _ = (IDisposable)store;

        Assert.True(await store.Remove<SoftDoc>("s1"));
        Assert.False(await store.Remove<SoftDoc>("s1"));
    }

    [Fact]
    public async Task ExecuteDelete_SoftDeletesTheMatchedSet()
    {
        var store = await this.Seeded();
        using var _ = (IDisposable)store;

        var affected = await store.Query<SoftDoc>().Where(x => x.Age >= 40).ExecuteDelete();

        Assert.Equal(2, affected);
        Assert.Equal(1, await store.Query<SoftDoc>().Count());
        Assert.Equal(3, await store.Query<SoftDoc>().IncludeDeleted().Count());
    }

    [Fact]
    public async Task Clear_SoftDeletesEverything()
    {
        var store = await this.Seeded();
        using var _ = (IDisposable)store;

        var affected = await store.Clear<SoftDoc>();

        Assert.Equal(3, affected);
        Assert.Equal(0, await store.Query<SoftDoc>().Count());
        Assert.Equal(3, await store.Query<SoftDoc>().IncludeDeleted().Count());
    }

    [Fact]
    public async Task OnlyDeleted_Restore_And_Purge()
    {
        var store = await this.Seeded();
        using var _ = (IDisposable)store;

        await store.Remove<SoftDoc>("s1");
        await store.Remove<SoftDoc>("s2");

        var deleted = await store.Query<SoftDoc>().OnlyDeleted().ToList();
        Assert.Equal(2, deleted.Count);

        Assert.Equal(1, await store.Restore<SoftDoc>(x => x.Id == "s1"));
        Assert.NotNull(await store.Get<SoftDoc>("s1"));

        Assert.Equal(1, await store.PurgeDeleted<SoftDoc>());
        Assert.Equal(2, await store.Query<SoftDoc>().IncludeDeleted().Count());
    }

    [Fact]
    public async Task HardDelete_BypassesSoftDelete()
    {
        var store = await this.Seeded();
        using var _ = (IDisposable)store;

        Assert.True(await store.HardDelete<SoftDoc>("s1"));

        Assert.Equal(2, await store.Query<SoftDoc>().IncludeDeleted().Count());
    }

    // ── Interceptor cancellation (the mechanism soft delete rides on) ────

    sealed class CancelDeletes : IDocumentInterceptor
    {
        public bool Result { get; init; } = true;
        public readonly List<string> Events = new();

        public Task BeforeWrite(DocumentWriteContext ctx, CancellationToken ct)
        {
            this.Events.Add($"before:{ctx.Operation}");
            if (ctx.Operation == DocumentOperation.Delete)
                ctx.Cancel(this.Result);
            return Task.CompletedTask;
        }

        public Task AfterWrite(DocumentWriteContext ctx, CancellationToken ct)
        {
            this.Events.Add($"after:{ctx.Operation}");
            return Task.CompletedTask;
        }
    }

    sealed class CancelBulk : IDocumentBulkInterceptor
    {
        public int Affected { get; init; } = 9;
        public bool SawSourceQuery;

        public Task BeforeBulkWrite(DocumentBulkContext ctx, CancellationToken ct)
        {
            if (ctx.Operation == DocumentOperation.Delete)
                this.SawSourceQuery = ctx.QueryAs<User>() != null;
            ctx.Cancel(this.Affected);
            return Task.CompletedTask;
        }

        public Task AfterBulkWrite(DocumentBulkContext ctx, CancellationToken ct) => Task.CompletedTask;
    }

    [Fact]
    public async Task Cancel_Remove_SkipsTheDelete_AndSkipsAfterWrite()
    {
        var interceptor = new CancelDeletes();
        using var owner = (IDisposable)this.Fixture.CreateStore(
            $"t{Guid.NewGuid():N}", o => o.AddInterceptor(interceptor));
        var store = (IDocumentStore)owner;

        await store.Insert(new User { Id = "u1", Name = "Alice", Age = 30 });
        var removed = await store.Remove<User>("u1");

        Assert.True(removed);
        Assert.NotNull(await store.Get<User>("u1"));
        Assert.Equal(new[] { "before:Insert", "after:Insert", "before:Delete" }, interceptor.Events);
    }

    [Fact]
    public async Task Cancel_Remove_CanReportFalse()
    {
        using var owner = (IDisposable)this.Fixture.CreateStore(
            $"t{Guid.NewGuid():N}", o => o.AddInterceptor(new CancelDeletes { Result = false }));
        var store = (IDocumentStore)owner;

        await store.Insert(new User { Id = "u1", Name = "Alice", Age = 30 });

        Assert.False(await store.Remove<User>("u1"));
        Assert.NotNull(await store.Get<User>("u1"));
    }

    [Fact]
    public async Task Cancel_ExecuteDelete_SkipsTheWrite_AndExposesTheSourceQuery()
    {
        var interceptor = new CancelBulk();
        using var owner = (IDisposable)this.Fixture.CreateStore(
            $"t{Guid.NewGuid():N}", o => o.AddBulkInterceptor(interceptor));
        var store = (IDocumentStore)owner;

        await store.Insert(new User { Id = "u1", Name = "Alice", Age = 30 });
        await store.Insert(new User { Id = "u2", Name = "Bob", Age = 40 });

        var affected = await store.Query<User>().Where(x => x.Age >= 40).ExecuteDelete();

        Assert.Equal(9, affected);
        Assert.Equal(2, await store.Query<User>().Count());
        Assert.True(interceptor.SawSourceQuery);
    }

    [Fact]
    public async Task Cancel_Clear_SkipsTheWrite()
    {
        using var owner = (IDisposable)this.Fixture.CreateStore(
            $"t{Guid.NewGuid():N}", o => o.AddBulkInterceptor(new CancelBulk { Affected = 3 }));
        var store = (IDocumentStore)owner;

        await store.Insert(new User { Id = "u1", Name = "Alice", Age = 30 });

        Assert.Equal(3, await store.Clear<User>());
        Assert.Equal(1, await store.Query<User>().Count());
    }
}
