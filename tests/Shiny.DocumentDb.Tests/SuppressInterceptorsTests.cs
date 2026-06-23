using Shiny.DocumentDb.Sqlite;
using Shiny.DocumentDb.Tests.Fixtures;
using Xunit;

namespace Shiny.DocumentDb.Tests;

public class SuppressInterceptorsTests
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

    sealed class Spy : IDocumentInterceptor, IDocumentBulkInterceptor
    {
        public int PerDocCalls;
        public int BulkCalls;
        public Task BeforeWrite(DocumentWriteContext ctx, CancellationToken ct) { this.PerDocCalls++; return Task.CompletedTask; }
        public Task AfterWrite(DocumentWriteContext ctx, CancellationToken ct) { this.PerDocCalls++; return Task.CompletedTask; }
        public Task BeforeBulkWrite(DocumentBulkContext ctx, CancellationToken ct) { this.BulkCalls++; return Task.CompletedTask; }
        public Task AfterBulkWrite(DocumentBulkContext ctx, CancellationToken ct) { this.BulkCalls++; return Task.CompletedTask; }
    }

    [Fact]
    public async Task SuppressedUnit_RunsNoInterceptors_ButWritesData()
    {
        var spy = new Spy();
        using var store = CreateStore(o => { o.AddInterceptor(spy); o.AddBulkInterceptor(spy); });

        var uow = store.CreateUnitOfWork();
        uow.Upsert(new User { Id = "u1", Name = "A", Age = 1 });
        uow.Upsert(new User { Id = "u2", Name = "B", Age = 2 });
        await uow.SaveChanges(suppressInterceptors: true);

        Assert.Equal(0, spy.PerDocCalls);
        Assert.Equal("A", (await store.Get<User>("u1"))!.Name);
        Assert.Equal("B", (await store.Get<User>("u2"))!.Name);
    }

    [Fact]
    public async Task Suppression_IsTransactionBounded_WriteOutsideUnitStillFires()
    {
        var spy = new Spy();
        using var store = CreateStore(o => o.AddInterceptor(spy));

        var uow = store.CreateUnitOfWork();
        uow.Upsert(new User { Id = "u1", Name = "A", Age = 1 });
        await uow.SaveChanges(suppressInterceptors: true);
        Assert.Equal(0, spy.PerDocCalls);

        await store.Insert(new User { Id = "u2", Name = "B", Age = 2 });   // outside the suppressed unit
        Assert.True(spy.PerDocCalls > 0);
    }

    [Fact]
    public async Task NonSuppressedUnit_FiresInterceptors()
    {
        var spy = new Spy();
        using var store = CreateStore(o => o.AddInterceptor(spy));

        var uow = store.CreateUnitOfWork();
        uow.Upsert(new User { Id = "u1", Name = "A", Age = 1 });
        await uow.SaveChanges();   // not suppressed

        Assert.True(spy.PerDocCalls > 0);
    }
}
