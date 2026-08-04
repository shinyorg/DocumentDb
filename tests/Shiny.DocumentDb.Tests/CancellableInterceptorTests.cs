using Shiny.DocumentDb.LiteDb;
using Shiny.DocumentDb.Sqlite;
using Shiny.DocumentDb.Tests.Fixtures;
using Xunit;

namespace Shiny.DocumentDb.Tests;

// DocumentWriteContext.Cancel() / DocumentBulkContext.Cancel() — an interceptor replacing the store's write with
// one of its own (the mechanism soft delete is built on).
public class CancellableInterceptorTests
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

    sealed class Canceller : IDocumentInterceptor
    {
        public DocumentOperation CancelOn { get; init; } = DocumentOperation.Delete;
        public bool Result { get; init; } = true;
        public readonly List<string> Events = new();

        public Task BeforeWrite(DocumentWriteContext ctx, CancellationToken ct)
        {
            this.Events.Add($"before:{ctx.Operation}");
            if (ctx.Operation == this.CancelOn)
                ctx.Cancel(this.Result);
            return Task.CompletedTask;
        }

        public Task AfterWrite(DocumentWriteContext ctx, CancellationToken ct)
        {
            this.Events.Add($"after:{ctx.Operation}");
            return Task.CompletedTask;
        }
    }

    sealed class Observer : IDocumentInterceptor
    {
        public readonly List<string> Events = new();
        public int Order { get; init; }
        public Task BeforeWrite(DocumentWriteContext ctx, CancellationToken ct) { this.Events.Add($"before:{ctx.Operation}"); return Task.CompletedTask; }
        public Task AfterWrite(DocumentWriteContext ctx, CancellationToken ct) { this.Events.Add($"after:{ctx.Operation}"); return Task.CompletedTask; }
    }

    sealed class BulkCanceller : IDocumentBulkInterceptor
    {
        public int Affected { get; init; } = 7;
        public readonly List<string> Events = new();
        public bool SawSourceQuery;

        public Task BeforeBulkWrite(DocumentBulkContext ctx, CancellationToken ct)
        {
            this.Events.Add($"before:{ctx.Operation}");
            this.SawSourceQuery = ctx.QueryAs<User>() != null;
            ctx.Cancel(this.Affected);
            return Task.CompletedTask;
        }

        public Task AfterBulkWrite(DocumentBulkContext ctx, CancellationToken ct)
        {
            this.Events.Add($"after:{ctx.Operation}");
            return Task.CompletedTask;
        }
    }

    [Fact]
    public async Task Cancel_Remove_SkipsTheDelete_AndReportsTheGivenResult()
    {
        var canceller = new Canceller();
        using var store = CreateStore(o => o.AddInterceptor(canceller));
        await store.Insert(new User { Id = "u1", Name = "Alice" });

        var removed = await store.Remove<User>("u1");

        Assert.True(removed);
        Assert.NotNull(await store.Get<User>("u1"));
        // No AfterWrite for the cancelled delete — nothing was written.
        Assert.Equal(new[] { "before:Insert", "after:Insert", "before:Delete" }, canceller.Events);
    }

    [Fact]
    public async Task Cancel_Remove_CanReportFalse()
    {
        using var store = CreateStore(o => o.AddInterceptor(new Canceller { Result = false }));
        await store.Insert(new User { Id = "u1", Name = "Alice" });

        Assert.False(await store.Remove<User>("u1"));
        Assert.NotNull(await store.Get<User>("u1"));
    }

    [Fact]
    public async Task Cancel_Insert_SkipsTheWrite()
    {
        using var store = CreateStore(o => o.AddInterceptor(new Canceller { CancelOn = DocumentOperation.Insert }));

        await store.Insert(new User { Id = "u1", Name = "Alice" });

        Assert.Null(await store.Get<User>("u1"));
        Assert.Equal(0, await store.Query<User>().Count());
    }

    [Fact]
    public async Task Cancel_Update_SkipsTheWrite()
    {
        var canceller = new Canceller { CancelOn = DocumentOperation.Update };
        using var store = CreateStore(o => o.AddInterceptor(canceller));
        await store.Insert(new User { Id = "u1", Name = "Alice" });

        await store.Update(new User { Id = "u1", Name = "Changed" });

        Assert.Equal("Alice", (await store.Get<User>("u1"))!.Name);
    }

    [Fact]
    public async Task Cancel_Upsert_SkipsTheWrite()
    {
        using var store = CreateStore(o => o.AddInterceptor(new Canceller { CancelOn = DocumentOperation.Upsert }));

        await store.Upsert(new User { Id = "u1", Name = "Alice" });

        Assert.Null(await store.Get<User>("u1"));
    }

    [Fact]
    public async Task Cancel_StopsLaterInterceptors()
    {
        var first = new Canceller();                 // Order 0
        var later = new Observer { Order = 10 };
        using var store = CreateStore(o => o.AddInterceptor(first).AddInterceptor(later));
        await store.Insert(new User { Id = "u1", Name = "Alice" });

        await store.Remove<User>("u1");

        Assert.Contains("before:Insert", later.Events);
        Assert.DoesNotContain("before:Delete", later.Events);
    }

    [Fact]
    public async Task Cancel_OutsideBeforeWrite_Throws()
    {
        using var store = CreateStore(o => o.ConfigureDocument<User>(cfg => cfg.OnAfterWrite((ctx, _) =>
        {
            ctx.Cancel();
            return Task.CompletedTask;
        })));

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => store.Insert(new User { Id = "u1", Name = "Alice" }));
        Assert.Contains("only valid inside", ex.Message);
    }

    [Fact]
    public async Task Cancel_ExecuteDelete_SkipsTheDelete_AndReportsTheGivenCount()
    {
        var bulk = new BulkCanceller();
        using var store = CreateStore(o => o.AddBulkInterceptor(bulk));
        await store.Insert(new User { Id = "u1", Name = "Alice", Age = 30 });
        await store.Insert(new User { Id = "u2", Name = "Bob", Age = 40 });

        var affected = await store.Query<User>().Where(x => x.Age > 20).ExecuteDelete();

        Assert.Equal(7, affected);
        Assert.Equal(2, await store.Query<User>().Count());
        Assert.True(bulk.SawSourceQuery);
        Assert.Equal(new[] { "before:Delete" }, bulk.Events);
    }

    [Fact]
    public async Task Cancel_Clear_SkipsTheDelete_AndExposesTheStoreNotAQuery()
    {
        var bulk = new BulkCanceller { Affected = 3 };
        using var store = CreateStore(o => o.AddBulkInterceptor(bulk));
        await store.Insert(new User { Id = "u1", Name = "Alice" });

        var affected = await store.Clear<User>();

        Assert.Equal(3, affected);
        Assert.Equal(1, await store.Query<User>().Count());
        // Clear has no source query — the store is the handle a cancelling interceptor gets instead.
        Assert.False(bulk.SawSourceQuery);
    }

    [Fact]
    public async Task Cancel_BatchInsert_SkipsTheCancelledDocuments()
    {
        // Relational BatchInsert is not fast-eligible once a per-doc interceptor is registered, so it loops the
        // single-document Insert inside one unit — cancellation applies per document.
        using var store = CreateStore(o => o.ConfigureDocument<User>(cfg => cfg.OnBeforeWrite((ctx, _) =>
        {
            if (ctx.Document is User { Name: "Bob" })
                ctx.Cancel();
            return Task.CompletedTask;
        })));

        await store.BatchInsert(new[]
        {
            new User { Id = "u1", Name = "Alice" },
            new User { Id = "u2", Name = "Bob" }
        });

        Assert.NotNull(await store.Get<User>("u1"));
        Assert.Null(await store.Get<User>("u2"));
    }

    [Fact]
    public async Task Cancel_DuringAProviderBatchInsert_ThrowsNotSupported()
    {
        // Providers that write a batch as one set run BeforeWrite per document up front, and cannot skip an
        // individual row afterwards — cancelling there is refused loudly rather than silently writing the row.
        var path = Path.Combine(Path.GetTempPath(), $"ci{Guid.NewGuid():N}.db");
        try
        {
            var opts = new LiteDbDocumentStoreOptions { ConnectionString = $"Filename={path};Connection=direct" };
            opts.AddInterceptor(new Canceller { CancelOn = DocumentOperation.Insert });
            using var store = new LiteDbDocumentStore(opts);

            var ex = await Assert.ThrowsAsync<NotSupportedException>(() => store.BatchInsert(new[]
            {
                new User { Id = "u1", Name = "Alice" },
                new User { Id = "u2", Name = "Bob" }
            }));
            Assert.Contains("not supported for batch writes", ex.Message);
        }
        finally
        {
            try { File.Delete(path); } catch { /* best effort */ }
        }
    }

    [Fact]
    public async Task Cancel_InsideUnitOfWork_KeepsTheInterceptorsOwnWrite()
    {
        // The cancelling interceptor writes through ctx.Store (transaction-bound), so its replacement write must
        // survive the unit's commit even though the original delete never ran.
        using var store = CreateStore(o => o.ConfigureDocument<User>(cfg => cfg.OnBeforeWrite(async (ctx, ct) =>
        {
            if (ctx.Operation != DocumentOperation.Delete)
                return;
            await ctx.Store.SetProperty<User>(ctx.Id!, x => x.Name, "tombstoned", null, ct);
            ctx.Cancel();
        })));
        await store.Insert(new User { Id = "u1", Name = "Alice" });

        await using var session = store.OpenSession();
        session.Remove<User>("u1");
        await session.SaveChanges();

        var stored = await store.Get<User>("u1");
        Assert.NotNull(stored);
        Assert.Equal("tombstoned", stored!.Name);
    }
}
