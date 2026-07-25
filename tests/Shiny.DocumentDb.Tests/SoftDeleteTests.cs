using Shiny.DocumentDb.LiteDb;
using Shiny.DocumentDb.Sqlite;
using Xunit;

namespace Shiny.DocumentDb.Tests;

// Soft delete: a query filter + a cancelling interceptor, no store involvement. The mapping registry is keyed by
// document type process-wide, so each test type is always mapped on the same flag.
public class SoftDeleteTests
{
    public class Customer
    {
        public string Id { get; set; } = "";
        public string Name { get; set; } = "";
        public int Age { get; set; }
        public bool IsDeleted { get; set; }
    }

    public class Invoice
    {
        public string Id { get; set; } = "";
        public string Number { get; set; } = "";
        public DateTimeOffset? DeletedAt { get; set; }
    }

    public class PlainDoc
    {
        public string Id { get; set; } = "";
        public string Name { get; set; } = "";
    }

    static DocumentStore CreateStore()
    {
        var opts = new DocumentStoreOptions
        {
            DatabaseProvider = new SqliteDatabaseProvider("Data Source=:memory:"),
            TableName = $"t{Guid.NewGuid():N}"
        };
        opts.AddSoftDelete<Customer>(x => x.IsDeleted);
        opts.AddSoftDelete<Invoice>(x => x.DeletedAt);
        return new DocumentStore(opts);
    }

    static async Task<DocumentStore> SeededStore()
    {
        var store = CreateStore();
        await store.Insert(new Customer { Id = "c1", Name = "Alice", Age = 30 });
        await store.Insert(new Customer { Id = "c2", Name = "Bob", Age = 40 });
        await store.Insert(new Customer { Id = "c3", Name = "Carol", Age = 50 });
        return store;
    }

    [Fact]
    public async Task Remove_SetsTheFlag_AndHidesTheDocument()
    {
        using var store = await SeededStore();

        var removed = await store.Remove<Customer>("c1");

        Assert.True(removed);
        Assert.Null(await store.Get<Customer>("c1"));
        Assert.Equal(2, await store.Query<Customer>().Count());

        var all = await store.Query<Customer>().IncludeDeleted().ToList();
        Assert.Equal(3, all.Count);
        Assert.True(all.Single(x => x.Id == "c1").IsDeleted);
        // The document body is intact — only the flag changed.
        Assert.Equal("Alice", all.Single(x => x.Id == "c1").Name);
    }

    [Fact]
    public async Task SoftDelete_Extension_MatchesRemove()
    {
        using var store = await SeededStore();

        Assert.True(await store.SoftDelete<Customer>("c1"));
        Assert.Null(await store.Get<Customer>("c1"));
    }

    [Fact]
    public async Task SoftDelete_OnUnmappedType_Throws()
    {
        using var store = await SeededStore();
        await store.Insert(new PlainDoc { Id = "p1", Name = "x" });

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => store.SoftDelete<PlainDoc>("p1"));
        Assert.Contains("not mapped for soft delete", ex.Message);
        // ...and it did not fall through to a hard delete.
        Assert.NotNull(await store.Get<PlainDoc>("p1"));
    }

    [Fact]
    public async Task Remove_Twice_ReportsFalseTheSecondTime()
    {
        using var store = await SeededStore();

        Assert.True(await store.Remove<Customer>("c1"));
        Assert.False(await store.Remove<Customer>("c1"));
    }

    [Fact]
    public async Task ExecuteDelete_SoftDeletesTheMatchingSet()
    {
        using var store = await SeededStore();

        var affected = await store.Query<Customer>().Where(x => x.Age >= 40).ExecuteDelete();

        Assert.Equal(2, affected);
        Assert.Equal(1, await store.Query<Customer>().Count());
        Assert.Equal(3, await store.Query<Customer>().IncludeDeleted().Count());
    }

    [Fact]
    public async Task Clear_SoftDeletesEverything()
    {
        using var store = await SeededStore();

        var affected = await store.Clear<Customer>();

        Assert.Equal(3, affected);
        Assert.Equal(0, await store.Query<Customer>().Count());
        Assert.Equal(3, await store.Query<Customer>().IncludeDeleted().Count());
    }

    [Fact]
    public async Task OnlyDeleted_ReturnsTheFlaggedDocuments()
    {
        using var store = await SeededStore();
        await store.Remove<Customer>("c2");

        var deleted = await store.Query<Customer>().OnlyDeleted().ToList();

        Assert.Single(deleted);
        Assert.Equal("c2", deleted[0].Id);
    }

    [Fact]
    public async Task Restore_ClearsTheFlag()
    {
        using var store = await SeededStore();
        await store.Remove<Customer>("c1");

        var restored = await store.Restore<Customer>(x => x.Id == "c1");

        Assert.Equal(1, restored);
        var back = await store.Get<Customer>("c1");
        Assert.NotNull(back);
        Assert.False(back!.IsDeleted);
        Assert.Equal(3, await store.Query<Customer>().Count());
    }

    [Fact]
    public async Task PurgeDeleted_HardDeletesFlaggedOnly()
    {
        using var store = await SeededStore();
        await store.Remove<Customer>("c1");
        await store.Remove<Customer>("c2");

        var purged = await store.PurgeDeleted<Customer>();

        Assert.Equal(2, purged);
        Assert.Equal(1, await store.Query<Customer>().IncludeDeleted().Count());
        Assert.Equal(1, await store.Query<Customer>().Count());
    }

    [Fact]
    public async Task PurgeDeleted_WithPredicate_PurgesTheSubset()
    {
        using var store = await SeededStore();
        await store.Remove<Customer>("c1");
        await store.Remove<Customer>("c2");

        var purged = await store.PurgeDeleted<Customer>(x => x.Id == "c1");

        Assert.Equal(1, purged);
        var remaining = await store.Query<Customer>().IncludeDeleted().ToList();
        Assert.Equal(2, remaining.Count);
        Assert.Contains(remaining, x => x.Id == "c2" && x.IsDeleted);
    }

    [Fact]
    public async Task HardDelete_BypassesSoftDelete()
    {
        using var store = await SeededStore();

        Assert.True(await store.HardDelete<Customer>("c1"));

        Assert.Equal(2, await store.Query<Customer>().IncludeDeleted().Count());
    }

    [Fact]
    public async Task SuppressInterceptors_MakesRemoveAHardDelete()
    {
        using var store = await SeededStore();

        using (store.SuppressInterceptors())
            await store.Remove<Customer>("c1");

        Assert.Equal(2, await store.Query<Customer>().IncludeDeleted().Count());
    }

    [Fact]
    public async Task TimestampFlag_StampsOnDeleteAndNullsOnRestore()
    {
        using var store = CreateStore();
        await store.Insert(new Invoice { Id = "i1", Number = "INV-1" });
        var before = DateTimeOffset.UtcNow.AddSeconds(-5);

        Assert.True(await store.Remove<Invoice>("i1"));

        Assert.Null(await store.Get<Invoice>("i1"));
        var flagged = await store.Query<Invoice>().IncludeDeleted().ToList();
        Assert.Single(flagged);
        Assert.NotNull(flagged[0].DeletedAt);
        Assert.True(flagged[0].DeletedAt >= before, $"expected a recent stamp, got {flagged[0].DeletedAt}");

        Assert.Equal(1, await store.Restore<Invoice>(x => x.Id == "i1"));
        var back = await store.Get<Invoice>("i1");
        Assert.NotNull(back);
        Assert.Null(back!.DeletedAt);
    }

    [Fact]
    public void AddSoftDelete_OnAnUnsupportedPropertyType_Throws()
    {
        var opts = new DocumentStoreOptions
        {
            DatabaseProvider = new SqliteDatabaseProvider("Data Source=:memory:"),
            TableName = "t_bad"
        };

        var ex = Assert.Throws<ArgumentException>(() => opts.AddSoftDelete<Customer>(x => x.Name));
        Assert.Contains("bool flag or a nullable DateTime", ex.Message);
    }

    [Fact]
    public void AddSoftDelete_RemappingADifferentProperty_Throws()
    {
        var opts = new DocumentStoreOptions
        {
            DatabaseProvider = new SqliteDatabaseProvider("Data Source=:memory:"),
            TableName = "t_conflict"
        };
        opts.AddSoftDelete<ConflictDoc>(x => x.IsDeleted);

        var ex = Assert.Throws<InvalidOperationException>(() => opts.AddSoftDelete<ConflictDoc>(x => x.RemovedAt));
        Assert.Contains("already mapped for soft delete", ex.Message);
    }

    public class ConflictDoc
    {
        public string Id { get; set; } = "";
        public bool IsDeleted { get; set; }
        public DateTime? RemovedAt { get; set; }
    }

    // Proof the same mechanism works on a non-relational provider (LiteDB is embedded, so it needs no container):
    // DocumentProviderBase supplies the same cancellation wiring every provider shares.
    public class LiteDoc
    {
        public string Id { get; set; } = "";
        public string Name { get; set; } = "";
        public bool IsDeleted { get; set; }
    }

    [Fact]
    public async Task LiteDb_SoftDeletesToo()
    {
        var path = Path.Combine(Path.GetTempPath(), $"sd{Guid.NewGuid():N}.db");
        try
        {
            var opts = new LiteDbDocumentStoreOptions { ConnectionString = $"Filename={path};Connection=direct" };
            opts.AddSoftDelete<LiteDoc>(x => x.IsDeleted);
            using var store = new LiteDbDocumentStore(opts);

            await store.Insert(new LiteDoc { Id = "l1", Name = "Alice" });
            await store.Insert(new LiteDoc { Id = "l2", Name = "Bob" });

            Assert.True(await store.Remove<LiteDoc>("l1"));
            Assert.Null(await store.Get<LiteDoc>("l1"));
            Assert.Equal(1, await store.Query<LiteDoc>().Count());
            Assert.Equal(2, await store.Query<LiteDoc>().IncludeDeleted().Count());

            // Set-based: Clear has no source query, so the interceptor uses ctx.Store.
            Assert.Equal(1, await store.Clear<LiteDoc>());
            Assert.Equal(0, await store.Query<LiteDoc>().Count());
            Assert.Equal(2, await store.Query<LiteDoc>().IncludeDeleted().Count());
        }
        finally
        {
            try { File.Delete(path); } catch { /* best effort */ }
        }
    }
}
