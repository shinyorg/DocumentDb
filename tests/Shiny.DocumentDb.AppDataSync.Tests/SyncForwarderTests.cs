using System.Text.Json;
using Shiny.Data.Sync;
using Xunit;

namespace Shiny.DocumentDb.AppDataSync.Tests;


public class SyncForwarderTests
{
    // ── Outbound: write → correct verb + identifier ─────────────────────────

    [Fact]
    public async Task Insert_Enqueues_Create()
    {
        using var h = new SyncHarness(sync => sync.Sync<TodoItem>());
        var id = Guid.NewGuid();
        await h.Store.Insert(new TodoItem { Id = id, Title = "Buy milk" });

        var op = Assert.Single(h.Manager.Queued);
        Assert.Equal(SyncVerb.Create, op.Verb);
        Assert.Equal(id.ToString(), op.Identifier);
    }

    [Fact]
    public async Task Update_Enqueues_Update()
    {
        using var h = new SyncHarness(sync => sync.Sync<TodoItem>());
        var id = Guid.NewGuid();
        await h.Store.Insert(new TodoItem { Id = id, Title = "A" });
        h.Manager.Queued.Clear();

        await h.Store.Update(new TodoItem { Id = id, Title = "B" });

        var op = Assert.Single(h.Manager.Queued);
        Assert.Equal(SyncVerb.Update, op.Verb);
        Assert.Equal(id.ToString(), op.Identifier);
    }

    [Fact]
    public async Task Upsert_Enqueues_Update()
    {
        using var h = new SyncHarness(sync => sync.Sync<TodoItem>());
        var id = Guid.NewGuid();
        await h.Store.Upsert(new TodoItem { Id = id, Title = "A" });

        var op = Assert.Single(h.Manager.Queued);
        Assert.Equal(SyncVerb.Update, op.Verb);
        Assert.Equal(id.ToString(), op.Identifier);
    }

    [Fact]
    public async Task Remove_Enqueues_Delete()
    {
        using var h = new SyncHarness(sync => sync.Sync<TodoItem>());
        var id = Guid.NewGuid();
        await h.Store.Insert(new TodoItem { Id = id, Title = "A" });
        h.Manager.Queued.Clear();

        await h.Store.Remove<TodoItem>(id);

        var op = Assert.Single(h.Manager.Queued);
        Assert.Equal(SyncVerb.Delete, op.Verb);
        Assert.Equal(id.ToString(), op.Identifier);
    }

    [Fact]
    public async Task Remove_StringId_Enqueues_Delete()
    {
        using var h = new SyncHarness(sync => sync.Sync<Note>());
        await h.Store.Insert(new Note { Id = "note-1", Text = "hi" });
        h.Manager.Queued.Clear();

        await h.Store.Remove<Note>("note-1");

        var op = Assert.Single(h.Manager.Queued);
        Assert.Equal(SyncVerb.Delete, op.Verb);
        Assert.Equal("note-1", op.Identifier);
    }

    [Fact]
    public async Task StringId_RoundTrips()
    {
        using var h = new SyncHarness(sync => sync.Sync<Note>());
        await h.Store.Insert(new Note { Id = "note-1", Text = "hi" });

        var op = Assert.Single(h.Manager.Queued);
        Assert.Equal(SyncVerb.Create, op.Verb);
        Assert.Equal("note-1", op.Identifier);
    }

    [Fact]
    public async Task NonSyncedType_IsIgnored()
    {
        using var h = new SyncHarness(sync => sync.Sync<TodoItem>());
        await h.Store.Insert(new LocalScratch { Id = "x", Value = "v" });
        Assert.Empty(h.Manager.Queued);
    }

    [Fact]
    public async Task OutboundKinds_Filters_Which_Verbs_Enqueue()
    {
        using var h = new SyncHarness(sync => sync.Sync<TodoItem>(o => o.OutboundKinds = SyncWriteKinds.Insert));
        var id = Guid.NewGuid();
        await h.Store.Insert(new TodoItem { Id = id, Title = "A" });
        await h.Store.Update(new TodoItem { Id = id, Title = "B" });

        var op = Assert.Single(h.Manager.Queued);
        Assert.Equal(SyncVerb.Create, op.Verb);
    }

    // ── Batch writes: every item enqueues ───────────────────────────────────

    [Fact]
    public async Task BatchUpsert_Enqueues_Every_Item()
    {
        using var h = new SyncHarness(sync => sync.Sync<TodoItem>());
        var a = Guid.NewGuid();
        var b = Guid.NewGuid();
        await h.Store.BatchUpsert(new[]
        {
            new TodoItem { Id = a, Title = "A" },
            new TodoItem { Id = b, Title = "B" }
        });

        Assert.Equal(2, h.Manager.Queued.Count);
        Assert.All(h.Manager.Queued, op => Assert.Equal(SyncVerb.Update, op.Verb));
        Assert.Contains(h.Manager.Queued, op => op.Identifier == a.ToString());
        Assert.Contains(h.Manager.Queued, op => op.Identifier == b.ToString());
    }

    [Fact]
    public async Task BatchInsert_Enqueues_Every_Item_As_Create()
    {
        using var h = new SyncHarness(sync => sync.Sync<TodoItem>());
        var ids = new[] { Guid.NewGuid(), Guid.NewGuid(), Guid.NewGuid() };
        await h.Store.BatchInsert(ids.Select(i => new TodoItem { Id = i, Title = "x" }).ToList());

        Assert.Equal(3, h.Manager.Queued.Count);
        Assert.All(h.Manager.Queued, op => Assert.Equal(SyncVerb.Create, op.Verb));
    }

    // ── Set-based writes: rejected on synced types ──────────────────────────

    [Fact]
    public async Task Clear_On_Synced_Type_Throws()
    {
        using var h = new SyncHarness(sync => sync.Sync<TodoItem>());
        await Assert.ThrowsAsync<SyncBulkWriteNotSupportedException>(() => h.Store.Clear<TodoItem>());
        Assert.Empty(h.Manager.Queued);
    }

    [Fact]
    public async Task ExecuteDelete_On_Synced_Type_Throws()
    {
        using var h = new SyncHarness(sync => sync.Sync<TodoItem>());
        await h.Store.Insert(new TodoItem { Id = Guid.NewGuid(), Title = "A", Completed = true });
        h.Manager.Queued.Clear();

        await Assert.ThrowsAsync<SyncBulkWriteNotSupportedException>(
            () => h.Store.Query<TodoItem>().Where(x => x.Completed).ExecuteDelete()
        );
    }

    [Fact]
    public async Task ExecuteUpdate_On_Synced_Type_Throws()
    {
        using var h = new SyncHarness(sync => sync.Sync<TodoItem>());
        await h.Store.Insert(new TodoItem { Id = Guid.NewGuid(), Title = "A" });
        h.Manager.Queued.Clear();

        await Assert.ThrowsAsync<SyncBulkWriteNotSupportedException>(
            () => h.Store.Query<TodoItem>().Where(x => x.Title == "A").ExecuteUpdate(x => x.Completed, true)
        );
    }

    // ── ClearAll: no throw, nothing enqueued ────────────────────────────────

    [Fact]
    public async Task ClearAll_Does_Not_Throw_And_Enqueues_Nothing()
    {
        using var h = new SyncHarness(sync => sync.Sync<TodoItem>());
        await h.Store.Insert(new TodoItem { Id = Guid.NewGuid(), Title = "A" });
        h.Manager.Queued.Clear();

        await h.Store.ClearAll();
        Assert.Empty(h.Manager.Queued);
    }
}
