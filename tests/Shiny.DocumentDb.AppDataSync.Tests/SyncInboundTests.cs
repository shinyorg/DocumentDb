using System.Text.Json;
using Shiny.Data.Sync;
using Xunit;

namespace Shiny.DocumentDb.AppDataSync.Tests;


public class SyncInboundTests
{
    static SyncReceivedItem Received<T>(T entity, SyncVerb verb) where T : ISyncEntity
        => new(typeof(T).FullName!, typeof(T), entity, "{}", verb);


    [Fact]
    public async Task OnReceived_Create_Upserts_Into_Store()
    {
        using var h = new SyncHarness(sync => sync.Sync<TodoItem>());
        var id = Guid.NewGuid();

        await h.Delegate.OnReceived(Received(new TodoItem { Id = id, Title = "from server" }, SyncVerb.Create));

        var stored = await h.Store.Get<TodoItem>(id);
        Assert.NotNull(stored);
        Assert.Equal("from server", stored!.Title);
    }

    [Fact]
    public async Task OnReceived_Update_Upserts_Into_Store()
    {
        using var h = new SyncHarness(sync => sync.Sync<TodoItem>());
        var id = Guid.NewGuid();
        await h.Delegate.OnReceived(Received(new TodoItem { Id = id, Title = "v1" }, SyncVerb.Create));

        await h.Delegate.OnReceived(Received(new TodoItem { Id = id, Title = "v2", Completed = true }, SyncVerb.Update));

        var stored = await h.Store.Get<TodoItem>(id);
        Assert.Equal("v2", stored!.Title);
        Assert.True(stored.Completed);
    }

    [Fact]
    public async Task OnReceived_Delete_Removes_From_Store()
    {
        using var h = new SyncHarness(sync => sync.Sync<TodoItem>());
        var id = Guid.NewGuid();
        await h.Delegate.OnReceived(Received(new TodoItem { Id = id, Title = "x" }, SyncVerb.Create));

        await h.Delegate.OnReceived(Received(new TodoItem { Id = id }, SyncVerb.Delete));

        var stored = await h.Store.Get<TodoItem>(id);
        Assert.Null(stored);
    }

    // ── The critical loop guard: inbound apply must NOT enqueue back ─────────

    [Fact]
    public async Task OnReceived_Does_Not_Echo_Back_To_Outbox()
    {
        using var h = new SyncHarness(sync => sync.Sync<TodoItem>());

        await h.Delegate.OnReceived(Received(new TodoItem { Id = Guid.NewGuid(), Title = "server" }, SyncVerb.Create));
        await h.Delegate.OnReceived(Received(new TodoItem { Id = Guid.NewGuid(), Title = "server2" }, SyncVerb.Update));
        await h.Delegate.OnReceived(Received(new TodoItem { Id = Guid.NewGuid() }, SyncVerb.Delete));

        Assert.Empty(h.Manager.Queued);
    }

    // ── Inbound applies fire NO interceptor (spy must not run) ───────────────

    [Fact]
    public async Task OnReceived_Fires_No_Interceptor()
    {
        var spy = new SpyInterceptor();
        using var h = new SyncHarness(sync => sync.Sync<TodoItem>(), spy);

        await h.Delegate.OnReceived(Received(new TodoItem { Id = Guid.NewGuid(), Title = "x" }, SyncVerb.Create));
        await h.Delegate.OnReceived(Received(new TodoItem { Id = Guid.NewGuid() }, SyncVerb.Delete));

        Assert.Equal(0, spy.BeforeWrites);
        Assert.Equal(0, spy.AfterWrites);
        Assert.Equal(0, spy.BeforeBulk);
        Assert.Equal(0, spy.AfterBulk);
    }

    // ── But a direct write still fires the spy (scope is transaction-bounded) ─

    [Fact]
    public async Task Direct_Write_Still_Fires_Interceptors()
    {
        var spy = new SpyInterceptor();
        using var h = new SyncHarness(sync => sync.Sync<TodoItem>(), spy);

        await h.Store.Insert(new TodoItem { Id = Guid.NewGuid(), Title = "direct" });

        Assert.True(spy.AfterWrites > 0);
    }

    // ── InboundMode.Update replaces the document ────────────────────────────

    [Fact]
    public async Task OnReceived_Update_Mode_Applies_As_Update()
    {
        using var h = new SyncHarness(sync => sync.Sync<TodoItem>(o => o.InboundMode = InboundApplyMode.Update));
        var id = Guid.NewGuid();
        await h.Store.Insert(new TodoItem { Id = id, Title = "local", Completed = true });
        h.Manager.Queued.Clear();

        await h.Delegate.OnReceived(Received(new TodoItem { Id = id, Title = "server" }, SyncVerb.Update));

        var stored = await h.Store.Get<TodoItem>(id);
        Assert.Equal("server", stored!.Title);
        // Update is a full replace — Completed resets to default.
        Assert.False(stored.Completed);
    }
}


public class SyncConflictTests
{
    [Fact]
    public void MergePatch_Overlays_Local_Onto_Remote()
    {
        var remote = """{"id":"1","title":"server","completed":true}""";
        var localPatch = """{"title":"client"}""";

        var merged = DocumentStoreSyncDelegate.MergePatch(remote, localPatch);

        using var doc = JsonDocument.Parse(merged);
        Assert.Equal("client", doc.RootElement.GetProperty("title").GetString());
        Assert.True(doc.RootElement.GetProperty("completed").GetBoolean());
        Assert.Equal("1", doc.RootElement.GetProperty("id").GetString());
    }

    [Fact]
    public void MergePatch_Null_Removes_Field()
    {
        var remote = """{"id":"1","title":"server","note":"keep"}""";
        var localPatch = """{"note":null}""";

        var merged = DocumentStoreSyncDelegate.MergePatch(remote, localPatch);

        using var doc = JsonDocument.Parse(merged);
        Assert.False(doc.RootElement.TryGetProperty("note", out _));
    }
}


public class SyncIdentityBridgeTests
{
    [Fact]
    public void Guid_Identity_RoundTrips()
    {
        var factory = SyncDeleteEntityFactory.Create<TodoItem>();
        var id = Guid.NewGuid();
        var entity = factory(id.ToString());
        Assert.NotNull(entity);
        Assert.Equal(id.ToString(), entity!.Identifier);
    }

    [Fact]
    public void String_Identity_RoundTrips()
    {
        var factory = SyncDeleteEntityFactory.Create<Note>();
        var entity = factory("note-42");
        Assert.NotNull(entity);
        Assert.Equal("note-42", entity!.Identifier);
    }
}
