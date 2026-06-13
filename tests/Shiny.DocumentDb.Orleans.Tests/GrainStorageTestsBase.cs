using Orleans.Runtime;
using Orleans.Storage;
using Xunit;

namespace Shiny.DocumentDb.Orleans.Tests;

/// <summary>
/// Backend-agnostic grain-storage conformance tests. Each subclass supplies a freshly-isolated
/// <see cref="DocumentDbGrainStorage"/> (its own table/collection) so tests don't collide.
/// </summary>
public abstract class GrainStorageTestsBase
{
    protected abstract DocumentDbGrainStorage CreateStorage();

    static GrainId NewGrain() => GrainId.Create("counter", Guid.NewGuid().ToString("N"));

    [Fact]
    public async Task Write_Then_Read_Roundtrips()
    {
        var storage = this.CreateStorage();
        var id = NewGrain();

        var write = new TestGrainState<CounterState>(new CounterState { Count = 5, Name = "alpha" });
        await storage.WriteStateAsync("state", id, write);
        Assert.True(write.RecordExists);
        Assert.Equal("1", write.ETag);

        var read = new TestGrainState<CounterState>(new CounterState());
        await storage.ReadStateAsync("state", id, read);
        Assert.True(read.RecordExists);
        Assert.Equal("1", read.ETag);
        Assert.Equal(5, read.State.Count);
        Assert.Equal("alpha", read.State.Name);
    }

    [Fact]
    public async Task Read_Missing_ReportsNoRecord()
    {
        var storage = this.CreateStorage();
        var read = new TestGrainState<CounterState>(new CounterState());

        await storage.ReadStateAsync("state", NewGrain(), read);

        Assert.False(read.RecordExists);
    }

    [Fact]
    public async Task Update_Increments_Etag()
    {
        var storage = this.CreateStorage();
        var id = NewGrain();

        var state = new TestGrainState<CounterState>(new CounterState { Count = 1 });
        await storage.WriteStateAsync("state", id, state);
        state.State.Count = 2;
        await storage.WriteStateAsync("state", id, state);
        Assert.Equal("2", state.ETag);

        var read = new TestGrainState<CounterState>(new CounterState());
        await storage.ReadStateAsync("state", id, read);
        Assert.Equal(2, read.State.Count);
        Assert.Equal("2", read.ETag);
    }

    [Fact]
    public async Task Stale_Write_Throws_InconsistentState()
    {
        var storage = this.CreateStorage();
        var id = NewGrain();

        var first = new TestGrainState<CounterState>(new CounterState { Count = 1 });
        await storage.WriteStateAsync("state", id, first); // ETag "1"

        // A second writer still holding the now-stale ETag "1".
        var stale = new TestGrainState<CounterState>(new CounterState { Count = 99 })
        {
            ETag = "1",
            RecordExists = true
        };

        first.State.Count = 2;
        await storage.WriteStateAsync("state", id, first); // winning write -> ETag "2"

        // This is the CAS test: without atomic compare-and-swap the stale write would clobber.
        await Assert.ThrowsAsync<InconsistentStateException>(
            async () => await storage.WriteStateAsync("state", id, stale));
    }

    [Fact]
    public async Task Clear_Removes_State()
    {
        var storage = this.CreateStorage();
        var id = NewGrain();

        var state = new TestGrainState<CounterState>(new CounterState { Count = 7 });
        await storage.WriteStateAsync("state", id, state);
        await storage.ClearStateAsync("state", id, state);
        Assert.False(state.RecordExists);

        var read = new TestGrainState<CounterState>(new CounterState());
        await storage.ReadStateAsync("state", id, read);
        Assert.False(read.RecordExists);
    }
}
