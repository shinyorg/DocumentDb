using Orleans.Runtime;

namespace Shiny.DocumentDb.Orleans.Tests;

public class CounterState
{
    public int Count { get; set; }
    public string? Name { get; set; }
}

/// <summary>Minimal <see cref="IGrainState{T}"/> so tests can drive the storage provider directly.</summary>
public sealed class TestGrainState<T> : IGrainState<T>
{
    public TestGrainState(T state) => this.State = state;
    public T State { get; set; }
    public string ETag { get; set; } = null!;
    public bool RecordExists { get; set; }
}
