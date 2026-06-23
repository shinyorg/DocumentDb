using System.Collections.Concurrent;
using Shiny.Data.Sync;

namespace Shiny.DocumentDb.AppDataSync.Tests;


public sealed record QueuedOp(SyncVerb Verb, string Identifier, object Entity);


/// <summary>In-memory fake of <see cref="IDataSyncManager"/> — records every Queue call, no network.</summary>
public sealed class FakeDataSyncManager : IDataSyncManager
{
    public ConcurrentQueue<QueuedOp> Queued { get; } = new();

    public Task<SyncOperation> Queue<T>(SyncVerb verb, T entity) where T : ISyncEntity
    {
        this.Queued.Enqueue(new QueuedOp(verb, entity.Identifier, entity!));
        var op = new SyncOperation(
            Identifier: Guid.NewGuid().ToString("N"),
            EndpointKey: typeof(T).FullName!,
            EntityIdentifier: entity.Identifier,
            Verb: verb,
            Payload: null,
            State: SyncOperationState.Pending,
            CreatedAt: DateTimeOffset.UtcNow,
            Attempts: 0,
            LastError: null
        );
        return Task.FromResult(op);
    }

    public Task PullNow<T>(CancellationToken cancelToken = default) where T : ISyncEntity => Task.CompletedTask;
    public Task PullAll(CancellationToken cancelToken = default) => Task.CompletedTask;
    public Task<IList<SyncOperation>> GetPending() => Task.FromResult<IList<SyncOperation>>(new List<SyncOperation>());
    public Task Cancel(string operationId) => Task.CompletedTask;
    public Task CancelAll() => Task.CompletedTask;
    public Task CancelAll<T>() where T : ISyncEntity => Task.CompletedTask;

#pragma warning disable CS0067
    public event EventHandler<int>? PendingCountChanged;
    public event EventHandler<SyncOperationResult>? UpdateReceived;
    public event EventHandler<SyncPullCompletion>? PullCompleted;
    public event EventHandler<SyncEvent>? Activity;
#pragma warning restore CS0067
}


/// <summary>A synced document type with a Guid id.</summary>
public sealed class TodoItem : ISyncEntity
{
    public Guid Id { get; set; }
    public string? Title { get; set; }
    public bool Completed { get; set; }

    public string Identifier => this.Id.ToString();
}


/// <summary>A synced document type with a string id.</summary>
public sealed class Note : ISyncEntity
{
    public string Id { get; set; } = "";
    public string? Text { get; set; }

    public string Identifier => this.Id;
}


/// <summary>A non-synced (local-only) type to prove the forwarder ignores it.</summary>
public sealed class LocalScratch
{
    public string Id { get; set; } = "";
    public string? Value { get; set; }
}


/// <summary>Spy interceptor used to prove inbound applies fire NO interceptor.</summary>
public sealed class SpyInterceptor : IDocumentInterceptor, IDocumentBulkInterceptor
{
    public int BeforeWrites;
    public int AfterWrites;
    public int BeforeBulk;
    public int AfterBulk;

    public Task BeforeWrite(DocumentWriteContext ctx, CancellationToken ct) { Interlocked.Increment(ref this.BeforeWrites); return Task.CompletedTask; }
    public Task AfterWrite(DocumentWriteContext ctx, CancellationToken ct) { Interlocked.Increment(ref this.AfterWrites); return Task.CompletedTask; }
    public Task BeforeBulkWrite(DocumentBulkContext ctx, CancellationToken ct) { Interlocked.Increment(ref this.BeforeBulk); return Task.CompletedTask; }
    public Task AfterBulkWrite(DocumentBulkContext ctx, CancellationToken ct) { Interlocked.Increment(ref this.AfterBulk); return Task.CompletedTask; }
}
