using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace Shiny.DocumentDb.Diagnostics;

/// <summary>
/// Shared instrumentation core used by <see cref="InstrumentedDocumentStore"/> and the query/transaction
/// wrappers: starts a span, times the call, records the duration/count/row metrics, and stamps span +
/// metric on failure. Holds the resolved <c>db.system.name</c> so every operation from one store —
/// including the fluent query terminals and the operations performed inside a transaction callback —
/// reports the same backend and nests under the right parent span.
/// </summary>
public sealed class OperationTracker(string system, string? storeName = null)
{
    // Re-entrancy guard. Embedded instrumentation fires at the public method boundary, but the store re-enters
    // itself internally — a single Insert routes through the implicit one-op unit of work (RunUnitAsync →
    // TransactionalDocumentStore.Insert), a temporal Restore calls Update, sidecars run within a write. Without
    // this, one logical operation would emit nested/duplicate spans and double-count the metrics. Only the
    // OUTERMOST tracked operation on an async flow emits; nested ones run span-free. Streaming (TrackStream)
    // reads the guard to skip when nested, but does NOT set it, so operations the consumer performs *during*
    // enumeration still record their own spans.
    static readonly AsyncLocal<bool> active = new();

    public string System => system;

    /// <summary>db.system.name for a relational provider (e.g. SqliteDatabaseProvider → "sqlite").</summary>
    public static string SystemName(IDatabaseProvider provider)
    {
        var name = provider.GetType().Name;
        if (name.EndsWith("DatabaseProvider", StringComparison.Ordinal))
            name = name[..^"DatabaseProvider".Length];
        return name.ToLowerInvariant();
    }

    public async Task Track(string op, string collection, Func<Task> action)
    {
        if (active.Value)
        {
            await action().ConfigureAwait(false);
            return;
        }
        active.Value = true;
        using var activity = DocumentStoreMetrics.StartActivity(system, op, collection, storeName);
        var start = Stopwatch.GetTimestamp();
        try
        {
            await action().ConfigureAwait(false);
            DocumentStoreMetrics.Record(system, op, collection, Stopwatch.GetElapsedTime(start), "success", null, null, storeName);
        }
        catch (Exception ex)
        {
            this.Fail(activity, start, op, collection, ex);
            throw;
        }
        finally
        {
            active.Value = false;
        }
    }

    public async Task<TResult> Track<TResult>(string op, string collection, Func<Task<TResult>> action, Func<TResult, long?>? rows = null)
    {
        if (active.Value)
            return await action().ConfigureAwait(false);
        active.Value = true;
        using var activity = DocumentStoreMetrics.StartActivity(system, op, collection, storeName);
        var start = Stopwatch.GetTimestamp();
        try
        {
            var result = await action().ConfigureAwait(false);
            DocumentStoreMetrics.Record(system, op, collection, Stopwatch.GetElapsedTime(start), "success", null, rows?.Invoke(result), storeName);
            return result;
        }
        catch (Exception ex)
        {
            this.Fail(activity, start, op, collection, ex);
            throw;
        }
        finally
        {
            active.Value = false;
        }
    }

    public IAsyncEnumerable<T> TrackStream<T>(string op, string collection, IAsyncEnumerable<T> source, CancellationToken cancellationToken)
        // Nested inside a tracked operation → enumerate span-free. Otherwise record the stream, without setting
        // the re-entrancy guard, so the consumer's per-item store calls still get their own spans.
        => active.Value ? source : this.TrackStreamCore(op, collection, source, cancellationToken);

    async IAsyncEnumerable<T> TrackStreamCore<T>(string op, string collection, IAsyncEnumerable<T> source, [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        using var activity = DocumentStoreMetrics.StartActivity(system, op, collection, storeName);
        var start = Stopwatch.GetTimestamp();
        long count = 0;
        var faulted = false;
        var enumerator = source.GetAsyncEnumerator(cancellationToken);
        try
        {
            while (true)
            {
                try
                {
                    if (!await enumerator.MoveNextAsync().ConfigureAwait(false))
                        break;
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    // The consumer stopped enumerating by cancelling the token — normal termination, not an
                    // error. Record it as "cancelled" so it isn't counted against the error rate.
                    faulted = true;
                    DocumentStoreMetrics.Record(system, op, collection, Stopwatch.GetElapsedTime(start), "cancelled", null, count, storeName);
                    throw;
                }
                catch (Exception ex)
                {
                    faulted = true;
                    this.Fail(activity, start, op, collection, ex);
                    throw;
                }
                count++;
                yield return enumerator.Current;
            }
        }
        finally
        {
            await enumerator.DisposeAsync().ConfigureAwait(false);
            if (!faulted)
                DocumentStoreMetrics.Record(system, op, collection, Stopwatch.GetElapsedTime(start), "success", null, count, storeName);
        }
    }

    void Fail(Activity? activity, long start, string op, string collection, Exception ex)
    {
        if (activity is { IsAllDataRequested: true })
        {
            activity.SetStatus(ActivityStatusCode.Error, ex.Message);
            activity.AddException(ex);
        }
        DocumentStoreMetrics.Record(system, op, collection, Stopwatch.GetElapsedTime(start), "error", ex.GetType().FullName, null, storeName);
    }
}
