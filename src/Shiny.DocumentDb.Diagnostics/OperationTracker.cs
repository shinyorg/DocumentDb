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
sealed class OperationTracker(DocumentStoreMetrics metrics, string system)
{
    public string System => system;

    public async Task Track(string op, string collection, Func<Task> action)
    {
        using var activity = DocumentStoreMetrics.StartActivity(system, op, collection);
        var start = Stopwatch.GetTimestamp();
        try
        {
            await action().ConfigureAwait(false);
            metrics.Record(system, op, collection, Stopwatch.GetElapsedTime(start), "success", null, null);
        }
        catch (Exception ex)
        {
            this.Fail(activity, start, op, collection, ex);
            throw;
        }
    }

    public async Task<TResult> Track<TResult>(string op, string collection, Func<Task<TResult>> action, Func<TResult, long?>? rows = null)
    {
        using var activity = DocumentStoreMetrics.StartActivity(system, op, collection);
        var start = Stopwatch.GetTimestamp();
        try
        {
            var result = await action().ConfigureAwait(false);
            metrics.Record(system, op, collection, Stopwatch.GetElapsedTime(start), "success", null, rows?.Invoke(result));
            return result;
        }
        catch (Exception ex)
        {
            this.Fail(activity, start, op, collection, ex);
            throw;
        }
    }

    public async IAsyncEnumerable<T> TrackStream<T>(string op, string collection, IAsyncEnumerable<T> source, [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        using var activity = DocumentStoreMetrics.StartActivity(system, op, collection);
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
                metrics.Record(system, op, collection, Stopwatch.GetElapsedTime(start), "success", null, count);
        }
    }

    void Fail(Activity? activity, long start, string op, string collection, Exception ex)
    {
        if (activity is { IsAllDataRequested: true })
        {
            activity.SetStatus(ActivityStatusCode.Error, ex.Message);
            activity.AddException(ex);
        }
        metrics.Record(system, op, collection, Stopwatch.GetElapsedTime(start), "error", ex.GetType().FullName, null);
    }
}
