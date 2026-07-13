using System.Text.Json.Serialization.Metadata;
using StackExchange.Redis;

namespace Shiny.DocumentDb.Redis;

// Native, cross-connection change feed backed by Redis keyspace notifications. Unlike the in-process
// IObservableDocumentStore (this instance's own writes), this observes the underlying data — including writes
// by other processes. It is best-effort: keyspace notifications are not durable, require the server config
// (enabled best-effort at subscribe time), and cannot distinguish an insert from an update, so a write event
// is surfaced as Updated. For a durable feed, mirror changes into a Redis Stream.
public partial class RedisDocumentStore : IChangeFeedDocumentStore
{
    /// <inheritdoc />
    public async Task<IAsyncDisposable> SubscribeChanges<T>(
        Func<DocumentChange<T>, CancellationToken, Task> onChange,
        CancellationToken cancellationToken = default) where T : class
    {
        ArgumentNullException.ThrowIfNull(onChange);
        await this.EnsureModulesAsync(cancellationToken).ConfigureAwait(false);

        var typeName = this.ResolveTypeName<T>();
        var typeInfo = this.FindTypeInfo<T>(null);
        var dbNum = this.db.Database < 0 ? 0 : this.db.Database;

        // Best-effort: enable keyspace + keyevent notifications. A managed Redis may forbid CONFIG SET —
        // if so the caller is responsible for enabling notify-keyspace-events server-side.
        try
        {
            await this.GetServer().ConfigSetAsync("notify-keyspace-events", "KEA").ConfigureAwait(false);
        }
        catch (RedisException)
        {
            this.Log("Redis could not set notify-keyspace-events (managed server?) — enable it for the change feed.");
        }

        var sub = this.mux.GetSubscriber();
        var channel = RedisChannel.Pattern($"__keyspace@{dbNum}__:{this.keyPrefix}doc:{typeName}:*");
        var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);

        void Handler(RedisChannel chan, RedisValue message)
        {
            var chName = chan.ToString();
            var marker = chName.IndexOf("__:", StringComparison.Ordinal);
            if (marker >= 0)
            {
                var key = chName[(marker + 3)..];
                _ = this.DispatchKeyspaceAsync(key, message.ToString(), typeInfo, onChange, cts.Token);
            }
        }

        await sub.SubscribeAsync(channel, Handler).ConfigureAwait(false);
        return new KeyspaceSubscription(sub, channel, Handler, cts);
    }

    async Task DispatchKeyspaceAsync<T>(
        string key,
        string evt,
        JsonTypeInfo<T>? typeInfo,
        Func<DocumentChange<T>, CancellationToken, Task> onChange,
        CancellationToken ct) where T : class
    {
        try
        {
            var id = key[(key.LastIndexOf(':') + 1)..];
            DocumentChange<T> change;
            if (evt.Contains("del", StringComparison.OrdinalIgnoreCase) || evt.Equals("expired", StringComparison.OrdinalIgnoreCase))
            {
                change = new DocumentChange<T> { ChangeType = DocumentChangeType.Removed, Id = id, Document = null };
            }
            else
            {
                var env = await this.GetEnvelopeAsync(key).ConfigureAwait(false);
                var dataJson = env == null ? null : RedisDocument.GetDataJson(env);
                var doc = dataJson == null ? null : Deserialize(dataJson, typeInfo, this.JsonOptions);
                change = new DocumentChange<T> { ChangeType = DocumentChangeType.Updated, Id = id, Document = doc };
            }
            await onChange(change, ct).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            // subscription cancelled
        }
        catch (Exception ex)
        {
            this.Log($"Redis change-feed dispatch error: {ex.Message}");
        }
    }

    sealed class KeyspaceSubscription : IAsyncDisposable
    {
        readonly ISubscriber sub;
        readonly RedisChannel channel;
        readonly Action<RedisChannel, RedisValue> handler;
        readonly CancellationTokenSource cts;

        public KeyspaceSubscription(ISubscriber sub, RedisChannel channel, Action<RedisChannel, RedisValue> handler, CancellationTokenSource cts)
        {
            this.sub = sub;
            this.channel = channel;
            this.handler = handler;
            this.cts = cts;
        }

        public async ValueTask DisposeAsync()
        {
            await this.cts.CancelAsync().ConfigureAwait(false);
            try
            {
                await this.sub.UnsubscribeAsync(this.channel, this.handler).ConfigureAwait(false);
            }
            catch (RedisException)
            {
                // connection already gone
            }
            this.cts.Dispose();
        }
    }
}
