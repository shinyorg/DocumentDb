using Microsoft.Extensions.Logging;
using global::Orleans.Serialization;
using global::Orleans.Streams;

namespace Shiny.DocumentDb.Orleans;

/// <summary>
/// Creates one <see cref="DocumentDbQueueCache"/> per queue — the rewind-capable cache that replaces
/// <c>SimpleQueueAdapterCache</c>.
/// </summary>
sealed class DocumentDbQueueAdapterCache : IQueueAdapterCache
{
    readonly int cacheSize;
    readonly IDocumentStore store;
    readonly DocumentDbStreamOptions options;
    readonly Serializer<DocumentDbBatchContainer> serializer;
    readonly ILoggerFactory loggerFactory;

    public DocumentDbQueueAdapterCache(
        int cacheSize,
        IDocumentStore store,
        DocumentDbStreamOptions options,
        Serializer<DocumentDbBatchContainer> serializer,
        ILoggerFactory loggerFactory)
    {
        this.cacheSize = cacheSize;
        this.store = store;
        this.options = options;
        this.serializer = serializer;
        this.loggerFactory = loggerFactory;
    }

    public IQueueCache CreateQueueCache(QueueId queueId)
        => new DocumentDbQueueCache(
            queueId,
            this.cacheSize,
            this.store,
            this.options,
            this.serializer,
            this.loggerFactory.CreateLogger($"Shiny.DocumentDb.Orleans.Cache.{queueId}"));
}
