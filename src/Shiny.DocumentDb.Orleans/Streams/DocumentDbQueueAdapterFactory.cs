using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using global::Orleans.Configuration;
using global::Orleans.Providers.Streams.Common;
using global::Orleans.Serialization;
using global::Orleans.Streams;

namespace Shiny.DocumentDb.Orleans;

/// <summary>
/// Builds the DocumentDb stream provider's adapter, cache, queue mapper and failure handler, and enforces the
/// backend gate before a silo finishes starting.
/// </summary>
public sealed class DocumentDbQueueAdapterFactory : IQueueAdapterFactory
{
    const string DefaultTable = "orleans_streams";

    readonly string name;
    readonly DocumentDbStreamOptions options;
    readonly IDocumentStore store;
    readonly HashRingBasedStreamQueueMapper queueMapper;
    readonly IQueueAdapterCache cache;
    readonly Serializer<DocumentDbBatchContainer> serializer;
    readonly ILoggerFactory loggerFactory;
    readonly TimeProvider clock;
    readonly string serviceId;
    readonly IStreamFailureHandler failureHandler = new NoOpStreamDeliveryFailureHandler();

    public DocumentDbQueueAdapterFactory(
        string name,
        DocumentDbStreamOptions options,
        IServiceProvider services)
    {
        ArgumentNullException.ThrowIfNull(options);
        options.Validate();

        this.name = name;
        this.options = options;
        this.serviceId = services.GetRequiredService<IOptions<ClusterOptions>>().Value.ServiceId;
        this.loggerFactory = services.GetRequiredService<ILoggerFactory>();
        this.serializer = services.GetRequiredService<Serializer<DocumentDbBatchContainer>>();
        this.clock = services.GetService<TimeProvider>() ?? TimeProvider.System;

        this.store = OrleansDocumentStore.Build(options, services, DefaultTable, (dso, _) =>
        {
            // All three document types share the one table, discriminated by TypeName — so none of them sets
            // cfg.Table. That property is a per-type table *override* and is exclusive: pointing a second type
            // at the same name throws. DocumentStoreOptions.TableName, which Build already set, is what puts
            // them in the right table.
            dso.ConfigureDocument<StreamSequenceDocument>(cfg => cfg.MapVersionProperty(x => x.Version));

            // The checkpoint deliberately has no version property. There is exactly one pulling agent per
            // queue, so there is nothing to race with, and optimistic concurrency here would only produce
            // spurious conflicts on a row whose whole purpose is to be overwritten.
        });

        EnsureSupportedBackend(this.store);

        this.queueMapper = new HashRingBasedStreamQueueMapper(
            new HashRingStreamQueueMapperOptions { TotalQueueCount = options.TotalQueueCount },
            name);

        // Not SimpleQueueAdapterCache: that one answers a too-old token with a cache miss, which is right
        // when a queue sits behind it and wrong when a table does. See DocumentDbQueueCache.
        this.cache = new DocumentDbQueueAdapterCache(
            new SimpleQueueCacheOptions().CacheSize,
            this.store,
            options,
            this.serializer,
            this.loggerFactory);
    }

    /// <summary>
    /// The backend gate, expressed as a capability rather than a list of provider names: this provider is
    /// correct only where a locking read takes a real row lock, because that lock is what keeps sequence order
    /// and commit order the same. SQLite, DuckDB and the document-native stores fail it, which is the intended
    /// answer — a local file cannot back a multi-silo cluster's queues, and a pay-per-request store would be
    /// charged for every idle poll forever.
    /// </summary>
    static void EnsureSupportedBackend(IDocumentStore store)
    {
        if (!store.SupportsTransactions)
            throw new NotSupportedException(
                "DocumentDb streams require a backend with real multi-document transactions. " +
                "Use PostgreSQL, SQL Server, MySQL, MariaDB, Oracle or CockroachDB.");

        if (!store.SupportsPessimisticLocking)
            throw new NotSupportedException(
                "DocumentDb streams require a backend with row-level pessimistic locking (SELECT … FOR UPDATE / " +
                "UPDLOCK, HOLDLOCK), which the sequence counter depends on for correctness — without it, two " +
                "concurrent producers can commit out of sequence order and an event is silently never delivered. " +
                "Use PostgreSQL, SQL Server, MySQL, MariaDB, Oracle or CockroachDB.");
    }

    /// <summary>The store this provider reads and writes. Exposed so <see cref="IStreamAdmin"/> can share
    /// it rather than opening a second connection pool against the same table.</summary>
    internal IDocumentStore Store => this.store;

    internal string ServiceId => this.serviceId;

    public static DocumentDbQueueAdapterFactory Create(IServiceProvider services, string name)
    {
        var options = services.GetOptionsByName<DocumentDbStreamOptions>(name);
        return new DocumentDbQueueAdapterFactory(name, options, services);
    }

    public async Task<IQueueAdapter> CreateAdapter()
    {
        // One change-feed subscription per silo, shared by every receiver — see StreamChangeNudge.
        var nudge = this.options.UseChangeFeedNudge
            ? await StreamChangeNudge.Attach(this.store, this.loggerFactory.CreateLogger<StreamChangeNudge>()).ConfigureAwait(false)
            : null;

        var adapter = new DocumentDbQueueAdapter(
            this.name,
            this.serviceId,
            this.store,
            this.options,
            this.queueMapper,
            this.serializer,
            nudge,
            this.loggerFactory,
            this.clock);

        await this.EnsureIndex().ConfigureAwait(false);
        await this.SeedCounters(adapter).ConfigureAwait(false);
        return adapter;
    }

    /// <summary>
    /// Creates the composite <c>(QueueId, Seq)</c> index the receiver's range read depends on.
    /// </summary>
    /// <remarks>
    /// This is the single biggest performance decision in the provider. Every pulling agent runs
    /// <c>WHERE QueueId = @q AND Seq &gt; @cursor ORDER BY Seq</c> on every poll; without the index each of
    /// those is a full scan with a JSON extract per row, on a table whose whole job is to be hot. Index
    /// creation is idempotent, so running it on every silo start is fine.
    /// </remarks>
    async Task EnsureIndex()
    {
        // CreateIndexAsync is a DocumentStore concern rather than an IDocumentStore one, so a caller who
        // supplied their own store through StoreFactory is responsible for their own indexing.
        if (this.store is not DocumentStore relational)
        {
            this.loggerFactory.CreateLogger<DocumentDbQueueAdapterFactory>().LogWarning(
                "Stream store was supplied through StoreFactory, so the (QueueId, Seq) index was not created automatically. Create it yourself or every poll will scan the table.");
            return;
        }

        // The receiver's hot path: every poll of every queue.
        await relational
            .CreateIndexAsync(OrleansSystemJsonContext.Default.StreamEventDocument, e => e.QueueId, e => e.Seq)
            .ConfigureAwait(false);

        // Rewind's path: replay one stream's history without reading (and deserializing) the whole queue.
        // A second index on a write-hot table is a real cost, paid deliberately — without it a rewind scans
        // every batch on the queue and discards the ones belonging to other streams.
        await relational
            .CreateIndexAsync(OrleansSystemJsonContext.Default.StreamEventDocument, e => e.QueueId, e => e.StreamId, e => e.Seq)
            .ConfigureAwait(false);
    }

    /// <summary>
    /// Creates the counter row for every queue up front.
    /// </summary>
    /// <remarks>
    /// A locking read cannot lock a row that does not exist, so seeding on first enqueue would leave exactly
    /// the race this design exists to close — two producers both find nothing, both insert, and one loses. The
    /// seed itself races harmlessly across silos: whoever loses the insert finds the row already there, which
    /// is the outcome either way.
    /// </remarks>
    async Task SeedCounters(DocumentDbQueueAdapter adapter)
    {
        foreach (var queueId in this.queueMapper.GetAllQueues())
        {
            var id = adapter.CounterId(queueId);
            if (await this.store.Get<StreamSequenceDocument>(id).ConfigureAwait(false) != null)
                continue;

            try
            {
                await this.store.Insert(new StreamSequenceDocument { Id = id, Next = 1 }).ConfigureAwait(false);
            }
            catch (Exception ex) when (ex is InvalidOperationException or ConcurrencyException)
            {
                // Another silo seeded it first — the row exists, which is all this method promises.
            }
        }
    }

    public IQueueAdapterCache GetQueueAdapterCache() => this.cache;

    public IStreamQueueMapper GetStreamQueueMapper() => this.queueMapper;

    public Task<IStreamFailureHandler> GetDeliveryFailureHandler(QueueId queueId)
        => Task.FromResult(this.failureHandler);
}
