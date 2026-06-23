using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Shiny.Data.Sync;
using Shiny.DocumentDb.Sqlite;

namespace Shiny.DocumentDb.AppDataSync.Tests;


/// <summary>
/// Builds a SQLite-backed document store wired exactly as production does: the SyncWriteForwarder is a
/// DI-registered per-document + bulk interceptor, and a FakeDataSyncManager captures enqueues. Extra
/// interceptors can be injected to prove suppression.
/// </summary>
public sealed class SyncHarness : IDisposable
{
    public SyncHarness(Action<ISyncDocumentBuilder> configure, params IDocumentInterceptor[] extraInterceptors)
    {
        var registry = new SyncDocumentRegistry();
        BuildRegistry(registry, configure);

        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton<IDataSyncManager>(this.Manager);
        services.AddSingleton(registry);

        services.AddSingleton<SyncWriteForwarder>();
        services.AddSingleton<IDocumentInterceptor>(sp => sp.GetRequiredService<SyncWriteForwarder>());
        services.AddSingleton<IDocumentBulkInterceptor>(sp => sp.GetRequiredService<SyncWriteForwarder>());

        foreach (var i in extraInterceptors)
        {
            services.AddSingleton(i);
            if (i is IDocumentBulkInterceptor bulk)
                services.AddSingleton(bulk);
        }

        this.Options = new DocumentStoreOptions
        {
            DatabaseProvider = new SqliteDatabaseProvider("Data Source=:memory:"),
            TableName = "documents"
        };
        services.AddSingleton(this.Options);

        this.Provider = services.BuildServiceProvider();
        this.Registry = registry;
        this.Store = new DocumentStore(this.Options, this.Provider);
        this.Delegate = new DocumentStoreSyncDelegate(
            this.Store,
            registry,
            this.Provider.GetRequiredService<ILogger<DocumentStoreSyncDelegate>>()
        );
    }

    public FakeDataSyncManager Manager { get; } = new();
    public DocumentStoreOptions Options { get; }
    public ServiceProvider Provider { get; }
    public SyncDocumentRegistry Registry { get; }
    public DocumentStore Store { get; }
    public DocumentStoreSyncDelegate Delegate { get; }


    static void BuildRegistry(SyncDocumentRegistry registry, Action<ISyncDocumentBuilder> configure)
        => configure(new HarnessBuilder(registry));


    public void Dispose()
    {
        this.Store.Dispose();
        this.Provider.Dispose();
    }


    sealed class HarnessBuilder(SyncDocumentRegistry registry) : ISyncDocumentBuilder
    {
        [System.Diagnostics.CodeAnalysis.UnconditionalSuppressMessage("AOT", "IL2026", Justification = "Test harness; types are statically present.")]
        [System.Diagnostics.CodeAnalysis.UnconditionalSuppressMessage("AOT", "IL3050", Justification = "Test harness; types are statically present.")]
        [System.Diagnostics.CodeAnalysis.UnconditionalSuppressMessage("Trimming", "IL2091", Justification = "Test harness; types are statically present.")]
        public ISyncDocumentBuilder Sync<[System.Diagnostics.CodeAnalysis.DynamicallyAccessedMembers(System.Diagnostics.CodeAnalysis.DynamicallyAccessedMemberTypes.PublicProperties | System.Diagnostics.CodeAnalysis.DynamicallyAccessedMemberTypes.PublicParameterlessConstructor)] T>(Action<SyncDocumentOptions<T>>? configure = null) where T : class, ISyncEntity
        {
            var opts = new SyncDocumentOptions<T>();
            configure?.Invoke(opts);
            registry.Add(new SyncDocumentRegistration
            {
                DocumentType = typeof(T),
                OutboundKinds = opts.OutboundKinds,
                InboundMode = opts.InboundMode,
                UseMergePatchConflicts = opts.UseMergePatchConflicts,
                Queue = (m, verb, entity) => m.Queue(verb, (T)entity),
                BufferUpsert = (uow, entity) => uow.Upsert((T)entity),
                BufferUpdate = (uow, entity) => uow.Update((T)entity),
                BufferRemove = (uow, entity) =>
                {
                    var idGetter = SyncDeleteEntityFactory.CreateIdGetter<T>();
                    var id = idGetter(entity);
                    if (id != null)
                        uow.Remove<T>(id);
                },
                BuildDeleteEntity = SyncDeleteEntityFactory.Create<T>(),
                CreateProbe = () => Activator.CreateInstance<T>(),
                SerializeViaSync = probe => Shiny.Json.Default.Serialize((T)probe),
                SerializeViaStore = (probe, options) => System.Text.Json.JsonSerializer.Serialize((T)probe, options)
            });
            return this;
        }
    }
}
