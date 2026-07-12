using System.Diagnostics.CodeAnalysis;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Shiny.Data.Sync;

namespace Shiny.DocumentDb.AppDataSync;


/// <summary>
/// Declares which document types participate in sync. Each type must already have a registered
/// Shiny.Data.Sync endpoint (via <c>AddDataSync(... RegisterEndpoint&lt;T&gt; ...)</c>).
/// </summary>
public interface ISyncDocumentBuilder
{
    /// <summary>
    /// Bridges a DocumentDb type to its registered Shiny.Data.Sync endpoint. Local writes to the type
    /// are auto-enqueued to the outbox; pulled changes are auto-applied to the store.
    /// </summary>
    ISyncDocumentBuilder Sync<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties | DynamicallyAccessedMemberTypes.PublicParameterlessConstructor)] T>(
        Action<SyncDocumentOptions<T>>? configure = null
    ) where T : class, ISyncEntity;
}


public static class SyncDocumentStoreExtensions
{
    /// <summary>
    /// Wires the outbound write-forwarder and the inbound apply delegate onto an existing
    /// <c>AddDocumentStore</c> + <c>AddDataSync</c> registration, then lets you declare which document
    /// types participate via <see cref="ISyncDocumentBuilder.Sync{T}"/>.
    /// </summary>
    public static IServiceCollection SyncDocumentStore(
        this IServiceCollection services,
        Action<ISyncDocumentBuilder> configure
    )
    {
        ArgumentNullException.ThrowIfNull(configure);

        var registry = new SyncDocumentRegistry();
        configure(new SyncDocumentBuilder(registry));
        services.AddSingleton(registry);

        // Outbound: the forwarder is a per-document + bulk interceptor resolved by the store's pipeline.
        services.AddSingleton<SyncWriteForwarder>();
        services.AddSingleton<IDocumentInterceptor>(sp => sp.GetRequiredService<SyncWriteForwarder>());
        services.AddSingleton<IDocumentBulkInterceptor>(sp => sp.GetRequiredService<SyncWriteForwarder>());

        // Inbound: decorate any app-supplied IDataSyncDelegate. AddDataSync registers the app delegate
        // as IDataSyncDelegate; we replace that registration with our decorator wrapping the original.
        var appDelegateDescriptor = services.LastOrDefault(d => d.ServiceType == typeof(IDataSyncDelegate));
        services.AddSingleton<IDataSyncDelegate>(sp =>
        {
            IDataSyncDelegate? inner = null;
            if (appDelegateDescriptor != null)
                inner = (IDataSyncDelegate?)Instantiate(sp, appDelegateDescriptor);

            return ActivatorUtilities.CreateInstance<DocumentStoreSyncDelegate>(
                sp,
                sp.GetRequiredService<IDocumentStore>(),
                sp.GetRequiredService<SyncDocumentRegistry>(),
                inner!
            );
        });

        // Startup: enforce the shared-serializer contract (Hard edge #5).
        services.AddHostedService<SyncSerializerValidator>();
        return services;
    }


    static object? Instantiate(IServiceProvider sp, ServiceDescriptor descriptor)
    {
        if (descriptor.ImplementationInstance != null)
            return descriptor.ImplementationInstance;
        if (descriptor.ImplementationFactory != null)
            return descriptor.ImplementationFactory(sp);
        if (descriptor.ImplementationType != null)
            return ActivatorUtilities.CreateInstance(sp, descriptor.ImplementationType);
        return null;
    }


    sealed class SyncDocumentBuilder(SyncDocumentRegistry registry) : ISyncDocumentBuilder
    {
        [UnconditionalSuppressMessage("AOT", "IL2026:RequiresUnreferencedCode", Justification = "The serializer-equivalence probe path is annotated with DynamicallyAccessedMembers on T and only runs at startup validation; types are user-constructed.")]
        [UnconditionalSuppressMessage("AOT", "IL3050:RequiresDynamicCode", Justification = "The serializer-equivalence probe path is annotated with DynamicallyAccessedMembers on T and only runs at startup validation; types are user-constructed.")]
        public ISyncDocumentBuilder Sync<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties | DynamicallyAccessedMemberTypes.PublicParameterlessConstructor)] T>(
            Action<SyncDocumentOptions<T>>? configure = null
        ) where T : class, ISyncEntity
        {
            var opts = new SyncDocumentOptions<T>();
            configure?.Invoke(opts);

            registry.Add(new SyncDocumentRegistration
            {
                DocumentType = typeof(T),
                OutboundKinds = opts.OutboundKinds,
                InboundMode = opts.InboundMode,
                UseMergePatchConflicts = opts.UseMergePatchConflicts,
                Queue = static (manager, verb, entity) => manager.Queue(verb, (T)entity),
                BufferUpsert = static (uow, entity) => uow.Upsert((T)entity),
                BufferUpdate = static (uow, entity) => uow.Update((T)entity),
                BufferRemove = BuildBufferRemove<T>(),
                BuildDeleteEntity = SyncDeleteEntityFactory.Create<T>(),
                CreateProbe = static () =>
                {
                    try { return Activator.CreateInstance<T>(); }
                    catch { return null; }
                },
                SerializeViaSync = static probe => Shiny.Json.Default.Serialize((T)probe),
                SerializeViaStore = static (probe, options) => JsonSerializer.Serialize((T)probe, options)
            });
            return this;
        }

        static Action<IDocumentSession, object> BuildBufferRemove<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)] T>() where T : class, ISyncEntity
        {
            var idGetter = SyncDeleteEntityFactory.CreateIdGetter<T>();
            return (uow, entity) =>
            {
                var id = idGetter(entity);
                if (id != null)
                    uow.Remove<T>(id);
            };
        }
    }
}
