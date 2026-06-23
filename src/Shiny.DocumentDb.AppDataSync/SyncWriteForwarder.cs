using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Shiny.Data.Sync;

namespace Shiny.DocumentDb.AppDataSync;


/// <summary>
/// The outbound half of the integration. Stateless forwarder: on each per-document write to a synced
/// type it maps the <see cref="DocumentOperation"/> to a <see cref="SyncVerb"/> and calls
/// <see cref="IDataSyncManager.Queue{T}"/>, persisting nothing of its own. On set-based writes it
/// throws <see cref="SyncBulkWriteNotSupportedException"/> for synced types.
/// <para>
/// Inbound applies run under <c>SaveChanges(suppressInterceptors: true)</c>, so the core pipeline
/// builds no context and never invokes this forwarder — that is the loop guard. Temporal-driven
/// writes are likewise skipped.
/// </para>
/// </summary>
public sealed class SyncWriteForwarder(
    SyncDocumentRegistry registry,
    IServiceProvider services,
    ILogger<SyncWriteForwarder> logger
) : IDocumentInterceptor, IDocumentBulkInterceptor
{
    // Lazily resolved to avoid a hard DI ordering dependency between the store and the sync manager.
    IDataSyncManager? manager;
    IDataSyncManager Manager => this.manager ??= services.GetRequiredService<IDataSyncManager>();


    public Task BeforeWrite(DocumentWriteContext ctx, CancellationToken ct) => Task.CompletedTask;


    public async Task AfterWrite(DocumentWriteContext ctx, CancellationToken ct)
    {
        // Mirrored / internal writes never reach here under suppression, but guard temporal anyway.
        if (ctx.Source != DocumentOperationSource.Direct)
            return;

        var registration = registry.Get(ctx.DocumentType);
        if (registration == null)
            return;

        if (!TryMapVerb(ctx.Operation, out var verb, out var kind))
            return;

        if ((registration.OutboundKinds & kind) == 0)
            return;

        // Insert/Update/Upsert carry a materialized document; Delete-by-id does not — rebuild a minimal
        // entity from the canonical string id so Queue<T> can read its Identifier (payload is null for deletes).
        var entity = ctx.Document;
        if (entity == null)
        {
            if (verb != SyncVerb.Delete)
                return;

            var id = SyncIdentityBridge.FromWriteContext(ctx);
            if (id == null)
                return;

            entity = registration.BuildDeleteEntity(id);
            if (entity == null)
            {
                logger.LogWarning(
                    "Sync delete on '{Type}' could not rebuild an entity for id '{Id}'; the outbox entry was skipped.",
                    ctx.DocumentType.FullName, id
                );
                return;
            }
        }

        try
        {
            await registration.Queue(this.Manager, verb, entity).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            // Eventual-consistency: a failed enqueue must not roll back a committed local write.
            // A reconciliation pull re-converges the local store with the server.
            logger.LogError(
                ex,
                "Failed to enqueue sync operation {Verb} for '{Type}' id '{Id}'",
                verb, ctx.DocumentType.FullName, SyncIdentityBridge.FromWriteContext(ctx)
            );
        }
    }


    public Task BeforeBulkWrite(DocumentBulkContext ctx, CancellationToken ct)
    {
        if (ctx.Source == DocumentOperationSource.Direct && registry.IsSynced(ctx.DocumentType))
            throw new SyncBulkWriteNotSupportedException(ctx.DocumentType, ctx.Operation);

        return Task.CompletedTask;
    }


    public Task AfterBulkWrite(DocumentBulkContext ctx, CancellationToken ct) => Task.CompletedTask;


    static bool TryMapVerb(DocumentOperation op, out SyncVerb verb, out SyncWriteKinds kind)
    {
        switch (op)
        {
            case DocumentOperation.Insert:
                verb = SyncVerb.Create;
                kind = SyncWriteKinds.Insert;
                return true;

            case DocumentOperation.Update:
            case DocumentOperation.Upsert:
                verb = SyncVerb.Update;
                kind = SyncWriteKinds.Update;
                return true;

            case DocumentOperation.Delete:
                verb = SyncVerb.Delete;
                kind = SyncWriteKinds.Delete;
                return true;

            default:
                verb = default;
                kind = SyncWriteKinds.None;
                return false;
        }
    }
}
