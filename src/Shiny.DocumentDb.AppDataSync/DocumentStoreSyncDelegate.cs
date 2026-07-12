using System.Text.Json.Nodes;
using Microsoft.Extensions.Logging;
using Shiny.Data.Sync;

namespace Shiny.DocumentDb.AppDataSync;


/// <summary>
/// The inbound half of the integration. Implements <see cref="IDataSyncDelegate"/> and applies each
/// pulled item back into the document store: Create/Update become an <c>Upsert</c> (or <c>Update</c>),
/// Delete becomes a <c>Remove</c>. All applies run through a <see cref="UnitOfWork"/> committed with
/// <c>SaveChanges(suppressInterceptors: true)</c>, so no interceptor (including the write forwarder)
/// fires on mirrored data — the loop guard.
/// <para>
/// Decorates an optional app-supplied <see cref="IDataSyncDelegate"/> so apps still observe
/// <c>OnSent</c>/<c>OnError</c> and can resolve their own conflicts when merge-patch is not enabled.
/// </para>
/// </summary>
public sealed class DocumentStoreSyncDelegate(
    IDocumentStore store,
    SyncDocumentRegistry registry,
    ILogger<DocumentStoreSyncDelegate> logger,
    IDataSyncDelegate? inner = null
) : IDataSyncDelegate
{
    public async Task OnReceived(SyncReceivedItem item)
    {
        ArgumentNullException.ThrowIfNull(item);

        var registration = registry.Get(item.EntityType);
        if (registration == null)
        {
            // Not a store-backed type — pass through to the app delegate if present.
            if (inner != null)
                await inner.OnReceived(item).ConfigureAwait(false);
            return;
        }

        if (item.Entity == null)
        {
            logger.LogWarning(
                "Inbound item for '{Type}' had no deserialized entity; skipping store apply.",
                item.EntityType.FullName
            );
            if (inner != null)
                await inner.OnReceived(item).ConfigureAwait(false);
            return;
        }

        await using var uow = store.OpenSession();
        switch (item.Verb)
        {
            case SyncVerb.Create:
            case SyncVerb.Update:
                if (registration.InboundMode == InboundApplyMode.Update)
                    registration.BufferUpdate(uow, item.Entity);
                else
                    registration.BufferUpsert(uow, item.Entity);
                break;

            case SyncVerb.Delete:
                registration.BufferRemove(uow, item.Entity);
                break;
        }

        // Suppressed: no interceptor runs, so this apply does NOT echo back to the outbox.
        await uow.SaveChanges(suppressInterceptors: true).ConfigureAwait(false);

        if (inner != null)
            await inner.OnReceived(item).ConfigureAwait(false);
    }


    public Task OnSent(SyncOperation operation, string? responseBody)
        => inner?.OnSent(operation, responseBody) ?? Task.CompletedTask;


    public Task OnError(SyncOperation operation, int statusCode, Exception ex)
        => inner?.OnError(operation, statusCode, ex) ?? Task.CompletedTask;


    public async Task<ConflictResolution> OnConflict(SyncOperation operation, string remotePayload)
    {
        var registration = FindByEndpointKey(operation.EndpointKey);
        if (registration is { UseMergePatchConflicts: true } && operation.Payload != null)
        {
            // RFC 7396 merge: take the remote state as the base and overlay the local pending changes.
            var merged = MergePatch(remotePayload, operation.Payload);
            return ConflictResolution.UseMerged(merged);
        }

        if (inner != null)
            return await inner.OnConflict(operation, remotePayload).ConfigureAwait(false);

        // No app delegate and no merge resolver — fall back to accepting the server state.
        return ConflictResolution.AcceptRemote;
    }


    SyncDocumentRegistration? FindByEndpointKey(string endpointKey)
    {
        // Sync's endpoint key defaults to the entity type's FullName.
        foreach (var r in registry.All)
        {
            if (r.DocumentType.FullName == endpointKey)
                return r;
        }
        return null;
    }


    /// <summary>RFC 7396 JSON Merge Patch — applies <paramref name="patchJson"/> onto <paramref name="targetJson"/>.</summary>
    internal static string MergePatch(string targetJson, string patchJson)
    {
        var target = JsonNode.Parse(targetJson);
        var patch = JsonNode.Parse(patchJson);
        var result = ApplyMergePatch(target, patch);
        return result?.ToJsonString() ?? "null";
    }


    static JsonNode? ApplyMergePatch(JsonNode? target, JsonNode? patch)
    {
        if (patch is not JsonObject patchObj)
            return patch?.DeepClone();

        var targetObj = target as JsonObject ?? new JsonObject();
        var result = (JsonObject)targetObj.DeepClone();

        foreach (var kvp in patchObj)
        {
            if (kvp.Value == null)
            {
                result.Remove(kvp.Key);
            }
            else
            {
                result[kvp.Key] = ApplyMergePatch(result.TryGetPropertyValue(kvp.Key, out var existing) ? existing : null, kvp.Value);
            }
        }
        return result;
    }
}
