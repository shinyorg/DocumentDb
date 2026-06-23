using System.Text.Json;
using Shiny.Data.Sync;
using Shiny;

namespace Shiny.DocumentDb.AppDataSync;


/// <summary>
/// One synced document type's wiring. Holds the per-type options plus typed delegates captured at
/// <see cref="ISyncDocumentBuilder.Sync{T}"/> time (where the type is concrete and known to implement
/// <see cref="ISyncEntity"/>), so the stateless forwarder / inbound delegate can act on the type
/// without runtime generic construction on the hot path.
/// </summary>
public sealed class SyncDocumentRegistration
{
    public required Type DocumentType { get; init; }
    public required SyncWriteKinds OutboundKinds { get; init; }
    public required InboundApplyMode InboundMode { get; init; }
    public required bool UseMergePatchConflicts { get; init; }

    /// <summary>Calls <c>IDataSyncManager.Queue&lt;T&gt;(verb, (T)entity)</c> typed.</summary>
    public required Func<IDataSyncManager, SyncVerb, object, Task> Queue { get; init; }

    /// <summary>Buffers an Upsert of <c>(T)entity</c> into the supplied unit of work.</summary>
    public required Action<UnitOfWork, object> BufferUpsert { get; init; }

    /// <summary>Buffers an Update of <c>(T)entity</c> into the supplied unit of work.</summary>
    public required Action<UnitOfWork, object> BufferUpdate { get; init; }

    /// <summary>Buffers a Remove of an entity (reading its strongly-typed id) into the unit of work.</summary>
    public required Action<UnitOfWork, object> BufferRemove { get; init; }

    /// <summary>
    /// Builds an entity instance carrying just the given identifier, so a delete-by-id write can be
    /// queued (<c>Queue&lt;T&gt;</c> reads <see cref="ISyncEntity.Identifier"/>). Returns null when the
    /// identifier can't be applied to the type's id property.
    /// </summary>
    public required Func<string, ISyncEntity?> BuildDeleteEntity { get; init; }

    /// <summary>Creates a probe instance for serializer-equivalence validation, or null if unavailable.</summary>
    public required Func<object?> CreateProbe { get; init; }

    /// <summary>Serializes a probe through Shiny.Data.Sync's <c>Json.Default</c> serializer.</summary>
    public required Func<object, string> SerializeViaSync { get; init; }

    /// <summary>Serializes a probe through the supplied DocumentDb <see cref="JsonSerializerOptions"/>.</summary>
    public required Func<object, JsonSerializerOptions, string> SerializeViaStore { get; init; }
}


/// <summary>
/// The set of document types wired for sync. Registered as a singleton; consumed by the write
/// forwarder and the inbound delegate.
/// </summary>
public sealed class SyncDocumentRegistry
{
    readonly Dictionary<Type, SyncDocumentRegistration> byType = new();

    internal void Add(SyncDocumentRegistration registration)
    {
        if (!this.byType.TryAdd(registration.DocumentType, registration))
            throw new InvalidOperationException(
                $"Document type '{registration.DocumentType.FullName}' is already registered for sync."
            );
    }

    public bool IsSynced(Type documentType) => this.byType.ContainsKey(documentType);

    public SyncDocumentRegistration? Get(Type documentType)
        => this.byType.TryGetValue(documentType, out var r) ? r : null;

    public IReadOnlyCollection<SyncDocumentRegistration> All => this.byType.Values;
}
