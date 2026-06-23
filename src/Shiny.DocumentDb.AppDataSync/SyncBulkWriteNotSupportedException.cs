namespace Shiny.DocumentDb.AppDataSync;


/// <summary>
/// Thrown when a set-based write (<c>ExecuteUpdate</c>, <c>ExecuteDelete</c>, or per-type
/// <c>Clear&lt;T&gt;</c>) is attempted against a sync-registered document type.
/// <para>
/// Set-based writes never materialize the affected documents, so there are no ids/payloads to
/// enqueue into the Shiny.Data.Sync outbox — propagating them would silently desync the server.
/// Use per-document writes (<c>Update</c>/<c>Upsert</c>/<c>Remove</c>) so each change is queued, or
/// use <c>IDocumentMaintenance.ClearAll</c> for a whole-store local wipe (which fires no interceptor
/// and therefore enqueues nothing).
/// </para>
/// </summary>
public sealed class SyncBulkWriteNotSupportedException : InvalidOperationException
{
    public SyncBulkWriteNotSupportedException(Type documentType, DocumentOperation operation)
        : base(
            $"Set-based write '{operation}' is not supported on sync-registered type " +
            $"'{documentType.FullName}'. Set-based writes bypass per-document change tracking, so they " +
            "cannot be queued to the sync outbox. Use per-document Update/Upsert/Remove, or ClearAll for " +
            "a whole-store local reset."
        )
    {
        this.DocumentType = documentType;
        this.Operation = operation;
    }

    public Type DocumentType { get; }
    public DocumentOperation Operation { get; }
}
