namespace Shiny.DocumentDb.AppDataSync;


/// <summary>Which local write operations are forwarded to the Shiny.Data.Sync outbox.</summary>
[Flags]
public enum SyncWriteKinds
{
    None = 0,
    Insert = 1,
    Update = 2,
    Delete = 4,
    All = Insert | Update | Delete
}


/// <summary>How inbound (pulled) items are applied to the local document store.</summary>
public enum InboundApplyMode
{
    /// <summary>Apply Create/Update via <c>Upsert</c> (RFC 7396 merge). Default.</summary>
    Upsert,

    /// <summary>Apply Create/Update via <c>Update</c> (full-document replace).</summary>
    Update
}


/// <summary>Per-type sync configuration declared via <see cref="ISyncDocumentBuilder.Sync{T}"/>.</summary>
public sealed class SyncDocumentOptions<T> where T : class
{
    /// <summary>Which local write operations are forwarded to <c>Queue</c> (default: all).</summary>
    public SyncWriteKinds OutboundKinds { get; set; } = SyncWriteKinds.All;

    /// <summary>Apply inbound items into the store with Upsert (merge) vs Update (replace). Default Upsert.</summary>
    public InboundApplyMode InboundMode { get; set; } = InboundApplyMode.Upsert;

    /// <summary>
    /// Opt into the field-level merge-patch conflict resolver for this type. When enabled the supplied
    /// sync delegate produces an RFC 7396 merged payload for <c>ConflictResolution.UseMerged(...)</c>
    /// instead of delegating to the app's <c>OnConflict</c>.
    /// </summary>
    public bool UseMergePatchConflicts { get; set; }
}
