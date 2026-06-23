using Shiny.Data.Sync;

namespace Shiny.DocumentDb.AppDataSync;


/// <summary>
/// Bridges DocumentDb's id model to Shiny.Data.Sync's string <see cref="ISyncEntity.Identifier"/>.
/// <para>
/// DocumentDb already stores every id as a string (via its configured <c>IdConverters</c>), and the
/// per-document interceptor exposes that exact string on <c>DocumentWriteContext.Id</c>. Synced types
/// implement <see cref="ISyncEntity"/>, so their <see cref="ISyncEntity.Identifier"/> is the
/// server-recognized identifier. This bridge resolves the identifier for either side and lets tests
/// assert the two agree.
/// </para>
/// </summary>
public static class SyncIdentityBridge
{
    /// <summary>Resolves the sync identifier from a populated <see cref="DocumentWriteContext"/> (outbound path).</summary>
    public static string? FromWriteContext(DocumentWriteContext ctx)
    {
        ArgumentNullException.ThrowIfNull(ctx);

        // The entity, when present, is authoritative (it is what Queue serializes + reads Identifier from).
        if (ctx.Document is ISyncEntity entity)
            return entity.Identifier;

        // Delete-by-id: the store has already converted the id to its canonical string form on ctx.Id.
        return ctx.Id?.ToString();
    }

    /// <summary>Resolves the sync identifier for an entity (inbound path).</summary>
    public static string FromEntity(ISyncEntity entity)
    {
        ArgumentNullException.ThrowIfNull(entity);
        return entity.Identifier;
    }
}
