using System.Text.Json.Nodes;
using Shiny.DocumentDb;
using ShinyDocDbMyAdmin.Models;

namespace ShinyDocDbMyAdmin.Services;

/// <summary>
/// Soft delete, as far as an admin tool can honestly go with it.
/// </summary>
/// <remarks>
/// <para>
/// <c>AddSoftDelete</c> creates no column, table or index - it registers an interceptor that turns a
/// <c>Remove</c> into a flag write, and a named query filter that hides flagged documents from every read.
/// Both live in the application's process. Nothing about it reaches the database, so no amount of catalog
/// reading can discover it, and the schema sample can only ever <i>suggest</i> it
/// (<c>SoftDeleteCandidateKind</c>).
/// </para>
/// <para>
/// That left this tool violating the invariant it could not see: flagged documents appeared in the browser
/// mixed in with live ones with nothing to say which was which, and the delete button issued a real
/// <c>DELETE</c> - hard-deleting a document the application would only have flagged, with no warning. The
/// fix is a declaration on the connection (<c>ConnectionProfile.SoftDeleteFlags</c>): the operator states
/// what their application configured, and the tool then treats it as fact. Until they do,
/// <see cref="DocumentAdminService.DeleteDocuments"/> behaves exactly as it always has.
/// </para>
/// <para>
/// <c>Restore</c> / <c>PurgeDeleted</c> / <c>HardDelete</c> are library calls this tool cannot make - it
/// writes raw JSON over ADO and has no CLR type to bind to. Their equivalents here are ordinary document
/// writes over the declared path, which is why the declaration has to carry
/// <see cref="SoftDeleteFlagKind"/>: restoring a boolean writes <c>false</c>, restoring a timestamp writes
/// <c>null</c>.
/// </para>
/// </remarks>
public sealed partial class DocumentAdminService
{
    /// <summary>
    /// The soft-delete flag declared for a type on this connection, or null when none is. Null means the
    /// tool has no reason to believe the type uses soft delete - <b>not</b> that it does not.
    /// </summary>
    public async Task<SoftDeleteFlag?> GetSoftDeleteFlag(string profileId, string typeName, CancellationToken ct = default)
    {
        var connection = await this.Connect(profileId, ct);
        return connection.Profile.Profile.SoftDeleteFlags
            .FirstOrDefault(f => f.TypeName.Equals(typeName, StringComparison.Ordinal));
    }

    /// <summary>
    /// Flags documents as deleted instead of removing them - what the application's interceptor would have
    /// done. The flag is written as a document update, so temporal history records it as the version it is.
    /// </summary>
    /// <returns>How many documents were flagged. A document that no longer exists is skipped, not an error.</returns>
    /// <exception cref="InvalidOperationException">The type has no declared soft-delete flag.</exception>
    public Task<int> SoftDeleteDocuments(
        string profileId,
        string table,
        string typeName,
        IReadOnlyList<string> ids,
        CancellationToken ct = default)
        => this.WriteSoftDeleteFlag(profileId, table, typeName, ids, deleted: true, ct);

    /// <summary>
    /// Clears the declared flag - <c>false</c> for a boolean, <c>null</c> for a timestamp, matching
    /// <c>SoftDeleteMapping.RestoreValue</c>.
    /// </summary>
    /// <exception cref="InvalidOperationException">The type has no declared soft-delete flag.</exception>
    public Task<int> RestoreDocuments(
        string profileId,
        string table,
        string typeName,
        IReadOnlyList<string> ids,
        CancellationToken ct = default)
        => this.WriteSoftDeleteFlag(profileId, table, typeName, ids, deleted: false, ct);

    async Task<int> WriteSoftDeleteFlag(
        string profileId,
        string table,
        string typeName,
        IReadOnlyList<string> ids,
        bool deleted,
        CancellationToken ct)
    {
        if (ids.Count == 0)
            return 0;

        var flag = await this.GetSoftDeleteFlag(profileId, typeName, ct)
                   ?? throw new InvalidOperationException(
                       $"'{typeName}' has no declared soft-delete flag on this connection, so there is nothing to write. " +
                       "Declare one in the connection's settings, or from the Structure tab.");

        var now = DateTimeOffset.UtcNow;
        var written = 0;

        foreach (var id in ids)
        {
            var row = await this.GetDocument(profileId, table, typeName, id, ct);
            if (row?.Body is not JsonObject body)
                continue;

            ApplyFlag(body, flag, deleted, now);

            // Through SaveDocument rather than an UPDATE of its own: a flag write is a document write, and
            // the history sidecar, the vector sidecar and the encryption guard all have to see it as one.
            await this.SaveDocument(profileId, table, typeName, id, body.ToJsonString(Compact), isNew: false, ct: ct);
            written++;
        }

        logger.LogInformation(
            "{Action} {Count} {Type} document(s) in {Table} on declared flag '{Path}'",
            deleted ? "Soft-deleted" : "Restored", written, typeName, table, flag.PropertyPath);

        return written;
    }

    /// <summary>
    /// Writes the flag at its declared path, creating the intermediate objects a nested path needs. The
    /// values match <c>SoftDeleteMapping</c> exactly: <c>true</c> / <c>false</c> for a boolean, the current
    /// UTC instant / <c>null</c> for a timestamp.
    /// </summary>
    static void ApplyFlag(JsonObject body, SoftDeleteFlag flag, bool deleted, DateTimeOffset now)
    {
        var segments = flag.PropertyPath.Split('.', StringSplitOptions.RemoveEmptyEntries);
        if (segments.Length == 0)
            throw new InvalidOperationException("The declared soft-delete flag has no property path.");

        var target = body;
        for (var i = 0; i < segments.Length - 1; i++)
        {
            if (target[segments[i]] is JsonObject nested)
            {
                target = nested;
                continue;
            }

            var created = new JsonObject();
            target[segments[i]] = created;
            target = created;
        }

        target[segments[^1]] = (flag.FlagKind, deleted) switch
        {
            (SoftDeleteFlagKind.Boolean, _) => JsonValue.Create(deleted),
            (SoftDeleteFlagKind.Timestamp, true) => JsonValue.Create(now),
            _ => null
        };
    }

    /// <summary>
    /// Whether a row the grid already holds is flagged - read from the body it parsed, so badging every row
    /// in an "all" listing costs no queries.
    /// </summary>
    public static bool IsFlagged(DocumentRow row, SoftDeleteFlag flag)
    {
        var node = row.Read(flag.PropertyPath);

        return flag.FlagKind == SoftDeleteFlagKind.Timestamp
            ? node is JsonValue timestamp && timestamp.GetValueKind() != System.Text.Json.JsonValueKind.Null
            : node is JsonValue boolean && boolean.GetValueKind() == System.Text.Json.JsonValueKind.True;
    }

    /// <summary>
    /// The predicate that partitions a declared type into live and deleted documents, in the same shape the
    /// library's query filter uses: a boolean flag is deleted when it is true, a timestamp when it is set.
    /// A document with no flag at all is live, which is what an application that added the flag later has.
    /// </summary>
    internal static string SoftDeletePredicate(IDatabaseProvider provider, SoftDeleteFlag flag, bool deleted)
    {
        if (flag.FlagKind == SoftDeleteFlagKind.Timestamp)
            return provider.JsonNullCheck("Data", flag.PropertyPath, isNull: !deleted);

        var isTrue = provider.BoolCondition(provider.JsonExtractTyped("Data", flag.PropertyPath, typeof(bool)));

        return deleted
            ? isTrue
            : $"({provider.JsonNullCheck("Data", flag.PropertyPath, isNull: true)} OR NOT ({isTrue}))";
    }
}

/// <summary>
/// Which partition of a declared soft-delete type the Browse grid is showing. Only meaningful for a type
/// with a declared flag; an undeclared type is always <see cref="All"/> because there is nothing to split on.
/// </summary>
public enum DeletedFilter
{
    /// <summary>Live documents only - what the application's own reads see.</summary>
    Live,

    /// <summary>Flagged documents only - what the application has deleted.</summary>
    Deleted,

    /// <summary>Everything, flagged rows included and badged as such.</summary>
    All
}
