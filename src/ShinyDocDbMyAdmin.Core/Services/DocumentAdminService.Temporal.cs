using System.Data.Common;
using ShinyDocDbMyAdmin.Models;
using Shiny.DocumentDb;

namespace ShinyDocDbMyAdmin.Services;

public sealed partial class DocumentAdminService
{
    /// <summary>How many recent versions the audit list pulls before you narrow it to a document.</summary>
    public const int RecentVersionLimit = 200;

    /// <summary>
    /// The actor stamped on versions this tool writes. The library takes its actor from the store's
    /// <c>CaptureActor</c>, which a tool poking at someone else's database has no access to - so an
    /// edit made here is attributed to the tool, which is the true answer.
    /// </summary>
    public const string HistoryActor = "shiny-docdb-myadmin";

    /// <summary>Operation names the library records, matched exactly so the two agree.</summary>
    internal const string OperationInserted = "Inserted";
    internal const string OperationUpdated = "Updated";
    internal const string OperationRemoved = "Removed";

    /// <summary>
    /// Appends a version to the history sidecar for a write this tool just made, using the provider's
    /// own history SQL.
    /// </summary>
    /// <remarks>
    /// <b>Why this exists at all.</b> Writes here go straight to SQL - <c>DocumentAdminService</c> sits
    /// below <c>IDocumentStore</c> because it has no CLR type to bind to - so none of the library's
    /// temporal tracking runs. Without this, editing a temporal-mapped document through the admin UI
    /// would change the row and leave the history sidecar insisting the old body is still current: the
    /// audit trail would be quietly wrong, which is worse than having no history tab at all.
    /// </remarks>
    static async Task RecordVersion(
        IDatabaseProvider provider,
        DbConnection db,
        DbTransaction? transaction,
        string safeTable,
        string typeName,
        string id,
        string? json,
        string operation,
        DateTimeOffset now,
        CancellationToken ct)
    {
        var timestamp = provider.NormalizeParameterValue(now);

        // Close the currently-open version first, exactly as DocumentStore does on every mutation.
        await using (var close = Ado.Command(db, provider.BuildHistoryCloseSql(safeTable)))
        {
            close.Transaction = transaction;
            Ado.Bind(close, "@id", id);
            Ado.Bind(close, "@typeName", typeName);
            Ado.Bind(close, "@validTo", timestamp);
            await close.ExecuteNonQueryAsync(ct);
        }

        await using var insert = Ado.Command(db, provider.BuildHistoryInsertSql(safeTable));
        insert.Transaction = transaction;
        Ado.Bind(insert, "@id", id);
        Ado.Bind(insert, "@typeName", typeName);
        Ado.Bind(insert, "@validFrom", timestamp);
        Ado.Bind(insert, "@operation", operation);
        Ado.Bind(insert, "@actor", HistoryActor);
        Ado.Bind(insert, "@data", json);
        await insert.ExecuteNonQueryAsync(ct);
    }

    /// <summary>True when the table has a <c>{table}_history</c> sidecar to read.</summary>
    public async Task<bool> HasHistorySidecar(string profileId, string table, CancellationToken ct = default)
    {
        var connection = await this.Connect(profileId, ct);
        var sidecar = connection.Provider.HistoryTableName(Ado.SafeIdentifier(table));
        var tables = await this.ListTables(profileId, refresh: false, ct);

        return tables.Any(t => t.Name.Equals(sidecar, StringComparison.OrdinalIgnoreCase));
    }

    /// <summary>
    /// Every version of one document, oldest first - the library's own history query, so the tool reads
    /// exactly what <c>ITemporalDocumentStore.History&lt;T&gt;</c> would return.
    /// </summary>
    public async Task<IReadOnlyList<DocumentVersion>> ListVersions(
        string profileId,
        string table,
        string typeName,
        string documentId,
        CancellationToken ct = default)
    {
        var connection = await this.Connect(profileId, ct);
        var sql = connection.Provider.BuildHistorySelectSql(Ado.SafeIdentifier(table));

        return await connection.Execute(async (db, token) =>
        {
            await using var cmd = Ado.Command(db, sql);
            Ado.Bind(cmd, "@id", documentId);
            Ado.Bind(cmd, "@typeName", typeName);

            return await ReadVersions(cmd, token);
        }, ct);
    }

    /// <summary>
    /// The most recent versions across every document of a type - an audit log, and the way into the
    /// tab when you do not already know which document you are looking for.
    /// </summary>
    /// <param name="actor">Optional: only versions recorded by this actor (needs CaptureActor).</param>
    public async Task<IReadOnlyList<DocumentVersion>> ListRecentVersions(
        string profileId,
        string table,
        string typeName,
        string? actor = null,
        int limit = RecentVersionLimit,
        CancellationToken ct = default)
    {
        var connection = await this.Connect(profileId, ct);
        var provider = connection.Provider;
        var sidecar = provider.QuoteTable(provider.HistoryTableName(Ado.SafeIdentifier(table)));
        var actorFilter = string.IsNullOrWhiteSpace(actor) ? "" : " AND Actor = @actor";

        // The library's own by-actor / between queries are time-bounded; "newest first, capped" is a
        // browsing concern, so it is built here off the same columns and the provider's paging clause.
        var sql = $"SELECT Id, Version, ValidFrom, ValidTo, Operation, Actor, Data FROM {sidecar} " +
                  $"WHERE TypeName = @typeName{actorFilter} " +
                  $"ORDER BY ValidFrom DESC, Id ASC, Version DESC {provider.BuildPaginationClause(0, limit)}";

        return await connection.Execute(async (db, token) =>
        {
            await using var cmd = Ado.Command(db, sql);
            Ado.Bind(cmd, "@typeName", typeName);
            if (!string.IsNullOrWhiteSpace(actor))
                Ado.Bind(cmd, "@actor", actor);

            return await ReadVersions(cmd, token);
        }, ct);
    }

    /// <summary>Compares two versions of a document, oldest side on the left regardless of argument order.</summary>
    public async Task<VersionComparison> CompareVersions(
        string profileId,
        string table,
        string typeName,
        string documentId,
        long leftVersion,
        long rightVersion,
        CancellationToken ct = default)
    {
        var versions = await this.ListVersions(profileId, table, typeName, documentId, ct);

        var left = Find(versions, leftVersion, documentId);
        var right = Find(versions, rightVersion, documentId);

        if (left.Version > right.Version)
            (left, right) = (right, left);

        return new VersionComparison(left, right, JsonComparer.Compare(left.Json, right.Json));
    }

    /// <summary>
    /// Writes a prior version's body back as the current document, which appends a fresh version -
    /// restoring moves history forward, it does not rewrite it.
    /// </summary>
    /// <returns>The version that was restored.</returns>
    public async Task<DocumentVersion> RestoreVersion(
        string profileId,
        string table,
        string typeName,
        string documentId,
        long version,
        CancellationToken ct = default)
    {
        var connection = await this.Connect(profileId, ct);
        connection.AssertWritable();

        var versions = await this.ListVersions(profileId, table, typeName, documentId, ct);
        var target = Find(versions, version, documentId);

        if (target.Json is null)
            throw new InvalidOperationException(
                $"Version {version} of '{documentId}' is a delete tombstone - there is no body to restore. " +
                "Restore an earlier version instead.");

        // Whether this is an insert or an update depends on whether the document exists *now*, not on
        // what the version being restored did: restoring over a deleted document has to re-insert it.
        var current = await this.GetDocument(profileId, table, typeName, documentId, ct);
        await this.SaveDocument(profileId, table, typeName, documentId, target.Json, isNew: current is null, ct: ct);

        logger.LogInformation(
            "Restored {Type}/{Id} in {Table} to version {Version}", typeName, documentId, table, version);

        return target;
    }

    static DocumentVersion Find(IReadOnlyList<DocumentVersion> versions, long version, string documentId)
        => versions.FirstOrDefault(x => x.Version == version)
           ?? throw new InvalidOperationException($"'{documentId}' has no version {version}.");

    /// <summary>
    /// Reads the seven columns every history query in the library yields, in their documented order:
    /// Id, Version, ValidFrom, ValidTo, Operation, Actor, Data.
    /// </summary>
    static async Task<IReadOnlyList<DocumentVersion>> ReadVersions(DbCommand cmd, CancellationToken ct)
    {
        await using var reader = await cmd.ExecuteReaderAsync(ct);

        var results = new List<DocumentVersion>();
        while (await reader.ReadAsync(ct))
        {
            results.Add(new DocumentVersion(
                Ado.Text(reader, 0),
                Ado.Count(reader.GetValue(1)),
                Ado.Timestamp(reader, 2),
                Ado.Timestamp(reader, 3),
                Ado.NullableText(reader, 4) ?? "Unknown",
                Ado.NullableText(reader, 5),
                Ado.NullableText(reader, 6)));
        }

        return results;
    }
}
