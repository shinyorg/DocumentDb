using System.Data;
using ShinyDocDbMyAdmin.Models;

namespace ShinyDocDbMyAdmin.Services;

public sealed partial class DocumentAdminService
{
    /// <summary>True when the table has a <c>{table}_blobs</c> sidecar to read.</summary>
    public async Task<bool> HasBlobSidecar(string profileId, string table, CancellationToken ct = default)
    {
        var connection = await this.Connect(profileId, ct);
        var sidecar = connection.Provider.BlobTableName(Ado.SafeIdentifier(table));
        var tables = await this.ListTables(profileId, refresh: false, ct);
        return tables.Any(t => t.Name.Equals(sidecar, StringComparison.OrdinalIgnoreCase));
    }

    /// <summary>
    /// Lists blob metadata for a type, optionally narrowed to one document. The payload column is
    /// deliberately not selected - a listing should never pull megabytes per row.
    /// </summary>
    public async Task<IReadOnlyList<BlobInfo>> ListBlobs(
        string profileId,
        string table,
        string typeName,
        string? documentId = null,
        CancellationToken ct = default)
    {
        var connection = await this.Connect(profileId, ct);
        var sidecar = connection.Provider.QuoteTable(connection.Provider.BlobTableName(Ado.SafeIdentifier(table)));
        var filter = documentId is null ? "" : " AND Id = @id";

        return await connection.Execute(async (db, token) =>
        {
            await using var cmd = Ado.Command(db,
                $"SELECT Id, BlobKey, FileName, ContentType, Length, Hash, CreatedAt, UpdatedAt " +
                $"FROM {sidecar} WHERE TypeName = @typeName{filter} ORDER BY Id, BlobKey");

            Ado.Bind(cmd, "@typeName", typeName);
            if (documentId is not null)
                Ado.Bind(cmd, "@id", documentId);

            await using var reader = await cmd.ExecuteReaderAsync(token);
            var results = new List<BlobInfo>();
            while (await reader.ReadAsync(token))
            {
                results.Add(new BlobInfo(
                    Ado.Text(reader, 0),
                    Ado.Text(reader, 1),
                    Ado.NullableText(reader, 2),
                    Ado.NullableText(reader, 3),
                    Ado.Count(reader.GetValue(4)),
                    Ado.NullableText(reader, 5),
                    Ado.Timestamp(reader, 6),
                    Ado.Timestamp(reader, 7)));
            }

            return (IReadOnlyList<BlobInfo>)results;
        }, ct);
    }

    /// <summary>Reads one payload with its metadata. Returns null when the blob is gone.</summary>
    public async Task<BlobPayload?> ReadBlob(
        string profileId,
        string table,
        string typeName,
        string documentId,
        string blobKey,
        CancellationToken ct = default)
    {
        var connection = await this.Connect(profileId, ct);
        var provider = connection.Provider;
        var sidecar = provider.QuoteTable(provider.BlobTableName(Ado.SafeIdentifier(table)));

        return await connection.Execute(async (db, token) =>
        {
            await using var cmd = Ado.Command(db,
                $"SELECT Data, FileName, ContentType, Length, Hash, CreatedAt, UpdatedAt " +
                $"FROM {sidecar} WHERE Id = @id AND TypeName = @typeName AND BlobKey = @blobKey");

            Ado.Bind(cmd, "@id", documentId);
            Ado.Bind(cmd, "@typeName", typeName);
            Ado.Bind(cmd, "@blobKey", blobKey);

            await using var reader = await cmd.ExecuteReaderAsync(CommandBehavior.SequentialAccess, token);
            if (!await reader.ReadAsync(token))
                return null;

            var data = reader.IsDBNull(0) ? [] : (byte[])reader.GetValue(0);
            var info = new BlobInfo(
                documentId,
                blobKey,
                Ado.NullableText(reader, 1),
                Ado.NullableText(reader, 2),
                Ado.Count(reader.GetValue(3)),
                Ado.NullableText(reader, 4),
                Ado.Timestamp(reader, 5),
                Ado.Timestamp(reader, 6));

            return new BlobPayload(info, data);
        }, ct);
    }

    public async Task<bool> DeleteBlob(
        string profileId,
        string table,
        string typeName,
        string documentId,
        string blobKey,
        CancellationToken ct = default)
    {
        var connection = await this.Connect(profileId, ct);
        connection.AssertWritable();

        var provider = connection.Provider;
        var safeTable = Ado.SafeIdentifier(table);

        var affected = await connection.Execute(async (db, token) =>
        {
            await using var cmd = Ado.Command(db, provider.BuildBlobDeleteSql(safeTable));
            Ado.Bind(cmd, "@id", documentId);
            Ado.Bind(cmd, "@typeName", typeName);
            Ado.Bind(cmd, "@blobKey", blobKey);
            return await cmd.ExecuteNonQueryAsync(token);
        }, ct);

        if (affected > 0)
            logger.LogInformation("Deleted blob {Key} from {Type}/{Id}", blobKey, typeName, documentId);

        return affected > 0;
    }
}
