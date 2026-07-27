using System.Text.Json;
using System.Text.Json.Nodes;
using ShinyDocDbMyAdmin.Models;

namespace ShinyDocDbMyAdmin.Services;

public sealed partial class DocumentAdminService
{
    // Relaxed escaping so the editor shows `+` in a timestamp rather than `+`. The default
    // encoder's extra escapes are for embedding JSON in HTML/JS, which is not what happens here -
    // Blazor escapes on render, and the value is identical once parsed either way.
    static readonly JsonSerializerOptions Readable = new()
    {
        WriteIndented = true,
        Encoder = System.Text.Encodings.Web.JavaScriptEncoder.UnsafeRelaxedJsonEscaping
    };

    static readonly JsonSerializerOptions Compact = new()
    {
        Encoder = System.Text.Encodings.Web.JavaScriptEncoder.UnsafeRelaxedJsonEscaping
    };

    public async Task<DocumentRow?> GetDocument(string profileId, string table, string typeName, string id, CancellationToken ct = default)
    {
        var connection = await this.Connect(profileId, ct);
        var quoted = connection.Provider.QuoteTable(Ado.SafeIdentifier(table));

        return await connection.Execute(async (db, token) =>
        {
            await using var cmd = Ado.Command(db, $"SELECT {EnvelopeColumns} FROM {quoted} WHERE TypeName = @typeName AND Id = @id");
            Ado.Bind(cmd, "@typeName", typeName);
            Ado.Bind(cmd, "@id", id);

            await using var reader = await cmd.ExecuteReaderAsync(token);
            return await reader.ReadAsync(token) ? ReadRow(reader, includeTenant: false) : null;
        }, ct);
    }

    /// <summary>
    /// Writes a document. The body's own <c>id</c> property is aligned with the envelope Id, because
    /// DocumentDb mirrors the Id into the JSON and a mismatch makes LINQ predicates on Id miss.
    /// </summary>
    public async Task SaveDocument(
        string profileId,
        string table,
        string typeName,
        string id,
        string json,
        bool isNew,
        CancellationToken ct = default)
    {
        var connection = await this.Connect(profileId, ct);
        connection.AssertWritable();

        if (string.IsNullOrWhiteSpace(id))
            throw new ArgumentException("A document id is required.", nameof(id));

        var normalized = AlignIdIntoBody(json, id);
        var provider = connection.Provider;
        var safeTable = Ado.SafeIdentifier(table);
        var now = DateTimeOffset.UtcNow;

        // A temporal-mapped type records every mutation. This write bypasses the library, so the
        // version has to be appended here or the sidecar starts disagreeing with the row.
        var hasHistory = await this.HasHistorySidecar(profileId, table, ct);

        await connection.Execute(async (db, token) =>
        {
            await using var transaction = hasHistory ? await db.BeginTransactionAsync(token) : null;

            var sql = isNew ? provider.BuildInsertSql(safeTable) : provider.BuildUpdateSql(safeTable);
            await using (var cmd = Ado.Command(db, sql))
            {
                cmd.Transaction = transaction;
                Ado.Bind(cmd, "@id", id);
                Ado.Bind(cmd, "@typeName", typeName);
                Ado.Bind(cmd, "@data", normalized);
                Ado.Bind(cmd, "@now", provider.NormalizeParameterValue(now));

                var affected = await cmd.ExecuteNonQueryAsync(token);
                if (!isNew && affected == 0)
                    throw new InvalidOperationException($"No '{typeName}' document with id '{id}' exists any more.");
            }

            if (transaction is not null)
            {
                await RecordVersion(
                    provider, db, transaction, safeTable, typeName, id, normalized,
                    isNew ? OperationInserted : OperationUpdated, now, token);

                await transaction.CommitAsync(token);
            }
        }, ct);

        logger.LogInformation("{Action} {Type}/{Id} in {Table}", isNew ? "Inserted" : "Updated", typeName, id, table);
    }

    public async Task<int> DeleteDocuments(
        string profileId,
        string table,
        string typeName,
        IReadOnlyList<string> ids,
        CancellationToken ct = default)
    {
        if (ids.Count == 0)
            return 0;

        var connection = await this.Connect(profileId, ct);
        connection.AssertWritable();

        var provider = connection.Provider;
        var safeTable = Ado.SafeIdentifier(table);

        // Blobs live in a sidecar with no foreign key, so orphans are ours to prevent.
        var tables = await this.ListTables(profileId, refresh: false, ct);
        var hasBlobs = tables.Any(t => t.Name.Equals(provider.BlobTableName(safeTable), StringComparison.OrdinalIgnoreCase));
        var hasHistory = tables.Any(t => t.Name.Equals(provider.HistoryTableName(safeTable), StringComparison.OrdinalIgnoreCase));
        var now = DateTimeOffset.UtcNow;

        var deleted = await connection.Execute(async (db, token) =>
        {
            await using var transaction = await db.BeginTransactionAsync(token);

            if (hasBlobs)
            {
                foreach (var id in ids)
                {
                    await using var blobCmd = Ado.Command(db, provider.BuildBlobDeleteAllSql(safeTable));
                    blobCmd.Transaction = transaction;
                    Ado.Bind(blobCmd, "@id", id);
                    Ado.Bind(blobCmd, "@typeName", typeName);
                    await blobCmd.ExecuteNonQueryAsync(token);
                }
            }

            await using var cmd = Ado.Command(db, provider.BuildBatchDeleteByIdsSql(safeTable, ids.Count));
            cmd.Transaction = transaction;
            Ado.Bind(cmd, "@typeName", typeName);
            for (var i = 0; i < ids.Count; i++)
                Ado.Bind(cmd, $"@id_{i}", ids[i]);

            var affected = await cmd.ExecuteNonQueryAsync(token);

            // A delete is a version too: a Removed tombstone with no body, as the library records it.
            if (hasHistory)
            {
                foreach (var id in ids)
                {
                    await RecordVersion(
                        provider, db, transaction, safeTable, typeName, id,
                        json: null, OperationRemoved, now, token);
                }
            }

            await transaction.CommitAsync(token);
            return affected;
        }, ct);

        logger.LogInformation("Deleted {Count} {Type} document(s) from {Table}", deleted, typeName, table);
        return deleted;
    }

    /// <summary>
    /// Removes every document of a type. The nuclear button, so it reports what it did.
    /// </summary>
    /// <remarks>
    /// Deliberately does not write history, matching the library: <c>Clear&lt;T&gt;</c> is a bulk delete
    /// and is the one mutation temporal tracking skips.
    /// </remarks>
    public async Task<int> ClearType(string profileId, string table, string typeName, CancellationToken ct = default)
    {
        var connection = await this.Connect(profileId, ct);
        connection.AssertWritable();

        var quoted = connection.Provider.QuoteTable(Ado.SafeIdentifier(table));
        var deleted = await connection.Execute(async (db, token) =>
        {
            await using var cmd = Ado.Command(db, $"DELETE FROM {quoted} WHERE TypeName = @typeName");
            Ado.Bind(cmd, "@typeName", typeName);
            return await cmd.ExecuteNonQueryAsync(token);
        }, ct);

        logger.LogWarning("Cleared {Count} {Type} document(s) from {Table}", deleted, typeName, table);
        return deleted;
    }

    /// <summary>Pretty-prints for the editor, leaving unparseable text exactly as stored.</summary>
    public static string Prettify(string json)
    {
        try
        {
            var node = JsonNode.Parse(json);
            return node?.ToJsonString(Readable) ?? json;
        }
        catch (JsonException)
        {
            return json;
        }
    }

    public static string Minify(string json)
    {
        var node = JsonNode.Parse(json) ?? throw new JsonException("Document is empty.");
        return node.ToJsonString(Compact);
    }

    /// <summary>A blank body for the insert form, seeded with the id so the mirror invariant holds from the start.</summary>
    public static string NewDocumentTemplate(string id) => Prettify($$"""{"id": {{JsonSerializer.Serialize(id)}}}""");

    static string AlignIdIntoBody(string json, string id)
    {
        var node = JsonNode.Parse(json) ?? throw new JsonException("Document is empty.");
        if (node is not JsonObject obj)
            // Arrays and scalars are legal JSON but not legal documents - there is nowhere to mirror the id.
            throw new JsonException("A document must be a JSON object.");

        // Match whichever casing the document already uses; default to the library's camelCase output.
        var key = obj.ContainsKey("Id") ? "Id" : "id";

        // The envelope value wins, but its JSON *type* does not: an int-keyed document stores its id as
        // a number, and rewriting that to a string would stop LINQ predicates on Id from matching.
        var wasNumber = obj.TryGetPropertyValue(key, out var existing)
                        && existing is JsonValue value
                        && value.GetValueKind() == System.Text.Json.JsonValueKind.Number;

        obj[key] = wasNumber && long.TryParse(id, out var numeric)
            ? JsonValue.Create(numeric)
            : JsonValue.Create(id);

        return obj.ToJsonString(Compact);
    }
}
