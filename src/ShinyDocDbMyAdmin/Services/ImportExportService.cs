using System.Data.Common;
using System.Globalization;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using ShinyDocDbMyAdmin.Models;

namespace ShinyDocDbMyAdmin.Services;

public enum ExportFormat
{
    /// <summary>One JSON array of document bodies.</summary>
    Json,

    /// <summary>One document body per line - the format the importer reads back most cheaply.</summary>
    Ndjson,

    /// <summary>Flattened scalar columns, for spreadsheets.</summary>
    Csv,

    /// <summary>Envelope and body together, so a round-trip preserves ids and timestamps.</summary>
    JsonEnvelope
}

public enum ImportMode
{
    /// <summary>Fail on a duplicate id.</summary>
    Insert,

    /// <summary>Overwrite the stored body on a duplicate id.</summary>
    Replace,

    /// <summary>Leave existing documents alone.</summary>
    SkipExisting
}

public sealed record ImportResult(int Read, int Written, int Skipped, IReadOnlyList<string> Errors);

/// <summary>Bulk movement of documents in and out of a type.</summary>
public sealed class ImportExportService(DocumentAdminService admin, DemoMode demo, ILogger<ImportExportService> logger)
{
    const int BatchSize = 200;

    // ── Export ──────────────────────────────────────────────────────────

    public static string ContentType(ExportFormat format) => format switch
    {
        ExportFormat.Csv => "text/csv",
        ExportFormat.Ndjson => "application/x-ndjson",
        _ => "application/json"
    };

    public static string FileExtension(ExportFormat format) => format switch
    {
        ExportFormat.Csv => "csv",
        ExportFormat.Ndjson => "ndjson",
        _ => "json"
    };

    /// <summary>Streams a type straight to the response so an export is never held in memory.</summary>
    public async Task Export(
        string profileId,
        string table,
        string typeName,
        ExportFormat format,
        BrowseQuery query,
        Stream destination,
        CancellationToken ct = default)
    {
        await using var writer = new StreamWriter(destination, new UTF8Encoding(false), leaveOpen: true);

        if (format == ExportFormat.Csv)
        {
            await this.ExportCsv(profileId, table, typeName, query, writer, ct);
            return;
        }

        var array = format is ExportFormat.Json or ExportFormat.JsonEnvelope;
        var first = true;

        if (array)
            await writer.WriteAsync('[');

        await foreach (var row in admin.StreamAll(profileId, table, typeName, query, ct))
        {
            var payload = format == ExportFormat.JsonEnvelope ? Envelope(row) : row.Json;

            if (array)
            {
                if (!first)
                    await writer.WriteAsync(',');

                await writer.WriteLineAsync();
                await writer.WriteAsync("  ");
                await writer.WriteAsync(payload);
            }
            else
            {
                await writer.WriteLineAsync(payload);
            }

            first = false;
        }

        if (array)
        {
            if (!first)
                await writer.WriteLineAsync();

            await writer.WriteAsync(']');
        }

        await writer.FlushAsync(ct);
    }

    static string Envelope(DocumentRow row)
    {
        var envelope = new JsonObject
        {
            ["id"] = row.Id,
            ["typeName"] = row.TypeName,
            ["createdAt"] = row.CreatedAt?.ToString("O", CultureInfo.InvariantCulture),
            ["updatedAt"] = row.UpdatedAt?.ToString("O", CultureInfo.InvariantCulture),
            ["data"] = row.Body?.DeepClone() ?? JsonValue.Create(row.Json)
        };
        return envelope.ToJsonString();
    }

    async Task ExportCsv(string profileId, string table, string typeName, BrowseQuery query, TextWriter writer, CancellationToken ct)
    {
        // The column set has to be fixed before the first row goes out, so it comes from the same
        // sampled schema the browse grid uses rather than from the rows being streamed.
        var schema = await admin.InferSchema(profileId, table, typeName, ct: ct);
        var paths = schema.Fields
            .Where(f => !f.Types.Contains("object") && !f.Types.Contains("array"))
            .Select(f => f.Path)
            .ToList();

        await writer.WriteLineAsync(string.Join(",", new[] { "Id", "CreatedAt", "UpdatedAt" }.Concat(paths).Select(Csv)));

        await foreach (var row in admin.StreamAll(profileId, table, typeName, query, ct))
        {
            var cells = new List<string>(paths.Count + 3)
            {
                Csv(row.Id),
                Csv(row.CreatedAt?.ToString("O", CultureInfo.InvariantCulture)),
                Csv(row.UpdatedAt?.ToString("O", CultureInfo.InvariantCulture))
            };

            foreach (var path in paths)
            {
                var node = row.Read(path);
                cells.Add(Csv(node switch
                {
                    null => null,
                    JsonValue v when v.GetValueKind() == JsonValueKind.String => v.GetValue<string>(),
                    _ => node.ToJsonString()
                }));
            }

            await writer.WriteLineAsync(string.Join(",", cells));
        }
    }

    static string Csv(string? value)
    {
        if (string.IsNullOrEmpty(value))
            return "";

        return value.Any(c => c is ',' or '"' or '\n' or '\r')
            ? '"' + value.Replace("\"", "\"\"") + '"'
            : value;
    }

    // ── Import ──────────────────────────────────────────────────────────

    /// <summary>
    /// Reads a JSON array or NDJSON stream into a type. Accepts both a bare document body and the
    /// envelope form produced by <see cref="ExportFormat.JsonEnvelope"/>, so an export round-trips.
    /// </summary>
    public async Task<ImportResult> Import(
        string profileId,
        string table,
        string typeName,
        Stream source,
        ImportMode mode,
        CancellationToken ct = default)
    {
        // Before the connection check, not after: a demo instance refuses on principle rather than
        // because a particular connection happens to be read-only. Export has no such guard.
        demo.AssertCanImportData();

        var connection = await admin.Connect(profileId, ct);
        connection.AssertWritable();

        var documents = await Parse(source, ct);
        if (documents.Count == 0)
            return new ImportResult(0, 0, 0, ["No documents found in the file."]);

        var provider = connection.Provider;
        var safeTable = Ado.SafeIdentifier(table);
        var errors = new List<string>();
        var written = 0;

        // Replace/SkipExisting have a single-round-trip form on most providers; where they do not,
        // fall back to a per-document loop inside the same transaction.
        var bulkCapable = mode == ImportMode.Insert || provider.SupportsBulkReplace;

        foreach (var batch in documents.Chunk(BatchSize))
        {
            try
            {
                written += await connection.Execute(async (db, token) =>
                    bulkCapable
                        ? await WriteBatch(db, provider, safeTable, typeName, batch, mode, token)
                        : await WriteIndividually(db, provider, safeTable, typeName, batch, mode, token),
                    ct);
            }
            catch (DbException ex)
            {
                errors.Add($"Batch starting at '{batch[0].Id}' failed: {ex.Message}");
            }
        }

        logger.LogInformation("Imported {Written}/{Read} {Type} document(s) into {Table} ({Mode})",
            written, documents.Count, typeName, table, mode);

        return new ImportResult(documents.Count, written, documents.Count - written, errors);
    }

    static async Task<int> WriteBatch(
        DbConnection db,
        Shiny.DocumentDb.IDatabaseProvider provider,
        string table,
        string typeName,
        IReadOnlyList<ParsedDocument> batch,
        ImportMode mode,
        CancellationToken ct)
    {
        var sql = mode switch
        {
            ImportMode.Replace => provider.BuildBatchReplaceSql(table, batch.Count),
            ImportMode.SkipExisting => provider.BuildBatchSkipExistingSql(table, batch.Count),
            _ => provider.BuildBatchInsertSql(table, batch.Count)
        };

        await using var cmd = Ado.Command(db, sql);
        Ado.Bind(cmd, "@typeName", typeName);
        Ado.Bind(cmd, "@now", provider.NormalizeParameterValue(DateTimeOffset.UtcNow));

        for (var i = 0; i < batch.Count; i++)
        {
            Ado.Bind(cmd, $"@id_{i}", batch[i].Id);
            Ado.Bind(cmd, $"@data_{i}", batch[i].Json);
        }

        var affected = await cmd.ExecuteNonQueryAsync(ct);
        // Some drivers report -1 for a successful multi-row statement; treat that as "all of them".
        return affected < 0 ? batch.Count : affected;
    }

    static async Task<int> WriteIndividually(
        DbConnection db,
        Shiny.DocumentDb.IDatabaseProvider provider,
        string table,
        string typeName,
        IReadOnlyList<ParsedDocument> batch,
        ImportMode mode,
        CancellationToken ct)
    {
        var written = 0;
        await using var transaction = await db.BeginTransactionAsync(ct);
        var now = provider.NormalizeParameterValue(DateTimeOffset.UtcNow);

        foreach (var document in batch)
        {
            if (mode == ImportMode.SkipExisting && await Exists(db, transaction, provider, table, typeName, document.Id, ct))
                continue;

            var sql = mode == ImportMode.Replace
                ? provider.BuildUpdateSql(table)
                : provider.BuildInsertSql(table);

            await using (var cmd = Ado.Command(db, sql))
            {
                cmd.Transaction = transaction;
                Ado.Bind(cmd, "@id", document.Id);
                Ado.Bind(cmd, "@typeName", typeName);
                Ado.Bind(cmd, "@data", document.Json);
                Ado.Bind(cmd, "@now", now);

                var affected = await cmd.ExecuteNonQueryAsync(ct);
                if (affected > 0)
                {
                    written++;
                    continue;
                }
            }

            // Replace against a row that does not exist yet is an insert.
            if (mode == ImportMode.Replace)
            {
                await using var insert = Ado.Command(db, provider.BuildInsertSql(table));
                insert.Transaction = transaction;
                Ado.Bind(insert, "@id", document.Id);
                Ado.Bind(insert, "@typeName", typeName);
                Ado.Bind(insert, "@data", document.Json);
                Ado.Bind(insert, "@now", now);
                written += await insert.ExecuteNonQueryAsync(ct);
            }
        }

        await transaction.CommitAsync(ct);
        return written;
    }

    static async Task<bool> Exists(
        DbConnection db,
        DbTransaction transaction,
        Shiny.DocumentDb.IDatabaseProvider provider,
        string table,
        string typeName,
        string id,
        CancellationToken ct)
    {
        await using var cmd = Ado.Command(db, $"SELECT COUNT(*) FROM {provider.QuoteTable(table)} WHERE Id = @id AND TypeName = @typeName");
        cmd.Transaction = transaction;
        Ado.Bind(cmd, "@id", id);
        Ado.Bind(cmd, "@typeName", typeName);
        return Ado.Count(await cmd.ExecuteScalarAsync(ct)) > 0;
    }

    sealed record ParsedDocument(string Id, string Json);

    static async Task<IReadOnlyList<ParsedDocument>> Parse(Stream source, CancellationToken ct)
    {
        using var reader = new StreamReader(source, Encoding.UTF8, detectEncodingFromByteOrderMarks: true);
        var text = (await reader.ReadToEndAsync(ct)).Trim();
        if (text.Length == 0)
            return [];

        var documents = new List<ParsedDocument>();

        if (text[0] == '[')
        {
            if (JsonNode.Parse(text) is JsonArray array)
            {
                foreach (var element in array)
                    Collect(element, documents);
            }
            return documents;
        }

        foreach (var line in text.Split('\n'))
        {
            var trimmed = line.Trim().TrimEnd(',');
            if (trimmed.Length == 0)
                continue;

            Collect(JsonNode.Parse(trimmed), documents);
        }

        return documents;
    }

    static void Collect(JsonNode? node, List<ParsedDocument> into)
    {
        if (node is not JsonObject obj)
            return;

        // Envelope form from a JsonEnvelope export: unwrap it back to the body.
        if (obj.TryGetPropertyValue("data", out var data) && data is JsonObject body && obj.ContainsKey("typeName"))
        {
            var envelopeId = obj.TryGetPropertyValue("id", out var eid) ? eid?.ToString() : null;
            into.Add(Build(body, envelopeId));
            return;
        }

        into.Add(Build(obj, null));
    }

    /// <summary>
    /// Settles on one id and makes sure the body carries it. DocumentDb mirrors the Id into the JSON,
    /// so a document imported without one would be invisible to any LINQ predicate on Id.
    /// </summary>
    static ParsedDocument Build(JsonObject body, string? envelopeId)
    {
        var key = body.ContainsKey("Id") ? "Id" : "id";
        var existing = ReadId(body);

        var id = envelopeId ?? existing
                 // Nothing to go on: mint one rather than rejecting the row, matching Insert's
                 // behaviour for Guid-keyed types.
                 ?? Guid.NewGuid().ToString();

        // Keep a numeric id numeric - an int-keyed type stores it as a number, and rewriting it to a
        // string would stop predicates on Id from matching.
        var wasNumber = body.TryGetPropertyValue(key, out var current)
                        && current is JsonValue value
                        && value.GetValueKind() == JsonValueKind.Number;

        body[key] = wasNumber && long.TryParse(id, out var numeric)
            ? JsonValue.Create(numeric)
            : JsonValue.Create(id);

        return new ParsedDocument(id, body.ToJsonString());
    }

    static string? ReadId(JsonObject body)
    {
        foreach (var key in (string[])["id", "Id"])
        {
            if (body.TryGetPropertyValue(key, out var value) && value is not null)
            {
                var text = value.GetValueKind() == JsonValueKind.String ? value.GetValue<string>() : value.ToJsonString();
                if (!string.IsNullOrWhiteSpace(text))
                    return text;
            }
        }

        return null;
    }
}
