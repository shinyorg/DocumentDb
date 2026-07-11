using System.Runtime.CompilerServices;
using System.Text;
using System.Text.Json;

namespace Shiny.DocumentDb.Internal;

/// <summary>
/// Shared streaming helpers for the v1 backup format (a JSON array of <c>{ id, docType, data }</c> records),
/// reused by every <see cref="IDocumentBackup"/> implementation. The reader is forward-only and the document
/// body is captured as raw JSON — never deserialized into a domain type.
/// </summary>
public static class BackupStreams
{
    /// <summary>
    /// Streams a v1 backup array from <paramref name="source"/> as <see cref="RawDocument"/> rows. Uses a
    /// source-generated envelope DTO (AOT-safe); the body passes through as raw UTF-8 JSON.
    /// </summary>
    public static async IAsyncEnumerable<RawDocument> ReadAsync(
        Stream source,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        await foreach (var record in JsonSerializer
            .DeserializeAsyncEnumerable(source, BackupJsonContext.Default.BackupRecord, ct)
            .ConfigureAwait(false))
        {
            if (record == null)
                continue;

            var id = record.Id.ValueKind == JsonValueKind.String
                ? record.Id.GetString()!
                : record.Id.GetRawText();
            var data = Encoding.UTF8.GetBytes(record.Data.GetRawText());
            yield return new RawDocument(id, record.DocType, data, record.CreatedAt, record.UpdatedAt);
        }
    }

    /// <summary>
    /// Writes one record to an open backup array, emitting the body verbatim. When
    /// <paramref name="createdAt"/> / <paramref name="updatedAt"/> are supplied they are written as
    /// round-trippable ISO-8601 (the v2 envelope); omitted entirely when null so a v1 reader is unaffected.
    /// </summary>
    public static void WriteRecord(
        Utf8JsonWriter writer,
        string id,
        string docType,
        string rawDataJson,
        DateTimeOffset? createdAt = null,
        DateTimeOffset? updatedAt = null)
    {
        writer.WriteStartObject();
        writer.WriteString("id", id);
        writer.WriteString("docType", docType);
        writer.WritePropertyName("data");
        writer.WriteRawValue(rawDataJson);
        if (createdAt.HasValue)
            writer.WriteString("createdAt", createdAt.Value);
        if (updatedAt.HasValue)
            writer.WriteString("updatedAt", updatedAt.Value);
        writer.WriteEndObject();
    }
}
