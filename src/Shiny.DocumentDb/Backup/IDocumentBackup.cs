using System.Text.Json;
using System.Text.Json.Serialization;

namespace Shiny.DocumentDb;

/// <summary>
/// Streaming bulk export/import — a separate store capability (probe with <c>store is IDocumentBackup</c>),
/// like <see cref="IDocumentMaintenance"/>. The import path binds document bodies <b>as-is</b> — no
/// <c>&lt;T&gt;</c>, no <c>JsonTypeInfo</c>, no reflection over the documents — and streams so a multi-GB
/// backup never lands fully in memory.
/// <para>
/// Because the speed comes precisely from skipping per-document work, the import path does <b>not</b> honor
/// versioning/CAS, temporal history, interceptors, tenant scoping, or global query filters. Treat it as a
/// raw restore lane, not a transparent replacement for <see cref="IDocumentStore.BatchUpsert"/>.
/// </para>
/// </summary>
public interface IDocumentBackup
{
    /// <summary>
    /// Streams the entire store (every document table) out to <paramref name="destination"/> as a v1 backup
    /// document — a JSON array of <c>{ id, docType, data }</c> records. Sidecar tables (history, spatial,
    /// vector, full-text) are not exported; they are rebuilt on restore by the write path.
    /// </summary>
    Task ExportAsync(
        Stream destination,
        BackupExportOptions? options = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Streams a v1 backup document in from <paramref name="source"/>, writing in committed chunks. Document
    /// bodies are bound verbatim; the records are parsed with a forward-only reader so the stream is never
    /// fully buffered.
    /// </summary>
    Task<BulkRestoreResult> RestoreAsync(
        Stream source,
        BulkRestoreOptions? options = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Lower-level primitive: write pre-shaped raw rows from any source (another store, a network feed, a
    /// custom file). <see cref="RestoreAsync"/> is the JSON adapter on top of this.
    /// </summary>
    Task<BulkRestoreResult> BulkImportAsync(
        IAsyncEnumerable<RawDocument> documents,
        BulkRestoreOptions? options = null,
        CancellationToken cancellationToken = default);
}

/// <summary>
/// A pre-shaped row for bulk import. <see cref="Data"/> is raw UTF-8 JSON, bound verbatim into the document
/// <c>Data</c> column — it is never parsed into a CLR type.
/// </summary>
public readonly record struct RawDocument(string Id, string DocType, ReadOnlyMemory<byte> Data);

/// <summary>A type-homogeneous insert row handed to <c>IDatabaseProvider.BulkCopyInsertAsync</c>. The body
/// is raw JSON as a string (already decoded by the engine).</summary>
public readonly record struct RawBulkRow(string Id, string Data);

/// <summary>How an imported row resolves a collision with an existing document of the same Id + type.</summary>
public enum BulkWriteMode
{
    /// <summary>Fail the chunk on a duplicate Id (fastest; multi-row VALUES — supported by every provider).</summary>
    Insert,

    /// <summary>Overwrite the existing body wholesale on conflict (<c>ON CONFLICT … DO UPDATE SET Data = …</c>).</summary>
    Replace,

    /// <summary>RFC 7396 deep-merge into the existing body — the same semantics as <see cref="IDocumentStore.BatchUpsert"/>.</summary>
    Merge,

    /// <summary>Insert new rows and silently skip ones whose Id already exists (<c>ON CONFLICT … DO NOTHING</c>).</summary>
    SkipExisting
}

/// <summary>Options controlling a bulk import / restore.</summary>
public class BulkRestoreOptions
{
    /// <summary>Collision strategy for existing rows. Defaults to <see cref="BulkWriteMode.Insert"/> (restore-into-empty).</summary>
    public BulkWriteMode Mode { get; set; } = BulkWriteMode.Insert;

    /// <summary>When true, wipe every document table (<see cref="IDocumentMaintenance.ClearAll"/>) before importing.</summary>
    public bool ClearExistingFirst { get; set; }

    /// <summary>Rows per statement / per committed transaction. Defaults to 500.</summary>
    public int ChunkSize { get; set; } = 500;

    /// <summary>
    /// When false (default) each chunk commits in its own transaction — resumable and bounded WAL/log growth
    /// for very large imports. When true the whole import is one transaction (atomic, but heavy).
    /// </summary>
    public bool SingleTransaction { get; set; }

    /// <summary>Optional progress callback, invoked after each committed chunk.</summary>
    public IProgress<BulkProgress>? Progress { get; set; }
}

/// <summary>Options controlling a bulk export.</summary>
public class BackupExportOptions
{
    /// <summary>When set, only documents whose resolved type name is in this set are exported. Null exports everything.</summary>
    public IReadOnlyCollection<string>? DocTypes { get; set; }

    /// <summary>Pretty-print the output. Defaults to false (compact).</summary>
    public bool Indented { get; set; }
}

/// <summary>The outcome of a bulk import / restore.</summary>
public readonly record struct BulkRestoreResult(
    long DocumentsRead,
    long DocumentsWritten,
    long DocumentsSkipped,
    int ChunksCommitted);

/// <summary>Progress reported after each committed chunk during a bulk import.</summary>
public readonly record struct BulkProgress(long DocumentsRead, long DocumentsWritten, int ChunksCommitted);

/// <summary>
/// The v1 backup record DTO. The envelope fields are read via source-generated metadata (AOT-safe); the
/// document body is captured as a raw <see cref="JsonElement"/> and never deserialized into a domain type.
/// </summary>
internal sealed class BackupRecord
{
    [JsonPropertyName("id")] public JsonElement Id { get; set; }
    [JsonPropertyName("docType")] public string DocType { get; set; } = "";
    [JsonPropertyName("data")] public JsonElement Data { get; set; }
}

[JsonSourceGenerationOptions(PropertyNamingPolicy = JsonKnownNamingPolicy.CamelCase)]
[JsonSerializable(typeof(BackupRecord))]
internal sealed partial class BackupJsonContext : JsonSerializerContext;
