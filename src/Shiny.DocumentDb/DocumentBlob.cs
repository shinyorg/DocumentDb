using System.Text.Json.Serialization;
using Shiny.DocumentDb.Internal;

namespace Shiny.DocumentDb;

/// <summary>
/// A binary payload attached to a document. The payload itself lives in a sidecar table and is
/// <b>not</b> loaded by document queries — only the metadata below rides along in the document JSON.
/// Call <see cref="LoadAsync"/> (or <see cref="GetBytesAsync"/>) to fetch <see cref="Bytes"/> on demand.
/// </summary>
/// <remarks>
/// Because the metadata is real JSON in the document body, it is queryable like any other member
/// (<c>Where(x =&gt; x.Pdf.Length &gt; 1_000_000)</c>); <see cref="Bytes"/> is not, and referencing it in a
/// query throws at translation time.
/// </remarks>
[JsonConverter(typeof(DocumentBlobJsonConverter))]
public sealed class DocumentBlob
{
    byte[]? bytes;

    internal DocumentBlob() { }

    /// <summary>
    /// The per-document loader stamped on during hydration. Null for a blob that was constructed via
    /// <see cref="FromBytes"/> or deserialized outside the store — those cannot self-load.
    /// </summary>
    internal IBlobLoader? Loader { get; set; }

    /// <summary>Identifies this payload's row within the owning document. Defaults to the mapped property name.</summary>
    public string Key { get; internal set; } = "";

    /// <summary>Size of the payload in bytes.</summary>
    public long Length { get; internal set; }

    /// <summary>MIME type, when the caller supplied one.</summary>
    public string? ContentType { get; internal set; }

    /// <summary>Original file name. Often absent — not every payload comes from a file.</summary>
    public string? FileName { get; internal set; }

    /// <summary>Lowercase hex SHA-256 of the payload, populated only when the mapping sets <c>ComputeHash</c>.</summary>
    public string? Hash { get; internal set; }

    public DateTimeOffset CreatedAt { get; internal set; }

    public DateTimeOffset UpdatedAt { get; internal set; }

    /// <summary>True once the payload has been fetched (or when this instance was built from bytes to be written).</summary>
    [JsonIgnore]
    public bool IsLoaded => this.bytes != null;

    /// <summary>
    /// The payload. Throws when it has not been loaded — call <see cref="LoadAsync"/> first, or check
    /// <see cref="IsLoaded"/>.
    /// </summary>
    [JsonIgnore]
    public byte[] Bytes =>
        this.bytes ?? throw new InvalidOperationException(
            $"Blob '{this.Key}' is not loaded. Call LoadAsync/GetBytesAsync before reading Bytes, or check IsLoaded.");

    /// <summary>Set when this instance carries bytes that still need writing to the sidecar.</summary>
    internal bool IsPendingWrite { get; set; }

    internal byte[]? RawBytes => this.bytes;

    internal void Attach(byte[] payload) => this.bytes = payload;

    /// <summary>
    /// Fetches this payload if it is not already loaded. No-op when <see cref="IsLoaded"/>. Throws
    /// <see cref="InvalidOperationException"/> when the blob is not attached to a store (constructed via
    /// <see cref="FromBytes"/> without a save, or deserialized outside the store).
    /// </summary>
    public async Task LoadAsync(CancellationToken cancellationToken = default)
    {
        if (this.IsLoaded)
            return;
        if (this.Loader == null)
            throw new InvalidOperationException(
                $"Blob '{this.Key}' is not attached to a store and cannot self-load. Load it through the document it was read from.");

        var payload = await this.Loader.LoadOneAsync(this.Key, cancellationToken).ConfigureAwait(false);
        if (payload != null)
            this.bytes = payload;
    }

    /// <summary><see cref="LoadAsync"/> then <see cref="Bytes"/> — the one-call convenience.</summary>
    public async Task<byte[]> GetBytesAsync(CancellationToken cancellationToken = default)
    {
        await this.LoadAsync(cancellationToken).ConfigureAwait(false);
        return this.Bytes;
    }

    /// <summary>
    /// Creates a blob to be persisted on the next write of the owning document.
    /// </summary>
    /// <param name="bytes">The payload. Bound as-is — no copy is taken.</param>
    /// <param name="contentType">Optional MIME type.</param>
    /// <param name="fileName">Optional original file name.</param>
    /// <param name="key">
    /// Optional explicit sidecar key. Single mapped properties default to the property name; collection items
    /// get a generated key so reordering the list does not re-point rows.
    /// </param>
    public static DocumentBlob FromBytes(byte[] bytes, string? contentType = null, string? fileName = null, string? key = null)
    {
        ArgumentNullException.ThrowIfNull(bytes);

        var now = DateTimeOffset.UtcNow;
        return new DocumentBlob
        {
            bytes = bytes,
            Key = key ?? "",
            Length = bytes.LongLength,
            ContentType = contentType,
            FileName = fileName,
            CreatedAt = now,
            UpdatedAt = now,
            IsPendingWrite = true
        };
    }
}
