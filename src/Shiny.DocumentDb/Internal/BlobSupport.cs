using System.Security.Cryptography;

namespace Shiny.DocumentDb.Internal;

/// <summary>
/// Provider-agnostic blob mechanics shared by every store that supports blobs. The relational
/// <c>DocumentStore</c> and the NoSQL stores all reuse these so the prepare/stamp/enumerate rules stay
/// identical across backends. Storage is provider-specific; this handles the object-model side.
/// </summary>
public static class BlobSupport
{
    /// <summary>
    /// Stamps keys, lengths and (when configured) hashes onto every blob the document carries, enforcing the
    /// size ceiling, and returns the full set the document now claims. Call before serializing the document.
    /// </summary>
    public static IReadOnlyList<PreparedBlobRow> Prepare(
        IReadOnlyList<BlobMapping> mappings, long maxBlobSize, object document)
    {
        if (mappings.Count == 0)
            return Array.Empty<PreparedBlobRow>();
        if (maxBlobSize == 0)
            throw new NotSupportedException(
                $"'{document.GetType().Name}' maps blob properties but this provider does not support blobs (MaxBlobSize is 0).");

        var prepared = new List<PreparedBlobRow>();
        foreach (var mapping in mappings)
        {
            var value = mapping.GetValue(document);
            if (value == null)
                continue;

            if (!mapping.IsCollection)
            {
                var blob = (DocumentBlob)value;
                prepared.Add(Stamp(mapping, blob, mapping.Key!, maxBlobSize));
            }
            else
            {
                foreach (var blob in (DocumentBlobCollection)value)
                {
                    if (blob == null)
                        continue;
                    var key = string.IsNullOrWhiteSpace(blob.Key) ? Guid.CreateVersion7().ToString("N") : blob.Key;
                    prepared.Add(Stamp(mapping, blob, key, maxBlobSize));
                }
            }
        }

        var dup = prepared.GroupBy(x => x.Key, StringComparer.OrdinalIgnoreCase).FirstOrDefault(g => g.Count() > 1);
        if (dup != null)
            throw new InvalidOperationException(
                $"Two blobs on '{document.GetType().Name}' resolve to the same key '{dup.Key}'. Give collection items distinct keys.");

        return prepared;
    }

    static PreparedBlobRow Stamp(BlobMapping mapping, DocumentBlob blob, string key, long maxBlobSize)
    {
        blob.Key = key;
        if (blob.IsPendingWrite)
        {
            var payload = blob.RawBytes!;
            var ceiling = mapping.MaxSize > 0 && mapping.MaxSize < maxBlobSize ? mapping.MaxSize : maxBlobSize;
            if (payload.LongLength > ceiling)
                throw new NotSupportedException(
                    $"Blob '{key}' is {payload.LongLength:N0} bytes, exceeding the {ceiling:N0} byte limit for this provider/mapping.");
            blob.Length = payload.LongLength;
            blob.UpdatedAt = DateTimeOffset.UtcNow;
            if (mapping.ComputeHash)
                blob.Hash = Convert.ToHexString(SHA256.HashData(payload)).ToLowerInvariant();
        }
        return new PreparedBlobRow(key, blob);
    }

    /// <summary>Stamps <paramref name="loader"/> onto every blob / collection the document carries.</summary>
    public static void AttachLoaders(IReadOnlyList<BlobMapping> mappings, object document, IBlobLoader loader)
    {
        foreach (var mapping in mappings)
        {
            var value = mapping.GetValue(document);
            if (value == null)
                continue;
            if (mapping.IsCollection)
                ((DocumentBlobCollection)value).StampLoader(loader);
            else
                ((DocumentBlob)value).Loader = loader;
        }
    }

    /// <summary>Every blob a document carries (single + collection items), skipping nulls.</summary>
    public static IEnumerable<DocumentBlob> Enumerate(IReadOnlyList<BlobMapping> mappings, object document)
    {
        foreach (var mapping in mappings)
        {
            var value = mapping.GetValue(document);
            if (value == null)
                continue;
            if (!mapping.IsCollection)
            {
                yield return (DocumentBlob)value;
            }
            else
            {
                foreach (var blob in (DocumentBlobCollection)value)
                    if (blob != null)
                        yield return blob;
            }
        }
    }
}

/// <summary>A blob resolved from a live document during a write, with the sidecar key it lands under.</summary>
public readonly record struct PreparedBlobRow(string Key, DocumentBlob Blob)
{
    public bool IsPendingWrite => this.Blob.IsPendingWrite;
    public byte[]? RawBytes => this.Blob.RawBytes;
}
