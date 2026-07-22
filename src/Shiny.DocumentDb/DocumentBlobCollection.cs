using System.Collections;
using System.Text.Json.Serialization;
using Shiny.DocumentDb.Internal;

namespace Shiny.DocumentDb;

/// <summary>
/// A document property holding several <see cref="DocumentBlob"/> payloads. Purpose-built rather than a plain
/// <c>List&lt;DocumentBlob&gt;</c> so it can (a) load every item in one round trip via <see cref="LoadAllAsync"/>,
/// (b) carry the per-document loader stamped on during hydration, and (c) assign each item's sidecar key at
/// <see cref="Add(byte[], string?, string?, string?)"/> time.
/// </summary>
/// <remarks>
/// Loading is granular: an individual item is a <see cref="DocumentBlob"/> and exposes its own
/// <see cref="DocumentBlob.LoadAsync"/>, so a caller can pull one attachment or the whole set.
/// </remarks>
[JsonConverter(typeof(DocumentBlobCollectionJsonConverter))]
public sealed class DocumentBlobCollection : IList<DocumentBlob>
{
    readonly List<DocumentBlob> items = new();

    internal IBlobLoader? Loader { get; set; }

    /// <summary>Attaches <paramref name="loader"/> to the collection and every item currently in it.</summary>
    internal void StampLoader(IBlobLoader loader)
    {
        this.Loader = loader;
        foreach (var item in this.items)
            item.Loader = loader;
    }

    /// <summary>True when every item's payload is loaded (vacuously true when empty).</summary>
    [JsonIgnore]
    public bool IsLoaded => this.items.TrueForAll(x => x.IsLoaded);

    /// <summary>
    /// Fetches every not-yet-loaded item in a single round trip. No-op when all items are loaded. Throws
    /// when the collection is not attached to a store and has outstanding items.
    /// </summary>
    public async Task LoadAllAsync(CancellationToken cancellationToken = default)
    {
        var pending = this.items.FindAll(x => !x.IsLoaded);
        if (pending.Count == 0)
            return;
        if (this.Loader == null)
            throw new InvalidOperationException(
                "This blob collection is not attached to a store and cannot self-load. Load it through the document it was read from.");

        await this.Loader.LoadManyAsync(pending, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Adds a payload to be persisted on the next write of the owning document, assigning its sidecar key now
    /// (generated when <paramref name="key"/> is omitted) so it is stable across reorders.
    /// </summary>
    public DocumentBlob Add(byte[] bytes, string? contentType = null, string? fileName = null, string? key = null)
    {
        var blob = DocumentBlob.FromBytes(bytes, contentType, fileName, key ?? Guid.CreateVersion7().ToString("N"));
        blob.Loader = this.Loader;
        this.items.Add(blob);
        return blob;
    }

    // ── IList<DocumentBlob> (delegates to the backing list) ─────────────────

    public DocumentBlob this[int index] { get => this.items[index]; set => this.items[index] = value; }
    public int Count => this.items.Count;
    public bool IsReadOnly => false;
    public void Add(DocumentBlob item) => this.items.Add(item);
    public void Clear() => this.items.Clear();
    public bool Contains(DocumentBlob item) => this.items.Contains(item);
    public void CopyTo(DocumentBlob[] array, int arrayIndex) => this.items.CopyTo(array, arrayIndex);
    public IEnumerator<DocumentBlob> GetEnumerator() => this.items.GetEnumerator();
    public int IndexOf(DocumentBlob item) => this.items.IndexOf(item);
    public void Insert(int index, DocumentBlob item) => this.items.Insert(index, item);
    public bool Remove(DocumentBlob item) => this.items.Remove(item);
    public void RemoveAt(int index) => this.items.RemoveAt(index);
    IEnumerator IEnumerable.GetEnumerator() => this.items.GetEnumerator();
}
