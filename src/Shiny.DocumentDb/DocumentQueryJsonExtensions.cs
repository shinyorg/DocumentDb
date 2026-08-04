using System.Buffers;
using System.Runtime.CompilerServices;
using System.Text;
using System.Text.Json.Nodes;

namespace Shiny.DocumentDb;

/// <summary>
/// JSON terminals for <see cref="IDocumentQuery{T}"/> — build the query with the typed surface, then take the
/// result as raw JSON instead of <typeparamref name="T"/>. The point is the read that only ever leaves the
/// process again: an HTTP endpoint that deserializes a document solely to re-serialize it into the response
/// pays for both, and these terminals pay for neither.
/// </summary>
/// <remarks>
/// Every one of these is a thin layer over <see cref="IDocumentQuery{T}.RawJsonRows"/>, which is where the
/// providers plug in and where the fidelity rules (persisted bytes vs. re-serialized, computed properties,
/// blobs, encrypted types) are documented.
/// </remarks>
public static class DocumentQueryJsonExtensions
{
    static readonly byte[] ArrayOpen = "["u8.ToArray();
    static readonly byte[] ArrayClose = "]"u8.ToArray();
    static readonly byte[] Separator = ","u8.ToArray();

    // ── Node lane — parsed into a mutable tree ──────────────────────────

    /// <summary>
    /// Materializes every matching document as a <see cref="JsonObject"/>, in query order. The JSON twin of
    /// <see cref="IDocumentQuery{T}.ToList"/>.
    /// </summary>
    public static async Task<IReadOnlyList<JsonObject>> ToJsonList<T>(
        this IDocumentQuery<T> query,
        CancellationToken ct = default
    ) where T : class
    {
        ArgumentNullException.ThrowIfNull(query);

        var list = new List<JsonObject>();
        await foreach (var raw in query.RawJsonRows(null, ct).ConfigureAwait(false))
            list.Add(Parse<T>(raw));

        return list.AsReadOnly();
    }

    /// <summary>Streams every matching document as a <see cref="JsonObject"/> without buffering the set.</summary>
    public static async IAsyncEnumerable<JsonObject> ToJsonAsyncEnumerable<T>(
        this IDocumentQuery<T> query,
        [EnumeratorCancellation] CancellationToken ct = default
    ) where T : class
    {
        ArgumentNullException.ThrowIfNull(query);

        await foreach (var raw in query.RawJsonRows(null, ct).ConfigureAwait(false))
            yield return Parse<T>(raw);
    }

    /// <summary>
    /// The first matching document as a <see cref="JsonObject"/>, or <c>null</c> when nothing matches. Fetches
    /// a single row, and honors <see cref="IDocumentQuery{T}.Paginate"/> the same way
    /// <see cref="IDocumentQuery{T}.FirstOrDefault"/> does.
    /// </summary>
    public static async Task<JsonObject?> FirstOrDefaultJson<T>(
        this IDocumentQuery<T> query,
        CancellationToken ct = default
    ) where T : class
    {
        var raw = await query.FirstOrDefaultRawJson(ct).ConfigureAwait(false);
        return raw is null ? null : Parse<T>(raw);
    }

    /// <summary>The first matching document as a <see cref="JsonObject"/>. Throws when nothing matches.</summary>
    public static async Task<JsonObject> FirstJson<T>(
        this IDocumentQuery<T> query,
        CancellationToken ct = default
    ) where T : class
        => await query.FirstOrDefaultJson(ct).ConfigureAwait(false)
            ?? throw new InvalidOperationException($"No '{typeof(T).Name}' matched the query.");

    /// <summary>
    /// The only matching document as a <see cref="JsonObject"/>, or <c>null</c> when nothing matches. Throws
    /// when more than one matches — two rows are fetched to detect it, matching
    /// <see cref="IDocumentQuery{T}.SingleOrDefault"/>.
    /// </summary>
    public static async Task<JsonObject?> SingleOrDefaultJson<T>(
        this IDocumentQuery<T> query,
        CancellationToken ct = default
    ) where T : class
    {
        ArgumentNullException.ThrowIfNull(query);

        string? found = null;
        await foreach (var raw in query.RawJsonRows(2, ct).ConfigureAwait(false))
        {
            if (found != null)
                throw new InvalidOperationException($"More than one '{typeof(T).Name}' matched the query.");
            found = raw;
        }
        return found is null ? null : Parse<T>(found);
    }

    /// <summary>
    /// The only matching document as a <see cref="JsonObject"/>. Throws when none or more than one matches.
    /// </summary>
    public static async Task<JsonObject> SingleJson<T>(
        this IDocumentQuery<T> query,
        CancellationToken ct = default
    ) where T : class
        => await query.SingleOrDefaultJson(ct).ConfigureAwait(false)
            ?? throw new InvalidOperationException($"No '{typeof(T).Name}' matched the query.");

    // ── Raw lane — no parse at all ──────────────────────────────────────

    /// <summary>
    /// The first matching document's JSON body, or <c>null</c> when nothing matches. Nothing is parsed, so
    /// this is what a single-document endpoint wants:
    /// <c>Results.Content(json, "application/json")</c>.
    /// </summary>
    public static async Task<string?> FirstOrDefaultRawJson<T>(
        this IDocumentQuery<T> query,
        CancellationToken ct = default
    ) where T : class
    {
        ArgumentNullException.ThrowIfNull(query);

        await foreach (var raw in query.RawJsonRows(1, ct).ConfigureAwait(false))
            return raw;

        return null;
    }

    /// <summary>Streams every matching document's JSON body, unparsed, in query order.</summary>
    public static IAsyncEnumerable<string> ToRawJsonAsyncEnumerable<T>(
        this IDocumentQuery<T> query,
        CancellationToken ct = default
    ) where T : class
    {
        ArgumentNullException.ThrowIfNull(query);
        return query.RawJsonRows(null, ct);
    }

    /// <summary>
    /// Writes every matching document to <paramref name="destination"/> as one UTF-8 JSON array and returns
    /// how many were written. Bodies go out exactly as they came back — nothing is parsed, nothing is
    /// re-serialized, and the set is never buffered — which makes this the whole-list twin of
    /// <see cref="FirstOrDefaultRawJson"/>:
    /// <code>
    /// ctx.Response.ContentType = "application/json";
    /// await store.Query&lt;Order&gt;().Where(o => o.Status == "open").WriteJsonArrayTo(ctx.Response.Body, ct);
    /// </code>
    /// An empty result writes <c>[]</c>.
    /// </summary>
    public static async Task<int> WriteJsonArrayTo<T>(
        this IDocumentQuery<T> query,
        Stream destination,
        CancellationToken ct = default
    ) where T : class
    {
        ArgumentNullException.ThrowIfNull(query);
        ArgumentNullException.ThrowIfNull(destination);

        var count = 0;
        await destination.WriteAsync(ArrayOpen, ct).ConfigureAwait(false);
        await foreach (var raw in query.RawJsonRows(null, ct).ConfigureAwait(false))
        {
            if (count > 0)
                await destination.WriteAsync(Separator, ct).ConfigureAwait(false);

            await WriteUtf8Async(destination, raw, ct).ConfigureAwait(false);
            count++;
        }
        await destination.WriteAsync(ArrayClose, ct).ConfigureAwait(false);
        return count;
    }

    // Transcodes straight into a pooled buffer — a JSON body can be large, and Encoding.GetBytes(string)
    // would allocate a second copy of every document on the way to the socket.
    static async Task WriteUtf8Async(Stream destination, string json, CancellationToken ct)
    {
        var buffer = ArrayPool<byte>.Shared.Rent(Encoding.UTF8.GetMaxByteCount(json.Length));
        try
        {
            var written = Encoding.UTF8.GetBytes(json, buffer);
            await destination.WriteAsync(buffer.AsMemory(0, written), ct).ConfigureAwait(false);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    static JsonObject Parse<T>(string raw) => Internal.RawJson.ParseObject(raw, typeof(T));
}
