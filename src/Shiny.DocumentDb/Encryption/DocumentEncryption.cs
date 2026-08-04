using System.Text.Json;
using System.Text.Json.Serialization.Metadata;

namespace Shiny.DocumentDb;

/// <summary>
/// Helpers for code that serializes a <b>materialized</b> document — a page of query results on its way into
/// an HTTP response, a tool result, a diff — rather than persisting one.
/// </summary>
public static class DocumentEncryption
{
    /// <summary>
    /// The serializer to write an already-materialized document with. Identical to what you pass in unless the
    /// document type maps encrypted properties, in which case it writes their values instead of re-encrypting
    /// them.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Field-level encryption is installed as a <c>JsonTypeInfo</c> modifier whose converters are symmetric:
    /// reading a stored body decrypts, writing encrypts. That is exactly right on the way to the database and
    /// exactly wrong on the way back out — a document read through <c>Get</c>/<c>ToList</c> holds plaintext,
    /// but serializing it again through the store's own options silently re-encrypts it, and randomized mode
    /// yields a different envelope every time. Anything that turns a materialized document back into JSON for
    /// a caller should go through here.
    /// </para>
    /// <para>
    /// <b>Write-only.</b> The returned serializer does not decrypt, so it must never be pointed at a stored
    /// body — materialize the document with the store's own options first.
    /// </para>
    /// </remarks>
    public static JsonSerializerOptions PlaintextView(JsonSerializerOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        return Internal.EncryptionRegistry.PlaintextView(options);
    }

    /// <summary>
    /// The <see cref="JsonTypeInfo{T}"/> twin of <see cref="PlaintextView(JsonSerializerOptions)"/>. Returns
    /// <paramref name="typeInfo"/> itself when its options carry no encryption, so this is free on the
    /// overwhelmingly common path.
    /// </summary>
    public static JsonTypeInfo<T> PlaintextView<T>(JsonTypeInfo<T> typeInfo)
    {
        ArgumentNullException.ThrowIfNull(typeInfo);

        var view = Internal.EncryptionRegistry.PlaintextView(typeInfo.Options);
        return ReferenceEquals(view, typeInfo.Options)
            ? typeInfo
            : (JsonTypeInfo<T>)view.GetTypeInfo(typeof(T));
    }
}
