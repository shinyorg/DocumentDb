using System.Globalization;
using System.Text.Json.Serialization.Metadata;
using Shiny.DocumentDb.Internal;

namespace Shiny.DocumentDb.CosmosDb;

// DocumentMetadata on Cosmos DB. The envelope's createdAt/updatedAt are ISO-8601 round-trip ("o") strings at the item
// root, always written in UTC — so ordinal string order is instant order, and a constant compared against them only has
// to be rendered the same way. "o" keeps all seven fractional digits, so "now" needs no truncation to round-trip.
public partial class CosmosDbDocumentStore
{
    /// <summary>The select list for a typed read — the body, plus the envelope timestamps when the type declares metadata.</summary>
    internal static string SelectData(DocumentMetadataAccessor? metadata)
        => metadata == null ? "c.data" : "c.data, c.createdAt, c.updatedAt";

    /// <summary>The envelope field a <see cref="DocumentMetadata"/> timestamp is answered from.</summary>
    internal static string EnvelopePath(Internal.Query.EnvelopeField field)
        => field == Internal.Query.EnvelopeField.CreatedAt ? "c.createdAt" : "c.updatedAt";

    /// <summary>An envelope timestamp as stored: UTC, round-trip format.</summary>
    internal static string FormatTimestamp(DateTimeOffset value)
        => value.ToUniversalTime().ToString("o", CultureInfo.InvariantCulture);

    DocumentMetadataAccessor? MetadataFor<T>(JsonTypeInfo<T>? typeInfo) => MetadataSupport.For(typeInfo, this.jsonOptions);

    /// <summary>
    /// Stamps the instance a caller just wrote with the timestamp the write stored. <paramref name="inserted"/> is true
    /// only when the write created the item, so <c>CreatedAt</c> is known to be the same instant.
    /// </summary>
    void StampWritten<T>(T document, JsonTypeInfo<T>? typeInfo, DateTimeOffset now, bool inserted) where T : class
        => this.MetadataFor(typeInfo)?.StampWrite(document, now, inserted ? now : null);

    /// <summary>
    /// Materializes an item read with <see cref="SelectData"/> (or read whole) and stamps its envelope timestamps onto
    /// a <see cref="DocumentMetadata"/> property when <paramref name="metadata"/> is non-null.
    /// </summary>
    T? MaterializeItem<T>(CosmosDocument item, JsonTypeInfo<T>? typeInfo, DocumentMetadataAccessor? metadata) where T : class
        => this.MaterializeStamped(item.Data, item.CreatedAt, item.UpdatedAt, typeInfo, metadata);

    T? MaterializeStamped<T>(string json, string? createdAt, string? updatedAt, JsonTypeInfo<T>? typeInfo, DocumentMetadataAccessor? metadata) where T : class
    {
        var document = this.Materialize(json, typeInfo);
        if (document != null && metadata != null)
            metadata.Stamp(document, ParseCosmosTimestamp(createdAt) ?? default, ParseCosmosTimestamp(updatedAt) ?? default);
        return document;
    }

    // A string field of a projected row read off the raw query stream, or null when absent.
    static string? StringProperty(System.Text.Json.JsonElement row, string name)
        => row.TryGetProperty(name, out var value) && value.ValueKind == System.Text.Json.JsonValueKind.String ? value.GetString() : null;

    /// <summary>
    /// Materializes a temporal snapshot. History items carry no envelope, so a <see cref="DocumentMetadata"/> property
    /// is newed up but left unstamped (<see cref="DocumentMetadata.IsPersisted"/> false).
    /// </summary>
    T? MaterializeSnapshot<T>(string json, JsonTypeInfo<T>? typeInfo) where T : class
    {
        var document = Deserialize(json, typeInfo, this.jsonOptions);
        if (document != null)
            this.MetadataFor(typeInfo)?.EnsureInstance(document);
        return document;
    }
}
