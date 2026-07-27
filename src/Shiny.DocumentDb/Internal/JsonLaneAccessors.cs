using System.Reflection;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.Json.Serialization.Metadata;

namespace Shiny.DocumentDb.Internal;

/// <summary>
/// Node-based Id handling for the late-bound JSON write lane. Resolves the Id member's serialized JSON name
/// and <see cref="IdKind"/> from the type's <see cref="JsonTypeInfo"/> (the same resolution the typed
/// <see cref="IdAccessor{T}"/> performs), then reads/injects the Id directly on a <see cref="JsonObject"/> —
/// bridging the JSON representation (e.g. a Guid's <c>D</c> form on the wire) and the storage-string form the
/// <c>Id</c> column uses (a Guid's <c>N</c> form). Custom (converter-based) Id types are not supported on this
/// lane — callers with a mapped Id type use the typed <c>Insert&lt;T&gt;</c>.
/// </summary>
sealed class JsonLaneIdAccessor : IJsonLaneIds
{
    public IdKind Kind { get; }
    public string MemberName { get; }

    JsonLaneIdAccessor(IdKind kind, string memberName)
    {
        this.Kind = kind;
        this.MemberName = memberName;
    }

    /// <summary>Storage-string form of the Id (matches <see cref="IdAccessor{T}.GetIdAsString"/>), or null if absent/JSON-null.</summary>
    public string? ReadStorageId(JsonObject doc)
    {
        if (!doc.TryGetPropertyValue(this.MemberName, out var node) || node is null)
            return null;

        return this.Kind switch
        {
            IdKind.Guid => node.GetValue<Guid>().ToString("N"),
            IdKind.Int => node.GetValue<int>().ToString(),
            IdKind.Long => node.GetValue<long>().ToString(),
            IdKind.String => node.GetValue<string>(),
            _ => throw new InvalidOperationException($"Unsupported Id kind: {this.Kind}")
        };
    }

    /// <summary>True when the Id member is absent, JSON-null, or the type's default value.</summary>
    public bool IsDefaultId(JsonObject doc)
    {
        if (!doc.TryGetPropertyValue(this.MemberName, out var node) || node is null)
            return true;

        return this.Kind switch
        {
            IdKind.Guid => node.GetValue<Guid>() == Guid.Empty,
            IdKind.Int => node.GetValue<int>() == 0,
            IdKind.Long => node.GetValue<long>() == 0L,
            IdKind.String => string.IsNullOrEmpty(node.GetValue<string>()),
            _ => true
        };
    }

    /// <summary>Converts a caller-supplied Id object into the storage-string form used by the <c>Id</c> column.</summary>
    public string ResolveId(object id) => this.Kind switch
    {
        IdKind.Guid => id is Guid g ? g.ToString("N") : throw Mismatch(id),
        IdKind.Int => id is int i ? i.ToString() : throw Mismatch(id),
        IdKind.Long => id is long l ? l.ToString() : throw Mismatch(id),
        IdKind.String => id is string s ? s : throw Mismatch(id),
        _ => throw new InvalidOperationException($"Unsupported Id kind: {this.Kind}")
    };

    ArgumentException Mismatch(object id)
        => new($"Id type mismatch. Expected '{this.Kind}' but received '{id.GetType().Name}'.", nameof(id));

    /// <summary>Injects a generated Id (given in storage-string form) into the node in its JSON representation.</summary>
    public void WriteId(JsonObject doc, string storageId)
    {
        doc[this.MemberName] = this.Kind switch
        {
            IdKind.Guid => JsonValue.Create(Guid.Parse(storageId)),
            IdKind.Int => JsonValue.Create(int.Parse(storageId)),
            IdKind.Long => JsonValue.Create(long.Parse(storageId)),
            IdKind.String => JsonValue.Create(storageId),
            _ => throw new InvalidOperationException($"Unsupported Id kind: {this.Kind}")
        };
    }

    /// <summary>
    /// Defers to the store's sequence generator for every kind it can generate. A declared <c>string</c> id
    /// is the one refusal — the library has never invented string ids, and the schema-free lane's UUIDv7
    /// default does not apply to a type that declared what its id looks like.
    /// </summary>
    public string? TryGenerateId(string typeName)
        => this.Kind == IdKind.String
            ? throw new InvalidOperationException(
                $"Insert requires a non-empty string Id on '{typeName}'. String Id properties are not auto-generated.")
            : null;

    public static JsonLaneIdAccessor Create(Type type, string idPropertyName, JsonSerializerOptions jsonOptions)
    {
        JsonTypeInfo typeInfo;
        try
        {
            typeInfo = jsonOptions.GetTypeInfo(type);
        }
        catch (Exception ex)
        {
            throw new InvalidOperationException(
                $"The late-bound JSON write lane requires a resolvable JsonTypeInfo for '{type.Name}' to locate its Id member. " +
                "Configure a JsonSerializerContext via DocumentStoreOptions.JsonSerializerOptions, or register the type.", ex);
        }

        var expectedJsonName = jsonOptions.PropertyNamingPolicy?.ConvertName(idPropertyName) ?? idPropertyName;
        foreach (var prop in typeInfo.Properties)
        {
            var matches =
                (prop.AttributeProvider is MemberInfo member && member.Name == idPropertyName) ||
                string.Equals(prop.Name, expectedJsonName, StringComparison.Ordinal) ||
                string.Equals(prop.Name, idPropertyName, StringComparison.OrdinalIgnoreCase);
            if (!matches)
                continue;

            var kind = ResolveKind(prop.PropertyType, type);
            return new JsonLaneIdAccessor(kind, prop.Name);
        }

        throw new InvalidOperationException(
            $"Type '{type.Name}' has no '{idPropertyName}' property resolvable for the JSON write lane.");
    }

    static IdKind ResolveKind(Type propertyType, Type documentType) => propertyType switch
    {
        _ when propertyType == typeof(Guid) => IdKind.Guid,
        _ when propertyType == typeof(int) => IdKind.Int,
        _ when propertyType == typeof(long) => IdKind.Long,
        _ when propertyType == typeof(string) => IdKind.String,
        _ => throw new NotSupportedException(
            $"The Id property on '{documentType.Name}' has type '{propertyType.Name}', which is not supported on the " +
            "late-bound JSON write lane (Guid, int, long, and string are supported). Use the typed Insert<T>/Update<T>/Upsert<T>.")
    };
}

/// <summary>
/// Pure <see cref="JsonNode"/> helpers shared by the JSON write lane: mapped-property presence checks, version
/// get/set, and reading the spatial (GeoJSON) / vector members out of a supplied document node.
/// </summary>
static class JsonLaneNodes
{
    /// <summary>True when <paramref name="member"/> is present on the object (an explicit JSON null still counts as present).</summary>
    public static bool HasMember(JsonObject doc, string member)
        => doc.ContainsKey(member);

    /// <summary>True when the member is absent (a JSON null is treated as an intentional "no value", not missing).</summary>
    public static bool IsMemberMissing(JsonObject doc, string member)
        => !doc.ContainsKey(member);

    public static int ReadVersion(JsonObject doc, string member)
        => doc.TryGetPropertyValue(member, out var node) && node is not null ? node.GetValue<int>() : 0;

    public static void WriteVersion(JsonObject doc, string member, int version)
        => doc[member] = JsonValue.Create(version);

    /// <summary>
    /// Reads a <see cref="GeoPoint"/> from the GeoJSON member value (<c>{"type":"Point","coordinates":[lng,lat]}</c>).
    /// Returns false when the member is JSON-null (deliberate "no location"); throws when the shape is invalid.
    /// </summary>
    public static bool TryReadGeoPoint(JsonNode? member, string memberName, out GeoPoint point)
    {
        point = default;
        if (member is null)
            return false;

        if (member["coordinates"] is not JsonArray coords || coords.Count < 2)
            throw new ArgumentException(
                $"The spatial property '{memberName}' must be GeoJSON of the form {{\"type\":\"Point\",\"coordinates\":[longitude,latitude]}}.");

        var lng = coords[0]!.GetValue<double>();
        var lat = coords[1]!.GetValue<double>();
        point = new GeoPoint(lat, lng);
        return true;
    }

    /// <summary>
    /// Reads the embedding array from a vector member. Returns an empty memory when the member is JSON-null
    /// (deliberate "no embedding" — the sidecar is skipped, mirroring the typed empty-embedding rule).
    /// </summary>
    public static ReadOnlyMemory<float> ReadVector(JsonNode? member, string memberName)
    {
        if (member is null)
            return ReadOnlyMemory<float>.Empty;

        if (member is not JsonArray array)
            throw new ArgumentException($"The vector property '{memberName}' must be a JSON array of numbers.");

        var vec = new float[array.Count];
        for (var i = 0; i < array.Count; i++)
            vec[i] = array[i]!.GetValue<float>();
        return vec;
    }
}
