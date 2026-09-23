using System.Buffers;
using System.Collections.Concurrent;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;

namespace Shiny.DocumentDb.Internal;

/// <summary>
/// The one <see cref="DocumentMetadata"/> property a document type declares: how to read it, how to assign a
/// fresh instance when it is null, and the JSON name it would have in the body (so the body can be stripped of it).
/// </summary>
sealed class DocumentMetadataAccessor
{
    readonly Func<object, object?> get;
    readonly Action<object, object?> set;

    internal DocumentMetadataAccessor(string clrName, string jsonName, Func<object, object?> get, Action<object, object?> set)
    {
        this.ClrName = clrName;
        this.JsonName = jsonName;
        this.get = get;
        this.set = set;
    }

    /// <summary>The property's CLR name — what a member expression names.</summary>
    public string ClrName { get; }

    /// <summary>The property's name in the JSON body, after the naming policy / <c>[JsonPropertyName]</c>.</summary>
    public string JsonName { get; }

    /// <summary>The document's metadata, assigning a new instance first when the property is null.</summary>
    public DocumentMetadata GetOrCreate(object document)
    {
        if (this.get(document) is DocumentMetadata existing)
            return existing;

        var created = new DocumentMetadata();
        this.set(document, created);
        return created;
    }

    /// <summary>
    /// Stamps the envelope read back from the store. <paramref name="tenantId"/> is the tenant the row belongs to —
    /// null on a store without shared-table multi-tenancy.
    /// </summary>
    public void Stamp(object document, DateTimeOffset createdAt, DateTimeOffset updatedAt, string? tenantId = null)
    {
        var metadata = this.GetOrCreate(document);
        metadata.CreatedAt = createdAt;
        metadata.UpdatedAt = updatedAt;
        metadata.TenantId = tenantId;
        metadata.IsPersisted = true;
    }

    /// <summary>
    /// Stamps the instance a caller just wrote. <paramref name="createdAt"/> is only known when the write was
    /// an insert; on an update it is left as the caller had it rather than guessed.
    /// </summary>
    public void StampWrite(object document, DateTimeOffset updatedAt, DateTimeOffset? createdAt, string? tenantId = null)
    {
        var metadata = this.GetOrCreate(document);
        if (createdAt.HasValue)
            metadata.CreatedAt = createdAt.Value;
        metadata.UpdatedAt = updatedAt;
        metadata.TenantId = tenantId;
        metadata.IsPersisted = true;
    }

    /// <summary>
    /// Ensures the property holds an instance without stamping it — for reads that have no envelope to stamp
    /// from (temporal snapshots), so the "never null" guarantee still holds.
    /// </summary>
    public void EnsureInstance(object document) => this.GetOrCreate(document);
}

/// <summary>
/// Discovery, stamping and body-stripping for <see cref="DocumentMetadata"/>. Every provider goes through here,
/// so a type with no metadata property pays one cached dictionary lookup and nothing else.
/// </summary>
static class MetadataSupport
{
    static readonly ConcurrentDictionary<(Type Type, JsonSerializerOptions Options), DocumentMetadataAccessor?> cache = new();

    /// <summary>The select list a relational read uses — the body, plus the envelope timestamps when the type needs them.</summary>
    public const string DataWithTimestamps = "Data, CreatedAt, UpdatedAt";

    /// <summary>
    /// The metadata accessor for <paramref name="type"/>, or null when it declares no <see cref="DocumentMetadata"/>
    /// property. Resolved from <paramref name="typeInfo"/> when supplied (the AOT-safe path), otherwise from
    /// <paramref name="options"/>' resolver, otherwise by reflection (the reflection-fallback lane).
    /// </summary>
    /// <exception cref="InvalidOperationException">The type declares more than one, or a property the store cannot assign.</exception>
    public static DocumentMetadataAccessor? For(Type type, JsonTypeInfo? typeInfo, JsonSerializerOptions options)
    {
        var effectiveOptions = typeInfo?.Options ?? options;
        return cache.GetOrAdd((type, effectiveOptions), static (key, ti) => Resolve(key.Type, ti, key.Options), typeInfo);
    }

    /// <summary>Generic convenience over <see cref="For(Type, JsonTypeInfo?, JsonSerializerOptions)"/>.</summary>
    public static DocumentMetadataAccessor? For<T>(JsonTypeInfo<T>? typeInfo, JsonSerializerOptions options)
        => For(typeof(T), typeInfo, options);

    /// <summary>The select list for a typed relational read of <paramref name="type"/>.</summary>
    public static string SelectColumns(DocumentMetadataAccessor? accessor)
        => accessor == null ? "Data" : DataWithTimestamps;

    /// <summary>
    /// Stamps <paramref name="document"/> from the timestamps at <paramref name="createdOrdinal"/> and the column
    /// after it, plus the tenant the read was scoped to. No-op when <paramref name="accessor"/> is null.
    /// </summary>
    public static void StampFromReader(DocumentMetadataAccessor? accessor, object? document, DbDataReader reader, int createdOrdinal, string? tenantId)
    {
        if (accessor == null || document == null)
            return;

        var created = ReadTimestamp(reader, createdOrdinal) ?? default;
        var updated = ReadTimestamp(reader, createdOrdinal + 1) ?? default;
        accessor.Stamp(document, created, updated, tenantId);
    }

    /// <summary>
    /// "Now" at the precision every relational engine keeps (microseconds), so the value stamped on a written
    /// instance equals the value read back. <c>DATETIME(6)</c> on MySQL would otherwise <i>round</i> the seventh
    /// digit and the two would differ by a tick.
    /// </summary>
    public static DateTimeOffset UtcNowMicroseconds()
    {
        var now = DateTimeOffset.UtcNow;
        return new DateTimeOffset(now.Ticks - now.Ticks % 10, TimeSpan.Zero);
    }

    /// <summary>"Now" at millisecond precision — for backends that store BSON dates or other millisecond timestamps.</summary>
    public static DateTimeOffset UtcNowMilliseconds()
    {
        var now = DateTimeOffset.UtcNow;
        return new DateTimeOffset(now.Ticks - now.Ticks % TimeSpan.TicksPerMillisecond, TimeSpan.Zero);
    }

    /// <summary>
    /// Removes the metadata property from a serialized body, so the envelope stays the only copy. Returns
    /// <paramref name="json"/> untouched when <paramref name="accessor"/> is null or the property is absent.
    /// </summary>
    public static string StripFromBody(string json, DocumentMetadataAccessor? accessor)
    {
        if (accessor == null || json.IndexOf(accessor.JsonName, StringComparison.Ordinal) < 0)
            return json;

        var utf8 = Encoding.UTF8.GetBytes(json);
        var reader = new Utf8JsonReader(utf8, new JsonReaderOptions { CommentHandling = JsonCommentHandling.Skip });
        if (!reader.Read() || reader.TokenType != JsonTokenType.StartObject)
            return json;

        var buffer = new ArrayBufferWriter<byte>(utf8.Length);
        using (var writer = new Utf8JsonWriter(buffer))
        {
            writer.WriteStartObject();
            while (reader.Read() && reader.TokenType != JsonTokenType.EndObject)
            {
                var name = reader.GetString()!;
                reader.Read();
                if (string.Equals(name, accessor.JsonName, StringComparison.Ordinal))
                {
                    reader.Skip();
                }
                else
                {
                    // Copy the value's bytes through verbatim — no re-parse, no reformatting.
                    var start = (int)reader.TokenStartIndex;
                    reader.Skip();
                    writer.WritePropertyName(name);
                    writer.WriteRawValue(utf8.AsSpan(start, (int)reader.BytesConsumed - start), skipInputValidation: true);
                }
            }
            writer.WriteEndObject();
        }
        return Encoding.UTF8.GetString(buffer.WrittenSpan);
    }

    /// <summary>
    /// Reads a <c>CreatedAt</c>/<c>UpdatedAt</c> column back as a <see cref="DateTimeOffset"/> across providers
    /// (SQLite stores ISO text, SQL Server a zone-less <c>DATETIME2</c> holding UTC, the others a native
    /// timestamp). Null on a NULL column or an unreadable value.
    /// </summary>
    public static DateTimeOffset? ReadTimestamp(DbDataReader reader, int ordinal)
    {
        if (reader.IsDBNull(ordinal))
            return null;
        try
        {
            return reader.GetFieldValue<DateTimeOffset>(ordinal);
        }
        catch (Exception ex) when (ex is InvalidCastException or FormatException)
        {
            try
            {
                return FromValue(reader.GetValue(ordinal));
            }
            catch (Exception inner) when (inner is InvalidCastException or FormatException)
            {
                return null;
            }
        }
    }

    /// <summary>Normalizes a timestamp value in whatever shape a driver returned it.</summary>
    public static DateTimeOffset? FromValue(object? value) => value switch
    {
        DateTimeOffset dto => dto,
        DateTime dt => new DateTimeOffset(dt.Kind == DateTimeKind.Local ? dt.ToUniversalTime() : DateTime.SpecifyKind(dt, DateTimeKind.Utc)),
        string s when DateTimeOffset.TryParse(s, System.Globalization.CultureInfo.InvariantCulture,
            System.Globalization.DateTimeStyles.RoundtripKind | System.Globalization.DateTimeStyles.AssumeUniversal, out var parsed) => parsed,
        _ => null
    };

    /// <summary>
    /// Build-time shape check for a type the application configured: at most one metadata property, and it must
    /// be assignable. Returns the problems rather than throwing, for the configuration validator.
    /// </summary>
    public static IEnumerable<string> Validate([DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)] Type type)
    {
        var properties = type.GetProperties(BindingFlags.Instance | BindingFlags.Public)
            .Where(p => p.PropertyType == typeof(DocumentMetadata))
            .ToList();

        if (properties.Count > 1)
            yield return $"'{type.Name}' declares {properties.Count} DocumentMetadata properties ({string.Join(", ", properties.Select(p => p.Name))}); a document type can have at most one.";

        foreach (var property in properties.Where(p => p.SetMethod == null && !HasJsonIncludeSetter(p)))
            yield return $"'{type.Name}.{property.Name}' is a get-only DocumentMetadata property. The store assigns it when it is null, so it needs a setter — declare it {{ get; set; }} (or init).";
    }

    /// <summary>
    /// True when <paramref name="expression"/> reads a <see cref="DocumentMetadata"/> member anywhere — the signal
    /// for a server-side projection to hand the query to a client-side one, since the envelope timestamps are not in
    /// the body a SQL <c>json_object</c> projection reads from.
    /// </summary>
    public static bool ReferencesMetadata(Expression expression)
    {
        var finder = new MetadataReferenceFinder();
        finder.Visit(expression);
        return finder.Found;
    }

    sealed class MetadataReferenceFinder : ExpressionVisitor
    {
        public bool Found { get; private set; }

        protected override Expression VisitMember(MemberExpression node)
        {
            if (node.Type == typeof(DocumentMetadata) || node.Expression?.Type == typeof(DocumentMetadata))
                this.Found = true;
            return base.VisitMember(node);
        }
    }

    static bool HasJsonIncludeSetter(PropertyInfo property)
        => property.GetSetMethod(nonPublic: true) != null
           && property.IsDefined(typeof(System.Text.Json.Serialization.JsonIncludeAttribute), inherit: true);

    static DocumentMetadataAccessor? Resolve(Type type, JsonTypeInfo? typeInfo, JsonSerializerOptions options)
    {
        if (typeInfo == null && !options.TryGetTypeInfo(type, out typeInfo))
            return ResolveByReflection(type, options);

        if (typeInfo.Kind != JsonTypeInfoKind.Object)
            return null;

        JsonPropertyInfo? found = null;
        foreach (var property in typeInfo.Properties)
        {
            if (property.PropertyType == typeof(DocumentMetadata))
            {
                if (found != null)
                    throw new InvalidOperationException(
                        $"'{type.Name}' declares more than one DocumentMetadata property; a document type can have at most one.");
                found = property;
            }
        }

        if (found == null)
            return null;

        var clrName = (found.AttributeProvider as MemberInfo)?.Name ?? found.Name;
        var getter = found.Get
            ?? throw new InvalidOperationException($"'{type.Name}.{clrName}' (DocumentMetadata) has no getter the serializer can use.");
        var setter = found.Set
            ?? throw GetOnly(type, clrName);

        return new DocumentMetadataAccessor(clrName, found.Name, getter, setter);
    }

    [UnconditionalSuppressMessage("Trimming", "IL2070", Justification = "Reflection-fallback lane only — reached when no JsonTypeInfo exists for the type, which already means reflection serialization.")]
    [UnconditionalSuppressMessage("Trimming", "IL2075", Justification = "Reflection-fallback lane only.")]
    static DocumentMetadataAccessor? ResolveByReflection(Type type, JsonSerializerOptions options)
    {
        var properties = type.GetProperties(BindingFlags.Instance | BindingFlags.Public)
            .Where(p => p.PropertyType == typeof(DocumentMetadata))
            .ToList();

        if (properties.Count == 0)
            return null;
        if (properties.Count > 1)
            throw new InvalidOperationException(
                $"'{type.Name}' declares more than one DocumentMetadata property; a document type can have at most one.");

        var property = properties[0];
        var setMethod = property.GetSetMethod(nonPublic: true) ?? throw GetOnly(type, property.Name);
        var jsonName = JsonPropertyNameResolver.ResolveJsonName(options, type, property.Name);

        return new DocumentMetadataAccessor(
            property.Name,
            jsonName,
            property.GetValue,
            (target, value) => setMethod.Invoke(target, [value]));
    }

    static InvalidOperationException GetOnly(Type type, string name)
        => new($"'{type.Name}.{name}' is a get-only DocumentMetadata property. The store assigns it when it is null, so it needs a setter — declare it {{ get; set; }} (or init).");
}
