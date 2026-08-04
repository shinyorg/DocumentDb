using System.Text.Json;
using System.Text.Json.Serialization;

namespace Shiny.DocumentDb.Internal;

/// <summary>
/// Encrypts on write and decrypts on read for one mapped property. Installed as the property's
/// <c>CustomConverter</c> by <see cref="EncryptionRegistry"/>, which is what makes encryption a serialization
/// concern: every write path (typed, batch, session, temporal history, backup) and every read path is covered
/// without a single provider knowing about it.
/// </summary>
sealed class EncryptedConverter<TValue>(EncryptedProperty property, ValueCodec<TValue> codec) : JsonConverter<TValue>
{
    public override TValue? Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
    {
        if (reader.TokenType == JsonTokenType.Null)
            return default;

        if (reader.TokenType == JsonTokenType.String)
        {
            var raw = reader.GetString();
            if (EncryptionEnvelope.IsEnvelope(raw))
                return codec.Decode(property.Encryptor.Decrypt(raw!));
        }

        // Not an envelope: a document written before this property was mapped. Read it as-is rather than failing,
        // so enabling encryption on a populated store is a no-downtime change — RewrapAsync converts the backlog.
        return codec.ReadPlain(ref reader);
    }

    public override void Write(Utf8JsonWriter writer, TValue value, JsonSerializerOptions options)
    {
        if (value is null)
        {
            writer.WriteNullValue();
            return;
        }

        writer.WriteStringValue(property.Encryptor.Encrypt(codec.Encode(value), property.Mode));
    }
}

/// <summary>
/// Builds the converter for a property's CLR type. Every generic instantiation is written out explicitly — no
/// <c>MakeGenericType</c> — so the package stays native-AOT clean, and an unsupported type is rejected at
/// mapping time with a message that says what to do instead.
/// </summary>
static class EncryptedConverterFactory
{
    public static JsonConverter Create(Type propertyType, EncryptedProperty property)
    {
        if (propertyType == typeof(string))
            return new EncryptedConverter<string>(property, ValueCodecs.String);

        if (propertyType == typeof(bool))
            return new EncryptedConverter<bool>(property, ValueCodecs.Boolean);
        if (propertyType == typeof(bool?))
            return new EncryptedConverter<bool?>(property, ValueCodecs.Nullable(ValueCodecs.Boolean));

        if (propertyType == typeof(int))
            return new EncryptedConverter<int>(property, ValueCodecs.Int32);
        if (propertyType == typeof(int?))
            return new EncryptedConverter<int?>(property, ValueCodecs.Nullable(ValueCodecs.Int32));

        if (propertyType == typeof(long))
            return new EncryptedConverter<long>(property, ValueCodecs.Int64);
        if (propertyType == typeof(long?))
            return new EncryptedConverter<long?>(property, ValueCodecs.Nullable(ValueCodecs.Int64));

        if (propertyType == typeof(double))
            return new EncryptedConverter<double>(property, ValueCodecs.Double);
        if (propertyType == typeof(double?))
            return new EncryptedConverter<double?>(property, ValueCodecs.Nullable(ValueCodecs.Double));

        if (propertyType == typeof(decimal))
            return new EncryptedConverter<decimal>(property, ValueCodecs.Decimal);
        if (propertyType == typeof(decimal?))
            return new EncryptedConverter<decimal?>(property, ValueCodecs.Nullable(ValueCodecs.Decimal));

        if (propertyType == typeof(Guid))
            return new EncryptedConverter<Guid>(property, ValueCodecs.Guid);
        if (propertyType == typeof(Guid?))
            return new EncryptedConverter<Guid?>(property, ValueCodecs.Nullable(ValueCodecs.Guid));

        if (propertyType == typeof(DateTime))
            return new EncryptedConverter<DateTime>(property, ValueCodecs.DateTime);
        if (propertyType == typeof(DateTime?))
            return new EncryptedConverter<DateTime?>(property, ValueCodecs.Nullable(ValueCodecs.DateTime));

        if (propertyType == typeof(DateTimeOffset))
            return new EncryptedConverter<DateTimeOffset>(property, ValueCodecs.DateTimeOffset);
        if (propertyType == typeof(DateTimeOffset?))
            return new EncryptedConverter<DateTimeOffset?>(property, ValueCodecs.Nullable(ValueCodecs.DateTimeOffset));

        throw new NotSupportedException(
            $"'{property.DeclaringType.Name}.{property.ClrName}' is a {propertyType.Name}, which field-level encryption does not support. " +
            "Encryptable properties are string, bool, int, long, double, decimal, Guid, DateTime, DateTimeOffset and their nullable forms. " +
            "For anything else, hold the value as a string property and convert it yourself.");
    }

    /// <summary>True when <paramref name="propertyType"/> can be encrypted — checked at mapping time.</summary>
    public static bool IsSupported(Type propertyType)
        => propertyType == typeof(string)
        || Supported.Contains(System.Nullable.GetUnderlyingType(propertyType) ?? propertyType);

    static readonly HashSet<Type> Supported =
    [
        typeof(bool), typeof(int), typeof(long), typeof(double),
        typeof(decimal), typeof(Guid), typeof(DateTime), typeof(DateTimeOffset)
    ];
}
