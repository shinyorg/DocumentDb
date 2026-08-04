using System.Globalization;
using System.Text;
using System.Text.Json;

namespace Shiny.DocumentDb.Internal;

delegate TValue PlainReader<out TValue>(ref Utf8JsonReader reader);

/// <summary>
/// Turns a property value into the bytes that get encrypted, and back. Deliberately <b>not</b>
/// <see cref="JsonSerializer"/>: serializing the value with the store's own options from inside a converter for
/// that same property recurses straight back into this converter, and it would make the encoding depend on the
/// configured resolver — which deterministic mode cannot tolerate, because the same value has to produce the same
/// bytes forever.
/// </summary>
sealed class ValueCodec<TValue>(Func<TValue, byte[]> encode, Func<byte[], TValue> decode, PlainReader<TValue> readPlain)
{
    public Func<TValue, byte[]> Encode { get; } = encode;
    public Func<byte[], TValue> Decode { get; } = decode;

    /// <summary>Reads a value that is <b>not</b> an envelope — a document written before the property was mapped.</summary>
    public PlainReader<TValue> ReadPlain { get; } = readPlain;
}

/// <summary>
/// The value types a mapped property may have. Each is instantiated explicitly (never through
/// <c>MakeGenericType</c>) so the whole package stays native-AOT clean.
/// </summary>
static class ValueCodecs
{
    static byte[] Utf8(string value) => Encoding.UTF8.GetBytes(value);
    static string Utf8(byte[] value) => Encoding.UTF8.GetString(value);

    public static readonly ValueCodec<string> String = new(
        Utf8,
        Utf8,
        (ref Utf8JsonReader r) => r.GetString()!);

    public static readonly ValueCodec<bool> Boolean = new(
        v => Utf8(v ? "true" : "false"),
        b => System.Boolean.Parse(Utf8(b)),
        (ref Utf8JsonReader r) => r.GetBoolean());

    public static readonly ValueCodec<int> Int32 = new(
        v => Utf8(v.ToString(CultureInfo.InvariantCulture)),
        b => System.Int32.Parse(Utf8(b), CultureInfo.InvariantCulture),
        (ref Utf8JsonReader r) => r.GetInt32());

    public static readonly ValueCodec<long> Int64 = new(
        v => Utf8(v.ToString(CultureInfo.InvariantCulture)),
        b => System.Int64.Parse(Utf8(b), CultureInfo.InvariantCulture),
        (ref Utf8JsonReader r) => r.GetInt64());

    public static readonly ValueCodec<double> Double = new(
        v => Utf8(v.ToString("R", CultureInfo.InvariantCulture)),
        b => System.Double.Parse(Utf8(b), CultureInfo.InvariantCulture),
        (ref Utf8JsonReader r) => r.GetDouble());

    public static readonly ValueCodec<decimal> Decimal = new(
        v => Utf8(v.ToString(CultureInfo.InvariantCulture)),
        b => System.Decimal.Parse(Utf8(b), CultureInfo.InvariantCulture),
        (ref Utf8JsonReader r) => r.GetDecimal());

    public static readonly ValueCodec<Guid> Guid = new(
        v => Utf8(v.ToString("D")),
        b => System.Guid.Parse(Utf8(b)),
        (ref Utf8JsonReader r) => r.GetGuid());

    // Round-trip ("O") so a DateTime survives encryption with its kind and sub-second precision intact.
    public static readonly ValueCodec<DateTime> DateTime = new(
        v => Utf8(v.ToString("O", CultureInfo.InvariantCulture)),
        b => System.DateTime.Parse(Utf8(b), CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind),
        (ref Utf8JsonReader r) => r.GetDateTime());

    public static readonly ValueCodec<DateTimeOffset> DateTimeOffset = new(
        v => Utf8(v.ToString("O", CultureInfo.InvariantCulture)),
        b => System.DateTimeOffset.Parse(Utf8(b), CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind),
        (ref Utf8JsonReader r) => r.GetDateTimeOffset());

    /// <summary>Lifts a value-type codec to its nullable form. Null never reaches it — the converter writes and
    /// reads JSON null directly, so <c>x.Ssn == null</c> keeps working against encrypted data.</summary>
    public static ValueCodec<TValue?> Nullable<TValue>(ValueCodec<TValue> inner) where TValue : struct
        => new(
            v => inner.Encode(v!.Value),
            b => inner.Decode(b),
            (ref Utf8JsonReader r) => inner.ReadPlain(ref r));
}
