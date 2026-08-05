namespace Shiny.DocumentDb.Internal;

/// <summary>
/// The stored form of an encrypted value: <c>enc:&lt;version&gt;:&lt;keyId&gt;:&lt;base64 payload&gt;</c>.
/// </summary>
/// <remarks>
/// It is a JSON string on purpose — every provider stores one, backup/replication move it verbatim, and an
/// operator scanning a table can see at a glance which columns are protected and under which key.
/// </remarks>
static class EncryptionEnvelope
{
    public const string Prefix = DocumentEncryptionFormat.Prefix;
    public const int CurrentVersion = 1;

    public static string Format(string keyId, ReadOnlySpan<byte> payload)
        => String.Concat(Prefix, CurrentVersion.ToString(), ":", keyId, ":", Convert.ToBase64String(payload));

    /// <summary>
    /// A cheap shape test used on every read — a value that is not an envelope is pre-encryption plaintext and is
    /// returned as-is, so turning encryption on for a populated store does not break its existing documents.
    /// </summary>
    /// <remarks>
    /// The shape test itself lives in <see cref="DocumentEncryptionFormat"/>, which is public: the envelope is an
    /// on-disk contract that tooling outside this assembly has to recognise too, and two implementations of it
    /// would drift. What stays here is <see cref="Parse"/>, which hands back raw ciphertext bytes.
    /// </remarks>
    public static bool IsEnvelope(string? value) => DocumentEncryptionFormat.IsEnvelope(value);

    public static (string KeyId, byte[] Payload) Parse(string envelope)
    {
        ArgumentNullException.ThrowIfNull(envelope);

        // enc : version : keyId : base64  — the payload is split off last so a base64 '+' or '/' is never an issue
        // and a key id containing ':' is rejected at construction rather than mis-parsed here.
        var parts = envelope.Split(':', 4);
        if (parts.Length != 4 || parts[0] != "enc")
            throw new FormatException("The value is not a Shiny.DocumentDb encryption envelope.");

        if (!Int32.TryParse(parts[1], out var version) || version != CurrentVersion)
            throw new FormatException($"Unsupported encryption envelope version '{parts[1]}'.");

        return (parts[2], Convert.FromBase64String(parts[3]));
    }
}
