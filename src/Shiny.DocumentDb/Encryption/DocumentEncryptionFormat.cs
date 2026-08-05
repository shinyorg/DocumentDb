using System.Text;

namespace Shiny.DocumentDb;

/// <summary>What an encryption envelope says about itself, without any key.</summary>
/// <param name="Version">The envelope format version — currently always 1.</param>
/// <param name="KeyId">The key the value was written under, as stamped into the envelope.</param>
/// <param name="PayloadLength">Length of the base64 payload segment, in characters.</param>
public readonly record struct EncryptedValueInfo(int Version, string KeyId, int PayloadLength);

/// <summary>
/// The stored form of an encrypted value — <c>enc:&lt;version&gt;:&lt;keyId&gt;:&lt;base64&gt;</c> — as a
/// read-only contract, for tooling that inspects stored documents without the key ring.
/// </summary>
/// <remarks>
/// Public because the envelope is already an on-disk contract: backups carry it verbatim, replicas copy it,
/// and an operator scanning a table sees it. Anything that reads stored bodies — an admin tool, a migration
/// script, a repair job — needs to recognise one, and a second copy of the format would drift from this one.
/// Nothing here decrypts; it describes the shape.
/// </remarks>
public static class DocumentEncryptionFormat
{
    /// <summary>The prefix every envelope starts with.</summary>
    public const string Prefix = "enc:";

    /// <summary>
    /// A cheap shape test. A value that is not an envelope is pre-encryption plaintext — including an ordinary
    /// value that happens to start with <c>enc:</c>, which is why the version segment has to parse as a number.
    /// </summary>
    public static bool IsEnvelope(string? value) => TryParse(value, out _);

    /// <summary>Reads what an envelope declares about itself. False when <paramref name="value"/> is not one.</summary>
    public static bool TryParse(string? value, out EncryptedValueInfo info)
    {
        info = default;

        if (value == null || !value.StartsWith(Prefix, StringComparison.Ordinal))
            return false;

        var afterPrefix = value.AsSpan(Prefix.Length);
        var versionEnd = afterPrefix.IndexOf(':');
        if (versionEnd <= 0 || !Int32.TryParse(afterPrefix[..versionEnd], out var version))
            return false;

        // …and a key id and a payload have to follow it.
        var afterVersion = afterPrefix[(versionEnd + 1)..];
        var keyEnd = afterVersion.IndexOf(':');
        if (keyEnd <= 0)
            return false;

        info = new EncryptedValueInfo(version, afterVersion[..keyEnd].ToString(), afterVersion.Length - keyEnd - 1);
        return true;
    }

    /// <summary>
    /// Renders decrypted payload bytes for display. Every value codec encodes as UTF-8 text, so a caller
    /// that decrypted an envelope can show the value without knowing the property's CLR type.
    /// </summary>
    /// <remarks>
    /// Strict UTF-8: a payload that is not valid UTF-8 did not come from this library's codecs, and showing
    /// replacement characters for it would read as a successful decrypt of a corrupt value rather than as
    /// "this is not something we can render".
    /// </remarks>
    public static bool TryRenderPlaintext(ReadOnlySpan<byte> plaintext, out string text)
    {
        try
        {
            text = new UTF8Encoding(encoderShouldEmitUTF8Identifier: false, throwOnInvalidBytes: true).GetString(plaintext);
            return true;
        }
        catch (DecoderFallbackException)
        {
            text = "";
            return false;
        }
    }
}
