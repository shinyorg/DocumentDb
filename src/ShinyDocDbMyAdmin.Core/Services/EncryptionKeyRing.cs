using System.Security.Cryptography;
using Shiny.DocumentDb;
using ShinyDocDbMyAdmin.Models;

namespace ShinyDocDbMyAdmin.Services;

/// <summary>What became of a reveal, in enough detail to say something true about it.</summary>
public enum DecryptOutcome
{
    /// <summary>The value came back.</summary>
    Decrypted,

    /// <summary>The connection carries no keys, so nothing was attempted.</summary>
    NotConfigured,

    /// <summary>Keys are configured, but not the one this envelope names.</summary>
    KeyNotHeld,

    /// <summary>The key is held and the value would not authenticate — tampered with, or the wrong key.</summary>
    Tampered,

    /// <summary>A key entry is not usable AES-256 material.</summary>
    UnusableKey,

    /// <summary>Decrypted, but the bytes are not text this tool can show.</summary>
    Unrenderable,

    /// <summary>Reading protected values is closed on this instance.</summary>
    Refused,

    /// <summary>The value handed in was not an envelope in the first place.</summary>
    NotEncrypted
}

/// <summary>The result of a reveal. <see cref="Text"/> is set only when <see cref="Outcome"/> is Decrypted.</summary>
public sealed record DecryptedValue(DecryptOutcome Outcome, string? Text, string Message)
{
    public bool Succeeded => this.Outcome == DecryptOutcome.Decrypted;
}

/// <summary>
/// Decrypts stored field-level encryption envelopes for display, using keys an operator deliberately put on
/// a connection profile.
/// </summary>
/// <remarks>
/// <para>
/// This is the smaller half of the encryption feature and deliberately the last part of it. Everything the
/// admin does with envelopes — recognising them, reporting key coverage, refusing to overwrite one — works
/// with no key at all. This adds the ability to see the value, and the cost is that data keys now live
/// beside the connection string.
/// </para>
/// <para>
/// It is only cheap to build because of one property of the library: every <c>ValueCodec</c> encodes to
/// UTF-8 text, so decryption needs the key and not the property's CLR type. A tool that had to know a path
/// held a <c>Guid</c> rather than a <c>decimal</c> before it could render it would be guesswork.
/// </para>
/// <para>
/// <b>Where this is never reached:</b> the AI tool surface (a decrypted value would be posted to a model
/// provider), export (an export decides what leaves the building and stays ciphertext), and demo mode.
/// </para>
/// </remarks>
public sealed class EncryptionKeyRing(ProfileStore profiles, DemoMode demo, ILogger<EncryptionKeyRing> logger)
{
    /// <summary>True when this connection could decrypt something if asked.</summary>
    public async Task<bool> IsAvailable(string profileId, CancellationToken ct = default)
    {
        if (!demo.CanUseEncryptionKeys)
            return false;

        var profile = await profiles.Get(profileId, ct);
        return profile is not null && profiles.RevealEncryptionKeys(profile).Count > 0;
    }

    /// <summary>The key ids this connection holds, for the "which keys can I read?" line on the card.</summary>
    public async Task<IReadOnlyList<string>> HeldKeyIds(string profileId, CancellationToken ct = default)
    {
        if (!demo.CanUseEncryptionKeys)
            return [];

        var profile = await profiles.Get(profileId, ct);
        return profile is null ? [] : [.. profiles.RevealEncryptionKeys(profile).Select(k => k.KeyId)];
    }

    /// <summary>
    /// Reveals one stored value. Never throws for an expected failure — a wrong key, a retired key and a
    /// tampered value are three different things an operator needs told apart, not one exception.
    /// </summary>
    public async Task<DecryptedValue> Reveal(string profileId, string? storedValue, CancellationToken ct = default)
    {
        if (!demo.CanUseEncryptionKeys)
            return new DecryptedValue(DecryptOutcome.Refused, null, "This is a demo instance; protected values are not readable here.");

        if (!DocumentEncryptionFormat.TryParse(storedValue, out var info))
            return new DecryptedValue(DecryptOutcome.NotEncrypted, null, "This value is not an encryption envelope.");

        var profile = await profiles.Get(profileId, ct);
        var keys = profile is null ? [] : profiles.RevealEncryptionKeys(profile);

        if (keys.Count == 0)
        {
            return new DecryptedValue(
                DecryptOutcome.NotConfigured,
                null,
                $"This value is encrypted under key '{info.KeyId}'. Add that key to the connection to read it.");
        }

        if (!keys.Any(k => string.Equals(k.KeyId, info.KeyId, StringComparison.Ordinal)))
        {
            return new DecryptedValue(
                DecryptOutcome.KeyNotHeld,
                null,
                $"This value was encrypted under key '{info.KeyId}', which is not on this connection. " +
                $"Held here: {string.Join(", ", keys.Select(k => k.KeyId))}.");
        }

        AesGcmDocumentEncryptor encryptor;
        try
        {
            encryptor = Build(keys);
        }
        catch (Exception ex) when (ex is ArgumentException or FormatException)
        {
            // A mistyped key is a configuration error, not a data problem, and saying which is the
            // difference between fixing a text box and suspecting the database.
            return new DecryptedValue(DecryptOutcome.UnusableKey, null, $"A key on this connection is not usable: {ex.Message}");
        }

        try
        {
            var plaintext = encryptor.Decrypt(storedValue!);

            return DocumentEncryptionFormat.TryRenderPlaintext(plaintext, out var text)
                ? new DecryptedValue(DecryptOutcome.Decrypted, text, $"Decrypted with key '{info.KeyId}'.")
                : new DecryptedValue(DecryptOutcome.Unrenderable, null, "The value decrypted but is not text this tool can display.");
        }
        catch (CryptographicException)
        {
            // AES-GCM authenticates: this is either a tampered value or key material that is not the one
            // it was written under, and the envelope cannot tell those apart.
            logger.LogWarning("A value under key {KeyId} failed to authenticate on connection {Profile}", info.KeyId, profileId);
            return new DecryptedValue(
                DecryptOutcome.Tampered,
                null,
                $"This value would not authenticate under key '{info.KeyId}'. Either the stored value was altered, " +
                "or the key configured under that id is not the one it was written with.");
        }
        catch (InvalidOperationException ex)
        {
            // The library's own wording for "that key is not in the ring", which it phrases well.
            return new DecryptedValue(DecryptOutcome.KeyNotHeld, null, ex.Message);
        }
    }

    static AesGcmDocumentEncryptor Build(IReadOnlyList<EncryptionKeyEntry> keys)
    {
        var ring = keys.ToDictionary(
            k => k.KeyId,
            k => AesGcmDocumentEncryptor.KeyFromBase64(k.Key),
            StringComparer.Ordinal);

        // The constructor insists on a current key because it is built for writing too. This one never
        // writes, so any held id will do - reads resolve whichever key the envelope names.
        return new AesGcmDocumentEncryptor(keys[0].KeyId, ring);
    }
}
