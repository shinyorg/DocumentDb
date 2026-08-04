using System.Security.Cryptography;
using Shiny.DocumentDb.Internal;

namespace Shiny.DocumentDb;

/// <summary>
/// AES-256-GCM over caller-supplied key material — authenticated encryption, so a tampered value fails loudly
/// instead of decrypting to garbage. Holds a key ring: writes use <see cref="CurrentKeyId"/>, reads resolve
/// whichever key the envelope names, which is what makes rotation a three-step operation (add key, switch
/// current, <c>RewrapAsync</c>).
/// </summary>
/// <remarks>
/// <para>
/// <b>Randomized</b> mode draws a fresh 96-bit nonce per write. <b>Deterministic</b> mode derives the nonce from
/// the plaintext with HMAC-SHA256 under a separate key derived from the data key, so the same plaintext always
/// yields the same ciphertext (queryable) while the nonce still varies across values and across keys.
/// </para>
/// <para>Key material never leaves this object, and is not written to any log, span, or exception message.</para>
/// </remarks>
public sealed class AesGcmDocumentEncryptor : IDocumentEncryptor
{
    const int NonceSize = 12;   // AES-GCM standard nonce
    const int TagSize = 16;     // AES-GCM standard tag
    const int KeySize = 32;     // AES-256
    static readonly byte[] NonceDerivationLabel = "shiny.documentdb.encryption.nonce.v1"u8.ToArray();

    readonly Dictionary<string, byte[]> keys;
    readonly Dictionary<string, byte[]> nonceKeys;

    /// <summary>
    /// Builds a key ring.
    /// </summary>
    /// <param name="currentKeyId">The key new writes are encrypted under. Must be present in <paramref name="keys"/>.</param>
    /// <param name="keys">Every key that must remain readable, by id. Each must be exactly 32 bytes (AES-256).</param>
    public AesGcmDocumentEncryptor(string currentKeyId, IReadOnlyDictionary<string, byte[]> keys)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(currentKeyId);
        ArgumentNullException.ThrowIfNull(keys);

        if (keys.Count == 0)
            throw new ArgumentException("At least one key is required.", nameof(keys));

        this.keys = new Dictionary<string, byte[]>(keys.Count, StringComparer.Ordinal);
        this.nonceKeys = new Dictionary<string, byte[]>(keys.Count, StringComparer.Ordinal);

        foreach (var (id, key) in keys)
        {
            if (String.IsNullOrWhiteSpace(id))
                throw new ArgumentException("A key id must not be empty.", nameof(keys));
            if (id.Contains(':', StringComparison.Ordinal))
                throw new ArgumentException($"Key id '{id}' must not contain ':' — it is the envelope separator.", nameof(keys));
            if (key is not { Length: KeySize })
                throw new ArgumentException($"Key '{id}' must be exactly {KeySize} bytes (AES-256).", nameof(keys));

            this.keys[id] = (byte[])key.Clone();
            this.nonceKeys[id] = HMACSHA256.HashData(key, NonceDerivationLabel);
        }

        if (!this.keys.ContainsKey(currentKeyId))
            throw new ArgumentException($"The current key id '{currentKeyId}' is not in the supplied key ring.", nameof(currentKeyId));

        this.CurrentKeyId = currentKeyId;
    }

    /// <summary>Convenience for a single-key ring — the common starting point before any rotation.</summary>
    public AesGcmDocumentEncryptor(string keyId, byte[] key)
        : this(keyId, new Dictionary<string, byte[]>(StringComparer.Ordinal) { [keyId] = key })
    {
    }

    public string CurrentKeyId { get; }

    /// <summary>Generates a 32-byte key suitable for this encryptor.</summary>
    public static byte[] GenerateKey() => RandomNumberGenerator.GetBytes(KeySize);

    public string Encrypt(ReadOnlySpan<byte> plaintext, EncryptionMode mode)
    {
        var key = this.keys[this.CurrentKeyId];

        // nonce | tag | ciphertext, so the envelope is one blob and the sizes are fixed and known.
        var payload = new byte[NonceSize + TagSize + plaintext.Length];
        var nonce = payload.AsSpan(0, NonceSize);
        var tag = payload.AsSpan(NonceSize, TagSize);
        var ciphertext = payload.AsSpan(NonceSize + TagSize);

        if (mode == EncryptionMode.Deterministic)
            DeriveNonce(this.nonceKeys[this.CurrentKeyId], plaintext, nonce);
        else
            RandomNumberGenerator.Fill(nonce);

        using var aes = new AesGcm(key, TagSize);
        aes.Encrypt(nonce, plaintext, ciphertext, tag);

        return EncryptionEnvelope.Format(this.CurrentKeyId, payload);
    }

    public byte[] Decrypt(string envelope)
    {
        var (keyId, payload) = EncryptionEnvelope.Parse(envelope);

        if (!this.keys.TryGetValue(keyId, out var key))
            throw new InvalidOperationException(
                $"This document was encrypted under key '{keyId}', which is not in the key ring. Add it to read the value, " +
                "or restore it from a backup taken before the key was retired.");

        if (payload.Length < NonceSize + TagSize)
            throw new CryptographicException("The encryption envelope is truncated.");

        var nonce = payload.AsSpan(0, NonceSize);
        var tag = payload.AsSpan(NonceSize, TagSize);
        var ciphertext = payload.AsSpan(NonceSize + TagSize);
        var plaintext = new byte[ciphertext.Length];

        using var aes = new AesGcm(key, TagSize);
        aes.Decrypt(nonce, ciphertext, tag, plaintext);   // throws CryptographicException when tampered with
        return plaintext;
    }

    // The nonce is a MAC of the plaintext under a key derived from (but not equal to) the data key, so the same
    // value encrypts identically under one key and differently under another.
    static void DeriveNonce(byte[] nonceKey, ReadOnlySpan<byte> plaintext, Span<byte> nonce)
    {
        Span<byte> mac = stackalloc byte[32];
        HMACSHA256.HashData(nonceKey, plaintext, mac);
        mac[..NonceSize].CopyTo(nonce);
    }

    /// <summary>Utility for callers holding keys as text (base64 or UTF-8 passphrase material is NOT accepted —
    /// this parses base64 only, so a short or mistyped key fails immediately rather than silently weakening).</summary>
    public static byte[] KeyFromBase64(string base64)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(base64);
        var key = Convert.FromBase64String(base64);
        if (key.Length != KeySize)
            throw new ArgumentException($"An AES-256 key must be exactly {KeySize} bytes; this one is {key.Length}.", nameof(base64));
        return key;
    }

}
