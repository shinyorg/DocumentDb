namespace Shiny.DocumentDb;

/// <summary>How a mapped property's value is encrypted, and therefore what you can still do with it.</summary>
public enum EncryptionMode
{
    /// <summary>
    /// A fresh random nonce per write, so the same plaintext produces different ciphertext every time. Nothing
    /// can be inferred from the stored values — and nothing can be queried, indexed, sorted or grouped by them.
    /// The default, and the right choice unless you specifically need equality.
    /// </summary>
    Randomized,

    /// <summary>
    /// A nonce derived from the plaintext, so the same plaintext always produces the same ciphertext. That makes
    /// <b>equality</b> predicates and ordinary JSON indexes work — the query's constant is encrypted the same way
    /// before it reaches the store.
    /// <para>
    /// <b>It also leaks equality and frequency:</b> anyone with read access to the table can tell which documents
    /// share a value, and can count how often each value occurs. Choose it deliberately, per property.
    /// </para>
    /// Supported on <see cref="String"/> properties only — a comparison against a non-string property could not
    /// be rewritten into a ciphertext comparison without changing the expression's type.
    /// </summary>
    Deterministic
}

/// <summary>
/// Encrypts and decrypts a single property value. One instance is a key ring: it writes under
/// <see cref="CurrentKeyId"/> and can read anything written under a key it still holds, which is what makes
/// rotation possible. Implementations must be thread-safe.
/// </summary>
/// <remarks>
/// Implement this to put a KMS / Key Vault / HSM behind the mapping. <see cref="AesGcmDocumentEncryptor"/> is the
/// in-process implementation over caller-supplied key material.
/// </remarks>
public interface IDocumentEncryptor
{
    /// <summary>The key id stamped into every envelope this encryptor writes. Must not contain <c>':'</c>.</summary>
    string CurrentKeyId { get; }

    /// <summary>Encrypts <paramref name="plaintext"/> and returns the self-describing envelope to store.</summary>
    string Encrypt(ReadOnlySpan<byte> plaintext, EncryptionMode mode);

    /// <summary>
    /// Decrypts an envelope produced by <see cref="Encrypt"/>. Throws
    /// <see cref="System.Security.Cryptography.CryptographicException"/> when the envelope was tampered with, and
    /// <see cref="InvalidOperationException"/> when its key is no longer held.
    /// </summary>
    byte[] Decrypt(string envelope);
}
