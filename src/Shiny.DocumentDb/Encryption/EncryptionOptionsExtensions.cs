namespace Shiny.DocumentDb;

/// <summary>
/// Field-level encryption, configured on any store's options — one extension for every provider. Encryption is a
/// serialization-level transform (a <c>JsonTypeInfo</c> modifier installs an encrypting converter on the mapped
/// property), so it needs no provider support: the value is ciphertext everywhere it is stored, including temporal
/// history, backups and replicas, and plaintext everywhere the document is materialized.
/// <para>
/// Set the key ring here; map the properties inside each type's <c>ConfigureDocument</c> block:
/// <code>
/// options.UseEncryptor(new AesGcmDocumentEncryptor("k1", key));
/// options.ConfigureDocument&lt;Patient&gt;(cfg => cfg.MapProperty(x => x.Ssn, p => p.Encrypt(EncryptionMode.Deterministic)));
/// </code>
/// </para>
/// </summary>
public static class EncryptionOptionsExtensions
{
    /// <summary>
    /// The encryptor used by every <see cref="DocumentPropertyBuilder{T}.Encrypt"/> on this store that does not
    /// name its own. Call once while configuring the store.
    /// </summary>
    public static IDocumentStoreOptions UseEncryptor(this IDocumentStoreOptions options, IDocumentEncryptor encryptor)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(encryptor);

        Internal.EncryptionRegistry.SetDefaultEncryptor(options, encryptor);
        return options;
    }
}
