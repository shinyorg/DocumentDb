using System.Security.Cryptography;
using System.Text;

namespace ShinyDocDbMyAdmin.Services;

/// <summary>
/// AES-GCM protection for the secret-bearing parts of a saved profile. The key comes from
/// <c>ShinyDocDbMyAdmin:SecretKey</c> (base64 or passphrase) or <c>SHINYDOCDBMYADMIN_KEY</c>; with neither set a
/// random key is generated once into the data directory with owner-only permissions.
/// </summary>
/// <remarks>
/// This protects stored connection strings from casual disclosure - a backup, a synced folder, a
/// shoulder-surf. It is not a defence against someone who can already read the data directory,
/// because the key sits beside the database unless you supply one out of band. Supply one in any
/// shared or multi-user deployment.
/// </remarks>
public sealed class SecretProtector
{
    const string Prefix = "enc:v1:";
    readonly byte[] key;

    public SecretProtector(IConfiguration configuration, AppPaths paths, ILogger<SecretProtector> logger)
    {
        var configured = configuration["ShinyDocDbMyAdmin:SecretKey"]
                         ?? Environment.GetEnvironmentVariable("SHINYDOCDBMYADMIN_KEY");

        if (!string.IsNullOrWhiteSpace(configured))
        {
            this.key = DeriveKey(configured);
            this.KeySource = "configuration";
        }
        else
        {
            this.key = LoadOrCreateKeyFile(paths.KeyFilePath);
            this.KeySource = "generated key file";
            logger.LogInformation(
                "No ShinyDocDbMyAdmin:SecretKey configured; using the generated key at {Path}. Set a key out of band for shared deployments.",
                paths.KeyFilePath);
        }
    }

    /// <summary>Where the key came from - surfaced in the UI so the security posture is not a mystery.</summary>
    public string KeySource { get; }

    public string Protect(string? plaintext)
    {
        if (string.IsNullOrEmpty(plaintext))
            return "";

        var nonce = RandomNumberGenerator.GetBytes(AesGcm.NonceByteSizes.MaxSize);
        var plainBytes = Encoding.UTF8.GetBytes(plaintext);
        var cipher = new byte[plainBytes.Length];
        var tag = new byte[AesGcm.TagByteSizes.MaxSize];

        using var aes = new AesGcm(this.key, tag.Length);
        aes.Encrypt(nonce, plainBytes, cipher, tag);

        return Prefix + Convert.ToBase64String([.. nonce, .. tag, .. cipher]);
    }

    public string Unprotect(string? value)
    {
        if (string.IsNullOrEmpty(value))
            return "";

        // Values written before a key existed - or hand-edited into the store - pass through as-is.
        if (!value.StartsWith(Prefix, StringComparison.Ordinal))
            return value;

        var blob = Convert.FromBase64String(value[Prefix.Length..]);
        var nonceLength = AesGcm.NonceByteSizes.MaxSize;
        var tagLength = AesGcm.TagByteSizes.MaxSize;
        if (blob.Length < nonceLength + tagLength)
            throw new CryptographicException("Stored secret is truncated.");

        var nonce = blob.AsSpan(0, nonceLength);
        var tag = blob.AsSpan(nonceLength, tagLength);
        var cipher = blob.AsSpan(nonceLength + tagLength);
        var plain = new byte[cipher.Length];

        using var aes = new AesGcm(this.key, tagLength);
        aes.Decrypt(nonce, cipher, tag, plain);
        return Encoding.UTF8.GetString(plain);
    }

    static byte[] DeriveKey(string configured)
    {
        // A 32-byte base64 value is used directly; anything else is treated as a passphrase.
        if (configured.Length is 44 && Convert.TryFromBase64String(configured, new byte[33], out var written) && written == 32)
            return Convert.FromBase64String(configured);

        return SHA256.HashData(Encoding.UTF8.GetBytes(configured));
    }

    static byte[] LoadOrCreateKeyFile(string path)
    {
        if (File.Exists(path))
        {
            var existing = Convert.FromBase64String(File.ReadAllText(path).Trim());
            if (existing.Length == 32)
                return existing;
        }

        var generated = RandomNumberGenerator.GetBytes(32);
        File.WriteAllText(path, Convert.ToBase64String(generated));
        if (!OperatingSystem.IsWindows())
            File.SetUnixFileMode(path, UnixFileMode.UserRead | UnixFileMode.UserWrite);

        return generated;
    }
}
