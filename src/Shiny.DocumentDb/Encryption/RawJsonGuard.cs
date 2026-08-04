namespace Shiny.DocumentDb.Internal;

/// <summary>
/// Keeps the raw JSON terminals off types with encrypted properties.
/// </summary>
/// <remarks>
/// Field-level encryption is a <c>JsonTypeInfo</c> modifier, so the ciphertext is what is actually persisted and
/// the decrypt happens on the way back through the typed deserialize. A raw read skips that step by definition:
/// on a store that hands back the persisted body it would emit ciphertext, and on a store that re-serializes
/// from <c>T</c> it would emit plaintext — the same call returning different content per provider. Throwing is
/// the only answer that is right on both, and it fails at the terminal rather than in whatever the caller was
/// piping the JSON into.
/// </remarks>
static class RawJsonGuard
{
    /// <summary>Whether the raw terminals can read <paramref name="documentType"/> at all — what
    /// <see cref="IDocumentQuery{T}.SupportsRawJson"/> reports, so a caller can choose the lane instead of
    /// catching the throw.</summary>
    public static bool IsSupported(Type documentType) => EncryptionRegistry.For(documentType).Count == 0;

    public static void Ensure(Type documentType)
    {
        var encrypted = EncryptionRegistry.For(documentType);
        if (encrypted.Count == 0)
            return;

        var names = String.Join(", ", encrypted.Select(p => $"{documentType.Name}.{p.ClrName}").Order(StringComparer.Ordinal));
        throw new NotSupportedException(
            $"The raw JSON terminals cannot read '{documentType.Name}': the stored body holds ciphertext for {names}, " +
            "and only the typed terminals (ToList, FirstOrDefault, …) decrypt on read. Read it typed, or stop mapping " +
            "those properties for encryption.");
    }
}
