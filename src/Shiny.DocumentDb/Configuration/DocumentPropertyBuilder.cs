using System.Linq.Expressions;
using Shiny.DocumentDb.Internal;

namespace Shiny.DocumentDb;

/// <summary>
/// Options for one property of a document type, obtained from
/// <see cref="DocumentTypeBuilder{T}.MapProperty"/>:
/// <code>cfg.MapProperty(x => x.Ssn, p => p.Encrypt(EncryptionMode.Deterministic));</code>
/// <para>
/// Deliberately small. It carries field-level encryption today and is the seam provider packages extend with
/// their own per-property concerns, so those arrive without widening the core surface.
/// </para>
/// </summary>
public sealed class DocumentPropertyBuilder<T> where T : class
{
    internal DocumentPropertyBuilder(IDocumentStoreOptions options, Expression<Func<T, object?>> property)
    {
        this.Options = options;
        this.Property = property;
    }

    /// <summary>The store options being configured — the seam extension methods build on.</summary>
    public IDocumentStoreOptions Options { get; }

    /// <summary>The property being configured.</summary>
    public Expression<Func<T, object?>> Property { get; }

    /// <summary>
    /// Encrypts this property at rest on every write and decrypts it on every read.
    /// <para>
    /// <see cref="EncryptionMode.Randomized"/> (the default) is opaque: the property cannot be filtered,
    /// sorted, indexed, grouped or full-text searched, and attempting it throws.
    /// <see cref="EncryptionMode.Deterministic"/> keeps <b>equality</b> filters working — at the cost of
    /// leaking which documents share a value. See <see cref="EncryptionMode"/> before choosing.
    /// </para>
    /// <para>
    /// Values written before the property was mapped read back as-is, so this can be turned on for a
    /// populated store; convert the backlog with <see cref="EncryptionMaintenanceExtensions.RewrapAsync{T}"/>.
    /// </para>
    /// </summary>
    /// <param name="mode">How the value is encrypted, and therefore whether it stays queryable.</param>
    /// <param name="encryptor">The key ring. Defaults to the one set with <c>options.UseEncryptor(…)</c>.</param>
    public DocumentPropertyBuilder<T> Encrypt(EncryptionMode mode = EncryptionMode.Randomized, IDocumentEncryptor? encryptor = null)
    {
        EncryptionRegistry.Map(this.Options, this.Property, mode, encryptor);
        return this;
    }
}
