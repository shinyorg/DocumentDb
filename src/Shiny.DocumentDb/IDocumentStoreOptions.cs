using System.Linq.Expressions;
using System.Text.Json;

namespace Shiny.DocumentDb;

/// <summary>
/// The provider-agnostic slice of a store's options — implemented by <see cref="DocumentStoreOptions"/> and by
/// every provider options class. It exists so a cross-cutting feature can be written <b>once</b> against any
/// store instead of once per provider: <c>ConfigureDocument</c>, <c>UseEncryptor</c> and
/// <c>ConfigureJsonSchemaValidation</c> are all single extension methods over this interface.
/// <para>
/// Per-type mapping lives on <see cref="Mappings"/> (reached through <see cref="DocumentTypeBuilder{T}"/>);
/// what remains here is store-level. Reach for the interface when you are writing a feature that must work on
/// any provider; reach for the concrete options type otherwise.
/// </para>
/// </summary>
public interface IDocumentStoreOptions
{
    /// <summary>
    /// The per-type mapping state this store's options carries. Every provider holds one, which is what lets
    /// <see cref="DocumentTypeBuilder{T}"/> — and therefore <c>ConfigureDocument</c> — be written once instead
    /// of once per backend. Whether a mapped feature is actually supported by the chosen provider is settled by
    /// the configuration validation pass when the store is built, not here.
    /// </summary>
    DocumentMappingRegistry Mappings { get; }

    /// <summary>How a document type's name is derived when a table/collection/container is not named explicitly.</summary>
    TypeNameResolution TypeNameResolution { get; }

    /// <summary>
    /// What the backend behind these options can do. <see cref="DocumentConfigurationValidator"/> reads it when
    /// the store is built, so a mapping the provider cannot honor is one clear startup error rather than a
    /// surprise later.
    /// </summary>
    DocumentStoreCapabilities Capabilities { get; }

    /// <summary>Registers a per-document write interceptor. Registration order = execution order.</summary>
    IDocumentStoreOptions AddInterceptor(IDocumentInterceptor interceptor);

    /// <summary>Registers a set-based (bulk) write interceptor. Registration order = execution order.</summary>
    IDocumentStoreOptions AddBulkInterceptor(IDocumentBulkInterceptor interceptor);

    /// <summary>
    /// The serializer options this store will use — the same instance as the concrete options class's
    /// <c>JsonSerializerOptions</c>. <c>null</c> until the caller sets one (the store then builds its default);
    /// use <see cref="EnsureSerializerOptions"/> to get a live instance to configure.
    /// </summary>
    JsonSerializerOptions? SerializerOptions { get; set; }

    /// <summary>
    /// The serializer options, materializing (and storing) the store's default when none was supplied — the seam
    /// for serialization-level features such as field-level encryption, which need to attach a
    /// <see cref="System.Text.Json.Serialization.Metadata.JsonTypeInfo"/> modifier before the store is built.
    /// </summary>
    JsonSerializerOptions EnsureSerializerOptions()
        => this.SerializerOptions ??= new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
            WriteIndented = false
        };
}
