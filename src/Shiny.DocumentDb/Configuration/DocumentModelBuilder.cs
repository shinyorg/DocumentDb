namespace Shiny.DocumentDb;

/// <summary>
/// The root of a store's document model — one <see cref="DocumentTypeBuilder{T}"/> per type. Handed to a
/// generated <c>DocumentContext</c>'s <c>OnConfiguring</c> hook so a context-based app declares its mappings
/// next to its <c>[Document]</c> list rather than inside <c>AddDocumentStore</c>:
/// <code>
/// public partial class AppContext
/// {
///     static partial void OnConfiguring(DocumentModelBuilder model)
///         => model.Document&lt;Patient&gt;(cfg => cfg.MapTemporal());
/// }
/// </code>
/// </summary>
public sealed class DocumentModelBuilder
{
    /// <summary>Builds a model over a store's options. The generated <c>ConfigureModel</c> calls this.</summary>
    public DocumentModelBuilder(IDocumentStoreOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        this.Options = options;
    }

    /// <summary>The store options being configured.</summary>
    public IDocumentStoreOptions Options { get; }

    /// <summary>The builder for one document type. Repeat calls are additive — they configure the same type.</summary>
    public DocumentTypeBuilder<T> Document<T>() where T : class => new(this.Options);

    /// <summary>Configures one document type inline, and returns this builder so types can be chained.</summary>
    public DocumentModelBuilder Document<T>(Action<DocumentTypeBuilder<T>> configure) where T : class
    {
        ArgumentNullException.ThrowIfNull(configure);
        configure(new DocumentTypeBuilder<T>(this.Options));
        return this;
    }
}
