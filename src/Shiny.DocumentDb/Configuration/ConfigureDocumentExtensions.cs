namespace Shiny.DocumentDb;

/// <summary>Configures a document type on any store's options — one entry point for every provider.</summary>
public static class ConfigureDocumentExtensions
{
    /// <summary>
    /// Configures everything about one document type in a single block, with the type named once:
    /// <code>
    /// options.ConfigureDocument&lt;Patient&gt;(cfg =>
    /// {
    ///     cfg.Table = "Patients";
    ///     cfg.MapIdProperty(x => x.Id);
    ///     cfg.AddQueryFilter(x => !x.IsDeleted);
    ///     cfg.MapTemporal(o => o.Retention = TimeSpan.FromDays(90));
    /// });
    /// </code>
    /// <para>
    /// Calling it more than once for the same type is <b>additive</b> — a later block adds to what an earlier
    /// one (or a generated <c>[Document]</c> declaration) already configured. Mappings that a type can only
    /// have one of — spatial, vector, full-text — still refuse a second declaration.
    /// </para>
    /// </summary>
    public static IDocumentStoreOptions ConfigureDocument<T>(this IDocumentStoreOptions options, Action<DocumentTypeBuilder<T>> configure)
        where T : class
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(configure);

        configure(new DocumentTypeBuilder<T>(options));
        return options;
    }

    /// <summary>
    /// Configures several document types at once through a <see cref="DocumentModelBuilder"/> — the same root
    /// a generated <c>DocumentContext</c>'s <c>OnConfiguring</c> hook receives.
    /// </summary>
    public static IDocumentStoreOptions ConfigureModel(this IDocumentStoreOptions options, Action<DocumentModelBuilder> configure)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(configure);

        configure(new DocumentModelBuilder(options));
        return options;
    }
}
