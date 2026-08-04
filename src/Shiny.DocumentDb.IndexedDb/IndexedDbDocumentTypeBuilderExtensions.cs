namespace Shiny.DocumentDb.IndexedDb;

/// <summary>IndexedDB's vocabulary for a document type's storage unit — the same thing <c>cfg.Table</c> sets.</summary>
public static class IndexedDbDocumentTypeBuilderExtensions
{
    /// <summary>
    /// Gives this document type its own IndexedDB object store. Types without one share the store's
    /// <see cref="IndexedDbDocumentStoreOptions.StoreName"/>.
    /// </summary>
    /// <example>
    /// <code>options.ConfigureDocument&lt;Patient&gt;(cfg => cfg.ToStore("patients"));</code>
    /// </example>
    public static DocumentTypeBuilder<T> ToStore<T>(this DocumentTypeBuilder<T> cfg, string storeName) where T : class
    {
        ArgumentNullException.ThrowIfNull(cfg);
        cfg.Table = storeName;
        return cfg;
    }

    /// <summary>Gives this document type its own object store, named after the type.</summary>
    public static DocumentTypeBuilder<T> ToStore<T>(this DocumentTypeBuilder<T> cfg) where T : class
    {
        ArgumentNullException.ThrowIfNull(cfg);
        cfg.Table = cfg.TypeName;
        return cfg;
    }
}
