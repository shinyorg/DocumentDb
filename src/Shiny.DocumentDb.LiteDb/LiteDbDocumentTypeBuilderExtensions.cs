namespace Shiny.DocumentDb.LiteDb;

/// <summary>
/// LiteDB's vocabulary for a document type's storage unit. <c>cfg.Table</c> does the same thing on every
/// provider; this spells it the way LiteDB does.
/// </summary>
public static class LiteDbDocumentTypeBuilderExtensions
{
    /// <summary>
    /// Gives this document type its own LiteDB collection. Types without one share the store's
    /// <see cref="LiteDbDocumentStoreOptions.CollectionName"/>.
    /// </summary>
    /// <example>
    /// <code>options.ConfigureDocument&lt;Patient&gt;(cfg => cfg.ToCollection("patients"));</code>
    /// </example>
    public static DocumentTypeBuilder<T> ToCollection<T>(this DocumentTypeBuilder<T> cfg, string collectionName) where T : class
    {
        ArgumentNullException.ThrowIfNull(cfg);
        cfg.Table = collectionName;
        return cfg;
    }

    /// <summary>
    /// Gives this document type its own LiteDB collection, named after the type per the store's
    /// <see cref="TypeNameResolution"/>.
    /// </summary>
    public static DocumentTypeBuilder<T> ToCollection<T>(this DocumentTypeBuilder<T> cfg) where T : class
    {
        ArgumentNullException.ThrowIfNull(cfg);
        cfg.Table = cfg.TypeName;
        return cfg;
    }
}
