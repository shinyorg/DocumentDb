namespace Shiny.DocumentDb.Firestore;

/// <summary>Firestore's vocabulary for a document type's storage unit — the same thing <c>cfg.Table</c> sets.</summary>
public static class FirestoreDocumentTypeBuilderExtensions
{
    /// <summary>
    /// Overrides the Firestore collection this document type lives in. Defaults to the resolved type name;
    /// <see cref="FirestoreDocumentStoreOptions.CollectionPrefix"/> still applies on top.
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
}
