using System.Diagnostics.CodeAnalysis;

namespace Shiny.DocumentDb.MongoDb;

/// <summary>
/// MongoDB's vocabulary for a document type's storage unit. <c>cfg.Table</c> does the same thing on every
/// provider; this spells it the way MongoDB does.
/// </summary>
public static class MongoDbDocumentTypeBuilderExtensions
{
    /// <summary>
    /// Gives this document type its own MongoDB collection. Types without one share the store's
    /// <see cref="MongoDbDocumentStoreOptions.CollectionName"/>.
    /// </summary>
    /// <example>
    /// <code>options.ConfigureDocument&lt;Patient&gt;(cfg => cfg.ToCollection("patients"));</code>
    /// </example>
    public static DocumentTypeBuilder<T> ToCollection<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)] T>(this DocumentTypeBuilder<T> cfg, string collectionName) where T : class
    {
        ArgumentNullException.ThrowIfNull(cfg);
        cfg.Table = collectionName;
        return cfg;
    }

    /// <summary>
    /// Gives this document type its own MongoDB collection, named after the type per the store's
    /// <see cref="TypeNameResolution"/>.
    /// </summary>
    public static DocumentTypeBuilder<T> ToCollection<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)] T>(this DocumentTypeBuilder<T> cfg) where T : class
    {
        ArgumentNullException.ThrowIfNull(cfg);
        cfg.Table = cfg.TypeName;
        return cfg;
    }
}
