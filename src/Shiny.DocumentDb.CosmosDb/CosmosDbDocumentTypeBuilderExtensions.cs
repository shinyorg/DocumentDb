using System.Diagnostics.CodeAnalysis;

namespace Shiny.DocumentDb.CosmosDb;

/// <summary>
/// Cosmos DB's vocabulary for a document type's storage unit. <c>cfg.Table</c> does the same thing on every
/// provider; this spells it the way Cosmos does.
/// </summary>
public static class CosmosDbDocumentTypeBuilderExtensions
{
    /// <summary>
    /// Gives this document type its own Cosmos container. Types without one share the store's
    /// <see cref="CosmosDbDocumentStoreOptions.ContainerName"/>.
    /// </summary>
    /// <example>
    /// <code>options.ConfigureDocument&lt;Patient&gt;(cfg => cfg.ToContainer("patients"));</code>
    /// </example>
    public static DocumentTypeBuilder<T> ToContainer<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)] T>(this DocumentTypeBuilder<T> cfg, string containerName) where T : class
    {
        ArgumentNullException.ThrowIfNull(cfg);
        cfg.Table = containerName;
        return cfg;
    }

    /// <summary>
    /// Gives this document type its own Cosmos container, named after the type per the store's
    /// <see cref="TypeNameResolution"/>.
    /// </summary>
    public static DocumentTypeBuilder<T> ToContainer<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)] T>(this DocumentTypeBuilder<T> cfg) where T : class
    {
        ArgumentNullException.ThrowIfNull(cfg);
        cfg.Table = cfg.TypeName;
        return cfg;
    }
}
