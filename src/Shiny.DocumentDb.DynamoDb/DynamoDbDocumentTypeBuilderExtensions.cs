using System.Linq.Expressions;

namespace Shiny.DocumentDb.DynamoDb;

/// <summary>
/// The DynamoDB per-type mappings. Every type lives in one table there, so a type is separated by its
/// <b>partition key</b> rather than by a table of its own — and a property must be declared indexed before a
/// predicate over it can push down to the service.
/// </summary>
public static class DynamoDbDocumentTypeBuilderExtensions
{
    /// <summary>
    /// Overrides the partition key this document type is stored under. Defaults to the resolved type name.
    /// </summary>
    /// <example>
    /// <code>options.ConfigureDocument&lt;Patient&gt;(cfg => cfg.ToPartition("patients"));</code>
    /// </example>
    public static DocumentTypeBuilder<T> ToPartition<T>(this DocumentTypeBuilder<T> cfg, string partitionKey) where T : class
    {
        Require(cfg).MapTypeToPartition<T>(partitionKey);
        return cfg;
    }

    /// <summary>
    /// Declares a property as indexed, so <c>Where</c> / <c>OrderBy</c> over it push down to the service
    /// instead of being filtered client-side. Map before first use.
    /// </summary>
    /// <example>
    /// <code>options.ConfigureDocument&lt;Patient&gt;(cfg => cfg.MapIndexedProperty(x => x.Mrn));</code>
    /// </example>
    public static DocumentTypeBuilder<T> MapIndexedProperty<T>(this DocumentTypeBuilder<T> cfg, Expression<Func<T, object>> property) where T : class
    {
        Require(cfg).MapIndexedProperty(property);
        return cfg;
    }

    static DynamoDbDocumentStoreOptions Require<T>(DocumentTypeBuilder<T> cfg) where T : class
    {
        ArgumentNullException.ThrowIfNull(cfg);
        return cfg.Options as DynamoDbDocumentStoreOptions
            ?? throw new InvalidOperationException(
                $"This is a DynamoDB mapping, but the store is configured with '{cfg.Options.GetType().Name}'.");
    }
}
