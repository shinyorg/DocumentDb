using System.Linq.Expressions;

namespace Shiny.DocumentDb.Redis;

/// <summary>The Redis per-type mappings that shape its RediSearch index.</summary>
public static class RedisDocumentTypeBuilderExtensions
{
    /// <summary>
    /// Declares a queryable field on this type's RediSearch index, so <c>Where</c> / <c>OrderBy</c> over it
    /// push down to <c>FT.SEARCH</c>/<c>FT.AGGREGATE</c>. Numeric CLR types index as NUMERIC (ranges + sort);
    /// everything else as TAG (exact match + <c>in</c>). Undeclared fields still work but filter client-side.
    /// Map before first use.
    /// </summary>
    /// <example>
    /// <code>options.ConfigureDocument&lt;Patient&gt;(cfg => cfg.MapIndexedProperty(x => x.Status));</code>
    /// </example>
    public static DocumentTypeBuilder<T> MapIndexedProperty<T>(this DocumentTypeBuilder<T> cfg, Expression<Func<T, object>> property) where T : class
    {
        ArgumentNullException.ThrowIfNull(cfg);
        var options = cfg.Options as RedisDocumentStoreOptions
            ?? throw new InvalidOperationException(
                $"This is a Redis mapping, but the store is configured with '{cfg.Options.GetType().Name}'.");

        options.MapIndexedProperty(property);
        return cfg;
    }
}
