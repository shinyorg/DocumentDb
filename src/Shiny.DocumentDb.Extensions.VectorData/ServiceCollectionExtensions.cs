using Microsoft.Extensions.AI;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.VectorData;

namespace Shiny.DocumentDb.Extensions.VectorData;

/// <summary>Registers the MEVD <see cref="VectorStore"/> face of a document store.</summary>
public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Registers a document store <i>and</i> the <see cref="VectorStore"/> over it, so the AI stack can resolve
    /// MEVD's abstraction while the rest of the app keeps using <see cref="IDocumentStore"/> — one store, one
    /// connection pool, two faces. List your record types inside <paramref name="configure"/> with
    /// <c>MapVectorRecord&lt;T&gt;</c>.
    /// </summary>
    /// <example>
    /// <code>
    /// services.AddDocumentDbVectorStore(o =>
    /// {
    ///     o.DatabaseProvider = new SqliteDatabaseProvider("Data Source=app.db") { EnableVectorExtension = true };
    ///     o.MapVectorRecord&lt;Note&gt;();
    /// });
    /// </code>
    /// </example>
    public static IServiceCollection AddDocumentDbVectorStore(this IServiceCollection services, Action<DocumentStoreOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configure);

        services.AddDocumentStore(configure);
        return services.AddDocumentDbVectorStore();
    }

    /// <summary>
    /// Registers the <see cref="VectorStore"/> over the <see cref="IDocumentStore"/> already in the container —
    /// the overload for a store registered elsewhere, or one on a provider with its own options class (MongoDB,
    /// CosmosDB, Redis). An <see cref="IEmbeddingGenerator{TInput, TEmbedding}"/> of
    /// <c>string</c>/<c>Embedding&lt;float&gt;</c> is picked up when one is registered, which is what lets
    /// <c>SearchAsync</c> take a text query.
    /// </summary>
    public static IServiceCollection AddDocumentDbVectorStore(this IServiceCollection services)
    {
        ArgumentNullException.ThrowIfNull(services);

        services.TryAddSingleton<VectorStore>(sp => new DocumentDbVectorStore(
            sp.GetRequiredService<IDocumentStore>(),
            sp.GetService<IEmbeddingGenerator<string, Embedding<float>>>()
        ));
        return services;
    }
}
