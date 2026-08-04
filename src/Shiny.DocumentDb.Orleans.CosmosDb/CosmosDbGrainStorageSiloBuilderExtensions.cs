using Shiny.DocumentDb;
using Shiny.DocumentDb.CosmosDb;
using Shiny.DocumentDb.Orleans;

namespace Orleans.Hosting;

public static class CosmosDbGrainStorageSiloBuilderExtensions
{
    /// <summary>
    /// Adds an Azure Cosmos DB-backed Orleans grain storage provider. Grain state is stored through
    /// <c>CosmosDbDocumentStore</c>, whose native <c>IfMatchEtag</c> CAS honors the Orleans ETag.
    /// <para>
    /// Note: the underlying Cosmos provider partitions by document type name. For large grain populations
    /// of a single type, weigh the per-logical-partition limits before using in production.
    /// </para>
    /// </summary>
    public static ISiloBuilder AddCosmosDbGrainStorage(
        this ISiloBuilder builder,
        string name,
        string connectionString,
        string databaseName,
        Action<DocumentDbGrainStorageOptions>? configure = null
    ) => builder.AddCosmosDbGrainStorage(
        name,
        () => new CosmosDbDocumentStoreOptions { ConnectionString = connectionString, DatabaseName = databaseName },
        configure);

    /// <summary>
    /// Adds an Azure Cosmos DB-backed Orleans grain storage provider, building the underlying store options
    /// via <paramref name="storeOptionsFactory"/> (use this overload to set <c>CosmosClient</c>,
    /// <c>DefaultThroughput</c>, etc.). The grain-state container mapping and version property are applied
    /// automatically.
    /// </summary>
    public static ISiloBuilder AddCosmosDbGrainStorage(
        this ISiloBuilder builder,
        string name,
        Func<CosmosDbDocumentStoreOptions> storeOptionsFactory,
        Action<DocumentDbGrainStorageOptions>? configure = null
    ) => builder.AddDocumentDbGrainStorage(name, o =>
    {
        configure?.Invoke(o);
        var container = o.TableName ?? $"orleans_{name.ToLowerInvariant()}";
        o.StoreFactory = _ =>
        {
            var so = storeOptionsFactory();
            so.ConfigureDocument<GrainStateRecord>(cfg =>
            {
                cfg.ToContainer(container);
                cfg.MapVersionProperty(x => x.Version);
            });
            return new CosmosDbDocumentStore(so);
        };
    });
}
