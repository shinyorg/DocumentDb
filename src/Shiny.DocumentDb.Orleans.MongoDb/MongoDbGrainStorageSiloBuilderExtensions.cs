using Shiny.DocumentDb;
using Shiny.DocumentDb.MongoDb;
using Shiny.DocumentDb.Orleans;

namespace Orleans.Hosting;

public static class MongoDbGrainStorageSiloBuilderExtensions
{
    /// <summary>
    /// Adds a MongoDB-backed Orleans grain storage provider. Grain state is stored through
    /// <c>MongoDbDocumentStore</c>, whose atomic version-filter CAS honors the Orleans ETag.
    /// </summary>
    public static ISiloBuilder AddMongoDbGrainStorage(
        this ISiloBuilder builder,
        string name,
        string connectionString,
        string databaseName,
        Action<DocumentDbGrainStorageOptions>? configure = null
    ) => builder.AddMongoDbGrainStorage(
        name,
        () => new MongoDbDocumentStoreOptions { ConnectionString = connectionString, DatabaseName = databaseName },
        configure);

    /// <summary>
    /// Adds a MongoDB-backed Orleans grain storage provider, building the underlying store options via
    /// <paramref name="storeOptionsFactory"/> (use this overload to set <c>MongoClient</c>,
    /// <c>JsonSerializerOptions</c>, etc.). The grain-state collection mapping and version property are
    /// applied automatically.
    /// </summary>
    public static ISiloBuilder AddMongoDbGrainStorage(
        this ISiloBuilder builder,
        string name,
        Func<MongoDbDocumentStoreOptions> storeOptionsFactory,
        Action<DocumentDbGrainStorageOptions>? configure = null
    ) => builder.AddDocumentDbGrainStorage(name, o =>
    {
        configure?.Invoke(o);
        var collection = o.TableName ?? $"orleans_{name.ToLowerInvariant()}";
        o.StoreFactory = _ =>
        {
            var so = storeOptionsFactory();
            so.ConfigureDocument<GrainStateRecord>(cfg =>
            {
                cfg.ToCollection(collection);
                cfg.MapVersionProperty(x => x.Version);
            });
            return new MongoDbDocumentStore(so);
        };
    });
}
