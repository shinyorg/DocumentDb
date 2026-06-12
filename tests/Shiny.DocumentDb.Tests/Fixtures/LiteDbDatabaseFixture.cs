using System.Linq.Expressions;
using Shiny.DocumentDb.LiteDb;

namespace Shiny.DocumentDb.Tests.Fixtures;

public class LiteDbDatabaseFixture : IDocumentStoreFixture, ITemporalDocumentStoreFixture
{
    public ITemporalDocumentStore CreateTemporalStore(string tableName, Action<TemporalOptions>? configure = null, Func<string>? actor = null)
    {
        var opts = new LiteDbDocumentStoreOptions
        {
            ConnectionString = $"Filename={Path.GetTempFileName()};Connection=direct",
            CollectionName = tableName
        };
        opts.MapTemporal<VersionedUser>(o =>
        {
            configure?.Invoke(o);
            if (actor != null)
                o.CaptureActor = actor;
        });
        opts.MapTemporal<MergeDoc>();
        return new LiteDbDocumentStore(opts);
    }

    public IDocumentStore CreateStore(string tableName)
        => new LiteDbDocumentStore(new LiteDbDocumentStoreOptions
        {
            ConnectionString = $"Filename={Path.GetTempFileName()};Connection=direct",
            CollectionName = tableName
        });

    public IDocumentStore CreateStoreWithFilter<T>(string tableName, Expression<Func<T, bool>> filter) where T : class
    {
        var opts = new LiteDbDocumentStoreOptions
        {
            ConnectionString = $"Filename={Path.GetTempFileName()};Connection=direct",
            CollectionName = tableName
        };
        opts.AddQueryFilter(filter);
        return new LiteDbDocumentStore(opts);
    }

    public IDocumentStore CreateStoreWithNamedFilter<T>(string tableName, string filterName, Expression<Func<T, bool>> filter) where T : class
    {
        var opts = new LiteDbDocumentStoreOptions
        {
            ConnectionString = $"Filename={Path.GetTempFileName()};Connection=direct",
            CollectionName = tableName
        };
        opts.AddQueryFilter(filterName, filter);
        return new LiteDbDocumentStore(opts);
    }

    public IDocumentStore CreateStoreWithVersion<T>(string tableName, Expression<Func<T, int>> versionProperty) where T : class
    {
        var opts = new LiteDbDocumentStoreOptions
        {
            ConnectionString = $"Filename={Path.GetTempFileName()};Connection=direct",
            CollectionName = tableName
        };
        opts.MapVersionProperty(versionProperty);
        return new LiteDbDocumentStore(opts);
    }
}
