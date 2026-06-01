using System.Linq.Expressions;
using Shiny.DocumentDb.LiteDb;

namespace Shiny.DocumentDb.Tests.Fixtures;

public class LiteDbDatabaseFixture : IDocumentStoreFixture
{
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
