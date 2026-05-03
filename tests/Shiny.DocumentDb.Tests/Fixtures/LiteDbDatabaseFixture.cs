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
}
