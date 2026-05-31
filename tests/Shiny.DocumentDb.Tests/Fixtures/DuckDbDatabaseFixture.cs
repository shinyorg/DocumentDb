using Shiny.DocumentDb.DuckDb;

namespace Shiny.DocumentDb.Tests.Fixtures;

public class DuckDbDatabaseFixture : IDatabaseFixture, IDocumentStoreFixture
{
    public IDatabaseProvider CreateProvider()
        => new DuckDbDatabaseProvider("Data Source=:memory:");

    public IDocumentStore CreateStore(string tableName)
        => new DocumentStore(new DocumentStoreOptions
        {
            DatabaseProvider = this.CreateProvider(),
            TableName = tableName
        });
}
