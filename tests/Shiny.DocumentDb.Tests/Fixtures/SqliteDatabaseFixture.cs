using Shiny.DocumentDb.Sqlite;

namespace Shiny.DocumentDb.Tests.Fixtures;

public class SqliteDatabaseFixture : IDatabaseFixture, IDocumentStoreFixture
{
    public IDatabaseProvider CreateProvider()
        => new SqliteDatabaseProvider("Data Source=:memory:");

    public IDocumentStore CreateStore(string tableName)
        => new DocumentStore(new DocumentStoreOptions
        {
            DatabaseProvider = this.CreateProvider(),
            TableName = tableName
        });
}
