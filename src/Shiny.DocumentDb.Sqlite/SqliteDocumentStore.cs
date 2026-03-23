namespace Shiny.DocumentDb.Sqlite;

public class SqliteDocumentStore : DocumentStore
{
    public SqliteDocumentStore(string connectionString) : base(new DocumentStoreOptions
    {
        DatabaseProvider = new SqliteDatabaseProvider(connectionString)
    })
    {
    }

    public SqliteDocumentStore(DocumentStoreOptions options) : base(options)
    {
    }
}
