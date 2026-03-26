namespace Shiny.DocumentDb.Sqlite.SqlCipher;

public class SqlCipherDocumentStore : DocumentStore
{
    public SqlCipherDocumentStore(string filePath, string password) : base(new DocumentStoreOptions
    {
        DatabaseProvider = new SqlCipherDatabaseProvider(filePath, password)
    })
    {
    }

    public SqlCipherDocumentStore(DocumentStoreOptions options) : base(options)
    {
    }
}
