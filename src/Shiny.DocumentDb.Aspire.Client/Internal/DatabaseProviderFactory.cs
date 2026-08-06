using Shiny.DocumentDb;
using Shiny.DocumentDb.CockroachDb;
using Shiny.DocumentDb.MariaDb;
using Shiny.DocumentDb.MySql;
using Shiny.DocumentDb.PostgreSql;
using Shiny.DocumentDb.Sqlite;
using Shiny.DocumentDb.SqlServer;

namespace Shiny.DocumentDb.Aspire.Client.Internal;

/// <summary>
/// Maps a <see cref="DocumentProviderKind"/> + connection string to the matching
/// <see cref="IDatabaseProvider"/>. v1 is the relational + SQLite family, which all configure
/// uniformly via <c>DocumentStoreOptions.DatabaseProvider</c>.
/// </summary>
internal static class DatabaseProviderFactory
{
    public static IDatabaseProvider Create(
        DocumentProviderKind kind,
        string connectionString,
        bool portableSpatial = false
    )
        => kind switch
        {
            DocumentProviderKind.Sqlite => new SqliteDatabaseProvider(connectionString),
            DocumentProviderKind.Postgres => new PostgreSqlDatabaseProvider(connectionString)
            {
                PortableSpatial = portableSpatial
            },
            DocumentProviderKind.CockroachDb => new CockroachDbDatabaseProvider(connectionString)
            {
                PortableSpatial = portableSpatial
            },
            DocumentProviderKind.SqlServer => new SqlServerDatabaseProvider(connectionString)
            {
                PortableSpatial = portableSpatial
            },
            DocumentProviderKind.MySql => new MySqlDatabaseProvider(connectionString),
            DocumentProviderKind.MariaDb => new MariaDbDatabaseProvider(connectionString),
            // TODO: Mongo/Cosmos follow-up — these do NOT use DatabaseProvider; they have their own
            // options types (MongoDbDocumentStoreOptions / CosmosDbDocumentStoreOptions) and a divergent
            // registration path. Wire them when the providers/Aspire hosting packages land.
            _ => throw new NotSupportedException(
                $"DocumentProviderKind '{kind}' is not supported by the Aspire client integration " +
                "(Sqlite/Postgres/CockroachDb/SqlServer/MySql/MariaDb).")
        };
}
