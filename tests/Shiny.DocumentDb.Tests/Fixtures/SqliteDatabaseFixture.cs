using Shiny.DocumentDb.Sqlite;

namespace Shiny.DocumentDb.Tests.Fixtures;

public class SqliteDatabaseFixture : IDatabaseFixture
{
    public IDatabaseProvider CreateProvider()
        => new SqliteDatabaseProvider("Data Source=:memory:");
}
