using Shiny.DocumentDb.Tests.Fixtures;
using Xunit;

namespace Shiny.DocumentDb.Unions.Tests.AzureTable
{
    [Collection("AzureTable")]
    public class UnionQueryConformanceTests(AzureTableDatabaseFixture db) : UnionQueryConformanceTestsBase(db);
}

namespace Shiny.DocumentDb.Unions.Tests.CockroachDB
{
    [Collection("CockroachDB")]
    public class UnionQueryConformanceTests(CockroachDbDatabaseFixture db) : UnionQueryConformanceTestsBase(db);
}

namespace Shiny.DocumentDb.Unions.Tests.DocumentDB
{
    [Collection("DocumentDB")]
    public class UnionQueryConformanceTests(DocumentDbDatabaseFixture db) : UnionQueryConformanceTestsBase(db);
}

namespace Shiny.DocumentDb.Unions.Tests.DuckDB
{
    [Collection("DuckDB")]
    public class UnionQueryConformanceTests(DuckDbDatabaseFixture db) : UnionQueryConformanceTestsBase(db);
}

namespace Shiny.DocumentDb.Unions.Tests.CosmosDB
{
    [Collection("CosmosDB")]
    public class UnionQueryConformanceTests(CosmosDbDatabaseFixture db) : UnionQueryConformanceTestsBase(db);
}

namespace Shiny.DocumentDb.Unions.Tests.Firestore
{
    [Collection("Firestore")]
    public class UnionQueryConformanceTests(FirestoreDatabaseFixture db) : UnionQueryConformanceTestsBase(db);
}

namespace Shiny.DocumentDb.Unions.Tests.DynamoDB
{
    [Collection("DynamoDB")]
    public class UnionQueryConformanceTests(DynamoDbDatabaseFixture db) : UnionQueryConformanceTestsBase(db);
}

namespace Shiny.DocumentDb.Unions.Tests.LiteDB
{
    [Collection("LiteDB")]
    public class UnionQueryConformanceTests(LiteDbDatabaseFixture db) : UnionQueryConformanceTestsBase(db);
}

namespace Shiny.DocumentDb.Unions.Tests.MongoDB
{
    [Collection("MongoDB")]
    public class UnionQueryConformanceTests(MongoDbDatabaseFixture db) : UnionQueryConformanceTestsBase(db);
}

namespace Shiny.DocumentDb.Unions.Tests.MariaDB
{
    [Collection("MariaDB")]
    public class UnionQueryConformanceTests(MariaDbDatabaseFixture db) : UnionQueryConformanceTestsBase(db);
}

namespace Shiny.DocumentDb.Unions.Tests.Oracle
{
    [Collection("Oracle")]
    public class UnionQueryConformanceTests(OracleDatabaseFixture db) : UnionQueryConformanceTestsBase(db);
}

namespace Shiny.DocumentDb.Unions.Tests.RavenDB
{
    [Collection("RavenDB")]
    public class UnionQueryConformanceTests(RavenDbDatabaseFixture db) : UnionQueryConformanceTestsBase(db);
}

namespace Shiny.DocumentDb.Unions.Tests.MySQL
{
    [Collection("MySQL")]
    public class UnionQueryConformanceTests(MySqlDatabaseFixture db) : UnionQueryConformanceTestsBase(db);
}

namespace Shiny.DocumentDb.Unions.Tests.PostgreSQL
{
    [Collection("PostgreSQL")]
    public class UnionQueryConformanceTests(PostgreSqlDatabaseFixture db) : UnionQueryConformanceTestsBase(db);
}

namespace Shiny.DocumentDb.Unions.Tests.SQLite
{
    [Collection("SQLite")]
    public class UnionQueryConformanceTests(SqliteDatabaseFixture db) : UnionQueryConformanceTestsBase(db);
}

namespace Shiny.DocumentDb.Unions.Tests.Redis
{
    [Collection("Redis")]
    public class UnionQueryConformanceTests(RedisDatabaseFixture db) : UnionQueryConformanceTestsBase(db);
}

namespace Shiny.DocumentDb.Unions.Tests.MSSQL
{
    [Collection("MSSQL")]
    public class UnionQueryConformanceTests(MsSqlDatabaseFixture db) : UnionQueryConformanceTestsBase(db);
}
