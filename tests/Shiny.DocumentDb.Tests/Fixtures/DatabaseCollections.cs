using Xunit;

namespace Shiny.DocumentDb.Tests.Fixtures;

[CollectionDefinition("SQLite")]
public class SqliteCollection : ICollectionFixture<SqliteDatabaseFixture>;

[CollectionDefinition("MySQL")]
public class MySqlCollection : ICollectionFixture<MySqlDatabaseFixture>;

[CollectionDefinition("MSSQL")]
public class MsSqlCollection : ICollectionFixture<MsSqlDatabaseFixture>;

[CollectionDefinition("PostgreSQL")]
public class PostgreSqlCollection : ICollectionFixture<PostgreSqlDatabaseFixture>;
