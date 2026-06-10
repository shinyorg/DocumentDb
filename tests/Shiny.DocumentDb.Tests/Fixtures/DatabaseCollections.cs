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

[CollectionDefinition("LiteDB")]
public class LiteDbCollection : ICollectionFixture<LiteDbDatabaseFixture>;

[CollectionDefinition("CosmosDB")]
public class CosmosDbCollection : ICollectionFixture<CosmosDbDatabaseFixture>;

[CollectionDefinition("DuckDB")]
public class DuckDbCollection : ICollectionFixture<DuckDbDatabaseFixture>;

[CollectionDefinition("MongoDB")]
public class MongoDbCollection : ICollectionFixture<MongoDbDatabaseFixture>;

[CollectionDefinition("Oracle")]
public class OracleCollection : ICollectionFixture<OracleDatabaseFixture>;
