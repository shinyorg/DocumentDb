using Xunit;

namespace Shiny.DocumentDb.Tests.Fixtures;

[CollectionDefinition("SQLite")]
public class SqliteCollection : ICollectionFixture<SqliteDatabaseFixture>;

[CollectionDefinition("MySQL")]
public class MySqlCollection : ICollectionFixture<MySqlDatabaseFixture>;

[CollectionDefinition("MariaDB")]
public class MariaDbCollection : ICollectionFixture<MariaDbDatabaseFixture>;

[CollectionDefinition("CockroachDB")]
public class CockroachDbCollection : ICollectionFixture<CockroachDbDatabaseFixture>;

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

[CollectionDefinition("OracleNativeSpatial")]
public class OracleNativeSpatialCollection : ICollectionFixture<OracleNativeSpatialFixture>;

[CollectionDefinition("AzureTable")]
public class AzureTableCollection : ICollectionFixture<AzureTableDatabaseFixture>;

[CollectionDefinition("DynamoDB")]
public class DynamoDbCollection : ICollectionFixture<DynamoDbDatabaseFixture>;

[CollectionDefinition("Redis")]
public class RedisCollection : ICollectionFixture<RedisDatabaseFixture>;

[CollectionDefinition("RavenDB")]
public class RavenDbCollection : ICollectionFixture<RavenDbDatabaseFixture>;

[CollectionDefinition("Firestore")]
public class FirestoreCollection : ICollectionFixture<FirestoreDatabaseFixture>;

[CollectionDefinition("DocumentDB")]
public class DocumentDbCollection : ICollectionFixture<DocumentDbDatabaseFixture>;
