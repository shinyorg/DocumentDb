using Shiny.DocumentDb.Tests.Fixtures;
using Xunit;

namespace Shiny.DocumentDb.Tests.PostgreSql;

[Collection("PostgreSQL")]
public class DocumentStoreTests(PostgreSqlDatabaseFixture db) : DocumentStoreTestsBase(db);

[Collection("PostgreSQL")]
public class DocumentStoreResolverTests(PostgreSqlDatabaseFixture db) : DocumentStoreResolverTestsBase(db);

[Collection("PostgreSQL")]
public class ExpressionQueryTests(PostgreSqlDatabaseFixture db) : ExpressionQueryTestsBase(db);

[Collection("PostgreSQL")]
public class BatchInsertTests(PostgreSqlDatabaseFixture db) : BatchInsertTestsBase(db);

[Collection("PostgreSQL")]
public class PatchDocumentTests(PostgreSqlDatabaseFixture db) : PatchDocumentTestsBase(db);

[Collection("PostgreSQL")]
public class AggregateTests(PostgreSqlDatabaseFixture db) : AggregateTestsBase(db);

[Collection("PostgreSQL")]
public class AotSerializationTests(PostgreSqlDatabaseFixture db) : AotSerializationTestsBase(db);

[Collection("PostgreSQL")]
public class OrderByTests(PostgreSqlDatabaseFixture db) : OrderByTestsBase(db);

[Collection("PostgreSQL")]
public class PaginateTests(PostgreSqlDatabaseFixture db) : PaginateTestsBase(db);

[Collection("PostgreSQL")]
public class ProjectionQueryTests(PostgreSqlDatabaseFixture db) : ProjectionQueryTestsBase(db);

[Collection("PostgreSQL")]
public class StreamingTests(PostgreSqlDatabaseFixture db) : StreamingTestsBase(db);

[Collection("PostgreSQL")]
public class TableMappingTests(PostgreSqlDatabaseFixture db) : TableMappingTestsBase(db);

[Collection("PostgreSQL")]
public class IdAutoGenerationTests(PostgreSqlDatabaseFixture db) : IdAutoGenerationTestsBase(db);

[Collection("PostgreSQL")]
public class ChangeFeedTests(PostgreSqlDatabaseFixture db) : ChangeFeedTestsBase(db);

[Collection("PostgreSQL")]
public class QueryFilterTests(PostgreSqlDatabaseFixture db) : QueryFilterTestsBase(db);

[Collection("PostgreSQL")]
public class ConcurrentOperationsTests(PostgreSqlDatabaseFixture db) : ConcurrentOperationsTestsBase(db);

[Collection("PostgreSQL")]
public class VersionMappingTests(PostgreSqlDatabaseFixture db) : VersionMappingTestsBase(db);

[Collection("PostgreSQL")]
public class TemporalTests(PostgreSqlDatabaseFixture db) : TemporalTestsBase(db);

[Collection("PostgreSQL")]
public class MultiTenancyTests(PostgreSqlDatabaseFixture db) : MultiTenancyTestsBase(db);

[Collection("PostgreSQL")]
public class ObservableTests(PostgreSqlDatabaseFixture db) : ObservableTestsBase(db);

[Collection("PostgreSQL")]
public class ScalarFunctionTests(PostgreSqlDatabaseFixture db) : ScalarFunctionTestsBase(db);
