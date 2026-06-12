using Shiny.DocumentDb.Tests.Fixtures;
using Xunit;

namespace Shiny.DocumentDb.Tests.SqlServer;

[Collection("MSSQL")]
public class DocumentStoreTests(MsSqlDatabaseFixture db) : DocumentStoreTestsBase(db);

[Collection("MSSQL")]
public class DocumentStoreResolverTests(MsSqlDatabaseFixture db) : DocumentStoreResolverTestsBase(db);

[Collection("MSSQL")]
public class ExpressionQueryTests(MsSqlDatabaseFixture db) : ExpressionQueryTestsBase(db);

[Collection("MSSQL")]
public class BatchInsertTests(MsSqlDatabaseFixture db) : BatchInsertTestsBase(db);

[Collection("MSSQL")]
public class PatchDocumentTests(MsSqlDatabaseFixture db) : PatchDocumentTestsBase(db);

[Collection("MSSQL")]
public class AggregateTests(MsSqlDatabaseFixture db) : AggregateTestsBase(db);

[Collection("MSSQL")]
public class AotSerializationTests(MsSqlDatabaseFixture db) : AotSerializationTestsBase(db);

[Collection("MSSQL")]
public class OrderByTests(MsSqlDatabaseFixture db) : OrderByTestsBase(db);

[Collection("MSSQL")]
public class PaginateTests(MsSqlDatabaseFixture db) : PaginateTestsBase(db);

[Collection("MSSQL")]
public class ProjectionQueryTests(MsSqlDatabaseFixture db) : ProjectionQueryTestsBase(db);

[Collection("MSSQL")]
public class StreamingTests(MsSqlDatabaseFixture db) : StreamingTestsBase(db);

[Collection("MSSQL")]
public class TableMappingTests(MsSqlDatabaseFixture db) : TableMappingTestsBase(db);

[Collection("MSSQL")]
public class IdAutoGenerationTests(MsSqlDatabaseFixture db) : IdAutoGenerationTestsBase(db);

[Collection("MSSQL")]
public class QueryFilterTests(MsSqlDatabaseFixture db) : QueryFilterTestsBase(db);

[Collection("MSSQL")]
public class ConcurrentOperationsTests(MsSqlDatabaseFixture db) : ConcurrentOperationsTestsBase(db);

[Collection("MSSQL")]
public class VersionMappingTests(MsSqlDatabaseFixture db) : VersionMappingTestsBase(db);

[Collection("MSSQL")]
public class TemporalTests(MsSqlDatabaseFixture db) : TemporalTestsBase(db);

[Collection("MSSQL")]
public class MultiTenancyTests(MsSqlDatabaseFixture db) : MultiTenancyTestsBase(db);

[Collection("MSSQL")]
public class ObservableTests(MsSqlDatabaseFixture db) : ObservableTestsBase(db);

[Collection("MSSQL")]
public class ChangeFeedTests(MsSqlDatabaseFixture db) : ChangeFeedTestsBase(db);
