using Shiny.DocumentDb.Tests.Fixtures;
using Xunit;

namespace Shiny.DocumentDb.Tests.Sqlite;

[Collection("SQLite")]
public class DocumentStoreTests(SqliteDatabaseFixture db) : DocumentStoreTestsBase(db);

[Collection("SQLite")]
public class DocumentStoreResolverTests(SqliteDatabaseFixture db) : DocumentStoreResolverTestsBase(db);

[Collection("SQLite")]
public class ExpressionQueryTests(SqliteDatabaseFixture db) : ExpressionQueryTestsBase(db);

[Collection("SQLite")]
public class BatchInsertTests(SqliteDatabaseFixture db) : BatchInsertTestsBase(db);

[Collection("SQLite")]
public class PatchDocumentTests(SqliteDatabaseFixture db) : PatchDocumentTestsBase(db);

[Collection("SQLite")]
public class AggregateTests(SqliteDatabaseFixture db) : AggregateTestsBase(db);

[Collection("SQLite")]
public class GroupByQueryTests(SqliteDatabaseFixture db) : GroupByQueryTestsBase(db)
{
    // SQLite stores JSON numbers as REAL, so SUM/AVG of a decimal is float-based and cannot be bit-exact.
    protected override bool SupportsExactDecimalAggregate => false;
}

[Collection("SQLite")]
public class AotSerializationTests(SqliteDatabaseFixture db) : AotSerializationTestsBase(db);

[Collection("SQLite")]
public class OrderByTests(SqliteDatabaseFixture db) : OrderByTestsBase(db);

[Collection("SQLite")]
public class WhereStringTests(SqliteDatabaseFixture db) : WhereStringTestsBase(db);

[Collection("SQLite")]
public class ProjectStringTests(SqliteDatabaseFixture db) : ProjectStringTestsBase(db);

[Collection("SQLite")]
public class ToQueryStringTests(SqliteDatabaseFixture db) : ToQueryStringTestsBase(db);

[Collection("SQLite")]
public class PaginateTests(SqliteDatabaseFixture db) : PaginateTestsBase(db);

[Collection("SQLite")]
public class ProjectionQueryTests(SqliteDatabaseFixture db) : ProjectionQueryTestsBase(db);

[Collection("SQLite")]
public class StreamingTests(SqliteDatabaseFixture db) : StreamingTestsBase(db);

[Collection("SQLite")]
public class TableMappingTests(SqliteDatabaseFixture db) : TableMappingTestsBase(db);

[Collection("SQLite")]
public class IdAutoGenerationTests(SqliteDatabaseFixture db) : IdAutoGenerationTestsBase(db);

[Collection("SQLite")]
public class ObservableTests(SqliteDatabaseFixture db) : ObservableTestsBase(db);

[Collection("SQLite")]
public class QueryFilterTests(SqliteDatabaseFixture db) : QueryFilterTestsBase(db);

[Collection("SQLite")]
public class ConcurrentOperationsTests(SqliteDatabaseFixture db) : ConcurrentOperationsTestsBase(db);

[Collection("SQLite")]
public class VersionMappingTests(SqliteDatabaseFixture db) : VersionMappingTestsBase(db);

[Collection("SQLite")]
public class TemporalTests(SqliteDatabaseFixture db) : TemporalTestsBase(db);

[Collection("SQLite")]
public class MultiTenancyTests(SqliteDatabaseFixture db) : MultiTenancyTestsBase(db);

[Collection("SQLite")]
public class ScalarFunctionTests(SqliteDatabaseFixture db) : ScalarFunctionTestsBase(db);

[Collection("SQLite")]
public class SoundexTests(SqliteDatabaseFixture db) : SoundexTestsBase(db);

[Collection("SQLite")]
public class DocumentQueryConformanceTests(SqliteDatabaseFixture db) : DocumentQueryConformanceTestsBase(db);

[Collection("SQLite")]
public class JsonCollectionConformanceTests(SqliteDatabaseFixture db) : JsonCollectionConformanceTestsBase(db);

[Collection("SQLite")]
public class SoftDeleteConformanceTests(SqliteDatabaseFixture db) : SoftDeleteConformanceTestsBase(db);

[Collection("SQLite")]
public class OutboxConformanceTests(SqliteDatabaseFixture db) : OutboxConformanceTestsBase(db);
