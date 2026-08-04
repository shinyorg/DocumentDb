using Shiny.DocumentDb.Tests.Fixtures;
using Xunit;

namespace Shiny.DocumentDb.Tests.DuckDb;

[Collection("DuckDB")]
public class DocumentStoreTests(DuckDbDatabaseFixture db) : DocumentStoreTestsBase(db);

[Collection("DuckDB")]
public class DocumentStoreResolverTests(DuckDbDatabaseFixture db) : DocumentStoreResolverTestsBase(db);

[Collection("DuckDB")]
public class ExpressionQueryTests(DuckDbDatabaseFixture db) : ExpressionQueryTestsBase(db);

[Collection("DuckDB")]
public class BatchInsertTests(DuckDbDatabaseFixture db) : BatchInsertTestsBase(db);

[Collection("DuckDB")]
public class PatchDocumentTests(DuckDbDatabaseFixture db) : PatchDocumentTestsBase(db);

[Collection("DuckDB")]
public class AggregateTests(DuckDbDatabaseFixture db) : AggregateTestsBase(db);

[Collection("DuckDB")]
public class GroupByQueryTests(DuckDbDatabaseFixture db) : GroupByQueryTestsBase(db);

[Collection("DuckDB")]
public class AotSerializationTests(DuckDbDatabaseFixture db) : AotSerializationTestsBase(db);

[Collection("DuckDB")]
public class OrderByTests(DuckDbDatabaseFixture db) : OrderByTestsBase(db);

[Collection("DuckDB")]
public class PaginateTests(DuckDbDatabaseFixture db) : PaginateTestsBase(db);

[Collection("DuckDB")]
public class ProjectionQueryTests(DuckDbDatabaseFixture db) : ProjectionQueryTestsBase(db);

[Collection("DuckDB")]
public class StreamingTests(DuckDbDatabaseFixture db) : StreamingTestsBase(db);

[Collection("DuckDB")]
public class TableMappingTests(DuckDbDatabaseFixture db) : TableMappingTestsBase(db);

[Collection("DuckDB")]
public class IdAutoGenerationTests(DuckDbDatabaseFixture db) : IdAutoGenerationTestsBase(db);

[Collection("DuckDB")]
public class QueryFilterTests(DuckDbDatabaseFixture db) : QueryFilterTestsBase(db);

[Collection("DuckDB")]
public class ConcurrentOperationsTests(DuckDbDatabaseFixture db) : ConcurrentOperationsTestsBase(db);

[Collection("DuckDB")]
public class VersionMappingTests(DuckDbDatabaseFixture db) : VersionMappingTestsBase(db);

[Collection("DuckDB")]
public class TemporalTests(DuckDbDatabaseFixture db) : TemporalTestsBase(db);

[Collection("DuckDB")]
public class MultiTenancyTests(DuckDbDatabaseFixture db) : MultiTenancyTestsBase(db);

[Collection("DuckDB")]
public class ObservableTests(DuckDbDatabaseFixture db) : ObservableTestsBase(db);

[Collection("DuckDB")]
public class ScalarFunctionTests(DuckDbDatabaseFixture db) : ScalarFunctionTestsBase(db);

[Collection("DuckDB")]
public class DocumentQueryConformanceTests(DuckDbDatabaseFixture db) : DocumentQueryConformanceTestsBase(db);

[Collection("DuckDB")]
public class JsonCollectionConformanceTests(DuckDbDatabaseFixture db) : JsonCollectionConformanceTestsBase(db)
{
    // DuckDB rejects an index over a json_extract_string expression ("Binder Error: cannot use json_extract_string in this context").
    protected override bool SupportsJsonExpressionIndexes => false;
}

[Collection("DuckDB")]
public class SoftDeleteConformanceTests(DuckDbDatabaseFixture db) : SoftDeleteConformanceTestsBase(db);

[Collection("DuckDB")]
public class OutboxConformanceTests(DuckDbDatabaseFixture db) : OutboxConformanceTestsBase(db);
