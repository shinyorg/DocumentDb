using Shiny.DocumentDb.Tests.Fixtures;
using Xunit;

namespace Shiny.DocumentDb.Tests.MariaDb;

[Collection("MariaDB")]
public class DocumentStoreTests(MariaDbDatabaseFixture db) : DocumentStoreTestsBase(db);

[Collection("MariaDB")]
public class DocumentStoreResolverTests(MariaDbDatabaseFixture db) : DocumentStoreResolverTestsBase(db);

[Collection("MariaDB")]
public class ExpressionQueryTests(MariaDbDatabaseFixture db) : ExpressionQueryTestsBase(db)
{
    // MariaDB has no JSON_TABLE, so array-unnest (Any/All over a collection) is unsupported.
    protected override bool SupportsArrayUnnest => false;
}

[Collection("MariaDB")]
public class BatchInsertTests(MariaDbDatabaseFixture db) : BatchInsertTestsBase(db);

[Collection("MariaDB")]
public class PatchDocumentTests(MariaDbDatabaseFixture db) : PatchDocumentTestsBase(db);

[Collection("MariaDB")]
public class AggregateTests(MariaDbDatabaseFixture db) : AggregateTestsBase(db)
{
    // MariaDB has no JSON_TABLE, so collection-aggregate projections are unsupported.
    protected override bool SupportsArrayUnnest => false;
}

[Collection("MariaDB")]
public class GroupByQueryTests(MariaDbDatabaseFixture db) : GroupByQueryTestsBase(db);

[Collection("MariaDB")]
public class AotSerializationTests(MariaDbDatabaseFixture db) : AotSerializationTestsBase(db);

[Collection("MariaDB")]
public class OrderByTests(MariaDbDatabaseFixture db) : OrderByTestsBase(db);

[Collection("MariaDB")]
public class PaginateTests(MariaDbDatabaseFixture db) : PaginateTestsBase(db);

[Collection("MariaDB")]
public class ProjectionQueryTests(MariaDbDatabaseFixture db) : ProjectionQueryTestsBase(db)
{
    // MariaDB has no JSON_TABLE, so array-unnest projections (collection Count()/Any()) are unsupported.
    protected override bool SupportsArrayUnnest => false;
}

[Collection("MariaDB")]
public class StreamingTests(MariaDbDatabaseFixture db) : StreamingTestsBase(db);

[Collection("MariaDB")]
public class TableMappingTests(MariaDbDatabaseFixture db) : TableMappingTestsBase(db);

[Collection("MariaDB")]
public class IdAutoGenerationTests(MariaDbDatabaseFixture db) : IdAutoGenerationTestsBase(db);

[Collection("MariaDB")]
public class QueryFilterTests(MariaDbDatabaseFixture db) : QueryFilterTestsBase(db);

[Collection("MariaDB")]
public class ConcurrentOperationsTests(MariaDbDatabaseFixture db) : ConcurrentOperationsTestsBase(db);

[Collection("MariaDB")]
public class VersionMappingTests(MariaDbDatabaseFixture db) : VersionMappingTestsBase(db);

[Collection("MariaDB")]
public class TemporalTests(MariaDbDatabaseFixture db) : TemporalTestsBase(db);

[Collection("MariaDB")]
public class MultiTenancyTests(MariaDbDatabaseFixture db) : MultiTenancyTestsBase(db);

[Collection("MariaDB")]
public class ObservableTests(MariaDbDatabaseFixture db) : ObservableTestsBase(db);

[Collection("MariaDB")]
public class ScalarFunctionTests(MariaDbDatabaseFixture db) : ScalarFunctionTestsBase(db);

[Collection("MariaDB")]
public class SoundexTests(MariaDbDatabaseFixture db) : SoundexTestsBase(db);

[Collection("MariaDB")]
public class DocumentQueryConformanceTests(MariaDbDatabaseFixture db) : DocumentQueryConformanceTestsBase(db);

[Collection("MariaDB")]
public class SoftDeleteConformanceTests(MariaDbDatabaseFixture db) : SoftDeleteConformanceTestsBase(db);
