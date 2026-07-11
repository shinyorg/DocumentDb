using Shiny.DocumentDb.Tests.Fixtures;
using Xunit;

namespace Shiny.DocumentDb.Tests.CockroachDb;

// CockroachDB mirrors the PostgreSQL suite. ChangeFeed is intentionally absent — CockroachDB has no
// LISTEN/NOTIFY, so SupportsChangeFeed is false and the native change-feed suite does not apply.

[Collection("CockroachDB")]
public class DocumentStoreTests(CockroachDbDatabaseFixture db) : DocumentStoreTestsBase(db);

[Collection("CockroachDB")]
public class DocumentStoreResolverTests(CockroachDbDatabaseFixture db) : DocumentStoreResolverTestsBase(db);

[Collection("CockroachDB")]
public class ExpressionQueryTests(CockroachDbDatabaseFixture db) : ExpressionQueryTestsBase(db);

[Collection("CockroachDB")]
public class BatchInsertTests(CockroachDbDatabaseFixture db) : BatchInsertTestsBase(db);

[Collection("CockroachDB")]
public class PatchDocumentTests(CockroachDbDatabaseFixture db) : PatchDocumentTestsBase(db);

[Collection("CockroachDB")]
public class AggregateTests(CockroachDbDatabaseFixture db) : AggregateTestsBase(db);

[Collection("CockroachDB")]
public class GroupByQueryTests(CockroachDbDatabaseFixture db) : GroupByQueryTestsBase(db);

[Collection("CockroachDB")]
public class AotSerializationTests(CockroachDbDatabaseFixture db) : AotSerializationTestsBase(db);

[Collection("CockroachDB")]
public class OrderByTests(CockroachDbDatabaseFixture db) : OrderByTestsBase(db);

[Collection("CockroachDB")]
public class PaginateTests(CockroachDbDatabaseFixture db) : PaginateTestsBase(db);

[Collection("CockroachDB")]
public class ProjectionQueryTests(CockroachDbDatabaseFixture db) : ProjectionQueryTestsBase(db);

[Collection("CockroachDB")]
public class StreamingTests(CockroachDbDatabaseFixture db) : StreamingTestsBase(db);

[Collection("CockroachDB")]
public class TableMappingTests(CockroachDbDatabaseFixture db) : TableMappingTestsBase(db);

[Collection("CockroachDB")]
public class IdAutoGenerationTests(CockroachDbDatabaseFixture db) : IdAutoGenerationTestsBase(db);

[Collection("CockroachDB")]
public class QueryFilterTests(CockroachDbDatabaseFixture db) : QueryFilterTestsBase(db);

[Collection("CockroachDB")]
public class ConcurrentOperationsTests(CockroachDbDatabaseFixture db) : ConcurrentOperationsTestsBase(db);

[Collection("CockroachDB")]
public class VersionMappingTests(CockroachDbDatabaseFixture db) : VersionMappingTestsBase(db);

[Collection("CockroachDB")]
public class TemporalTests(CockroachDbDatabaseFixture db) : TemporalTestsBase(db);

[Collection("CockroachDB")]
public class MultiTenancyTests(CockroachDbDatabaseFixture db) : MultiTenancyTestsBase(db);

[Collection("CockroachDB")]
public class ObservableTests(CockroachDbDatabaseFixture db) : ObservableTestsBase(db);

[Collection("CockroachDB")]
public class ScalarFunctionTests(CockroachDbDatabaseFixture db) : ScalarFunctionTestsBase(db);
