using Shiny.DocumentDb.Tests.Fixtures;
using Xunit;

namespace Shiny.DocumentDb.Tests.Oracle;

[Collection("Oracle")]
public class DocumentStoreTests(OracleDatabaseFixture db) : DocumentStoreTestsBase(db);

[Collection("Oracle")]
public class DocumentStoreResolverTests(OracleDatabaseFixture db) : DocumentStoreResolverTestsBase(db);

[Collection("Oracle")]
public class ExpressionQueryTests(OracleDatabaseFixture db) : ExpressionQueryTestsBase(db);

[Collection("Oracle")]
public class BatchInsertTests(OracleDatabaseFixture db) : BatchInsertTestsBase(db);

[Collection("Oracle")]
public class PatchDocumentTests(OracleDatabaseFixture db) : PatchDocumentTestsBase(db);

[Collection("Oracle")]
public class AggregateTests(OracleDatabaseFixture db) : AggregateTestsBase(db);

[Collection("Oracle")]
public class AotSerializationTests(OracleDatabaseFixture db) : AotSerializationTestsBase(db);

[Collection("Oracle")]
public class OrderByTests(OracleDatabaseFixture db) : OrderByTestsBase(db);

[Collection("Oracle")]
public class PaginateTests(OracleDatabaseFixture db) : PaginateTestsBase(db);

[Collection("Oracle")]
public class ProjectionQueryTests(OracleDatabaseFixture db) : ProjectionQueryTestsBase(db);

[Collection("Oracle")]
public class StreamingTests(OracleDatabaseFixture db) : StreamingTestsBase(db);

[Collection("Oracle")]
public class TableMappingTests(OracleDatabaseFixture db) : TableMappingTestsBase(db);

[Collection("Oracle")]
public class IdAutoGenerationTests(OracleDatabaseFixture db) : IdAutoGenerationTestsBase(db);

[Collection("Oracle")]
public class QueryFilterTests(OracleDatabaseFixture db) : QueryFilterTestsBase(db);

[Collection("Oracle")]
public class ConcurrentOperationsTests(OracleDatabaseFixture db) : ConcurrentOperationsTestsBase(db);

[Collection("Oracle")]
public class VersionMappingTests(OracleDatabaseFixture db) : VersionMappingTestsBase(db);

[Collection("Oracle")]
public class MultiTenancyTests(OracleDatabaseFixture db) : MultiTenancyTestsBase(db);

[Collection("Oracle")]
public class ObservableTests(OracleDatabaseFixture db) : ObservableTestsBase(db);
