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
