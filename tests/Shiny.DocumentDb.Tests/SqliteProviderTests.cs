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
public class AotSerializationTests(SqliteDatabaseFixture db) : AotSerializationTestsBase(db);

[Collection("SQLite")]
public class OrderByTests(SqliteDatabaseFixture db) : OrderByTestsBase(db);

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
