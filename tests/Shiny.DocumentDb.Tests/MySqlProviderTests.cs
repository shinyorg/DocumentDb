using Shiny.DocumentDb.Tests.Fixtures;
using Xunit;

namespace Shiny.DocumentDb.Tests.MySql;

[Collection("MySQL")]
public class DocumentStoreTests(MySqlDatabaseFixture db) : DocumentStoreTestsBase(db);

[Collection("MySQL")]
public class DocumentStoreResolverTests(MySqlDatabaseFixture db) : DocumentStoreResolverTestsBase(db);

[Collection("MySQL")]
public class ExpressionQueryTests(MySqlDatabaseFixture db) : ExpressionQueryTestsBase(db);

[Collection("MySQL")]
public class BatchInsertTests(MySqlDatabaseFixture db) : BatchInsertTestsBase(db);

[Collection("MySQL")]
public class PatchDocumentTests(MySqlDatabaseFixture db) : PatchDocumentTestsBase(db);

[Collection("MySQL")]
public class AggregateTests(MySqlDatabaseFixture db) : AggregateTestsBase(db);

[Collection("MySQL")]
public class AotSerializationTests(MySqlDatabaseFixture db) : AotSerializationTestsBase(db);

[Collection("MySQL")]
public class OrderByTests(MySqlDatabaseFixture db) : OrderByTestsBase(db);

[Collection("MySQL")]
public class PaginateTests(MySqlDatabaseFixture db) : PaginateTestsBase(db);

[Collection("MySQL")]
public class ProjectionQueryTests(MySqlDatabaseFixture db) : ProjectionQueryTestsBase(db);

[Collection("MySQL")]
public class StreamingTests(MySqlDatabaseFixture db) : StreamingTestsBase(db);

[Collection("MySQL")]
public class TableMappingTests(MySqlDatabaseFixture db) : TableMappingTestsBase(db);

[Collection("MySQL")]
public class IdAutoGenerationTests(MySqlDatabaseFixture db) : IdAutoGenerationTestsBase(db);
