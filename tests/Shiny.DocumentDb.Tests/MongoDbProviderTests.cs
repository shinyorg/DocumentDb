using Shiny.DocumentDb.Tests.Fixtures;
using Xunit;

namespace Shiny.DocumentDb.Tests.MongoDb;

[Collection("MongoDB")]
public class DocumentStoreTests(MongoDbDatabaseFixture db) : DocumentStoreTestsBase(db);

[Collection("MongoDB")]
public class QueryFilterTests(MongoDbDatabaseFixture db) : QueryFilterTestsBase(db);

[Collection("MongoDB")]
public class VersionMappingTests(MongoDbDatabaseFixture db) : VersionMappingTestsBase(db);
