using Shiny.DocumentDb.Tests.Fixtures;
using Xunit;

namespace Shiny.DocumentDb.Tests.MongoDb;

[Collection("MongoDB")]
public class DocumentStoreTests(MongoDbDatabaseFixture db) : DocumentStoreTestsBase(db);

[Collection("MongoDB")]
public class QueryFilterTests(MongoDbDatabaseFixture db) : QueryFilterTestsBase(db);

[Collection("MongoDB")]
public class VersionMappingTests(MongoDbDatabaseFixture db) : VersionMappingTestsBase(db);

[Collection("MongoDB")]
public class TemporalTests(MongoDbDatabaseFixture db) : TemporalTestsBase(db);

[Collection("MongoDB")]
public class FlagEnumTests(MongoDbDatabaseFixture db) : FlagEnumTestsBase(db);

[Collection("MongoDB")]
public class SoundexStoredFieldTests(MongoDbDatabaseFixture db) : SoundexStoredFieldTestsBase(db);

[Collection("MongoDB")]
public class StringProjectionDocTests(MongoDbDatabaseFixture db) : StringProjectionDocTestsBase(db);

[Collection("MongoDB")]
public class DocumentQueryConformanceTests(MongoDbDatabaseFixture db) : DocumentQueryConformanceTestsBase(db);

[Collection("MongoDB")]
public class JsonCollectionNotSupportedTests(MongoDbDatabaseFixture db) : JsonCollectionNotSupportedTestsBase(db);

[Collection("MongoDB")]
public class SoftDeleteConformanceTests(MongoDbDatabaseFixture db) : SoftDeleteConformanceTestsBase(db);

[Collection("MongoDB")]
public class OutboxUnsupportedTests(MongoDbDatabaseFixture db) : OutboxUnsupportedTestsBase(db);

[Collection("MongoDB")]
public class UniqueIndexConformanceTests(MongoDbDatabaseFixture db) : UniqueIndexConformanceTestsBase(db);

[Collection("MongoDB")]
public class JoinQueryTests(MongoDbDatabaseFixture db) : JoinQueryTestsBase(db)
{
    protected override string QueryStringToken => "$lookup";
}

[Collection("MongoDB")]
public class DocumentMetadataConformanceTests(MongoDbDatabaseFixture db) : DocumentMetadataConformanceTestsBase(db);
