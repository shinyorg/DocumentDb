using Shiny.DocumentDb.Tests.Fixtures;
using Xunit;

namespace Shiny.DocumentDb.Tests.CosmosDb;

[Collection("CosmosDB")]
public class DocumentStoreTests(CosmosDbDatabaseFixture db) : DocumentStoreTestsBase(db);

[Collection("CosmosDB")]
public class QueryFilterTests(CosmosDbDatabaseFixture db) : QueryFilterTestsBase(db);

[Collection("CosmosDB")]
public class VersionMappingTests(CosmosDbDatabaseFixture db) : VersionMappingTestsBase(db);

[Collection("CosmosDB")]
public class TemporalTests(CosmosDbDatabaseFixture db) : TemporalTestsBase(db);

[Collection("CosmosDB")]
public class FlagEnumTests(CosmosDbDatabaseFixture db) : FlagEnumTestsBase(db);

[Collection("CosmosDB")]
public class SoundexStoredFieldTests(CosmosDbDatabaseFixture db) : SoundexStoredFieldTestsBase(db);

[Collection("CosmosDB")]
public class StringProjectionDocTests(CosmosDbDatabaseFixture db) : StringProjectionDocTestsBase(db);

[Collection("CosmosDB")]
public class DocumentQueryConformanceTests(CosmosDbDatabaseFixture db) : DocumentQueryConformanceTestsBase(db);

[Collection("CosmosDB")]
public class JsonCollectionNotSupportedTests(CosmosDbDatabaseFixture db) : JsonCollectionNotSupportedTestsBase(db);

[Collection("CosmosDB")]
public class SoftDeleteConformanceTests(CosmosDbDatabaseFixture db) : SoftDeleteConformanceTestsBase(db);
