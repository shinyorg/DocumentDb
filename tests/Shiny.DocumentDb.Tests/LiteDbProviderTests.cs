using Shiny.DocumentDb.Tests.Fixtures;
using Xunit;

namespace Shiny.DocumentDb.Tests.LiteDb;

[Collection("LiteDB")]
public class DocumentStoreTests(LiteDbDatabaseFixture db) : DocumentStoreTestsBase(db);

[Collection("LiteDB")]
public class ObservableTests(LiteDbDatabaseFixture db) : ObservableTestsBase(db);

[Collection("LiteDB")]
public class QueryFilterTests(LiteDbDatabaseFixture db) : QueryFilterTestsBase(db);

[Collection("LiteDB")]
public class VersionMappingTests(LiteDbDatabaseFixture db) : VersionMappingTestsBase(db);

[Collection("LiteDB")]
public class TemporalTests(LiteDbDatabaseFixture db) : TemporalTestsBase(db);

[Collection("LiteDB")]
public class SoundexStoredFieldTests(LiteDbDatabaseFixture db) : SoundexStoredFieldTestsBase(db);

[Collection("LiteDB")]
public class StringProjectionDocTests(LiteDbDatabaseFixture db) : StringProjectionDocTestsBase(db);
