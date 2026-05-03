using Shiny.DocumentDb.Tests.Fixtures;
using Xunit;

namespace Shiny.DocumentDb.Tests.LiteDb;

[Collection("LiteDB")]
public class DocumentStoreTests(LiteDbDatabaseFixture db) : DocumentStoreTestsBase(db);
