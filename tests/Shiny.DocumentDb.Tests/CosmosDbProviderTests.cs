using Shiny.DocumentDb.Tests.Fixtures;
using Xunit;

namespace Shiny.DocumentDb.Tests.CosmosDb;

[Collection("CosmosDB")]
public class DocumentStoreTests(CosmosDbDatabaseFixture db) : DocumentStoreTestsBase(db);

[Collection("CosmosDB")]
public class QueryFilterTests(CosmosDbDatabaseFixture db) : QueryFilterTestsBase(db);
