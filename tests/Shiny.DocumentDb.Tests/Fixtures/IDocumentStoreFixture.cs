namespace Shiny.DocumentDb.Tests.Fixtures;

public interface IDocumentStoreFixture
{
    IDocumentStore CreateStore(string tableName);
}
