namespace Shiny.DocumentDb;

public interface IDocumentStoreProvider
{
    IDocumentStore GetStore(string name);
}
