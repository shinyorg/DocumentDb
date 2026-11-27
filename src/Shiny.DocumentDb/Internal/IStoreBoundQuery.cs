namespace Shiny.DocumentDb.Internal;

/// <summary>A query that knows the store it runs against — what <c>Include</c> loads related documents through.</summary>
interface IStoreBoundQuery
{
    IDocumentStore? Store { get; }
}
