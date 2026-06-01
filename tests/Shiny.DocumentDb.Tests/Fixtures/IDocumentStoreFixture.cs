using System.Linq.Expressions;

namespace Shiny.DocumentDb.Tests.Fixtures;

public interface IDocumentStoreFixture
{
    IDocumentStore CreateStore(string tableName);

    /// <summary>
    /// Create a store with a single unnamed global query filter for <typeparamref name="T"/>.
    /// </summary>
    IDocumentStore CreateStoreWithFilter<T>(string tableName, Expression<Func<T, bool>> filter) where T : class;

    /// <summary>
    /// Create a store with a single named global query filter for <typeparamref name="T"/>.
    /// </summary>
    IDocumentStore CreateStoreWithNamedFilter<T>(string tableName, string filterName, Expression<Func<T, bool>> filter) where T : class;
}
