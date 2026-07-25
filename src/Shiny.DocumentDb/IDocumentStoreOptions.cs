using System.Linq.Expressions;

namespace Shiny.DocumentDb;

/// <summary>
/// The provider-agnostic slice of a store's options — implemented by <see cref="DocumentStoreOptions"/> and by
/// every provider options class. It exists so a cross-cutting feature can be written <b>once</b> against any
/// store instead of once per provider: <c>AddSoftDelete</c> and <c>MapJsonSchema</c> are both single extension
/// methods over this interface.
/// <para>
/// The members are implemented <b>explicitly</b> everywhere, so each options class keeps its own strongly-typed
/// fluent overloads (which return the concrete options type and chain normally). Reach for the interface when
/// you are writing a feature that must work on any provider; reach for the concrete options type otherwise.
/// </para>
/// </summary>
public interface IDocumentStoreOptions
{
    /// <summary>Registers a per-document write interceptor. Registration order = execution order.</summary>
    IDocumentStoreOptions AddInterceptor(IDocumentInterceptor interceptor);

    /// <summary>Registers a set-based (bulk) write interceptor. Registration order = execution order.</summary>
    IDocumentStoreOptions AddBulkInterceptor(IDocumentBulkInterceptor interceptor);

    /// <summary>
    /// Registers a global query filter for <typeparamref name="T"/>. Pass a <paramref name="name"/> so it can be
    /// lifted per query with <c>IgnoreQueryFilters(name)</c>, or null for an unnamed filter.
    /// </summary>
    IDocumentStoreOptions AddQueryFilter<T>(string? name, Expression<Func<T, bool>> predicate) where T : class;
}
