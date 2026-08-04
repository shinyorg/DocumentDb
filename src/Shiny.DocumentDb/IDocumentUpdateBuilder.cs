using System.Linq.Expressions;

namespace Shiny.DocumentDb;

/// <summary>
/// Collects the property assignments for a set-based
/// <see cref="IDocumentQuery{T}.ExecuteUpdate(Action{IDocumentUpdateBuilder{T}}, CancellationToken)"/> so several
/// properties are written in one statement instead of one call each.
/// </summary>
public interface IDocumentUpdateBuilder<T> where T : class
{
    /// <summary>
    /// Sets one JSON property. Call repeatedly to set several; assignments are applied in registration order.
    /// Setting the same property twice throws — an implicit last-one-wins is not worth guessing at.
    /// </summary>
    /// <param name="property">Selects the property to set, e.g. <c>x =&gt; x.Status</c>. Nested paths are supported.</param>
    /// <param name="value">The new value (a literal: string, number, bool, date, or null).</param>
    IDocumentUpdateBuilder<T> Set<TValue>(Expression<Func<T, TValue>> property, TValue value);
}
