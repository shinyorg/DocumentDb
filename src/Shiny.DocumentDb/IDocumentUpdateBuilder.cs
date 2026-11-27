using System.Linq.Expressions;
using System.Numerics;

namespace Shiny.DocumentDb;

/// <summary>
/// Collects the property assignments for a set-based
/// <see cref="IDocumentQuery{T}.ExecuteUpdate(Action{IDocumentUpdateBuilder{T}}, CancellationToken)"/> so several
/// properties are written in one statement instead of one call each. Besides <see cref="Set{TValue}"/>, it can
/// increment a number, remove a property, and push / pull / add-to-set on an array.
/// </summary>
public interface IDocumentUpdateBuilder<T> where T : class
{
    /// <summary>
    /// Sets one JSON property. Call repeatedly to set several; assignments are applied in registration order.
    /// Touching the same property twice throws — an implicit last-one-wins is not worth guessing at.
    /// </summary>
    /// <param name="property">Selects the property to set, e.g. <c>x =&gt; x.Status</c>. Nested paths are supported.</param>
    /// <param name="value">The new value (a literal: string, number, bool, date, or null).</param>
    IDocumentUpdateBuilder<T> Set<TValue>(Expression<Func<T, TValue>> property, TValue value);

    /// <summary>
    /// Adds <paramref name="amount"/> to a numeric property; pass a negative amount to subtract. A missing or
    /// null value counts as zero.
    /// </summary>
    IDocumentUpdateBuilder<T> Increment<TValue>(Expression<Func<T, TValue>> property, TValue amount) where TValue : struct, INumber<TValue>;

    /// <inheritdoc cref="Increment{TValue}(Expression{Func{T, TValue}}, TValue)"/>
    IDocumentUpdateBuilder<T> Increment<TValue>(Expression<Func<T, TValue?>> property, TValue amount) where TValue : struct, INumber<TValue>;

    /// <summary>Removes a property from the document. Reading the document back gives the property its default value.</summary>
    IDocumentUpdateBuilder<T> Unset<TValue>(Expression<Func<T, TValue>> property);

    /// <summary>Appends <paramref name="item"/> to a collection property, creating the collection when it is missing or null.</summary>
    IDocumentUpdateBuilder<T> Push<TItem>(Expression<Func<T, IEnumerable<TItem>?>> collection, TItem item);

    /// <summary>Removes every element equal to <paramref name="item"/> from a collection property. Objects compare by their JSON values.</summary>
    IDocumentUpdateBuilder<T> Pull<TItem>(Expression<Func<T, IEnumerable<TItem>?>> collection, TItem item);

    /// <summary>
    /// Appends <paramref name="item"/> to a collection property unless an equal element is already in it, creating the
    /// collection when it is missing or null. Objects compare by their JSON values.
    /// </summary>
    IDocumentUpdateBuilder<T> AddToSet<TItem>(Expression<Func<T, IEnumerable<TItem>?>> collection, TItem item);
}
