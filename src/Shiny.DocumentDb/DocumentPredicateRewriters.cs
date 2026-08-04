using System.Collections.Concurrent;
using System.Linq.Expressions;

namespace Shiny.DocumentDb;

/// <summary>
/// Per-document-type predicate rewriters, consulted by every query after its filters and <c>Where</c> clauses are
/// gathered and before they reach a provider. This is the seam a cross-cutting feature uses when it has to
/// transform the caller's expression rather than add to it — field-level encryption rewrites the constant in
/// <c>x.Email == "a@b.com"</c> into the ciphertext actually stored.
/// </summary>
/// <remarks>
/// <para>
/// Registration is process-wide (like the soft-delete mapping registry) because the transform is a property of
/// the document type, not of one store instance. It is keyed by name so re-registering the same feature for the
/// same type replaces rather than stacks — configuring two stores over one type must not rewrite twice.
/// </para>
/// <para>
/// Every query surface benefits: the typed LINQ <c>Where</c>, the string grammar, OData and the AI tools all
/// lower to the same predicate list. Ordering selectors and projections are <b>not</b> rewritten.
/// </para>
/// </remarks>
static class DocumentPredicateRewriters
{
    // Small, read-mostly, and probed on every query — a dictionary of ordered name/visitor pairs per type keeps
    // the no-rewriter path to a single failed lookup.
    static readonly ConcurrentDictionary<Type, (string Name, ExpressionVisitor Rewriter)[]> rewriters = new();

    /// <summary>
    /// Registers (or replaces, by <paramref name="name"/>) a rewriter for <typeparamref name="T"/>'s predicates.
    /// </summary>
    /// <param name="name">Feature identity, e.g. <c>"encryption"</c>. Re-registering the same name replaces.</param>
    /// <param name="rewriter">Visits each predicate body. Must be thread-safe and stateless.</param>
    internal static void Register<T>(string name, ExpressionVisitor rewriter) where T : class
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        ArgumentNullException.ThrowIfNull(rewriter);

        rewriters.AddOrUpdate(
            typeof(T),
            _ => [(name, rewriter)],
            (_, existing) =>
            {
                var replaced = new List<(string Name, ExpressionVisitor Rewriter)>(existing.Length + 1);
                foreach (var entry in existing)
                {
                    if (!String.Equals(entry.Name, name, StringComparison.Ordinal))
                        replaced.Add(entry);
                }
                replaced.Add((name, rewriter));
                return [.. replaced];
            });
    }

    /// <summary>Removes the named rewriter for <typeparamref name="T"/>. Returns false when none was registered.</summary>
    internal static bool Remove<T>(string name) where T : class
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);

        if (!rewriters.TryGetValue(typeof(T), out var existing))
            return false;

        var remaining = existing.Where(e => !String.Equals(e.Name, name, StringComparison.Ordinal)).ToArray();
        if (remaining.Length == existing.Length)
            return false;

        if (remaining.Length == 0)
            rewriters.TryRemove(typeof(T), out _);
        else
            rewriters[typeof(T)] = remaining;
        return true;
    }

    /// <summary>True when any rewriter is registered for <paramref name="documentType"/>.</summary>
    internal static bool Has(Type documentType) => rewriters.ContainsKey(documentType);

    /// <summary>
    /// Applies every registered rewriter to <paramref name="predicate"/>, in registration order. Returns the
    /// predicate unchanged when nothing is registered or nothing matched.
    /// </summary>
    internal static Expression<Func<T, bool>> Apply<T>(Expression<Func<T, bool>> predicate) where T : class
    {
        if (!rewriters.TryGetValue(typeof(T), out var registered))
            return predicate;

        var body = predicate.Body;
        foreach (var (_, rewriter) in registered)
            body = rewriter.Visit(body);

        return ReferenceEquals(body, predicate.Body)
            ? predicate
            : Expression.Lambda<Func<T, bool>>(body, predicate.Parameters);
    }

    /// <summary>Applies every registered rewriter across a predicate list, in place.</summary>
    internal static void ApplyAll<T>(IList<Expression<Func<T, bool>>> predicates) where T : class
    {
        if (!rewriters.ContainsKey(typeof(T)))
            return;

        for (var i = 0; i < predicates.Count; i++)
            predicates[i] = Apply(predicates[i]);
    }
}
