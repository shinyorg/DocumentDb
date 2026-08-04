using System.Linq.Expressions;

namespace Shiny.DocumentDb.Internal;

/// <summary>
/// The one <see cref="IDocumentUpdateBuilder{T}"/> implementation. It only collects (selector, value) pairs —
/// resolving them to JSON paths is the query's job, because that needs the store's naming policy and type
/// metadata, and duplicate detection has to happen on the resolved path rather than on the expression text.
/// </summary>
sealed class DocumentUpdateBuilder<T> : IDocumentUpdateBuilder<T> where T : class
{
    readonly List<(Expression<Func<T, object>> Property, object? Value)> assignments = new();

    public IReadOnlyList<(Expression<Func<T, object>> Property, object? Value)> Assignments => this.assignments;

    public IDocumentUpdateBuilder<T> Set<TValue>(Expression<Func<T, TValue>> property, TValue value)
    {
        ArgumentNullException.ThrowIfNull(property);

        // The rest of the pipeline speaks Expression<Func<T, object>> (that is what ResolveJsonPath takes), so a
        // value-typed selector is boxed here — the same Convert node the object-typed lambdas arrive with.
        Expression body = property.Body;
        if (body.Type.IsValueType)
            body = Expression.Convert(body, typeof(object));

        this.assignments.Add((Expression.Lambda<Func<T, object>>(body, property.Parameters), value));
        return this;
    }

    /// <summary>Runs <paramref name="build"/> and returns the collected assignments, rejecting an empty set.</summary>
    public static IReadOnlyList<(Expression<Func<T, object>> Property, object? Value)> Collect(Action<IDocumentUpdateBuilder<T>> build)
    {
        ArgumentNullException.ThrowIfNull(build);

        var builder = new DocumentUpdateBuilder<T>();
        build(builder);
        if (builder.assignments.Count == 0)
            throw new ArgumentException(
                $"No properties were set for the ExecuteUpdate of '{typeof(T).Name}'. Call Set(...) at least once.",
                nameof(build));

        return builder.assignments;
    }

    /// <summary>
    /// Guards against the same JSON path appearing twice in one statement, where the dialects disagree on which
    /// assignment wins.
    /// </summary>
    public static void RejectDuplicatePaths(IReadOnlyList<(string JsonPath, object? Value)> resolved)
    {
        for (var i = 1; i < resolved.Count; i++)
        {
            for (var j = 0; j < i; j++)
            {
                if (String.Equals(resolved[i].JsonPath, resolved[j].JsonPath, StringComparison.Ordinal))
                    throw new ArgumentException(
                        $"Property '{resolved[i].JsonPath}' is set more than once in the same ExecuteUpdate. Set it once with the value you want.");
            }
        }
    }
}
