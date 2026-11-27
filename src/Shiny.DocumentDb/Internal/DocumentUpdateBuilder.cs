using System.Linq.Expressions;
using System.Numerics;

namespace Shiny.DocumentDb.Internal;

/// <summary>
/// The one <see cref="IDocumentUpdateBuilder{T}"/> implementation. It only collects (selector, value) pairs —
/// resolving them to JSON paths is the query's job, because that needs the store's naming policy and type
/// metadata, and duplicate detection has to happen on the resolved path rather than on the expression text.
/// Anything other than a plain set travels as a <see cref="DocumentUpdateOperation"/> in the value slot.
/// </summary>
sealed class DocumentUpdateBuilder<T> : IDocumentUpdateBuilder<T> where T : class
{
    readonly List<(Expression<Func<T, object>> Property, object? Value)> assignments = new();

    public IReadOnlyList<(Expression<Func<T, object>> Property, object? Value)> Assignments => this.assignments;

    public IDocumentUpdateBuilder<T> Set<TValue>(Expression<Func<T, TValue>> property, TValue value)
        => this.Add(property, value);

    public IDocumentUpdateBuilder<T> Increment<TValue>(Expression<Func<T, TValue>> property, TValue amount) where TValue : struct, INumber<TValue>
        => this.Add(property, DocumentUpdateOperation.Increment(amount));

    public IDocumentUpdateBuilder<T> Increment<TValue>(Expression<Func<T, TValue?>> property, TValue amount) where TValue : struct, INumber<TValue>
        => this.Add(property, DocumentUpdateOperation.Increment(amount));

    public IDocumentUpdateBuilder<T> Unset<TValue>(Expression<Func<T, TValue>> property)
        => this.Add(property, DocumentUpdateOperation.Unset());

    public IDocumentUpdateBuilder<T> Push<TItem>(Expression<Func<T, IEnumerable<TItem>?>> collection, TItem item)
        => this.Add(collection, DocumentUpdateOperation.Push(item));

    public IDocumentUpdateBuilder<T> Pull<TItem>(Expression<Func<T, IEnumerable<TItem>?>> collection, TItem item)
        => this.Add(collection, DocumentUpdateOperation.Pull(item));

    public IDocumentUpdateBuilder<T> AddToSet<TItem>(Expression<Func<T, IEnumerable<TItem>?>> collection, TItem item)
        => this.Add(collection, DocumentUpdateOperation.AddToSet(item));

    IDocumentUpdateBuilder<T> Add<TValue>(Expression<Func<T, TValue>> property, object? value)
    {
        ArgumentNullException.ThrowIfNull(property);

        // The rest of the pipeline speaks Expression<Func<T, object>> (that is what ResolveJsonPath takes), so a
        // value-typed selector is boxed here — the same Convert node the object-typed lambdas arrive with. A
        // collection selector typed as IEnumerable<TItem> may carry a Convert to that interface; strip it so the
        // path resolves against the member itself.
        Expression body = property.Body;
        while (body is UnaryExpression { NodeType: ExpressionType.Convert or ExpressionType.ConvertChecked } convert && !convert.Type.IsValueType && !convert.Operand.Type.IsValueType)
            body = convert.Operand;
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
                $"No properties were set for the ExecuteUpdate of '{typeof(T).Name}'. Call Set(...) (or another update method) at least once.",
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
                        $"Property '{resolved[i].JsonPath}' is updated more than once in the same ExecuteUpdate. Update it once.");
            }
        }
    }

    /// <summary>True when any assignment is an operation rather than a plain set.</summary>
    public static bool HasOperations(IReadOnlyList<(string JsonPath, object? Value)> assignments)
    {
        for (var i = 0; i < assignments.Count; i++)
        {
            if (assignments[i].Value is DocumentUpdateOperation)
                return true;
        }
        return false;
    }
}
