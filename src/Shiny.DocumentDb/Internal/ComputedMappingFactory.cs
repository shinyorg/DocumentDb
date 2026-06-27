using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;
using Shiny.DocumentDb.Internal.Query;

namespace Shiny.DocumentDb.Internal;

/// <summary>
/// Builds <see cref="ComputedMapping"/> instances from the user-facing <c>MapComputedProperty</c> overloads.
/// The value definition is kept as an expression tree (lowered to SQL by the relational query pipeline) and
/// also turned into a compile-free getter via <see cref="ExpressionInterpreter"/> for read-back / in-memory
/// evaluation — so nothing here calls <see cref="Expression{TDelegate}.Compile()"/> and the feature stays
/// AOT/trim-safe.
/// </summary>
public static class ComputedMappingFactory
{
    [UnconditionalSuppressMessage("Trimming", "IL2075", Justification = "Property is resolved by name from a user-provided expression over a user-constructed type; not subject to trimming.")]
    public static ComputedMapping FromExpression<T, TValue>(
        Expression<Func<T, TValue>> property,
        Expression<Func<T, TValue>> definition,
        bool indexed) where T : class
    {
        ArgumentNullException.ThrowIfNull(property);
        ArgumentNullException.ThrowIfNull(definition);

        var name = ExtractMemberName(property);
        var propInfo = typeof(T).GetProperty(name)
            ?? throw new ArgumentException($"Property '{name}' not found on type '{typeof(T).Name}'.", nameof(property));
        if (!propInfo.CanWrite)
            throw new ArgumentException(
                $"Computed property '{name}' on '{typeof(T).Name}' must have a setter so the value can be populated on read.",
                nameof(property));

        return new ComputedMapping
        {
            DocumentType = typeof(T),
            PropertyName = name,
            JsonName = null!,
            ResultType = typeof(TValue),
            Definition = definition,
            Compute = BuildCompute(definition),
            SetValue = (obj, val) => propInfo.SetValue(obj, val),
            Materialize = indexed,
            Indexed = indexed,
            ColumnName = BuildColumnName(typeof(T).Name, name)
        };
    }

    /// <summary>
    /// AOT-clean overload that takes the property name and an explicit setter delegate, so the read-back path
    /// uses no reflection. The <paramref name="definition"/> is still an expression (it must be lowerable to SQL).
    /// </summary>
    public static ComputedMapping FromExpression<T, TValue>(
        string propertyName,
        Expression<Func<T, TValue>> definition,
        Action<T, TValue> setter,
        bool indexed) where T : class
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(propertyName);
        ArgumentNullException.ThrowIfNull(definition);
        ArgumentNullException.ThrowIfNull(setter);

        return new ComputedMapping
        {
            DocumentType = typeof(T),
            PropertyName = propertyName,
            JsonName = null!,
            ResultType = typeof(TValue),
            Definition = definition,
            Compute = BuildCompute(definition),
            SetValue = (obj, val) => setter((T)obj, val is TValue tv ? tv : default!),
            Materialize = indexed,
            Indexed = indexed,
            ColumnName = BuildColumnName(typeof(T).Name, propertyName)
        };
    }

    static string BuildColumnName(string typeName, string propertyName)
        => $"cc_{FullTextMappingFactory.SanitizeSuffix(typeName)}_{FullTextMappingFactory.SanitizeSuffix(propertyName)}";

    static Func<object, object?> BuildCompute<T, TValue>(Expression<Func<T, TValue>> definition) where T : class
    {
        // Interpret to TValue (not object) so the result is coerced to the property's type — the
        // interpreter promotes integer arithmetic to double, which the typed conversion narrows back.
        var getter = ExpressionInterpreter.Interpret(definition);
        return obj => getter((T)obj);
    }

    static string ExtractMemberName<T, TValue>(Expression<Func<T, TValue>> property)
    {
        var body = property.Body;
        if (body is UnaryExpression { NodeType: ExpressionType.Convert } u)
            body = u.Operand;
        if (body is MemberExpression m)
            return m.Member.Name;

        throw new ArgumentException("Expression must be a simple property access (e.g., x => x.Total).", nameof(property));
    }
}
