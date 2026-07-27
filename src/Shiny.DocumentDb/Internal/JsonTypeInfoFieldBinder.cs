using System.Linq.Expressions;
using System.Text.Json.Serialization.Metadata;

namespace Shiny.DocumentDb.Internal;

/// <summary>
/// The typed field binder: resolves a dotted path against the document type's <c>JsonTypeInfo</c> and knows
/// every leaf's CLR type, so nothing needs inferring.
/// </summary>
/// <remarks>
/// Every member here is the behaviour <c>FilterExpressionParser</c> had inline before the binder seam
/// existed. Most are the identity function — the leaf type is already correct, so there is nothing to adapt.
/// Serves both a typed <c>Query&lt;T&gt;()</c> and a type-keyed JSON collection.
/// </remarks>
sealed class JsonTypeInfoFieldBinder<T> : IFieldBinder where T : class
{
    readonly ParameterExpression parameter;
    readonly JsonTypeInfo<T> typeInfo;
    readonly IReadOnlyDictionary<string, ComputedMapping>? computed;

    public JsonTypeInfoFieldBinder(
        ParameterExpression parameter,
        JsonTypeInfo<T> typeInfo,
        IReadOnlyDictionary<string, ComputedMapping>? computed)
    {
        this.parameter = parameter;
        this.typeInfo = typeInfo;
        this.computed = computed;
    }

    public (Expression Body, Type LeafType) Resolve(string path)
        => DocumentQueryExtensions.BuildMemberAccess(this.parameter, path, this.typeInfo, this.computed);

    public bool IsUnresolved(Type leafType) => false;

    public Expression Require(Expression operand, Type leafType, Type required, string func, int position)
    {
        if (leafType != required)
            throw FilterParseError.Create($"'{func}' requires a {Describe(required)} argument but got '{leafType.Name}'", position);
        return operand;
    }

    public Expression NumericArg(Expression operand, Type leafType)
        => Expression.Convert(operand, typeof(double));

    // The date-part sites compile to Expression.Property(arg, "Year"), which validates the operand type
    // itself — leave it exactly as it was rather than pre-asserting and changing which exception surfaces.
    public Expression MemberArg(Expression operand, Type leafType, Type required) => operand;

    public Type LiteralType(Type leafType, Type naturalType) => leafType;

    public Expression AdaptTo(Expression operand, Type type) => operand;

    public Expression GeometryArg(Expression operand, Type leafType, int position)
    {
        if (typeof(Geometry).IsAssignableFrom(leafType))
            return leafType == typeof(Geometry) ? operand : Expression.Convert(operand, typeof(Geometry));

        throw FilterParseError.Create(
            $"A geo function's field argument must be a Geometry-mapped property, but got '{leafType.Name}'. " +
            "Use the dedicated store.Geo* methods for GeoPoint fields", position);
    }

    public Type EnumArg(Type leafType, string func, int position)
    {
        var underlying = Nullable.GetUnderlyingType(leafType) ?? leafType;
        if (!underlying.IsEnum)
            throw FilterParseError.Create($"'{func}' requires an enum field but got '{underlying.Name}'", position);
        return underlying;
    }

    public void RequireMapping(string func, int position) { }

    static string Describe(Type t) => t == typeof(string) ? "string" : t.Name;
}
