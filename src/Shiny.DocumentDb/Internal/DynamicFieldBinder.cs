using System.Linq.Expressions;
using Shiny.DocumentDb.Internal.Query;

namespace Shiny.DocumentDb.Internal;

/// <summary>
/// The schema-free field binder: a path resolves to a bare <see cref="DocumentFieldExpression"/> whose CLR
/// type is <em>inferred</em> rather than looked up.
/// </summary>
/// <remarks>
/// <para>Four rules, in the order they apply:</para>
/// <list type="number">
/// <item>An explicit <c>path:type</c> hint wins outright.</item>
/// <item>Otherwise infer from the other operand — <c>total &gt; 100</c> retypes the field to <see cref="int"/>.</item>
/// <item>Otherwise infer from the function's required argument type — <c>lower(name)</c> is a string,
/// <c>year(created)</c> a <see cref="DateTime"/>, <c>abs(total)</c> a <see cref="double"/>.</item>
/// <item>Otherwise the floor is <see cref="string"/>, never <see cref="object"/>: every dialect's
/// <c>JsonExtractTyped(_,_,string)</c> falls through to the plain extract, so the emitted SQL is
/// byte-identical to an untyped one — but the CLR expression is well-typed, so <c>Equal</c>, <c>IN</c>,
/// <c>IS NULL</c>, <c>LIKE</c> and the in-memory interpreter all construct cleanly instead of throwing.</item>
/// </list>
/// </remarks>
sealed class DynamicFieldBinder : IFieldBinder
{
    /// <summary>
    /// Sentinel returned as a leaf type when nothing has pinned the field's type yet. Only ever compared by
    /// reference through <see cref="IsUnresolved"/> — it is never used to build an expression, because
    /// <see cref="DocumentFieldExpression.Type"/> reports <see cref="string"/> while unresolved, so even an
    /// accidental escape still produces valid SQL.
    /// </summary>
    abstract class Unresolved;

    public (Expression Body, Type LeafType) Resolve(string path)
    {
        var (jsonPath, hint) = SplitHint(path);
        DynamicNames.ValidatePath(jsonPath);
        return (new DocumentFieldExpression(jsonPath, hint ?? typeof(string)), hint ?? typeof(Unresolved));
    }

    public bool IsUnresolved(Type leafType) => leafType == typeof(Unresolved);

    public Expression Require(Expression operand, Type leafType, Type required, string func, int position)
    {
        if (operand is DocumentFieldExpression field)
            return field.Retype(required);

        if (leafType != required && !this.IsUnresolved(leafType))
            throw FilterParseError.Create($"'{func}' requires a {Describe(required)} argument but got '{leafType.Name}'", position);

        return operand;
    }

    // A retyped field extracts as a double directly — no Expression.Convert wrapper needed (and the lowerer
    // would strip one anyway).
    public Expression NumericArg(Expression operand, Type leafType)
        => operand is DocumentFieldExpression field
            ? field.Retype(typeof(double))
            : Expression.Convert(operand, typeof(double));

    public Expression MemberArg(Expression operand, Type leafType, Type required)
        => operand is DocumentFieldExpression field ? field.Retype(required) : operand;

    public Type LiteralType(Type leafType, Type naturalType)
        => this.IsUnresolved(leafType) ? naturalType : leafType;

    public Expression AdaptTo(Expression operand, Type type)
        => operand is DocumentFieldExpression field ? field.Retype(type) : operand;

    public Expression GeometryArg(Expression operand, Type leafType, int position)
        => throw FilterParseError.Create(
            "Geo functions need a spatial mapping (MapSpatialProperty<T>), which a schema-free collection " +
            "has none of. Use Query<T>() on a mapped document type", position);

    public Type EnumArg(Type leafType, string func, int position)
        => throw FilterParseError.Create(
            $"'{func}' needs a CLR enum type to parse the flag name, which a schema-free collection has " +
            "none of. Use Query<T>() on a mapped document type", position);

    public void RequireMapping(string func, int position)
        => throw FilterParseError.Create(
            $"'{func}' needs a full-text mapping (MapFullTextProperty<T>), which a schema-free collection " +
            "has none of. Use Query<T>() on a mapped document type", position);

    /// <summary>Splits <c>"total:number"</c> into its path and hinted CLR type.</summary>
    static (string Path, Type? Hint) SplitHint(string path)
    {
        var colon = path.IndexOf(':');
        if (colon < 0)
            return (path, null);

        var name = path[(colon + 1)..];
        var hint = name.ToLowerInvariant() switch
        {
            "string" => typeof(string),
            "number" or "double" => typeof(double),
            "int" => typeof(int),
            "long" => typeof(long),
            "decimal" => typeof(decimal),
            "bool" => typeof(bool),
            "date" => typeof(DateTime),
            "guid" => typeof(Guid),
            _ => throw new ArgumentException(
                $"'{name}' is not a known type hint. Use one of: string, number, int, long, double, decimal, bool, date, guid.")
        };
        return (path[..colon], hint);
    }

    static string Describe(Type t) => t == typeof(string) ? "string" : t.Name;
}
