using System.Linq.Expressions;
using System.Text.Json.Serialization.Metadata;
using Shiny.DocumentDb.Internal.Query;

namespace Shiny.DocumentDb.Internal;

/// <summary>
/// The binder for a <b>type-keyed</b> JSON collection: field paths resolve through the document type's
/// metadata (so naming policies, <c>[JsonPropertyName]</c> and leaf CLR types are all honoured), but the
/// result is a <see cref="DocumentFieldExpression"/> rather than a member access.
/// </summary>
/// <remarks>
/// <para>
/// The member-access binder cannot be reused here because the query's parameter is a
/// <see cref="System.Text.Json.Nodes.JsonObject"/>, not the document type — you cannot hang
/// <c>Expression.Property(jsonObjectParam, userProperty)</c> off it. Resolving to a path instead sidesteps
/// that entirely and lowers to the identical <c>RootFieldNode</c>, so the emitted SQL matches the typed LINQ
/// surface byte for byte.
/// </para>
/// <para>
/// Because leaf types are known, nothing needs inferring: every method behaves like the typed binder, which
/// is what gives a type-keyed collection the full function set (<c>hasflag</c>, spatial, full-text) that a
/// schema-free one cannot reach.
/// </para>
/// </remarks>
sealed class JsonPathFieldBinder : IFieldBinder
{
    readonly JsonTypeInfo typeInfo;

    public JsonPathFieldBinder(JsonTypeInfo typeInfo) => this.typeInfo = typeInfo;

    public (Expression Body, Type LeafType) Resolve(string path)
    {
        var (jsonPath, leafType) = ResolvePath(this.typeInfo, path);
        return (new DocumentFieldExpression(jsonPath, leafType), leafType);
    }

    public bool IsUnresolved(Type leafType) => false;

    public Expression Require(Expression operand, Type leafType, Type required, string func, int position)
    {
        if ((Nullable.GetUnderlyingType(leafType) ?? leafType) != required)
            throw FilterParseError.Create($"'{func}' requires a {Describe(required)} argument but got '{leafType.Name}'", position);
        return operand;
    }

    public Expression NumericArg(Expression operand, Type leafType)
        => Expression.Convert(operand, typeof(double));

    // The operand is already typed by metadata; Expression.Property validates it as it always has.
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

    /// <summary>Walks a dotted path through <see cref="JsonTypeInfo.Properties"/> to its JSON path and leaf type.</summary>
    internal static (string JsonPath, Type LeafType) ResolvePath(JsonTypeInfo typeInfo, string propertyPath)
    {
        var current = typeInfo;
        var segments = propertyPath.Split('.');
        var jsonNames = new string[segments.Length];
        Type leafType = typeof(string);

        for (var i = 0; i < segments.Length; i++)
        {
            var name = segments[i].Trim();
            if (name.Length == 0)
                throw new ArgumentException("Property path contains an empty segment.", nameof(propertyPath));

            var property = Find(current, name)
                ?? throw new ArgumentException(
                    $"Property '{name}' not found on type '{current.Type.Name}'.", nameof(propertyPath));

            jsonNames[i] = property.Name;
            leafType = property.PropertyType;

            if (i < segments.Length - 1)
                current = typeInfo.Options.GetTypeInfo(property.PropertyType);
        }

        return (string.Join('.', jsonNames), leafType);
    }

    static JsonPropertyInfo? Find(JsonTypeInfo typeInfo, string name)
    {
        foreach (var prop in typeInfo.Properties)
        {
            if (prop.Name.Equals(name, StringComparison.OrdinalIgnoreCase)
                || (prop.AttributeProvider is System.Reflection.PropertyInfo pi
                    && pi.Name.Equals(name, StringComparison.OrdinalIgnoreCase)))
            {
                return prop;
            }
        }
        return null;
    }

    static string Describe(Type t) => t == typeof(string) ? "string" : t.Name;
}
