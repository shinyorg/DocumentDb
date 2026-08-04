using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;
using System.Text.Json;

namespace Shiny.DocumentDb.Internal;

/// <summary>
/// Builds <see cref="SpatialMapping"/> from either an expression (reflection accessor) or an explicit
/// accessor delegate (AOT-clean). One factory for every provider — the four backends that support
/// spatial each carried their own copy of this before.
/// </summary>
public static class SpatialMappingFactory
{
    public static SpatialMapping FromPointExpression<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)] T>(
        Expression<Func<T, GeoPoint?>> property) where T : class
    {
        ArgumentNullException.ThrowIfNull(property);
        var propInfo = ResolveProperty<T>(property.Body, "x => x.Location", nameof(property));
        return FromPointAccessor<T>(propInfo.Name, obj => (GeoPoint?)propInfo.GetValue(obj));
    }

    public static SpatialMapping FromPointAccessor<T>(string propertyName, Func<T, GeoPoint?> accessor) where T : class
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(propertyName);
        ArgumentNullException.ThrowIfNull(accessor);

        return new SpatialMapping
        {
            DocumentType = typeof(T),
            PropertyName = propertyName,
            GetGeoPoint = obj => accessor((T)obj)
        };
    }

    public static SpatialMapping FromGeometryExpression<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)] T>(
        Expression<Func<T, Geometry?>> property) where T : class
    {
        ArgumentNullException.ThrowIfNull(property);
        var propInfo = ResolveProperty<T>(property.Body, "x => x.Area", nameof(property));
        return FromGeometryAccessor<T>(propInfo.Name, obj => (Geometry?)propInfo.GetValue(obj));
    }

    public static SpatialMapping FromGeometryAccessor<T>(string propertyName, Func<T, Geometry?> accessor) where T : class
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(propertyName);
        ArgumentNullException.ThrowIfNull(accessor);

        return new SpatialMapping
        {
            DocumentType = typeof(T),
            PropertyName = propertyName,
            GetGeometry = obj => accessor((T)obj),
            GetGeoPoint = obj => accessor((T)obj) is GeoPointGeometry pg ? pg.Point : null
        };
    }

    public static void ResolveJsonPaths(IEnumerable<SpatialMapping> mappings, JsonSerializerOptions jsonOptions)
    {
        foreach (var mapping in mappings)
        {
            if (mapping.JsonPath == null!)
                mapping.JsonPath = JsonPropertyNameResolver.ResolveJsonName(jsonOptions, mapping.DocumentType, mapping.PropertyName);
        }
    }

    static System.Reflection.PropertyInfo ResolveProperty<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)] T>(
        Expression body,
        string example,
        string paramName)
    {
        // A nullable-to-nullable access appears as-is; a non-nullable property lifted to the nullable
        // parameter type shows up wrapped in a Convert node, so unwrap it to reach the underlying member.
        if (body is UnaryExpression { NodeType: ExpressionType.Convert } convert)
            body = convert.Operand;

        if (body is not MemberExpression member)
            throw new ArgumentException($"Expression must be a simple property access (e.g., {example}).", paramName);

        return typeof(T).GetProperty(member.Member.Name)
            ?? throw new ArgumentException($"Property '{member.Member.Name}' not found on type '{typeof(T).Name}'.", paramName);
    }
}
