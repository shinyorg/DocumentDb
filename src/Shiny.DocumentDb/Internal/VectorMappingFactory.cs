using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;
using System.Text.Json;

namespace Shiny.DocumentDb.Internal;

/// <summary>
/// Builds <see cref="VectorMapping"/> from either an expression (reflection accessors) or explicit
/// getter/setter delegates (AOT-clean). Shared by every vector-capable provider. A null index kind means
/// "the provider's own default", filled in at store construction by
/// <see cref="DocumentMappingRegistry.ResolveVectorIndexKinds"/>.
/// </summary>
public static class VectorMappingFactory
{
    public static VectorMapping FromExpression<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)] T>(
        Expression<Func<T, ReadOnlyMemory<float>>> property,
        int dimensions,
        VectorDistance metric,
        VectorIndexKind? indexKind,
        Action<VectorIndexOptions>? configureIndex) where T : class
    {
        ArgumentNullException.ThrowIfNull(property);

        if (property.Body is not MemberExpression member)
            throw new ArgumentException(
                "Expression must be a simple property access (e.g., x => x.Embedding).",
                nameof(property));

        var propInfo = typeof(T).GetProperty(member.Member.Name)
            ?? throw new ArgumentException($"Property '{member.Member.Name}' not found on type '{typeof(T).Name}'.", nameof(property));

        return FromAccessors<T>(
            propInfo.Name,
            obj => (ReadOnlyMemory<float>)propInfo.GetValue(obj)!,
            (obj, v) => propInfo.SetValue(obj, v),
            dimensions,
            metric,
            indexKind,
            configureIndex);
    }

    public static VectorMapping FromAccessors<T>(
        string propertyName,
        Func<T, ReadOnlyMemory<float>> getter,
        Action<T, ReadOnlyMemory<float>> setter,
        int dimensions,
        VectorDistance metric,
        VectorIndexKind? indexKind,
        Action<VectorIndexOptions>? configureIndex) where T : class
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(propertyName);
        ArgumentNullException.ThrowIfNull(getter);
        ArgumentNullException.ThrowIfNull(setter);
        if (dimensions <= 0)
            throw new ArgumentOutOfRangeException(nameof(dimensions), "Dimensions must be > 0.");

        var indexOptions = new VectorIndexOptions();
        configureIndex?.Invoke(indexOptions);

        return new VectorMapping
        {
            DocumentType = typeof(T),
            PropertyName = propertyName,
            Dimensions = dimensions,
            Metric = metric,
            IndexKind = indexKind ?? VectorIndexKind.Hnsw,
            RequestedIndexKind = indexKind,
            IndexOptions = indexOptions,
            GetVector = obj => getter((T)obj),
            SetVector = (obj, v) => setter((T)obj, v)
        };
    }

    public static void ResolveJsonPaths(IEnumerable<VectorMapping> mappings, JsonSerializerOptions jsonOptions)
    {
        foreach (var mapping in mappings)
        {
            if (mapping.JsonPath == null!)
                mapping.JsonPath = JsonPropertyNameResolver.ResolveJsonName(jsonOptions, mapping.DocumentType, mapping.PropertyName);
        }
    }
}
