using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;
using System.Reflection;
using System.Text.Json.Serialization.Metadata;

namespace Shiny.DocumentDb.Internal;

public enum IdKind { Guid, Int, Long, String, Custom }

public sealed class IdAccessor<T> where T : class
{
    public IdKind Kind { get; }
    readonly Func<T, object?> getRawId;
    readonly Action<T, object?> setRawId;
    readonly IIdConverter? converter;   // non-null when Kind == Custom

    IdAccessor(IdKind kind, Func<T, object?> get, Action<T, object?> set, IIdConverter? converter)
    {
        Kind = kind;
        getRawId = get;
        setRawId = set;
        this.converter = converter;
    }

    public string GetIdAsString(T doc)
    {
        var raw = getRawId(doc);
        return Kind switch
        {
            IdKind.Guid => ((Guid)raw!).ToString("N"),
            IdKind.Int => ((int)raw!).ToString(),
            IdKind.Long => ((long)raw!).ToString(),
            IdKind.String => (string)raw!,
            IdKind.Custom => converter!.ToStorageString(raw!),
            _ => throw new InvalidOperationException($"Unsupported Id kind: {Kind}")
        };
    }

    public string ResolveId(object id)
    {
        if (Kind == IdKind.Custom)
        {
            return converter!.IdType.IsInstanceOfType(id)
                ? converter.ToStorageString(id)
                : throw new ArgumentException(
                    $"Id type mismatch for '{typeof(T).Name}'. Expected '{converter.IdType.Name}' but received '{id.GetType().Name}'.",
                    nameof(id));
        }

        return (Kind, id) switch
        {
            (IdKind.Guid, Guid g) => g.ToString("N"),
            (IdKind.Int, int i) => i.ToString(),
            (IdKind.Long, long l) => l.ToString(),
            (IdKind.String, string s) => s,
            _ when id is not Guid and not int and not long and not string =>
                throw new ArgumentException(
                    $"Unsupported Id type '{id.GetType().Name}'. Supported types are: Guid, int, long, string, or a type registered via MapIdType.",
                    nameof(id)),
            _ => throw new ArgumentException(
                    $"Id type mismatch for '{typeof(T).Name}'. Expected '{Kind}' but received '{id.GetType().Name}'.",
                    nameof(id))
        };
    }

    public void SetId(T doc, string id)
    {
        object value = Kind switch
        {
            IdKind.Guid => Guid.Parse(id),
            IdKind.Int => int.Parse(id),
            IdKind.Long => long.Parse(id),
            IdKind.String => id,
            IdKind.Custom => converter!.FromStorageString(id),
            _ => throw new InvalidOperationException($"Unsupported Id kind: {Kind}")
        };
        setRawId(doc, value);
    }

    public bool IsDefaultId(T doc)
    {
        var raw = getRawId(doc);
        return Kind switch
        {
            IdKind.Guid => raw is null || (Guid)raw == Guid.Empty,
            IdKind.Int => raw is null || (int)raw == 0,
            IdKind.Long => raw is null || (long)raw == 0L,
            IdKind.String => raw is null || string.IsNullOrEmpty((string)raw),
            IdKind.Custom => converter!.IsDefault(raw),
            _ => true
        };
    }

    /// <summary>
    /// Generates a new Id string for a <see cref="IdKind.Custom"/> type via its converter, throwing if the
    /// converter opts out of generation. Built-in kinds generate elsewhere (Guid/string inline, int/long via provider).
    /// </summary>
    public string GenerateOrThrow()
    {
        if (Kind == IdKind.Custom && converter!.TryGenerate(out var value))
            return converter.ToStorageString(value);

        throw new InvalidOperationException(
            $"No Id generator is registered for the Id type on '{typeof(T).Name}'. " +
            "Assign the Id before inserting, or pass a 'generate' delegate (or override TryGenerate) in MapIdType.");
    }

    public static IdAccessor<T> Create(JsonTypeInfo<T>? typeInfo, string? idPropertyName = null, IdConverterRegistry? converters = null)
    {
        var targetName = idPropertyName ?? "Id";

        // AOT path: try to find Id via JsonTypeInfo.Properties
        if (typeInfo != null)
        {
            foreach (var prop in typeInfo.Properties)
            {
                if (prop.AttributeProvider is MemberInfo member && member.Name == targetName)
                    return Build(prop.PropertyType, converters, obj => prop.Get!(obj), (obj, val) => prop.Set!(obj, val));
            }
        }

        // Reflection path
        return CreateViaReflection(targetName, converters);
    }

    static IdAccessor<T> Build(Type propertyType, IdConverterRegistry? converters, Func<T, object?> get, Action<T, object?> set)
    {
        if (converters != null && converters.TryGet(propertyType, out var converter))
            return new IdAccessor<T>(IdKind.Custom, get, set, converter);

        return new IdAccessor<T>(ResolveKind(propertyType), get, set, null);
    }

    [UnconditionalSuppressMessage("Trimming", "IL2070", Justification = "Reflection path only used when JsonTypeInfo is not available.")]
    [UnconditionalSuppressMessage("Trimming", "IL2090", Justification = "Reflection path only used when JsonTypeInfo is not available.")]
    static IdAccessor<T> CreateViaReflection(string idPropertyName, IdConverterRegistry? converters)
    {
        var propInfo = typeof(T).GetProperty(idPropertyName, BindingFlags.Public | BindingFlags.Instance);
        if (propInfo == null)
            throw new InvalidOperationException(
                $"Type '{typeof(T).FullName}' must have a public '{idPropertyName}' property. " +
                $"Document types require a property named '{idPropertyName}' of type Guid, int, long, string, or a type registered via MapIdType.");

        return Build(propInfo.PropertyType, converters, obj => propInfo.GetValue(obj), (obj, val) => propInfo.SetValue(obj, val));
    }

    static IdKind ResolveKind(Type type) => type switch
    {
        _ when type == typeof(Guid) => IdKind.Guid,
        _ when type == typeof(int) => IdKind.Int,
        _ when type == typeof(long) => IdKind.Long,
        _ when type == typeof(string) => IdKind.String,
        _ => throw new InvalidOperationException(
            $"Id property on type '{typeof(T).FullName}' has unsupported type '{type.Name}'. " +
            "Supported types are: Guid, int, long, string. Register a converter via MapIdType to use other types.")
    };
}

public sealed class IdAccessorCache
{
    readonly ConcurrentDictionary<Type, object> cache = new();
    readonly Func<Type, string?>? resolveIdPropertyName;
    readonly IdConverterRegistry? converters;

    public IdAccessorCache(Func<Type, string?>? resolveIdPropertyName = null, IdConverterRegistry? converters = null)
    {
        this.resolveIdPropertyName = resolveIdPropertyName;
        this.converters = converters;
    }

    public IdAccessor<T> GetOrCreate<T>(JsonTypeInfo<T>? typeInfo) where T : class
    {
        return (IdAccessor<T>)cache.GetOrAdd(typeof(T), _ =>
        {
            var customName = resolveIdPropertyName?.Invoke(typeof(T));
            return IdAccessor<T>.Create(typeInfo, customName, converters);
        });
    }
}
