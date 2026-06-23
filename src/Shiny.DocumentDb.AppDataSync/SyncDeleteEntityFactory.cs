using System.Diagnostics.CodeAnalysis;
using System.Reflection;
using Shiny.Data.Sync;

namespace Shiny.DocumentDb.AppDataSync;


/// <summary>
/// Builds a minimal entity instance for a delete-by-id write so the operation can be queued
/// (Shiny.Data.Sync's <c>Queue&lt;T&gt;</c> reads <see cref="ISyncEntity.Identifier"/> and sends a
/// null payload for deletes). The identifier is applied to the type's id property (convention: a public
/// property named <c>Id</c>) and converted to the property's CLR type
/// (<see cref="Guid"/>/<see cref="string"/>/<see cref="int"/>/<see cref="long"/>).
/// </summary>
public static class SyncDeleteEntityFactory
{
    public static Func<string, ISyncEntity?> Create<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties | DynamicallyAccessedMemberTypes.PublicParameterlessConstructor)] T>()
        where T : class, ISyncEntity
    {
        var idProp = typeof(T).GetProperty("Id", BindingFlags.Public | BindingFlags.Instance);
        return id =>
        {
            if (idProp == null || !idProp.CanWrite)
                return null;

            T instance;
            try { instance = Activator.CreateInstance<T>(); }
            catch { return null; }

            object? value = ConvertId(id, idProp.PropertyType);
            if (value == null)
                return null;

            idProp.SetValue(instance, value);
            return instance;
        };
    }


    /// <summary>Reads the strongly-typed id value off an entity's <c>Id</c> property (convention).</summary>
    public static Func<object, object?> CreateIdGetter<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)] T>()
        where T : class
    {
        var idProp = typeof(T).GetProperty("Id", BindingFlags.Public | BindingFlags.Instance);
        return entity => idProp?.GetValue(entity);
    }


    static object? ConvertId(string id, Type targetType)
    {
        if (targetType == typeof(string))
            return id;
        if (targetType == typeof(Guid))
            return Guid.TryParse(id, out var g) ? g : null;
        if (targetType == typeof(int))
            return int.TryParse(id, out var i) ? i : null;
        if (targetType == typeof(long))
            return long.TryParse(id, out var l) ? l : null;
        return null;
    }
}
