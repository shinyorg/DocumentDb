namespace Shiny.DocumentDb.Internal;

internal static class TypeNameResolver
{
    // A mapped derived type is stored under its hierarchy's base type, so every provider resolves it to the base
    // type's name (and so the base type's table, collection or container).
    public static string Resolve(Type type, TypeNameResolution resolution) => ResolveExact(DocumentShapes.RootOf(type) ?? type, resolution);

    static string ResolveExact(Type type, TypeNameResolution resolution) => resolution switch
    {
        TypeNameResolution.ShortName => type.Name,
        TypeNameResolution.FullName => type.FullName ?? type.Name,
        _ => throw new ArgumentOutOfRangeException(nameof(resolution))
    };
}
