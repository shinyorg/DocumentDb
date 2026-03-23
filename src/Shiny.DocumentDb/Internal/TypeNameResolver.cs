namespace Shiny.DocumentDb.Internal;

internal static class TypeNameResolver
{
    public static string Resolve(Type type, TypeNameResolution resolution) => resolution switch
    {
        TypeNameResolution.ShortName => type.Name,
        TypeNameResolution.FullName => type.FullName ?? type.Name,
        _ => throw new ArgumentOutOfRangeException(nameof(resolution))
    };
}
