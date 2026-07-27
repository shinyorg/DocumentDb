namespace Shiny.DocumentDb.Internal;

/// <summary>
/// Schema-free twins of <c>DocumentQueryExtensions.ResolveJsonPath</c> /
/// <c>ResolveJsonPathWithType</c>, for the callers that turn a path string straight into SQL without going
/// through the expression grammar — the grouped-projection translator and <c>Project(string)</c>.
/// </summary>
/// <remarks>
/// On a schema-free collection the path <em>is</em> the JSON path (there is no naming policy to apply), so
/// the work here is validating it and peeling off an optional <c>:type</c> hint.
/// </remarks>
static class DynamicPathResolver
{
    /// <summary>Returns the validated JSON path and the leaf segment, used as a projection's default alias.</summary>
    public static (string JsonPath, string LeafJsonName) ResolveJsonPath(string path)
    {
        var (jsonPath, _) = Split(path);
        var dot = jsonPath.LastIndexOf('.');
        return (jsonPath, dot < 0 ? jsonPath : jsonPath[(dot + 1)..]);
    }

    /// <summary>
    /// Returns the validated JSON path and the CLR type its extraction should be cast to — the
    /// <c>:type</c> hint when present, otherwise <see cref="string"/> (which every dialect emits as the plain
    /// untyped extract).
    /// </summary>
    public static (string JsonPath, Type LeafType) ResolveJsonPathWithType(string path)
    {
        var (jsonPath, hint) = Split(path);
        return (jsonPath, hint ?? typeof(string));
    }

    static (string JsonPath, Type? Hint) Split(string path)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(path);

        var colon = path.IndexOf(':');
        if (colon < 0)
            return (DynamicNames.ValidatePath(path.Trim()), null);

        var jsonPath = DynamicNames.ValidatePath(path[..colon].Trim());
        var name = path[(colon + 1)..].Trim();
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
                $"'{name}' is not a known type hint. Use one of: string, number, int, long, double, decimal, bool, date, guid.",
                nameof(path))
        };
        return (jsonPath, hint);
    }
}
