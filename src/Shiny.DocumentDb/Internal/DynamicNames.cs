namespace Shiny.DocumentDb.Internal;

/// <summary>
/// Validates the two user-supplied strings that reach DDL and interpolated SQL on a schema-free collection:
/// the collection name (which becomes the row's <c>TypeName</c> and part of an index identifier) and a JSON
/// path (which every relational provider interpolates into <c>json_extract(Data, '$.{path}')</c> as a
/// literal, not a parameter).
/// </summary>
/// <remarks>
/// Deliberately strict rather than clever: an allow-list of identifier characters cannot be escaped out of,
/// whereas quoting rules differ per dialect. The documented consequence is that JSON keys containing dashes,
/// spaces or dots are storable but not addressable — a bracket/quoted-segment syntax is a later cut.
/// </remarks>
static class DynamicNames
{
    const int MaxCollectionLength = 128;
    const int MaxPathSegments = 16;

    /// <summary>Validates a collection name — <c>^[A-Za-z_][A-Za-z0-9_]{0,127}$</c>. Returns it unchanged.</summary>
    public static string ValidateCollection(string name)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);

        if (name.Length > MaxCollectionLength)
            throw new ArgumentException(
                $"Collection name '{name}' is longer than {MaxCollectionLength} characters.", nameof(name));

        if (!IsIdentifierStart(name[0]))
            throw new ArgumentException(
                $"Collection name '{name}' must start with a letter or underscore.", nameof(name));

        for (var i = 1; i < name.Length; i++)
        {
            if (!IsIdentifierPart(name[i]))
                throw new ArgumentException(
                    $"Collection name '{name}' may only contain letters, digits and underscores.", nameof(name));
        }

        return name;
    }

    /// <summary>
    /// Validates a dot-separated JSON path — 1..16 segments, each <c>^[A-Za-z_][A-Za-z0-9_]*$</c>. Returns it
    /// unchanged.
    /// </summary>
    public static string ValidatePath(string path)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(path);

        var segments = path.Split('.');
        if (segments.Length > MaxPathSegments)
            throw new ArgumentException(
                $"Field path '{path}' has more than {MaxPathSegments} segments.", nameof(path));

        foreach (var segment in segments)
        {
            if (segment.Length == 0)
                throw new ArgumentException($"Field path '{path}' contains an empty segment.", nameof(path));

            if (!IsIdentifierStart(segment[0]))
                throw new ArgumentException(
                    $"Field path segment '{segment}' must start with a letter or underscore.", nameof(path));

            for (var i = 1; i < segment.Length; i++)
            {
                if (!IsIdentifierPart(segment[i]))
                    throw new ArgumentException(
                        $"Field path segment '{segment}' may only contain letters, digits and underscores. " +
                        "JSON keys with dashes, spaces or dots are storable but not addressable.", nameof(path));
            }
        }

        return path;
    }

    // ASCII-only on purpose: 'A'..'Z' rather than char.IsLetter, so a Unicode letter that a dialect might
    // fold or normalize differently never reaches an identifier.
    static bool IsIdentifierStart(char c)
        => c is (>= 'A' and <= 'Z') or (>= 'a' and <= 'z') or '_';

    static bool IsIdentifierPart(char c)
        => c is (>= 'A' and <= 'Z') or (>= 'a' and <= 'z') or (>= '0' and <= '9') or '_';
}
