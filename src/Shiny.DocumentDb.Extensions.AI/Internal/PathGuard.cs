namespace Shiny.DocumentDb.Extensions.AI.Internal;

/// <summary>
/// Validates a field path before it is written into a string-grammar filter. The store validates paths too
/// (they reach SQL as literals, not parameters), so this is not the only guard — it is the one that runs
/// first, on a path the <b>LLM</b> chose, so the model gets a clean tool error naming the field rather than
/// a parser or SQL error, and so nothing it supplies can widen the generated grammar.
/// </summary>
/// <remarks>Deliberately the same allow-list as the store: 1..16 dot-separated <c>[A-Za-z_][A-Za-z0-9_]*</c>
/// segments, ASCII only.</remarks>
static class PathGuard
{
    const int MaxSegments = 16;

    public static string Validate(string path)
    {
        if (string.IsNullOrWhiteSpace(path))
            throw new InvalidOperationException("A field path is required.");

        var segments = path.Split('.');
        if (segments.Length > MaxSegments)
            throw new InvalidOperationException($"Field path '{path}' has more than {MaxSegments} segments.");

        foreach (var segment in segments)
        {
            if (segment.Length == 0 || !IsStart(segment[0]))
                throw new InvalidOperationException(
                    $"Field path '{path}' is not a valid document path. Each segment must start with a letter or underscore.");

            for (var i = 1; i < segment.Length; i++)
            {
                if (!IsPart(segment[i]))
                    throw new InvalidOperationException(
                        $"Field path '{path}' is not a valid document path. Segments may only contain letters, digits and underscores.");
            }
        }
        return path;
    }

    static bool IsStart(char c) => c is (>= 'A' and <= 'Z') or (>= 'a' and <= 'z') or '_';
    static bool IsPart(char c) => IsStart(c) || c is >= '0' and <= '9';
}
