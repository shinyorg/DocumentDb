namespace Shiny.DocumentDb.Internal;

/// <summary>
/// Splits an ordering string — <c>"total:number desc, lower(name)"</c> — into its individual keys and
/// directions.
/// </summary>
/// <remarks>
/// Genuinely new parsing rather than a reuse: the typed <c>OrderBy(string)</c> helper only routes into the
/// filter grammar when the string contains <c>(</c>, so <c>"total desc"</c> would otherwise be handed to the
/// path resolver whole and fail as an unknown property. Splitting happens here, and each key then goes
/// through the same value-selector grammar as everything else.
/// </remarks>
static class DynamicOrderByParser
{
    public readonly record struct OrderKey(string Expression, bool Descending);

    public static IReadOnlyList<OrderKey> Parse(string ordering)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(ordering);

        var keys = new List<OrderKey>();
        foreach (var part in SplitTopLevel(ordering))
        {
            var text = part.Trim();
            if (text.Length == 0)
                throw new ArgumentException($"Ordering '{ordering}' contains an empty key.", nameof(ordering));

            var descending = TryStripDirection(ref text);
            if (text.Length == 0)
                throw new ArgumentException($"Ordering '{ordering}' has a direction with no key.", nameof(ordering));

            keys.Add(new OrderKey(text, descending));
        }

        if (keys.Count == 0)
            throw new ArgumentException($"Ordering '{ordering}' has no keys.", nameof(ordering));

        return keys;
    }

    // Commas inside a function call — distance(area, '<geojson>') — or inside a quoted literal are not key
    // separators.
    static List<string> SplitTopLevel(string ordering)
    {
        var parts = new List<string>();
        var depth = 0;
        var quote = '\0';
        var start = 0;

        for (var i = 0; i < ordering.Length; i++)
        {
            var c = ordering[i];
            if (quote != '\0')
            {
                if (c == quote)
                    quote = '\0';
                continue;
            }

            switch (c)
            {
                case '\'' or '"':
                    quote = c;
                    break;
                case '(':
                    depth++;
                    break;
                case ')':
                    depth--;
                    break;
                case ',' when depth == 0:
                    parts.Add(ordering[start..i]);
                    start = i + 1;
                    break;
            }
        }

        parts.Add(ordering[start..]);
        return parts;
    }

    // Same direction words the typed OrderBy(path, direction) helper accepts.
    static bool TryStripDirection(ref string text)
    {
        var space = text.LastIndexOfAny([' ', '\t']);
        if (space < 0)
            return false;

        var tail = text[(space + 1)..];
        switch (tail.ToLowerInvariant())
        {
            case "desc":
            case "descending":
                text = text[..space].TrimEnd();
                return true;
            case "asc":
            case "ascending":
                text = text[..space].TrimEnd();
                return false;
            default:
                return false;
        }
    }
}
