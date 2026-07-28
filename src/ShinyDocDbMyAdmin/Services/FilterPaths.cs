using System.Text.RegularExpressions;

namespace ShinyDocDbMyAdmin.Services;

/// <summary>
/// Pulls the field paths out of a filter-grammar expression, so the console can say which of them are
/// unindexed.
/// </summary>
/// <remarks>
/// A regex rather than the real parser, and deliberately so: this drives a <i>suggestion</i>, and the
/// cost of getting it wrong is a path offered that should not be (harmless — creating the index is a
/// separate, explicit click) or one missed (the user types it themselves). Reaching into the library's
/// parser to recover its field references would couple an admin convenience to internals that exist to
/// build queries, not to describe them. If the grammar itself gains a "what does this reference"
/// surface, this should use it.
/// </remarks>
public static partial class FilterPaths
{
    /// <summary>
    /// Dotted identifiers, optionally carrying a <c>:type</c> hint, which the grammar allows on any
    /// field reference.
    /// </summary>
    [GeneratedRegex(@"\b([A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)*)\s*(?::[a-z]+)?", RegexOptions.Compiled)]
    private static partial Regex PathPattern();

    /// <summary>String and number literals, stripped before matching so their contents cannot look like a field.</summary>
    [GeneratedRegex(@"'[^']*'|""[^""]*""|\b\d+(\.\d+)?\b", RegexOptions.Compiled)]
    private static partial Regex LiteralPattern();

    /// <summary>
    /// Grammar keywords, functions and sort directions — everything that matches the shape of a field
    /// reference without being one.
    /// </summary>
    static readonly HashSet<string> Reserved = new(StringComparer.OrdinalIgnoreCase)
    {
        "and", "or", "not", "null", "true", "false", "asc", "desc", "in", "like",
        "lower", "upper", "trim", "length", "substring", "concat", "replace", "abs", "round",
        "floor", "ceiling", "year", "month", "day", "hour", "minute", "second", "date",
        "contains", "startswith", "endswith", "isnull", "coalesce", "soundex",
        "min", "max", "sum", "avg", "count", "hasflag", "lucenematch", "lucenescore",
        "string", "number", "int", "long", "double", "decimal", "bool", "guid"
    };

    /// <summary>The distinct field paths a filter or ordering expression references, in first-seen order.</summary>
    public static IReadOnlyList<string> Extract(string? expression)
    {
        if (string.IsNullOrWhiteSpace(expression))
            return [];

        // Blank out literals first: 'Shipped' must not contribute a path called Shipped.
        var stripped = LiteralPattern().Replace(expression, " ");

        var seen = new HashSet<string>(StringComparer.Ordinal);
        var paths = new List<string>();

        foreach (Match match in PathPattern().Matches(stripped))
        {
            var path = match.Groups[1].Value;

            // A bare reserved word is grammar. A dotted path whose first segment happens to be one
            // ("date.created") is a field, so only the single-segment case is filtered.
            var isKeyword = !path.Contains('.') && Reserved.Contains(path);

            if (!isKeyword && seen.Add(path))
                paths.Add(path);
        }

        return paths;
    }
}
