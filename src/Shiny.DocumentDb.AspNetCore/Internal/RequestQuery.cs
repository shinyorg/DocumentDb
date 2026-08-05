using System.Text;

namespace Shiny.DocumentDb.AspNetCore.Internal;

/// <summary>Raised for anything the caller got wrong. Becomes a <c>400</c> with a ProblemDetails body.</summary>
sealed class BadRequestException(string message) : Exception(message);

/// <summary>
/// The field allowlist. It is enforced <b>lexically</b>, before the filter reaches the grammar parser: every
/// bare identifier in a filter or sort expression must be an allowed field.
/// </summary>
/// <remarks>
/// <para>
/// Lexical rather than semantic because the point is to refuse an unlisted field <i>early</i> — a filter that
/// parses fine and then scans a column nobody meant to publish is exactly the outcome the allowlist exists to
/// prevent. Identifiers followed by <c>(</c> are function calls and are skipped, so the whole scalar-function
/// library keeps working without this file knowing any of their names.
/// </para>
/// <para>
/// String literals are skipped entirely, so a value that happens to look like a field name is never mistaken
/// for one. An empty allowlist means "everything" — the documented default for an internal API.
/// </para>
/// </remarks>
static class FieldAllowList
{
    static readonly HashSet<string> Keywords = new(StringComparer.OrdinalIgnoreCase)
    {
        "and", "or", "not", "in", "true", "false", "null", "asc", "desc", "ascending", "descending",
        // The grammar's :type hints, which follow a path rather than being one.
        "string", "number", "int", "long", "double", "decimal", "bool", "date", "guid"
    };

    /// <summary>
    /// The id is always available, whatever the allowlist says. It is in the route, it is in every response
    /// body, and it is how a client addresses a document at all — refusing <c>fields=id</c> or
    /// <c>filter=id == '…'</c> would protect nothing and make a sparse fieldset useless.
    /// </summary>
    /// <remarks>
    /// This covers the conventional <c>Id</c> property. A type whose identifier is mapped to a
    /// differently-named property still needs that name listed explicitly.
    /// </remarks>
    static readonly HashSet<string> AlwaysAllowed = new(StringComparer.OrdinalIgnoreCase) { "id" };

    public static void Validate(string? expression, IReadOnlySet<string>? allowed, string what)
    {
        if (allowed is null || allowed.Count == 0 || string.IsNullOrWhiteSpace(expression))
            return;

        foreach (var identifier in Identifiers(expression))
        {
            // A dotted path is allowed when its root is — publishing "customer" publishes "customer.name".
            var root = identifier.Split('.')[0];
            if (!Keywords.Contains(identifier) && !AlwaysAllowed.Contains(identifier)
                && !allowed.Contains(identifier) && !allowed.Contains(root))
                throw new BadRequestException(
                    $"'{identifier}' is not available for {what} on this resource. " +
                    $"Available: {string.Join(", ", allowed.Concat(AlwaysAllowed).Order(StringComparer.OrdinalIgnoreCase))}.");
        }
    }

    /// <summary>Bare identifiers — not inside a string literal, not immediately followed by <c>(</c>.</summary>
    static IEnumerable<string> Identifiers(string expression)
    {
        var results = new List<string>();
        var i = 0;

        while (i < expression.Length)
        {
            var c = expression[i];

            if (c is '\'' or '"')
            {
                var quote = c;
                i++;
                while (i < expression.Length)
                {
                    if (expression[i] == quote)
                    {
                        // Doubled quote escapes itself in the grammar.
                        if (i + 1 < expression.Length && expression[i + 1] == quote)
                            i++;
                        else
                            break;
                    }
                    i++;
                }
                i++;
            }
            else if (char.IsLetter(c) || c == '_')
            {
                var start = i;
                while (i < expression.Length && (char.IsLetterOrDigit(expression[i]) || expression[i] is '_' or '.' or ':'))
                    i++;

                var token = expression[start..i];
                var next = i;
                while (next < expression.Length && char.IsWhiteSpace(expression[next]))
                    next++;

                // A function call, not a field.
                if (next >= expression.Length || expression[next] != '(')
                {
                    // "path:type" — the hint is a keyword, the path is what must be allowed.
                    var colon = token.IndexOf(':');
                    results.Add(colon < 0 ? token : token[..colon]);
                }
            }
            else
            {
                i++;
            }
        }
        return results;
    }
}

/// <summary>The paging/sorting/projection arguments every list endpoint reads, already validated.</summary>
sealed class ListRequest
{
    public string? Filter { get; init; }
    public string? OrderBy { get; init; }
    public string? Fields { get; init; }
    public string? Cursor { get; init; }
    public int Skip { get; init; }
    public int Take { get; init; }

    public static ListRequest Parse(
        Microsoft.AspNetCore.Http.IQueryCollection query,
        int defaultPageSize,
        int maxPageSize,
        IReadOnlySet<string>? allowed)
    {
        var take = Int(query, "take") ?? defaultPageSize;
        if (take <= 0)
            take = defaultPageSize;

        // Clamped, not refused: a client asking for 10,000 gets the cap, which is the friendlier contract and
        // still bounds the work.
        if (take > maxPageSize)
            take = maxPageSize;

        var skip = Int(query, "skip") ?? 0;
        if (skip < 0)
            skip = 0;

        var request = new ListRequest
        {
            Filter = Str(query, "filter"),
            OrderBy = Str(query, "orderby"),
            Fields = Str(query, "fields"),
            Cursor = Str(query, "cursor"),
            Skip = skip,
            Take = take
        };

        FieldAllowList.Validate(request.Filter, allowed, "filtering");
        FieldAllowList.Validate(request.OrderBy, allowed, "sorting");
        FieldAllowList.Validate(request.Fields, allowed, "projection");
        return request;
    }

    static string? Str(Microsoft.AspNetCore.Http.IQueryCollection query, string key)
        => query.TryGetValue(key, out var value) && !string.IsNullOrWhiteSpace(value)
            ? value.ToString()
            : null;

    static int? Int(Microsoft.AspNetCore.Http.IQueryCollection query, string key)
    {
        var raw = Str(query, key);
        if (raw is null)
            return null;

        return int.TryParse(raw, out var parsed)
            ? parsed
            : throw new BadRequestException($"'{key}' must be an integer.");
    }

    /// <summary>Splits <c>"total desc, name"</c> into its keys, each already allowlist-checked.</summary>
    public IEnumerable<(string Path, string? Direction)> SortKeys()
    {
        if (string.IsNullOrWhiteSpace(this.OrderBy))
            yield break;

        foreach (var part in this.OrderBy.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
        {
            var pieces = part.Split(' ', StringSplitOptions.RemoveEmptyEntries);
            yield return (pieces[0], pieces.Length > 1 ? pieces[1] : null);
        }
    }
}
