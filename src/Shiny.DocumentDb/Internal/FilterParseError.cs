namespace Shiny.DocumentDb.Internal;

/// <summary>
/// The one place the filter grammar's error shape is defined, so the parser and both field binders report
/// identically.
/// </summary>
static class FilterParseError
{
    public static ArgumentException Create(string message, int position)
        => new($"Filter parse error at position {position}: {message}.");
}
