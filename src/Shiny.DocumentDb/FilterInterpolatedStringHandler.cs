using System.Runtime.CompilerServices;
using System.Text;

namespace Shiny.DocumentDb;

/// <summary>
/// Interpolated string handler used by
/// <see cref="DocumentQueryExtensions.Where{T}(IDocumentQuery{T}, FilterInterpolatedStringHandler, System.Text.Json.Serialization.Metadata.JsonTypeInfo{T})"/>.
/// Captures each interpolated <c>{value}</c> hole as a strongly-typed argument rather than formatting it
/// into the filter text, so values are bound as parameters instead of being concatenated into the query.
/// </summary>
/// <remarks>
/// <para>
/// This mirrors the Dapper / InterpolatedSql pattern: writing
/// <c>query.Where($"Status == {status} and Age &gt;= {minAge}")</c> never produces a string with the
/// values spliced in. Each hole is collected here and later resolved to a typed constant, which keeps the
/// filter injection-safe and removes the need to manually quote string values.
/// </para>
/// <para>
/// Because an interpolated string literal prefers an interpolated-string-handler overload over a
/// <see cref="string"/> overload, <c>Where($"...")</c> binds here while <c>Where(rawString)</c> still binds
/// to the raw <see cref="DocumentQueryExtensions.Where{T}(IDocumentQuery{T}, string, System.Text.Json.Serialization.Metadata.JsonTypeInfo{T})"/> overload.
/// </para>
/// </remarks>
[InterpolatedStringHandler]
public readonly ref struct FilterInterpolatedStringHandler
{
    /// <summary>
    /// Sentinel (NUL) that brackets a placeholder index in the assembled filter text. The lexer treats a
    /// sentinel-delimited index as a single argument reference. NUL does not appear in normal filter
    /// content, so it can never collide with user-written text.
    /// </summary>
    internal const char PlaceholderSentinel = (char)0;

    readonly StringBuilder builder;
    readonly List<object?> arguments;

    /// <summary>Creates the handler. Invoked by the compiler for an interpolated string argument.</summary>
    /// <param name="literalLength">Total length of the literal (non-hole) segments.</param>
    /// <param name="formattedCount">Number of interpolation holes.</param>
    public FilterInterpolatedStringHandler(int literalLength, int formattedCount)
    {
        this.builder = new StringBuilder(literalLength + (formattedCount * 4));
        this.arguments = new List<object?>(formattedCount);
    }

    /// <summary>Appends a literal (non-interpolated) segment of the filter verbatim.</summary>
    public void AppendLiteral(string value) => this.builder.Append(value);

    /// <summary>Captures an interpolated value and writes a placeholder token in its place.</summary>
    public void AppendFormatted<T>(T value)
    {
        var index = this.arguments.Count;
        this.arguments.Add(value);
        this.builder
            .Append(PlaceholderSentinel)
            .Append(index)
            .Append(PlaceholderSentinel);
    }

    /// <summary>The assembled filter text, with each interpolated value replaced by a placeholder token.</summary>
    internal string Filter => this.builder.ToString();

    /// <summary>The captured interpolation values, indexed by placeholder position.</summary>
    internal IReadOnlyList<object?> Arguments => this.arguments;
}
