using System.Linq.Expressions;
using System.Text.Json.Nodes;
using System.Text.Json.Serialization.Metadata;
using Shiny.DocumentDb.Internal;

namespace Shiny.DocumentDb.Hosting;

/// <summary>
/// Parses the document filter grammar — the same string a caller passes to
/// <see cref="DocumentQueryExtensions.Where{T}(IDocumentQuery{T}, string, JsonTypeInfo{T})"/> — into an
/// expression a host can use directly.
/// </summary>
/// <remarks>
/// <para>
/// A host that only pushes filters into a query never needs this: it hands the string to <c>Where</c> and the
/// query layer parses it. It is needed when the filter has to be applied to documents that never pass through
/// a query — the live tail of a change stream, where each change arrives in memory and <c>?filter=</c> still
/// has to mean the same thing it meant for the initial page. Without it, a streaming endpoint has to drop
/// filtering entirely.
/// </para>
/// <para>
/// Pair it with <see cref="DocumentPredicate.Compile{T}"/> to get a delegate: parsing produces an expression
/// tree, and compiling it with <c>Reflection.Emit</c> would forfeit the host's AOT guarantee.
/// </para>
/// <para>Parsing itself is AOT-safe — fields resolve by walking <see cref="JsonTypeInfo.Properties"/>.</para>
/// </remarks>
public static class DocumentFilter
{
    /// <summary>
    /// Parses a filter over a document type.
    /// </summary>
    /// <typeparam name="T">The document type.</typeparam>
    /// <param name="filter">The filter expression, e.g. <c>"Age &gt;= 30 and Status == 'open'"</c>.</param>
    /// <param name="typeInfo">Source-generated metadata for <typeparamref name="T"/>, used to resolve fields.</param>
    /// <exception cref="ArgumentException">The filter is malformed, or names a field that cannot be resolved.</exception>
    public static Expression<Func<T, bool>> Parse<T>(string filter, JsonTypeInfo<T> typeInfo) where T : class
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(filter);
        ArgumentNullException.ThrowIfNull(typeInfo);

        return FilterExpressionParser.Parse(filter, typeInfo);
    }

    /// <summary>
    /// Parses a filter over a schema-free JSON collection, producing a predicate on the raw body.
    /// </summary>
    /// <param name="filter">The filter expression.</param>
    /// <param name="typeInfo">
    /// Metadata for the collection's document type when it has one — field names and leaf types then resolve the
    /// same way they do on the typed lane. Pass <c>null</c> for a truly schema-free collection, where paths take a
    /// <c>path:type</c> hint instead (e.g. <c>"weight:number &gt; 5"</c>).
    /// </param>
    /// <exception cref="ArgumentException">The filter is malformed, or names a field that cannot be resolved.</exception>
    public static Expression<Func<JsonObject, bool>> ParseJson(string filter, JsonTypeInfo? typeInfo = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(filter);

        return FilterExpressionParser.ParseJson(filter, null, typeInfo);
    }
}
