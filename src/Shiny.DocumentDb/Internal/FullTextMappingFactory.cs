using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;
using System.Text.Json;

namespace Shiny.DocumentDb.Internal;

/// <summary>
/// Builds <see cref="FullTextMapping"/> instances from the user-facing <c>MapFullTextProperty</c>
/// overloads. Shared by every options class (relational + Mongo/Cosmos/LiteDB/IndexedDB) so the
/// property-resolution and text-extraction rules stay identical across providers.
/// </summary>
public static class FullTextMappingFactory
{
    public static FullTextMapping FromExpressions<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)] T>(
        IReadOnlyList<Expression<Func<T, string?>>> properties,
        FullTextLanguage language) where T : class
    {
        if (properties.Count == 0)
            throw new ArgumentException("At least one property must be supplied.", nameof(properties));

        var names = new List<string>(properties.Count);
        var accessors = new List<Func<T, string?>>(properties.Count);
        foreach (var property in properties)
        {
            if (property.Body is not MemberExpression member)
                throw new ArgumentException(
                    "Each expression must be a simple property access (e.g., x => x.Body).",
                    nameof(properties));

            var name = member.Member.Name;
            var propInfo = typeof(T).GetProperty(name)
                ?? throw new ArgumentException($"Property '{name}' not found on type '{typeof(T).Name}'.");
            names.Add(name);
            accessors.Add(obj => propInfo.GetValue(obj) as string);
        }

        return new FullTextMapping
        {
            DocumentType = typeof(T),
            PropertyNames = names,
            JsonPaths = null!,
            Language = language,
            GetTexts = obj => accessors.Select(a => a((T)obj))
        };
    }

    public static FullTextMapping FromAccessor<T>(
        IReadOnlyList<string> propertyNames,
        Func<T, IEnumerable<string?>> textSelector,
        FullTextLanguage language) where T : class
    {
        if (propertyNames.Count == 0)
            throw new ArgumentException("At least one property name must be supplied.", nameof(propertyNames));
        ArgumentNullException.ThrowIfNull(textSelector);

        return new FullTextMapping
        {
            DocumentType = typeof(T),
            PropertyNames = propertyNames,
            JsonPaths = null!,
            Language = language,
            GetTexts = obj => textSelector((T)obj)
        };
    }

    /// <summary>
    /// Resolves the JSON paths for any mappings still awaiting them. Prefers the resolved
    /// <see cref="System.Text.Json.Serialization.Metadata.JsonTypeInfo"/> so source-generated
    /// (metadata-mode) contexts and <c>[JsonPropertyName]</c> are honored, falling back to the naming
    /// policy when the type has no resolvable metadata.
    /// </summary>
    public static void ResolveJsonPaths(IEnumerable<FullTextMapping> mappings, JsonSerializerOptions jsonOptions)
    {
        foreach (var mapping in mappings)
        {
            if (mapping.JsonPaths != null!)
                continue;

            mapping.JsonPaths = mapping.PropertyNames
                .Select(n => DocumentStoreOptions.ResolveJsonName(jsonOptions, mapping.DocumentType, n))
                .ToList();
        }
    }

    /// <summary>Sanitizes a type name into a safe SQL identifier suffix (letters/digits/underscore).</summary>
    public static string SanitizeSuffix(string typeName)
    {
        var chars = new char[typeName.Length];
        for (var i = 0; i < typeName.Length; i++)
            chars[i] = char.IsLetterOrDigit(typeName[i]) || typeName[i] == '_' ? typeName[i] : '_';
        return new string(chars);
    }

    /// <summary>Splits search text into lower-cased alphanumeric terms — used to build provider OR-queries safely.</summary>
    public static IReadOnlyList<string> Tokenize(string text)
    {
        var tokens = new List<string>();
        if (string.IsNullOrEmpty(text))
            return tokens;

        var start = -1;
        for (var i = 0; i < text.Length; i++)
        {
            if (char.IsLetterOrDigit(text[i]))
            {
                if (start < 0) start = i;
            }
            else if (start >= 0)
            {
                tokens.Add(text.Substring(start, i - start).ToLowerInvariant());
                start = -1;
            }
        }
        if (start >= 0)
            tokens.Add(text.Substring(start).ToLowerInvariant());
        return tokens;
    }

    /// <summary>
    /// Flattens a document's mapped text into a single lower-cased string for the in-memory fallback —
    /// string values plus the elements of any string collection, space-joined.
    /// </summary>
    public static string ExtractText(FullTextMapping mapping, object document)
    {
        var parts = new List<string>();
        foreach (var fragment in mapping.GetTexts(document))
        {
            if (fragment != null)
                parts.Add(fragment);
        }
        return string.Join(' ', parts);
    }
}
