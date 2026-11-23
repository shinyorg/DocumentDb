namespace Shiny.DocumentDb.Internal;

/// <summary>
/// Registration record produced by <c>MapFullTextProperty&lt;T&gt;</c>. Public because
/// <see cref="IDatabaseProvider"/> SQL builders read its JSON paths and language to emit the
/// index DDL and search query — there is no behavior here for consumers to override.
/// </summary>
/// <remarks>
/// <see cref="GetTexts"/> is used only by the in-memory fallback providers (LiteDB, IndexedDB),
/// which tokenize the live object rather than pushing the search into the database. Relational and
/// document providers index the text extracted at <see cref="JsonPaths"/> directly.
/// </remarks>
public class FullTextMapping
{
    public required Type DocumentType { get; init; }

    /// <summary>The CLR property names that feed the index, in declaration order.</summary>
    public required IReadOnlyList<string> PropertyNames { get; init; }

    /// <summary>The resolved JSON paths (naming policy applied) — set lazily once JsonSerializerOptions are known.</summary>
    public IReadOnlyList<string> JsonPaths { get; set; } = null!;

    public required FullTextLanguage Language { get; init; }

    /// <summary>Word (the default) or trigram (substring) indexing.</summary>
    public FullTextTokenizer Tokenizer { get; init; } = FullTextTokenizer.Word;

    /// <summary>
    /// The element type of a collection mapping (see <see cref="ElementPropertyNames"/>), null for a plain one.
    /// </summary>
    public Type? ElementType { get; init; }

    /// <summary>
    /// Parallel to <see cref="PropertyNames"/>: for an entry that names a collection of objects, the property of each
    /// element that is indexed (e.g. a programme's title out of a list of programmes); null indexes the property itself.
    /// Null when no entry is a collection.
    /// </summary>
    public IReadOnlyList<string?>? ElementPropertyNames { get; init; }

    /// <summary>The resolved JSON names of <see cref="ElementPropertyNames"/> — set alongside <see cref="JsonPaths"/>.</summary>
    public IReadOnlyList<string?>? ElementJsonPaths { get; set; }

    /// <summary>
    /// Extracts the searchable text fragments from a live document. A null/empty fragment is skipped.
    /// Used only by the in-memory fallback providers.
    /// </summary>
    public required Func<object, IEnumerable<string?>> GetTexts { get; init; }
}
