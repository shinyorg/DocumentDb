namespace Shiny.DocumentDb;

/// <summary>
/// The natural language used for tokenization, stemming, and stop-word handling when building a
/// full-text index via <c>MapFullTextProperty</c>. Providers map this to their native configuration:
/// PostgreSQL <c>regconfig</c> (<c>to_tsvector('english', …)</c>), Oracle Text lexer language, the
/// in-memory fallback stemmer, etc. Backends without per-language support (MySQL, SQL Server default
/// catalog, Cosmos, MongoDB <c>$text</c>) ignore everything except <see cref="Simple"/> vs a stemmed
/// language where they can express it.
/// </summary>
public enum FullTextLanguage
{
    /// <summary>No stemming or stop-words — tokenize on whitespace/punctuation only. Maps to PostgreSQL <c>simple</c>.</summary>
    Simple,
    English,
    Spanish,
    French,
    German,
    Italian,
    Portuguese,
    Dutch,
    Russian
}
