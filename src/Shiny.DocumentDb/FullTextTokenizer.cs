namespace Shiny.DocumentDb;

/// <summary>How a full-text index splits text into searchable units.</summary>
public enum FullTextTokenizer
{
    /// <summary>Whole words, stemmed per <see cref="FullTextLanguage"/> where the provider can. The default.</summary>
    Word,

    /// <summary>
    /// Every three-character run (SQLite FTS5 <c>tokenize='trigram'</c>; a <c>pg_trgm</c> GIN index on PostgreSQL and
    /// CockroachDB), which gives case-insensitive substring
    /// matching: "spo" finds "Sports". A search ANDs its whitespace-separated terms; terms shorter than three
    /// characters can't be looked up in a trigram index and are ignored, so a search made only of such terms
    /// returns nothing. SQLite, PostgreSQL and CockroachDB — other providers throw <see cref="NotSupportedException"/> at table init.
    /// </summary>
    Trigram
}
