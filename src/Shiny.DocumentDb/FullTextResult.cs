namespace Shiny.DocumentDb;

/// <summary>
/// A document returned from a full-text search together with its relevance score. Results are ordered
/// by <see cref="Score"/> descending (most relevant first). The score is normalized so that higher
/// always means a better match, but its absolute scale is provider-specific (SQLite/DuckDB BM25,
/// PostgreSQL <c>ts_rank</c>, MySQL <c>MATCH … AGAINST</c>, MongoDB <c>textScore</c>, Cosmos
/// <c>FullTextScore</c>, or the in-memory TF-IDF fallback) — compare scores only within one result set,
/// never across providers.
/// </summary>
public class FullTextResult<T> where T : class
{
    public required T Document { get; init; }
    public double Score { get; init; }
}
