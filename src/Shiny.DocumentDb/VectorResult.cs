namespace Shiny.DocumentDb;

/// <summary>
/// A document returned from a vector search with its computed score. Results are always ordered nearest-first
/// regardless of provider, so rely on the ordering rather than the absolute <see cref="Score"/> value when you
/// only need ranking.
/// <para>
/// <b>Score semantics are provider-specific by design</b> and are not converted to a single canonical scale —
/// the underlying engines expose fundamentally different, sometimes lossy, metrics (Atlas returns a normalized
/// similarity that has no exact inverse to a distance), so any forced conversion would be misleading. Read
/// <see cref="Score"/> as follows:
/// </para>
/// <list type="bullet">
/// <item><description>
/// <b>Relational providers</b> (SQLite/DuckDb/PostgreSQL/SQL Server/MySQL/Oracle): for
/// <see cref="VectorDistance.Cosine"/> and <see cref="VectorDistance.Euclidean"/> the score is a <i>distance</i>
/// (lower = closer). For <see cref="VectorDistance.DotProduct"/> it is the raw inner product (higher = closer),
/// and for <see cref="VectorDistance.Hamming"/> the bit-difference count (lower = closer).
/// </description></item>
/// <item><description>
/// <b>Document providers</b> (MongoDB Atlas <c>vectorSearchScore</c>, CosmosDB <c>VectorDistance</c>): for
/// Cosine/Euclidean the score is a normalized <i>similarity</i> (higher = closer) — the opposite direction and
/// scale from the relational distance for the same metric. DotProduct still reads higher = closer.
/// </description></item>
/// </list>
/// <para>
/// In short: the ordering is portable; the <see cref="Score"/> number's meaning and direction depend on the
/// backend, so don't compare raw scores across providers or apply a fixed threshold without accounting for the
/// backend in use.
/// </para>
/// </summary>
public class VectorResult<T> where T : class
{
    public required T Document { get; init; }
    public float Score { get; init; }
}
