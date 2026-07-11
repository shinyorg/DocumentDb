using System.Data.Common;
using Shiny.DocumentDb.Internal;
using Shiny.DocumentDb.PostgreSql;

namespace Shiny.DocumentDb.CockroachDb;

/// <summary>
/// CockroachDB provider. CockroachDB is PostgreSQL wire-compatible and uses the same <c>Npgsql</c> driver,
/// so nearly the entire SQL surface is inherited from <see cref="PostgreSqlDatabaseProvider"/> — JSONB
/// storage and operators, CRUD, batch/upsert (ON CONFLICT), read-merge-write patch, temporal history,
/// computed (STORED generated) columns, partial JSON indexes, and full-text search (tsvector STORED column
/// + GIN index + <c>ts_rank</c>, available in CockroachDB 23.1+) all lower to identical SQL. Only the
/// features that PostgreSQL implements through extensions or Postgres-only server machinery are overridden.
///
/// <para><b>Divergences handled:</b></para>
/// <list type="bullet">
///   <item><b>Spatial</b> works natively (CockroachDB ships PostGIS-compatible <c>ST_*</c> built-ins and
///   GiST spatial indexes) but there is <b>no <c>CREATE EXTENSION postgis</c></b> — the spatial machinery is
///   built into the binary — so that one provisioning statement is stripped from the sidecar DDL.</item>
///   <item><b>Vector search is native</b> — CockroachDB ships a pgvector-compatible <c>VECTOR(n)</c> type and
///   the same <c>&lt;-&gt;</c> / <c>&lt;=&gt;</c> / <c>&lt;#&gt;</c> distance operators, built into the binary
///   (no <c>CREATE EXTENSION</c>). The sidecar and ANN query are reused from the PostgreSQL provider; the two
///   differences are that no pgvector extension is loaded and search runs <b>brute-force</b> (no
///   <c>USING hnsw</c>/<c>ivfflat</c> index — CockroachDB's native <c>CREATE VECTOR INDEX</c> is v25.2+ and
///   L2-only, a possible future opt-in). <c>Hamming</c> distance is unsupported.</item>
///   <item><b>Native change feed is disabled.</b> CockroachDB has no <c>LISTEN</c>/<c>NOTIFY</c> (the PostgreSQL
///   change feed is built on trigger-driven <c>pg_notify</c>), so <c>SubscribeChanges</c> throws
///   <see cref="NotSupportedException"/> (<c>SupportsChangeFeed</c> is false). In-process change observation
///   (<c>IObservableDocumentStore</c>) is unaffected — writes through this store are still observed in-process.</item>
///   <item><b>Native bulk copy is disabled</b> in favour of the multi-row INSERT path — CockroachDB's binary
///   <c>COPY</c> support does not cover the typed JSONB stream the PostgreSQL bulk importer relies on.</item>
///   <item><b>Soundex is unavailable</b> (no <c>fuzzystrmatch</c> extension), and the per-connection extension
///   provisioning is skipped.</item>
/// </list>
///
/// <para>CockroachDB transactions are <c>SERIALIZABLE</c> by default and may surface retryable
/// (<c>40001</c>) errors under contention; wrap contended batch/unit-of-work operations in application-level
/// retry as CockroachDB recommends.</para>
/// </summary>
public class CockroachDbDatabaseProvider : PostgreSqlDatabaseProvider
{
    public CockroachDbDatabaseProvider(string connectionString) : base(connectionString)
    {
    }

    // No fuzzystrmatch extension → no server-side soundex().
    public override bool SupportsSoundex => false;

    // CockroachDB has no CREATE EXTENSION; skip the per-connection provisioning the base uses for fuzzystrmatch.
    public override Task InitializeConnectionAsync(DbConnection connection, CancellationToken ct)
        => Task.CompletedTask;

    // Native ST_* + GiST spatial is built in, but `CREATE EXTENSION postgis` is neither needed nor accepted.
    public override string? BuildCreateSpatialTablesSql(string tableName)
        => base.BuildCreateSpatialTablesSql(tableName)
            ?.Replace("CREATE EXTENSION IF NOT EXISTS postgis;", "");

    // ── Vector (native pgvector-compatible VECTOR type, brute-force ANN) ──
    // CockroachDB's VECTOR type and <->/<=>/<#> operators are built in, so the upsert/delete/clear/param
    // paths are inherited verbatim from the PostgreSQL provider; only table creation (no pgvector index)
    // and the search prelude (no HNSW ef_search GUC) diverge.

    public override bool SupportsVector => true;

    // VECTOR is built into the binary — no CREATE EXTENSION.
    public override Task LoadVectorExtensionAsync(DbConnection connection, CancellationToken ct)
        => Task.CompletedTask;

    public override string BuildCreateVectorTablesSql(string tableName, string typeName, VectorMapping mapping)
    {
        if (mapping.Metric == VectorDistance.Hamming)
            throw new NotSupportedException("CockroachDB's VECTOR type does not support the Hamming distance metric.");

        // No vector index: CockroachDB's CREATE VECTOR INDEX is v25.2+ and only accelerates L2. The <->/<=>/<#>
        // operators evaluate correctly without one (brute-force scan), which is correct on every VECTOR-capable
        // release. Column type is the pgvector-compatible VECTOR(dimensions).
        var vec = VecTable(tableName, typeName);
        return $"""
            CREATE TABLE IF NOT EXISTS {vec} (
                docId TEXT PRIMARY KEY,
                typeName TEXT NOT NULL,
                embedding VECTOR({mapping.Dimensions}) NOT NULL
            );
            """;
    }

    public override (string Sql, IReadOnlyDictionary<string, object> Parameters) BuildVectorSearchSql(
        string tableName, string typeName, VectorMapping mapping,
        ReadOnlyMemory<float> query, int k, string? additionalWhere)
    {
        // Same shape as the base, minus pgvector's `SET LOCAL hnsw.ef_search` prelude (CockroachDB has no
        // such session setting and no HNSW index to tune).
        var vec = VecTable(tableName, typeName);
        var op = DistanceOperator(mapping.Metric);
        var sql = $"""
            SELECT d.Data::text, (v.embedding {op} CAST(@embedding AS vector)) AS score
            FROM {vec} v
            INNER JOIN "{tableName}" d ON d.Id = v.docId AND d.TypeName = @typeName
            {(additionalWhere != null ? $"WHERE {additionalWhere}" : "")}
            ORDER BY v.embedding {op} CAST(@embedding AS vector)
            LIMIT {k};
            """;

        return (sql, new Dictionary<string, object> { ["@embedding"] = this.FormatVectorParameter(query, mapping) });
    }

    // No LISTEN/NOTIFY.
    public override bool SupportsChangeFeed => false;

    // Binary COPY of the typed JSONB stream isn't supported; use the multi-row INSERT path instead.
    public override bool SupportsBulkCopy => false;
}
