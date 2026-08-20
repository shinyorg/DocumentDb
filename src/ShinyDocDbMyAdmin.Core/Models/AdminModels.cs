using System.Text.Json.Nodes;
using ShinyDocDbMyAdmin.Services;

namespace ShinyDocDbMyAdmin.Models;

/// <summary>A table in the target database, classified by what ShinyDocDbMyAdmin can do with it.</summary>
/// <param name="Owner">
/// The documents table this one belongs to, for a sidecar. Null for a documents table and for a foreign
/// one - a sidecar with no owner is not a sidecar, it is someone else's table that happens to be named
/// like ours.
/// </param>
/// <param name="Confidence">
/// How sure the classification is. Only ever <see cref="TableConfidence.Probable"/> for a documents table
/// that carries the envelope and nothing else; every other role is named from a table we found, so it is
/// <see cref="TableConfidence.Confirmed"/> by construction.
/// </param>
/// <param name="Feature">
/// What this table is beyond its role - the sidecar's flavour ("R*Tree shadow", "FTS5 shadow"), or why a
/// documents table is only probable. Null when the role says everything there is to say.
/// </param>
public sealed record TableInfo(
    string Name,
    TableRole Role,
    bool HasTenantColumn = false,
    string? Owner = null,
    TableConfidence Confidence = TableConfidence.Confirmed,
    string? Feature = null)
{
    public bool IsBrowsable => this.Role == TableRole.Documents;

    /// <summary>True when this table is DocumentDb's - a documents table or one of its sidecars.</summary>
    public bool IsOwned => this.Role != TableRole.Foreign;
}

public enum TableRole
{
    /// <summary>Carries the Id / TypeName / Data envelope - browsable.</summary>
    Documents,

    /// <summary>A <c>{table}_history</c> temporal sidecar.</summary>
    History,

    /// <summary>A <c>{table}_blobs</c> binary sidecar.</summary>
    Blobs,

    /// <summary>A <c>{table}_spatial</c> sidecar, or one of the engine shadows behind it.</summary>
    Spatial,

    /// <summary>A <c>{table}_vec_{type}</c> sidecar, its id map, or one of the engine shadows behind it.</summary>
    Vector,

    /// <summary>A full-text index table or one of its shadows.</summary>
    FullText,

    /// <summary>Not a DocumentDb table at all.</summary>
    Foreign
}

/// <summary>How sure a table classification is. See <see cref="TableInfo.Confidence"/>.</summary>
public enum TableConfidence
{
    /// <summary>The envelope plus at least one signal only DocumentDb leaves behind.</summary>
    Confirmed,

    /// <summary>The envelope columns and nothing else - most likely ours, but nothing proves it.</summary>
    Probable
}

/// <summary>
/// The answer to "is this database participating in DocumentDb, and what of it is ours?" - computed from
/// the same single catalog read that classifies the tables, so asking costs nothing extra.
/// </summary>
/// <param name="Participates">True when at least one table carries the document envelope.</param>
/// <param name="Confidence">
/// <see cref="IdentityConfidence.Confirmed"/> when at least one document table is itself confirmed,
/// <see cref="IdentityConfidence.Probable"/> when every one of them is only probable, and
/// <see cref="IdentityConfidence.None"/> when there are none.
/// </param>
/// <param name="Features">
/// The DocumentDb features this database provably uses - temporal, blobs, spatial, vector, full text,
/// outbox, tenant, encryption, soft delete. Only what the evidence supports: soft delete appears here
/// exclusively when it has been declared on the connection, because nothing in the database records it.
/// </param>
/// <param name="Reasons">Why the verdict reads the way it does, in the operator's words.</param>
public sealed record DatabaseIdentity(
    bool Participates,
    IdentityConfidence Confidence,
    int DocumentTables,
    int TypeCount,
    int OwnedSidecars,
    int ForeignTables,
    IReadOnlyList<string> Features,
    IReadOnlyList<string> Reasons)
{
    /// <summary>The verdict as one sentence - the banner, the TUI status line and the connection test all use it.</summary>
    public string Summary
    {
        get
        {
            if (!this.Participates)
                return $"Not a DocumentDb database - {this.ForeignTables} table(s), none carry the envelope.";

            var parts = new List<string>
            {
                this.Confidence == IdentityConfidence.Probable ? "Probably a DocumentDb store" : "DocumentDb store",
                $"{this.DocumentTables} document table(s)",
                $"{this.TypeCount} type(s)"
            };

            if (this.Features.Count > 0)
                parts.Add(string.Join(", ", this.Features));

            return string.Join(" · ", parts);
        }
    }
}

public enum IdentityConfidence
{
    /// <summary>Nothing in this database carries the document envelope.</summary>
    None,

    /// <summary>Every document table is envelope-only - no index, no data, nothing that proves authorship.</summary>
    Probable,

    /// <summary>At least one document table carries a signal only DocumentDb leaves behind.</summary>
    Confirmed
}

/// <summary>One distinct <c>TypeName</c> value inside a documents table - the equivalent of a "table" in phpMyAdmin.</summary>
public sealed record DocumentTypeInfo(string TypeName, long Count);

/// <summary>An envelope row plus its parsed body.</summary>
public sealed record DocumentRow(
    string Id,
    string TypeName,
    string Json,
    DateTimeOffset? CreatedAt,
    DateTimeOffset? UpdatedAt,
    string? TenantId = null
)
{
    JsonNode? parsed;
    bool parseAttempted;

    /// <summary>The document body, or null when the stored text is not valid JSON.</summary>
    public JsonNode? Body
    {
        get
        {
            if (!this.parseAttempted)
            {
                this.parseAttempted = true;
                try { this.parsed = JsonNode.Parse(this.Json); }
                catch (Exception) { this.parsed = null; }
            }
            return this.parsed;
        }
    }

    /// <summary>Reads a dotted JSON path out of the body for grid display.</summary>
    public JsonNode? Read(string path)
    {
        var node = this.Body;
        foreach (var segment in path.Split('.', StringSplitOptions.RemoveEmptyEntries))
        {
            if (node is not JsonObject obj || !obj.TryGetPropertyValue(segment, out var next))
                return null;
            node = next;
        }
        return node;
    }
}

public sealed record DocumentPage(IReadOnlyList<DocumentRow> Rows, long TotalCount, int Page, int PageSize)
{
    public int PageCount => this.PageSize <= 0 ? 1 : (int)Math.Max(1, Math.Ceiling(this.TotalCount / (double)this.PageSize));
}

/// <summary>A field discovered by sampling documents of one type.</summary>
/// <param name="Encryption">
/// Set when at least one sampled value on this path was a field-level encryption envelope; null otherwise.
/// </param>
/// <param name="SoftDeleteCandidate">
/// The kind of soft-delete flag this path <b>looks like</b>, or null. Never a verdict: soft delete writes
/// no DDL, so nothing in the database can prove it, and this is only a suggestion for the operator to
/// confirm into <c>ConnectionProfile.SoftDeleteFlags</c>. It changes no role, no browsability and no
/// delete behaviour on its own. See <c>DocumentAdminService.SoftDeleteCandidateKind</c>.
/// </param>
public sealed record InferredField(
    string Path,
    string Types,
    int Occurrences,
    int SampleSize,
    string? Example,
    EncryptedFieldInfo? Encryption = null,
    SoftDeleteFlagKind? SoftDeleteCandidate = null)
{
    public int PercentPresent => this.SampleSize == 0 ? 0 : (int)Math.Round(this.Occurrences * 100.0 / this.SampleSize);
    public bool IsOptional => this.Occurrences < this.SampleSize;

    /// <summary>True when the sample proves this path holds encrypted values.</summary>
    public bool IsEncrypted => this.Encryption is not null;

    /// <summary>True when this path looks like a soft-delete flag - a suggestion, never a statement.</summary>
    public bool IsSoftDeleteCandidate => this.SoftDeleteCandidate is not null;
}

/// <summary>
/// What a sample of documents proves about one encrypted path, without any key.
/// </summary>
/// <param name="KeyIds">Key ids seen in the sample, in first-seen order.</param>
/// <param name="EncryptedCount">Sampled values on this path that were envelopes.</param>
/// <param name="PlaintextCount">Sampled values on this path that were <b>not</b> envelopes.</param>
/// <param name="DeterministicObserved">
/// True when a ciphertext repeated across the sample. Randomized mode draws a fresh nonce per write and so
/// can never repeat one, which makes a repeat proof of deterministic mode. The converse is <b>not</b> proof:
/// a deterministic column of unique values is indistinguishable from a randomized one, which is why there is
/// no "randomized" flag here and nothing in the UI may claim it.
/// </param>
public sealed record EncryptedFieldInfo(
    IReadOnlyList<string> KeyIds,
    int EncryptedCount,
    int PlaintextCount,
    bool DeterministicObserved)
{
    /// <summary>How the mode reads to an operator — honest about what the data can and cannot prove.</summary>
    public string ModeSummary => this.DeterministicObserved ? "deterministic (observed)" : "mode unknown";
}

public sealed record InferredSchema(string TypeName, int SampleSize, IReadOnlyList<InferredField> Fields);

/// <summary>
/// A JSON property index created by DocumentDb (named <c>idx_json_{type}_{path}</c>). A composite index
/// covers several paths, recovered from the name.
/// </summary>
public sealed record JsonIndexInfo(string Name, string TypeName, IReadOnlyList<string> Paths)
{
    /// <summary>The paths as one displayable string.</summary>
    public string Path => string.Join(", ", this.Paths);

    public bool IsComposite => this.Paths.Count > 1;
}

/// <summary>
/// Any index on a documents table, whether DocumentDb built it or someone else did.
/// </summary>
/// <param name="Definition">The creating DDL or column list, where the catalog keeps one.</param>
/// <param name="SizeBytes">On-disk size, where the engine tracks it (PostgreSQL, SQL Server, Oracle).</param>
/// <param name="Scans">
/// How often the planner has used it, where the engine counts (PostgreSQL, SQL Server). SQL Server's
/// counter resets with the instance, so zero means "not since startup" rather than "never".
/// </param>
/// <param name="Json">The DocumentDb interpretation when the name matches its convention, else null.</param>
public sealed record IndexInfo(
    string Name,
    string? Definition,
    long? SizeBytes,
    long? Scans,
    JsonIndexInfo? Json
)
{
    /// <summary>True when DocumentDb created this index, so the tool may offer to drop it.</summary>
    public bool IsDocumentDb => this.Json is not null;
}

/// <summary>
/// Records the generator would write, and the seed that produced them. The seed is what makes the
/// preview a promise rather than an illustration: committing replays it and writes these documents.
/// </summary>
public sealed record GenerationPreview(
    DocumentShape Shape,
    IReadOnlyList<System.Text.Json.Nodes.JsonObject> Sample,
    int Count,
    int Seed
);

/// <summary>
/// What a commit did, and which indexes it could not maintain.
/// </summary>
/// <param name="StaleSidecars">
/// Sidecars the generated rows are absent from, because bulk generation goes through the
/// schema-free lane. Empty for a type with none.
/// </param>
public sealed record GenerationResult(int Written, IReadOnlyList<string> StaleSidecars);

/// <summary>A query plan as the provider's EXPLAIN rendered it, one line per row.</summary>
/// <param name="Supported">False when the provider offers no plan form the tool can run.</param>
public sealed record QueryPlan(IReadOnlyList<string> Lines, TimeSpan Elapsed, bool Supported)
{
    public bool IsEmpty => this.Lines.Count == 0;

    /// <summary>
    /// Whether the plan mentions a full scan. Deliberately a text match: every engine words it
    /// differently and none exposes it as a flag, so this is a hint that prompts a closer look rather
    /// than a verdict — the plan itself is always shown.
    /// </summary>
    public bool LooksLikeScan => this.Lines.Any(l =>
        l.Contains("SCAN", StringComparison.OrdinalIgnoreCase) ||
        l.Contains("Seq Scan", StringComparison.OrdinalIgnoreCase) ||
        l.Contains("FULL", StringComparison.OrdinalIgnoreCase));
}

/// <summary>Result of an ad-hoc SQL statement in the query console.</summary>
public sealed record SqlResult(
    IReadOnlyList<string> Columns,
    IReadOnlyList<IReadOnlyList<string?>> Rows,
    int RecordsAffected,
    TimeSpan Elapsed,
    bool Truncated
)
{
    public bool HasGrid => this.Columns.Count > 0;
}

/// <summary>A filter row in the Browse toolbar.</summary>
public sealed record BrowseFilter(string Path, FilterOperator Operator, string Value);

public enum FilterOperator
{
    Equals,
    NotEquals,
    Contains,
    StartsWith,
    EndsWith,
    GreaterThan,
    GreaterOrEqual,
    LessThan,
    LessOrEqual,
    IsNull,
    IsNotNull
}

/// <summary>How the browse grid is sorted. <see cref="Path"/> is either an envelope column or a JSON path.</summary>
public sealed record BrowseSort(string Path, bool Descending, bool IsEnvelopeColumn);

/// <summary>Counts and byte sizes for a documents table.</summary>
public sealed record TableStats(long DocumentCount, long TypeCount, long? TotalJsonBytes, long? LargestJsonBytes);

/// <summary>
/// What the filter console asks for: DocumentDb's string grammar rather than the database's SQL.
/// </summary>
/// <param name="Where">Filter grammar, e.g. <c>status == 'Shipped' and total:number &gt; 100</c>.</param>
/// <param name="OrderBy">Comma-separated sort keys, e.g. <c>total:number desc, name</c>.</param>
/// <param name="Project">
/// Optional database-level projection, e.g. <c>name, lower(email) as email</c>. Empty returns whole
/// documents.
/// </param>
public sealed record FilterQuery(
    string? Where = null,
    string? OrderBy = null,
    string? Project = null,
    int Offset = 0,
    int Take = 50
);

/// <summary>
/// The documents a filter matched, and the SQL it compiled to — which is half the point of the
/// console: the grammar is not a separate query language, it is a shorthand for the same statement.
/// </summary>
public sealed record FilterResult(
    IReadOnlyList<string> Columns,
    IReadOnlyList<JsonObject> Rows,
    string Sql,
    IReadOnlyDictionary<string, object?> Parameters,
    TimeSpan Elapsed,
    long TotalCount,
    bool PageFull
);
