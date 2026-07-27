using System.Text.Json.Nodes;

namespace ShinyDocDbMyAdmin.Models;

/// <summary>A table in the target database, classified by what ShinyDocDbMyAdmin can do with it.</summary>
public sealed record TableInfo(string Name, TableRole Role, bool HasTenantColumn = false)
{
    public bool IsBrowsable => this.Role == TableRole.Documents;
}

public enum TableRole
{
    /// <summary>Carries the Id / TypeName / Data envelope - browsable.</summary>
    Documents,

    /// <summary>A <c>{table}_history</c> temporal sidecar.</summary>
    History,

    /// <summary>A <c>{table}_blobs</c> binary sidecar.</summary>
    Blobs,

    /// <summary>A spatial or vector sidecar.</summary>
    Sidecar,

    /// <summary>Not a DocumentDb table at all.</summary>
    Foreign
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
public sealed record InferredField(string Path, string Types, int Occurrences, int SampleSize, string? Example)
{
    public int PercentPresent => this.SampleSize == 0 ? 0 : (int)Math.Round(this.Occurrences * 100.0 / this.SampleSize);
    public bool IsOptional => this.Occurrences < this.SampleSize;
}

public sealed record InferredSchema(string TypeName, int SampleSize, IReadOnlyList<InferredField> Fields);

/// <summary>A JSON property index created by DocumentDb (named <c>idx_json_{type}_{path}</c>).</summary>
public sealed record JsonIndexInfo(string Name, string TypeName, string Path);

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
