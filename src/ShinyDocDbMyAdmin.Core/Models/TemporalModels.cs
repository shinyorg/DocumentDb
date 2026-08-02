namespace ShinyDocDbMyAdmin.Models;

/// <summary>
/// One row of a <c>{table}_history</c> sidecar - the state of a document over the interval it was
/// current. The shape mirrors the library's <c>DocumentVersion&lt;T&gt;</c>, minus the CLR type this
/// tool has no way to bind to.
/// </summary>
/// <param name="DocumentId">The document this version belongs to.</param>
/// <param name="Version">Monotonic, starting at 1.</param>
/// <param name="ValidFrom">When this version became current.</param>
/// <param name="ValidTo">When it stopped being current. Null for the version in effect now.</param>
/// <param name="Operation">Inserted, Updated or Removed, as the library recorded it.</param>
/// <param name="Actor">Whoever the store's CaptureActor returned, when one was configured.</param>
/// <param name="Json">The state at this version. Null for a Removed tombstone.</param>
public sealed record DocumentVersion(
    string DocumentId,
    long Version,
    DateTimeOffset? ValidFrom,
    DateTimeOffset? ValidTo,
    string Operation,
    string? Actor,
    string? Json
)
{
    /// <summary>True for a delete tombstone, which has no body to show or restore.</summary>
    public bool IsTombstone => this.Json is null;

    /// <summary>True for the version currently in effect - the one Browse would show.</summary>
    public bool IsCurrent => this.ValidTo is null && !this.IsTombstone;

    /// <summary>How long this version stood, for the versions table.</summary>
    public TimeSpan? Duration => this.ValidFrom is { } from && this.ValidTo is { } to ? to - from : null;
}

/// <summary>What happened to one JSON path between two versions.</summary>
public enum JsonChangeKind
{
    Added,
    Removed,
    Modified
}

/// <summary>
/// A single difference between two documents, keyed by a dotted path. Deliberately a flat list
/// rather than an RFC 6902 patch: a person comparing two versions wants to read what changed, and
/// the library's patch form is generic over a document type this tool does not have.
/// </summary>
/// <param name="Path">Dotted path, with array indices in brackets - <c>items[2].price</c>.</param>
/// <param name="Kind">Added, Removed or Modified.</param>
/// <param name="Before">Rendered value in the older version. Null when <see cref="Kind"/> is Added.</param>
/// <param name="After">Rendered value in the newer version. Null when <see cref="Kind"/> is Removed.</param>
public sealed record JsonChange(string Path, JsonChangeKind Kind, string? Before, string? After);

/// <summary>The full comparison of two versions of a document.</summary>
public sealed record VersionComparison(
    DocumentVersion Left,
    DocumentVersion Right,
    IReadOnlyList<JsonChange> Changes
)
{
    public bool IsIdentical => this.Changes.Count == 0;

    public int Added => this.Changes.Count(x => x.Kind == JsonChangeKind.Added);
    public int Removed => this.Changes.Count(x => x.Kind == JsonChangeKind.Removed);
    public int Modified => this.Changes.Count(x => x.Kind == JsonChangeKind.Modified);
}
