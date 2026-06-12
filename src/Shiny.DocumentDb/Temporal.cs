namespace Shiny.DocumentDb;

/// <summary>
/// The kind of mutation that produced a temporal history version.
/// </summary>
public enum TemporalOperation
{
    /// <summary>The document was first created.</summary>
    Inserted,

    /// <summary>An existing document was modified (Update, Upsert, SetProperty, RemoveProperty).</summary>
    Updated,

    /// <summary>The document was deleted. The version carries no body (<see cref="DocumentVersion{T}.Document"/> is null).</summary>
    Removed
}

/// <summary>
/// Configures system-time temporal history for a document type via
/// <see cref="DocumentStoreOptions.MapTemporal{T}"/>. History is opt-in per type and append-only:
/// every Insert/Update/Upsert/Remove writes a versioned snapshot to a sidecar history table so the
/// state of a document can be read back as of any point in time.
/// </summary>
public class TemporalOptions
{
    /// <summary>
    /// When set, expired (closed) versions whose validity ended more than this far in the past are
    /// pruned on each write. The current version is never pruned. Null keeps history forever.
    /// </summary>
    public TimeSpan? Retention { get; set; }

    /// <summary>
    /// When set, only the newest N versions per document are retained; older versions are pruned on
    /// each write. Null keeps every version.
    /// </summary>
    public int? MaxVersions { get; set; }

    /// <summary>
    /// Optional accessor invoked on each write to capture "who" made the change. The returned value
    /// is stored on the history row and surfaced via <see cref="DocumentVersion{T}.Actor"/>.
    /// </summary>
    public Func<string?>? CaptureActor { get; set; }
}

/// <summary>
/// A single point-in-time version of a document, as recorded in the temporal history.
/// </summary>
/// <typeparam name="T">The document type.</typeparam>
public sealed class DocumentVersion<T> where T : class
{
    /// <summary>The string Id of the document.</summary>
    public required string Id { get; init; }

    /// <summary>Monotonically increasing version number, starting at 1.</summary>
    public required long Version { get; init; }

    /// <summary>When this version became the current state of the document.</summary>
    public required DateTimeOffset ValidFrom { get; init; }

    /// <summary>
    /// When this version stopped being current (superseded by the next version or removed).
    /// Null for the version that is currently in effect.
    /// </summary>
    public DateTimeOffset? ValidTo { get; init; }

    /// <summary>The mutation that produced this version.</summary>
    public required TemporalOperation Operation { get; init; }

    /// <summary>The captured actor, when a <see cref="TemporalOptions.CaptureActor"/> was configured.</summary>
    public string? Actor { get; init; }

    /// <summary>
    /// The document state at this version. Null for <see cref="TemporalOperation.Removed"/> versions
    /// (a tombstone marking when the document was deleted).
    /// </summary>
    public T? Document { get; init; }
}
