namespace Shiny.DocumentDb.Internal;

/// <summary>
/// Registration record produced by <c>MapBlob&lt;T&gt;</c> / <c>MapBlobCollection&lt;T&gt;</c>. Tells the store
/// which members carry <see cref="DocumentBlob"/> payloads so they can be split out to the sidecar table on
/// write and re-attached on demand.
/// </summary>
/// <remarks>
/// Public so provider assemblies can read the mapping; there is no behavior here for consumers to override.
/// </remarks>
public sealed class BlobMapping
{
    public required Type DocumentType { get; init; }

    /// <summary>The CLR property holding the blob (or the blob collection).</summary>
    public required string PropertyName { get; init; }

    /// <summary>True when the property is a list of blobs rather than a single one.</summary>
    public required bool IsCollection { get; init; }

    /// <summary>
    /// The sidecar key for a single-blob property. Collection items carry their own keys, so this is unset
    /// when <see cref="IsCollection"/> is true.
    /// </summary>
    public string? Key { get; init; }

    /// <summary>When true, a SHA-256 of each payload is computed on write and exposed as <see cref="DocumentBlob.Hash"/>.</summary>
    public bool ComputeHash { get; init; }

    /// <summary>Per-mapping size ceiling in bytes; 0 defers entirely to the provider's <c>MaxBlobSize</c>.</summary>
    public long MaxSize { get; init; }

    /// <summary>Reads the <see cref="DocumentBlob"/> (or blob list) off a live document.</summary>
    public required Func<object, object?> GetValue { get; init; }

    /// <summary>Writes the <see cref="DocumentBlob"/> (or blob list) back onto a live document.</summary>
    public required Action<object, object?> SetValue { get; init; }
}
