namespace Shiny.DocumentDb.IndexedDb;

/// <summary>
/// A temporal-history version row stored in the <c>{store}_history</c> object store. Shares the
/// <c>key</c> keyPath and <c>typeName</c> index of the main stores so the existing JS interop
/// (put / getAllByTypeName / remove / batchDelete) can manage it without schema changes.
/// </summary>
internal class HistoryRecord
{
    /// <summary>Object-store key, unique per version: <c>"{typeName}:{id}:{version}"</c>.</summary>
    public string Key { get; set; } = default!;
    public string Id { get; set; } = default!;
    public string TypeName { get; set; } = default!;
    public long Version { get; set; }
    public string ValidFrom { get; set; } = default!;
    public string? ValidTo { get; set; }
    public string Operation { get; set; } = default!;
    public string? Actor { get; set; }

    /// <summary>The post-image JSON, or null for a <see cref="TemporalOperation.Removed"/> tombstone.</summary>
    public string? Data { get; set; }
}
