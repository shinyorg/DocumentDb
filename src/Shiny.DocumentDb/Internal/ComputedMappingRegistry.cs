using System.Text.Json;

namespace Shiny.DocumentDb.Internal;

/// <summary>
/// Shared storage + resolution for computed-property mappings, reused by every options class (the core
/// relational <see cref="DocumentStoreOptions"/> and each document-provider options) so the registration
/// and lookup rules stay identical across providers.
/// </summary>
public sealed class ComputedMappingRegistry
{
    readonly Dictionary<Type, List<ComputedMapping>> mappings = new();

    public void Add(ComputedMapping mapping)
    {
        if (!this.mappings.TryGetValue(mapping.DocumentType, out var list))
            this.mappings[mapping.DocumentType] = list = new List<ComputedMapping>();

        list.RemoveAll(m => m.PropertyName.Equals(mapping.PropertyName, StringComparison.Ordinal));
        list.Add(mapping);
    }

    public IReadOnlyList<ComputedMapping> Resolve(Type type)
        => this.mappings.TryGetValue(type, out var list) ? list : Array.Empty<ComputedMapping>();

    /// <summary>A name → mapping lookup (CLR property name, case-insensitive) for the query layer, or null if none.</summary>
    public IReadOnlyDictionary<string, ComputedMapping>? ResolveLookup(Type type)
    {
        if (!this.mappings.TryGetValue(type, out var list) || list.Count == 0)
            return null;

        var lookup = new Dictionary<string, ComputedMapping>(StringComparer.OrdinalIgnoreCase);
        foreach (var mapping in list)
            lookup[mapping.PropertyName] = mapping;
        return lookup;
    }

    public void ResolveJsonNames(JsonSerializerOptions jsonOptions)
    {
        foreach (var list in this.mappings.Values)
            foreach (var mapping in list)
                if (mapping.JsonName == null!)
                    mapping.JsonName = JsonPropertyNameResolver.ResolveJsonName(jsonOptions, mapping.DocumentType, mapping.PropertyName);
    }

    /// <summary>Marks which materialize-requested mappings get a native column on a relational provider.</summary>
    public void ResolveColumns(IDatabaseProvider provider)
    {
        var supported = provider.SupportsComputedColumns;
        foreach (var list in this.mappings.Values)
            foreach (var mapping in list)
                mapping.MaterializedColumn = supported && mapping.Materialize ? mapping.ColumnName : null;
    }

    public IEnumerable<ComputedMapping> All()
    {
        foreach (var list in this.mappings.Values)
            foreach (var mapping in list)
                yield return mapping;
    }
}
