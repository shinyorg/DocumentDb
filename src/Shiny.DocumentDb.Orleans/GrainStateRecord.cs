using System.Text.Json;

namespace Shiny.DocumentDb.Orleans;

/// <summary>
/// The single concrete envelope persisted for every grain's state. Deliberately non-generic so that
/// exactly one type/table mapping (and one <c>MapVersionProperty</c>) is registered at startup — the
/// alternative, a generic <c>GrainStateRecord&lt;T&gt;</c>, would require a separate mapping per closed
/// grain-state type, which the <see cref="DocumentStoreOptions"/> mapping registry cannot accept lazily.
/// <para>
/// The grain's actual state is stored as a nested <see cref="JsonElement"/> in <see cref="State"/>, so it
/// remains queryable (e.g. <c>json_extract(Data, '$.state.someField')</c>) rather than an opaque blob.
/// </para>
/// </summary>
public sealed class GrainStateRecord
{
    /// <summary>Composite document key: <c>"{stateName}|{grainId}"</c>.</summary>
    public string Id { get; set; } = null!;

    /// <summary>
    /// Monotonic version backing the Orleans ETag. Mapped via <c>MapVersionProperty</c> so that
    /// <see cref="IDocumentStore.Update{T}"/> performs an atomic compare-and-swap and raises
    /// <see cref="ConcurrencyException"/> on a mismatch.
    /// </summary>
    public int Version { get; set; }

    /// <summary>The grain's serialized state.</summary>
    public JsonElement State { get; set; }
}
