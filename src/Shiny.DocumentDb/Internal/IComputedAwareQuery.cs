namespace Shiny.DocumentDb.Internal;

/// <summary>
/// Implemented by query builders that know their type's computed-property mappings, so the string-based
/// <c>Where</c>/<c>OrderBy</c>/<c>Project</c> extension helpers (and the OData layer) can resolve a computed
/// property by name even though it is <c>[JsonIgnore]</c>'d and therefore absent from the JSON metadata.
/// </summary>
internal interface IComputedAwareQuery
{
    IReadOnlyDictionary<string, ComputedMapping>? ComputedLookup { get; }
}
