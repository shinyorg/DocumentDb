using System.Text.Json.Nodes;

namespace Shiny.DocumentDb.OData;

/// <summary>
/// The materialized result of executing an <see cref="ODataQueryModel"/>. The host package serializes
/// this into the OData JSON envelope (<c>@odata.context</c>, <c>@odata.count</c>, <c>value</c>).
/// </summary>
public sealed class ODataResult
{
    /// <summary>
    /// The page of results as <see cref="JsonNode"/> items. Whole-entity queries serialize each
    /// document through its <c>JsonTypeInfo</c>; <c>$select</c> queries carry the projected
    /// <see cref="JsonObject"/> shapes directly.
    /// </summary>
    public required IReadOnlyList<JsonNode> Value { get; init; }

    /// <summary>The pre-paging total when <c>$count=true</c> was requested; otherwise <c>null</c>.</summary>
    public long? Count { get; init; }
}
