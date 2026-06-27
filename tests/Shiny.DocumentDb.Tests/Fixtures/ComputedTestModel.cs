using System.Text.Json.Serialization;

namespace Shiny.DocumentDb.Tests.Fixtures;

/// <summary>
/// Shared model for the cross-provider computed-property suite. <see cref="LineTotalCents"/> exercises
/// numeric arithmetic (materialized via <c>indexed: true</c> on the relational providers); <see cref="FullName"/>
/// exercises string concatenation in alias mode. Both are <c>[JsonIgnore]</c>'d so they never persist.
/// </summary>
public sealed class ComputedSale
{
    public string Id { get; set; } = "";
    public string First { get; set; } = "";
    public string Last { get; set; } = "";
    public int Quantity { get; set; }
    public int UnitPriceCents { get; set; }

    [JsonIgnore] public string FullName { get; set; } = "";
    [JsonIgnore] public int LineTotalCents { get; set; }
}
