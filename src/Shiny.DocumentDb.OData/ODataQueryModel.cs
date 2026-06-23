namespace Shiny.DocumentDb.OData;

/// <summary>A single <c>$orderby</c> term: a property path plus direction.</summary>
public sealed record ODataOrderByItem(string PropertyPath, bool Descending);

/// <summary>
/// Provider-neutral, parsed representation of the OData system query options the engine supports.
/// The engine never references <c>Microsoft.OData</c> types — the ASP.NET host converts the Microsoft
/// parse result into this model, and the engine's own lightweight string parser produces the same model.
/// This seam keeps the engine dependency-free and AOT-clean.
/// </summary>
public sealed class ODataQueryModel
{
    /// <summary>The parsed <c>$filter</c> node tree, or <c>null</c> when no filter was supplied.</summary>
    public ODataFilterNode? Filter { get; set; }

    /// <summary>The ordered list of <c>$orderby</c> terms.</summary>
    public IReadOnlyList<ODataOrderByItem> OrderBy { get; set; } = Array.Empty<ODataOrderByItem>();

    /// <summary><c>$top</c> — maximum rows to return.</summary>
    public int? Top { get; set; }

    /// <summary><c>$skip</c> — rows to skip before the page.</summary>
    public int? Skip { get; set; }

    /// <summary><c>$select</c> — comma-separated sparse fieldset, or <c>null</c> for full entities.</summary>
    public string? Select { get; set; }

    /// <summary><c>$count=true</c> — emit the pre-paging total on the envelope.</summary>
    public bool Count { get; set; }

    /// <summary>
    /// Set when a request used <c>$expand</c>. There is no document-relationship analog, so the
    /// engine/host surfaces this as <c>501 Not Implemented</c> rather than silently ignoring it.
    /// </summary>
    public bool HasExpand { get; set; }
}
