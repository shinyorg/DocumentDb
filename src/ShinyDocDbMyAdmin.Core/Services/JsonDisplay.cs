using System.Text.Json;
using System.Text.Json.Nodes;

namespace ShinyDocDbMyAdmin.Services;

/// <summary>
/// How a JSON value reads when it is shown in a cell, whatever is doing the showing.
/// </summary>
/// <remarks>
/// Presentation-neutral on purpose: the web front end wraps these in HTML spans and the terminal front
/// end writes them into grid cells, and the two have to agree on what a value <i>says</i> even though
/// they disagree about everything to do with how it looks. Anything markup-specific stays in the front
/// end - this decides only the text.
/// </remarks>
public static class JsonDisplay
{
    /// <summary>
    /// Numeric arrays longer than this read as a summary rather than element by element. They are
    /// embeddings, and there is nothing to read in the 1536th component of one - while writing it out
    /// costs tens of kilobytes per document, per row.
    /// </summary>
    public const int VectorSummaryOver = 24;

    /// <summary>How many components a summarised vector shows.</summary>
    public const int VectorPreviewLength = 6;

    /// <summary>
    /// Describes a long numeric array as <c>float[1536] [0.021, -0.114, …]</c>, or returns false when
    /// the node is not one.
    /// </summary>
    public static bool TryVectorSummary(JsonNode? node, out string summary)
    {
        summary = "";
        if (node is not JsonArray array || array.Count <= VectorSummaryOver)
            return false;

        if (!array.All(x => x is JsonValue v && v.GetValueKind() == JsonValueKind.Number))
            return false;

        var preview = string.Join(", ", array.Take(VectorPreviewLength).Select(x => x!.ToJsonString()));
        summary = $"float[{array.Count}] [{preview}, …]";
        return true;
    }

    /// <summary>
    /// One cell's worth of a value: strings unquoted (a cell is not a JSON document), embeddings
    /// summarised, everything else as its JSON form.
    /// </summary>
    public static string Cell(JsonNode? node)
    {
        if (node is null)
            return "null";

        if (TryVectorSummary(node, out var summary))
            return summary;

        return node is JsonValue value && value.GetValueKind() == JsonValueKind.String
            ? value.GetValue<string>()
            : node.ToJsonString();
    }
}
