using System.Net;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace ShinyDocDbMyAdmin.Services;

/// <summary>
/// Renders a document body as syntax-highlighted, collapsible HTML.
/// </summary>
/// <remarks>
/// Server-rendered, like <see cref="SvgMap"/>, rather than handed to a client-side highlighter: the
/// tool ships no JavaScript library and the collapsing is done with native <c>&lt;details&gt;</c>
/// elements, so it works with scripting off.
/// <para>
/// <b>Every value here comes out of someone else's database</b> and is interpolated into markup, so
/// each one goes through <see cref="WebUtility.HtmlEncode"/> on the way. That is the whole security
/// story of this file - a document with <c>&lt;script&gt;</c> in a string field must render as text.
/// </para>
/// </remarks>
public static class JsonHtml
{
    /// <summary>Nesting beyond this renders compact rather than expandable, to bound the markup.</summary>
    public const int MaxDepth = 20;

    /// <summary>
    /// Objects and arrays with more than this many children start collapsed, so a large document does
    /// not fill the page before you have decided what you are looking for.
    /// </summary>
    public const int CollapseOver = 40;

    /// <inheritdoc cref="JsonDisplay.VectorSummaryOver"/>
    public const int VectorSummaryOver = JsonDisplay.VectorSummaryOver;

    /// <inheritdoc cref="JsonDisplay.VectorPreviewLength"/>
    public const int VectorPreviewLength = JsonDisplay.VectorPreviewLength;

    /// <summary>
    /// Describes a long numeric array as <c>float[1536] [0.021, -0.114, …]</c>, or returns false when
    /// the node is not one. Lives in <see cref="JsonDisplay"/> so the JSON tree, the browse grid and
    /// the terminal front end all agree on what counts as an embedding and how it reads.
    /// </summary>
    public static bool TryVectorSummary(JsonNode? node, out string summary)
        => JsonDisplay.TryVectorSummary(node, out summary);

    /// <summary>
    /// Renders JSON as highlighted HTML. Text that will not parse comes back as escaped plain text,
    /// because an admin tool that hides a malformed body is worse than one that shows it.
    /// </summary>
    public static string Render(string? json)
    {
        if (string.IsNullOrWhiteSpace(json))
            return "<span class=\"jt-null\">(empty)</span>";

        JsonNode? node;
        try
        {
            node = JsonNode.Parse(json);
        }
        catch (JsonException ex)
        {
            return $"<span class=\"jt-error\">{WebUtility.HtmlEncode(ex.Message)}</span>\n" +
                   $"<span class=\"jt-raw\">{WebUtility.HtmlEncode(json)}</span>";
        }

        var builder = new StringBuilder();
        WriteNode(builder, node, depth: 0, trailing: "");

        return builder.ToString();
    }

    /// <summary>
    /// Renders one node. <paramref name="trailing"/> is the separating comma, written by whoever owns
    /// the node rather than after it - a container's comma belongs inside its
    /// <c>&lt;details&gt;</c>, or it lands on a line of its own once the node collapses.
    /// </summary>
    static void WriteNode(StringBuilder builder, JsonNode? node, int depth, string trailing)
    {
        switch (node)
        {
            case JsonObject obj when depth < MaxDepth:
                WriteContainer(builder, obj.Select(x => (Key: (string?)x.Key, Value: x.Value)), "{", "}", obj.Count, depth, trailing);
                break;

            // Checked before the general array case: an embedding is a container by JSON's rules but
            // not by any useful reading of it. The Raw toggle still shows every component.
            case JsonArray vector when TryVectorSummary(vector, out var summary):
                builder.Append($"<span class=\"jt-vector\" title=\"{WebUtility.HtmlEncode(summary)}\">{WebUtility.HtmlEncode(summary)}</span>");
                builder.Append(Punct(trailing));
                break;

            case JsonArray array when depth < MaxDepth:
                WriteContainer(builder, array.Select(x => ((string?)null, x)), "[", "]", array.Count, depth, trailing);
                break;

            case null:
                builder.Append("<span class=\"jt-null\">null</span>");
                builder.Append(Punct(trailing));
                break;

            // Past the depth limit a container still has to render as something; compact escaped JSON
            // keeps it readable without recursing further.
            case JsonObject or JsonArray:
                builder.Append($"<span class=\"jt-raw\">{WebUtility.HtmlEncode(node.ToJsonString())}</span>");
                builder.Append(Punct(trailing));
                break;

            default:
                WriteValue(builder, node);
                builder.Append(Punct(trailing));
                break;
        }
    }

    static string Punct(string text)
        => text.Length == 0 ? "" : $"<span class=\"jt-punct\">{text}</span>";

    static void WriteContainer(
        StringBuilder builder,
        IEnumerable<(string? Key, JsonNode? Value)> children,
        string open,
        string close,
        int count,
        int depth,
        string trailing)
    {
        if (count == 0)
        {
            builder.Append($"<span class=\"jt-punct\">{open}{close}{trailing}</span>");
            return;
        }

        // <details> gives real collapsing with no script. Large nodes start closed.
        var openAttribute = count > CollapseOver ? "" : " open";
        builder.Append($"<details class=\"jt-node\"{openAttribute}><summary><span class=\"jt-punct\">{open}</span>");
        builder.Append($"<span class=\"jt-count\">{count} {(count == 1 ? "entry" : "entries")}</span>");
        builder.Append($"<span class=\"jt-punct jt-closed\">{close}{trailing}</span></summary><div class=\"jt-children\">");

        var index = 0;
        foreach (var (key, value) in children)
        {
            builder.Append("<div class=\"jt-row\">");
            if (key is not null)
                builder.Append($"<span class=\"jt-key\">{WebUtility.HtmlEncode(key)}</span><span class=\"jt-punct\">: </span>");

            WriteNode(builder, value, depth + 1, ++index < count ? "," : "");
            builder.Append("</div>");
        }

        builder.Append($"</div><span class=\"jt-punct\">{close}{trailing}</span></details>");
    }

    static void WriteValue(StringBuilder builder, JsonNode node)
    {
        var value = node.AsValue();
        switch (value.GetValueKind())
        {
            case JsonValueKind.String:
                var text = value.GetValue<string>();
                builder.Append($"<span class=\"jt-string\">\"{WebUtility.HtmlEncode(text)}\"</span>");
                break;

            case JsonValueKind.Number:
                builder.Append($"<span class=\"jt-number\">{WebUtility.HtmlEncode(value.ToJsonString())}</span>");
                break;

            case JsonValueKind.True or JsonValueKind.False:
                builder.Append($"<span class=\"jt-bool\">{WebUtility.HtmlEncode(value.ToJsonString())}</span>");
                break;

            case JsonValueKind.Null:
                builder.Append("<span class=\"jt-null\">null</span>");
                break;

            default:
                builder.Append($"<span class=\"jt-raw\">{WebUtility.HtmlEncode(value.ToJsonString())}</span>");
                break;
        }
    }
}
