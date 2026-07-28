using ShinyDocDbMyAdmin.Services;

namespace ShinyDocDbMyAdmin.Tests;

/// <summary>
/// Picking the embedding a document write should push into the vector sidecar. The tool has no CLR
/// type and therefore no registered mapping, so the shape of the body is the only evidence - and the
/// rule is deliberately conservative, because writing the <i>wrong</i> array into a search index is
/// worse than writing none and saying so.
/// </summary>
public class SoleVectorTests
{
    [Fact]
    public void SingleEmbedding_IsFound()
    {
        var json = """{"id":"1","title":"hello","embedding":[1,2,3,4,5,6,7,8]}""";

        var found = DocumentAdminService.SoleVector(json);

        Assert.NotNull(found);
        Assert.Equal(8, found.Length);
    }

    [Fact]
    public void NestedEmbedding_IsFound()
    {
        var json = """{"id":"1","content":{"vector":[1,2,3,4,5,6,7,8]}}""";

        Assert.NotNull(DocumentAdminService.SoleVector(json));
    }

    [Fact]
    public void TwoEmbeddings_AreTreatedAsNone()
    {
        // A type has one vector mapping and one sidecar column; guessing which of two arrays it is
        // would silently index the wrong one.
        var json = """{"a":[1,2,3,4,5,6,7,8],"b":[8,7,6,5,4,3,2,1]}""";

        Assert.Null(DocumentAdminService.SoleVector(json));
    }

    [Fact]
    public void NoEmbedding_IsNone()
    {
        Assert.Null(DocumentAdminService.SoleVector("""{"id":"1","title":"hello"}"""));
        Assert.Null(DocumentAdminService.SoleVector("""{"id":"1","tags":["a","b","c"]}"""));
    }

    [Fact]
    public void ShortNumericArrays_AreNotMistakenForEmbeddings()
    {
        var json = """{"id":"1","location":[51.5,-0.12],"rgb":[255,128,0]}""";

        Assert.Null(DocumentAdminService.SoleVector(json));
    }

    [Fact]
    public void MalformedOrNonObjectBody_IsNone()
    {
        Assert.Null(DocumentAdminService.SoleVector("not json"));
        Assert.Null(DocumentAdminService.SoleVector("[1,2,3,4,5,6,7,8]"));
    }
}

/// <summary>
/// Embeddings in the read-only views. A 1536-component array rendered element by element is tens of
/// kilobytes of markup per row, so both views collapse it - and have to agree on when.
/// </summary>
public class VectorSummaryTests
{
    static System.Text.Json.Nodes.JsonNode Node(string json) => System.Text.Json.Nodes.JsonNode.Parse(json)!;

    [Fact]
    public void LongNumericArray_IsSummarised()
    {
        var array = Node("[" + string.Join(",", Enumerable.Range(0, 1536).Select(i => i / 1000.0)) + "]");

        Assert.True(JsonHtml.TryVectorSummary(array, out var summary));
        Assert.StartsWith("float[1536]", summary);
        Assert.EndsWith("…]", summary);
    }

    [Fact]
    public void ShortArraysAndNonNumericArrays_AreLeftAlone()
    {
        Assert.False(JsonHtml.TryVectorSummary(Node("[1,2,3]"), out _));
        Assert.False(JsonHtml.TryVectorSummary(Node("""["a","b"]"""), out _));
        Assert.False(JsonHtml.TryVectorSummary(Node("""{"a":1}"""), out _));
        Assert.False(JsonHtml.TryVectorSummary(null, out _));
    }

    [Fact]
    public void ArrayOfObjects_IsNotSummarised_EvenWhenLong()
    {
        var array = Node("[" + string.Join(",", Enumerable.Range(0, 100).Select(i => $$"""{"i":{{i}}}""")) + "]");

        Assert.False(JsonHtml.TryVectorSummary(array, out _));
    }

    [Fact]
    public void RenderedTree_CollapsesAnEmbedding_RatherThanEmittingEveryComponent()
    {
        var components = string.Join(",", Enumerable.Range(0, 1536).Select(i => i / 1000.0));
        var html = JsonHtml.Render($$"""{"title":"hello","embedding":[{{components}}]}""");

        Assert.Contains("float[1536]", html);
        Assert.Contains("hello", html);

        // The whole point: the markup is a summary, not 1536 spans.
        Assert.True(html.Length < 2000, $"markup was {html.Length} characters");
    }

    [Fact]
    public void RenderedTree_StillShowsShortArraysInFull()
    {
        var html = JsonHtml.Render("""{"rgb":[255,128,0]}""");

        Assert.Contains("255", html);
        Assert.Contains("128", html);
        Assert.DoesNotContain("float[", html);
    }
}
