using ShinyDocDbMyAdmin.Services;

namespace ShinyDocDbMyAdmin.Tests;

/// <summary>
/// The formatted JSON view. The escaping tests are the important ones: this markup is built from
/// document content the tool did not write, and is rendered as raw HTML.
/// </summary>
public class JsonHtmlTests
{
    [Fact]
    public void ScriptInAStringValue_IsEscaped()
    {
        var html = JsonHtml.Render("""{"bio":"<script>alert(1)</script>"}""");

        Assert.DoesNotContain("<script>", html);
        Assert.Contains("&lt;script&gt;", html);
    }

    [Fact]
    public void ScriptInAKey_IsEscaped()
    {
        var html = JsonHtml.Render("""{"<img src=x onerror=alert(1)>":1}""");

        Assert.DoesNotContain("<img", html);
        Assert.Contains("&lt;img", html);
    }

    [Fact]
    public void QuotesAndAmpersands_AreEscaped()
    {
        var html = JsonHtml.Render("""{"v":"a & b \" c"}""");

        Assert.Contains("&amp;", html);
        Assert.Contains("&quot;", html);
    }

    [Fact]
    public void MalformedJson_IsShownAsEscapedText_NotHidden()
    {
        var html = JsonHtml.Render("{ this is not json <b>x</b>");

        Assert.Contains("jt-error", html);
        Assert.Contains("&lt;b&gt;", html);
        Assert.DoesNotContain("<b>", html);
    }

    [Fact]
    public void EmptyInput_SaysSo()
    {
        Assert.Contains("(empty)", JsonHtml.Render(""));
        Assert.Contains("(empty)", JsonHtml.Render(null));
    }

    [Fact]
    public void ValueKinds_GetTheirOwnClasses()
    {
        var html = JsonHtml.Render("""{"s":"x","n":1,"b":true,"z":null}""");

        Assert.Contains("jt-string", html);
        Assert.Contains("jt-number", html);
        Assert.Contains("jt-bool", html);
        Assert.Contains("jt-null", html);
        Assert.Contains("jt-key", html);
    }

    [Fact]
    public void ObjectsAndArrays_AreCollapsible()
    {
        var html = JsonHtml.Render("""{"items":[1,2,3]}""");

        // <details> is what makes the tree collapse without any script on the page.
        Assert.Contains("<details", html);
        Assert.Contains("<summary>", html);
    }

    [Fact]
    public void SmallNodes_StartOpen()
    {
        var html = JsonHtml.Render("""{"a":1}""");

        Assert.Contains("<details class=\"jt-node\" open>", html);
    }

    [Fact]
    public void LargeNodes_StartCollapsed()
    {
        var entries = string.Join(",", Enumerable.Range(0, JsonHtml.CollapseOver + 5).Select(i => $"\"k{i}\":{i}"));
        var html = JsonHtml.Render($"{{{entries}}}");

        Assert.Contains("<details class=\"jt-node\">", html);
    }

    [Fact]
    public void EmptyObjectAndArray_RenderInline()
    {
        Assert.Contains("{}", JsonHtml.Render("{}"));
        Assert.Contains("[]", JsonHtml.Render("[]"));
    }

    [Fact]
    public void EntryCountIsShown_SoACollapsedNodeStillTellsYouSomething()
    {
        var html = JsonHtml.Render("""{"a":1,"b":2}""");

        Assert.Contains("2 entries", html);
    }

    [Fact]
    public void SingleEntry_IsNotPluralised()
        => Assert.Contains("1 entry", JsonHtml.Render("""{"a":1}"""));

    [Fact]
    public void VeryDeepNesting_DoesNotRecurseWithoutBound()
    {
        var json = "1";
        for (var i = 0; i < JsonHtml.MaxDepth + 10; i++)
            json = $$"""{"n":{{json}}}""";

        // Bounded rather than a stack overflow; past the limit the node renders as a plain value.
        var html = JsonHtml.Render(json);

        Assert.NotEmpty(html);
    }

    [Fact]
    public void TopLevelArray_IsSupported()
    {
        var html = JsonHtml.Render("""[{"a":1},{"a":2}]""");

        Assert.Contains("<details", html);
        Assert.Contains("jt-number", html);
    }
}
