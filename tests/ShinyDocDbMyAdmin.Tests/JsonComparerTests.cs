using ShinyDocDbMyAdmin.Models;
using ShinyDocDbMyAdmin.Services;

namespace ShinyDocDbMyAdmin.Tests;

/// <summary>What the History tab reports when you compare two versions of a document.</summary>
public class JsonComparerTests
{
    static JsonChange Single(string? before, string? after)
        => Assert.Single(JsonComparer.Compare(before, after));

    [Fact]
    public void IdenticalDocuments_HaveNoChanges()
        => Assert.Empty(JsonComparer.Compare("""{"a":1,"b":"x"}""", """{"b":"x","a":1}"""));

    [Fact]
    public void ChangedScalar_IsModified()
    {
        var change = Single("""{"total":10}""", """{"total":12}""");

        Assert.Equal("total", change.Path);
        Assert.Equal(JsonChangeKind.Modified, change.Kind);
        Assert.Equal("10", change.Before);
        Assert.Equal("12", change.After);
    }

    [Fact]
    public void NewKey_IsAdded()
    {
        var change = Single("""{"a":1}""", """{"a":1,"note":"hi"}""");

        Assert.Equal("note", change.Path);
        Assert.Equal(JsonChangeKind.Added, change.Kind);
        Assert.Null(change.Before);
        Assert.Equal("hi", change.After);
    }

    [Fact]
    public void DroppedKey_IsRemoved()
    {
        var change = Single("""{"a":1,"note":"hi"}""", """{"a":1}""");

        Assert.Equal(JsonChangeKind.Removed, change.Kind);
        Assert.Equal("hi", change.Before);
        Assert.Null(change.After);
    }

    [Fact]
    public void NestedPaths_AreDotted()
    {
        var change = Single("""{"customer":{"tier":1}}""", """{"customer":{"tier":2}}""");

        Assert.Equal("customer.tier", change.Path);
    }

    [Fact]
    public void ArrayElements_AreIndexed()
    {
        var change = Single("""{"items":[{"qty":1}]}""", """{"items":[{"qty":3}]}""");

        Assert.Equal("items[0].qty", change.Path);
    }

    [Fact]
    public void AppendedArrayElement_IsAdded()
    {
        var change = Single("""{"tags":["a"]}""", """{"tags":["a","b"]}""");

        Assert.Equal("tags[1]", change.Path);
        Assert.Equal(JsonChangeKind.Added, change.Kind);
        Assert.Equal("b", change.After);
    }

    [Fact]
    public void ShapeChange_IsOneChange_NotADescent()
    {
        // object -> scalar: descending into a structure that no longer lines up would produce noise.
        var change = Single("""{"customer":{"name":"Ada"}}""", """{"customer":"Ada"}""");

        Assert.Equal("customer", change.Path);
        Assert.Equal(JsonChangeKind.Modified, change.Kind);
    }

    [Fact]
    public void AddedSubtree_IsReportedWhole_NotFieldByField()
    {
        var change = Single("""{"a":1}""", """{"a":1,"customer":{"name":"Ada","tier":2}}""");

        Assert.Equal("customer", change.Path);
        Assert.Equal(JsonChangeKind.Added, change.Kind);
        Assert.Contains("Ada", change.After);
    }

    [Fact]
    public void NullBefore_ReadsAsEverythingAdded()
    {
        // The "before" side of a restore-from-tombstone comparison.
        var change = Single(null, """{"a":1}""");

        Assert.Equal("(document)", change.Path);
        Assert.Equal(JsonChangeKind.Added, change.Kind);
    }

    [Fact]
    public void NullAfter_ReadsAsEverythingRemoved()
    {
        var change = Single("""{"a":1}""", null);

        Assert.Equal(JsonChangeKind.Removed, change.Kind);
    }

    [Fact]
    public void BothNull_IsNoChange()
        => Assert.Empty(JsonComparer.Compare(null, null));

    [Fact]
    public void NumericFormatting_IsNotNormalisedAway()
    {
        // 1 and 1.0 are different bytes in the column, and this tool reports what is stored.
        var change = Single("""{"n":1}""", """{"n":1.0}""");

        Assert.Equal(JsonChangeKind.Modified, change.Kind);
    }

    [Fact]
    public void JsonNull_IsDistinctFromAMissingKey()
    {
        var nulled = Single("""{"a":1}""", """{"a":null}""");
        Assert.Equal(JsonChangeKind.Modified, nulled.Kind);

        var dropped = Single("""{"a":1}""", """{}""");
        Assert.Equal(JsonChangeKind.Removed, dropped.Kind);
    }

    [Fact]
    public void LongValues_AreTruncated()
    {
        var before = $$"""{"blob":"{{new string('x', JsonComparer.MaxValueLength + 200)}}"}""";
        var change = Single(before, """{"blob":"short"}""");

        Assert.EndsWith("…", change.Before);
        Assert.True(change.Before!.Length <= JsonComparer.MaxValueLength + 1);
    }

    [Fact]
    public void UnparseableBody_ComparesInsteadOfThrowing()
    {
        // A body the store somehow wrote as non-JSON must not take the page down.
        var changes = JsonComparer.Compare("not json at all", """{"a":1}""");

        Assert.NotEmpty(changes);
    }

    [Fact]
    public void ChangesAreSortedByPath()
    {
        var changes = JsonComparer.Compare("""{"z":1,"a":1,"m":1}""", """{"z":2,"a":2,"m":2}""");

        Assert.Equal(["a", "m", "z"], changes.Select(x => x.Path));
    }

    [Fact]
    public void DeeplyNested_StopsAtTheDepthLimit()
    {
        // Build a chain deeper than MaxDepth and change the innermost value.
        var depth = JsonComparer.MaxDepth + 4;
        var before = Nest(depth, "1");
        var after = Nest(depth, "2");

        var change = Single(before, after);

        Assert.True(change.Path.Split('.').Length <= JsonComparer.MaxDepth + 1);
        Assert.Equal(JsonChangeKind.Modified, change.Kind);
    }

    static string Nest(int depth, string leaf)
    {
        var json = leaf;
        for (var i = 0; i < depth; i++)
            json = $$"""{"n":{{json}}}""";

        return json;
    }
}
