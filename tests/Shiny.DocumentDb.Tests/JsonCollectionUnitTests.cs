using System.Linq.Expressions;
using System.Text.Json.Nodes;
using Shiny.DocumentDb.Internal;
using Shiny.DocumentDb.Internal.Query;
using Xunit;

namespace Shiny.DocumentDb.Tests;

/// <summary>
/// The pieces of the JSON-collection seam that need no database: the extension expression node, the
/// name/path validator, and the schema-free field reader.
/// </summary>
public class DocumentFieldExpressionTests
{
    static DocumentFieldExpression Field(string path, Type type) => new(path, type);

    [Fact]
    public void SurvivesAnExpressionVisitor()
    {
        // The regression this guards: without a VisitChildren override, ExpressionVisitor falls through to
        // Expression.VisitChildren and throws "must be reducible" — and nothing hits that until a visitor
        // actually walks the tree, i.e. on the SECOND Where.
        var parameter = Expression.Parameter(typeof(JsonObject), "x");
        var body = Expression.GreaterThan(Field("total", typeof(int)), Expression.Constant(100));
        var lambda = Expression.Lambda<Func<JsonObject, bool>>(body, parameter);

        var visited = new IdentityVisitor().Visit(lambda);
        var twice = new IdentityVisitor().Visit(visited);

        Assert.NotNull(twice);
    }

    sealed class IdentityVisitor : ExpressionVisitor;

    [Fact]
    public void RetypeIsIdentityWhenUnchanged()
    {
        var field = Field("total", typeof(int));
        Assert.Same(field, field.Retype(typeof(int)));
        Assert.Equal(typeof(double), field.Retype(typeof(double)).Type);
        Assert.Equal("total", field.Retype(typeof(double)).JsonPath);
    }

    [Fact]
    public void ToStringIncludesTheType()
    {
        // Cursor pagination hashes the ordering's string form to derive the cursor shape, so two orderings
        // that extract the same path as different types must not produce interchangeable cursors.
        Assert.NotEqual(Field("total", typeof(int)).ToString(), Field("total", typeof(string)).ToString());
        Assert.Equal(Field("total", typeof(int)).ToString(), Field("total", typeof(int)).ToString());
    }
}

public class DynamicNamesTests
{
    [Theory]
    [InlineData("orders")]
    [InlineData("_private")]
    [InlineData("Orders2")]
    [InlineData("a_b_c")]
    public void AcceptsIdentifierNames(string name) => Assert.Equal(name, DynamicNames.ValidateCollection(name));

    [Theory]
    [InlineData("orders'; DROP TABLE x--")]
    [InlineData("a.b")]
    [InlineData("a b")]
    [InlineData("1abc")]
    [InlineData("")]
    [InlineData("   ")]
    [InlineData("$.a")]
    [InlineData("a[0]")]
    [InlineData("naïve")]
    [InlineData("a-b")]
    [InlineData("\"quoted\"")]
    public void RejectsEverythingElse(string name)
        => Assert.ThrowsAny<ArgumentException>(() => DynamicNames.ValidateCollection(name));

    [Fact]
    public void RejectsOverlongNames()
        => Assert.Throws<ArgumentException>(() => DynamicNames.ValidateCollection(new string('a', 129)));

    [Theory]
    [InlineData("total")]
    [InlineData("customer.name")]
    [InlineData("a.b.c.d")]
    public void AcceptsDottedPaths(string path) => Assert.Equal(path, DynamicNames.ValidatePath(path));

    [Theory]
    [InlineData("items[0]")]
    [InlineData("a..b")]
    [InlineData("order-id")]
    [InlineData("a b")]
    [InlineData("'; DROP TABLE x--")]
    public void RejectsUnsafePaths(string path)
        => Assert.ThrowsAny<ArgumentException>(() => DynamicNames.ValidatePath(path));

    [Fact]
    public void RejectsTooManySegments()
        => Assert.Throws<ArgumentException>(() => DynamicNames.ValidatePath(string.Join('.', Enumerable.Repeat("a", 17))));
}

public class DynamicFieldReaderTests
{
    static JsonObject Doc => JsonNode.Parse(
        """{ "name": "bob", "total": 100, "nested": { "deep": 7 }, "flag": true, "nothing": null }""")!.AsObject();

    [Fact]
    public void ReadsScalarsAndNestedPaths()
    {
        Assert.Equal("bob", DynamicFieldReader.Read(Doc, "name", typeof(string)));
        Assert.Equal(100, DynamicFieldReader.Read(Doc, "total", typeof(int)));
        Assert.Equal(7, DynamicFieldReader.Read(Doc, "nested.deep", typeof(int)));
        Assert.Equal(true, DynamicFieldReader.Read(Doc, "flag", typeof(bool)));
    }

    [Fact]
    public void MissingOrNullPathsReadAsNull()
    {
        Assert.Null(DynamicFieldReader.Read(Doc, "absent", typeof(string)));
        Assert.Null(DynamicFieldReader.Read(Doc, "nested.absent", typeof(int)));
        Assert.Null(DynamicFieldReader.Read(Doc, "name.deep", typeof(int)));
        Assert.Null(DynamicFieldReader.Read(Doc, "nothing", typeof(string)));
        Assert.Null(DynamicFieldReader.Read(null, "name", typeof(string)));
    }

    [Fact]
    public void CoercesAcrossJsonKinds()
    {
        // A schema-free lane cannot assume a writer stored a number as a number.
        var doc = JsonNode.Parse("""{ "n": "42" }""")!.AsObject();
        Assert.Equal(42, DynamicFieldReader.Read(doc, "n", typeof(int)));
        // A value that does not fit is "no match", not an error — the same thing a CAST yielding NULL does.
        Assert.Null(DynamicFieldReader.Read(Doc, "name", typeof(int)));
    }

    [Fact]
    public void ReadsPathsCaseInsensitively()
        => Assert.Equal("bob", DynamicFieldReader.Read(Doc, "Name", typeof(string)));
}

public class DynamicOrderByParserTests
{
    [Fact]
    public void SplitsKeysAndDirections()
    {
        var keys = DynamicOrderByParser.Parse("total:number desc, name");

        Assert.Equal(2, keys.Count);
        Assert.Equal("total:number", keys[0].Expression);
        Assert.True(keys[0].Descending);
        Assert.Equal("name", keys[1].Expression);
        Assert.False(keys[1].Descending);
    }

    [Fact]
    public void AcceptsEveryDirectionWord()
    {
        Assert.True(DynamicOrderByParser.Parse("a descending")[0].Descending);
        Assert.False(DynamicOrderByParser.Parse("a ascending")[0].Descending);
        Assert.False(DynamicOrderByParser.Parse("a asc")[0].Descending);
    }

    [Fact]
    public void DoesNotSplitInsideFunctionsOrQuotes()
    {
        var keys = DynamicOrderByParser.Parse("distance(area, '{\"type\":\"Point\",\"coordinates\":[0,0]}') desc, name");

        Assert.Equal(2, keys.Count);
        Assert.StartsWith("distance(", keys[0].Expression);
        Assert.True(keys[0].Descending);
        Assert.Equal("name", keys[1].Expression);
    }

    [Theory]
    [InlineData("")]
    [InlineData("   ")]
    [InlineData("a,,b")]
    [InlineData("a desc,")]
    public void RejectsMalformedOrderings(string ordering)
        => Assert.ThrowsAny<ArgumentException>(() => DynamicOrderByParser.Parse(ordering));

    [Fact]
    public void ABareDirectionWordIsAFieldName()
    {
        // A direction is only stripped when something precedes it, so a field genuinely called "desc"
        // still sorts ascending rather than parsing as a dangling direction.
        var key = Assert.Single(DynamicOrderByParser.Parse("desc"));
        Assert.Equal("desc", key.Expression);
        Assert.False(key.Descending);
    }
}
