using Xunit;

namespace Shiny.DocumentDb.OData.Tests;

public class ODataFilterParserTests
{
    [Fact]
    public void Parses_SimpleEquality()
    {
        var node = ODataFilterParser.Parse("Country eq 'CA'");
        Assert.Equal(ODataNodeKind.Binary, node.Kind);
        Assert.Equal(ODataBinaryOperator.Equal, node.BinaryOperator);
        Assert.Equal("Country", node.Left!.PropertyPath);
        Assert.Equal("CA", node.Right!.Value);
    }

    [Fact]
    public void Parses_DoubledQuoteEscaping()
    {
        var node = ODataFilterParser.Parse("Name eq 'O''Brien'");
        Assert.Equal("O'Brien", node.Right!.Value);
    }

    [Fact]
    public void Parses_AndOrPrecedence()
    {
        // a and b or c => (a and b) or c
        var node = ODataFilterParser.Parse("Age gt 10 and Age lt 20 or Country eq 'CA'");
        Assert.Equal(ODataBinaryOperator.Or, node.BinaryOperator);
        Assert.Equal(ODataBinaryOperator.And, node.Left!.BinaryOperator);
    }

    [Fact]
    public void Parses_NestedPath()
    {
        var node = ODataFilterParser.Parse("Address/City eq 'Toronto'");
        Assert.Equal("Address/City", node.Left!.PropertyPath);
    }

    [Fact]
    public void Parses_Function()
    {
        var node = ODataFilterParser.Parse("contains(Name, 'ar')");
        Assert.Equal(ODataNodeKind.Function, node.Kind);
        Assert.Equal("contains", node.FunctionName);
        Assert.Equal(2, node.Arguments.Count);
    }

    [Fact]
    public void Parses_NullLiteral()
    {
        var node = ODataFilterParser.Parse("Email eq null");
        Assert.Null(node.Right!.Value);
        Assert.Equal(ODataNodeKind.Constant, node.Right.Kind);
    }

    [Fact]
    public void Parses_NumberLiteral()
    {
        var node = ODataFilterParser.Parse("Age ge 30");
        Assert.Equal(30L, Convert.ToInt64(node.Right!.Value));
    }

    [Fact]
    public void Parses_IntegerLiteral_AsLong()
    {
        var node = ODataFilterParser.Parse("Age ge 30");
        Assert.IsType<long>(node.Right!.Value);
    }

    [Fact]
    public void QueryString_ParsesAllOptions()
    {
        var model = ODataQueryModelParser.ParseQueryString(
            "?$filter=Country eq 'CA'&$orderby=Age desc&$top=20&$skip=40&$count=true&$select=Name,Country");
        Assert.NotNull(model.Filter);
        Assert.Single(model.OrderBy);
        Assert.True(model.OrderBy[0].Descending);
        Assert.Equal(20, model.Top);
        Assert.Equal(40, model.Skip);
        Assert.True(model.Count);
        Assert.Equal("Name,Country", model.Select);
    }
}
