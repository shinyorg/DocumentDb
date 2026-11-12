using System.Text.Json;
using System.Text.Json.Serialization;
using Shiny.DocumentDb.Tests.Fixtures;
using Xunit;

namespace Shiny.DocumentDb.Tests;

/// <summary>
/// <c>DocumentStore.ToJsonLiteral</c> writes the values of <c>ExecuteUpdate</c> / <c>SetProperty</c> into the statement.
/// It must produce exactly what a document write produces for the same value (date predicates compare those strings),
/// without reflection-based serialization; <c>samples/Sample.Aot</c> runs the same path with reflection switched off.
/// </summary>
public class JsonLiteralTests
{
    public static TheoryData<object> QuotedScalars => new()
    {
        "plain",
        "quote \" backslash \\ <tag> & ü – \u0001",
        new DateTime(2026, 11, 12, 8, 30, 15, 123, DateTimeKind.Utc),
        new DateTime(2026, 11, 12, 8, 30, 15, DateTimeKind.Unspecified),
        new DateTimeOffset(2026, 11, 12, 8, 30, 15, 123, TimeSpan.FromHours(1)),
        new DateOnly(2026, 11, 12),
        new TimeOnly(8, 30, 15, 123),
        TimeSpan.FromMinutes(90.5),
        Guid.Parse("0b3c8f8e-6d2a-4c8e-9a51-2f4d7c1e0a99"),
        'q',
        '"',
        Priority.High,
        Permissions.Read | Permissions.Write
    };

    [Theory]
    [MemberData(nameof(QuotedScalars))]
    public void ToJsonLiteral_QuotedScalar_MatchesDocumentWrite(object value)
        => Assert.Equal(JsonSerializer.Serialize(value, value.GetType()), DocumentStore.ToJsonLiteral(value));

    [Theory]
    [InlineData(double.NaN)]
    [InlineData(double.PositiveInfinity)]
    [InlineData(float.NegativeInfinity)]
    public void ToJsonLiteral_NonFiniteNumber_Throws(object value)
        => Assert.Throws<ArgumentException>(() => DocumentStore.ToJsonLiteral(value));

    [Fact]
    public void NormalizeUpdateValue_Enum_FollowsTheStoresConverter()
    {
        var numeric = new JsonSerializerOptions(TestJsonContext.Default.Options);
        var named = new JsonSerializerOptions(TestJsonContext.Default.Options) { Converters = { new JsonStringEnumConverter() } };

        Assert.Equal(2, DocumentStore.NormalizeUpdateValue(Priority.High, numeric));
        Assert.Equal("High", DocumentStore.NormalizeUpdateValue(Priority.High, named));
    }

    [Fact]
    public void NormalizeUpdateValue_Char_BecomesAString()
        => Assert.Equal("q", DocumentStore.NormalizeUpdateValue('q', new JsonSerializerOptions(TestJsonContext.Default.Options)));

    [Fact]
    public void NormalizeUpdateValue_NonFiniteNumber_FollowsNamedLiteralHandling()
    {
        var strict = new JsonSerializerOptions(TestJsonContext.Default.Options);
        var named = new JsonSerializerOptions(TestJsonContext.Default.Options) { NumberHandling = JsonNumberHandling.AllowNamedFloatingPointLiterals };

        Assert.Throws<ArgumentException>(() => DocumentStore.NormalizeUpdateValue(double.NaN, strict));
        Assert.Equal("NaN", DocumentStore.NormalizeUpdateValue(double.NaN, named));
        Assert.Equal("-Infinity", DocumentStore.NormalizeUpdateValue(float.NegativeInfinity, named));
        Assert.Equal(1.5d, DocumentStore.NormalizeUpdateValue(1.5d, strict));
    }
}
