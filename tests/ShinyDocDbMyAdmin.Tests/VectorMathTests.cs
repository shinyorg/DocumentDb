using System.Text.Json.Nodes;
using Shiny.DocumentDb;
using ShinyDocDbMyAdmin.Models;
using ShinyDocDbMyAdmin.Services;

namespace ShinyDocDbMyAdmin.Tests;

/// <summary>
/// Reading embeddings out of document bodies and comparing them. The tool ranks similarity itself
/// rather than pushing an ANN query down, so these are the results, not a convenience layer over
/// someone else's.
/// </summary>
public class VectorMathTests
{
    static JsonNode Node(string json) => JsonNode.Parse(json)!;

    [Fact]
    public void NumericArray_IsReadAsAVector()
    {
        var values = VectorMath.Read(Node("[1,2,3,4,5,6,7,8]"));

        Assert.NotNull(values);
        Assert.Equal(8, values.Length);
        Assert.Equal(1f, values[0]);
        Assert.Equal(8f, values[7]);
    }

    [Fact]
    public void ShortArray_IsNotAVector()
    {
        // A lat/lng pair or an RGB triplet is a numeric array too, and is not an embedding.
        Assert.Null(VectorMath.Read(Node("[51.5,-0.12]")));
        Assert.Null(VectorMath.Read(Node("[255,128,0]")));
    }

    [Fact]
    public void ArrayOfStrings_IsNotAVector()
        => Assert.Null(VectorMath.Read(Node("""["a","b","c","d","e","f","g","h"]""")));

    [Fact]
    public void MixedArray_IsNotAVector()
        => Assert.Null(VectorMath.Read(Node("""[1,2,3,"four",5,6,7,8]""")));

    [Fact]
    public void ObjectOrScalar_IsNotAVector()
    {
        Assert.Null(VectorMath.Read(Node("""{"a":1}""")));
        Assert.Null(VectorMath.Read(Node("42")));
        Assert.Null(VectorMath.Read(null));
    }

    [Theory]
    [InlineData("[1, 2, 3]")]
    [InlineData("1, 2, 3")]
    [InlineData("1 2 3")]
    [InlineData(" [1,2,3] ")]
    public void PastedVectors_AreParsedInEitherForm(string text)
        => Assert.Equal([1f, 2f, 3f], VectorMath.Parse(text));

    [Fact]
    public void NonNumericPaste_IsRejectedWithTheOffendingToken()
    {
        var ex = Assert.Throws<FormatException>(() => VectorMath.Parse("1, two, 3"));

        Assert.Contains("two", ex.Message);
    }

    [Fact]
    public void EmptyPaste_IsRejected()
    {
        Assert.Throws<FormatException>(() => VectorMath.Parse("   "));
        Assert.Throws<FormatException>(() => VectorMath.Parse("[]"));
    }

    [Fact]
    public void FormattedVector_RoundTripsThroughParse()
    {
        float[] original = [0.5f, -0.25f, 1e-8f, 12345.678f];

        Assert.Equal(original, VectorMath.Parse(VectorMath.Format(original)));
    }

    [Fact]
    public void IdenticalVectors_HaveZeroCosineDistance()
    {
        float[] a = [1, 2, 3, 4];

        Assert.Equal(0, VectorMath.Distance(a, a, VectorDistance.Cosine), 6);
    }

    [Fact]
    public void OppositeVectors_AreTheFurthestApartByCosine()
    {
        float[] a = [1, 0, 0, 0];
        float[] b = [-1, 0, 0, 0];

        Assert.Equal(2, VectorMath.Distance(a, b, VectorDistance.Cosine), 6);
    }

    [Fact]
    public void CosineIgnoresMagnitude_ButEuclideanDoesNot()
    {
        float[] a = [1, 1, 0, 0];
        float[] scaled = [10, 10, 0, 0];

        Assert.Equal(0, VectorMath.Distance(a, scaled, VectorDistance.Cosine), 6);
        Assert.True(VectorMath.Distance(a, scaled, VectorDistance.Euclidean) > 0);
    }

    [Fact]
    public void ZeroVector_HasZeroCosineSimilarity_RatherThanNaN()
    {
        float[] zero = [0, 0, 0, 0];
        float[] other = [1, 2, 3, 4];

        var distance = VectorMath.Distance(zero, other, VectorDistance.Cosine);

        Assert.False(double.IsNaN(distance));
        Assert.Equal(1, distance, 6);
    }

    [Fact]
    public void DotProductIsNegated_SoSmallerIsStillNearer()
    {
        float[] probe = [1, 1, 0, 0];
        float[] aligned = [5, 5, 0, 0];
        float[] opposed = [-5, -5, 0, 0];

        var near = VectorMath.Distance(aligned, probe, VectorDistance.DotProduct);
        var far = VectorMath.Distance(opposed, probe, VectorDistance.DotProduct);

        Assert.True(near < far);
    }

    [Fact]
    public void DimensionMismatch_Throws_RatherThanScoringTheComparablePrefix()
    {
        float[] a = [1, 2, 3, 4];
        float[] b = [1, 2, 3];

        var ex = Assert.Throws<ArgumentException>(() => VectorMath.Distance(a, b, VectorDistance.Cosine));

        Assert.Contains("4", ex.Message);
        Assert.Contains("3", ex.Message);
    }

    [Fact]
    public void EuclideanDistance_IsTheStraightLine()
    {
        float[] a = [0, 0, 0, 0];
        float[] b = [3, 4, 0, 0];

        Assert.Equal(5, VectorMath.Distance(a, b, VectorDistance.Euclidean), 6);
    }

    [Fact]
    public void Norm_IdentifiesANormalisedVector()
    {
        float[] unit = [0.6f, 0.8f, 0, 0];
        float[] raw = [3, 4, 0, 0];

        Assert.True(VectorMath.IsNormalised(VectorMath.Norm(unit)));
        Assert.False(VectorMath.IsNormalised(VectorMath.Norm(raw)));
        Assert.Equal(5, VectorMath.Norm(raw), 6);
    }
}

/// <summary>The per-document summary the Vectors tab shows in place of the components themselves.</summary>
public class VectorEntryTests
{
    [Fact]
    public void Describe_ReportsRangeAndMean()
    {
        var entry = VectorEntry.Describe("doc-1", [1, 2, 3, 4]);

        Assert.Equal(4, entry.Dimensions);
        Assert.Equal(1f, entry.Min);
        Assert.Equal(4f, entry.Max);
        Assert.Equal(2.5, entry.Mean, 6);
        Assert.False(entry.HasNonFinite);
    }

    [Fact]
    public void Describe_KeepsOnlyAShortPreview_NotTheWholeVector()
    {
        var big = Enumerable.Range(0, 1536).Select(i => (float)i).ToArray();

        var entry = VectorEntry.Describe("doc-1", big);

        Assert.Equal(1536, entry.Dimensions);
        Assert.Equal(8, entry.Preview.Count);
    }

    [Fact]
    public void AllZeroVector_IsFlagged()
    {
        var entry = VectorEntry.Describe("doc-1", new float[16]);

        Assert.True(entry.IsZero);
        Assert.False(entry.IsNormalised);
    }

    [Fact]
    public void NonFiniteComponents_AreFlagged_AndDoNotPoisonTheRange()
    {
        var entry = VectorEntry.Describe("doc-1", [1, float.NaN, 3, float.PositiveInfinity]);

        Assert.True(entry.HasNonFinite);
        Assert.Equal(1f, entry.Min);
        Assert.Equal(3f, entry.Max);
    }
}
