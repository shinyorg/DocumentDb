using Shiny.DocumentDb.Internal.FullText;
using Xunit;

namespace Shiny.DocumentDb.Tests;

/// <summary>
/// Unit coverage for the dependency-free Lucene parser, the in-memory evaluator (which backs the
/// DocumentFunctions.LuceneMatch/LuceneScore BCL bodies), and the provider capability gate.
/// </summary>
public class LuceneQueryParserTests
{
    static FtQuery Parse(string q) => LuceneQueryParser.Parse(q);

    [Fact]
    public void Term_ParsesToFtTerm()
    {
        var root = Parse("quick").Root;
        var term = Assert.IsType<FtTerm>(root);
        Assert.Equal("quick", term.Text);
    }

    [Fact]
    public void Phrase_ParsesToFtPhrase()
    {
        var phrase = Assert.IsType<FtPhrase>(Parse("\"quick brown\"").Root);
        Assert.Equal(new[] { "quick", "brown" }, phrase.Terms);
    }

    [Fact]
    public void Prefix_Fuzzy_Proximity_Boost_Parse()
    {
        Assert.IsType<FtPrefix>(Parse("qui*").Root);
        Assert.IsType<FtFuzzy>(Parse("roam~").Root);
        Assert.IsType<FtProximity>(Parse("\"quick fox\"~5").Root);
        Assert.IsType<FtBoost>(Parse("quick^2").Root);
    }

    [Fact]
    public void And_PromotesBothOperandsToMust()
    {
        var b = Assert.IsType<FtBool>(Parse("quick AND brown").Root);
        Assert.All(b.Clauses, c => Assert.Equal(FtOccur.Must, c.Occur));
    }

    [Fact]
    public void Space_DefaultsToShould()
    {
        var b = Assert.IsType<FtBool>(Parse("quick brown").Root);
        Assert.All(b.Clauses, c => Assert.Equal(FtOccur.Should, c.Occur));
    }

    [Fact]
    public void Not_And_Minus_ProduceMustNot()
    {
        var viaNot = Assert.IsType<FtBool>(Parse("quick NOT brown").Root);
        Assert.Contains(viaNot.Clauses, c => c.Occur == FtOccur.MustNot);

        var viaMinus = Assert.IsType<FtBool>(Parse("+quick -brown").Root);
        Assert.Contains(viaMinus.Clauses, c => c.Occur == FtOccur.MustNot);
        Assert.Contains(viaMinus.Clauses, c => c.Occur == FtOccur.Must);
    }

    [Fact]
    public void Range_IsRejected()
    {
        Assert.Throws<FormatException>(() => Parse("[1 TO 5]"));
    }

    [Fact]
    public void NonTrailingWildcard_IsRejected()
    {
        Assert.Throws<FormatException>(() => Parse("te*t"));
    }

    [Fact]
    public void MalformedFuzzyNumber_ThrowsPositionedFormatException()
    {
        // A malformed number after '~' (e.g. two decimal points) must raise the parser's positioned
        // FormatException, not a bare double.Parse FormatException.
        var ex = Assert.Throws<FormatException>(() => Parse("foo~1.2.3"));
        Assert.Contains("1.2.3", ex.Message);
    }

    // ── Evaluator ──────────────────────────────────────────────────────

    [Theory]
    [InlineData("quick brown", "the quick fox", true)]   // OR — one term present
    [InlineData("quick AND fox", "the quick fox", true)]
    [InlineData("quick AND cat", "the quick fox", false)]
    [InlineData("quick NOT fox", "the quick fox", false)]
    [InlineData("quick NOT cat", "the quick fox", true)]
    [InlineData("qui*", "the quick fox", true)]
    [InlineData("\"quick fox\"", "the quick brown fox", false)] // not adjacent, slop 0
    [InlineData("\"quick fox\"~2", "the quick brown fox", true)]
    [InlineData("quic~1", "the quick fox", true)]         // edit distance 1
    [InlineData("quick^3", "the quick fox", true)]        // boost is a no-op for a boolean match
    [InlineData("(quick OR slow) AND fox", "the quick fox", true)]
    public void Evaluator_Matches(string query, string text, bool expected)
        => Assert.Equal(expected, FtQueryEvaluator.Matches(Parse(query), text));

    [Fact]
    public void Evaluator_Score_RewardsFrequency()
    {
        var q = Parse("quick");
        Assert.True(FtQueryEvaluator.Score(q, "quick quick quick") > FtQueryEvaluator.Score(q, "quick"));
        Assert.Equal(0, FtQueryEvaluator.Score(q, "nothing here"));
    }

    [Theory]
    [InlineData("-saga")]                 // top-level pure negation
    [InlineData("NOT saga")]
    [InlineData("foo AND (-bar)")]        // nested pure-negation group
    public void Evaluator_PureNegation_Throws_ConsistentWithRelational(string query)
    {
        // A "match everything except X" query is rejected on every pushdown provider; the in-memory oracle
        // must reject it too rather than silently matching every non-excluded document.
        Assert.Throws<NotSupportedException>(() => FtQueryEvaluator.Matches(Parse(query), "anything"));
    }

    [Fact]
    public void Evaluator_Field_Throws()
        => Assert.Throws<NotSupportedException>(() => FtQueryEvaluator.Matches(Parse("title:quick"), "quick"));

    // ── Capability gate ────────────────────────────────────────────────

    [Fact]
    public void Capability_BaselineTermsAlwaysAllowed()
        => FtCapabilityCheck.EnsureSupported(Parse("quick AND (brown OR fox)"), FtCapabilities.None, "Test", forScore: false);

    [Fact]
    public void Capability_UnsupportedPrefix_Throws()
        => Assert.Throws<NotSupportedException>(
            () => FtCapabilityCheck.EnsureSupported(Parse("qui*"), FtCapabilities.None, "Test", forScore: false));

    [Fact]
    public void Capability_Boost_IgnoredForMatch_ButCheckedForScore()
    {
        // No Boost capability: fine for a boolean match, rejected for a score.
        FtCapabilityCheck.EnsureSupported(Parse("quick^2"), FtCapabilities.None, "Test", forScore: false);
        Assert.Throws<NotSupportedException>(
            () => FtCapabilityCheck.EnsureSupported(Parse("quick^2"), FtCapabilities.None, "Test", forScore: true));
    }

    [Fact]
    public void Capability_PurelyNegative_Throws()
        => Assert.Throws<NotSupportedException>(
            () => FtCapabilityCheck.EnsureSupported(Parse("-quick"), FtCapabilities.All, "Test", forScore: false));
}
