using System.Text.Json;
using System.Text.Json.Nodes;
using ShinyDocDbMyAdmin.Services;

namespace ShinyDocDbMyAdmin.Tests;

/// <summary>
/// Reading a type's documents closely enough to make more of them.
/// </summary>
public class DocumentShapeProfilerTests
{
    static DocumentShape Profile(params string[] bodies) => DocumentShapeProfiler.Profile(bodies);

    [Fact]
    public void TopLevelFields_AreFoundInDocumentOrder()
    {
        var shape = Profile("""{"id":"a","title":"x","views":3}""");

        Assert.Equal(["id", "title", "views"], shape.Fields.Select(f => f.Name));
        Assert.Equal(1, shape.Samples);
    }

    [Fact]
    public void AFieldAbsentFromSomeDocuments_IsReportedAsOptional()
    {
        var shape = Profile(
            """{"id":"a","note":"hi"}""",
            """{"id":"b"}""",
            """{"id":"c"}""",
            """{"id":"d"}""");

        Assert.Equal(100, shape.Fields.Single(f => f.Name == "id").PercentPresent);
        Assert.Equal(25, shape.Fields.Single(f => f.Name == "note").PercentPresent);
    }

    [Fact]
    public void NumericRange_IsLearned()
    {
        var shape = Profile("""{"n":5}""", """{"n":100}""", """{"n":42}""");
        var n = shape.Root.Properties["n"];

        Assert.Equal(5, n.Min);
        Assert.Equal(100, n.Max);
        Assert.True(n.AllIntegral);
    }

    [Fact]
    public void AFractionalValue_MakesTheFieldNonIntegral()
    {
        Assert.False(Profile("""{"n":1}""", """{"n":2.5}""").Root.Properties["n"].AllIntegral);
    }

    [Fact]
    public void DateRange_AndItsFormat_AreLearned()
    {
        var shape = Profile("""{"at":"2026-01-01"}""", """{"at":"2026-06-30"}""");
        var at = shape.Root.Properties["at"];

        Assert.True(at.IsDate);
        Assert.Equal(new DateTime(2026, 1, 1), at.MinDate!.Value.Date);
        Assert.Equal(new DateTime(2026, 6, 30), at.MaxDate!.Value.Date);
        Assert.Equal("yyyy-MM-dd", at.DateFormat);
    }

    [Fact]
    public void AStringFieldWithOneNonDate_IsNotADateField()
    {
        Assert.False(Profile("""{"at":"2026-01-01"}""", """{"at":"soon"}""").Root.Properties["at"].IsDate);
    }

    [Fact]
    public void ASmallValueSet_IsCategorical()
    {
        var shape = Profile(
            """{"tier":"gold"}""", """{"tier":"silver"}""",
            """{"tier":"gold"}""", """{"tier":"bronze"}""");

        var tier = shape.Root.Properties["tier"];
        Assert.True(tier.IsCategorical);
        Assert.False(tier.Saturated);
        Assert.Equal(3, tier.Values.Count);
    }

    [Fact]
    public void ManyDistinctValues_SaturateIntoFreeText()
    {
        var bodies = Enumerable.Range(0, 200).Select(i => $$"""{"title":"unique {{i}}"}""").ToArray();

        var title = Profile(bodies).Root.Properties["title"];

        Assert.True(title.Saturated);
        Assert.Equal(NodeShape.MaxTrackedValues, title.Values.Count);
    }

    [Fact]
    public void NestedObjects_AreProfiledRecursively()
    {
        var shape = Profile(
            """{"customer":{"name":"bob","tier":"gold"}}""",
            """{"customer":{"name":"alice","tier":"silver"}}""");

        var customer = shape.Root.Properties["customer"];
        Assert.True(customer.IsObject);
        Assert.Equal(["name", "tier"], customer.PropertyOrder);
        Assert.Equal(2, customer.Properties["name"].Values.Count);
    }

    [Fact]
    public void Arrays_LearnTheirLengthRangeAndElementShape()
    {
        var shape = Profile("""{"tags":["a"]}""", """{"tags":["a","b","c"]}""");
        var tags = shape.Root.Properties["tags"];

        Assert.True(tags.IsArray);
        Assert.Equal(1, tags.MinLength);
        Assert.Equal(3, tags.MaxLength);
        Assert.Equal(JsonValueKind.String, tags.Element!.DominantKind);
    }

    [Fact]
    public void GuidStrings_AreRecognised()
    {
        var shape = Profile(
            $$"""{"ref":"{{Guid.NewGuid()}}"}""",
            $$"""{"ref":"{{Guid.NewGuid()}}"}""");

        Assert.True(shape.Root.Properties["ref"].IsGuid);
    }

    [Fact]
    public void TheIdProperty_MatchesTheCasingTheDocumentsUse()
    {
        Assert.Equal("Id", Profile("""{"Id":"a"}""").IdProperty);
        Assert.Equal("id", Profile("""{"id":"a"}""").IdProperty);
    }

    [Fact]
    public void MalformedAndNonObjectBodies_AreSkipped()
    {
        var shape = Profile("not json", "[1,2,3]", """{"id":"a"}""");

        Assert.Equal(1, shape.Samples);
    }

    [Fact]
    public void BooleanCounts_AreKept_SoTheDistributionSurvives()
    {
        var shape = Profile("""{"ok":true}""", """{"ok":true}""", """{"ok":true}""", """{"ok":false}""");
        var ok = shape.Root.Properties["ok"];

        Assert.Equal(3, ok.Kinds[JsonValueKind.True]);
        Assert.Equal(1, ok.Kinds[JsonValueKind.False]);
    }
}

/// <summary>
/// Making documents from a learned shape. The property that matters throughout: generated values
/// come from the range the real data occupies, because data that is merely well-typed is no use for
/// judging a query plan or reading a grid.
/// </summary>
public class DocumentGeneratorTests
{
    const string Gold = """{"id":"art-0001","title":"Alpha","tier":"gold","views":10,"at":"2026-01-01"}""";
    const string Silver = """{"id":"art-0002","title":"Beta","tier":"silver","views":90,"at":"2026-12-31"}""";

    static IReadOnlyList<JsonObject> Generate(int count, int seed = 1234, params string[] bodies)
        => new DocumentGenerator(DocumentShapeProfiler.Profile(bodies.Length > 0 ? bodies : [Gold, Silver]), seed)
            .Generate(count);

    [Fact]
    public void ItProducesTheRequestedCount()
        => Assert.Equal(50, Generate(50).Count);

    [Fact]
    public void TheSameSeed_ProducesIdenticalDocuments()
    {
        // The preview would be a decoration otherwise: it shows one set and commits another.
        var first = Generate(10, seed: 99);
        var second = Generate(10, seed: 99);

        Assert.Equal(
            first.Select(d => d.ToJsonString()),
            second.Select(d => d.ToJsonString()));
    }

    [Fact]
    public void DifferentSeeds_ProduceDifferentDocuments()
    {
        var a = Generate(20, seed: 1);
        var b = Generate(20, seed: 2);

        Assert.NotEqual(a.Select(d => d.ToJsonString()), b.Select(d => d.ToJsonString()));
    }

    [Fact]
    public void EveryFieldOfTheSource_IsPresent()
    {
        var document = Generate(1)[0];

        Assert.Equal(["id", "title", "tier", "views", "at"], document.Select(p => p.Key));
    }

    [Fact]
    public void NumbersLandInsideTheObservedRange()
    {
        var views = Generate(200).Select(d => d["views"]!.GetValue<long>()).ToList();

        Assert.All(views, v => Assert.InRange(v, 10, 90));
        // And they actually vary, rather than pinning to one end.
        Assert.True(views.Distinct().Count() > 5);
    }

    [Fact]
    public void CategoricalFields_OnlyEverUseObservedValues()
    {
        var tiers = Generate(200).Select(d => d["tier"]!.GetValue<string>()).Distinct().Order();

        Assert.Equal(["gold", "silver"], tiers);
    }

    [Fact]
    public void DatesLandInsideTheObservedSpan_AndKeepTheirFormat()
    {
        foreach (var document in Generate(100))
        {
            var text = document["at"]!.GetValue<string>();

            Assert.Matches(@"^\d{4}-\d{2}-\d{2}$", text);
            Assert.InRange(DateTime.Parse(text), new DateTime(2026, 1, 1), new DateTime(2026, 12, 31));
        }
    }

    [Fact]
    public void IdsContinueADetectedSequence_RatherThanRestartingOrColliding()
    {
        var ids = Generate(3).Select(d => d["id"]!.GetValue<string>()).ToList();

        Assert.Equal(["art-0003", "art-0004", "art-0005"], ids);
    }

    [Fact]
    public void IdsAreAlwaysUnique()
    {
        var ids = Generate(500).Select(d => d["id"]!.GetValue<string>()).ToList();

        Assert.Equal(500, ids.Distinct().Count());
    }

    [Fact]
    public void GuidIds_StayGuids()
    {
        var ids = Generate(5, 7,
            $$"""{"id":"{{Guid.NewGuid()}}","n":1}""",
            $$"""{"id":"{{Guid.NewGuid()}}","n":2}""")
            .Select(d => d["id"]!.GetValue<string>());

        Assert.All(ids, id => Assert.True(Guid.TryParse(id, out _)));
    }

    [Fact]
    public void UnpaddedSequenceIds_StayUnpadded()
    {
        var ids = Generate(2, 7, """{"id":"order_7"}""", """{"id":"order_8"}""")
            .Select(d => d["id"]!.GetValue<string>());

        Assert.Equal(["order_9", "order_10"], ids);
    }

    [Fact]
    public void OptionalFields_AreSometimesAbsent_AndSometimesPresent()
    {
        // Present in half the source, so neither always nor never in the output.
        var bodies = new[]
        {
            """{"id":"a-1","note":"x"}""", """{"id":"a-2"}""",
            """{"id":"a-3","note":"y"}""", """{"id":"a-4"}"""
        };

        var withNote = Generate(200, 5, bodies).Count(d => d.ContainsKey("note"));

        Assert.InRange(withNote, 20, 180);
    }

    [Fact]
    public void NestedObjects_AreRebuilt()
    {
        var document = Generate(1, 3,
            """{"id":"x-1","customer":{"name":"bob","tier":"gold"}}""",
            """{"id":"x-2","customer":{"name":"alice","tier":"silver"}}""")[0];

        var customer = Assert.IsType<JsonObject>(document["customer"]);
        Assert.Contains(customer["name"]!.GetValue<string>(), new[] { "bob", "alice" });
    }

    [Fact]
    public void Arrays_AreRebuiltWithinTheObservedLengths()
    {
        var documents = Generate(50, 3,
            """{"id":"x-1","tags":["a"]}""",
            """{"id":"x-2","tags":["a","b","c"]}""");

        foreach (var document in documents)
        {
            var tags = Assert.IsType<JsonArray>(document["tags"]);
            Assert.InRange(tags.Count, 1, 3);
        }
    }

    [Fact]
    public void BooleanDistribution_FollowsTheSource()
    {
        var bodies = new[]
        {
            """{"id":"b-1","ok":true}""", """{"id":"b-2","ok":true}""",
            """{"id":"b-3","ok":true}""", """{"id":"b-4","ok":false}"""
        };

        var trues = Generate(400, 11, bodies).Count(d => d["ok"]!.GetValue<bool>());

        // Source is 75% true; allow a wide band, the point is that it is not 50/50 or absolute.
        Assert.InRange(trues, 240, 360);
    }

    [Fact]
    public void GuidFieldsThatAreNotTheId_AreStillFreshEachTime()
    {
        var refs = Generate(20, 3,
            $$"""{"id":"x-1","ref":"{{Guid.NewGuid()}}"}""",
            $$"""{"id":"x-2","ref":"{{Guid.NewGuid()}}"}""")
            .Select(d => d["ref"]!.GetValue<string>());

        Assert.Equal(20, refs.Distinct().Count());
    }
}
