using System.Text.Json.Nodes;
using ShinyDocDbMyAdmin.Services;

namespace ShinyDocDbMyAdmin.Tests;

/// <summary>
/// Fixed-length arrays generate per position rather than from one pooled range.
/// </summary>
/// <remarks>
/// The case that forced this: a GeoJSON <c>coordinates</c> pair. Merging both slots into a single
/// element shape pools longitude and latitude, so a generated pair can put a longitude where the
/// latitude belongs - documents describing New York generate points in Antarctica.
/// </remarks>
public class DocumentGeneratorTupleTests
{
    static DocumentShape Profile(params string[] bodies) => DocumentShapeProfiler.Profile(bodies);

    static IReadOnlyList<JsonObject> Generate(DocumentShape shape, int count, int seed = 1234)
        => new DocumentGenerator(shape, seed).Generate(count);

    /// <summary>Three points around midtown Manhattan, as GeoJSON writes them: [longitude, latitude].</summary>
    static readonly string[] NewYork =
    [
        """{"id":"a","centre":{"type":"Point","coordinates":[-73.9855,40.7580]}}""",
        """{"id":"b","centre":{"type":"Point","coordinates":[-73.9654,40.7829]}}""",
        """{"id":"c","centre":{"type":"Point","coordinates":[-74.0445,40.6892]}}"""
    ];

    // ── Profiling ───────────────────────────────────────────────────────

    [Fact]
    public void AFixedLengthArray_IsProfiledPerPosition()
    {
        var shape = Profile(NewYork);
        var coordinates = shape.Root.Properties["centre"].Properties["coordinates"];

        Assert.True(coordinates.IsPositional);
        Assert.Equal(2, coordinates.Positions.Count);

        // Longitude in slot 0, latitude in slot 1 - each with its own range, not one pooled one.
        Assert.InRange(coordinates.Positions[0].Min!.Value, -74.05, -73.96);
        Assert.InRange(coordinates.Positions[1].Min!.Value, 40.68, 40.79);
    }

    [Fact]
    public void AVariableLengthArray_IsNotPositional()
    {
        // An ordinary collection: the third line item is not a different kind of thing from the first.
        var shape = Profile(
            """{"id":"a","items":[1,2,3]}""",
            """{"id":"b","items":[4]}""");

        Assert.False(shape.Root.Properties["items"].IsPositional);
    }

    [Fact]
    public void ALongFixedArray_IsNotPositional()
    {
        // Past the tracking cap this is a list, not a tuple - and 200 per-position shapes for an
        // embedding would cost more than it explains.
        var values = string.Join(",", Enumerable.Range(0, 20));
        var shape = Profile($$"""{"id":"a","vector":[{{values}}]}""");

        Assert.False(shape.Root.Properties["vector"].IsPositional);
    }

    [Fact]
    public void TheMergedElementShape_IsStillRecorded()
    {
        // Kept alongside the positional shapes: whether an array is a tuple is only known after
        // every document has been seen.
        var shape = Profile(NewYork);
        var coordinates = shape.Root.Properties["centre"].Properties["coordinates"];

        Assert.NotNull(coordinates.Element);
    }

    // ── Generation ──────────────────────────────────────────────────────

    [Fact]
    public void GeneratedCoordinates_StayInTheirOwnSlot()
    {
        var shape = Profile(NewYork);

        foreach (var document in Generate(shape, 200))
        {
            var pair = document["centre"]!["coordinates"]!.AsArray();

            Assert.Equal(2, pair.Count);

            var longitude = pair[0]!.GetValue<double>();
            var latitude = pair[1]!.GetValue<double>();

            // The whole point: before the fix these ranges were pooled, so a latitude of -73 was
            // routine - which is Antarctica, from documents describing Manhattan.
            Assert.InRange(longitude, -74.05, -73.96);
            Assert.InRange(latitude, 40.68, 40.79);
        }
    }

    [Fact]
    public void GeneratedTuples_KeepTheirLength()
    {
        var shape = Profile(NewYork);

        Assert.All(
            Generate(shape, 50),
            d => Assert.Equal(2, d["centre"]!["coordinates"]!.AsArray().Count));
    }

    [Fact]
    public void PositionsWithDifferentTypes_KeepTheirTypes()
    {
        // A heterogeneous tuple is exactly what a single merged shape gets wrong.
        var shape = Profile(
            """{"id":"a","reading":["temp",21.5,true]}""",
            """{"id":"b","reading":["humidity",44.2,false]}""");

        foreach (var document in Generate(shape, 50))
        {
            var reading = document["reading"]!.AsArray();
            Assert.Equal(3, reading.Count);
            Assert.Contains(reading[0]!.GetValue<string>(), new[] { "temp", "humidity" });
            Assert.InRange(reading[1]!.GetValue<double>(), 21.5, 44.2);
            Assert.IsType<bool>(reading[2]!.GetValue<bool>());
        }
    }

    [Fact]
    public void OrdinaryCollections_StillVaryInLength()
    {
        var shape = Profile(
            """{"id":"a","tags":["x"]}""",
            """{"id":"b","tags":["x","y","z"]}""");

        var lengths = Generate(shape, 100).Select(d => d["tags"]!.AsArray().Count).Distinct().ToList();

        // Unchanged behaviour for the non-tuple case.
        Assert.True(lengths.Count > 1, "expected variable-length arrays to still vary");
        Assert.All(lengths, l => Assert.InRange(l, 1, 3));
    }

    [Fact]
    public void GenerationStaysReproducible()
    {
        var shape = Profile(NewYork);

        var first = Generate(shape, 20, seed: 99).Select(d => d.ToJsonString()).ToList();
        var second = Generate(shape, 20, seed: 99).Select(d => d.ToJsonString()).ToList();

        // The preview/commit contract depends on this: the same seed must reproduce the same rows.
        Assert.Equal(first, second);
    }

    [Fact]
    public void GuidIdsAreReproducibleToo()
    {
        // These ids follow no numeric scheme, so the generator falls back to GUIDs. Drawn from
        // Guid.NewGuid() they were the one thing a replayed seed could not reproduce - so the
        // preview showed rows the commit then did not write.
        var shape = Profile(NewYork);

        var first = Generate(shape, 10, seed: 7).Select(d => d["id"]!.GetValue<string>()).ToList();
        var second = Generate(shape, 10, seed: 7).Select(d => d["id"]!.GetValue<string>()).ToList();

        Assert.Equal(first, second);
        Assert.Equal(first.Count, first.Distinct().Count());
        Assert.All(first, id => Assert.True(Guid.TryParse(id, out _), $"'{id}' is not a GUID"));
    }

    [Fact]
    public void PreviewAndCommit_ProduceTheSameDocuments()
    {
        // The promise the Generate tab makes in the UI, end to end: Preview picks a seed, Commit
        // replays it, and the rows written are the rows shown.
        var shape = Profile(NewYork);
        var preview = DocumentAdminService.Preview(shape, count: 25);

        var committed = new DocumentGenerator(shape, preview.Seed).Generate(preview.Count);

        Assert.Equal(
            preview.Sample.Select(d => d.ToJsonString()),
            committed.Take(preview.Sample.Count).Select(d => d.ToJsonString()));
    }

    [Fact]
    public void GuidFieldsThatAreNotIds_AreAlsoReproducible()
    {
        var shape = Profile(
            """{"id":"a","correlationId":"6f9619ff-8b86-d011-b42d-00cf4fc964ff"}""",
            """{"id":"b","correlationId":"6f9619ff-8b86-d011-b42d-00cf4fc964fe"}""");

        var first = Generate(shape, 10, seed: 3).Select(d => d["correlationId"]!.GetValue<string>()).ToList();
        var second = Generate(shape, 10, seed: 3).Select(d => d["correlationId"]!.GetValue<string>()).ToList();

        Assert.Equal(first, second);
        Assert.Equal(first.Count, first.Distinct().Count());
    }
}
