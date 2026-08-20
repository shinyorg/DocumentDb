using ShinyDocDbMyAdmin.Models;
using ShinyDocDbMyAdmin.Tui.Widgets;
using Shiny.DocumentDb;

namespace ShinyDocDbMyAdmin.Tui.Tests;

/// <summary>The pieces that had to be written for the terminal rather than reused from Core.</summary>
public sealed class WidgetTests
{
    // ── The browse grid's cell reader ───────────────────────────────────

    static DocumentGridRow Row(string json, SoftDeleteFlag? softDelete = null)
        => new(new DocumentRow("id-1", "Order", json, DateTimeOffset.Parse("2026-01-02T03:04:05Z"), null), softDelete);

    static SoftDeleteFlag Declared(string path, SoftDeleteFlagKind kind = SoftDeleteFlagKind.Boolean)
        => new() { TypeName = "Order", PropertyPath = path, FlagKind = kind };

    [Fact]
    public void A_flagged_row_says_so_in_the_id_cell()
    {
        // The grid has no colour to spare, so the marker rides the one column always on screen. It only
        // ever appears for a type whose flag the operator declared - nothing here infers one.
        var flagged = Row("""{"isDeleted":true}""", Declared("isDeleted"));
        var live = Row("""{"isDeleted":false}""", Declared("isDeleted"));
        var undeclared = Row("""{"isDeleted":true}""");

        Assert.Equal("id-1  (deleted)", flagged.Read("Id"));
        Assert.Equal("id-1", live.Read("Id"));
        Assert.Equal("id-1", undeclared.Read("Id"));
    }

    [Fact]
    public void A_timestamp_flag_is_deleted_when_it_is_set()
    {
        var flag = Declared("deletedAt", SoftDeleteFlagKind.Timestamp);

        Assert.Equal("id-1  (deleted)", Row("""{"deletedAt":"2026-01-02T03:04:05Z"}""", flag).Read("Id"));
        Assert.Equal("id-1", Row("""{"deletedAt":null}""", flag).Read("Id"));
        Assert.Equal("id-1", Row("{}", flag).Read("Id"));
    }

    [Fact]
    public void Envelope_columns_come_from_the_envelope_not_the_body()
    {
        var row = Row("""{"id":"something-else","status":"Shipped"}""");

        Assert.Equal("id-1", row.Read("Id"));
        Assert.Equal("Order", row.Read("TypeName"));
        Assert.Equal("Shipped", row.Read("status"));
    }

    [Fact]
    public void An_absent_path_and_an_explicit_null_both_read_as_null()
    {
        var row = Row("""{"status":null}""");

        // The same as the web front end's grid. Which fields are optional is the Structure tab's
        // question, not something a cell should answer two ways.
        Assert.Equal("null", row.Read("nothing.here"));
        Assert.Equal("null", row.Read("status"));
    }

    [Fact]
    public void Nested_paths_are_followed()
        => Assert.Equal("London", Row("""{"customer":{"city":"London"}}""").Read("customer.city"));

    [Fact]
    public void Strings_are_unquoted_but_objects_stay_as_json()
    {
        var row = Row("""{"name":"Widget","tags":["a","b"]}""");

        Assert.Equal("Widget", row.Read("name"));
        Assert.Equal("""["a","b"]""", row.Read("tags"));
    }

    [Fact]
    public void An_embedding_is_summarised_rather_than_printed()
    {
        var components = string.Join(",", Enumerable.Range(0, 64).Select(i => (i / 100.0).ToString("0.00")));
        var row = Row($$"""{"embedding":[{{components}}]}""");

        var cell = row.Read("embedding");

        Assert.StartsWith("float[64]", cell);
        Assert.True(cell.Length < 100, $"An embedding cell should be a summary, not 64 numbers: {cell}");
    }

    [Fact]
    public void A_multiline_value_is_flattened_so_it_cannot_break_the_row_height()
    {
        var row = Row("""{"note":"first\nsecond"}""");

        Assert.DoesNotContain('\n', row.Read("note"));
    }

    [Fact]
    public void A_body_that_will_not_parse_reads_as_blank_rather_than_throwing()
        => Assert.Equal("", Row("{not json").Read("anything"));

    // ── The map ─────────────────────────────────────────────────────────

    [Fact]
    public void The_canvas_draws_braille_for_the_geometry_it_is_given()
    {
        var canvas = new GeoCanvas();

        var features = new List<GeoFeature>
        {
            new("a", "at", new GeoPolygon([
                new GeoPoint(51.0, -1.0), new GeoPoint(52.0, -1.0),
                new GeoPoint(52.0, 1.0), new GeoPoint(51.0, 1.0), new GeoPoint(51.0, -1.0)
            ])),
            new("b", "at", new GeoLineString([new GeoPoint(51.2, -0.8), new GeoPoint(51.8, 0.8)]))
        };

        canvas.Draw(new GeometrySet(features, DocumentAdminServiceExtent(features), 2, false));

        var drawn = Rendered(canvas);
        Assert.Contains(drawn, c => c is >= '⠁' and <= '⣿');

        // Two families present, so the render carries two distinct foreground colours - the markup
        // names resolve to theme colours by the time this is a buffer, so count them rather than
        // look for the words.
        var foregrounds = System.Text.RegularExpressions.Regex
            .Matches(drawn, @"\[(#[0-9a-f]{6})[^\]]*\]")
            .Select(m => m.Groups[1].Value)
            .Distinct()
            .Count();

        Assert.True(foregrounds >= 2, $"Expected areas and lines to be drawn in different colours, saw {foregrounds}.");
    }

    [Fact]
    public void A_single_point_still_draws_rather_than_dividing_by_a_zero_span()
    {
        var canvas = new GeoCanvas();

        var features = new List<GeoFeature> { new("a", "at", new GeoPointGeometryStandIn(51.5, -0.12)) };
        canvas.Draw(new GeometrySet(features, DocumentAdminServiceExtent(features), 1, false));

        Assert.Contains(Rendered(canvas), c => c is >= '⠁' and <= '⣿');
    }

    [Fact]
    public void An_empty_set_draws_nothing_rather_than_an_empty_grid()
    {
        var canvas = new GeoCanvas();
        canvas.Draw(new GeometrySet([], null, 0, false));

        Assert.DoesNotContain(Rendered(canvas), c => c is >= '⠁' and <= '⣿');
    }

    static string Rendered(GeoCanvas canvas)
    {
        var buffer = XenoAtom.Terminal.UI.Rendering.VisualSnapshotRenderer.Render(canvas, 80, 24);
        return string.Join("\n", buffer.ToMarkupLines());
    }

    static GeoBoundingBox? DocumentAdminServiceExtent(IReadOnlyList<GeoFeature> features)
        => ShinyDocDbMyAdmin.Services.DocumentAdminService.Extent(features);

    /// <summary>A point geometry, which the library models as a wrapper the tool only ever reads.</summary>
    sealed class GeoPointGeometryStandIn(double latitude, double longitude) : Geometry
    {
        readonly GeoPoint point = new(latitude, longitude);

        public override GeoGeometryType GeometryType => GeoGeometryType.Point;
        public override bool IsEmpty => false;
        public override GeoBoundingBox GetEnvelope()
            => new(this.point.Latitude, this.point.Longitude, this.point.Latitude, this.point.Longitude);
    }
}
