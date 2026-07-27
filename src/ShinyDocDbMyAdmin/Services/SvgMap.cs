using System.Globalization;
using System.Text;
using ShinyDocDbMyAdmin.Models;
using Shiny.DocumentDb;

namespace ShinyDocDbMyAdmin.Services;

/// <summary>
/// Renders geometries as a self-contained inline SVG.
/// </summary>
/// <remarks>
/// <para>
/// There is deliberately no basemap. Any tile layer means sending the coordinates of whatever is in the
/// database to a third party on every pan - which is not a reasonable default for a tool that gets
/// pointed at production data. So this draws the shapes over a graticule and a scale bar, offline, with
/// nothing leaving the server.
/// </para>
/// <para>
/// Colour is by geometry family, capped at three because a map is an all-pairs colour form where only
/// three categorical slots clear the colour-vision and contrast gates. Each family also has a distinct
/// mark - filled ring, stroked path, dot - so the distinction never rests on hue alone. Both palettes
/// were validated against this app's own light and dark panel surfaces.
/// </para>
/// </remarks>
public static class SvgMap
{
    /// <summary>Web Mercator is undefined at the poles; clamp to the standard cutoff.</summary>
    const double MaxLatitude = 85.05112878;

    public static string FamilyLabel(GeoFamily family) => family switch
    {
        GeoFamily.Point => "Points",
        GeoFamily.Line => "Lines",
        _ => "Areas"
    };

    /// <summary>The CSS custom property carrying this family's colour, defined for both themes in app.css.</summary>
    public static string FamilyVariable(GeoFamily family) => family switch
    {
        GeoFamily.Point => "--geo-point",
        GeoFamily.Line => "--geo-line",
        _ => "--geo-area"
    };

    /// <summary>
    /// Draws the set. <paramref name="focus"/> overrides the fitted extent - the "zoom to this feature"
    /// path, which matters because a fit over two distant clusters honestly renders both as specks.
    /// </summary>
    public static string Render(GeometrySet set, GeoBoundingBox? focus = null, int width = 900, int height = 480)
    {
        var svg = new StringBuilder();
        svg.Append(CultureInfo.InvariantCulture, $"<svg viewBox=\"0 0 {width} {height}\" width=\"100%\" ")
           .Append(CultureInfo.InvariantCulture, $"height=\"{height}\" role=\"img\" class=\"geomap\" ")
           .Append("aria-label=\"Map of the geometries in this view\" xmlns=\"http://www.w3.org/2000/svg\">");

        if ((focus ?? set.Extent) is not { } extent)
        {
            svg.Append("</svg>");
            return svg.ToString();
        }

        var view = Viewport.Fit(extent, width, height, padding: 26);

        AppendGraticule(svg, view, width, height);

        // Areas first, then lines, then points: the smallest marks stay on top and clickable.
        foreach (var family in (GeoFamily[])[GeoFamily.Area, GeoFamily.Line, GeoFamily.Point])
        {
            foreach (var feature in set.Features.Where(f => f.Family == family))
                AppendFeature(svg, feature, view);
        }

        AppendScaleBar(svg, view, width, height);
        svg.Append("</svg>");
        return svg.ToString();
    }

    static void AppendGraticule(StringBuilder svg, Viewport view, int width, int height)
    {
        svg.Append("<g class=\"geomap-grid\">");

        // Steps are chosen from the pixels available, not from the data span: a tall narrow extent
        // otherwise asks for gridlines every few pixels and the labels pile on top of each other.
        var latStep = NiceStep(view.Extent.MaxLatitude - view.Extent.MinLatitude, view.PlotHeight, 70);
        var lngStep = NiceStep(view.Extent.MaxLongitude - view.Extent.MinLongitude, view.PlotWidth, 90);

        for (var lat = Math.Ceiling(view.Extent.MinLatitude / latStep) * latStep; lat <= view.Extent.MaxLatitude; lat += latStep)
        {
            var y = view.Y(lat);
            Line(svg, 0, y, width, y);
            Label(svg, 4, y - 3, Format(lat) + "°");
        }

        for (var lng = Math.Ceiling(view.Extent.MinLongitude / lngStep) * lngStep; lng <= view.Extent.MaxLongitude; lng += lngStep)
        {
            var x = view.X(lng);
            Line(svg, x, 0, x, height);
            Label(svg, x + 3, height - 6, Format(lng) + "°");
        }

        svg.Append("</g>");
    }

    static void AppendFeature(StringBuilder svg, GeoFeature feature, Viewport view)
    {
        var family = feature.Family;
        var colour = $"var({FamilyVariable(family)})";

        svg.Append(CultureInfo.InvariantCulture,
            $"<g class=\"geomark geomark-{family.ToString().ToLowerInvariant()}\" style=\"--mark:{colour}\">");

        // A native SVG tooltip: identity without a JS layer, and it survives with scripting off.
        svg.Append("<title>")
           .Append(Escape($"{feature.DocumentId} — {feature.Geometry.GeometryType}, {feature.Geometry.NumPoints} point(s)"))
           .Append("</title>");

        Draw(svg, feature.Geometry, view);
        svg.Append("</g>");
    }

    static void Draw(StringBuilder svg, Geometry geometry, Viewport view)
    {
        switch (geometry)
        {
            case GeoPolygon polygon:
                var path = new StringBuilder();
                AppendRing(path, polygon.ExteriorRing, view);
                foreach (var hole in polygon.InteriorRings)
                    AppendRing(path, hole, view);

                // evenodd so interior rings punch actual holes rather than painting over them.
                svg.Append(CultureInfo.InvariantCulture, $"<path class=\"geo-area\" fill-rule=\"evenodd\" d=\"{path}\" />");
                break;

            case GeoMultiPolygon multi:
                foreach (var member in multi.Polygons)
                    Draw(svg, member, view);
                break;

            case GeoLineString line:
                var linePath = new StringBuilder();
                AppendOpen(linePath, line.Coordinates, view);
                svg.Append(CultureInfo.InvariantCulture, $"<path class=\"geo-line\" d=\"{linePath}\" />");
                break;

            case GeoMultiLineString multiLine:
                foreach (var member in multiLine.LineStrings)
                    Draw(svg, member, view);
                break;

            case GeoMultiPoint multiPoint:
                foreach (var point in multiPoint.Points)
                    Dot(svg, point, view);
                break;

            case GeoGeometryCollection collection:
                foreach (var member in collection.Geometries)
                    Draw(svg, member, view);
                break;

            default:
                // A bare Point arrives as the lightweight struct wrapped in a point geometry; its
                // envelope is the coordinate itself, which is all a dot needs.
                var envelope = geometry.GetEnvelope();
                Dot(svg, new GeoPoint(envelope.MinLatitude, envelope.MinLongitude), view);
                break;
        }
    }

    static void Dot(StringBuilder svg, GeoPoint point, Viewport view)
        => svg.Append(CultureInfo.InvariantCulture,
            $"<circle class=\"geo-point\" cx=\"{F(view.X(point.Longitude))}\" cy=\"{F(view.Y(point.Latitude))}\" r=\"4.5\" />");

    static void AppendRing(StringBuilder path, IReadOnlyList<GeoPoint> ring, Viewport view)
    {
        if (ring.Count == 0)
            return;

        AppendOpen(path, ring, view);
        path.Append('Z');
    }

    static void AppendOpen(StringBuilder path, IReadOnlyList<GeoPoint> points, Viewport view)
    {
        for (var i = 0; i < points.Count; i++)
        {
            path.Append(i == 0 ? 'M' : 'L')
                .Append(F(view.X(points[i].Longitude)))
                .Append(' ')
                .Append(F(view.Y(points[i].Latitude)));
        }
    }

    static void AppendScaleBar(StringBuilder svg, Viewport view, int width, int height)
    {
        // A bar spanning a round number of kilometres at the centre latitude.
        var centreLat = (view.Extent.MinLatitude + view.Extent.MaxLatitude) / 2;
        var metresPerPixel = MetresPerDegreeLongitude(centreLat) * (view.Extent.MaxLongitude - view.Extent.MinLongitude) / Math.Max(1, view.PlotWidth);
        if (metresPerPixel <= 0 || double.IsNaN(metresPerPixel) || double.IsInfinity(metresPerPixel))
            return;

        var rounded = NiceStep(metresPerPixel * 240);
        var pixels = rounded / metresPerPixel;
        if (pixels is < 20 or > 400)
            return;

        var x = width - pixels - 14;
        var y = height - 18;

        svg.Append("<g class=\"geomap-scale\">");
        svg.Append(CultureInfo.InvariantCulture,
            $"<line x1=\"{F(x)}\" y1=\"{F(y)}\" x2=\"{F(x + pixels)}\" y2=\"{F(y)}\" />");
        Label(svg, x, y - 5, rounded >= 1000 ? $"{F(rounded / 1000)} km" : $"{F(rounded)} m");
        svg.Append("</g>");
    }

    static void Line(StringBuilder svg, double x1, double y1, double x2, double y2)
        => svg.Append(CultureInfo.InvariantCulture,
            $"<line x1=\"{F(x1)}\" y1=\"{F(y1)}\" x2=\"{F(x2)}\" y2=\"{F(y2)}\" />");

    static void Label(StringBuilder svg, double x, double y, string text)
        => svg.Append(CultureInfo.InvariantCulture, $"<text x=\"{F(x)}\" y=\"{F(y)}\">").Append(Escape(text)).Append("</text>");

    /// <summary>Projects lat/lng into the SVG box, fitting the data extent with a fixed padding.</summary>
    sealed class Viewport
    {
        GeoBoundingBox extent;
        double scale;
        double offsetX;
        double offsetY;

        public GeoBoundingBox Extent => this.extent;
        public double PlotWidth { get; private set; }
        public double PlotHeight { get; private set; }

        public static Viewport Fit(GeoBoundingBox box, int width, int height, double padding)
        {
            // A single point, or a perfectly flat line, has zero span in one axis; give it a small
            // window so it lands in the middle of the canvas instead of dividing by zero.
            var minLat = box.MinLatitude;
            var maxLat = box.MaxLatitude;
            var minLng = box.MinLongitude;
            var maxLng = box.MaxLongitude;

            if (maxLat - minLat < 1e-9) { minLat -= 0.01; maxLat += 0.01; }
            if (maxLng - minLng < 1e-9) { minLng -= 0.01; maxLng += 0.01; }

            var padded = new GeoBoundingBox(minLat, minLng, maxLat, maxLng);

            var x0 = MercatorX(minLng);
            var x1 = MercatorX(maxLng);
            var y0 = MercatorY(maxLat);
            var y1 = MercatorY(minLat);

            var spanX = x1 - x0;
            var spanY = y1 - y0;

            var usableWidth = width - padding * 2;
            var usableHeight = height - padding * 2;

            // One scale for both axes so shapes keep their aspect ratio.
            var scale = Math.Min(usableWidth / spanX, usableHeight / spanY);

            return new Viewport
            {
                extent = padded,
                scale = scale,
                offsetX = padding + (usableWidth - spanX * scale) / 2 - x0 * scale,
                offsetY = padding + (usableHeight - spanY * scale) / 2 - y0 * scale,
                PlotWidth = spanX * scale,
                PlotHeight = spanY * scale
            };
        }

        public double X(double longitude) => MercatorX(longitude) * this.scale + this.offsetX;

        public double Y(double latitude) => MercatorY(latitude) * this.scale + this.offsetY;
    }

    static double MercatorX(double longitude) => longitude;

    static double MercatorY(double latitude)
    {
        var clamped = Math.Clamp(latitude, -MaxLatitude, MaxLatitude) * Math.PI / 180;
        // Negated so north is up once written into SVG coordinates, which grow downward.
        return -Math.Log(Math.Tan(Math.PI / 4 + clamped / 2)) * 180 / Math.PI;
    }

    static double MetresPerDegreeLongitude(double latitude)
        => 111_320 * Math.Cos(latitude * Math.PI / 180);

    /// <summary>
    /// Rounds a span to a 1/2/5 × 10ⁿ step, so gridlines and scale bars land on readable numbers.
    /// <paramref name="pixels"/> and <paramref name="minSpacing"/> cap how many divisions are drawn, so
    /// the labels never collide; omit them for a plain four-division step.
    /// </summary>
    static double NiceStep(double span, double pixels = 0, double minSpacing = 0)
    {
        if (span <= 0 || double.IsNaN(span) || double.IsInfinity(span))
            return 1;

        var divisions = 4.0;
        if (pixels > 0 && minSpacing > 0)
            divisions = Math.Clamp(pixels / minSpacing, 1, 10);

        var magnitude = Math.Pow(10, Math.Floor(Math.Log10(span / divisions)));
        var normalized = span / divisions / magnitude;

        var step = normalized switch
        {
            < 1.5 => 1,
            < 3 => 2,
            < 7 => 5,
            _ => 10
        };

        return step * magnitude;
    }

    static string F(double value) => value.ToString("0.###", CultureInfo.InvariantCulture);

    static string Format(double value) => value.ToString("0.##", CultureInfo.InvariantCulture);

    /// <summary>
    /// The SVG is emitted as raw markup, so every value that comes out of a document has to be escaped
    /// here - a document id is attacker-controlled data as far as this tool is concerned.
    /// </summary>
    static string Escape(string text) => text
        .Replace("&", "&amp;")
        .Replace("<", "&lt;")
        .Replace(">", "&gt;")
        .Replace("\"", "&quot;")
        .Replace("'", "&#39;");
}
