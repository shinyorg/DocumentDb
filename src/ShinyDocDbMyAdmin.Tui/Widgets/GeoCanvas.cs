using System.Text;
using ShinyDocDbMyAdmin.Models;
using Shiny.DocumentDb;
using XenoAtom.Terminal.UI;
using XenoAtom.Terminal.UI.Geometry;
using XenoAtom.Terminal.UI.Layout;
using XenoAtom.Terminal.UI.Rendering;
using XenoAtom.Terminal.UI.Styling;

namespace ShinyDocDbMyAdmin.Tui.Widgets;

/// <summary>
/// Draws geometries into the terminal with braille cells.
/// </summary>
/// <remarks>
/// <para>
/// The terminal answer to the web front end's <c>SvgMap</c>, and it keeps that renderer's two
/// decisions. There is <b>no basemap</b>: any tile layer means sending the coordinates of whatever is
/// in the database to a third party on every pan, which is not a reasonable default for a tool that
/// gets pointed at production. And colour is by geometry family, capped at three, because that is how
/// many categorical slots an all-pairs form can carry - each with its own mark density as well, so the
/// distinction never rests on hue alone.
/// </para>
/// <para>
/// Braille rather than half-blocks: U+2800's eight dots give 2x4 subcells per character, so a 120x30
/// pane draws at 240x120 - enough for a coastline to read as a coastline. The cost is that a cell has
/// one colour for up to eight dots, so a cell takes the colour of whichever family owns most of them,
/// and points win ties because they are the smallest marks.
/// </para>
/// <para>
/// A <see cref="Visual"/> rather than a block of markup, because the plot has to be rasterised at the
/// size it is actually given. Anything that produced text in advance would have to guess that size,
/// and a map drawn for the wrong pane is either cropped or postage-stamped.
/// </para>
/// </remarks>
public sealed class GeoCanvas : Visual
{
    /// <summary>Web Mercator is undefined at the poles; clamp to the standard cutoff, as SvgMap does.</summary>
    const double MaxLatitude = 85.05112878;

    const int DotsPerCellX = 2;
    const int DotsPerCellY = 4;

    /// <summary>Braille dot numbering is column-major and out of order; this is the bit for (x, y).</summary>
    static readonly int[,] DotBits =
    {
        { 0x01, 0x02, 0x04, 0x40 },
        { 0x08, 0x10, 0x20, 0x80 }
    };

    readonly State<int> revision = new(0);

    GeometrySet? set;
    GeoBoundingBox? focus;

    /// <summary>The set to draw, and optionally the box to draw it over.</summary>
    /// <remarks>
    /// <paramref name="focus"/> overrides the fitted extent - "zoom to this feature", which matters
    /// because a fit over two distant clusters honestly renders both as specks.
    /// </remarks>
    public void Draw(GeometrySet geometry, GeoBoundingBox? focus = null)
    {
        this.set = geometry;
        this.focus = focus;
        this.revision.Value++;
    }

    public void Clear()
    {
        this.set = null;
        this.focus = null;
        this.revision.Value++;
    }

    public static string FamilyLabel(GeoFamily family) => family switch
    {
        GeoFamily.Point => "Points",
        GeoFamily.Line => "Lines",
        _ => "Areas"
    };

    /// <summary>
    /// Magenta, cyan, green - three hues that stay distinguishable under the common colour-vision
    /// deficiencies and hold contrast on both the light and the dark terminal themes.
    /// </summary>
    public static string FamilyColour(GeoFamily family) => family switch
    {
        GeoFamily.Point => "magenta",
        GeoFamily.Line => "cyan",
        _ => "green"
    };

    static Color FamilyColor(GeoFamily family, Theme theme) => family switch
    {
        GeoFamily.Point => theme.Accent ?? Colors.Magenta,
        GeoFamily.Line => theme.Primary ?? Colors.Cyan,
        _ => theme.Success ?? Colors.Green
    };

    // ── Layout ──────────────────────────────────────────────────────────

    protected override SizeHints MeasureCore(in LayoutConstraints constraints)
    {
        // Asks for a readable map and grows into whatever is left over. Asking for the full available
        // height instead would be taken at its word by a stack, and the list underneath would be
        // measured out of the pane entirely.
        var width = constraints.IsWidthBounded ? constraints.MaxWidth : 96;
        var natural = new Size(width, Math.Min(24, constraints.IsHeightBounded ? constraints.MaxHeight : 24));
        var max = new Size(width, constraints.IsHeightBounded ? constraints.MaxHeight : int.MaxValue);

        return SizeHints.Flex(new Size(20, 6), natural, max, growX: 1, growY: 1, shrinkX: 1, shrinkY: 1);
    }

    protected override void RenderOverride(CellBuffer buffer)
    {
        _ = this.revision.Value;

        var bounds = this.Bounds;
        if (bounds.Width <= 0 || bounds.Height <= 0)
            return;

        var theme = this.GetTheme();

        if (this.set is not { IsEmpty: false } geometry || (this.focus ?? geometry.Extent) is not { } extent)
        {
            var message = "No geometry to draw.";
            var style = Style.None.WithForeground(theme.Muted ?? Colors.Gray);
            buffer.WriteText(
                bounds.X + Math.Max(0, (bounds.Width - message.Length) / 2),
                bounds.Y + bounds.Height / 2,
                message,
                style);

            return;
        }

        var dotsWide = bounds.Width * DotsPerCellX;
        var dotsHigh = bounds.Height * DotsPerCellY;
        var view = new Viewport(extent, dotsWide, dotsHigh);

        // One bitmap per family, so a cell can be coloured by whoever owns most of it rather than by
        // whichever feature happened to be drawn last.
        var layers = new byte[3][];
        for (var i = 0; i < layers.Length; i++)
            layers[i] = new byte[dotsWide * dotsHigh];

        // Areas first, then lines, then points: the smallest marks stay on top, as on the SVG map.
        foreach (var family in (GeoFamily[])[GeoFamily.Area, GeoFamily.Line, GeoFamily.Point])
        {
            var layer = layers[(int)family];
            foreach (var feature in geometry.Features.Where(f => f.Family == family))
                Plot(layer, feature.Geometry, view, dotsWide, dotsHigh);
        }

        var styles = new[]
        {
            Style.None.WithForeground(FamilyColor(GeoFamily.Point, theme)),
            Style.None.WithForeground(FamilyColor(GeoFamily.Line, theme)),
            Style.None.WithForeground(FamilyColor(GeoFamily.Area, theme))
        };

        for (var cy = 0; cy < bounds.Height; cy++)
        {
            for (var cx = 0; cx < bounds.Width; cx++)
            {
                var pattern = 0;
                Span<int> owned = [0, 0, 0];

                for (var dx = 0; dx < DotsPerCellX; dx++)
                {
                    for (var dy = 0; dy < DotsPerCellY; dy++)
                    {
                        var index = (cy * DotsPerCellY + dy) * dotsWide + cx * DotsPerCellX + dx;

                        for (var family = 0; family < layers.Length; family++)
                        {
                            if (layers[family][index] == 0)
                                continue;

                            pattern |= DotBits[dx, dy];
                            owned[family]++;
                        }
                    }
                }

                if (pattern == 0)
                    continue;

                // A tie goes to the later family - points over lines over areas - which is the same
                // precedence the draw order establishes.
                var winner = 0;
                for (var family = 1; family < owned.Length; family++)
                {
                    if (owned[family] >= owned[winner] && owned[family] > 0)
                        winner = family;
                }

                buffer.SetCell(bounds.X + cx, bounds.Y + cy, new System.Text.Rune(0x2800 + pattern), styles[winner]);
            }
        }
    }

    // ── Rasterising ─────────────────────────────────────────────────────

    static void Plot(byte[] layer, Geometry geometry, Viewport view, int width, int height)
    {
        switch (geometry)
        {
            case GeoPolygon polygon:
                Outline(layer, polygon.ExteriorRing, view, width, height, closed: true);
                foreach (var hole in polygon.InteriorRings)
                    Outline(layer, hole, view, width, height, closed: true);
                break;

            case GeoMultiPolygon multi:
                foreach (var member in multi.Polygons)
                    Plot(layer, member, view, width, height);
                break;

            case GeoLineString line:
                Outline(layer, line.Coordinates, view, width, height, closed: false);
                break;

            case GeoMultiLineString multiLine:
                foreach (var member in multiLine.LineStrings)
                    Plot(layer, member, view, width, height);
                break;

            case GeoMultiPoint multiPoint:
                foreach (var point in multiPoint.Points)
                    Dot(layer, view.X(point.Longitude), view.Y(point.Latitude), width, height);
                break;

            case GeoGeometryCollection collection:
                foreach (var member in collection.Geometries)
                    Plot(layer, member, view, width, height);
                break;

            default:
                // A bare point arrives as the lightweight struct wrapped in a point geometry; its
                // envelope is the coordinate itself, which is all a dot needs.
                var envelope = geometry.GetEnvelope();
                Dot(layer, view.X(envelope.MinLongitude), view.Y(envelope.MinLatitude), width, height);
                break;
        }
    }

    /// <summary>
    /// Polygons are drawn as outlines rather than filled. A fill at braille resolution turns a county
    /// into a solid block that hides every other shape underneath it, and the boundary is the part
    /// anyone is reading anyway.
    /// </summary>
    static void Outline(byte[] layer, IReadOnlyList<GeoPoint> points, Viewport view, int width, int height, bool closed)
    {
        if (points.Count == 0)
            return;

        if (points.Count == 1)
        {
            Dot(layer, view.X(points[0].Longitude), view.Y(points[0].Latitude), width, height);
            return;
        }

        for (var i = 0; i < points.Count - 1; i++)
            Line(layer, view, points[i], points[i + 1], width, height);

        if (closed && points.Count > 2)
            Line(layer, view, points[^1], points[0], width, height);
    }

    static void Line(byte[] layer, Viewport view, GeoPoint a, GeoPoint b, int width, int height)
    {
        var x0 = (int)Math.Round(view.X(a.Longitude));
        var y0 = (int)Math.Round(view.Y(a.Latitude));
        var x1 = (int)Math.Round(view.X(b.Longitude));
        var y1 = (int)Math.Round(view.Y(b.Latitude));

        // Bresenham. Integer-only, so a long coastline costs no floating point per dot.
        var dx = Math.Abs(x1 - x0);
        var dy = -Math.Abs(y1 - y0);
        var sx = x0 < x1 ? 1 : -1;
        var sy = y0 < y1 ? 1 : -1;
        var error = dx + dy;

        while (true)
        {
            Dot(layer, x0, y0, width, height);

            if (x0 == x1 && y0 == y1)
                return;

            var doubled = 2 * error;
            if (doubled >= dy)
            {
                error += dy;
                x0 += sx;
            }

            if (doubled <= dx)
            {
                error += dx;
                y0 += sy;
            }
        }
    }

    static void Dot(byte[] layer, double x, double y, int width, int height)
        => Dot(layer, (int)Math.Round(x), (int)Math.Round(y), width, height);

    static void Dot(byte[] layer, int x, int y, int width, int height)
    {
        if (x >= 0 && x < width && y >= 0 && y < height)
            layer[y * width + x] = 1;
    }

    /// <summary>Maps longitude/latitude onto the dot grid, in Web Mercator like the SVG map.</summary>
    sealed class Viewport
    {
        readonly double minX, minY, spanX, spanY;
        readonly int width, height;

        public Viewport(GeoBoundingBox box, int width, int height)
        {
            this.width = width;
            this.height = height;

            this.minX = box.MinLongitude;
            var maxX = box.MaxLongitude;
            this.minY = Mercator(box.MinLatitude);
            var maxY = Mercator(box.MaxLatitude);

            // A single point, or a set all on one meridian, has no span to divide by. Give it an
            // arbitrary one so the shape lands in the middle rather than producing NaN.
            this.spanX = maxX - this.minX;
            if (this.spanX <= 0)
            {
                this.spanX = 0.001;
                this.minX -= 0.0005;
            }

            this.spanY = maxY - this.minY;
            if (this.spanY <= 0)
            {
                this.spanY = 0.001;
                this.minY -= 0.0005;
            }
        }

        public double X(double longitude) => (longitude - this.minX) / this.spanX * (this.width - 1);

        public double Y(double latitude) => (1 - (Mercator(latitude) - this.minY) / this.spanY) * (this.height - 1);

        static double Mercator(double latitude)
        {
            var clamped = Math.Clamp(latitude, -MaxLatitude, MaxLatitude) * Math.PI / 180;
            return Math.Log(Math.Tan(Math.PI / 4 + clamped / 2));
        }
    }
}
