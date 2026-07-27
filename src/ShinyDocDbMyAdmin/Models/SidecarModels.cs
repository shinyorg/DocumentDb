using Shiny.DocumentDb;

namespace ShinyDocDbMyAdmin.Models;

/// <summary>A row in a <c>{table}_blobs</c> sidecar, without its payload.</summary>
public sealed record BlobInfo(
    string DocumentId,
    string BlobKey,
    string? FileName,
    string? ContentType,
    long Length,
    string? Hash,
    DateTimeOffset? CreatedAt,
    DateTimeOffset? UpdatedAt
)
{
    public BlobKind Kind => Classify(this.ContentType, this.FileName);

    /// <summary>
    /// What the tool is willing to do with a payload. Only a short allowlist of raster types is
    /// previewed inline: the bytes come from whoever wrote the document, and serving arbitrary
    /// user-controlled content from our own origin is how an admin tool becomes an XSS vector.
    /// SVG is deliberately excluded - it is a document format, not just an image.
    /// </summary>
    static BlobKind Classify(string? contentType, string? fileName)
    {
        var type = contentType?.Trim().ToLowerInvariant() ?? "";
        var semicolon = type.IndexOf(';');
        if (semicolon > 0)
            type = type[..semicolon].Trim();

        if (InlineImageTypes.Contains(type))
            return BlobKind.Image;

        if (type.StartsWith("text/") || TextishTypes.Contains(type))
            return BlobKind.Text;

        // Nothing declared: fall back to the file extension, and only for text.
        if (type.Length == 0 && fileName is not null)
        {
            var extension = Path.GetExtension(fileName).ToLowerInvariant();
            if (TextExtensions.Contains(extension))
                return BlobKind.Text;
        }

        return BlobKind.Binary;
    }

    static readonly HashSet<string> InlineImageTypes = new(StringComparer.Ordinal)
    {
        "image/png", "image/jpeg", "image/gif", "image/webp", "image/avif", "image/bmp"
    };

    static readonly HashSet<string> TextishTypes = new(StringComparer.Ordinal)
    {
        "application/json", "application/xml", "application/x-ndjson", "application/javascript", "application/sql"
    };

    static readonly HashSet<string> TextExtensions = new(StringComparer.Ordinal)
    {
        ".txt", ".json", ".ndjson", ".xml", ".csv", ".md", ".log", ".yaml", ".yml", ".sql"
    };
}

public enum BlobKind
{
    /// <summary>A raster image on the inline allowlist - safe to show in an <c>img</c> tag.</summary>
    Image,

    /// <summary>Decoded and shown as escaped text, server-side. Never served to the browser as a document.</summary>
    Text,

    /// <summary>Download only.</summary>
    Binary
}

/// <summary>A payload plus enough metadata to serve or render it.</summary>
public sealed record BlobPayload(BlobInfo Info, byte[] Data);

/// <summary>One geometry found inside a document body, at a known JSON path.</summary>
public sealed record GeoFeature(string DocumentId, string Path, Geometry Geometry)
{
    public GeoFamily Family => Classify(this.Geometry.GeometryType);

    public static GeoFamily Classify(GeoGeometryType type) => type switch
    {
        GeoGeometryType.Point or GeoGeometryType.MultiPoint => GeoFamily.Point,
        GeoGeometryType.LineString or GeoGeometryType.MultiLineString => GeoFamily.Line,
        GeoGeometryType.Polygon or GeoGeometryType.MultiPolygon => GeoFamily.Area,
        // A collection renders as its members, each under its own family; the wrapper itself is drawn
        // as an area only if it somehow reaches the renderer intact.
        _ => GeoFamily.Area
    };
}

/// <summary>
/// The three visual families geometry collapses into. Kept to three deliberately: a map is an
/// all-pairs colour form, where only the first three categorical slots clear the contrast and
/// colour-vision gates. Each family also has its own mark shape, so colour never carries it alone.
/// </summary>
public enum GeoFamily
{
    Point,
    Line,
    Area
}

/// <summary>The geometries gathered for one view, with the extent they occupy.</summary>
public sealed record GeometrySet(
    IReadOnlyList<GeoFeature> Features,
    GeoBoundingBox? Extent,
    int DocumentsScanned,
    bool Truncated
)
{
    public bool IsEmpty => this.Features.Count == 0;

    public IReadOnlyList<GeoFamily> FamiliesPresent =>
        [.. this.Features.Select(f => f.Family).Distinct().OrderBy(f => (int)f)];
}
