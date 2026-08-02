using System.Text.Json;
using System.Text.Json.Nodes;
using ShinyDocDbMyAdmin.Models;
using Shiny.DocumentDb;

namespace ShinyDocDbMyAdmin.Services;

/// <summary>
/// A field that holds geometry, with the kind most of its values were.
/// </summary>
/// <param name="Kind">GeoJSON type name - <c>Point</c>, <c>Polygon</c>, <c>LineString</c>, …</param>
/// <param name="Occurrences">How many sampled documents had geometry here.</param>
public sealed record GeometryPath(string Path, string Kind, int Occurrences)
{
    /// <summary>
    /// How much a map has to say about this field, used to pick which one to open on.
    /// </summary>
    /// <remarks>
    /// A scatter of dots is the least a map can tell you, and opening on it hides that the type also
    /// carries service areas or routes - a field of points reads as "this data has no shapes".
    /// Ranking areas over lines over points means the first thing drawn is the most informative one.
    /// </remarks>
    public int Rank => this.Kind switch
    {
        "Polygon" or "MultiPolygon" => 3,
        "LineString" or "MultiLineString" => 2,
        "GeometryCollection" => 2,
        _ => 1
    };

    /// <summary>What the picker shows: the field, and what is actually in it.</summary>
    public string Label => $"{this.Path} — {this.Kind}";
}

public sealed partial class DocumentAdminService
{
    /// <summary>How many documents a geometry view will read before it stops and says so.</summary>
    public const int GeometryScanLimit = 500;

    static readonly HashSet<string> GeoJsonTypes = new(StringComparer.OrdinalIgnoreCase)
    {
        "Point", "LineString", "Polygon", "MultiPoint", "MultiLineString", "MultiPolygon", "GeometryCollection"
    };

    /// <summary>
    /// The JSON paths at which this type stores geometry, found by sampling documents and looking for
    /// GeoJSON-shaped objects.
    /// </summary>
    /// <remarks>
    /// Geometry is read from the document body, not from the <c>{table}_spatial</c> sidecar. The sidecar
    /// holds only bounding boxes for R*Tree / index pruning, and its shape differs per provider; the
    /// GeoJSON in <c>Data</c> is the actual value and is identical everywhere. So this works on every
    /// provider, including the ones with no spatial support at all.
    /// </remarks>
    public async Task<IReadOnlyList<GeometryPath>> FindGeometryPaths(
        string profileId,
        string table,
        string typeName,
        CancellationToken ct = default)
    {
        var bodies = await this.SampleBodies(profileId, table, typeName, SchemaSampleSize, ct);
        var kinds = new Dictionary<string, Dictionary<string, int>>(StringComparer.Ordinal);
        var order = new List<string>();

        foreach (var body in bodies)
        {
            if (Parse(body) is not JsonObject obj)
                continue;

            foreach (var (path, kind) in ScanForGeometry(obj, prefix: ""))
            {
                if (!kinds.TryGetValue(path, out var seen))
                {
                    kinds[path] = seen = new Dictionary<string, int>(StringComparer.Ordinal);
                    order.Add(path);
                }

                seen[kind] = seen.GetValueOrDefault(kind) + 1;
            }
        }

        return
        [
            .. order.Select(path =>
            {
                var seen = kinds[path];
                var dominant = seen.OrderByDescending(k => k.Value).First();
                return new GeometryPath(path, dominant.Key, seen.Values.Sum());
            })
        ];
    }

    /// <summary>
    /// Loads the geometries at one path across the documents matching a browse query, parsed with the
    /// library's own <c>GeometryJsonConverter</c> - so the tool inherits the real OGC model, and with it
    /// Centroid / Area / Length / IsValid, rather than reimplementing any of it.
    /// </summary>
    public async Task<GeometrySet> LoadGeometry(
        string profileId,
        string table,
        string typeName,
        string path,
        BrowseQuery query,
        int limit = GeometryScanLimit,
        CancellationToken ct = default)
    {
        var features = new List<GeoFeature>();
        var scanned = 0;
        var truncated = false;

        await foreach (var row in this.StreamAll(profileId, table, typeName, query, ct))
        {
            if (scanned >= limit)
            {
                truncated = true;
                break;
            }

            scanned++;
            if (row.Read(path) is not { } node)
                continue;

            if (TryReadGeometry(node, out var geometry) && !geometry.IsEmpty)
                features.Add(new GeoFeature(row.Id, path, geometry));
        }

        return new GeometrySet(features, Extent(features), scanned, truncated);
    }

    /// <summary>
    /// The combined envelope of a set of geometries - the same box the spatial sidecar would hold for
    /// them, computed from the values themselves so it is available on every provider.
    /// </summary>
    public static GeoBoundingBox? Extent(IReadOnlyList<GeoFeature> features)
    {
        if (features.Count == 0)
            return null;

        double minLat = double.MaxValue, minLng = double.MaxValue;
        double maxLat = double.MinValue, maxLng = double.MinValue;

        foreach (var feature in features)
        {
            var box = feature.Geometry.GetEnvelope();
            minLat = Math.Min(minLat, box.MinLatitude);
            minLng = Math.Min(minLng, box.MinLongitude);
            maxLat = Math.Max(maxLat, box.MaxLatitude);
            maxLng = Math.Max(maxLng, box.MaxLongitude);
        }

        return new GeoBoundingBox(minLat, minLng, maxLat, maxLng);
    }

    static bool TryReadGeometry(JsonNode node, out Geometry geometry)
    {
        geometry = default!;
        if (node is not JsonObject obj || !LooksLikeGeoJson(obj))
            return false;

        try
        {
            // Geometry carries [JsonConverter(typeof(GeometryJsonConverter))], so this is the library's
            // own GeoJSON reader - not a second, subtly different parser living in this tool.
            var parsed = node.Deserialize<Geometry>();
            if (parsed is null)
                return false;

            geometry = parsed;
            return true;
        }
        catch (JsonException)
        {
            // A field that merely looks GeoJSON-shaped but is not; skip it rather than failing the view.
            return false;
        }
        catch (ArgumentException)
        {
            // Coordinate counts that violate the OGC minimums (a 1-point LineString, an unclosed ring).
            return false;
        }
    }

    static bool LooksLikeGeoJson(JsonObject obj)
    {
        if (!obj.TryGetPropertyValue("type", out var type) || type is not JsonValue value)
            return false;

        if (value.GetValueKind() != JsonValueKind.String || !GeoJsonTypes.Contains(value.GetValue<string>()))
            return false;

        return obj.ContainsKey("coordinates") || obj.ContainsKey("geometries");
    }

    /// <summary>Walks a document body for GeoJSON-shaped objects, returning their dotted paths and kinds.</summary>
    static IEnumerable<(string Path, string Kind)> ScanForGeometry(JsonObject obj, string prefix, int depth = 0)
    {
        // Three levels covers the realistic shapes (Location, address.point, route.legs[0] is an array
        // and handled below) without walking a large document exhaustively.
        if (depth > 3)
            yield break;

        foreach (var (key, value) in obj)
        {
            var path = prefix.Length == 0 ? key : $"{prefix}.{key}";

            switch (value)
            {
                case JsonObject nested when LooksLikeGeoJson(nested):
                    yield return (path, nested["type"]!.GetValue<string>());
                    break;

                case JsonObject nested:
                    foreach (var found in ScanForGeometry(nested, path, depth + 1))
                        yield return found;
                    break;
            }
        }
    }

    static JsonNode? Parse(string json)
    {
        try
        {
            return JsonNode.Parse(json);
        }
        catch (JsonException)
        {
            return null;
        }
    }
}
