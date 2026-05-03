using System.Text.Json.Serialization;
using Shiny.DocumentDb.Internal;

namespace Shiny.DocumentDb;

/// <summary>
/// Represents a geographic point using WGS84 coordinates.
/// Serializes as GeoJSON: {"type":"Point","coordinates":[longitude,latitude]}
/// </summary>
[JsonConverter(typeof(GeoPointJsonConverter))]
public readonly record struct GeoPoint(double Latitude, double Longitude);
