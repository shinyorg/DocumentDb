using System.Linq.Expressions;
using System.Text;
using Microsoft.Azure.Cosmos;
using Shiny.DocumentDb.Internal.Spatial;

namespace Shiny.DocumentDb.CosmosDb;

public partial class CosmosDbDocumentStore
{
    // Native where possible; Cosmos lacks ST_TOUCHES/CROSSES/OVERLAPS/EQUALS/CONTAINS, so those fetch the
    // ST_INTERSECTS candidate set and refine with the C# relate engine.

    public Task<IReadOnlyList<SpatialResult<T>>> GeoIntersects<T>(Geometry geometry, Geometry? orderByDistanceFrom = null, Expression<Func<T, bool>>? filter = null, CancellationToken ct = default) where T : class
        => this.Tracker.Track("geo_intersects", typeof(T).Name, () => this.GeoQueryAsync<T>(g => $"ST_INTERSECTS(c.data.{g}, {Gj(geometry)})", null, orderByDistanceFrom, filter, ct), r => r.Count);

    public Task<IReadOnlyList<SpatialResult<T>>> GeoContainedBy<T>(Geometry container, Geometry? orderByDistanceFrom = null, Expression<Func<T, bool>>? filter = null, CancellationToken ct = default) where T : class
        => this.Tracker.Track("geo_contained_by", typeof(T).Name, () => this.GeoQueryAsync<T>(g => $"ST_WITHIN(c.data.{g}, {Gj(container)})", null, orderByDistanceFrom, filter, ct), r => r.Count);

    public Task<IReadOnlyList<SpatialResult<T>>> GeoDisjoint<T>(Geometry geometry, Geometry? orderByDistanceFrom = null, Expression<Func<T, bool>>? filter = null, CancellationToken ct = default) where T : class
        => this.Tracker.Track("geo_disjoint", typeof(T).Name, () => this.GeoQueryAsync<T>(g => $"NOT ST_INTERSECTS(c.data.{g}, {Gj(geometry)})", null, orderByDistanceFrom, filter, ct), r => r.Count);

    public Task<IReadOnlyList<SpatialResult<T>>> GeoWithinDistance<T>(Geometry geometry, double meters, Geometry? orderByDistanceFrom = null, Expression<Func<T, bool>>? filter = null, CancellationToken ct = default) where T : class
        => this.Tracker.Track("geo_within_distance", typeof(T).Name, () => this.GeoQueryAsync<T>(g => $"ST_DISTANCE(c.data.{g}, {Gj(geometry)}) <= {meters.ToString(System.Globalization.CultureInfo.InvariantCulture)}", null, orderByDistanceFrom, filter, ct), r => r.Count);

    // Refine-based (candidate = ST_INTERSECTS, C# relate narrows). "stored" is the document's geometry.
    public Task<IReadOnlyList<SpatialResult<T>>> GeoContains<T>(Geometry geometry, Geometry? orderByDistanceFrom = null, Expression<Func<T, bool>>? filter = null, CancellationToken ct = default) where T : class
        => this.Tracker.Track("geo_contains", typeof(T).Name, () => this.GeoQueryAsync<T>(g => $"ST_INTERSECTS(c.data.{g}, {Gj(geometry)})", stored => SpatialPredicates.Contains(stored, geometry), orderByDistanceFrom, filter, ct), r => r.Count);

    public Task<IReadOnlyList<SpatialResult<T>>> GeoTouches<T>(Geometry geometry, Geometry? orderByDistanceFrom = null, Expression<Func<T, bool>>? filter = null, CancellationToken ct = default) where T : class
        => this.Tracker.Track("geo_touches", typeof(T).Name, () => this.GeoQueryAsync<T>(g => $"ST_INTERSECTS(c.data.{g}, {Gj(geometry)})", stored => SpatialPredicates.Touches(stored, geometry), orderByDistanceFrom, filter, ct), r => r.Count);

    public Task<IReadOnlyList<SpatialResult<T>>> GeoCrosses<T>(Geometry geometry, Geometry? orderByDistanceFrom = null, Expression<Func<T, bool>>? filter = null, CancellationToken ct = default) where T : class
        => this.Tracker.Track("geo_crosses", typeof(T).Name, () => this.GeoQueryAsync<T>(g => $"ST_INTERSECTS(c.data.{g}, {Gj(geometry)})", stored => SpatialPredicates.Crosses(stored, geometry), orderByDistanceFrom, filter, ct), r => r.Count);

    public Task<IReadOnlyList<SpatialResult<T>>> GeoOverlaps<T>(Geometry geometry, Geometry? orderByDistanceFrom = null, Expression<Func<T, bool>>? filter = null, CancellationToken ct = default) where T : class
        => this.Tracker.Track("geo_overlaps", typeof(T).Name, () => this.GeoQueryAsync<T>(g => $"ST_INTERSECTS(c.data.{g}, {Gj(geometry)})", stored => SpatialPredicates.Overlaps(stored, geometry), orderByDistanceFrom, filter, ct), r => r.Count);

    public Task<IReadOnlyList<SpatialResult<T>>> GeoEquals<T>(Geometry geometry, Geometry? orderByDistanceFrom = null, Expression<Func<T, bool>>? filter = null, CancellationToken ct = default) where T : class
        => this.Tracker.Track("geo_equals", typeof(T).Name, () => this.GeoQueryAsync<T>(g => $"ST_INTERSECTS(c.data.{g}, {Gj(geometry)})", stored => SpatialPredicates.GeometryEquals(stored, geometry), orderByDistanceFrom, filter, ct), r => r.Count);

    public Task<IReadOnlyList<SpatialResult<T>>> GeoCovers<T>(Geometry geometry, Geometry? orderByDistanceFrom = null, Expression<Func<T, bool>>? filter = null, CancellationToken ct = default) where T : class
        => this.Tracker.Track("geo_covers", typeof(T).Name, () => this.GeoQueryAsync<T>(g => $"ST_INTERSECTS(c.data.{g}, {Gj(geometry)})", stored => SpatialPredicates.Covers(stored, geometry), orderByDistanceFrom, filter, ct), r => r.Count);

    public Task<IReadOnlyList<SpatialResult<T>>> GeoCoveredBy<T>(Geometry geometry, Geometry? orderByDistanceFrom = null, Expression<Func<T, bool>>? filter = null, CancellationToken ct = default) where T : class
        => this.Tracker.Track("geo_covered_by", typeof(T).Name, () => this.GeoQueryAsync<T>(g => $"ST_INTERSECTS(c.data.{g}, {Gj(geometry)})", stored => SpatialPredicates.CoveredBy(stored, geometry), orderByDistanceFrom, filter, ct), r => r.Count);

    string Gj(Geometry g) => SpatialJson.ToGeoJson(g);

    async Task<IReadOnlyList<SpatialResult<T>>> GeoQueryAsync<T>(
        Func<string, string> spatialWhere,
        Func<Geometry, bool>? refine,
        Geometry? orderRef,
        Expression<Func<T, bool>>? filter,
        CancellationToken ct) where T : class
    {
        var mapping = this.options.ResolveSpatialMapping(typeof(T))
            ?? throw new NotSupportedException($"No spatial property mapped for type '{typeof(T).Name}'. Call MapSpatialProperty<{typeof(T).Name}>() in options.");

        var typeInfo = this.FindTypeInfo<T>(null);
        var typeName = this.ResolveTypeName<T>();
        var container = await this.GetContainerAsync<T>(ct).ConfigureAwait(false);

        var sql = new StringBuilder();
        sql.Append($"SELECT VALUE c.data FROM c WHERE c.typeName = @typeName AND ({spatialWhere(mapping.JsonPath)})");

        Dictionary<string, object?>? filterParams = null;
        if (filter != null)
        {
            var translated = CosmosExpressionVisitor.Translate(filter, this.jsonOptions, typeInfo);
            sql.Append($" AND ({translated.sql})");
            filterParams = translated.parameters;
        }

        if (orderRef is not null)
            sql.Append($" ORDER BY ST_DISTANCE(c.data.{mapping.JsonPath}, {Gj(orderRef)})");

        var queryDef = new QueryDefinition(sql.ToString()).WithParameter("@typeName", typeName);
        if (filterParams != null)
        {
            foreach (var kvp in filterParams)
                queryDef.WithParameter(kvp.Key, kvp.Value);
        }

        this.Log(sql.ToString());
        var docs = await this.ExecuteRawQueryAsync(container, queryDef, typeName, typeInfo, ct).ConfigureAwait(false);

        var results = new List<SpatialResult<T>>();
        foreach (var doc in docs)
        {
            var stored = mapping.ResolveGeometry(doc!);
            if (stored is null || (refine != null && !refine(stored)))
                continue;

            var distance = orderRef is null ? 0.0 : GeoDistance.Between(stored, orderRef);
            results.Add(new SpatialResult<T> { Document = doc!, DistanceMeters = distance });
        }
        return results;
    }
}
