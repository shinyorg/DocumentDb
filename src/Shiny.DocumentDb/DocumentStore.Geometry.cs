using System.Linq.Expressions;
using Shiny.DocumentDb.Internal;
using Shiny.DocumentDb.Internal.Query;
using Shiny.DocumentDb.Internal.Spatial;

namespace Shiny.DocumentDb;

public partial class DocumentStore
{
    static readonly GeoBoundingBox WorldBox = new(-90, -180, 90, 180);

    public Task<IReadOnlyList<SpatialResult<T>>> GeoIntersects<T>(Geometry geometry, Geometry? orderByDistanceFrom = null, Expression<Func<T, bool>>? filter = null, CancellationToken ct = default) where T : class
        => this.tracker.Track("geo_intersects", typeof(T).Name, () => this.GeometryQuery(geometry, stored => SpatialPredicates.Intersects(stored, geometry), geometry.GetEnvelope(), orderByDistanceFrom, filter, ct), r => r.Count);

    public Task<IReadOnlyList<SpatialResult<T>>> GeoContainedBy<T>(Geometry container, Geometry? orderByDistanceFrom = null, Expression<Func<T, bool>>? filter = null, CancellationToken ct = default) where T : class
        => this.tracker.Track("geo_contained_by", typeof(T).Name, () => this.GeometryQuery(container, stored => SpatialPredicates.Within(stored, container), container.GetEnvelope(), orderByDistanceFrom, filter, ct), r => r.Count);

    public Task<IReadOnlyList<SpatialResult<T>>> GeoContains<T>(Geometry geometry, Geometry? orderByDistanceFrom = null, Expression<Func<T, bool>>? filter = null, CancellationToken ct = default) where T : class
        => this.tracker.Track("geo_contains", typeof(T).Name, () => this.GeometryQuery(geometry, stored => SpatialPredicates.Contains(stored, geometry), geometry.GetEnvelope(), orderByDistanceFrom, filter, ct), r => r.Count);

    public Task<IReadOnlyList<SpatialResult<T>>> GeoTouches<T>(Geometry geometry, Geometry? orderByDistanceFrom = null, Expression<Func<T, bool>>? filter = null, CancellationToken ct = default) where T : class
        => this.tracker.Track("geo_touches", typeof(T).Name, () => this.GeometryQuery(geometry, stored => SpatialPredicates.Touches(stored, geometry), geometry.GetEnvelope(), orderByDistanceFrom, filter, ct), r => r.Count);

    public Task<IReadOnlyList<SpatialResult<T>>> GeoCrosses<T>(Geometry geometry, Geometry? orderByDistanceFrom = null, Expression<Func<T, bool>>? filter = null, CancellationToken ct = default) where T : class
        => this.tracker.Track("geo_crosses", typeof(T).Name, () => this.GeometryQuery(geometry, stored => SpatialPredicates.Crosses(stored, geometry), geometry.GetEnvelope(), orderByDistanceFrom, filter, ct), r => r.Count);

    public Task<IReadOnlyList<SpatialResult<T>>> GeoOverlaps<T>(Geometry geometry, Geometry? orderByDistanceFrom = null, Expression<Func<T, bool>>? filter = null, CancellationToken ct = default) where T : class
        => this.tracker.Track("geo_overlaps", typeof(T).Name, () => this.GeometryQuery(geometry, stored => SpatialPredicates.Overlaps(stored, geometry), geometry.GetEnvelope(), orderByDistanceFrom, filter, ct), r => r.Count);

    public Task<IReadOnlyList<SpatialResult<T>>> GeoEquals<T>(Geometry geometry, Geometry? orderByDistanceFrom = null, Expression<Func<T, bool>>? filter = null, CancellationToken ct = default) where T : class
        => this.tracker.Track("geo_equals", typeof(T).Name, () => this.GeometryQuery(geometry, stored => SpatialPredicates.GeometryEquals(stored, geometry), geometry.GetEnvelope(), orderByDistanceFrom, filter, ct), r => r.Count);

    public Task<IReadOnlyList<SpatialResult<T>>> GeoCovers<T>(Geometry geometry, Geometry? orderByDistanceFrom = null, Expression<Func<T, bool>>? filter = null, CancellationToken ct = default) where T : class
        => this.tracker.Track("geo_covers", typeof(T).Name, () => this.GeometryQuery(geometry, stored => SpatialPredicates.Covers(stored, geometry), geometry.GetEnvelope(), orderByDistanceFrom, filter, ct), r => r.Count);

    public Task<IReadOnlyList<SpatialResult<T>>> GeoCoveredBy<T>(Geometry geometry, Geometry? orderByDistanceFrom = null, Expression<Func<T, bool>>? filter = null, CancellationToken ct = default) where T : class
        => this.tracker.Track("geo_covered_by", typeof(T).Name, () => this.GeometryQuery(geometry, stored => SpatialPredicates.CoveredBy(stored, geometry), geometry.GetEnvelope(), orderByDistanceFrom, filter, ct), r => r.Count);

    // Disjoint is anti-selective — the bbox can't prune, so scan every spatial-indexed row of the type.
    public Task<IReadOnlyList<SpatialResult<T>>> GeoDisjoint<T>(Geometry geometry, Geometry? orderByDistanceFrom = null, Expression<Func<T, bool>>? filter = null, CancellationToken ct = default) where T : class
        => this.tracker.Track("geo_disjoint", typeof(T).Name, () => this.GeometryQuery(geometry, stored => SpatialPredicates.Disjoint(stored, geometry), WorldBox, orderByDistanceFrom, filter, ct), r => r.Count);

    public Task<IReadOnlyList<SpatialResult<T>>> GeoWithinDistance<T>(Geometry geometry, double meters, Geometry? orderByDistanceFrom = null, Expression<Func<T, bool>>? filter = null, CancellationToken ct = default) where T : class
        => this.tracker.Track("geo_within_distance", typeof(T).Name, () => this.GeometryQuery(geometry, stored => GeoDistance.Between(stored, geometry) <= meters, geometry.GetEnvelope().ExpandByMeters(meters), orderByDistanceFrom, filter, ct), r => r.Count);

    Task<IReadOnlyList<SpatialResult<T>>> GeometryQuery<T>(
        Geometry query,
        Func<Geometry, bool> refine,
        GeoBoundingBox candidateBox,
        Geometry? orderByDistanceFrom,
        Expression<Func<T, bool>>? filter,
        CancellationToken ct) where T : class
    {
        if (!this.provider.SupportsSpatial)
            throw new NotSupportedException("Spatial queries are not supported by this provider.");

        var typeInfo = FindTypeInfo<T>(null);
        var tableName = this.ResolveTableName<T>();
        var typeName = this.ResolveTypeName<T>();
        var mapping = this.options.ResolveSpatialMapping(typeof(T))
            ?? throw new InvalidOperationException($"No spatial mapping registered for '{typeof(T).Name}'. Call MapSpatialProperty at startup.");

        return this.ExecuteAsync(tableName, async session =>
        {
            string? additionalWhere = null;
            Dictionary<string, object?>? filterParams = null;
            if (filter != null)
            {
                var translated = JsonExpressionVisitor.Translate(filter, typeInfo!, this.provider);
                additionalWhere = translated.WhereClause;
                filterParams = translated.Parameters;
            }

            var sql = this.provider.BuildSpatialBoundingBoxQuerySql(tableName, additionalWhere)!;
            await using var cmd = session.CreateCommand();
            cmd.CommandText = sql + ";";
            AddParameter(cmd, "@typeName", typeName);
            AddParameter(cmd, "@minLat", candidateBox.MinLatitude);
            AddParameter(cmd, "@maxLat", candidateBox.MaxLatitude);
            AddParameter(cmd, "@minLng", candidateBox.MinLongitude);
            AddParameter(cmd, "@maxLng", candidateBox.MaxLongitude);
            if (filterParams != null)
            {
                foreach (var kvp in filterParams)
                    AddParameter(cmd, kvp.Key, kvp.Value ?? DBNull.Value);
            }

            this.Log(cmd.CommandText);
            var candidates = await ReadListAsync(cmd, json => DeserializeDocument(json, typeInfo, this.jsonOptions)!, ct).ConfigureAwait(false);

            var results = new List<SpatialResult<T>>();
            foreach (var doc in candidates)
            {
                var stored = mapping.ResolveGeometry(doc!);
                if (stored is null || !refine(stored))
                    continue;

                var distance = orderByDistanceFrom is null ? 0.0 : GeoDistance.Between(stored, orderByDistanceFrom);
                results.Add(new SpatialResult<T> { Document = (T)doc!, DistanceMeters = distance });
            }

            if (orderByDistanceFrom is not null)
                results.Sort((a, b) => a.DistanceMeters.CompareTo(b.DistanceMeters));

            return (IReadOnlyList<SpatialResult<T>>)results;
        }, ct);
    }
}
