using System.Collections.Concurrent;
using System.Linq.Expressions;
using System.Text.Json;
using MongoDB.Bson;
using MongoDB.Driver;
using Shiny.DocumentDb.Internal;
using Shiny.DocumentDb.Internal.Query;
using Shiny.DocumentDb.Internal.Spatial;

namespace Shiny.DocumentDb.MongoDb;

public partial class MongoDbDocumentStore
{
    const double GeoEarthRadiusMeters = 6_378_137.0;
    readonly ConcurrentDictionary<string, byte> geoIndexed = new();

    public bool SupportsSpatial => this.options.spatialMappings.Count > 0;

    // ── Point methods (native $near / $geoWithin) ─────────────────────────

    public Task<IReadOnlyList<SpatialResult<T>>> WithinRadius<T>(GeoPoint center, double radiusMeters, Expression<Func<T, bool>>? filter = null, CancellationToken cancellationToken = default) where T : class
        => this.Tracker.Track("within_radius", typeof(T).Name, () => this.WithinRadiusImpl(center, radiusMeters, filter, cancellationToken), r => r.Count);

    Task<IReadOnlyList<SpatialResult<T>>> WithinRadiusImpl<T>(GeoPoint center, double radiusMeters, Expression<Func<T, bool>>? filter, CancellationToken cancellationToken) where T : class
    {
        var radians = radiusMeters / GeoEarthRadiusMeters;
        var geoFilter = CenterSphere(this.SpatialField<T>(), center, radians);
        return this.GeoQueryAsync<T>(geoFilter, stored => GeoDistance.PointToGeometry(center, stored) <= radiusMeters, center, filter, cancellationToken);
    }

    public Task<IReadOnlyList<T>> WithinBoundingBox<T>(GeoBoundingBox box, Expression<Func<T, bool>>? filter = null, CancellationToken cancellationToken = default) where T : class
        => this.Tracker.Track("within_bounding_box", typeof(T).Name, () => this.WithinBoundingBoxImpl(box, filter, cancellationToken), r => r.Count);

    async Task<IReadOnlyList<T>> WithinBoundingBoxImpl<T>(GeoBoundingBox box, Expression<Func<T, bool>>? filter, CancellationToken cancellationToken) where T : class
    {
        var poly = EnvelopePolygon(box);
        var geoFilter = new BsonDocument(this.SpatialField<T>(), new BsonDocument("$geoWithin", new BsonDocument("$geometry", this.Gj(poly))));
        var hits = await this.GeoQueryAsync<T>(geoFilter, null, null, filter, cancellationToken).ConfigureAwait(false);
        return hits.Select(h => h.Document).ToList();
    }

    public Task<IReadOnlyList<SpatialResult<T>>> NearestNeighbors<T>(GeoPoint center, int count, Expression<Func<T, bool>>? filter = null, CancellationToken cancellationToken = default) where T : class
        => this.Tracker.Track("nearest_neighbors", typeof(T).Name, () => this.NearestNeighborsImpl(center, count, filter, cancellationToken), r => r.Count);

    async Task<IReadOnlyList<SpatialResult<T>>> NearestNeighborsImpl<T>(GeoPoint center, int count, Expression<Func<T, bool>>? filter, CancellationToken cancellationToken) where T : class
    {
        var mapping = this.RequireSpatial<T>();
        var typeInfo = this.FindTypeInfo<T>(null);
        var typeName = this.ResolveTypeName<T>();
        var collection = this.GetCollection<T>();
        await this.EnsureGeoIndexAsync(collection, mapping, cancellationToken).ConfigureAwait(false);

        var near = new BsonDocument(this.SpatialField<T>(), new BsonDocument("$near",
            new BsonDocument("$geometry", this.Gj((Geometry)center))));
        var combined = Builders<BsonDocument>.Filter.And(
            Builders<BsonDocument>.Filter.Eq(MongoFields.TypeName, typeName),
            (FilterDefinition<BsonDocument>)near);

        var postFilter = filter == null ? null : ExpressionInterpreter.Interpret(filter);
        var overFetch = filter == null ? count : count * 4;
        var rows = await collection.Find(combined).Limit(overFetch).ToListAsync(cancellationToken).ConfigureAwait(false);

        var results = new List<SpatialResult<T>>();
        foreach (var row in rows)
        {
            var doc = this.DeserializeRow(row, typeInfo);
            if (doc is null || (postFilter != null && !postFilter(doc)))
                continue;
            var stored = mapping.ResolveGeometry(doc);
            if (stored is null)
                continue;
            results.Add(new SpatialResult<T> { Document = doc, DistanceMeters = GeoDistance.PointToGeometry(center, stored) });
            if (results.Count >= count)
                break;
        }
        return results;
    }

    // ── Geometry predicate family ─────────────────────────────────────────

    public Task<IReadOnlyList<SpatialResult<T>>> GeoIntersects<T>(Geometry geometry, Geometry? orderByDistanceFrom = null, Expression<Func<T, bool>>? filter = null, CancellationToken ct = default) where T : class
        => this.Tracker.Track("geo_intersects", typeof(T).Name, () => this.GeoQueryAsync<T>(this.GeoIntersectsFilter<T>(geometry), null, orderByDistanceFrom, filter, ct), r => r.Count);

    public Task<IReadOnlyList<SpatialResult<T>>> GeoContainedBy<T>(Geometry container, Geometry? orderByDistanceFrom = null, Expression<Func<T, bool>>? filter = null, CancellationToken ct = default) where T : class
        => this.Tracker.Track("geo_contained_by", typeof(T).Name, () => this.GeoQueryAsync<T>(new BsonDocument(this.SpatialField<T>(), new BsonDocument("$geoWithin", new BsonDocument("$geometry", this.Gj(container)))), null, orderByDistanceFrom, filter, ct), r => r.Count);

    public Task<IReadOnlyList<SpatialResult<T>>> GeoContains<T>(Geometry geometry, Geometry? orderByDistanceFrom = null, Expression<Func<T, bool>>? filter = null, CancellationToken ct = default) where T : class
        => this.Tracker.Track("geo_contains", typeof(T).Name, () => this.GeoQueryAsync<T>(this.GeoIntersectsFilter<T>(geometry), stored => SpatialPredicates.Contains(stored, geometry), orderByDistanceFrom, filter, ct), r => r.Count);

    public Task<IReadOnlyList<SpatialResult<T>>> GeoTouches<T>(Geometry geometry, Geometry? orderByDistanceFrom = null, Expression<Func<T, bool>>? filter = null, CancellationToken ct = default) where T : class
        => this.Tracker.Track("geo_touches", typeof(T).Name, () => this.GeoQueryAsync<T>(this.GeoIntersectsFilter<T>(geometry), stored => SpatialPredicates.Touches(stored, geometry), orderByDistanceFrom, filter, ct), r => r.Count);

    public Task<IReadOnlyList<SpatialResult<T>>> GeoCrosses<T>(Geometry geometry, Geometry? orderByDistanceFrom = null, Expression<Func<T, bool>>? filter = null, CancellationToken ct = default) where T : class
        => this.Tracker.Track("geo_crosses", typeof(T).Name, () => this.GeoQueryAsync<T>(this.GeoIntersectsFilter<T>(geometry), stored => SpatialPredicates.Crosses(stored, geometry), orderByDistanceFrom, filter, ct), r => r.Count);

    public Task<IReadOnlyList<SpatialResult<T>>> GeoOverlaps<T>(Geometry geometry, Geometry? orderByDistanceFrom = null, Expression<Func<T, bool>>? filter = null, CancellationToken ct = default) where T : class
        => this.Tracker.Track("geo_overlaps", typeof(T).Name, () => this.GeoQueryAsync<T>(this.GeoIntersectsFilter<T>(geometry), stored => SpatialPredicates.Overlaps(stored, geometry), orderByDistanceFrom, filter, ct), r => r.Count);

    public Task<IReadOnlyList<SpatialResult<T>>> GeoEquals<T>(Geometry geometry, Geometry? orderByDistanceFrom = null, Expression<Func<T, bool>>? filter = null, CancellationToken ct = default) where T : class
        => this.Tracker.Track("geo_equals", typeof(T).Name, () => this.GeoQueryAsync<T>(this.GeoIntersectsFilter<T>(geometry), stored => SpatialPredicates.GeometryEquals(stored, geometry), orderByDistanceFrom, filter, ct), r => r.Count);

    public Task<IReadOnlyList<SpatialResult<T>>> GeoCovers<T>(Geometry geometry, Geometry? orderByDistanceFrom = null, Expression<Func<T, bool>>? filter = null, CancellationToken ct = default) where T : class
        => this.Tracker.Track("geo_covers", typeof(T).Name, () => this.GeoQueryAsync<T>(this.GeoIntersectsFilter<T>(geometry), stored => SpatialPredicates.Covers(stored, geometry), orderByDistanceFrom, filter, ct), r => r.Count);

    public Task<IReadOnlyList<SpatialResult<T>>> GeoCoveredBy<T>(Geometry geometry, Geometry? orderByDistanceFrom = null, Expression<Func<T, bool>>? filter = null, CancellationToken ct = default) where T : class
        => this.Tracker.Track("geo_covered_by", typeof(T).Name, () => this.GeoQueryAsync<T>(this.GeoIntersectsFilter<T>(geometry), stored => SpatialPredicates.CoveredBy(stored, geometry), orderByDistanceFrom, filter, ct), r => r.Count);

    // No $not over $geoIntersects — scan the type and refine (O(n), documented).
    public Task<IReadOnlyList<SpatialResult<T>>> GeoDisjoint<T>(Geometry geometry, Geometry? orderByDistanceFrom = null, Expression<Func<T, bool>>? filter = null, CancellationToken ct = default) where T : class
        => this.Tracker.Track("geo_disjoint", typeof(T).Name, () => this.GeoQueryAsync<T>(Builders<BsonDocument>.Filter.Empty, stored => SpatialPredicates.Disjoint(stored, geometry), orderByDistanceFrom, filter, ct), r => r.Count);

    public Task<IReadOnlyList<SpatialResult<T>>> GeoWithinDistance<T>(Geometry geometry, double meters, Geometry? orderByDistanceFrom = null, Expression<Func<T, bool>>? filter = null, CancellationToken ct = default) where T : class
        => this.Tracker.Track("geo_within_distance", typeof(T).Name, () => this.GeoWithinDistanceImpl(geometry, meters, orderByDistanceFrom, filter, ct), r => r.Count);

    Task<IReadOnlyList<SpatialResult<T>>> GeoWithinDistanceImpl<T>(Geometry geometry, double meters, Geometry? orderByDistanceFrom, Expression<Func<T, bool>>? filter, CancellationToken ct) where T : class
    {
        // Point query → native $centerSphere; otherwise prune by the meters-expanded envelope and refine.
        FilterDefinition<BsonDocument> candidate = geometry is GeoPointGeometry pt
            ? CenterSphere(this.SpatialField<T>(), pt.Point, meters / GeoEarthRadiusMeters)
            : new BsonDocument(this.SpatialField<T>(), new BsonDocument("$geoIntersects",
                new BsonDocument("$geometry", this.Gj(EnvelopePolygon(geometry.GetEnvelope().ExpandByMeters(meters))))));
        return this.GeoQueryAsync<T>(candidate, stored => GeoDistance.Between(stored, geometry) <= meters, orderByDistanceFrom, filter, ct);
    }

    // ── plumbing ──────────────────────────────────────────────────────────

    async Task<IReadOnlyList<SpatialResult<T>>> GeoQueryAsync<T>(
        FilterDefinition<BsonDocument> geoFilter,
        Func<Geometry, bool>? refine,
        Geometry? orderRef,
        Expression<Func<T, bool>>? filter,
        CancellationToken ct) where T : class
    {
        var mapping = this.RequireSpatial<T>();
        var typeInfo = this.FindTypeInfo<T>(null);
        var typeName = this.ResolveTypeName<T>();
        var collection = this.GetCollection<T>();
        await this.EnsureGeoIndexAsync(collection, mapping, ct).ConfigureAwait(false);

        var combined = Builders<BsonDocument>.Filter.And(
            Builders<BsonDocument>.Filter.Eq(MongoFields.TypeName, typeName), geoFilter);

        var postFilter = filter == null ? null : ExpressionInterpreter.Interpret(filter);
        this.Log("geo query on " + collection.CollectionNamespace.CollectionName);
        var rows = await collection.Find(combined).ToListAsync(ct).ConfigureAwait(false);

        var results = new List<SpatialResult<T>>();
        foreach (var row in rows)
        {
            var doc = this.DeserializeRow(row, typeInfo);
            if (doc is null || (postFilter != null && !postFilter(doc)))
                continue;
            var stored = mapping.ResolveGeometry(doc);
            if (stored is null || (refine != null && !refine(stored)))
                continue;
            var distance = orderRef is null ? 0.0 : GeoDistance.Between(stored, orderRef);
            results.Add(new SpatialResult<T> { Document = doc, DistanceMeters = distance });
        }

        if (orderRef is not null)
            results.Sort((a, b) => a.DistanceMeters.CompareTo(b.DistanceMeters));
        return results;
    }

    MongoDbSpatialMapping RequireSpatial<T>() where T : class
        => this.options.ResolveSpatialMapping(typeof(T))
           ?? throw new NotSupportedException($"No spatial property mapped for type '{typeof(T).Name}'. Call MapSpatialProperty<{typeof(T).Name}>() in options.");

    string SpatialField<T>() where T : class => $"{MongoFields.Data}.{this.RequireSpatial<T>().JsonPath}";

    T? DeserializeRow<T>(BsonDocument row, System.Text.Json.Serialization.Metadata.JsonTypeInfo<T>? typeInfo) where T : class
        => row.Contains(MongoFields.Data) && row[MongoFields.Data].BsonType == BsonType.Document
            ? Deserialize(row[MongoFields.Data].AsBsonDocument, typeInfo, this.jsonOptions)
            : null;

    FilterDefinition<BsonDocument> GeoIntersectsFilter<T>(Geometry geometry) where T : class
        => new BsonDocument(this.SpatialField<T>(), new BsonDocument("$geoIntersects", new BsonDocument("$geometry", this.Gj(geometry))));

    static FilterDefinition<BsonDocument> CenterSphere(string field, GeoPoint center, double radians)
        => new BsonDocument(field, new BsonDocument("$geoWithin",
            new BsonDocument("$centerSphere", new BsonArray
            {
                new BsonArray { center.Longitude, center.Latitude },
                radians
            })));

    BsonValue Gj(Geometry g) => BsonDocument.Parse(JsonSerializer.Serialize(g, this.jsonOptions));

    static GeoPolygon EnvelopePolygon(GeoBoundingBox b)
        => new(new GeoPoint[]
        {
            new(b.MinLatitude, b.MinLongitude),
            new(b.MinLatitude, b.MaxLongitude),
            new(b.MaxLatitude, b.MaxLongitude),
            new(b.MaxLatitude, b.MinLongitude),
            new(b.MinLatitude, b.MinLongitude)
        });

    async Task EnsureGeoIndexAsync(IMongoCollection<BsonDocument> collection, MongoDbSpatialMapping mapping, CancellationToken ct)
    {
        var field = $"{MongoFields.Data}.{mapping.JsonPath}";
        var key = collection.CollectionNamespace.FullName + ":" + field;
        if (!this.geoIndexed.TryAdd(key, 0))
            return;
        var model = new CreateIndexModel<BsonDocument>(new BsonDocument(field, "2dsphere"),
            new CreateIndexOptions { Name = "geo_" + mapping.JsonPath.Replace('.', '_') });
        try
        {
            await collection.Indexes.CreateOneAsync(model, cancellationToken: ct).ConfigureAwait(false);
        }
        catch
        {
            this.geoIndexed.TryRemove(key, out _);
            throw;
        }
    }
}
