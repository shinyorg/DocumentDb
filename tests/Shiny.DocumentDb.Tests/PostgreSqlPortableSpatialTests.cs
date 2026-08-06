using Npgsql;
using Shiny.DocumentDb.PostgreSql;
using Shiny.DocumentDb.Tests.Fixtures;
using Xunit;

namespace Shiny.DocumentDb.Tests;

/// <summary>
/// The remedy hook itself: it must fire only for a genuinely missing/ungrantable extension, so an unrelated
/// failure at the same statement (a dropped connection, a real DDL error) is never relabelled as "install
/// PostGIS" and keeps its own exception type.
/// </summary>
public class PostgreSqlSpatialFailureDescriptionTests
{
    static PostgreSqlDatabaseProvider Provider(bool portableSpatial = false)
        => new("Host=localhost;Database=x;Username=u;Password=p") { PortableSpatial = portableSpatial };

    static PostgresException Pg(string sqlState, string message = "boom")
        => new(message, "ERROR", "ERROR", sqlState);

    [Theory]
    [InlineData("0A000")]   // feature_not_supported - extension not installed on the server
    [InlineData("42501")]   // insufficient_privilege - role may not CREATE EXTENSION
    public void ExtensionFailures_ReturnRemedy(string sqlState)
    {
        var hint = Provider().DescribeSpatialInitFailure(Pg(sqlState));
        Assert.NotNull(hint);
        Assert.Contains("PostGIS", hint);
        Assert.Contains("PortableSpatial", hint);
    }

    [Fact]
    public void UnrelatedPostgresError_IsNotRelabelled()
        // 42P01 undefined_table is a real DDL problem, not a missing extension.
        => Assert.Null(Provider().DescribeSpatialInitFailure(Pg("42P01")));

    [Fact]
    public void NonPostgresException_IsNotRelabelled()
        => Assert.Null(Provider().DescribeSpatialInitFailure(new TimeoutException("connection lost")));

    [Fact]
    public void PortableTier_NeverDescribes()
        => Assert.Null(Provider(portableSpatial: true).DescribeSpatialInitFailure(Pg("0A000")));
}

/// <summary>
/// The full geometry surface running against <b>stock PostgreSQL</b> (no PostGIS) via the portable envelope
/// tier — the escape hatch for servers where the extension can't be installed. Same assertions as the
/// PostGIS-backed <c>PostgreSqlGeometryTests</c>, so the tier swap must be behaviour-preserving.
/// </summary>
[Collection("PostgreSQLNoPostGis")]
public class PostgreSqlPortableGeometryTests(PostgreSqlNoPostGisFixture fx) : GeometryProviderTestsBase
{
    protected override IDocumentStore CreateStore(string tableName)
        => fx.CreateSpatialStore(tableName, portableSpatial: true);
}

/// <summary>
/// The other half: on a server without PostGIS the native tier must fail with a message that names the
/// remedy, not the raw "extension postgis is not available" that Npgsql surfaces from deep inside the
/// first spatial write.
/// </summary>
[Collection("PostgreSQLNoPostGis")]
public class PostgreSqlMissingPostGisTests(PostgreSqlNoPostGisFixture fx)
{
    static GeoPolygon Square(double minLng, double minLat, double maxLng, double maxLat) =>
        new(new GeoPoint[]
        {
            new(minLat, minLng), new(minLat, maxLng), new(maxLat, maxLng), new(maxLat, minLng), new(minLat, minLng)
        });

    [Fact]
    public async Task NativeTier_WithoutPostGis_ThrowsActionableError()
    {
        var store = fx.CreateSpatialStore($"geo_nopostgis_{Guid.NewGuid():N}", portableSpatial: false);

        var ex = await Assert.ThrowsAsync<DocumentConfigurationException>(
            () => store.Upsert(new GeoZone { Id = "unit", Name = "Unit", Area = Square(0, 0, 1, 1) })
        );

        Assert.Contains("PostGIS", ex.Message);
        Assert.Contains("PortableSpatial", ex.Message);
        // The driver error is the cause, not the headline - keep it reachable for diagnostics.
        Assert.NotNull(ex.InnerException);
    }

    [Fact]
    public async Task PortableTier_WithoutPostGis_Writes_And_Queries()
    {
        var store = fx.CreateSpatialStore($"geo_portable_{Guid.NewGuid():N}", portableSpatial: true);
        await store.Upsert(new GeoZone { Id = "unit", Name = "Unit", Area = Square(0, 0, 1, 1) });

        var hits = await store.GeoIntersects<GeoZone>(new GeoPoint(0.5, 0.5));
        Assert.Equal(["unit"], hits.Select(h => h.Document.Id));
    }
}
