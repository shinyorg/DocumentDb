using Shiny.DocumentDb.Oracle;
using Testcontainers.Oracle;
using Xunit;

namespace Shiny.DocumentDb.Tests.Fixtures;

/// <summary>
/// Exercises Oracle's <b>native</b> spatial pushdown — the SDO_GEOMETRY column, USER_SDO_GEOM_METADATA
/// registration and MDSYS spatial index — which the default slim test image can't run (it ships without the
/// Oracle Spatial option). It uses the full <c>gvenzl/oracle-free</c> image (Spatial included), configured via
/// the <c>ORACLE_SPATIAL_IMAGE</c> environment variable so normal CI stays on the lightweight slim image and
/// only opts in when that variable is set (e.g. <c>ORACLE_SPATIAL_IMAGE=gvenzl/oracle-free:23-faststart</c>).
/// </summary>
public class OracleNativeSpatialFixture : IAsyncLifetime
{
    OracleContainer? container;

    /// <summary>The full image to run, or null when the opt-in env var isn't set (tests skip).</summary>
    public static string? SpatialImage => Environment.GetEnvironmentVariable("ORACLE_SPATIAL_IMAGE");

    public bool Enabled => SpatialImage != null;

    public string ConnectionString => this.container!.GetConnectionString();

    public async ValueTask InitializeAsync()
    {
        if (!this.Enabled)
            return;
        this.container = new OracleBuilder().WithImage(SpatialImage!).Build();
        await this.container.StartAsync();
    }

    public IDocumentStore CreateSpatialStore(string tableName)
    {
        // No PortableSpatial → native SDO_GEOMETRY column + spatial index + SDO_* operator pushdown.
        var opts = new DocumentStoreOptions
        {
            DatabaseProvider = new OracleDatabaseProvider(this.container!.GetConnectionString()),
            TableName = tableName
        };
        opts.ConfigureDocument<GeoZone>(cfg => cfg.MapSpatialProperty(z => z.Area));
        return new DocumentStore(opts);
    }

    public async ValueTask DisposeAsync()
    {
        if (this.container != null)
            await this.container.DisposeAsync();
    }
}
