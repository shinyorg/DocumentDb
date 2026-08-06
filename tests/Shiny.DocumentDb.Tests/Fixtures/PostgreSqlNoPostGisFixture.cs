using Shiny.DocumentDb.PostgreSql;
using Testcontainers.PostgreSql;
using Xunit;

namespace Shiny.DocumentDb.Tests.Fixtures;

/// <summary>
/// Stock PostgreSQL, deliberately <b>without</b> PostGIS — unlike <see cref="PostgreSqlDatabaseFixture"/>,
/// which builds a custom PostGIS+pgvector image for the native-spatial surface. This is the server most
/// people actually have, and it backs both halves of the portable-spatial contract: the envelope tier runs
/// on it unchanged, and the native tier fails with an actionable message instead of a raw driver error.
/// </summary>
public class PostgreSqlNoPostGisFixture : IAsyncLifetime
{
    PostgreSqlContainer container = null!;

    public async ValueTask InitializeAsync()
    {
        this.container = new PostgreSqlBuilder().Build();
        await this.container.StartAsync();
    }

    public string ConnectionString => this.container.GetConnectionString();

    public IDocumentStore CreateSpatialStore(string tableName, bool portableSpatial)
    {
        var opts = new DocumentStoreOptions
        {
            DatabaseProvider = new PostgreSqlDatabaseProvider(this.ConnectionString)
            {
                PortableSpatial = portableSpatial
            },
            TableName = tableName
        };
        opts.ConfigureDocument<GeoZone>(cfg => cfg.MapSpatialProperty(z => z.Area));
        return new DocumentStore(opts);
    }

    public async ValueTask DisposeAsync()
        => await this.container.DisposeAsync();
}
