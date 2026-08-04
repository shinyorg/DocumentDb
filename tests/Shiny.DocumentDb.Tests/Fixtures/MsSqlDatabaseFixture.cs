using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;
using Microsoft.Data.SqlClient;
using Shiny.DocumentDb.SqlServer;
using Testcontainers.MsSql;
using Xunit;

namespace Shiny.DocumentDb.Tests.Fixtures;

public class MsSqlDatabaseFixture : IDatabaseFixture, IDocumentStoreFixture, ITenantDocumentStoreFixture, ITemporalDocumentStoreFixture, ISpatialDocumentStoreFixture, IAsyncLifetime
{
    MsSqlContainer container = null!;

    public ITemporalDocumentStore CreateTemporalStore(string tableName, Action<TemporalOptions>? configure = null, Func<string>? actor = null)
    {
        var opts = new DocumentStoreOptions { DatabaseProvider = this.CreateProvider(), TableName = tableName };
        opts.ConfigureDocument<VersionedUser>(cfg => cfg.MapTemporal(o =>
        {
            configure?.Invoke(o);
            if (actor != null)
                o.CaptureActor = actor;
        }));
        opts.ConfigureDocument<MergeDoc>(cfg => cfg.MapTemporal());
        return new DocumentStore(opts);
    }
    string connectionString = null!;

    const string TestDatabaseName = "documentdb_tests";

    public IDatabaseProvider CreateProvider()
        => new SqlServerDatabaseProvider(this.connectionString);

    public async ValueTask InitializeAsync()
    {
        container = new MsSqlBuilder()
            .WithImage("mcr.microsoft.com/mssql/server:2025-latest")
            .Build();
        await container.StartAsync();

        // The default MsSqlBuilder connection string targets `master`, which can't be ALTERed via
        // CURRENT (blocks change-tracking provisioning). Create a user database and re-point at it.
        var masterCs = container.GetConnectionString();
        await using (var conn = new SqlConnection(masterCs))
        {
            await conn.OpenAsync();
            await using var cmd = conn.CreateCommand();
            cmd.CommandText = $"IF DB_ID('{TestDatabaseName}') IS NULL CREATE DATABASE [{TestDatabaseName}];";
            await cmd.ExecuteNonQueryAsync();
        }

        var builder = new SqlConnectionStringBuilder(masterCs) { InitialCatalog = TestDatabaseName };
        this.connectionString = builder.ConnectionString;
    }

    public IDocumentStore CreateStore(string tableName)
        => new DocumentStore(new DocumentStoreOptions
        {
            DatabaseProvider = this.CreateProvider(),
            TableName = tableName
        });

    /// <summary>Builds a store with caller-supplied provider-agnostic options — the hook the
    /// cross-provider conformance suites configure themselves through.</summary>
    public IDocumentStore CreateStore(string tableName, Action<IDocumentStoreOptions> configure)
    {
        var opts = new DocumentStoreOptions { DatabaseProvider = this.CreateProvider(), TableName = tableName };
        configure(opts);
        return new DocumentStore(opts);
    }

    public IDocumentStore CreateStoreWithVersion<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)] T>(string tableName, Expression<Func<T, int>> versionProperty) where T : class
    {
        var opts = new DocumentStoreOptions { DatabaseProvider = this.CreateProvider(), TableName = tableName };
        opts.ConfigureDocument<T>(cfg => cfg.MapVersionProperty(versionProperty));
        return new DocumentStore(opts);
    }

    public IDocumentStore CreateStoreWithTenant(string tableName, Func<string> tenantIdAccessor)
        => new DocumentStore(new DocumentStoreOptions
        {
            DatabaseProvider = this.CreateProvider(),
            TableName = tableName,
            TenantIdAccessor = tenantIdAccessor
        });

    public IDocumentStore CreateFullTextStore(string tableName)
    {
        var opts = new DocumentStoreOptions
        {
            DatabaseProvider = this.CreateProvider(),
            TableName = tableName
        };
        opts.ConfigureDocument<FtArticle>(cfg => cfg.MapFullTextProperty([a => a.Title, a => a.Body]));
        return new DocumentStore(opts);
    }

    public IDocumentStore CreateSpatialStore(string tableName)
    {
        var opts = new DocumentStoreOptions
        {
            DatabaseProvider = this.CreateProvider(),
            TableName = tableName
        };
        opts.ConfigureDocument<GeoZone>(cfg => cfg.MapSpatialProperty(z => z.Area));
        return new DocumentStore(opts);
    }

    public IDocumentStore CreateComputedStore(string tableName)
    {
        var opts = new DocumentStoreOptions
        {
            DatabaseProvider = this.CreateProvider(),
            TableName = tableName
        };
        opts.ConfigureDocument<ComputedSale>(cfg =>
        {
            cfg.MapComputedProperty<int>(s => s.LineTotalCents, s => s.Quantity * s.UnitPriceCents, indexed: true);
            cfg.MapComputedProperty<string>(s => s.FullName, s => s.First + " " + s.Last);
        });
        return new DocumentStore(opts);
    }

    public IDocumentStore CreateVectorStore(string tableName, int dimensions = 4, VectorDistance metric = VectorDistance.Cosine, VectorIndexKind indexKind = VectorIndexKind.None)
    {
        // Use IndexKind.None by default — CREATE VECTOR INDEX is preview-gated; the tests only
        // care that NearestVectors works under VECTOR_DISTANCE, which works without an index.
        var opts = new DocumentStoreOptions
        {
            DatabaseProvider = this.CreateProvider(),
            TableName = tableName
        };
        opts.ConfigureDocument<VectorDoc>(cfg => cfg.MapVectorProperty(d => d.Embedding, dimensions, metric, indexKind));
        return new DocumentStore(opts);
    }

    public async ValueTask DisposeAsync()
        => await container.DisposeAsync();
}
