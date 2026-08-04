using Microsoft.Data.Sqlite;
using Shiny.DocumentDb.Sqlite;
using Shiny.DocumentDb.Tests.Fixtures;
using Xunit;

namespace Shiny.DocumentDb.Tests;

sealed class TenantTemporalDoc
{
    public string Id { get; set; } = "";
    public string? Name { get; set; }
}

/// <summary>Verifies the tenant-scoped history SQL against a real container dialect (parameter binding + the
/// TenantId column rewrite), complementing the SQLite logic test.</summary>
[Collection("PostgreSQL")]
public class PostgreSqlTemporalTenantTests(PostgreSqlDatabaseFixture fx)
{
    [Fact]
    public async Task History_AsOfAll_TenantScoped()
    {
        var tenant = "A";
        var opts = new DocumentStoreOptions
        {
            DatabaseProvider = fx.CreateProvider(),
            TableName = $"t{Guid.NewGuid():N}",
            TenantIdAccessor = () => tenant
        };
        opts.ConfigureDocument<TenantTemporalDoc>(cfg => cfg.MapTemporal());
        using var store = new DocumentStore(opts);

        tenant = "A";
        await store.Insert(new TenantTemporalDoc { Id = "a1", Name = "A1" });
        tenant = "B";
        await store.Insert(new TenantTemporalDoc { Id = "b1", Name = "B1" });

        tenant = "B";
        Assert.Empty(await store.History<TenantTemporalDoc>("a1"));
        Assert.Equal(new[] { "b1" }, (await store.AsOfAll<TenantTemporalDoc>(DateTimeOffset.UtcNow)).Select(d => d.Id).ToArray());

        tenant = "A";
        Assert.Single(await store.History<TenantTemporalDoc>("a1"));
        Assert.Equal(new[] { "a1" }, (await store.AsOfAll<TenantTemporalDoc>(DateTimeOffset.UtcNow)).Select(d => d.Id).ToArray());
    }
}

/// <summary>
/// Regression: temporal history was tenant-blind — a tenant could read another tenant's version history (via a
/// known id) and AsOfAll returned every tenant's documents. The history sidecar now carries a TenantId column
/// and every temporal read/write is tenant-scoped.
/// </summary>
public class TemporalTenantTests : IDisposable
{
    sealed class Doc
    {
        public string Id { get; set; } = "";
        public string? Name { get; set; }
    }

    readonly SqliteConnection hold;
    readonly DocumentStore store;
    string tenant = "A";

    public TemporalTenantTests()
    {
        var cs = $"Data Source=ttenant_{Guid.NewGuid():N};Mode=Memory;Cache=Shared";
        this.hold = new SqliteConnection(cs);
        this.hold.Open();
        var opts = new DocumentStoreOptions
        {
            DatabaseProvider = new SqliteDatabaseProvider(cs),
            TableName = $"t{Guid.NewGuid():N}",
            TenantIdAccessor = () => this.tenant
        };
        opts.ConfigureDocument<Doc>(cfg => cfg.MapTemporal());
        this.store = new DocumentStore(opts);
    }

    public void Dispose()
    {
        this.store.Dispose();
        this.hold.Dispose();
    }

    [Fact]
    public async Task History_AsOf_AsOfAll_AreTenantScoped()
    {
        this.tenant = "A";
        await this.store.Insert(new Doc { Id = "a1", Name = "A1" });
        await this.store.Update(new Doc { Id = "a1", Name = "A1-v2" });
        this.tenant = "B";
        await this.store.Insert(new Doc { Id = "b1", Name = "B1" });

        // Tenant B cannot see tenant A's history/state via a known id, and AsOfAll returns only B's documents.
        this.tenant = "B";
        Assert.Empty(await this.store.History<Doc>("a1"));
        Assert.Null(await this.store.AsOf<Doc>("a1", DateTimeOffset.UtcNow));
        var bAll = await this.store.AsOfAll<Doc>(DateTimeOffset.UtcNow);
        Assert.Equal(new[] { "b1" }, bAll.Select(d => d.Id).ToArray());

        // Tenant A sees only its own — two versions of a1, and just a1 fleet-wide.
        this.tenant = "A";
        Assert.Equal(2, (await this.store.History<Doc>("a1")).Count);
        Assert.Equal("A1-v2", (await this.store.AsOf<Doc>("a1", DateTimeOffset.UtcNow))!.Name);
        var aAll = await this.store.AsOfAll<Doc>(DateTimeOffset.UtcNow);
        Assert.Equal(new[] { "a1" }, aAll.Select(d => d.Id).ToArray());
    }
}
