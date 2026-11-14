using System.Text;
using Shiny.DocumentDb.Internal;
using Shiny.DocumentDb.Tests.Fixtures;
using Xunit;

namespace Shiny.DocumentDb.Tests;

/// <summary>
/// A delete narrowed by a WHERE clause, a tenant or a global query filter must purge exactly the deleted documents'
/// sidecar rows (vector index, blob payloads): an orphaned vector row still competes for ANN top-k slots, and the
/// by-type sidecar clear would drop the rows of documents that survive. Asserted on every relational provider.
/// </summary>
public abstract class SidecarDeletionConformanceTestsBase(IDocumentStoreFixture fixture)
{
    protected readonly IDocumentStoreFixture Fixture = fixture;

    string tenant = "first";

    IDocumentStore VectorStore(string table, bool tenanted = false, bool filtered = false)
        => this.Fixture.CreateStore(table, o =>
        {
            if (tenanted)
                ((DocumentStoreOptions)o).TenantIdAccessor = () => this.tenant;
            o.ConfigureDocument<VectorDoc>(cfg =>
            {
                cfg.MapVectorProperty(x => x.Embedding, 4, VectorDistance.Cosine, VectorIndexKind.None);
                if (filtered)
                    cfg.AddQueryFilter(x => x.Tag == "a");
            });
        });

    IDocumentStore BlobStore(string table, bool tenanted = false)
        => this.Fixture.CreateStore(table, o =>
        {
            if (tenanted)
                ((DocumentStoreOptions)o).TenantIdAccessor = () => this.tenant;
            o.ConfigureDocument<BlobDoc>(cfg => cfg.MapBlob(x => x.Pdf));
        });

    static async Task<IDocumentStore> Seed(IDocumentStore store)
    {
        foreach (var doc in VectorTestSeed.Docs)
            await store.Insert(doc);
        return store;
    }

    static string Table() => $"t{Guid.NewGuid():N}"[..20];

    static void RequireVectors(IDocumentStore store)
    {
        if (!store.SupportsVector)
            Assert.Skip("Provider has no vector support in this fixture.");
    }

    static void RequireBlobs(IDocumentStore store)
    {
        if (((DocumentStore)store).DatabaseProvider.MaxBlobSize == 0)
            Assert.Skip("Provider has no blob storage.");
    }

    /// <summary>
    /// The ids in the vector sidecar, read straight from the database over the store's own connection (an in-memory
    /// database is invisible to a second one).
    /// </summary>
    static Task<string[]> IndexedIds(IDocumentStore store, string table)
        => ((IQueryExecutor)store).ExecuteAsync(table, async session =>
        {
            var provider = ((DocumentStore)store).DatabaseProvider;
            await using var cmd = session.CreateCommand();
            cmd.CommandText = provider.BuildVectorDocIdsSql(table, nameof(VectorDoc))!;
            var p = cmd.CreateParameter();
            p.ParameterName = "@vecTypeName";
            p.Value = nameof(VectorDoc);
            cmd.Parameters.Add(p);

            var ids = new List<string>();
            await using var reader = await cmd.ExecuteReaderAsync();
            while (await reader.ReadAsync())
                ids.Add(reader.GetString(0));
            return ids.Order().ToArray();
        }, default);

    [Fact]
    public async Task ExecuteDelete_with_a_where_clause_purges_only_the_deleted_vectors()
    {
        var table = Table();
        var store = await Seed(this.VectorStore(table));
        RequireVectors(store);

        Assert.Equal(2, await store.Query<VectorDoc>().Where(x => x.Tag == "a").ExecuteDelete());

        Assert.Equal(["u3", "u4", "u5"], await IndexedIds(store, table));
        var hits = await store.NearestVectors<VectorDoc>(VectorTestSeed.Query, k: 3);
        Assert.Equal(["u3", "u4", "u5"], hits.Select(h => h.Document.Id).ToArray());
    }

    [Fact]
    public async Task ExecuteDelete_inside_a_unit_purges_vectors_on_commit()
    {
        var table = Table();
        var store = await Seed(this.VectorStore(table));
        RequireVectors(store);

        await ((IUnitOfWorkEngine)store).RunUnitAsync(async (tx, ct) =>
            Assert.Equal(2, await tx.Query<VectorDoc>().Where(x => x.Tag == "b").ExecuteDelete(ct)), default);

        Assert.Equal(["u1", "u2", "u5"], await IndexedIds(store, table));
    }

    [Fact]
    public async Task ExecuteDelete_inside_a_rolled_back_unit_keeps_documents_and_vectors()
    {
        var table = Table();
        var store = await Seed(this.VectorStore(table));
        RequireVectors(store);

        await Assert.ThrowsAsync<InvalidOperationException>(() => ((IUnitOfWorkEngine)store).RunUnitAsync(async (tx, ct) =>
        {
            await tx.Query<VectorDoc>().Where(x => x.Tag == "b").ExecuteDelete(ct);
            throw new InvalidOperationException("roll back");
        }, default));

        Assert.Equal(5, await store.Count<VectorDoc>());
        Assert.Equal(["u1", "u2", "u3", "u4", "u5"], await IndexedIds(store, table));
    }

    [Fact]
    public async Task Clear_with_a_tenant_keeps_the_other_tenants_vectors()
    {
        var table = Table();
        var store = this.VectorStore(table, tenanted: true);
        RequireVectors(store);
        this.tenant = "first";
        await store.Insert(VectorTestSeed.Docs[0]);
        await store.Insert(VectorTestSeed.Docs[1]);
        this.tenant = "second";
        await store.Insert(VectorTestSeed.Docs[2]);

        this.tenant = "first";
        Assert.Equal(2, await store.Clear<VectorDoc>());

        Assert.Equal(["u3"], await IndexedIds(store, table));
        this.tenant = "second";
        Assert.Equal("u3", Assert.Single(await store.NearestVectors<VectorDoc>(VectorTestSeed.Query, k: 5)).Document.Id);
    }

    [Fact]
    public async Task Clear_inside_a_unit_with_a_tenant_keeps_the_other_tenants_vectors()
    {
        var table = Table();
        var store = this.VectorStore(table, tenanted: true);
        RequireVectors(store);
        this.tenant = "first";
        await store.Insert(VectorTestSeed.Docs[0]);
        this.tenant = "second";
        await store.Insert(VectorTestSeed.Docs[2]);

        this.tenant = "first";
        await ((IUnitOfWorkEngine)store).RunUnitAsync(async (tx, ct) =>
            Assert.Equal(1, await tx.Clear<VectorDoc>(ct)), default);

        Assert.Equal(["u3"], await IndexedIds(store, table));
    }

    [Fact]
    public async Task Clear_with_a_query_filter_purges_only_the_matching_vectors_and_publishes_one_cleared_change()
    {
        var table = Table();
        var store = await Seed(this.VectorStore(table, filtered: true));
        RequireVectors(store);
        using var changes = new ChangeCollector<VectorDoc>(((IObservableDocumentStore)store).NotifyOnChange<VectorDoc>());
        await changes.Ready;

        Assert.Equal(2, await store.Clear<VectorDoc>());

        Assert.Equal(["u3", "u4", "u5"], await IndexedIds(store, table));
        await changes.SettleAsync();
        Assert.Equal(DocumentChangeType.Cleared, Assert.Single(changes.Snapshot).ChangeType);
    }

    [Fact]
    public async Task Clear_without_scope_still_drops_every_vector()
    {
        var table = Table();
        var store = await Seed(this.VectorStore(table));
        RequireVectors(store);

        Assert.Equal(5, await store.Clear<VectorDoc>());

        Assert.Empty(await IndexedIds(store, table));
    }

    [Fact]
    public async Task ExecuteDelete_purges_the_deleted_documents_blobs()
    {
        var store = this.BlobStore(Table());
        RequireBlobs(store);
        await store.Insert(new BlobDoc { Id = "b1", Name = "drop", Pdf = DocumentBlob.FromBytes(Encoding.UTF8.GetBytes("one")) });
        await store.Insert(new BlobDoc { Id = "b2", Name = "keep", Pdf = DocumentBlob.FromBytes(Encoding.UTF8.GetBytes("two")) });

        Assert.Equal(1, await store.Query<BlobDoc>().Where(x => x.Name == "drop").ExecuteDelete());

        // Re-inserting the id without a payload must not resurrect the deleted one.
        await store.Insert(new BlobDoc { Id = "b1", Name = "again" });
        Assert.Null(await ((IBlobDocumentStore)store).GetBlob<BlobDoc>("b1", "Pdf"));
        Assert.NotNull(await ((IBlobDocumentStore)store).GetBlob<BlobDoc>("b2", "Pdf"));
    }

    [Fact]
    public async Task Clear_with_a_tenant_keeps_the_other_tenants_blobs()
    {
        var store = this.BlobStore(Table(), tenanted: true);
        RequireBlobs(store);
        this.tenant = "first";
        await store.Insert(new BlobDoc { Id = "b1", Pdf = DocumentBlob.FromBytes(Encoding.UTF8.GetBytes("one")) });
        this.tenant = "second";
        await store.Insert(new BlobDoc { Id = "b2", Pdf = DocumentBlob.FromBytes(Encoding.UTF8.GetBytes("two")) });

        this.tenant = "first";
        Assert.Equal(1, await store.Clear<BlobDoc>());

        this.tenant = "second";
        Assert.NotNull(await ((IBlobDocumentStore)store).GetBlob<BlobDoc>("b2", "Pdf"));
    }
}

[Collection("SQLite")]
public class SqliteSidecarDeletionConformanceTests(SqliteDatabaseFixture db) : SidecarDeletionConformanceTestsBase(db);

[Collection("PostgreSQL")]
public class PostgreSqlSidecarDeletionConformanceTests(PostgreSqlDatabaseFixture db) : SidecarDeletionConformanceTestsBase(db);

[Collection("CockroachDB")]
public class CockroachDbSidecarDeletionConformanceTests(CockroachDbDatabaseFixture db) : SidecarDeletionConformanceTestsBase(db);

[Collection("MSSQL")]
public class SqlServerSidecarDeletionConformanceTests(MsSqlDatabaseFixture db) : SidecarDeletionConformanceTestsBase(db);

[Collection("Oracle")]
public class OracleSidecarDeletionConformanceTests(OracleDatabaseFixture db) : SidecarDeletionConformanceTestsBase(db);

[Collection("DuckDB")]
public class DuckDbSidecarDeletionConformanceTests(DuckDbDatabaseFixture db) : SidecarDeletionConformanceTestsBase(db);

[Collection("MySQL")]
public class MySqlSidecarDeletionConformanceTests(MySqlDatabaseFixture db) : SidecarDeletionConformanceTestsBase(db);

[Collection("MariaDB")]
public class MariaDbSidecarDeletionConformanceTests(MariaDbDatabaseFixture db) : SidecarDeletionConformanceTestsBase(db);
