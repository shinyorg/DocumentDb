using Microsoft.Data.Sqlite;
using System.Data;
using System.Data.Common;
using Shiny.DocumentDb.Sqlite;
using Shiny.DocumentDb.Tests.Fixtures;
using Xunit;

namespace Shiny.DocumentDb.Tests;

/// <summary>Verifies physical vector-index consistency for every SQLite document-delete path.</summary>
[Collection("SQLite")]
public sealed class SqliteVectorDeletionConsistencyTests : IDisposable
{
    private readonly string dbPath = Path.Combine(Path.GetTempPath(), $"vector-delete-{Guid.NewGuid():N}.db");
    private readonly List<DocumentStore> ownedStores = [];
    private static readonly ReadOnlyMemory<float> Probe = VectorDoc.V(1, 0, 0, 0);

    /// <summary>Removes vector candidates together with documents in a set-based query.</summary>
    [Fact]
    public async Task ExecuteDelete_RemovesVectorAndMapping_WithoutHidingSurvivors()
    {
        var store = await SeedAsync();

        Assert.Equal(1, await store.Query<VectorDoc>().Where(x => x.Tag == "remove").ExecuteDelete());

        await AssertSurvivingCandidateAsync(store);
    }

    /// <summary>Keeps the index aligned when batch deletion uses a transaction's per-document path.</summary>
    [Fact]
    public async Task BatchRemove_RemovesVectorAndMapping()
    {
        var store = await SeedAsync();

        Assert.Equal(1, await store.BatchRemove<VectorDoc>(["remove"]));

        await AssertSurvivingCandidateAsync(store);
    }

    /// <summary>Does not require trusted schema or virtual-table triggers to keep deletion consistent.</summary>
    [Fact]
    public async Task ExecuteDelete_WithTrustedSchemaDisabled_CleansVectors()
    {
        var options = new DocumentStoreOptions
        {
            DatabaseProvider = new HardenedProvider($"Data Source={dbPath};Pooling=False")
            {
                EnableVectorExtension = true,
                VectorExtensionPath = SqliteDatabaseFixture.FindVec0BinaryPath()!
            }
        };
        options.ConfigureDocument<VectorDoc>(cfg => cfg.MapVectorProperty(x => x.Embedding, 4, VectorDistance.Cosine));
        var store = Own(options);
        await store.Insert(new VectorDoc { Id = "remove", Tag = "remove", Embedding = Probe });
        await store.Insert(new VectorDoc { Id = "keep", Tag = "keep", Embedding = VectorDoc.V(.8f, .6f, 0, 0) });

        Assert.Equal(1, await store.Query<VectorDoc>().Where(x => x.Tag == "remove").ExecuteDelete());

        await AssertSurvivingCandidateAsync(store);
    }

    /// <summary>Joins the caller's transaction and preserves its rollback boundary.</summary>
    [Fact]
    public async Task ExecuteDelete_InAmbientTransaction_RollsBackVectorsAndDocuments()
    {
        var store = await SeedAsync();
        await using var session = store.OpenSession();
        await using (var transaction = await session.BeginTransaction())
        {
            Assert.Equal(1, await session.Query<VectorDoc>().Where(x => x.Tag == "remove").ExecuteDelete());
            await transaction.Rollback();
        }

        Assert.Equal(2, await store.Query<VectorDoc>().Count());
        Assert.Equal(2L, await CountAsync("documents_vec_VectorDoc"));
        Assert.Equal("remove", Assert.Single(await store.NearestVectors<VectorDoc>(Probe, 1)).Document.Id);
    }

    /// <summary>Rolls the document, vector and mapping deletion back as one SQLite operation.</summary>
    [Fact]
    public async Task Rollback_PreservesDocumentVectorAndMapping()
    {
        var store = await SeedAsync();
        await using var session = store.OpenSession();
        await using (var transaction = await session.BeginTransaction())
        {
            session.Remove<VectorDoc>("remove");
            await session.SaveChanges();
            await transaction.Rollback();
        }

        Assert.Equal(2, await store.Query<VectorDoc>().Count());
        Assert.Equal(2L, await CountAsync("documents_vec_VectorDoc"));
        Assert.Equal(2L, await CountAsync("documents_vec_map_VectorDoc"));
        Assert.Equal("remove", Assert.Single(await store.NearestVectors<VectorDoc>(Probe, 1)).Document.Id);
    }

    /// <summary>Repairs legacy orphan candidates before the first query on a reopened store.</summary>
    [Fact]
    public async Task Reopen_RepairsLegacyOrphans_AndPreservesSurvivingVectors()
    {
        await SeedAsync();
        using (var connection = await OpenRawAsync())
        {
            using var command = connection.CreateCommand();
            command.CommandText = "DELETE FROM documents WHERE Id='remove' AND TypeName='VectorDoc';";
            await command.ExecuteNonQueryAsync();
        }
        Assert.Equal(2L, await CountAsync("documents_vec_VectorDoc"));
        Assert.Equal(2L, await CountAsync("documents_vec_map_VectorDoc"));

        var reopened = CreateStore();

        await AssertSurvivingCandidateAsync(reopened);
    }

    /// <summary>Does not delete one type's vector when another document type uses the same identifier.</summary>
    [Fact]
    public async Task DeleteOtherType_PreservesVectorWithSameDocumentId()
    {
        var store = await SeedAsync();
        await store.Insert(new OtherDoc { Id = "remove" });
        Assert.Equal(1, await store.Query<OtherDoc>().ExecuteDelete());

        Assert.Equal(2L, await CountAsync("documents_vec_VectorDoc"));
        Assert.Equal("remove", Assert.Single(await store.NearestVectors<VectorDoc>(Probe, 1)).Document.Id);
    }

    /// <summary>Applies the same cleanup to a type-keyed JSON collection query.</summary>
    [Fact]
    public async Task TypedJsonExecuteDelete_RemovesVectorAndMapping()
    {
        var store = await SeedAsync();

        Assert.Equal(1, await store.Collection(typeof(VectorDoc)).Query().Where("tag == 'remove'").ExecuteDelete());

        await AssertSurvivingCandidateAsync(store);
    }

    /// <summary>Does not commit a document deletion when vector cleanup fails, even if an outer caller commits.</summary>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task CleanupFailure_RollsBackDocumentAndVectors(bool ambient)
    {
        var store = await SeedAsync();
        using (var connection = await OpenRawAsync())
        {
            using var command = connection.CreateCommand();
            command.CommandText = "CREATE TRIGGER reject_vector_map_delete BEFORE DELETE ON documents_vec_map_VectorDoc BEGIN SELECT RAISE(ABORT,'test cleanup failure'); END;";
            await command.ExecuteNonQueryAsync();
        }
        if (ambient)
        {
            await using var session = store.OpenSession();
            await using var transaction = await session.BeginTransaction();
            await Assert.ThrowsAsync<SqliteException>(() => session.Query<VectorDoc>().Where(x => x.Tag == "remove").ExecuteDelete());
            await transaction.Commit();
        }
        else
        {
            await Assert.ThrowsAsync<SqliteException>(() => store.Query<VectorDoc>().Where(x => x.Tag == "remove").ExecuteDelete());
        }

        Assert.Equal(2, await store.Query<VectorDoc>().Count());
        Assert.Equal(2L, await CountAsync("documents_vec_VectorDoc"));
        Assert.Equal(2L, await CountAsync("documents_vec_map_VectorDoc"));
    }

    /// <summary>Deletes mapped documents without touching vector tables when the extension is disabled.</summary>
    [Theory]
    [InlineData("query")]
    [InlineData("batch")]
    [InlineData("session")]
    public async Task Delete_WithVectorExtensionDisabled_RemovesOnlyDocuments(string path)
    {
        var options = new DocumentStoreOptions
        {
            DatabaseProvider = new SqliteDatabaseProvider($"Data Source={dbPath};Pooling=False")
            {
                EnableVectorExtension = false,
                VectorExtensionPreloaded = false
            }
        };
        options.ConfigureDocument<VectorDoc>(cfg => cfg.MapVectorProperty(x => x.Embedding, 4, VectorDistance.Cosine));
        using var store = Own(options);
        Assert.False(store.SupportsVector);
        await store.Insert(new VectorDoc { Id = "remove", Tag = "remove", Embedding = Probe });
        await store.Insert(new VectorDoc { Id = "keep", Tag = "keep", Embedding = Probe });

        if (path == "query")
            Assert.Equal(1, await store.Query<VectorDoc>().Where(x => x.Tag == "remove").ExecuteDelete());
        else if (path == "batch")
            Assert.Equal(1, await store.BatchRemove<VectorDoc>(["remove"]));
        else
        {
            await using var session = store.OpenSession();
            session.Remove<VectorDoc>("remove");
            await session.SaveChanges();
        }

        Assert.Equal("keep", Assert.Single(await store.Query<VectorDoc>().ToList()).Id);
        using var connection = new SqliteConnection($"Data Source={dbPath};Pooling=False");
        await connection.OpenAsync();
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT COUNT(*) FROM sqlite_master WHERE name IN ('documents_vec_VectorDoc', 'documents_vec_map_VectorDoc')";
        Assert.Equal(0L, Convert.ToInt64(await command.ExecuteScalarAsync()));
    }

    /// <summary>Commits cleanup through the buffered session Remove path.</summary>
    [Fact]
    public async Task SessionRemove_CommitsVectorAndDocumentDeletion()
    {
        var store = await SeedAsync();
        await using var session = store.OpenSession();
        session.Remove<VectorDoc>("remove");
        await session.SaveChanges();
        await AssertSurvivingCandidateAsync(store);
    }

    /// <summary>Commits a query delete in the caller's transaction.</summary>
    [Fact]
    public async Task ExecuteDelete_InAmbientTransaction_CommitsCleanup()
    {
        var store = await SeedAsync();
        await using var session = store.OpenSession();
        await using var transaction = await session.BeginTransaction();
        Assert.Equal(1, await session.Query<VectorDoc>().Where(x => x.Tag == "remove").ExecuteDelete());
        await transaction.Commit();
        await AssertSurvivingCandidateAsync(store);
    }

    /// <summary>Clears precisely the visible documents, and rolls back sidecars with the parent transaction.</summary>
    [Theory]
    [InlineData(false, false)]
    [InlineData(true, false)]
    [InlineData(false, true)]
    [InlineData(true, true)]
    public async Task TransactionalClear_PreservesFiltersAndRollback(bool filtered, bool rollback)
    {
        var store = Own(Options(filtered: filtered));
        await store.Insert(new VectorDoc { Id = "remove", Tag = "remove", Embedding = Probe });
        await store.Insert(new VectorDoc { Id = "keep", Tag = "keep", Embedding = VectorDoc.V(.8f, .6f, 0, 0) });
        async Task Clear(IDocumentStore tx, CancellationToken ct)
        {
            Assert.Equal(filtered ? 1 : 2, await tx.Clear<VectorDoc>(ct));
            if (rollback) throw new InvalidOperationException("test rollback");
        }
        var engine = (IUnitOfWorkEngine)store;
        if (rollback)
            await Assert.ThrowsAsync<InvalidOperationException>(() => engine.RunUnitAsync(Clear, default));
        else
            await engine.RunUnitAsync(Clear, default);
        var expected = rollback ? 2 : filtered ? 1 : 0;
        Assert.Equal(expected, await store.Query<VectorDoc>().IgnoreQueryFilters().Count());
        Assert.Equal((long)expected, await CountAsync("documents_vec_VectorDoc"));
        Assert.Equal((long)expected, await CountAsync("documents_vec_map_VectorDoc"));
        if (filtered && !rollback)
            Assert.Equal("keep", Assert.Single(await store.Query<VectorDoc>().IgnoreQueryFilters().ToList()).Id);
    }

    /// <summary>Does not clear the vectors belonging to another tenant.</summary>
    [Fact]
    public async Task TransactionalClear_PreservesOtherTenant()
    {
        var tenant = "first";
        var options = Options();
        options.TenantIdAccessor = () => tenant;
        var store = Own(options);
        await store.Insert(new VectorDoc { Id = "remove", Embedding = Probe });
        tenant = "second";
        await store.Insert(new VectorDoc { Id = "keep", Embedding = Probe });
        tenant = "first";
        await ((IUnitOfWorkEngine)store).RunUnitAsync(async (tx, ct) =>
            Assert.Equal(1, await tx.Clear<VectorDoc>(ct)), default);
        Assert.Equal(1L, await CountAsync("documents_vec_VectorDoc"));
        tenant = "second";
        Assert.Equal("keep", Assert.Single(await store.NearestVectors<VectorDoc>(Probe, 1)).Document.Id);
    }

    /// <summary>Creates custom-table vector storage when its first write is transactional.</summary>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task CustomTable_FirstTransactionalAccess_InitializesVectorsEvenAfterRollback(bool rollback)
    {
        var store = Own(Options(table: "custom_docs"));
        async Task Write(IDocumentStore tx, CancellationToken ct)
        {
            await tx.Insert(new VectorDoc { Id = "new", Embedding = Probe }, cancellationToken: ct);
            if (rollback) throw new InvalidOperationException("test rollback");
        }
        var engine = (IUnitOfWorkEngine)store;
        if (rollback)
        {
            await Assert.ThrowsAsync<InvalidOperationException>(() => engine.RunUnitAsync(Write, default));
            await store.Insert(new VectorDoc { Id = "new", Embedding = Probe });
        }
        else
            await engine.RunUnitAsync(Write, default);
        Assert.Equal("new", Assert.Single(await store.NearestVectors<VectorDoc>(Probe, 1)).Document.Id);
        Assert.Equal(1L, await CountAsync("custom_docs_vec_VectorDoc"));
    }

    /// <summary>Repairs legacy orphans when a custom table is first read inside a transaction.</summary>
    [Fact]
    public async Task CustomTable_FirstTransactionalRead_RepairsLegacyOrphans()
    {
        var original = Own(Options(table: "custom_docs"));
        await original.Insert(new VectorDoc { Id = "orphan", Embedding = Probe });
        await original.Insert(new VectorDoc { Id = "keep", Embedding = VectorDoc.V(.8f, .6f, 0, 0) });
        using (var connection = await OpenRawAsync())
        {
            using var command = connection.CreateCommand();
            command.CommandText = "DELETE FROM custom_docs WHERE Id='orphan'";
            await command.ExecuteNonQueryAsync();
        }
        var reopened = Own(Options(table: "custom_docs"));
        await ((IUnitOfWorkEngine)reopened).RunUnitAsync(async (tx, ct) =>
            Assert.Equal(1, await tx.Query<VectorDoc>().Count(ct)), default);
        Assert.Equal(1L, await CountAsync("custom_docs_vec_VectorDoc"));
        Assert.Equal("keep", Assert.Single(await reopened.NearestVectors<VectorDoc>(Probe, 1)).Document.Id);
    }

    /// <summary>Rejects configured type-name collisions before sidecar initialization.</summary>
    [Fact]
    public void CollidingMappedTypes_FailConfiguration()
    {
        var options = Options();
        options.TypeNameResolution = TypeNameResolution.FullName;
        options.ConfigureDocument<Collision.Item>(cfg => cfg.MapVectorProperty(x => x.Embedding, 4, VectorDistance.Cosine));
        options.ConfigureDocument<Collision_Item>(cfg => cfg.MapVectorProperty(x => x.Embedding, 4, VectorDistance.Cosine));
        var error = Assert.Throws<DocumentConfigurationException>(() => Own(options));
        Assert.Contains("share sidecar", error.Message);
    }

    /// <summary>Does not purge legacy vectors if an unconfigured type aliases the same sidecar.</summary>
    [Fact]
    public async Task LegacyTypeCollision_FailsBeforeRepair_PreservingVectors()
    {
        var original = await SeedAsync();
        using (var connection = await OpenRawAsync())
        {
            using var command = connection.CreateCommand();
            command.CommandText = "UPDATE documents SET TypeName='vectordoc' WHERE Id='keep'";
            await command.ExecuteNonQueryAsync();
        }
        var reopened = CreateStore();
        var error = await Assert.ThrowsAsync<SqliteException>(() => reopened.Query<VectorDoc>().Count());
        Assert.Contains("collision", error.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Equal(2L, await CountAsync("documents_vec_VectorDoc"));
        Assert.Equal(2L, await CountAsync("documents_vec_map_VectorDoc"));
    }

    /// <summary>Creates missing sidecars when vector support is enabled for an existing document table.</summary>
    [Fact]
    public async Task EnableVectors_AfterDocumentTableExists_CreatesSidecars()
    {
        var disabled = Own(Options(enabled: false));
        await disabled.Insert(new VectorDoc { Id = "new", Embedding = Probe });
        var enabled = CreateStore();
        await enabled.Upsert(new VectorDoc { Id = "new", Embedding = Probe });
        Assert.Equal("new", Assert.Single(await enabled.NearestVectors<VectorDoc>(Probe, 1)).Document.Id);
    }

    /// <summary>Repairs vectors left behind while their extension was disabled.</summary>
    [Fact]
    public async Task DisabledExtension_DeleteThenReenable_RepairsOrphans()
    {
        await SeedAsync();
        var disabled = Own(Options(enabled: false));
        Assert.Equal(1, await disabled.Query<VectorDoc>().Where(x => x.Tag == "remove").ExecuteDelete());
        Assert.Equal(2L, await CountAsync("documents_vec_VectorDoc"));
        await AssertSurvivingCandidateAsync(CreateStore());
    }

    /// <summary>A non-vector type with a colliding spelling must not block unrelated vector storage.</summary>
    [Fact]
    public async Task NonVectorTypeCollision_DoesNotBlockInitialization()
    {
        await SeedAsync();
        using (var connection = await OpenRawAsync())
        {
            using var command = connection.CreateCommand();
            command.CommandText = "INSERT INTO documents(Id,TypeName,Data,CreatedAt,UpdatedAt) VALUES('other','vectordoc','{}','2026-11-13','2026-11-13')";
            await command.ExecuteNonQueryAsync();
        }
        var reopened = CreateStore();
        Assert.Equal(2, await reopened.Query<VectorDoc>().Count());
        Assert.Equal(2L, await CountAsync("documents_vec_VectorDoc"));
        Assert.Equal(1, await reopened.Collection("vectordoc").Query().Count());
    }

    /// <summary>Reuses committed transactional initialization, without suppressing the parent's full init.</summary>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task CustomTable_CommittedInitialization_IsReusedByNextUnit(bool explicitTransaction)
    {
        var sql = new List<string>();
        var options = Options(table: "custom_docs");
        options.Logging = sql.Add;
        var store = Own(options);
        if (explicitTransaction)
        {
            await using var session = store.OpenSession();
            await using var transaction = await session.BeginTransaction();
            session.Add(new VectorDoc { Id = "new", Embedding = Probe });
            await session.SaveChanges();
            await transaction.Commit();
        }
        else
        {
            await ((IUnitOfWorkEngine)store).RunUnitAsync(async (tx, ct) =>
                await tx.Insert(new VectorDoc { Id = "new", Embedding = Probe }, cancellationToken: ct), default);
        }
        var first = sql.Count(x => x.Contains("CREATE VIRTUAL TABLE IF NOT EXISTS custom_docs_vec_VectorDoc"));
        Assert.Equal(1, first);
        await ((IUnitOfWorkEngine)store).RunUnitAsync(async (tx, ct) =>
            Assert.Equal(1, await tx.Query<VectorDoc>().Count(ct)), default);
        Assert.Equal(first, sql.Count(x => x.Contains("CREATE VIRTUAL TABLE IF NOT EXISTS custom_docs_vec_VectorDoc")));
        Assert.Equal("new", Assert.Single(await store.NearestVectors<VectorDoc>(Probe, 1)).Document.Id);
        Assert.Equal(first + 1, sql.Count(x => x.Contains("CREATE VIRTUAL TABLE IF NOT EXISTS custom_docs_vec_VectorDoc")));
    }

    private async Task<IDocumentStore> SeedAsync()
    {
        Assert.NotNull(SqliteDatabaseFixture.FindVec0BinaryPath());
        var store = CreateStore();
        await store.Insert(new VectorDoc { Id = "remove", Tag = "remove", Embedding = Probe });
        await store.Insert(new VectorDoc { Id = "keep", Tag = "keep", Embedding = VectorDoc.V(.8f, .6f, 0, 0) });
        return store;
    }

    private async Task AssertSurvivingCandidateAsync(IDocumentStore store)
    {
        Assert.Equal("keep", Assert.Single(await store.NearestVectors<VectorDoc>(Probe, 1)).Document.Id);
        Assert.Single(await store.Query<VectorDoc>().ToList());
        Assert.Equal(1L, await CountAsync("documents_vec_VectorDoc"));
        Assert.Equal(1L, await CountAsync("documents_vec_map_VectorDoc"));
    }

    private async Task<SqliteConnection> OpenRawAsync()
    {
        var connection = new SqliteConnection($"Data Source={dbPath};Pooling=False");
        await connection.OpenAsync();
        connection.LoadExtension(SqliteDatabaseFixture.FindVec0BinaryPath()!);
        return connection;
    }

    private async Task<long> CountAsync(string table)
    {
        using var connection = await OpenRawAsync();
        using var command = connection.CreateCommand();
        command.CommandText = $"SELECT COUNT(*) FROM {table}";
        return Convert.ToInt64(await command.ExecuteScalarAsync());
    }

    /// <inheritdoc />
    public void Dispose()
    {
        foreach (var store in ownedStores)
            store.Dispose();
        foreach (var suffix in new[] { "", "-wal", "-shm" })
            File.Delete(dbPath + suffix);
    }

    private DocumentStore Own(DocumentStoreOptions options)
    {
        var store = new DocumentStore(options);
        ownedStores.Add(store);
        return store;
    }

    private DocumentStoreOptions Options(bool enabled = true, string? table = null, bool filtered = false)
    {
        var options = new DocumentStoreOptions
        {
            DatabaseProvider = new SqliteDatabaseProvider($"Data Source={dbPath};Pooling=False")
            {
                EnableVectorExtension = enabled,
                VectorExtensionPath = SqliteDatabaseFixture.FindVec0BinaryPath()!
            }
        };
        options.ConfigureDocument<VectorDoc>(cfg =>
        {
            cfg.MapVectorProperty(x => x.Embedding, 4, VectorDistance.Cosine);
            if (table != null) cfg.Table = table;
            if (filtered) cfg.AddQueryFilter(x => x.Tag == "remove");
        });
        return options;
    }

    private DocumentStore CreateStore() => Own(Options());

    private sealed class OtherDoc
    {
        public string Id { get; set; } = "";
    }

    private sealed class Collision
    {
        public sealed class Item : VectorDoc { }
    }

    private sealed class Collision_Item : VectorDoc { }

    private sealed class HardenedProvider(string connectionString) : SqliteDatabaseProvider(connectionString), IDatabaseProvider
    {
        DbConnection IDatabaseProvider.CreateConnection()
        {
            var connection = base.CreateConnection();
            connection.StateChange += (_, state) =>
            {
                if (state.CurrentState != ConnectionState.Open) return;
                using var command = connection.CreateCommand();
                command.CommandText = "PRAGMA trusted_schema=OFF";
                command.ExecuteNonQuery();
            };
            return connection;
        }
    }
}
